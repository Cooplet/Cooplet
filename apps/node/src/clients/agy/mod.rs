//! Managed Antigravity CLI sessions over ACP v1 stdio.

mod acp_wire;
mod inbound;
mod open;
mod session;
mod settings;

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use cooplet_platform::{ProcessHandle, ProcessProbe, SpawnError};
use cooplet_protocol::ErrorCode;
use futures_util::future::BoxFuture;
use serde_json::json;

use self::{acp_wire::Wire, session::Session, settings::StoreBinding};
use super::{
    AdapterCapabilities, AdapterConfig, ApprovalDecision, ApprovalRespondOutcome, ClientAdapter,
    DiscoveryReport, NativeSessionKey, OpenedSession, ParticipantRuntimeState, PersistedSessionRef,
    ProcessOwnership, SessionHandle, SessionInspection, SessionOpenMode, SessionOpenRequest,
    SpawnManagedError, StopMode, StopOutcome, SubmitOutcome, SubmitTurnRequest, TuiAttachPlan,
};
use crate::error::{NodeError, Result};

const KIND: &str = "agy";
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone, Debug)]
pub struct AgyAdapterConfig {
    /// An explicit executable, or `None` to search PATH.
    pub agy_binary: Option<PathBuf>,
    /// Absolute Cooplet executable used for the participant's MCP server.
    pub cooplet_binary: PathBuf,
    /// An absolute path or a path relative to HOME. Defaults to `.gemini`.
    pub gemini_dir: Option<PathBuf>,
    /// A path relative to the Gemini directory. Defaults to `antigravity-cli`.
    pub app_data_dir: Option<PathBuf>,
    /// Time after prompt handoff during which an immediate RPC error is detected.
    pub fail_fast_window: Duration,
}

impl AgyAdapterConfig {
    pub fn new(cooplet_binary: PathBuf) -> Self {
        Self {
            agy_binary: None,
            cooplet_binary,
            gemini_dir: None,
            app_data_dir: None,
            fail_fast_window: Duration::from_millis(75),
        }
    }
}

pub struct AgyAdapter {
    config: AgyAdapterConfig,
}

impl AgyAdapter {
    pub fn new(config: AgyAdapterConfig) -> Self {
        Self { config }
    }

    fn binary(&self) -> Option<PathBuf> {
        if let Some(path) = &self.config.agy_binary {
            return fs::canonicalize(path)
                .ok()
                .filter(|path| settings::executable(path));
        }
        env::split_paths(&env::var_os("PATH")?)
            .map(|dir| dir.join("agy"))
            .filter(|path| settings::executable(path))
            .find_map(|path| fs::canonicalize(path).ok())
    }

    async fn discovery(&self) -> DiscoveryReport {
        let binary = self.binary();
        let mut report = DiscoveryReport {
            client_kind: KIND.into(),
            available: false,
            binary_path: binary.clone(),
            version: None,
            capabilities: AdapterCapabilities::default(),
            detail: None,
        };
        let Some(binary) = &binary else {
            report.detail = Some("Agy executable was not found".into());
            return report;
        };
        let result = match tokio::time::timeout(DISCOVERY_TIMEOUT, probe(binary)).await {
            Ok(result) => result,
            Err(_) => Err(failure(
                ErrorCode::Overloaded,
                "Agy ACP discovery timed out",
            )),
        };
        match result {
            Ok(info) => {
                report.available = true;
                report.version = info.version;
                report.capabilities = self.capabilities();
            }
            Err(error) => report.detail = Some(error.0.detail),
        }
        report
    }

    async fn open(&self, request: SessionOpenRequest) -> Result<OpenedSession> {
        settings::validate_options(&request.effective_options)?;
        let cwd = settings::path_text(&request.workspace_dir)?;
        let credential = settings::path_text(&request.cred_file_path)?;
        let cooplet = &self.config.cooplet_binary;
        let cooplet = if cooplet.is_absolute() && cooplet.to_str().is_some() {
            fs::canonicalize(cooplet)
                .ok()
                .filter(|path| settings::executable(path))
        } else {
            None
        }
        .ok_or_else(|| invalid("The Cooplet MCP executable must be an absolute executable path"))?;
        let cooplet = settings::path_text(&cooplet)?;
        let mut binding = StoreBinding::resolve(&self.config, &request.mode)?;
        let binary = self
            .binary()
            .ok_or_else(|| failure(ErrorCode::Overloaded, "Agy executable was not found"))?;

        let mut command = acp_command(&binary);
        command
            .arg("--dangerously-skip-permissions")
            .arg("--gemini-dir")
            .arg(&binding.gemini_dir)
            .arg("--app-data-dir")
            .arg(&binding.app_data_dir)
            .current_dir(&request.workspace_dir)
            .stderr(Stdio::piped());
        let spawned = request
            .spawner
            .spawn(command)
            .await
            .map_err(spawn_failure)?;
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (spawned.stdin, spawned.stdout, spawned.stderr)
        else {
            return Err(failure(
                ErrorCode::InternalError,
                "Managed Agy process has missing pipes",
            ));
        };
        let mut wire = Wire::start(stdin, stdout, Some(stderr)).map_err(|_| {
            failure(
                ErrorCode::Overloaded,
                "Could not start Agy ACP pipe workers",
            )
        })?;
        let info = open::initialize(&mut wire).await?;
        let mut params = json!({
            "cwd": cwd,
            "mcpServers": [{
                "name": "cooplet",
                "command": cooplet,
                "args": ["node", "mcp", "--participant-cred", credential],
                "env": []
            }]
        });
        if let Some(instructions) = request.instructions {
            params["_meta"] = json!({"antigravity": {"systemPrompt": {"append": instructions}}});
        }
        let method = match &request.mode {
            SessionOpenMode::Create => "session/new",
            SessionOpenMode::Resume { native_key, .. } => {
                params["sessionId"] = json!(native_key.native_session_id);
                info.reopen_method()
            }
        };
        let native_session_id =
            open::open(&mut wire, method, params, &request.effective_options).await?;
        let store_namespace = binding
            .check_paths(true)?
            .ok_or_else(|| invalid("Agy session store is missing"))?;
        let native_key = NativeSessionKey {
            client_kind: KIND.into(),
            store_namespace,
            native_session_id: native_session_id.clone(),
        };
        if let SessionOpenMode::Resume {
            native_key: expected,
            ..
        } = &request.mode
            && &native_key != expected
        {
            return Err(invalid("Agy session identity changed during reopen"));
        }
        let adapter_binding = serde_json::to_value(binding).map_err(|_| {
            failure(
                ErrorCode::InternalError,
                "Could not encode Agy session binding",
            )
        })?;
        let persisted = PersistedSessionRef {
            participant_id: request.participant_id,
            generation: request.generation,
            native_key: Some(native_key.clone()),
            process_ownership: ProcessOwnership::Managed,
            workspace_dir: request.workspace_dir,
            runtime_dir: request.runtime_dir,
            process_identity: Some(spawned.identity),
            adapter_binding: adapter_binding.clone(),
        };
        let (session, events) =
            Session::start(wire, native_session_id, self.config.fail_fast_window);
        Ok(OpenedSession {
            native_key,
            process_ownership: ProcessOwnership::Managed,
            handle: SessionHandle::new(session, persisted),
            capabilities: self.capabilities(),
            initial_state: ParticipantRuntimeState::Idle,
            adapter_binding,
            events,
        })
    }
}

impl ClientAdapter for AgyAdapter {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn discover<'a>(
        &'a self,
        _config: &'a AdapterConfig,
    ) -> BoxFuture<'a, Result<DiscoveryReport>> {
        Box::pin(async { Ok(self.discovery().await) })
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            supports_create: true,
            supports_resume: true,
            ..AdapterCapabilities::default()
        }
    }

    fn create_or_resume(
        &self,
        request: SessionOpenRequest,
    ) -> BoxFuture<'_, Result<OpenedSession>> {
        Box::pin(self.open(request))
    }

    fn inspect_state<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>> {
        Box::pin(async move {
            let state = if let Some(handle) = live {
                handle
                    .downcast_ref::<Session>()
                    .ok_or_else(|| failure(ErrorCode::InternalError, "Invalid Agy session handle"))?
                    .state()
            } else if let Some(identity) = target.process_identity.clone() {
                match tokio::task::spawn_blocking(move || identity.probe()).await {
                    Ok(Ok(ProcessProbe::Missing | ProcessProbe::IdentityMismatch)) => {
                        ParticipantRuntimeState::Stopped
                    }
                    Ok(Ok(ProcessProbe::Running | ProcessProbe::Exited)) => {
                        ParticipantRuntimeState::Unknown {
                            reason: "Agy process still present without a live transport".into(),
                        }
                    }
                    Ok(Err(_)) | Err(_) => ParticipantRuntimeState::Unknown {
                        reason: "Agy process identity could not be verified".into(),
                    },
                }
            } else {
                ParticipantRuntimeState::Stopped
            };
            Ok(SessionInspection {
                state,
                detail: None,
            })
        })
    }

    fn submit_turn<'a>(
        &'a self,
        handle: &'a SessionHandle,
        request: SubmitTurnRequest,
    ) -> BoxFuture<'a, SubmitOutcome> {
        Box::pin(async move {
            if let Some(session) = handle.downcast_ref::<Session>() {
                session.submit(request).await
            } else {
                SubmitOutcome::Unavailable("Invalid Agy session handle".into())
            }
        })
    }

    fn respond_approval<'a>(
        &'a self,
        _handle: &'a SessionHandle,
        _decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async {
            ApprovalRespondOutcome::Unsupported(
                "Remote approvals are not supported for Agy full-access sessions".into(),
            )
        })
    }

    fn cancel_or_stop<'a>(
        &'a self,
        handle: &'a SessionHandle,
        mode: StopMode,
    ) -> BoxFuture<'a, Result<StopOutcome>> {
        Box::pin(async move {
            let Some(session) = handle.downcast_ref::<Session>() else {
                return Ok(StopOutcome::AlreadyGone);
            };
            Ok(match mode {
                StopMode::CancelTurn { admission } => session.cancel(admission).await,
                StopMode::StopSession => session.stop(),
            })
        })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        _target: &'a PersistedSessionRef,
        _live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
        Box::pin(async {
            Ok(TuiAttachPlan::Unsupported {
                reason: "Interactive TUI attach is not supported for Agy sessions".into(),
            })
        })
    }
}

fn acp_command(binary: &Path) -> Command {
    let mut command = Command::new(binary);
    command
        .args([
            "acp",
            "--stdio",
            "--disable-telemetry",
            "--include-default-customizations=false",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
    for (key, _) in
        env::vars_os().filter(|(key, _)| key.as_encoded_bytes().starts_with(b"COOPLET_"))
    {
        command.env_remove(key);
    }
    command
}

async fn probe(binary: &Path) -> Result<open::AgentInfo> {
    let process =
        match ProcessHandle::spawn_isolated_group(acp_command(binary).stderr(Stdio::null())) {
            Ok(process) => process,
            Err(SpawnError::CleanupPending { mut cleanup, .. }) => {
                tokio::task::spawn_blocking(move || {
                    let _ = cleanup.terminate_and_reap(Duration::ZERO);
                });
                return Err(failure(
                    ErrorCode::Overloaded,
                    "Could not verify Agy discovery process startup",
                ));
            }
            Err(_) => {
                return Err(failure(
                    ErrorCode::Overloaded,
                    "Could not start Agy discovery process",
                ));
            }
        };
    let mut probe = ProbeProcess(Some(process));
    let wire = probe
        .0
        .as_mut()
        .and_then(|process| Some((process.take_stdin()?, process.take_stdout()?)));
    let (stdin, stdout) =
        wire.ok_or_else(|| failure(ErrorCode::InternalError, "Agy discovery pipes are missing"))?;
    let mut wire = Wire::start(stdin, stdout, None).map_err(|_| {
        failure(
            ErrorCode::Overloaded,
            "Could not start Agy discovery pipe workers",
        )
    })?;
    let info = open::initialize(&mut wire).await;
    drop(wire);
    if let Some(cleanup) = probe.reap() {
        cleanup.await.map_err(|_| {
            failure(
                ErrorCode::Overloaded,
                "Could not clean up the Agy discovery process",
            )
        })??;
    }
    info
}

struct ProbeProcess(Option<ProcessHandle>);

impl ProbeProcess {
    fn reap(&mut self) -> Option<tokio::task::JoinHandle<Result<()>>> {
        let mut process = self.0.take()?;
        Some(tokio::task::spawn_blocking(move || {
            let verified = process
                .terminate_and_reap(Duration::ZERO)
                .is_ok_and(|termination| {
                    termination.outcome != cooplet_platform::TerminateOutcome::CleanupUnverified
                });
            if verified {
                Ok(())
            } else {
                Err(failure(
                    ErrorCode::Overloaded,
                    "Could not clean up the Agy discovery process",
                ))
            }
        }))
    }
}

impl Drop for ProbeProcess {
    fn drop(&mut self) {
        // Cancellation still schedules group termination with the owned Child.
        self.reap();
    }
}

fn spawn_failure(error: SpawnManagedError) -> NodeError {
    match error {
        SpawnManagedError::AdmissionExpired => crate::operations::admission_expired(),
        SpawnManagedError::SpawnFailed(_) => failure(ErrorCode::Overloaded, "Could not spawn Agy"),
        SpawnManagedError::CleanupPending(_) => failure(
            ErrorCode::DeliveryUnknown,
            "Agy launch cleanup is unconfirmed",
        ),
        SpawnManagedError::PersistenceFailed(_) | SpawnManagedError::AlreadySpawned => failure(
            ErrorCode::InternalError,
            "Could not establish Agy process ownership",
        ),
    }
}

fn invalid(detail: &'static str) -> NodeError {
    failure(ErrorCode::InvalidRequest, detail)
}

fn failure(code: ErrorCode, detail: &'static str) -> NodeError {
    tracing::warn!(%code, reason = detail, "Agy adapter operation failed");
    NodeError::new(code, detail)
}
