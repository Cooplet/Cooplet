//! Managed Codex app-server adapter.

mod approvals;
mod discovery;
mod events;
mod provider;
mod transport;

use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::net::SocketAddr,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use cooplet_platform::{AdmissionGuard, PrivateFs, ProcessProbe};
use cooplet_protocol::{AuthorKind, ErrorCode};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

use self::{
    discovery::{initialize_params, wire_features},
    events::{ProtocolResult, native_id, thread_state},
    transport::{Failure, Operation, Rejection, Session},
};
use super::{
    AdapterCapabilities, AdapterConfig, ApprovalDecision, ApprovalRespondOutcome, ClientAdapter,
    DiscoveryReport, NativeSessionKey, OpenedSession, ParticipantRuntimeState as State,
    PersistedSessionRef, ProcessOwnership, SessionHandle, SessionInspection, SessionOpenMode,
    SessionOpenRequest, StopMode, StopOutcome, SubmitOutcome, SubmitTurnRequest, TuiAttachPlan,
    TurnInput,
};
use crate::error::{NodeError, Result};

const KIND: &str = "codex";
const CAPABILITIES: AdapterCapabilities = AdapterCapabilities {
    supports_create: true,
    supports_resume: true,
    supports_steer_while_working: false,
    supports_structured_output: true,
    supports_approval_observe: true,
    supports_remote_approval: true,
    supports_tui_attach: true,
};

pub struct CodexAdapter {
    binary: Option<PathBuf>,
}

struct Live {
    session: Session,
    program: PathBuf,
    socket_url: String,
    hint: String,
    output_schema: bool,
    named_output_revision: Option<u64>,
}

impl CodexAdapter {
    pub const fn new(binary: Option<PathBuf>) -> Self {
        Self { binary }
    }

    async fn open(&self, request: &SessionOpenRequest) -> ProtocolResult<OpenedSession> {
        let program =
            discovery::executable(self.binary.as_deref()).ok_or("codex_executable_unavailable")?;
        let runtime = PrivateFs::validate_private_dir(&request.runtime_dir)
            .map_err(|_| "codex_runtime_directory_invalid")?;
        let socket = runtime.join("c.sock");
        SocketAddr::from_pathname(&socket).map_err(|_| "socket_path_too_long")?;
        let socket_url = format!("unix://{}", path_text(&socket)?);
        path_text(&program)?;
        let credential = path_text(&request.cred_file_path)?;
        let cwd = path_text(&request.workspace_dir)?;
        let cooplet = env::current_exe().map_err(|_| "cooplet_executable_unavailable")?;
        let cooplet = path_text(&cooplet)?;

        let home = home_directory(&request.mode)?;
        let namespace = path_text(&home)?;
        let schema_directory = runtime.join("schema");
        let features = wire_features(&program, &schema_directory, &home).await;
        let environment_allows = provider::environment_allows_named_output();

        let mut command = Command::new(&program);
        command
            .args(["app-server", "--listen", &socket_url, "-c"])
            .arg(format!(
                "mcp_servers.cooplet.command={}",
                toml_value(json!(cooplet))
            ))
            .arg("-c")
            .arg(format!(
                "mcp_servers.cooplet.args={}",
                toml_value(json!(["node", "mcp", "--participant-cred", credential]))
            ))
            .args([
                "-c",
                "mcp_servers.cooplet.enabled=true",
                "-c",
                "mcp_servers.cooplet.required=true",
            ])
            .current_dir(&request.workspace_dir)
            .env("CODEX_HOME", &home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let spawned = request
            .spawner
            .spawn(command)
            .await
            .map_err(|_| "codex_spawn_failed")?;
        let wire = transport::connect(&socket, &spawned.identity).await?;
        let hint = format!("cooplet node attach {}", request.participant_id);
        let (session, events) = Session::start(wire, request.generation, hint.clone());
        let mut initialization = initialize_params();
        // The routing field in account/read is optional and gated by this capability.
        initialization["capabilities"] = json!({"experimentalApi": true});
        let initialized = session
            .rpc("initialize", initialization)
            .await
            .map_err(|_| "codex_initialize_failed")?;
        if initialized["codexHome"].as_str() != Some(namespace) {
            return Err("codex_home_mismatch");
        }
        session
            .request(Operation::Initialized)
            .await
            .map_err(|_| "codex_initialize_failed")?;

        let options = &request.effective_options;
        let mut params = json!({
            "cwd": cwd,
            "model": options.model,
            "config": options.effort.as_ref().map(|effort| json!({"model_reasoning_effort": effort})),
            "developerInstructions": request.instructions,
            "approvalPolicy": options.approval,
            "sandbox": options.sandbox,
        });
        let (method, expected_id) = match &request.mode {
            SessionOpenMode::Create => ("thread/start", None),
            SessionOpenMode::Resume { native_key, .. } => {
                params["threadId"] = json!(native_key.native_session_id);
                params["excludeTurns"] = json!(true);
                ("thread/resume", Some(native_key.native_session_id.as_str()))
            }
        };
        let opened = session
            .rpc(method, params)
            .await
            .map_err(|error| match error {
                Failure::Rejected(Rejection::Mcp) => "codex_required_mcp_unavailable",
                Failure::Rejected(Rejection::NoRollout) => "codex_rollout_unavailable",
                _ => "codex_thread_open_failed",
            })?;
        let id = native_id(&opened["thread"]["id"])?;
        if expected_id.is_some_and(|expected| id != expected)
            || opened["thread"]["cwd"].as_str() != Some(cwd)
            || opened["cwd"].as_str() != Some(cwd)
        {
            return Err("codex_thread_identity_mismatch");
        }
        require_mcp(&session, id).await?;
        let named_output_revision = if features.tool_output {
            provider::named_output_revision(&session, &opened, cwd, environment_allows).await
        } else {
            None
        };
        let initial_state = session.snapshot().state;
        if matches!(initial_state, State::Unknown { .. }) {
            return Err("codex_open_transport_failed");
        }

        let key = NativeSessionKey {
            client_kind: KIND.into(),
            store_namespace: namespace.into(),
            native_session_id: id.into(),
        };
        let process_ownership = ProcessOwnership::Managed;
        let adapter_binding = Value::Null;
        let persisted = PersistedSessionRef {
            participant_id: request.participant_id.clone(),
            generation: request.generation,
            native_key: Some(key.clone()),
            process_ownership,
            workspace_dir: request.workspace_dir.clone(),
            runtime_dir: runtime,
            process_identity: Some(spawned.identity),
            adapter_binding: adapter_binding.clone(),
        };
        let live = Live {
            session,
            program,
            socket_url,
            hint,
            output_schema: features.output_schema,
            named_output_revision,
        };
        Ok(OpenedSession {
            native_key: key,
            process_ownership,
            handle: SessionHandle::new(live, persisted),
            capabilities: AdapterCapabilities {
                supports_structured_output: features.output_schema,
                ..CAPABILITIES
            },
            initial_state,
            adapter_binding,
            events,
        })
    }
}

impl ClientAdapter for CodexAdapter {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn capabilities(&self) -> AdapterCapabilities {
        CAPABILITIES
    }

    fn discover<'a>(&'a self, _: &'a AdapterConfig) -> BoxFuture<'a, Result<DiscoveryReport>> {
        Box::pin(async move { Ok(discovery::discover(self.binary.as_deref()).await) })
    }

    fn create_or_resume(
        &self,
        request: SessionOpenRequest,
    ) -> BoxFuture<'_, Result<OpenedSession>> {
        Box::pin(async move {
            self.open(&request).await.map_err(|reason| {
                tracing::warn!(participant_id = %request.participant_id, generation = request.generation, reason, "Codex session could not open");
                NodeError::new(ErrorCode::Overloaded, "Codex session could not open")
            })
        })
    }

    fn inspect_state<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        handle: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>> {
        Box::pin(async move {
            let state = match live_for(target, handle) {
                Some((live, key)) => live
                    .read_thread(&key.native_session_id)
                    .await
                    .map(|thread| {
                        let state = thread_state(&thread, &live.hint);
                        match (&state, live.session.snapshot().state) {
                            (State::AwaitingApproval(_), current @ State::AwaitingApproval(_)) => {
                                current
                            }
                            _ => state,
                        }
                    })
                    .unwrap_or_else(|_| State::Unknown {
                        reason: "Codex thread inspection failed".into(),
                    }),
                None => match target
                    .process_identity
                    .as_ref()
                    .map(|process| process.probe())
                {
                    None | Some(Ok(ProcessProbe::Exited | ProcessProbe::Missing)) => State::Stopped,
                    _ => State::Unknown {
                        reason: "Codex has no live transport for this generation".into(),
                    },
                },
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
            let Some(live) = handle.downcast_ref::<Live>() else {
                return SubmitOutcome::Unavailable("Codex session is not live".into());
            };
            if request.output_schema.is_some() && !live.output_schema {
                return SubmitOutcome::Unavailable(
                    "This Codex CLI lacks native structured output".into(),
                );
            }
            if request.admission.is_expired() {
                return SubmitOutcome::Expired;
            }
            let tool = live
                .named_output_revision
                .zip(tool_name(&request.input))
                .map(|(revision, name)| (name, revision));
            let Ok(rendered) = request.input.render_canonical() else {
                return SubmitOutcome::Unavailable(
                    "Cooplet could not render the verified input".into(),
                );
            };
            match live
                .session
                .request(Operation::Turn {
                    operation_key: request.operation_key,
                    rendered,
                    tool,
                    schema: request.output_schema,
                    admission: request.admission,
                })
                .await
            {
                Ok(ack) => match native_id(&ack["turn"]["id"]) {
                    Ok(id) => SubmitOutcome::Accepted {
                        native_turn_id: Some(id.into()),
                    },
                    Err(_) => SubmitOutcome::Unknown(
                        "Codex returned an invalid turn acknowledgement".into(),
                    ),
                },
                Err(Failure::Expired) => SubmitOutcome::Expired,
                Err(Failure::Busy | Failure::Rejected(Rejection::SchemaMismatch)) => {
                    SubmitOutcome::Busy
                }
                Err(Failure::Waiting(wait)) => SubmitOutcome::AwaitingApproval(wait),
                Err(Failure::NotSent(reason)) => SubmitOutcome::Unavailable(reason.into()),
                Err(Failure::Rejected(Rejection::NoIntake | Rejection::MissingThread)) => {
                    SubmitOutcome::Unavailable("Codex rejected intake".into())
                }
                Err(Failure::Uncertain(reason)) => SubmitOutcome::Unknown(reason.into()),
                Err(_) => SubmitOutcome::Unknown("Codex did not confirm turn intake".into()),
            }
        })
    }

    fn respond_approval<'a>(
        &'a self,
        handle: &'a SessionHandle,
        decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async move {
            approval_response(handle, decision, None)
                .await
                .unwrap_or_else(|| {
                    ApprovalRespondOutcome::Unavailable(
                        "Codex approval could not be admitted".into(),
                    )
                })
        })
    }

    fn respond_approval_with_admission<'a>(
        &'a self,
        handle: &'a SessionHandle,
        decision: ApprovalDecision,
        admission: &'a AdmissionGuard,
    ) -> BoxFuture<'a, Option<ApprovalRespondOutcome>> {
        Box::pin(approval_response(handle, decision, Some(admission.clone())))
    }

    fn cancel_or_stop<'a>(
        &'a self,
        handle: &'a SessionHandle,
        mode: StopMode,
    ) -> BoxFuture<'a, Result<StopOutcome>> {
        Box::pin(async move {
            let Some(live) = handle.downcast_ref::<Live>() else {
                return Ok(StopOutcome::AlreadyGone);
            };
            let StopMode::CancelTurn { admission } = mode else {
                live.session.stop().await;
                return Ok(StopOutcome::Stopped);
            };
            let snapshot = live.session.snapshot();
            if let State::Unknown { reason } = snapshot.state {
                return Ok(StopOutcome::Unknown(reason));
            }
            let Some(turn) = snapshot.active_turn else {
                return Ok(match snapshot.state {
                    State::Idle | State::Stopped => StopOutcome::AlreadyGone,
                    _ => StopOutcome::Unknown("Codex has not identified the active turn".into()),
                });
            };
            Ok(
                match live
                    .session
                    .request(Operation::Interrupt {
                        turn_id: turn,
                        admission,
                    })
                    .await
                {
                    Ok(_) => StopOutcome::TurnCancelled,
                    Err(
                        Failure::Stale
                        | Failure::Rejected(Rejection::StaleTurn | Rejection::MissingThread),
                    ) => StopOutcome::AlreadyGone,
                    Err(Failure::Rejected(Rejection::UnsupportedMethod)) => {
                        StopOutcome::Unsupported("Codex rejected the interrupt".into())
                    }
                    Err(Failure::Expired | Failure::Rejected(Rejection::NoIntake)) => {
                        StopOutcome::Unavailable("Codex could not accept the interrupt".into())
                    }
                    Err(Failure::NotSent(reason)) => StopOutcome::Unavailable(reason.into()),
                    Err(Failure::Uncertain(reason)) => StopOutcome::Unknown(reason.into()),
                    _ => StopOutcome::Unknown("Codex did not acknowledge the interrupt".into()),
                },
            )
        })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        handle: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
        Box::pin(async move {
            let unsupported = |reason: &str| {
                Ok(TuiAttachPlan::Unsupported {
                    reason: reason.into(),
                })
            };
            let Some((live, key)) = live_for(target, handle) else {
                return unsupported("Resume the Cooplet participant before attaching to Codex");
            };
            if matches!(live.session.snapshot().state, State::Unknown { .. }) {
                return unsupported("Resume the Cooplet participant before attaching to Codex");
            }
            if !live.materialized().await {
                let _ = live.read_thread(&key.native_session_id).await;
            }
            if matches!(live.session.snapshot().state, State::Unknown { .. }) {
                return unsupported("Resume the Cooplet participant before attaching to Codex");
            }
            if !live.materialized().await {
                return unsupported(
                    "Codex has no saved conversation to attach to yet. Send a message first",
                );
            }
            Ok(TuiAttachPlan::Exec {
                program: live.program.clone(),
                args: vec![
                    "--remote".into(),
                    live.socket_url.clone(),
                    "resume".into(),
                    key.native_session_id.clone(),
                ],
                cwd: Some(target.workspace_dir.clone()),
                env: BTreeMap::from([("CODEX_HOME".into(), key.store_namespace.clone())]),
            })
        })
    }
}

async fn approval_response(
    handle: &SessionHandle,
    decision: ApprovalDecision,
    admission: Option<AdmissionGuard>,
) -> Option<ApprovalRespondOutcome> {
    let Some(live) = handle.downcast_ref::<Live>() else {
        return Some(ApprovalRespondOutcome::Unavailable(
            "Codex session is not live".into(),
        ));
    };
    Some(
        match live
            .session
            .request(Operation::Approval {
                decision,
                admission,
            })
            .await
        {
            Err(Failure::Expired) => return None,
            Err(Failure::Stale) => {
                ApprovalRespondOutcome::Stale("Codex approval is no longer current".into())
            }
            Err(Failure::Unsupported) => {
                ApprovalRespondOutcome::Unsupported("Use the Codex TUI to respond".into())
            }
            Err(Failure::NotSent(reason)) => ApprovalRespondOutcome::Unavailable(reason.into()),
            Err(Failure::Uncertain(reason)) => ApprovalRespondOutcome::Unknown(reason.into()),
            _ => ApprovalRespondOutcome::Unknown("Codex did not confirm the response".into()),
        },
    )
}

impl Live {
    async fn materialized(&self) -> bool {
        let Some(path) = self.session.snapshot().rollout_path else {
            return false;
        };
        // Codex can announce the planned rollout path before creating it.
        tokio::fs::metadata(path)
            .await
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    }

    async fn read_thread(&self, id: &str) -> std::result::Result<Value, Failure> {
        self.session
            .rpc(
                "thread/read",
                json!({"threadId": id, "includeTurns": false}),
            )
            .await
            .map(|mut result| result["thread"].take())
    }
}

fn live_for<'a>(
    target: &PersistedSessionRef,
    handle: Option<&'a SessionHandle>,
) -> Option<(&'a Live, NativeSessionKey)> {
    let handle = handle?;
    let key = target.native_key.as_ref()?;
    let persisted = handle.persisted();
    if persisted.generation != target.generation
        || persisted.participant_id != target.participant_id
        || persisted.native_key.as_ref() != Some(key)
        || key.client_kind != KIND
    {
        return None;
    }
    Some((handle.downcast_ref::<Live>()?, key.clone()))
}

fn home_directory(mode: &SessionOpenMode) -> ProtocolResult<PathBuf> {
    let path = match mode {
        SessionOpenMode::Create => {
            if let Some(home) = env::var_os("CODEX_HOME") {
                PathBuf::from(home)
            } else {
                let home = env::home_dir().ok_or("codex_home_unavailable")?;
                let path = home.join(".codex");
                path_text(&path)?;
                if !path.exists() {
                    PrivateFs::ensure_private_dir(&path).map_err(|_| "codex_home_unavailable")?;
                }
                path
            }
        }
        SessionOpenMode::Resume { native_key, .. } => {
            native_key
                .validate(KIND)
                .map_err(|_| "codex_saved_key_invalid")?;
            PathBuf::from(&native_key.store_namespace)
        }
    };
    path_text(&path)?;
    let canonical = fs::canonicalize(path).map_err(|_| "codex_home_unavailable")?;
    if !canonical.is_dir() {
        return Err("codex_home_unavailable");
    }
    if let SessionOpenMode::Resume { native_key, .. } = mode
        && canonical.as_os_str() != native_key.store_namespace.as_str()
    {
        return Err("codex_home_mismatch");
    }
    Ok(canonical)
}

fn path_text(path: &Path) -> ProtocolResult<&str> {
    path.to_str().ok_or("native_path_not_utf8")
}

fn toml_value(value: Value) -> String {
    // JSON string/array literals are also valid TOML basic strings and arrays.
    value.to_string().replace('\u{7f}', "\\u007f")
}

fn tool_name(input: &TurnInput) -> Option<&'static str> {
    if input.is_direct_human() {
        return None;
    }
    match input {
        TurnInput::Thread(_) => Some("thread_delivery"),
        TurnInput::Direct(message) => match message.author.kind {
            AuthorKind::Agent => Some("peer_message"),
            AuthorKind::Service => Some("service_message"),
            AuthorKind::Human => None,
        },
    }
}

async fn require_mcp(session: &Session, thread_id: &str) -> ProtocolResult<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let result = session
            .rpc(
                "mcpServerStatus/list",
                json!({"threadId": thread_id, "serverName": "cooplet"}),
            )
            .await
            .map_err(|_| "codex_required_mcp_unavailable")?;
        let data = result["data"]
            .as_array()
            .ok_or("codex_required_mcp_unavailable")?;
        if let Some(server) = data.iter().find(|entry| entry["name"] == "cooplet") {
            if !server["toolsError"].is_null()
                || matches!(
                    server["runtimeStatus"].as_str(),
                    Some("failed" | "disabled" | "authenticationRequired" | "cancelled")
                )
            {
                return Err("codex_required_mcp_unavailable");
            }
            if server["tools"]["whoami"].is_object()
                && matches!(server["runtimeStatus"].as_str(), Some("connected") | None)
            {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err("codex_required_mcp_unavailable");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

#[cfg(test)]
mod tests;
