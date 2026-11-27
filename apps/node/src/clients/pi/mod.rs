//! Pi's native RPC protocol and managed participant integration.

mod rpc;
mod storage;
mod turn;

use std::{
    env, io,
    os::unix::fs::PermissionsExt as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::ErrorCode;
use futures_util::future::BoxFuture;
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt as _, sink},
    time::{Instant, sleep, timeout},
};

use crate::{
    clients::{
        AdapterCapabilities, AdapterConfig, ApprovalDecision, ApprovalRespondOutcome,
        ClientAdapter, DiscoveryReport, EffectiveAgentOptions, OpenedSession,
        ParticipantRuntimeState, PersistedSessionRef, ProcessOwnership, SessionHandle,
        SessionInspection, SessionOpenRequest, StopMode, StopOutcome, SubmitOutcome,
        SubmitTurnRequest, TuiAttachPlan,
    },
    error::{NodeError, Result},
};

use self::{
    rpc::Session,
    storage::{Binding, SessionFile},
};

const KIND: &str = "pi";
const EXTENSION_FILENAME: &str = "participant.ts";
const STARTUP_FILENAME: &str = "startup.json";
const CREDENTIAL_ENV: &str = "COOPLET_PARTICIPANT_CRED_FILE";
const MCP_BINARY_ENV: &str = "COOPLET_PARTICIPANT_MCP_BIN";
const BUILTIN_MCP: &str = "builtin:mcp";
const REQUIRED_TOOLS: &[&str] = &[
    "mcp__cooplet__whoami",
    "mcp__cooplet__list_participants",
    "mcp__cooplet__send_message",
    "mcp__cooplet__ask_user",
    "mcp__cooplet__blob_put",
];

pub struct PiAdapter;

#[derive(Serialize)]
struct Startup<'a> {
    executable: &'a Path,
    credential: &'a Path,
    instructions: &'a str,
}

impl ClientAdapter for PiAdapter {
    fn kind(&self) -> &'static str {
        KIND
    }

    fn discover<'a>(
        &'a self,
        _config: &'a AdapterConfig,
    ) -> BoxFuture<'a, Result<DiscoveryReport>> {
        Box::pin(async { Ok(discover().await) })
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
        Box::pin(open(request, self.capabilities()))
    }

    fn inspect_state<'a>(
        &'a self,
        _target: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>> {
        Box::pin(async move {
            let session = live.and_then(SessionHandle::downcast_ref::<Session>);
            let state = session.map_or_else(
                || ParticipantRuntimeState::Unknown {
                    reason: "pi_rpc_not_live".into(),
                },
                |session| session.state().runtime_state(),
            );
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
            let Some(session) = handle.downcast_ref::<Session>() else {
                return SubmitOutcome::Unavailable("Pi RPC is not live".into());
            };
            if request.steer || request.output_schema.is_some() {
                return SubmitOutcome::Unavailable(
                    "Pi does not support steering or structured output".into(),
                );
            }
            if request.admission.is_expired() {
                return SubmitOutcome::Expired;
            }
            let Ok(message) = request.input.render_canonical() else {
                return SubmitOutcome::Unavailable("Could not render the verified Pi input".into());
            };
            session
                .submit(request.operation_key, &message, &request.admission)
                .await
        })
    }

    fn respond_approval<'a>(
        &'a self,
        _handle: &'a SessionHandle,
        _decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async {
            ApprovalRespondOutcome::Unsupported("Pi has no remote approval support".into())
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
            match mode {
                StopMode::CancelTurn => Ok(session.cancel().await),
                StopMode::StopSession => {
                    session.close().await;
                    // The managed process owner confirms group termination.
                    Ok(StopOutcome::Unknown(
                        "Pi RPC closed; managed process cleanup is pending".into(),
                    ))
                }
            }
        })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        _target: &'a PersistedSessionRef,
        _live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
        Box::pin(async {
            Ok(TuiAttachPlan::Unsupported {
                reason:
                    "Cooplet manages Pi sessions exclusively and does not support TUI attachment"
                        .into(),
            })
        })
    }
}

async fn open(
    request: SessionOpenRequest,
    capabilities: AdapterCapabilities,
) -> Result<OpenedSession> {
    validate_options(&request.effective_options)?;
    let instructions = request
        .instructions
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| unavailable("Frozen Cooplet instructions are unavailable"))?;
    let binary =
        executable().ok_or_else(|| unavailable("The Pi executable was not found on PATH"))?;
    let cooplet = env::current_exe()
        .map_err(|_| unavailable("Could not resolve the Cooplet executable for Pi MCP"))?;
    let runtime = PrivateFs::validate_private_dir(&request.runtime_dir)
        .map_err(|_| unavailable("Could not open the private Pi generation directory"))?;
    let extension = runtime.join(EXTENSION_FILENAME);
    let extension_path = extension
        .to_str()
        .ok_or_else(|| unavailable("The Pi extension path is not valid UTF-8"))?;
    let startup = serde_json::to_vec(&Startup {
        executable: &cooplet,
        credential: &request.cred_file_path,
        instructions,
    })
    .map_err(|_| unavailable("Could not encode the private Pi startup configuration"))?;
    PrivateFs::write_private_file_atomic(&runtime, STARTUP_FILENAME, &startup)
        .and_then(|()| {
            PrivateFs::write_private_file_atomic(
                &runtime,
                EXTENSION_FILENAME,
                include_bytes!("participant.ts"),
            )
        })
        .map_err(|_| unavailable("Could not prepare the private Pi extension files"))?;

    let file = SessionFile::prepare(
        &request.workspace_dir,
        &request.participant_id,
        request.mode,
    )?;
    let mut command = Command::new(binary);
    command
        .args(["--mode", "rpc", "--session"])
        .arg(&file.path)
        .args(["--extension", extension_path])
        .current_dir(&file.workspace)
        .env("PI_CODING_AGENT_DIR", &file.agent_dir)
        .env_remove(CREDENTIAL_ENV)
        .env_remove(MCP_BINARY_ENV)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(model) = request.effective_options.model {
        command.args(["--model", &model]);
    }
    if let Some(effort) = request.effective_options.effort {
        command.args(["--thinking", &effort]);
    }
    let spawned = request
        .spawner
        .spawn(command)
        .await
        .map_err(|_| unavailable("Could not start the managed Pi process"))?;
    let identity = spawned.identity.clone();
    let (session, events) =
        Session::new(spawned).map_err(|_| unavailable("Could not open the Pi RPC pipes"))?;
    let state = session
        .request("get_state", None, false)
        .await
        .ok()
        .filter(|reply| reply.success)
        .ok_or_else(|| unavailable("Pi did not report its native session state"))?;
    if state.data["isStreaming"] != false
        || state.data["isCompacting"] != false
        || state.data["pendingMessageCount"].as_u64() != Some(0)
    {
        return Err(unavailable("Pi did not open an idle session"));
    }
    let native_key = file.verify(&state.data)?;
    verify_mcp(&session, extension_path).await?;
    if !session.mark_ready() {
        return Err(unavailable("Pi RPC did not remain idle through startup"));
    }
    let adapter_binding = serde_json::to_value(Binding {
        session_file: file.path,
    })
    .map_err(|_| unavailable("Could not encode the Pi session binding"))?;

    Ok(OpenedSession {
        handle: SessionHandle::new(
            session,
            PersistedSessionRef {
                participant_id: request.participant_id,
                generation: request.generation,
                native_key: Some(native_key.clone()),
                process_ownership: ProcessOwnership::Managed,
                workspace_dir: file.workspace,
                runtime_dir: runtime,
                process_identity: Some(identity),
                adapter_binding: adapter_binding.clone(),
            },
        ),
        native_key,
        process_ownership: ProcessOwnership::Managed,
        capabilities,
        initial_state: ParticipantRuntimeState::Idle,
        adapter_binding,
        events,
    })
}

fn validate_options(options: &EffectiveAgentOptions) -> Result<()> {
    let invalid = |message| NodeError::new(ErrorCode::InvalidRequest, message);
    if options.approval.is_some() || options.sandbox.is_some() {
        return Err(invalid(
            "Pi does not support explicit approval or sandbox options",
        ));
    }
    if let Some(effort) = options.effort.as_deref()
        && !matches!(
            effort,
            "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max"
        )
    {
        return Err(invalid("Invalid Pi thinking level"));
    }
    if options
        .model
        .as_deref()
        .is_some_and(|model| model.trim().is_empty() || model.starts_with('-'))
    {
        return Err(invalid("Invalid Pi model"));
    }
    Ok(())
}

async fn verify_mcp(session: &Session, extension: &str) -> Result<()> {
    let commands = session
        .request("get_commands", None, false)
        .await
        .ok()
        .filter(|reply| reply.success)
        .ok_or_else(|| unavailable("Pi did not report its startup commands"))?;
    let mcp = extension_command(&commands.data, "mcp", BUILTIN_MCP)
        .ok_or_else(|| unavailable("The built-in Pi MCP command is unavailable"))?;
    let diagnostic = extension_command(&commands.data, "cooplet-check-participant", extension)
        .ok_or_else(|| unavailable("The bundled Pi extension is unavailable"))?;

    let end = Instant::now() + Duration::from_secs(5);
    let mcp_command = format!("/{mcp}");
    loop {
        let notes = diagnostic_prompt(session, &mcp_command).await?;
        match mcp_status(&notes) {
            McpStatus::Ready => break,
            McpStatus::Connecting if Instant::now() < end => {
                sleep(Duration::from_millis(100)).await
            }
            _ => {
                return Err(unavailable(
                    "Pi did not verify the Cooplet MCP server and its direct exposure",
                ));
            }
        }
    }

    let notes = diagnostic_prompt(session, &format!("/{diagnostic}")).await?;
    let tools = notes
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|mut note| note.get_mut("coopletParticipantTools").map(Value::take));
    if !tools.is_some_and(|tools| {
        tools.as_array().is_some_and(|actual| {
            REQUIRED_TOOLS
                .iter()
                .all(|required| actual.iter().any(|tool| tool == *required))
        })
    }) {
        return Err(unavailable(
            "Pi did not verify the active Cooplet participant tools",
        ));
    }
    Ok(())
}

fn extension_command<'a>(commands: &'a Value, name: &str, source: &str) -> Option<&'a str> {
    let mut candidates = commands["commands"].as_array()?.iter().filter_map(|entry| {
        if entry["source"] != "extension" || entry["sourceInfo"]["path"] != source {
            return None;
        }
        let invocation = entry["name"].as_str()?;
        (invocation == name
            || invocation
                .strip_prefix(name)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .is_some_and(|number| {
                    !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
                }))
        .then_some(invocation)
    });
    let command = candidates.next()?;
    candidates.next().is_none().then_some(command)
}

async fn diagnostic_prompt(session: &Session, command: &str) -> Result<Vec<String>> {
    session
        .request("prompt", Some(command), true)
        .await
        .ok()
        .filter(|reply| reply.success && reply.data["disposition"] == "handled")
        .map(|reply| reply.notifications)
        .ok_or_else(|| unavailable("Pi did not complete a startup diagnostic"))
}

enum McpStatus {
    Ready,
    Connecting,
    Invalid,
}

fn mcp_status(notifications: &[String]) -> McpStatus {
    let lines = || notifications.iter().flat_map(|message| message.lines());
    if lines().any(|line| line.starts_with("overridden: \"cooplet\" ")) {
        return McpStatus::Invalid;
    }
    let mut servers = lines().filter_map(|line| line.strip_prefix("cooplet: "));
    let Some(status) = servers.next().filter(|_| servers.next().is_none()) else {
        return McpStatus::Invalid;
    };
    if status
        .strip_prefix("connected, ")
        .and_then(|rest| rest.strip_suffix(" tools (direct)"))
        .and_then(|count| count.parse::<usize>().ok())
        .is_some_and(|count| count >= REQUIRED_TOOLS.len())
    {
        McpStatus::Ready
    } else if matches!(status, "starting (direct)" | "connecting (direct)") {
        McpStatus::Connecting
    } else {
        McpStatus::Invalid
    }
}

fn unavailable(message: &'static str) -> NodeError {
    tracing::warn!(detail = message, "Pi could not open a managed session");
    NodeError::new(ErrorCode::Overloaded, message)
}

fn executable() -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    let cwd = env::current_dir().ok()?;
    env::split_paths(&path)
        .map(|directory| cwd.join(directory).join(KIND))
        .find(|candidate| {
            candidate
                .metadata()
                .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
        })
}

async fn discover() -> DiscoveryReport {
    let mut report = DiscoveryReport {
        client_kind: KIND.into(),
        available: false,
        binary_path: executable(),
        version: None,
        capabilities: AdapterCapabilities::default(),
        detail: Some("The Pi executable was not found on PATH".into()),
    };
    let Some(binary) = report.binary_path.as_ref() else {
        return report;
    };
    report.detail = Some("Pi did not advertise native RPC, session and extension support".into());
    let Some(help) = probe(binary, "--help").await else {
        return report;
    };
    let declares = |flag: &str| {
        help.lines()
            .filter_map(|line| line.split_whitespace().next())
            .any(|word| word.trim_end_matches(',') == flag)
    };
    if ![
        "--mode",
        "--session",
        "--extension",
        "--model",
        "--thinking",
    ]
    .into_iter()
    .all(declares)
        || !help
            .lines()
            .any(|line| line.split_whitespace().next() == Some("--mode") && line.contains("rpc"))
    {
        return report;
    }
    report.version = probe(binary, "--version")
        .await
        .map(|value| value.trim().to_owned())
        .filter(|value| {
            !value.is_empty() && value.len() <= 128 && value.chars().all(|ch| !ch.is_control())
        });
    report.available = true;
    report.capabilities = PiAdapter.capabilities();
    report.detail = None;
    report
}

async fn probe(binary: &Path, flag: &str) -> Option<String> {
    let output = timeout(Duration::from_secs(4), async {
        let mut process = tokio::process::Command::new(binary)
            .arg(flag)
            .env("PI_OFFLINE", "1")
            .env_remove(CREDENTIAL_ENV)
            .env_remove(MCP_BINARY_ENV)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let mut stdout = process
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("Pi probe pipe is missing"))?;
        let mut output = Vec::new();
        (&mut stdout)
            .take(64 * 1024)
            .read_to_end(&mut output)
            .await?;
        let _ = tokio::io::copy(&mut stdout, &mut sink()).await?;
        Ok::<_, io::Error>(process.wait().await?.success().then_some(output))
    })
    .await
    .ok()?
    .ok()??;
    String::from_utf8(output).ok()
}
