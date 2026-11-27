use std::{
    error::Error,
    fs, io,
    io::Write,
    os::unix::{ffi::OsStrExt, process::CommandExt},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use clap::{Args, Parser, Subcommand};
use cooplet_client::{
    BlobUploadOutcome, BlobUploadSpec, CoopletClient, UploadVerification, api_path,
};
use cooplet_platform::{FileLock, LockProbe, PrivateFs};
use cooplet_protocol::{
    ApprovalAction, ApprovalDecision, BlobId, EventKind, ParticipantId, ParticipantState,
    PrincipalKind, RequestId, validate_idempotency_key,
};
use cooplet_storage::DEFAULT_MAX_BLOB_BYTES;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::signal::unix::{SignalKind, signal};

use cooplet_node::{
    auth::{Credential, read_master},
    clients::{
        AdapterConfig, AdapterRegistry, ParticipantPage, ParticipantView, PiAdapter, TuiAttachPlan,
        agy::{AgyAdapter, AgyAdapterConfig},
        codex::CodexAdapter,
        list_templates,
    },
    config::{NodeConfig, check_offline_state, default_state_dir, operator_path},
    daemon::Node,
    http::openapi_json,
    mcp,
};

type CliResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(
    name = "cooplet",
    version,
    about = "Run and operate a local Cooplet Node"
)]
pub struct Cli {
    /// Directory containing the Node database and local credentials (except openapi and node mcp)
    #[arg(long, global = true)]
    state_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: TopCommand,
}

#[derive(Subcommand)]
enum TopCommand {
    /// Run, inspect or manage the local daemon
    Node {
        #[command(subcommand)]
        command: NodeCommand,
    },
    /// Read the Node event stream as NDJSON
    Watch {
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, value_delimiter = ',')]
        kinds: Vec<EventKind>,
    },
    /// Upload or download a local Node attachment
    Blob {
        #[command(subcommand)]
        command: BlobCommand,
    },
    /// Print the Node API schema
    Openapi,
}

#[derive(Subcommand)]
enum NodeCommand {
    /// Run the daemon in the foreground
    Run {
        /// IPv4 loopback port to reserve on first launch (0 chooses a free port)
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        workspace_dir: Option<PathBuf>,
        #[arg(long)]
        display_name: Option<String>,
    },
    /// Check the daemon's lock and HTTP health
    Status,
    /// Inspect local Node state, templates and registered adapters
    Doctor {
        /// Workspace to inspect for agent templates (defaults to the current directory)
        #[arg(long)]
        workspace_dir: Option<PathBuf>,
    },
    /// Stop the active daemon and wait for its lock to be released
    Stop {
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Approve a pending native request
    Approve(ApprovalArgs),
    /// Deny a pending native request
    Deny(ApprovalArgs),
    /// Run the adapter's interactive attach command
    Attach {
        /// Participant name or exact participant ID
        name: String,
    },
    /// Run the participant MCP server over stdio
    Mcp {
        #[arg(long)]
        participant_cred: PathBuf,
    },
}

#[derive(Args)]
struct ApprovalArgs {
    /// Participant name or, with --approval-id, the exact participant ID
    name: String,
    #[arg(long, requires = "operation_key")]
    approval_id: Option<String>,
    #[arg(long)]
    operation_key: Option<String>,
}

#[derive(Subcommand)]
enum BlobCommand {
    /// Upload a regular file and print its attachment reference
    Put {
        path: PathBuf,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        mime_type: Option<String>,
        #[arg(long)]
        operation_key: Option<String>,
    },
    /// Download and verify an attachment before publishing it to disk
    Get {
        blob_id: BlobId,
        #[arg(long)]
        output: PathBuf,
    },
}

impl Cli {
    pub async fn run(self) -> CliResult {
        match self.command {
            TopCommand::Openapi => {
                reject_state_dir(&self.state_dir)?;
                io::stdout()
                    .lock()
                    .write_all(openapi_json().map_err(io::Error::other)?.as_bytes())?;
                Ok(())
            }
            TopCommand::Node {
                command: NodeCommand::Mcp { participant_cred },
            } => {
                reject_state_dir(&self.state_dir)?;
                mcp::run(&participant_cred).await
            }
            command => {
                let state_dir = match self.state_dir {
                    Some(dir) => dir,
                    None => default_state_dir()?,
                };
                run_local(command, &state_dir).await
            }
        }
    }
}

fn reject_state_dir(state_dir: &Option<PathBuf>) -> CliResult {
    if state_dir.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--state-dir does not apply to this command",
        )
        .into());
    }
    Ok(())
}

async fn run_local(command: TopCommand, dir: &Path) -> CliResult {
    match command {
        TopCommand::Node { command } => run_local_node(command, dir).await,
        TopCommand::Watch { after, kinds } => {
            let client = operator(dir).await?;
            let mut stream = client.stream_events(after, &kinds).await?;
            while let Some(frame) = stream.next_frame().await {
                print_json(&frame?)?;
            }
            Ok(())
        }
        TopCommand::Blob { command } => run_blob(command, dir).await,
        TopCommand::Openapi => Err(io::Error::other("Unexpected local command").into()),
    }
}

async fn run_local_node(command: NodeCommand, dir: &Path) -> CliResult {
    match command {
        NodeCommand::Run {
            port,
            workspace_dir,
            display_name,
        } => run_daemon(dir, port, workspace_dir, display_name).await,
        NodeCommand::Status => {
            let client = operator(dir).await?;
            print_json(&client.health().await?)
        }
        NodeCommand::Doctor { workspace_dir } => doctor(dir, workspace_dir.as_deref()).await,
        NodeCommand::Stop { operation_key } => stop(dir, operation_key).await,
        NodeCommand::Approve(args) => approval(dir, args, ApprovalAction::Approve).await,
        NodeCommand::Deny(args) => approval(dir, args, ApprovalAction::Deny).await,
        NodeCommand::Attach { name } => attach(dir, &name).await,
        NodeCommand::Mcp { .. } => Err(io::Error::other("Unexpected local command").into()),
    }
}

fn adapter_registry() -> CliResult<AdapterRegistry> {
    let mut registry = AdapterRegistry::default();
    registry.register(Arc::new(CodexAdapter::new(
        std::env::var_os("COOPLET_CODEX_BIN").map(PathBuf::from),
    )))?;
    registry.register(Arc::new(PiAdapter))?;
    let cooplet = fs::canonicalize(std::env::current_exe()?)?;
    registry.register(Arc::new(AgyAdapter::new(AgyAdapterConfig::new(cooplet))))?;
    Ok(registry)
}

async fn run_daemon(
    dir: &Path,
    port: Option<u16>,
    workspace_dir: Option<PathBuf>,
    display_name: Option<String>,
) -> CliResult {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let workspace_dir = match workspace_dir {
        Some(dir) => dir,
        None => std::env::current_dir()?,
    };
    let mut config = NodeConfig::new(dir.to_owned(), workspace_dir);
    config.port = port;
    config.display_name = display_name;
    let node = Node::open(config, adapter_registry()?).await?;
    let credential_path = operator_path(dir).canonicalize()?;
    let credential = Credential::read(&credential_path)?;
    let path_text = credential_path.to_str();
    let mut ready = json!({
        "node_id": credential.node_id,
        "endpoint": credential.endpoint(),
        "operator_participant_id": credential.participant_id,
        "operator_credential": path_text,
    });
    if path_text.is_none() {
        ready["operator_credential_bytes"] = json!(credential_path.as_os_str().as_bytes());
    }
    print_json(&ready)?;
    let shutdown = node.state.shutdown.clone();
    let signals = tokio::spawn(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
        shutdown.send_replace(true);
    });
    let result = node.serve().await;
    signals.abort();
    result
}

async fn operator(dir: &Path) -> CliResult<CoopletClient> {
    let lock = FileLock::probe_owner_state(&dir.join("node.lock"))?;
    if lock != LockProbe::OwnedByLiveProcess {
        return Err(
            io::Error::new(io::ErrorKind::NotConnected, "The local Node is not running").into(),
        );
    }
    let credential = Credential::read(&operator_path(dir))?;
    let (client, _) = credential.connect(PrincipalKind::Operator).await?;
    Ok(client)
}

async fn doctor(dir: &Path, workspace_dir: Option<&Path>) -> CliResult {
    let dir = PrivateFs::validate_private_dir(dir)?;
    let running =
        FileLock::probe_owner_state(&dir.join("node.lock"))? == LockProbe::OwnedByLiveProcess;
    let credential = if running {
        read_master(&dir)?;
        let credential = Credential::read(&operator_path(&dir))?;
        let (client, _) = credential.connect(PrincipalKind::Operator).await?;
        client.health().await?;
        credential
    } else {
        check_offline_state(&dir).await?
    };
    let registry = adapter_registry()?;
    let reports = registry
        .discover_all(&AdapterConfig {
            state_dir: dir.clone(),
            runtime_dir: dir.join("runtime"),
        })
        .await?;
    if reports.is_empty() {
        eprintln!("No client adapters are registered in this build");
    }
    let workspace = fs::canonicalize(workspace_dir.unwrap_or(Path::new(".")))?;
    let templates = list_templates(&workspace)?;
    print_json(&json!({
        "node_id": credential.node_id,
        "running": running,
        "state_dir": dir.display().to_string(),
        "adapters": reports,
        "workspace_dir": workspace.display().to_string(),
        "templates": templates,
    }))
}

async fn stop(dir: &Path, operation_key: Option<String>) -> CliResult {
    let client = operator(dir).await?;
    let key = operation_key_or_generate(operation_key)?;
    let receipt: Value = client
        .post_json("/v1/node/stop", &json!({}), Some(&key))
        .await?;
    // A replay keeps the current daemon ready. A newly accepted stop closes
    // readiness before returning its receipt, then releases the lock.
    if tokio::time::timeout(Duration::from_secs(2), client.health())
        .await
        .is_ok_and(|result| result.is_ok())
    {
        return print_json(&receipt);
    }
    let wait_for_unlock = async {
        loop {
            if FileLock::probe_owner_state(&dir.join("node.lock"))? == LockProbe::Idle {
                return Ok::<_, cooplet_platform::PlatformError>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(15), wait_for_unlock)
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Timed out waiting for the Node lock to be released",
            )
        })??;
    print_json(&receipt)
}

async fn participant_by_name(
    client: &CoopletClient,
    name: &str,
    include_stopped: bool,
) -> CliResult<ParticipantView> {
    let mut after = None::<ParticipantId>;
    let mut stopped = None;
    let mut ambiguous = false;
    loop {
        let query = after.as_ref().map(|id| [("after", id.to_string())]);
        let page: ParticipantPage = client
            .get_json(
                "/v1/participants",
                query.as_ref().map_or(&[], |value| value.as_slice()),
            )
            .await?;
        for participant in page
            .items
            .into_iter()
            .filter(|participant| participant.name == name)
        {
            if participant.state != ParticipantState::Stopped {
                return Ok(participant);
            }
            if include_stopped {
                if stopped.is_some() {
                    ambiguous = true;
                } else {
                    stopped = Some(participant);
                }
            }
        }
        after = page.next_after;
        if after.is_none() {
            break;
        }
    }
    if ambiguous {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "More than one stopped participant has that name. Pass a participant ID",
        )
        .into());
    }
    stopped.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No matching participant").into())
}

async fn approval(dir: &Path, args: ApprovalArgs, action: ApprovalAction) -> CliResult {
    let client = operator(dir).await?;
    let operation_key = operation_key_or_generate(args.operation_key)?;
    let (participant_id, approval_id) = if let Some(approval_id) = args.approval_id {
        (args.name.parse::<ParticipantId>()?, approval_id)
    } else {
        let participant = participant_by_name(&client, &args.name, false).await?;
        let approval_id = participant
            .approval_id
            .ok_or_else(|| io::Error::other("The participant has no pending approval"))?;
        (participant.participant_id, approval_id)
    };
    let path = api_path(&["participants", participant_id.as_str(), "approval"])?;
    print_json(
        &json!({ "participant_id": participant_id, "approval_id": approval_id, "operation_key": operation_key }),
    )?;
    let decision = ApprovalDecision {
        approval_id,
        action,
    };
    let receipt: Value = client
        .post_json(&path, &decision, Some(&operation_key))
        .await?;
    print_json(&receipt)
}

async fn attach(dir: &Path, name: &str) -> CliResult {
    let client = operator(dir).await?;
    let participant_id = match name.parse() {
        Ok(id) => id,
        Err(_) => {
            participant_by_name(&client, name, true)
                .await?
                .participant_id
        }
    };
    let path = api_path(&["participants", participant_id.as_str(), "attach-plan"])?;
    let plan: TuiAttachPlan = client.get_json(&path, &[]).await?;
    match plan {
        TuiAttachPlan::Exec {
            program,
            args,
            cwd,
            env,
        } => {
            let mut command = Command::new(program);
            command.args(args).envs(env);
            if let Some(cwd) = cwd {
                command.current_dir(cwd);
            }
            Err(command.exec().into())
        }
        TuiAttachPlan::Unsupported { reason } => {
            Err(io::Error::new(io::ErrorKind::Unsupported, reason).into())
        }
    }
}

async fn run_blob(command: BlobCommand, dir: &Path) -> CliResult {
    let client = operator(dir).await?;
    match command {
        BlobCommand::Put {
            path,
            name,
            mime_type,
            operation_key,
        } => {
            let name = match name {
                Some(value) => value,
                None => path
                    .file_name()
                    .and_then(|file| file.to_str())
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "The filename must be UTF-8 or supplied with --name",
                        )
                    })?,
            };
            if !fs::metadata(&path)?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "The input must be a regular file",
                )
                .into());
            }
            let file = tokio::fs::File::open(path).await?;
            let metadata = file.metadata().await?;
            if !metadata.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "The input must be a regular file",
                )
                .into());
            }
            let spec = BlobUploadSpec {
                filename: name,
                mime_type: mime_type.unwrap_or_else(|| "application/octet-stream".into()),
                verification: UploadVerification::LocalSha256,
                expected_sha256: None,
                idempotency_key: operation_key,
                content_length: Some(metadata.len()),
            };
            let attachment = match client.upload_blob(spec, file).await? {
                BlobUploadOutcome::VerifiedLocal(attachment)
                | BlobUploadOutcome::CompleteWithHubDigest(attachment)
                | BlobUploadOutcome::AcknowledgedWithoutLocalVerification(attachment) => attachment,
            };
            print_json(&attachment)
        }
        BlobCommand::Get { output, blob_id } => download(&client, &blob_id, &output).await,
    }
}

async fn download(client: &CoopletClient, blob_id: &BlobId, output: &Path) -> CliResult {
    let parent = output
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let path = parent.join(format!(".cooplet-download-{}", RequestId::generate()?));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .await?;
    let temporary = TemporaryDownload(path);
    client
        .download_blob_to_writer(blob_id, None, DEFAULT_MAX_BLOB_BYTES, &mut file)
        .await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary.0, output).await?;
    Ok(())
}

struct TemporaryDownload(PathBuf);

impl Drop for TemporaryDownload {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn operation_key_or_generate(key: Option<String>) -> CliResult<String> {
    let key = match key {
        Some(key) => key,
        None => RequestId::generate()?.to_string(),
    };
    validate_idempotency_key(&key)?;
    Ok(key)
}

fn print_json(value: &impl Serialize) -> CliResult {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}
