use std::{
    error::Error,
    fs,
    io::{self, Write},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};

use cooplet_client::{ClientError, CoopletClient};
use cooplet_node::{
    auth::Credential,
    clients::{
        AdapterBinding, AdapterCapabilities, AdapterConfig, AdapterRegistry, ApprovalDecision,
        ApprovalRespondOutcome, ClientAdapter, DiscoveryReport, LifecycleBlockKind,
        NativeSessionKey, OpenedSession, ParticipantRuntimeState, ParticipantView,
        PersistedSessionRef, ProcessOwnership, SessionHandle, SessionInspection, SessionOpenMode,
        SessionOpenRequest, StopMode, StopOutcome, SubmitOutcome, SubmitTurnRequest, TuiAttachPlan,
    },
    config::{NodeConfig, operator_path},
    daemon::Node,
    error::{NodeError, Result as NodeResult},
};
use cooplet_platform::{ProcessIdentity, ProcessProbe};
use cooplet_protocol::{ErrorCode, ParticipantId, ParticipantState, PrincipalKind};
use cooplet_storage::ReadPool;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const WAIT: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(10);
const ROOT_ENV: &str = "COOPLET_MANAGED_OPEN_TEST_ROOT";
const ATTACH_ARG: &str = "a single attach argument with spaces";
const COOPLET: &str = env!("CARGO_BIN_EXE_cooplet");

struct Tree(PathBuf);

impl Tree {
    fn new() -> TestResult<Self> {
        let root =
            std::env::temp_dir().join(format!("cooplet-managed-{}", ParticipantId::generate()?));
        fs::create_dir_all(root.join("workspace"))?;
        Ok(Self(root))
    }

    fn state(&self) -> PathBuf {
        self.0.join("state")
    }

    fn workspace(&self) -> PathBuf {
        self.0.join("workspace")
    }

    fn config(&self) -> NodeConfig {
        NodeConfig::new(self.state(), self.workspace())
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct ChildGuard(Child);

impl ChildGuard {
    async fn status(&mut self, stage: &'static str) -> TestResult<ExitStatus> {
        Ok(tokio::time::timeout(WAIT, async {
            loop {
                if let Some(status) = self.0.try_wait()? {
                    return Ok::<_, io::Error>(status);
                }
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .map_err(|error| timed_out(stage, error))??)
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[derive(Clone, Copy)]
enum OpenMode {
    Succeed,
    PauseAfterSpawn,
    FailBeforeSpawn,
    FailAfterSpawn,
    #[cfg(target_os = "linux")]
    ExitLeaderWithGrandchild,
}

struct ManagedAdapter {
    root: PathBuf,
    mode: Mutex<OpenMode>,
}

impl ManagedAdapter {
    fn new(root: &Path, mode: OpenMode) -> Self {
        Self {
            root: root.to_owned(),
            mode: Mutex::new(mode),
        }
    }

    fn set_mode(&self, mode: OpenMode) -> TestResult {
        *self
            .mode
            .lock()
            .map_err(|_| io::Error::other("Open fixture mode is poisoned"))? = mode;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SpawnReceipt {
    participant_id: ParticipantId,
    credential: PathBuf,
    process: ProcessIdentity,
}

fn receipt_path(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("spawn-{generation}.json"))
}

fn read_receipt(root: &Path, generation: u64) -> TestResult<SpawnReceipt> {
    Ok(serde_json::from_slice(&fs::read(receipt_path(
        root, generation,
    ))?)?)
}

fn open_error() -> NodeError {
    NodeError::new(
        ErrorCode::InvalidRequest,
        "The fixture declined to open its native session",
    )
}

impl ClientAdapter for ManagedAdapter {
    fn kind(&self) -> &'static str {
        "managed-fixture"
    }

    fn discover<'a>(&'a self, _: &'a AdapterConfig) -> BoxFuture<'a, NodeResult<DiscoveryReport>> {
        Box::pin(async move {
            Ok(DiscoveryReport {
                client_kind: self.kind().into(),
                available: true,
                binary_path: Some(std::env::current_exe()?),
                version: None,
                capabilities: self.capabilities(),
                detail: None,
            })
        })
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            supports_create: true,
            supports_resume: true,
            supports_tui_attach: true,
            ..AdapterCapabilities::default()
        }
    }

    fn create_or_resume(
        &self,
        request: SessionOpenRequest,
    ) -> BoxFuture<'_, NodeResult<OpenedSession>> {
        Box::pin(async move {
            let mode = *self
                .mode
                .lock()
                .map_err(|_| NodeError::internal("Open fixture mode is poisoned"))?;
            let credential = Credential::read(&request.cred_file_path)?;
            if credential.participant_id != request.participant_id {
                return Err(NodeError::internal(
                    "The fixture received another participant's credential",
                ));
            }
            if matches!(mode, OpenMode::FailBeforeSpawn) {
                return Err(open_error());
            }

            let command = match mode {
                #[cfg(target_os = "linux")]
                OpenMode::ExitLeaderWithGrandchild => {
                    let mut cmd = Command::new("sh");
                    cmd.args(["-c", "sleep 60 & exit 0"])
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::null());
                    cmd
                }
                _ => {
                    let mut cmd = Command::new(std::env::current_exe()?);
                    cmd.args(["--ignored", "--exact", "managed_worker"])
                        .stdin(Stdio::piped())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null());
                    cmd
                }
            };
            let launched = request
                .spawner
                .spawn(command)
                .await
                .map_err(NodeError::internal)?;
            let receipt = SpawnReceipt {
                participant_id: request.participant_id.clone(),
                credential: request.cred_file_path,
                process: launched.identity.clone(),
            };
            let destination = receipt_path(&self.root, request.generation);
            let staging = destination.with_extension("pending");
            let mut marker = fs::File::create(&staging)?;
            marker.write_all(&serde_json::to_vec(&receipt).map_err(NodeError::internal)?)?;
            marker.sync_all()?;
            fs::rename(staging, destination)?;

            // Keep the pipe open while paused. A killed Node closes it and lets
            // the child leave even on platforms without detached signaling.
            if matches!(mode, OpenMode::PauseAfterSpawn) {
                let reservation = self.root.join("uncommitted-session");
                let identity = launched.identity;
                request
                    .preparation
                    .run(move |rollback| {
                        fs::write(&reservation, b"pending")?;
                        *rollback = Some(Box::new(move || {
                            if identity.probe().map_err(io::Error::other)? == ProcessProbe::Running
                            {
                                return Err(io::Error::other(
                                    "The process still owns this reservation",
                                ));
                            }
                            fs::remove_file(reservation)
                        }));
                        Ok(())
                    })
                    .await?;
                let _keep_child_running = launched.stdin;
                return std::future::pending().await;
            }
            if matches!(mode, OpenMode::FailAfterSpawn) {
                return Err(open_error());
            }
            let (native_key, adapter_binding) = match request.mode {
                SessionOpenMode::Create => (
                    NativeSessionKey {
                        client_kind: self.kind().into(),
                        store_namespace: "fixture-store".into(),
                        native_session_id: request.participant_id.to_string(),
                    },
                    json!({"adapter_marker":"persisted"}),
                ),
                SessionOpenMode::Resume {
                    native_key,
                    previous_binding,
                } => (native_key, previous_binding),
            };
            let target = PersistedSessionRef {
                participant_id: request.participant_id,
                generation: request.generation,
                native_key: Some(native_key.clone()),
                process_ownership: ProcessOwnership::Managed,
                workspace_dir: request.workspace_dir,
                runtime_dir: request.runtime_dir,
                process_identity: Some(launched.identity),
                adapter_binding: adapter_binding.clone(),
            };
            let (event_sender, events) = mpsc::channel(4);
            let handle =
                SessionHandle::new((launched.stdin, launched.stdout, event_sender), target);
            Ok(OpenedSession {
                native_key,
                process_ownership: ProcessOwnership::Managed,
                handle,
                capabilities: self.capabilities(),
                initial_state: ParticipantRuntimeState::Idle,
                adapter_binding,
                events,
            })
        })
    }

    fn inspect_state<'a>(
        &'a self,
        _: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, NodeResult<SessionInspection>> {
        Box::pin(async move {
            Ok(SessionInspection {
                state: if live.is_some() {
                    ParticipantRuntimeState::Idle
                } else {
                    ParticipantRuntimeState::Stopped
                },
                detail: None,
            })
        })
    }

    fn submit_turn<'a>(
        &'a self,
        _: &'a SessionHandle,
        _: SubmitTurnRequest,
    ) -> BoxFuture<'a, SubmitOutcome> {
        Box::pin(async { SubmitOutcome::Unavailable("Fixture has no turn transport".into()) })
    }

    fn respond_approval<'a>(
        &'a self,
        _: &'a SessionHandle,
        _: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async { ApprovalRespondOutcome::Unsupported("Fixture has no approvals".into()) })
    }

    fn cancel_or_stop<'a>(
        &'a self,
        _: &'a SessionHandle,
        _: StopMode,
    ) -> BoxFuture<'a, NodeResult<StopOutcome>> {
        Box::pin(async {
            Ok(StopOutcome::Unsupported(
                "Fixture is stopped by its process owner".into(),
            ))
        })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, NodeResult<TuiAttachPlan>> {
        Box::pin(async move {
            if live.is_some() || target.native_key.is_none() {
                return Ok(TuiAttachPlan::Unsupported {
                    reason: "The fixture attaches only to a saved, stopped session".into(),
                });
            }
            Ok(TuiAttachPlan::Exec {
                program: std::env::current_exe()?,
                args: [
                    "--ignored",
                    "--exact",
                    "attach_helper",
                    "--no-capture",
                    ATTACH_ARG,
                ]
                .map(str::to_owned)
                .into(),
                cwd: Some(target.workspace_dir.clone()),
                env: Default::default(),
            })
        })
    }
}

async fn open_node(config: NodeConfig, adapter: Arc<ManagedAdapter>) -> TestResult<Node> {
    let mut registry = AdapterRegistry::default();
    registry.register(adapter)?;
    Node::open(config, registry).await
}

async fn operator(tree: &Tree) -> TestResult<CoopletClient> {
    let descriptor = Credential::read(&operator_path(&tree.state()))?;
    Ok(descriptor.connect(PrincipalKind::Operator).await?.0)
}

fn create_request() -> Value {
    json!({"client_kind":"managed-fixture","name":"managed-session","overrides":{"model":"operator-model"}})
}

fn invalid_request(result: Result<Value, ClientError>) -> TestResult {
    match result {
        Err(ClientError::Problem(problem)) => {
            assert_eq!(
                problem.code,
                ErrorCode::InvalidRequest,
                "{}",
                problem.detail
            );
            assert_eq!(problem.extensions["reason"], "native_open_failed");
            Ok(())
        }
        unexpected => {
            Err(io::Error::other(format!("Unexpected open outcome: {unexpected:?}")).into())
        }
    }
}

async fn stop_agent(client: &CoopletClient, id: &ParticipantId, key: &str) -> TestResult<Value> {
    Ok(client
        .post_json(&format!("/v1/agents/{id}/stop"), &json!({}), Some(key))
        .await?)
}

async fn wait_until_reaped(identity: &ProcessIdentity) -> TestResult {
    wait_for_process(identity, "reaping the managed worker", |probe| {
        matches!(
            probe,
            ProcessProbe::Missing | ProcessProbe::IdentityMismatch
        )
    })
    .await
}

async fn wait_for_process(
    identity: &ProcessIdentity,
    stage: &'static str,
    done: impl Fn(ProcessProbe) -> bool,
) -> TestResult {
    tokio::time::timeout(WAIT, async {
        loop {
            if done(identity.probe()?) {
                return Ok::<_, cooplet_platform::PlatformError>(());
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await
    .map_err(|error| timed_out(stage, error))??;
    Ok(())
}

fn timed_out(stage: &'static str, error: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, format!("{stage}: {error}"))
}

struct StoredSession {
    native_key: Option<NativeSessionKey>,
    process: Option<ProcessIdentity>,
    binding: AdapterBinding,
    generation: u64,
}

async fn stored_session(reads: &ReadPool, id: &ParticipantId) -> TestResult<StoredSession> {
    let id = id.clone();
    let (kind, namespace, native_id, pid, pgid, token, binding, generation) =
        reads.read(move |db| db.query_row(
            "SELECT client_kind,store_namespace,native_session_id,runtime_pid,runtime_pgid,
                    runtime_start_token,adapter_binding_json,generation FROM participants WHERE participant_id=?1",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get::<_, String>(6)?, row.get(7)?)),
        ).map_err(Into::into)).await?;
    let native_key = match (namespace, native_id) {
        (Some(store_namespace), Some(native_session_id)) => Some(NativeSessionKey {
            client_kind: kind,
            store_namespace,
            native_session_id,
        }),
        (None, None) => None,
        _ => return Err(io::Error::other("Partial native session key").into()),
    };
    let process = match (pid, pgid, token) {
        (Some(pid), Some(pgid), Some(start_token)) => Some(ProcessIdentity {
            pid,
            pgid,
            start_token,
        }),
        (None, None, None) => None,
        _ => return Err(io::Error::other("Partial process identity").into()),
    };
    Ok(StoredSession {
        native_key,
        process,
        binding: serde_json::from_str(&binding)?,
        generation,
    })
}

async fn run_attach(tree: &Tree, lookup: &str) -> TestResult {
    let log = tree.0.join("attach.log");
    let stdout = fs::File::create(&log)?;
    let mut child = ChildGuard(
        Command::new(COOPLET)
            .args(["node", "attach", lookup, "--state-dir"])
            .arg(tree.state())
            .stdout(stdout.try_clone()?)
            .stderr(stdout)
            .spawn()?,
    );
    let status = child.status("waiting for the CLI attach process").await?;
    let output = fs::read_to_string(&log)?;
    assert!(status.success(), "Attach failed: {output}");
    assert!(output.contains("attach received the literal argument"));
    Ok(())
}

#[tokio::test]
async fn managed_resume_failures_restore_the_stopped_session_after_cleanup() -> TestResult {
    let tree = Tree::new()?;
    fs::write(tree.workspace().join("attach-marker"), "saved workspace")?;
    let adapter = Arc::new(ManagedAdapter::new(&tree.0, OpenMode::Succeed));
    let node = open_node(tree.config(), adapter.clone()).await?;
    let reads = node.state.reads.clone();
    let shutdown = node.state.shutdown.clone();
    let serving = tokio::spawn(node.serve());
    let client = operator(&tree).await?;
    let created: Value = client
        .post_json("/v1/agents", &create_request(), Some("create-managed"))
        .await?;
    let id: ParticipantId = serde_json::from_value(created["participant_id"].clone())?;
    let first = read_receipt(&tree.0, 1)?;
    assert_eq!(first.process.probe()?, ProcessProbe::Running);
    assert_eq!(
        stop_agent(&client, &id, "stop-managed").await?["state"],
        "stopped"
    );
    wait_until_reaped(&first.process).await?;

    let original = stored_session(&reads, &id).await?;
    assert_eq!(
        original
            .native_key
            .as_ref()
            .map(|key| key.native_session_id.as_str()),
        Some(id.as_str())
    );
    assert_eq!(
        original.binding.adapter,
        json!({"adapter_marker":"persisted"})
    );
    assert_eq!(
        original
            .binding
            .core
            .overrides
            .as_ref()
            .and_then(|overrides| overrides.model.as_deref()),
        Some("operator-model"),
    );
    assert!(original.process.is_none());
    run_attach(&tree, "managed-session").await?;
    run_attach(&tree, id.as_str()).await?;

    let path = format!("/v1/agents/{id}/resume");
    for (mode, generation, key) in [
        (OpenMode::FailBeforeSpawn, 2, "resume-fails-before"),
        (OpenMode::FailAfterSpawn, 3, "resume-fails-after"),
    ] {
        adapter.set_mode(mode)?;
        invalid_request(client.post_json(&path, &json!({}), Some(key)).await)?;
        let view: ParticipantView = client
            .get_json(&format!("/v1/participants/{id}"), &[])
            .await?;
        let restored = stored_session(&reads, &id).await?;
        assert_eq!(view.state, ParticipantState::Stopped);
        assert_eq!(restored.native_key, original.native_key);
        assert_eq!(restored.binding.adapter, original.binding.adapter);
        assert_eq!(
            restored.binding.core.overrides,
            original.binding.core.overrides
        );
        assert!(restored.binding.core.lifecycle_block.is_none());
        assert!(restored.process.is_none());
        assert_eq!(restored.generation, generation);
        let rejected_credential = tree
            .state()
            .join("creds")
            .join(format!("{id}.g{generation}.json"));
        assert!(!rejected_credential.exists());
    }
    assert!(!receipt_path(&tree.0, 2).exists());
    wait_until_reaped(&read_receipt(&tree.0, 3)?.process).await?;

    adapter.set_mode(OpenMode::Succeed)?;
    let resumed: Value = client
        .post_json(&path, &json!({}), Some("resume-managed"))
        .await?;
    assert_eq!(resumed["participant_id"], id.as_str());
    let live = stored_session(&reads, &id).await?;
    assert_eq!(live.native_key, original.native_key);
    assert_eq!(live.generation, 4);
    assert_eq!(
        live.process.as_ref(),
        Some(&read_receipt(&tree.0, 4)?.process)
    );
    shutdown.send_replace(true);
    tokio::time::timeout(WAIT, serving)
        .await
        .map_err(|error| timed_out("shutting down the resumed Node", error))???;
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_pending_managed_open_and_reaps_the_process() -> TestResult {
    let tree = Tree::new()?;
    let adapter = Arc::new(ManagedAdapter::new(&tree.0, OpenMode::PauseAfterSpawn));
    let node = open_node(tree.config(), adapter).await?;
    let serving = tokio::spawn(node.serve());
    let client = operator(&tree).await?;
    let pending = {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .post_json::<_, Value>(
                    "/v1/agents",
                    &create_request(),
                    Some("opening-during-shutdown"),
                )
                .await
        })
    };
    tokio::time::timeout(WAIT, async {
        while !tree.0.join("uncommitted-session").is_file() {
            tokio::time::sleep(POLL).await;
        }
    })
    .await?;
    let spawned = read_receipt(&tree.0, 1)?;
    assert_eq!(spawned.process.probe()?, ProcessProbe::Running);
    let duplicate_open = client
        .post_json::<_, Value>(
            "/v1/agents",
            &create_request(),
            Some("opening-during-shutdown"),
        )
        .await;
    assert!(matches!(duplicate_open, Err(ClientError::Problem(problem))
        if problem.code == ErrorCode::OperationInProgress
            && problem.extensions.get("operation_id").and_then(Value::as_str).is_some()));
    let stop_open = client
        .post_json::<_, Value>(
            &format!("/v1/agents/{}/stop", spawned.participant_id),
            &json!({}),
            Some("stop-active-open"),
        )
        .await;
    assert!(matches!(stop_open, Err(ClientError::Problem(problem))
        if problem.code == ErrorCode::ParticipantBusy && problem.extensions["reason"] == "operation_in_flight"));
    assert_eq!(spawned.process.probe()?, ProcessProbe::Running);

    let stop: Value = client
        .post_json("/v1/node/stop", &json!({}), Some("shutdown-during-open"))
        .await?;
    assert_eq!(stop["stopping"], true);
    tokio::time::timeout(WAIT, serving)
        .await
        .map_err(|error| timed_out("shutting down during managed open", error))???;
    let interrupted = tokio::time::timeout(WAIT, pending).await??;
    assert!(
        matches!(interrupted, Err(ClientError::Problem(problem)) if problem.code == ErrorCode::Overloaded)
    );
    assert!(!spawned.credential.try_exists()?);
    assert!(!tree.0.join("uncommitted-session").try_exists()?);
    wait_until_reaped(&spawned.process).await?;
    let restarted = open_node(
        tree.config(),
        Arc::new(ManagedAdapter::new(&tree.0, OpenMode::Succeed)),
    )
    .await?;
    restarted.state.shutdown.send_replace(true);
    tokio::time::timeout(WAIT, restarted.serve()).await??;
    Ok(())
}

#[tokio::test]
async fn sigkill_after_managed_spawn_preserves_identity_and_never_replays_open() -> TestResult {
    let tree = Tree::new()?;
    let mut daemon = ChildGuard(
        Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "opening_daemon", "--no-capture"])
            .env(ROOT_ENV, &tree.0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()?,
    );
    let wait_for = |path: PathBuf, stage| async move {
        tokio::time::timeout(WAIT, async {
            while !path.is_file() {
                tokio::time::sleep(POLL).await;
            }
        })
        .await
        .map_err(|error| timed_out(stage, error))
    };
    wait_for(tree.0.join("daemon-ready"), "starting the fixture daemon").await?;
    let client = operator(&tree).await?;
    let pending = tokio::spawn(async move {
        client
            .post_json::<_, Value>("/v1/agents", &create_request(), Some("crashed-create"))
            .await
    });
    wait_for(
        receipt_path(&tree.0, 1),
        "waiting for durable managed spawn",
    )
    .await?;
    let observed = read_receipt(&tree.0, 1)?;
    let descriptor = Credential::read(&observed.credential)?;
    assert_eq!(observed.process.probe()?, ProcessProbe::Running);
    let agent = descriptor.connect(PrincipalKind::Participant).await?.0;
    assert_eq!(
        agent.whoami().await?.participant_id.as_ref(),
        Some(&observed.participant_id)
    );

    daemon.0.kill()?;
    assert_eq!(
        daemon.status("waiting for the killed Node").await?.signal(),
        Some(9)
    );
    assert!(
        tokio::time::timeout(WAIT, pending)
            .await
            .map_err(|error| timed_out("waiting for the interrupted create request", error))??
            .is_err()
    );
    wait_for_process(
        &observed.process,
        "waiting for worker exit after SIGKILL",
        |probe| probe != ProcessProbe::Running,
    )
    .await?;

    let node = open_node(
        tree.config(),
        Arc::new(ManagedAdapter::new(&tree.0, OpenMode::Succeed)),
    )
    .await?;
    let reads = node.state.reads.clone();
    let shutdown = node.state.shutdown.clone();
    let serving = tokio::spawn(node.serve());
    let client = operator(&tree).await?;
    let id = observed.participant_id;
    let view: ParticipantView = client
        .get_json(&format!("/v1/participants/{id}"), &[])
        .await?;
    let recovered = stored_session(&reads, &id).await?;
    assert_eq!(view.state, ParticipantState::Unknown);
    assert_eq!(recovered.process.as_ref(), Some(&observed.process));
    assert_eq!(recovered.generation, 1);
    assert!(recovered.native_key.is_none());
    assert_eq!(
        recovered
            .binding
            .core
            .lifecycle_block
            .as_ref()
            .map(|block| block.kind),
        Some(LifecycleBlockKind::SpawnUncertain),
    );
    assert!(
        descriptor
            .connect(PrincipalKind::Participant)
            .await
            .is_err_and(|error| error.code() == ErrorCode::Unauthorized)
    );
    let replay: Result<Value, _> = client
        .post_json("/v1/agents", &create_request(), Some("crashed-create"))
        .await;
    assert!(
        matches!(replay, Err(ClientError::Problem(problem)) if problem.code == ErrorCode::DeliveryUnknown)
    );
    assert_eq!(read_receipt(&tree.0, 1)?.process, observed.process);
    assert!(!receipt_path(&tree.0, 2).exists());

    let mut stopped = stop_agent(&client, &id, "stop-recovered").await?;
    if stopped["stop_outcome"] == "cleanup_unverified" {
        // Reaping an orphan belongs to its new parent. The fixture worker
        // launches no child groups, and its recorded identity has exited.
        assert_ne!(observed.process.probe()?, ProcessProbe::Running);
        assert_eq!(stopped["state"], "unknown");
        let unverified = stored_session(&reads, &id).await?;
        assert_eq!(unverified.process.as_ref(), Some(&observed.process));
        assert_eq!(
            unverified
                .binding
                .core
                .lifecycle_block
                .as_ref()
                .map(|block| block.kind),
            Some(LifecycleBlockKind::CleanupUnverified)
        );
        stopped = client
            .post_json(
                &format!("/v1/agents/{id}/stop"),
                &json!({"acknowledge_manual_cleanup": true}),
                Some("acknowledge-recovered"),
            )
            .await?;
        assert_eq!(stopped["stop_outcome"], "acknowledged_manual_cleanup");
    }
    assert_eq!(stopped["state"], "stopped");
    assert!(stored_session(&reads, &id).await?.process.is_none());
    shutdown.send_replace(true);
    tokio::time::timeout(WAIT, serving)
        .await
        .map_err(|error| timed_out("shutting down the recovered Node", error))???;
    Ok(())
}

#[tokio::test]
#[ignore = "spawned by the managed-open SIGKILL test"]
async fn opening_daemon() -> TestResult {
    let root = std::env::var_os(ROOT_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("Missing managed fixture root"))?;
    let config = NodeConfig::new(root.join("state"), root.join("workspace"));
    let node = open_node(
        config,
        Arc::new(ManagedAdapter::new(&root, OpenMode::PauseAfterSpawn)),
    )
    .await?;
    fs::write(root.join("daemon-ready"), [])?;
    node.serve().await
}

#[test]
#[ignore = "spawned by the managed adapter fixture"]
fn managed_worker() -> TestResult {
    let (done, eof) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = done.send(io::copy(&mut io::stdin().lock(), &mut io::sink()));
    });
    eof.recv_timeout(WAIT.saturating_mul(2))??;
    Ok(())
}

#[test]
#[ignore = "spawned by the stopped attach test"]
fn attach_helper() -> TestResult {
    assert!(std::env::args().any(|argument| argument == ATTACH_ARG));
    assert_eq!(fs::read_to_string("attach-marker")?, "saved workspace");
    println!("attach received the literal argument");
    Ok(())
}

#[tokio::test]
async fn failed_shutdown_reports_the_error_reaps_the_child_and_revokes_its_credential() -> TestResult
{
    let tree = Tree::new()?;
    let adapter = Arc::new(ManagedAdapter::new(&tree.0, OpenMode::Succeed));
    let node = open_node(tree.config(), adapter).await?;
    let shutdown = node.state.shutdown.clone();
    let writer = node.state.db.clone();
    let server = tokio::spawn(node.serve());
    let client = operator(&tree).await?;
    let _: Value = client
        .post_json("/v1/agents", &create_request(), Some("shutdown-fault"))
        .await?;
    let receipt = read_receipt(&tree.0, 1)?;
    assert_eq!(receipt.process.probe()?, ProcessProbe::Running);
    writer
        .write_sqlite(|tx| {
            tx.execute_batch(
                "CREATE TEMP TRIGGER fail_shutdown BEFORE UPDATE OF state ON participants
            WHEN NEW.state = 'stopping' BEGIN SELECT RAISE(ABORT, 'shutdown write refused'); END",
            )?;
            Ok(cooplet_storage::WriteOutcome::new(()))
        })
        .await?;
    shutdown.send_replace(true);
    assert!(tokio::time::timeout(WAIT, server).await??.is_err());
    wait_until_reaped(&receipt.process).await?;
    assert!(!receipt.credential.try_exists()?);

    let recovered = open_node(
        tree.config(),
        Arc::new(ManagedAdapter::new(&tree.0, OpenMode::Succeed)),
    )
    .await?;
    let active: i64 = recovered.state.reads.read(move |conn| {
        Ok(conn.query_row("SELECT count(*) FROM participant_credentials WHERE participant_id=?1 AND revoked_at_ms IS NULL",
            [receipt.participant_id.as_str()], |row| row.get(0))?)
    }).await?;
    assert_eq!(active, 0);
    recovered.state.shutdown.send_replace(true);
    recovered.serve().await
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn pidfd_exit_monitor_marks_unknown_when_grandchild_holds_stdio_and_stop_cleans_group()
-> TestResult {
    let tree = Tree::new()?;
    let adapter = Arc::new(ManagedAdapter::new(
        &tree.0,
        OpenMode::ExitLeaderWithGrandchild,
    ));
    let node = open_node(tree.config(), adapter).await?;
    let agents = node.state.agents.clone();
    let shutdown = node.state.shutdown.clone();
    let serving = tokio::spawn(node.serve());
    let client = operator(&tree).await?;

    let created: Value = client
        .post_json("/v1/agents", &create_request(), Some("create-grandchild"))
        .await?;
    let id: ParticipantId = serde_json::from_value(created["participant_id"].clone())?;
    let receipt = read_receipt(&tree.0, 1)?;
    let runtime_dir = tree.state().join("runtime").join(format!("{id}.g1"));
    assert!(runtime_dir.is_dir());

    // The shell leader exits immediately while its `sleep 60` grandchild keeps stdout open
    // and the adapter event channel stays open. The Linux pidfd ExitMonitor must detect
    // leader exit, mark the participant unknown, and evict the dead live session without
    // reaping `Child` yet.
    tokio::time::timeout(WAIT, async {
        loop {
            let view: ParticipantView = client
                .get_json(&format!("/v1/participants/{id}"), &[])
                .await?;
            if view.state == ParticipantState::Unknown {
                return Ok::<(), Box<dyn Error + Send + Sync>>(());
            }
            tokio::time::sleep(POLL).await;
        }
    })
    .await??;
    assert!(agents.live_session(&id).is_none());
    assert_eq!(receipt.process.probe()?, ProcessProbe::Exited);

    // inspect_state works on a non-live session and returns Stopped for our fixture.
    let inspected = agents.inspect_state(id.clone()).await?;
    assert_eq!(inspected.state, ParticipantRuntimeState::Stopped);

    // Explicit stop terminates the surviving grandchild process group, reaps the leader,
    // and cleans up the per-generation runtime directory.
    let stopped = stop_agent(&client, &id, "stop-grandchild").await?;
    assert_eq!(stopped["state"], "stopped");
    assert_eq!(stopped["stop_outcome"], "stopped");
    wait_until_reaped(&receipt.process).await?;
    assert!(!runtime_dir.exists());

    shutdown.send_replace(true);
    tokio::time::timeout(WAIT, serving)
        .await
        .map_err(|error| timed_out("shutting down after pidfd test", error))???;
    Ok(())
}
