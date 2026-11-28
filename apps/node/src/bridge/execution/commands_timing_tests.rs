use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc as blocking,
    },
    time::Duration,
};

use axum::{Router, routing::get};
use cooplet_client::{ClientConfig, CoopletClient, Destination};
use cooplet_platform::{AdmissionGuard, MonoClock, MonoInstant, PrivateFs};
use cooplet_protocol::{
    ApprovalAction, ApprovalDecision, ApprovalWait, ApprovalWaitKind, AttachmentRef, AuthorKind,
    BlobId, BridgeCommand, BridgeFrame, CommandOutcome, CommandReqPayload, CommandResPayload,
    DeliveryMode, EventKind, HubId, MessageId, NodeId, OperationId, ParticipantId,
};
use cooplet_storage::StorageEngine;
use sha2::{Digest, Sha256};
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

use super::{command, mock_adapter::MockAdapter, receipts};
use crate::{
    clients::{
        AdapterEventKind, AdapterRegistry, AgentConfig, AgentService, ApprovalRespondOutcome,
        CreateAgentRequest, SubmitOutcome,
    },
    config::{NodeConfig, OpenNode},
    delivery::DeliveryService,
    http::NodeState,
    operations::OperationBook,
};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(5);
const TEXT_BUDGET: u64 = 5_000;
const SLOW_BUDGET: u64 = 30_000;

#[derive(Default)]
struct Clock(AtomicU64);

impl MonoClock for Clock {
    fn now(&self) -> MonoInstant {
        MonoInstant::from_millis(self.0.load(Ordering::SeqCst))
    }
}

struct TestDir(PathBuf);

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

type WriterPause = (oneshot::Sender<()>, blocking::Receiver<()>);

struct Fixture {
    _dir: TestDir,
    storage: StorageEngine,
    state: NodeState,
    adapter: MockAdapter,
    clock: Arc<Clock>,
    writer_pause: Arc<Mutex<Option<WriterPause>>>,
    hub: HubId,
    node: NodeId,
    sender: ParticipantId,
}

struct PendingCommand {
    id: OperationId,
    guard: AdmissionGuard,
    result: JoinHandle<CommandResPayload>,
}

impl PendingCommand {
    async fn finish(self) -> TestResult<CommandResPayload> {
        Ok(timeout(WAIT, self.result).await??)
    }
}

impl Fixture {
    async fn start() -> TestResult<Self> {
        let dir = TestDir(PrivateFs::ensure_private_dir(
            &std::env::temp_dir().join(format!("cooplet-admission-{}", OperationId::generate()?)),
        )?);
        let config = NodeConfig::new(dir.0.join("node"), dir.0.clone());
        let max_blob_bytes = config.max_blob_bytes;
        let open = OpenNode::start(config).await?;
        let db = open.engine.db().clone();
        let reads = open.engine.reads().clone();
        let blobs = open.engine.blobs().ok_or("missing blob store")?.clone();
        let operations = OperationBook::new(db.clone(), reads.clone());
        let adapter = MockAdapter::default();
        let mut registry = AdapterRegistry::default();
        registry.register(Arc::new(adapter.clone()))?;
        let agents = Arc::new(AgentService::new(
            db.clone(),
            reads.clone(),
            open.auth.clone(),
            registry,
            operations.clone(),
            AgentConfig {
                node_id: open.info.node_id.clone(),
                endpoint: open.operator.address()?,
                state_dir: open.state_dir.clone(),
                workspace_dir: open.workspace_dir,
            },
        ));
        let writer_pause = Arc::new(Mutex::<Option<WriterPause>>::new(None));
        let pause = writer_pause.clone();
        let delivery = DeliveryService::new_with_clock(
            db.clone(),
            reads.clone(),
            blobs.clone(),
            open.state_dir.join("blobs"),
            agents.clone(),
            operations.clone(),
            Arc::new(move || {
                let pause = pause
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some((entered, release)) = pause {
                    entered
                        .send(())
                        .map_err(|_| io::Error::other("writer observer dropped"))?;
                    release.recv_timeout(WAIT).map_err(io::Error::other)?;
                }
                crate::now_ms()
            }),
        )?;
        let state = NodeState {
            info: open.info,
            db,
            reads,
            blobs,
            max_blob_bytes,
            auth: open.auth,
            operations,
            agents,
            delivery,
            bridge: Arc::default(),
            shutdown: watch::channel(false).0,
            tasks: Arc::default(),
        };
        Ok(Self {
            _dir: dir,
            storage: open.engine,
            state,
            adapter,
            clock: Arc::default(),
            writer_pause,
            hub: HubId::generate()?,
            node: NodeId::generate()?,
            sender: ParticipantId::generate()?,
        })
    }

    async fn agent(&self) -> TestResult<ParticipantId> {
        Ok(self
            .state
            .agents
            .create(
                self.sender.as_str(),
                "create-receiver",
                CreateAgentRequest {
                    client_kind: Some("mock".into()),
                    name: Some("receiver".into()),
                    ..CreateAgentRequest::default()
                },
                None,
            )
            .await?
            .participant_id)
    }

    fn hold_writer(&self) -> (oneshot::Receiver<()>, blocking::SyncSender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, hold) = blocking::sync_channel(1);
        *self
            .writer_pause
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((entered, hold));
        (observed, release)
    }

    fn at(&self, millis: u64) {
        self.clock.0.store(millis, Ordering::SeqCst);
    }

    fn send(
        &self,
        receiver: ParticipantId,
        budget: u64,
        attachments: Vec<AttachmentRef>,
        client: CoopletClient,
    ) -> TestResult<PendingCommand> {
        self.start_command(
            BridgeCommand::SendMessage {
                message_id: MessageId::generate()?,
                sender_kind: AuthorKind::Human,
                sender_name: "operator".into(),
                sender_node_id: None,
                recipient_id: receiver,
                in_reply_to: None,
                body: "Message delivered through the Hub".into(),
                attachments,
                question: None,
                answer: None,
            },
            budget,
            client,
        )
    }

    fn start_command(
        &self,
        operation: BridgeCommand,
        budget: u64,
        client: CoopletClient,
    ) -> TestResult<PendingCommand> {
        let id = OperationId::generate()?;
        let request = CommandReqPayload {
            op_id: id.clone(),
            actor_id: self.sender.to_string(),
            command: operation,
            delivery_mode: DeliveryMode::DirectNow,
            admission_budget_ms: budget,
            ttl_ms: None,
        };
        let frame = BridgeFrame::encode_json(1, &request)?;
        let request: CommandReqPayload = BridgeFrame::from_json(&frame)?.decode_payload()?;
        let guard = AdmissionGuard::with_clock(
            Duration::from_millis(request.admission_budget_ms),
            self.clock.clone(),
        );
        let admission = guard.clone();
        let state = self.state.clone();
        let hub = self.hub.clone();
        let node = self.node.clone();
        let result =
            tokio::spawn(
                async move { command(&state, &hub, &node, &client, request, admission).await },
            );
        Ok(PendingCommand { id, guard, result })
    }

    async fn operation_state(&self, id: &OperationId) -> TestResult<Option<String>> {
        use rusqlite::OptionalExtension;
        let id = id.clone();
        Ok(self
            .state
            .reads
            .read(move |db| {
                Ok(db
                    .query_row(
                        "SELECT state FROM operations WHERE operation_id = ?1",
                        [id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?)
            })
            .await?)
    }

    async fn roster_receipt(&self, id: OperationId) -> TestResult<CommandResPayload> {
        let hub = self.hub.clone();
        let node = self.node.clone();
        let now = crate::now_ms()?;
        self.state
            .reads
            .read(move |db| Ok(receipts(db, &hub, &node, &[id], now)?.pop()))
            .await?
            .ok_or_else(|| "missing persisted terminal receipt".into())
    }

    async fn stop(self) -> TestResult {
        self.state.agents.shutdown().await?;
        self.state.delivery.shutdown().await;
        self.storage.shutdown().await?;
        Ok(())
    }
}

fn relay_client(origin: &str) -> TestResult<CoopletClient> {
    Ok(CoopletClient::new(ClientConfig::new(
        origin.parse()?,
        Destination::LocalNode,
        None,
    ))?)
}

#[tokio::test]
async fn text_command_expires_while_in_the_sqlite_writer_before_adapter_input() -> TestResult {
    let fixture = Fixture::start().await?;
    let receiver = fixture.agent().await?;
    let (entered, release) = fixture.hold_writer();
    let pending = fixture.send(
        receiver,
        TEXT_BUDGET,
        Vec::new(),
        relay_client("http://127.0.0.1:1")?,
    )?;
    timeout(WAIT, entered).await??;
    fixture.at(TEXT_BUDGET + 1);
    release.send(())?;
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Expired);
    assert_eq!(fixture.adapter.submit_calls(), 0);
    assert_eq!(fixture.operation_state(&id).await?, None);
    fixture.stop().await
}

#[tokio::test]
async fn attachment_command_keeps_thirty_seconds_and_cannot_admit_after_download_delay()
-> TestResult {
    let fixture = Fixture::start().await?;
    let receiver = fixture.agent().await?;
    let body = b"attachment awaiting delivery";
    let declared = AttachmentRef {
        blob_id: BlobId::generate()?,
        name: "delivery.txt".into(),
        mime_type: "text/plain".into(),
        size_bytes: body.len() as u64,
        sha256: format!("{:x}", Sha256::digest(body)),
    };
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_entered = entered.clone();
    let handler_release = release.clone();
    let etag = format!("\"{}\"", declared.sha256);
    let app = Router::new().route(
        "/v1/blobs/{id}",
        get(move || {
            let entered = handler_entered.clone();
            let release = handler_release.clone();
            let etag = etag.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                (
                    [("content-type", "text/plain".to_owned()), ("etag", etag)],
                    body.as_slice(),
                )
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let client = relay_client(&format!("http://{}", listener.local_addr()?))?;
    let server = tokio::spawn(async move { axum::serve(listener, app).await });
    let pending = fixture.send(receiver, SLOW_BUDGET, vec![declared], client)?;
    timeout(WAIT, entered.notified()).await?;
    fixture.at(TEXT_BUDGET + 1);
    assert_eq!(
        pending.guard.remaining_ms(),
        Some(SLOW_BUDGET - TEXT_BUDGET - 1)
    );
    fixture.at(SLOW_BUDGET + 1);
    release.notify_one();
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Expired);
    assert_eq!(fixture.adapter.submit_calls(), 0);
    assert_eq!(fixture.operation_state(&id).await?, None);
    server.abort();
    fixture.stop().await
}

#[tokio::test]
async fn slow_create_expiry_after_discovery_does_not_open_a_native_session() -> TestResult {
    let fixture = Fixture::start().await?;
    let (entered, release) = fixture.adapter.hold_discovery();
    let pending = fixture.start_command(
        BridgeCommand::CreateAgent {
            client_kind: Some("mock".into()),
            template_id: None,
            name: Some("late-agent".into()),
            workspace_dir: None,
            overrides: None,
        },
        SLOW_BUDGET,
        relay_client("http://127.0.0.1:1")?,
    )?;
    timeout(WAIT, entered).await??;
    fixture.at(SLOW_BUDGET + 1);
    release.send(()).map_err(|_| "discovery barrier closed")?;
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Expired);
    assert_eq!(fixture.adapter.open_calls(), 0);
    assert_eq!(fixture.operation_state(&id).await?, None);
    fixture.stop().await
}

#[tokio::test]
async fn managed_launch_expiry_survives_the_adapter_error_boundary_and_rollback() -> TestResult {
    let fixture = Fixture::start().await?;
    let path = fixture._dir.0.join("managed-started");
    let (entered, release) = fixture.adapter.hold_managed_launch(path.clone());
    let pending = fixture.start_command(
        BridgeCommand::CreateAgent {
            client_kind: Some("mock".into()),
            template_id: None,
            name: Some("managed-receiver".into()),
            workspace_dir: None,
            overrides: None,
        },
        SLOW_BUDGET,
        relay_client("http://127.0.0.1:1")?,
    )?;
    timeout(WAIT, entered).await??;
    assert_eq!(fixture.adapter.open_calls(), 1);
    assert_eq!(
        fixture.operation_state(&pending.id).await?.as_deref(),
        Some("in_progress")
    );
    fixture.at(SLOW_BUDGET + 1);
    release
        .send(())
        .map_err(|_| "managed launch barrier closed")?;
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Expired);
    assert!(!path.exists());
    assert_eq!(
        fixture.operation_state(&id).await?.as_deref(),
        Some("failed")
    );
    assert_eq!(
        fixture.roster_receipt(id).await?.outcome,
        CommandOutcome::Expired
    );
    let rolled_back = fixture
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT state = 'stopped' AND runtime_pid IS NULL
             FROM participants WHERE name = 'managed-receiver'",
                [],
                |row| row.get::<_, bool>(0),
            )?)
        })
        .await?;
    assert!(rolled_back);
    fixture.stop().await
}

#[tokio::test]
async fn timely_native_submit_accepts_and_persists_its_late_ack() -> TestResult {
    let fixture = Fixture::start().await?;
    let receiver = fixture.agent().await?;
    let (entered, release) = fixture.hold_writer();
    let pending = fixture.send(
        receiver,
        TEXT_BUDGET,
        Vec::new(),
        relay_client("http://127.0.0.1:1")?,
    )?;
    timeout(WAIT, entered).await??;
    fixture.at(TEXT_BUDGET - 1);
    release.send(())?;
    let native = fixture.adapter.next_submit().await?;
    let ack = native.admit()?;
    assert_eq!(fixture.adapter.submit_calls(), 1);
    assert_eq!(fixture.adapter.submits(), 1);
    assert_eq!(
        fixture.operation_state(&pending.id).await?.as_deref(),
        Some("in_progress")
    );

    fixture.at(6_000);
    assert!(pending.guard.is_expired());
    ack.send(SubmitOutcome::Accepted {
        native_turn_id: None,
    })
    .map_err(|_| "native ACK receiver closed")?;
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Delivered);
    let receipt = fixture.roster_receipt(id).await?;
    assert_eq!(receipt.outcome, CommandOutcome::Delivered);
    fixture.stop().await
}

#[tokio::test]
async fn delegated_approval_persists_its_ack_after_admission_expiry() -> TestResult {
    let fixture = Fixture::start().await?;
    let receiver = fixture.agent().await?;
    let session = fixture
        .adapter
        .session(&receiver, 1)
        .ok_or("missing mock session")?;
    let mut events = fixture.state.db.subscribe();
    let native_id = "native-approval";
    session
        .emit(AdapterEventKind::ApprovalRequested(ApprovalWait {
            approval_id: Some(native_id.into()),
            kind: ApprovalWaitKind::Permission,
            summary: None,
            can_respond_remotely: true,
            action_hint: None,
        }))
        .await?;
    timeout(WAIT, async {
        while events.recv().await?.kind != EventKind::ParticipantAwaitingApproval {}
        Ok::<_, tokio::sync::broadcast::error::RecvError>(())
    })
    .await??;
    let agent = receiver.clone();
    let approval_id: String = fixture
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT json_extract(adapter_binding_json, '$.core.pending_approval.public_id')
             FROM participants WHERE participant_id = ?1",
                [agent.as_str()],
                |row| row.get(0),
            )?)
        })
        .await?;
    let pending = fixture.start_command(
        BridgeCommand::RespondApproval {
            participant_id: receiver,
            decision: ApprovalDecision {
                approval_id: approval_id.clone(),
                action: ApprovalAction::Approve,
            },
        },
        TEXT_BUDGET,
        relay_client("http://127.0.0.1:1")?,
    )?;
    fixture.at(TEXT_BUDGET - 1);
    let native = fixture.adapter.next_approval().await?;
    assert_eq!(native.decision.approval_id, native_id);
    assert_eq!(
        fixture.operation_state(&pending.id).await?.as_deref(),
        Some("in_progress")
    );
    fixture.at(6_000);
    assert!(pending.guard.is_expired());
    native.finish(ApprovalRespondOutcome::Accepted)?;
    let id = pending.id.clone();
    let response = pending.finish().await?;
    assert_eq!(response.op_id, id);
    assert_eq!(response.outcome, CommandOutcome::Succeeded);
    let receipt = fixture.roster_receipt(id).await?;
    assert_eq!(receipt.outcome, CommandOutcome::Succeeded);
    assert_eq!(
        receipt.result.ok_or("missing approval view")?["approval_id"],
        approval_id.as_str()
    );
    fixture.stop().await
}

#[tokio::test]
async fn delegated_direct_dm_with_attachments_preserves_busy_and_awaiting_approval_without_blob_gets()
-> TestResult {
    use std::sync::atomic::AtomicUsize;

    use cooplet_protocol::ErrorCode;

    let fixture = Fixture::start().await?;
    let receiver = fixture.agent().await?;
    let session = fixture
        .adapter
        .session(&receiver, 1)
        .ok_or("missing mock session")?;

    let contents = b"should not be fetched on preflight denial";
    let attachment = AttachmentRef {
        blob_id: BlobId::generate()?,
        name: "note.txt".into(),
        mime_type: "text/plain".into(),
        size_bytes: contents.len() as u64,
        sha256: format!("{:x}", Sha256::digest(contents)),
    };
    let gets = Arc::new(AtomicUsize::new(0));
    let counter = gets.clone();
    let app = Router::new().route(
        "/v1/blobs/{id}",
        get(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                contents.as_slice()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let blob_origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, app).await });

    // 1. Move the live agent to `Working` and verify DirectNow + attachment returns
    // Busy (409 participant_busy) with 0 blob GETs.
    let mut events = fixture.state.db.subscribe();
    session
        .emit(AdapterEventKind::StateChanged(
            crate::clients::ParticipantRuntimeState::Working,
        ))
        .await?;
    timeout(WAIT, async {
        while events.recv().await?.kind != EventKind::ParticipantStateChanged {}
        Ok::<_, tokio::sync::broadcast::error::RecvError>(())
    })
    .await??;

    let busy_cmd = fixture.start_command(
        BridgeCommand::SendMessage {
            message_id: MessageId::generate()?,
            sender_kind: AuthorKind::Human,
            sender_name: "admin".into(),
            sender_node_id: None,
            recipient_id: receiver.clone(),
            in_reply_to: None,
            body: "direct while working".into(),
            attachments: vec![attachment.clone()],
            question: None,
            answer: None,
        },
        SLOW_BUDGET,
        relay_client(&blob_origin)?,
    )?;
    let busy_res = busy_cmd.finish().await?;
    assert_eq!(busy_res.outcome, CommandOutcome::Busy);
    assert_eq!(
        busy_res.error.as_ref().map(|p| p.code),
        Some(ErrorCode::ParticipantBusy)
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);

    // 2. Move the live agent to `AwaitingApproval` and verify DirectNow + attachment returns
    // AwaitingApproval (with approval_id metadata) and 0 blob GETs.
    let mut events = fixture.state.db.subscribe();
    session
        .emit(AdapterEventKind::ApprovalRequested(ApprovalWait {
            approval_id: Some("native-apr-direct".into()),
            kind: ApprovalWaitKind::Permission,
            summary: Some("Approve command".into()),
            can_respond_remotely: true,
            action_hint: None,
        }))
        .await?;
    timeout(WAIT, async {
        while events.recv().await?.kind != EventKind::ParticipantAwaitingApproval {}
        Ok::<_, tokio::sync::broadcast::error::RecvError>(())
    })
    .await??;
    let agent = receiver.clone();
    let public_approval_id: String = fixture
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT json_extract(adapter_binding_json, '$.core.pending_approval.public_id')
                 FROM participants WHERE participant_id = ?1",
                [agent.as_str()],
                |row| row.get(0),
            )?)
        })
        .await?;

    let apr_cmd = fixture.start_command(
        BridgeCommand::SendMessage {
            message_id: MessageId::generate()?,
            sender_kind: AuthorKind::Human,
            sender_name: "admin".into(),
            sender_node_id: None,
            recipient_id: receiver,
            in_reply_to: None,
            body: "direct while awaiting approval".into(),
            attachments: vec![attachment],
            question: None,
            answer: None,
        },
        SLOW_BUDGET,
        relay_client(&blob_origin)?,
    )?;
    let apr_res = apr_cmd.finish().await?;
    assert_eq!(apr_res.outcome, CommandOutcome::AwaitingApproval);
    let problem = apr_res.error.ok_or("missing awaiting_approval problem")?;
    assert_eq!(problem.code, ErrorCode::AwaitingApproval);
    assert_eq!(
        problem.extensions.get("approval_id"),
        Some(&serde_json::json!(public_approval_id))
    );
    assert_eq!(gets.load(Ordering::SeqCst), 0);

    server.abort();
    fixture.stop().await
}
