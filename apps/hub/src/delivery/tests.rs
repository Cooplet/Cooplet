use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use cooplet_protocol::{BlobId, CommandOutcome, CommandResPayload, ProblemDetails, TokenId};
use cooplet_storage::StorageEngine;
use futures_util::future::BoxFuture;
use serde_json::json;
use tokio::sync::{Notify, watch};

use crate::{
    auth::authenticate,
    bridge::{BridgeRegistry, OfflineBridge},
    credentials::mint_bearer,
    storage,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TestResult<Self> {
        let id = OperationId::generate()?;
        Ok(Self(
            std::env::temp_dir().join(format!("cooplet-delivery-{id}")),
        ))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    engine: StorageEngine,
    dir: TempDir,
    state: AppState,
    admin: AuthenticatedCaller,
    _shutdown: watch::Sender<()>,
}

impl Fixture {
    async fn new(bridge: Arc<dyn BridgeRegistry>) -> TestResult<Self> {
        let dir = TempDir::new()?;
        storage::init(&dir.0, "Delivery test Hub").await?;
        Self::open(dir, bridge).await
    }

    async fn open(dir: TempDir, bridge: Arc<dyn BridgeRegistry>) -> TestResult<Self> {
        let hub = storage::open(&dir.0, 1024 * 1024, 8 * 1024 * 1024, 20_000).await?;
        let (shutdown, shutdown_rx) = watch::channel(());
        let state = AppState::new(
            &hub.engine,
            hub.meta,
            hub.master,
            bridge,
            shutdown_rx,
            1024 * 1024,
        )?;
        let token =
            cooplet_platform::PrivateFs::read_private_file(&dir.0.join("admin.token"), 128)?;
        let admin = authenticate(
            &state.reads,
            &state.master,
            std::str::from_utf8(&token)?.trim(),
        )
        .await?;
        Ok(Self {
            engine: hub.engine,
            dir,
            state,
            admin,
            _shutdown: shutdown,
        })
    }

    async fn close(self) -> TestResult<TempDir> {
        self.state.background.close();
        self.state.background.drain().await;
        self.engine.shutdown().await?;
        Ok(self.dir)
    }

    async fn node(&self, id: &NodeId, participant: &ParticipantId, kind: &str) -> TestResult {
        let owner = self.admin.actor_id().to_owned();
        let node = id.clone();
        let participant = participant.clone();
        let kind = kind.to_owned();
        self.state.db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac, status, enrolled_at_ms)
                 VALUES (?1, 'Test node', ?2, ?1, 'offline', ?3)",
                rusqlite::params![node.as_str(), owner, now_ms()],
            )?;
            tx.execute(
                "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                 VALUES (?1, ?2, ?3, 'Participant', 'idle', 0, ?4)",
                rusqlite::params![node.as_str(), participant.as_str(), kind, now_ms()],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(())
    }

    async fn limit_admin_token_to(&self, node: &NodeId) -> TestResult<AuthenticatedCaller> {
        let token_id = TokenId::generate()?;
        let token = mint_bearer(&self.state.master, token_id.clone())?;
        let owner = self.admin.actor_id().to_owned();
        let allowed = serde_json::to_string(&[node])?;
        let verifier = token.verifier;
        self.state.db.write_sqlite(move |tx| {
            let now = now_ms();
            tx.execute(
                "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac, scopes_json,
                                          allowed_nodes_json, expires_at_ms, created_at_ms)
                 VALUES (?1, ?2, 'bearer_api', ?3, '[\"collaborate\"]', ?4, ?5, ?6)",
                rusqlite::params![token_id.as_str(), owner, verifier, allowed, now + 60_000, now],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(authenticate(&self.state.reads, &self.state.master, token.secret.as_str()).await?)
    }

    async fn inbound_question(
        &self,
        from: &NodeId,
        sender: &ParticipantId,
        to: &NodeId,
        recipient: &ParticipantId,
    ) -> TestResult<MessageId> {
        let id = MessageId::generate()?;
        let data = (
            id.clone(),
            sender.clone(),
            from.clone(),
            to.clone(),
            recipient.clone(),
        );
        self.state.db.write_sqlite(move |tx| {
            let (id, sender, from, to, recipient) = data;
            let now = now_ms();
            tx.execute(
                "INSERT INTO hub_messages (message_id, sender_actor_id, sender_node_id, target_node_id, target_participant_id,
                                           body, question_json, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, '', ?6, 'direct_now', 'delivered', ?7, ?8)",
                rusqlite::params![id.as_str(), sender.as_str(), from.as_str(), to.as_str(), recipient.as_str(),
                    r#"{"mode":"text","prompt":"Proceed?"}"#, now, now + 600_000],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(id)
    }
}

struct FakeBridge {
    shared: Arc<FakeState>,
}

struct FakeState {
    reserves: AtomicUsize,
    submits: AtomicUsize,
    permits: AtomicUsize,
    response: Mutex<(CommandOutcome, Option<ProblemDetails>)>,
    reserved: Notify,
    submitted: Notify,
    actor: Mutex<Option<String>>,
    stalled: AtomicBool,
    resume: Notify,
}

impl FakeBridge {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shared: Arc::new(FakeState {
                reserves: AtomicUsize::new(0),
                submits: AtomicUsize::new(0),
                permits: AtomicUsize::new(0),
                response: Mutex::new((CommandOutcome::Delivered, None)),
                reserved: Notify::new(),
                submitted: Notify::new(),
                actor: Mutex::new(None),
                stalled: AtomicBool::new(false),
                resume: Notify::new(),
            }),
        })
    }

    fn set(&self, outcome: CommandOutcome, problem: Option<ProblemDetails>) -> TestResult {
        *self
            .shared
            .response
            .lock()
            .map_err(|_| "fake bridge poisoned")? = (outcome, problem);
        Ok(())
    }
}

struct FakePermit(Arc<FakeState>);

impl Drop for FakePermit {
    fn drop(&mut self) {
        self.0.permits.fetch_sub(1, Ordering::SeqCst);
    }
}

impl CommandPermit for FakePermit {
    fn submit(
        self: Box<Self>,
        payload: CommandReqPayload,
    ) -> BoxFuture<'static, Result<CommandResPayload, SubmitError>> {
        Box::pin(async move {
            self.0.submits.fetch_add(1, Ordering::SeqCst);
            *self.0.actor.lock().map_err(|_| SubmitError::Unknown)? = Some(payload.actor_id);
            self.0.submitted.notify_one();
            if self.0.stalled.load(Ordering::SeqCst) {
                self.0.resume.notified().await;
            }
            let (outcome, problem) = self
                .0
                .response
                .lock()
                .map_err(|_| SubmitError::Unknown)?
                .clone();
            Ok(CommandResPayload {
                op_id: payload.op_id,
                outcome,
                resource_id: None,
                result: None,
                error: problem,
            })
        })
    }
}

impl BridgeRegistry for FakeBridge {
    fn reserve(&self, _node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        self.shared.reserves.fetch_add(1, Ordering::SeqCst);
        if self.shared.permits.fetch_add(1, Ordering::SeqCst) >= 32 {
            self.shared.permits.fetch_sub(1, Ordering::SeqCst);
            return Err(ReserveError::RateLimited);
        }
        self.shared.reserved.notify_one();
        Ok(Box::new(FakePermit(self.shared.clone())))
    }

    fn disconnect_revoked(&self, _node: &NodeId) {}
}

fn direct(node: &NodeId, recipient: &ParticipantId) -> TestResult<MessageRequest> {
    Ok(serde_json::from_value(json!({
        "target_node_id": node,
        "target_participant_id": recipient,
        "body": "Hello, 世界!",
    }))?)
}

fn result_code<T>(result: Result<T, HubError>, expected: ErrorCode) -> TestResult {
    let error = result.err().ok_or("the command unexpectedly succeeded")?;
    assert_eq!(error.problem().code, expected);
    Ok(())
}

#[tokio::test]
async fn all_bridge_outcomes_replay_exact_terminal_result() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let cases = [
        (
            CommandOutcome::Delivered,
            Some((201, DeliveryState::Delivered)),
            None,
        ),
        (
            CommandOutcome::Queued,
            Some((202, DeliveryState::Queued)),
            None,
        ),
        (
            CommandOutcome::Succeeded,
            None,
            Some(ErrorCode::DeliveryUnknown),
        ),
        (CommandOutcome::Busy, None, Some(ErrorCode::ParticipantBusy)),
        (
            CommandOutcome::AwaitingApproval,
            None,
            Some(ErrorCode::AwaitingApproval),
        ),
        (
            CommandOutcome::Unavailable,
            None,
            Some(ErrorCode::BridgeUnavailable),
        ),
        (
            CommandOutcome::Rejected,
            None,
            Some(ErrorCode::InvalidRequest),
        ),
        (
            CommandOutcome::Unknown,
            None,
            Some(ErrorCode::DeliveryUnknown),
        ),
        (CommandOutcome::Expired, None, Some(ErrorCode::Overloaded)),
    ];
    for (index, (outcome, success, error)) in cases.into_iter().enumerate() {
        let key = format!("known-outcome-{index}");
        bridge.set(
            outcome,
            Some(ProblemDetails::new(
                ErrorCode::HubIdentityMismatch,
                "forged coordinator identity",
            )),
        )?;
        let initial = send_message(&f.state, &f.admin, &key, direct(&node, &participant)?).await;
        let replay = send_message(&f.state, &f.admin, &key, direct(&node, &participant)?).await;
        if let Some((status, delivery)) = success {
            let (actual, first) = initial?;
            let (replayed, second) = replay?;
            assert_eq!(actual.as_u16(), status);
            assert_eq!(actual, replayed);
            assert_eq!(first.delivery_state, delivery);
            assert_eq!(first.operation_id, second.operation_id);
            assert_eq!(first.message_id, second.message_id);
            assert_eq!(
                get_message(&f.state, &f.admin, first.message_id)
                    .await?
                    .delivery_state,
                delivery
            );
        } else {
            let expected = error.ok_or("missing case expectation")?;
            result_code(initial, expected)?;
            result_code(replay, expected)?;
        }
        assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), index + 1);
        assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), index + 1);
        assert_eq!(bridge.shared.permits.load(Ordering::SeqCst), 0);
    }

    let mut remote = ProblemDetails::new(ErrorCode::AwaitingApproval, "Awaiting approval");
    remote.status = 500;
    remote.retryable = false;
    remote
        .extensions
        .insert("approval_id".into(), json!("approval-1"));
    remote
        .extensions
        .insert("actual_hub_id".into(), json!("forged"));
    remote
        .extensions
        .insert("action_hint".into(), json!("界".repeat(30_000)));
    bridge.set(CommandOutcome::Rejected, Some(remote))?;
    let problem = send_message(
        &f.state,
        &f.admin,
        "sanitized-reject",
        direct(&node, &participant)?,
    )
    .await
    .err()
    .ok_or("the rejected request succeeded")?;
    assert_eq!(problem.problem().status, 409);
    assert!(problem.problem().retryable);
    assert_eq!(
        problem.problem().extensions.get("approval_id"),
        Some(&json!("approval-1"))
    );
    assert!(!problem.problem().extensions.contains_key("actual_hub_id"));
    assert!(crate::error::bounded_problem_json(problem.problem())?.len() <= 65_536);

    bridge.set(CommandOutcome::Succeeded, None)?;
    let create = serde_json::from_value(json!({"target_node_id": node, "client_kind": "codex"}))?;
    create_agent(&f.state, &f.admin, "create-after-rejection", create).await?;
    assert_eq!(
        bridge
            .shared
            .actor
            .lock()
            .map_err(|_| "fake bridge poisoned")?
            .as_deref(),
        Some(f.admin.actor_id())
    );
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn matching_keys_share_one_delivery_and_normalize_queue_defaults() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.set(CommandOutcome::Queued, None)?;
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let first = serde_json::from_value(json!({
        "target_node_id": node,
        "target_participant_id": participant,
        "body": "Queue this",
        "delivery_mode": "queue_until_idle",
    }))?;
    let explicit_defaults = || {
        serde_json::from_value(json!({
            "target_node_id": node,
            "target_participant_id": participant,
            "body": "Queue this",
            "delivery_mode": "queue_until_idle",
            "queue_ttl_ms": 300_000,
            "attachments": [],
        }))
    };
    let key = "shared-queue-request";
    let state = f.state.clone();
    let admin = f.admin.clone();
    let pending = tokio::spawn(async move { send_message(&state, &admin, key, first).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;
    let duplicate = send_message(&f.state, &f.admin, key, explicit_defaults()?).await;
    bridge.shared.resume.notify_one();
    result_code(duplicate, ErrorCode::OperationInProgress)?;
    let (initial_status, initial) = pending.await??;
    let (replay_status, replay) =
        send_message(&f.state, &f.admin, key, explicit_defaults()?).await?;
    assert_eq!(initial_status, StatusCode::ACCEPTED);
    assert_eq!(initial_status, replay_status);
    assert_eq!(initial.message_id, replay.message_id);
    assert_eq!(initial.operation_id, replay.operation_id);
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    let actor = f.admin.participant_id().map(ParticipantId::as_str);
    assert_eq!(
        bridge
            .shared
            .actor
            .lock()
            .map_err(|_| "fake bridge poisoned")?
            .as_deref(),
        actor
    );
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn rejected_preflight_does_not_reserve_bridge_or_bind_the_key() -> TestResult {
    let f = Fixture::new(Arc::new(OfflineBridge)).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    let key = "not-yet-enrolled";
    result_code(
        send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await,
        ErrorCode::NotFound,
    )?;
    f.node(&node, &participant, "human").await?;
    result_code(
        send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await,
        ErrorCode::BridgeUnavailable,
    )?;
    let stop = serde_json::from_value(json!({"target_node_id": node}))?;
    result_code(
        stop_agent(&f.state, &f.admin, "wrong-target-kind", participant, stop).await,
        ErrorCode::InvalidRequest,
    )?;
    let operations: i64 = f
        .state
        .reads
        .read(|db| Ok(db.query_row("SELECT COUNT(*) FROM operations", [], |row| row.get(0))?))
        .await?;
    assert_eq!(operations, 0);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn failed_attachment_releases_the_bridge_permit_and_operation_key() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let blob_id = BlobId::generate()?;
    let key = "retry-after-upload";
    for _ in 0..33 {
        let input = serde_json::from_value(json!({
            "target_node_id": node,
            "target_participant_id": participant,
            "body": "Send this file",
            "attachments": [{
                "blob_id": blob_id,
                "name": "notes.txt",
                "mime_type": "text/plain",
                "size_bytes": 2,
                "sha256": "0".repeat(64),
            }],
        }))?;
        result_code(
            send_message(&f.state, &f.admin, key, input).await,
            ErrorCode::NotFound,
        )?;
    }
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 33);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 0);
    assert_eq!(bridge.shared.permits.load(Ordering::SeqCst), 0);
    let (_, delivered) = send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await?;
    assert_eq!(delivered.delivery_state, DeliveryState::Delivered);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn cancelling_http_while_acceptance_is_blocked_does_not_cancel_owner() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;

    let (entered, writing) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let db = f.state.db.clone();
    let writer = tokio::spawn(async move {
        db.write_sqlite(move |_tx| {
            let _ = entered.send(());
            let _ = blocked.recv_timeout(Duration::from_secs(5));
            Ok(WriteOutcome::new(()))
        })
        .await
    });
    writing.await?;
    let state = f.state.clone();
    let admin = f.admin.clone();
    let message = direct(&node, &participant)?;
    let key = "detached-external-command";
    let http = tokio::spawn(async move { send_message(&state, &admin, key, message).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.reserved.notified()).await?;
    http.abort();
    let _ = http.await;
    release.send(())?;
    writer.await??;
    tokio::time::timeout(Duration::from_secs(2), f.state.background.drain()).await?;

    let (_, replay) = send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await?;
    assert_eq!(replay.delivery_state, DeliveryState::Delivered);
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.shared.permits.load(Ordering::SeqCst), 0);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn cross_node_answer_remains_local_after_unknown_and_rechecks_saved_scope() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.set(CommandOutcome::Unknown, None)?;
    let f = Fixture::new(bridge.clone()).await?;
    let from = NodeId::generate()?;
    let to = NodeId::generate()?;
    let sender = ParticipantId::generate()?;
    let recipient = ParticipantId::generate()?;
    f.node(&from, &sender, "agent").await?;
    f.node(&to, &recipient, "agent").await?;
    let question = f.inbound_question(&from, &sender, &to, &recipient).await?;
    let input = || -> TestResult<MessageRequest> {
        Ok(serde_json::from_value(json!({
            "target_node_id": from, "target_participant_id": sender, "in_reply_to": question,
            "body": "", "answer": { "text": "Proceed" },
        }))?)
    };

    let limited = f.limit_admin_token_to(&from).await?;
    result_code(
        send_message(&f.state, &limited, "answer-question", input()?).await,
        ErrorCode::Forbidden,
    )?;
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 0);
    let (status, response) = send_message(&f.state, &f.admin, "answer-question", input()?).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(response.delivery_state, DeliveryState::Unknown);
    assert_eq!(response.in_reply_to, Some(question.clone()));
    result_code(
        send_message(&f.state, &limited, "answer-question", input()?).await,
        ErrorCode::Forbidden,
    )?;
    result_code(
        get_message(&f.state, &limited, response.message_id.clone()).await,
        ErrorCode::Forbidden,
    )?;
    result_code(
        send_message(&f.state, &f.admin, "duplicate-answer", input()?).await,
        ErrorCode::QuestionAlreadyAnswered,
    )?;
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);

    let dir = f.close().await?;
    let restart_bridge = FakeBridge::new();
    let f = Fixture::open(dir, restart_bridge.clone()).await?;
    let (status, replay) = send_message(&f.state, &f.admin, "answer-question", input()?).await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(replay.operation_id, response.operation_id);
    assert_eq!(replay.message_id, response.message_id);
    assert_eq!(replay.delivery_state, DeliveryState::Unknown);
    assert_eq!(restart_bridge.shared.reserves.load(Ordering::SeqCst), 0);
    assert_eq!(restart_bridge.shared.submits.load(Ordering::SeqCst), 0);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn offline_answer_commits_and_a_hub_originated_answer_has_no_bridge_target() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let from_hub = f
        .admin
        .participant_id()
        .ok_or("admin belongs to no participant")?
        .clone();
    let input: MessageRequest = serde_json::from_value(json!({
        "target_node_id": node, "target_participant_id": participant, "body": "",
        "question": {"mode": "text", "prompt": "Proceed?"}
    }))?;
    let (_, question) = send_message(&f.state, &f.admin, "create-hub-question", input).await?;
    let request_for = |parent: &MessageId, target: Option<&NodeId>, recipient: &ParticipantId| {
        serde_json::from_value(json!({
            "target_node_id": target, "target_participant_id": recipient, "body": "",
            "in_reply_to": parent, "answer": {"text": "Proceed"},
        }))
    };
    let (status, answer) = send_message(
        &f.state,
        &f.admin,
        "answer-hub-question",
        request_for(&question.message_id, None, &from_hub)?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(answer.delivery_state, DeliveryState::Delivered);
    assert_eq!(
        get_message(&f.state, &f.admin, answer.message_id)
            .await?
            .target_node_id,
        None
    );
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);

    let inbound = f
        .inbound_question(&node, &participant, &node, &from_hub)
        .await?;
    let node_id = node.clone();
    f.state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE nodes SET status = 'revoked', revoked_at_ms = ?2 WHERE node_id = ?1",
                rusqlite::params![node_id.as_str(), now_ms()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let (status, answer) = send_message(
        &f.state,
        &f.admin,
        "answer-offline-question",
        request_for(&inbound, Some(&node), &participant)?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(answer.delivery_state, DeliveryState::Unavailable);
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);
    let count: i64 = f
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
        "SELECT COUNT(*) FROM events WHERE kind = 'question.answered' AND aggregate_id IN (?1, ?2)",
        rusqlite::params![question.message_id.as_str(), inbound.as_str()],
        |row| row.get(0),
    )?)
        })
        .await?;
    assert_eq!(count, 2);
    f.close().await?;
    Ok(())
}
