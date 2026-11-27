use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use cooplet_protocol::{BlobId, CommandOutcome, CommandResPayload, HubId, ProblemDetails, TokenId};
use cooplet_storage::StorageEngine;
use futures_util::future::BoxFuture;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, watch},
};

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
    shutdown: watch::Sender<()>,
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
            shutdown,
        })
    }

    async fn close(self) -> TestResult<TempDir> {
        self.state.background.close();
        self.state.background.drain().await;
        self.engine.shutdown().await?;
        Ok(self.dir)
    }

    async fn listen(&self) -> TestResult<Loopback> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let token =
            cooplet_platform::PrivateFs::read_private_file(&self.dir.0.join("admin.token"), 128)?;
        let bearer = std::str::from_utf8(&token)?.trim().to_owned();
        let hub_id = self.state.meta.hub_id.clone();
        let router = crate::http::router(self.state.clone(), crate::http::RequestLimits::default());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(Loopback {
            address,
            bearer,
            hub_id,
            task,
        })
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

    async fn scoped_admin_token(
        &self,
        node: &NodeId,
    ) -> TestResult<(TokenId, AuthenticatedCaller)> {
        let token_id = TokenId::generate()?;
        let token = mint_bearer(&self.state.master, token_id.clone())?;
        let stored_id = token_id.clone();
        let owner = self.admin.actor_id().to_owned();
        let allowed = serde_json::to_string(&[node])?;
        let verifier = token.verifier;
        self.state.db.write_sqlite(move |tx| {
            let now = now_ms();
            tx.execute(
                "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac, scopes_json,
                                          allowed_nodes_json, expires_at_ms, created_at_ms)
                 VALUES (?1, ?2, 'bearer_api', ?3, '[\"collaborate\"]', ?4, ?5, ?6)",
                rusqlite::params![stored_id.as_str(), owner, verifier, allowed, now + 60_000, now],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        let caller =
            authenticate(&self.state.reads, &self.state.master, token.secret.as_str()).await?;
        Ok((token_id, caller))
    }

    async fn inbound_question(
        &self,
        from: &NodeId,
        sender: &ParticipantId,
        to: Option<&NodeId>,
        recipient: &ParticipantId,
    ) -> TestResult<MessageId> {
        let id = MessageId::generate()?;
        let data = (
            id.clone(),
            sender.clone(),
            from.clone(),
            to.cloned(),
            recipient.clone(),
        );
        self.state.db.write_sqlite(move |tx| {
            let (id, sender, from, to, recipient) = data;
            let now = now_ms();
            tx.execute(
                "INSERT INTO hub_messages (message_id, sender_actor_id, sender_node_id, target_node_id, target_participant_id,
                                           body, question_json, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, '', ?6, 'direct_now', 'delivered', ?7, ?8)",
                rusqlite::params![id.as_str(), sender.as_str(), from.as_str(), to.as_ref().map(NodeId::as_str), recipient.as_str(),
                    r#"{"mode":"text","prompt":"Proceed?"}"#, now, now + 600_000],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(id)
    }
}

struct Loopback {
    address: std::net::SocketAddr,
    bearer: String,
    hub_id: HubId,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Loopback {
    async fn post_message(
        &self,
        key: &str,
        request: &serde_json::Value,
    ) -> TestResult<(u16, MessageReceipt)> {
        let json = serde_json::to_vec(request)?;
        let mut socket = TcpStream::connect(self.address).await?;
        let headers = format!(
            "POST /v1/messages HTTP/1.1\r\nHost: {}\r\nX-Cooplet-Hub: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nIdempotency-Key: {key}\r\nConnection: close\r\n\r\n",
            self.address,
            self.hub_id,
            self.bearer,
            json.len(),
        );
        socket.write_all(headers.as_bytes()).await?;
        socket.write_all(&json).await?;
        let mut wire = Vec::new();
        tokio::time::timeout(Duration::from_secs(8), socket.read_to_end(&mut wire)).await??;
        let body_offset = wire
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .ok_or("missing response headers")?;
        let status = std::str::from_utf8(&wire[..body_offset])?
            .split_whitespace()
            .nth(1)
            .ok_or("missing response status")?
            .parse()?;
        let receipt = serde_json::from_slice(&wire[body_offset + 4..])?;
        Ok((status, receipt))
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        self.task.abort();
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
    resource_id: Mutex<Option<String>>,
    result: Mutex<Option<serde_json::Value>>,
    submit_error: Mutex<Option<SubmitError>>,
    reserved: Notify,
    submitted: Notify,
    actor: Mutex<Option<String>>,
    last_command: Mutex<Option<BridgeCommand>>,
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
                resource_id: Mutex::new(None),
                result: Mutex::new(None),
                submit_error: Mutex::new(None),
                reserved: Notify::new(),
                submitted: Notify::new(),
                actor: Mutex::new(None),
                last_command: Mutex::new(None),
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
        *self
            .shared
            .submit_error
            .lock()
            .map_err(|_| "fake bridge poisoned")? = None;
        Ok(())
    }

    fn set_payload(
        &self,
        resource_id: Option<String>,
        result: Option<serde_json::Value>,
    ) -> TestResult {
        *self
            .shared
            .resource_id
            .lock()
            .map_err(|_| "fake bridge poisoned")? = resource_id;
        *self
            .shared
            .result
            .lock()
            .map_err(|_| "fake bridge poisoned")? = result;
        Ok(())
    }

    fn set_submit_error(&self, error: Option<SubmitError>) -> TestResult {
        *self
            .shared
            .submit_error
            .lock()
            .map_err(|_| "fake bridge poisoned")? = error;
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
            *self
                .0
                .last_command
                .lock()
                .map_err(|_| SubmitError::Unknown)? = Some(payload.command);
            self.0.submitted.notify_one();
            if self.0.stalled.load(Ordering::SeqCst) {
                self.0.resume.notified().await;
            }
            if let Some(error) = *self
                .0
                .submit_error
                .lock()
                .map_err(|_| SubmitError::Unknown)?
            {
                return Err(error);
            }
            let (outcome, problem) = self
                .0
                .response
                .lock()
                .map_err(|_| SubmitError::Unknown)?
                .clone();
            let resource_id = self
                .0
                .resource_id
                .lock()
                .map_err(|_| SubmitError::Unknown)?
                .clone();
            let result = self
                .0
                .result
                .lock()
                .map_err(|_| SubmitError::Unknown)?
                .clone();
            Ok(CommandResPayload {
                op_id: payload.op_id,
                outcome,
                resource_id,
                result,
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

fn prepare_message(
    caller: &AuthenticatedCaller,
    key: &str,
    mut input: MessageRequest,
) -> TestResult<Arc<Request>> {
    let message_id = MessageId::generate()?;
    input.normalize()?;
    let sender = caller
        .participant_id()
        .ok_or("expected a participant identity")?
        .clone();
    let sender_kind = caller
        .author_kind()
        .ok_or("expected a human or service author kind")?;
    let action = Action::Message {
        message_id,
        sender,
        sender_kind,
        sender_name: caller.display_name().to_owned(),
        input: Box::new(input),
    };
    let kind = action.kind()?;
    Ok(Arc::new(Request {
        id: OperationId::generate()?,
        key: key.into(),
        fingerprint: action.fingerprint(kind)?,
        kind,
        action,
        caller: caller.clone(),
    }))
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
            None,
            Some(ErrorCode::DeliveryUnknown),
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
async fn revocation_between_initial_auth_and_receipt_lookup_is_rechecked() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let (token_id, caller) = f.scoped_admin_token(&node).await?;
    let key = "retained-after-revocation";
    let (_, accepted) = send_message(&f.state, &caller, key, direct(&node, &participant)?).await?;
    crate::admin::revoke_token(
        &f.state,
        &f.admin,
        "revoke-racing-token",
        caller.actor_id(),
        token_id,
    )
    .await?;

    result_code(
        send_message(&f.state, &caller, key, direct(&node, &participant)?).await,
        ErrorCode::Unauthorized,
    )?;
    let request = prepare_message(&caller, key, direct(&node, &participant)?)?;
    result_code(
        replay_only(&f.state, request).await,
        ErrorCode::Unauthorized,
    )?;
    let (_, authorized_replay) =
        send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await?;
    assert_eq!(authorized_replay.operation_id, accepted.operation_id);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
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
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 0);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 0);
    assert_eq!(bridge.shared.permits.load(Ordering::SeqCst), 0);
    let (_, delivered) =
        send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await?;
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
async fn receipt_finalization_outlives_the_client_deadline_during_writer_overload() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let state = f.state.clone();
    let admin = f.admin.clone();
    let input = direct(&node, &participant)?;
    let key = "temporarily-overloaded-writer";
    let response = tokio::spawn(async move { send_message(&state, &admin, key, input).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;

    let (entered, blocked) = oneshot::channel();
    let (release, released) = std::sync::mpsc::sync_channel(1);
    let db = f.state.db.clone();
    let writer = tokio::spawn(async move {
        db.write_sqlite(move |_| {
            let _ = entered.send(());
            let _ = released.recv_timeout(Duration::from_secs(8));
            Ok(WriteOutcome::new(()))
        })
        .await
    });
    blocked.await?;
    let config = cooplet_storage::StorageConfig::new(f.dir.0.join("hub.sqlite3"));
    let mut queued = Vec::with_capacity(config.write_queue_capacity);
    for _ in 0..config.write_queue_capacity {
        let mut write = Box::pin(f.state.db.write_sqlite(|_| Ok(WriteOutcome::new(()))));
        let pending = std::future::poll_fn(|context| {
            std::task::Poll::Ready(write.as_mut().poll(context).is_pending())
        })
        .await;
        assert!(pending);
        queued.push(write);
    }
    assert!(matches!(
        f.state.db.write_sqlite(|_| Ok(WriteOutcome::new(()))).await,
        Err(StorageError::Overloaded)
    ));

    bridge.shared.resume.notify_one();
    let limit = FINALIZATION_REPLY_TIMEOUT + Duration::from_secs(2);
    let error = tokio::time::timeout(limit, response)
        .await??
        .err()
        .ok_or("writer was still blocked")?;
    assert_eq!(error.problem().status, 503);
    assert_eq!(error.problem().code, ErrorCode::DeliveryUnknown);
    let pending_id = error.problem().extensions["operation_id"]
        .as_str()
        .ok_or("missing operation ID")?;
    let pending = f
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT operation_id, state FROM operations WHERE operation_key = ?1",
                [key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?)
        })
        .await?;
    assert_eq!(pending, (pending_id.to_owned(), "accepted".into()));

    release.send(())?;
    writer.await??;
    for write in queued {
        write.await?;
    }
    tokio::time::timeout(Duration::from_secs(2), f.state.background.drain()).await?;
    let (_, replay) = send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await?;
    assert_eq!(replay.operation_id.as_str(), pending_id);
    assert_eq!(replay.delivery_state, DeliveryState::Delivered);
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 1);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn recovering_an_unacknowledged_acceptance_never_starts_bridge_delivery() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let participant = ParticipantId::generate()?;
    f.node(&node, &participant, "agent").await?;
    let key = "lost-acceptance-ack";
    let request = prepare_message(&f.admin, key, direct(&node, &participant)?)?;
    assert!(matches!(
        accept_request(&f.state, request.clone(), Some(node.clone())).await?,
        Accepted::Dispatch(_)
    ));

    // Simulate losing the acknowledgement of the already committed first write.
    let mut client = ClientReply(None);
    result_code(
        recover_acceptance(&f.state, request.clone(), &mut client).await,
        ErrorCode::DeliveryUnknown,
    )?;
    result_code(
        send_message(&f.state, &f.admin, key, direct(&node, &participant)?).await,
        ErrorCode::DeliveryUnknown,
    )?;
    let message_id = request.message_id().ok_or("missing message ID")?.clone();
    assert_eq!(
        get_message(&f.state, &f.admin, message_id)
            .await?
            .delivery_state,
        DeliveryState::Unknown
    );
    assert_eq!(bridge.shared.reserves.load(Ordering::SeqCst), 0);
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 0);
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
    let question = f
        .inbound_question(&from, &sender, Some(&to), &recipient)
        .await?;
    let input = || -> TestResult<MessageRequest> {
        Ok(serde_json::from_value(json!({
            "target_node_id": from, "target_participant_id": sender, "in_reply_to": question,
            "body": "", "answer": { "text": "Proceed" },
        }))?)
    };

    let (_, limited) = f.scoped_admin_token(&from).await?;
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
        .inbound_question(&node, &participant, None, &from_hub)
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

fn answer_for(node: &NodeId, recipient: &ParticipantId, question: &MessageId) -> serde_json::Value {
    json!({
        "target_node_id": node,
        "target_participant_id": recipient,
        "in_reply_to": question,
        "body": "",
        "answer": { "text": "Proceed" },
    })
}

#[tokio::test]
async fn hub_participant_addresses_are_qualified_by_destination() -> TestResult {
    let f = Fixture::new(Arc::new(OfflineBridge)).await?;
    let participant = f.admin.participant_id().ok_or("missing Hub participant")?;
    assert!(f.admin.is_hub_recipient(None, participant));
    assert!(
        !f.admin
            .is_hub_recipient(Some(&NodeId::generate()?), participant)
    );
    assert!(!f.admin.is_hub_recipient(None, &ParticipantId::generate()?));
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_answer_keys_report_one_winner() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let asker = ParticipantId::generate()?;
    f.node(&node, &asker, "agent").await?;
    let recipient = f.admin.participant_id().ok_or("missing Hub participant")?;
    let question = f.inbound_question(&node, &asker, None, recipient).await?;
    let answer = answer_for(&node, &asker, &question);

    // Keep both callers past RO preflight before either acceptance can commit.
    let (entered, writing) = oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let db = f.state.db.clone();
    let writer = tokio::spawn(async move {
        db.write_sqlite(move |_tx| {
            let _ = entered.send(());
            let _ = blocked.recv_timeout(Duration::from_secs(10));
            Ok(WriteOutcome::new(()))
        })
        .await
    });
    writing.await?;
    let race = |key, body| {
        let state = f.state.clone();
        let caller = f.admin.clone();
        tokio::spawn(async move { send_message(&state, &caller, key, body).await })
    };
    let first = race("first-answer-key", serde_json::from_value(answer.clone())?);
    let second = race("second-answer-key", serde_json::from_value(answer)?);
    tokio::time::timeout(Duration::from_secs(3), async {
        while bridge.shared.reserves.load(Ordering::SeqCst) < 2 {
            bridge.shared.reserved.notified().await;
        }
    })
    .await?;
    release.send(())?;
    writer.await??;
    let (winner, conflict) = match (first.await?, second.await?) {
        (Ok(winner), Err(conflict)) | (Err(conflict), Ok(winner)) => (winner, conflict),
        _ => return Err("expected one answer and one conflict".into()),
    };
    assert_eq!(winner.0, StatusCode::CREATED);
    assert_eq!(conflict.problem().code, ErrorCode::QuestionAlreadyAnswered);
    assert_eq!(conflict.problem().status, 409);
    assert_eq!(
        conflict.problem().extensions["existing_answer_id"],
        winner.1.message_id.as_str()
    );
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    let counts: (u64,u64,u64) = f.state.reads.read(|db| Ok(db.query_row(
        "SELECT (SELECT COUNT(*) FROM operations), (SELECT COUNT(*) FROM hub_messages WHERE answer_json IS NOT NULL),
                (SELECT COUNT(*) FROM events WHERE kind = 'question.answered')",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?))
    )?)).await?;
    assert_eq!(counts, (1, 1, 1));
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn production_offline_bridge_accepts_answer_on_real_http() -> TestResult {
    let f = Fixture::new(Arc::new(OfflineBridge)).await?;
    let node = NodeId::generate()?;
    let asker = ParticipantId::generate()?;
    let recipient = f
        .admin
        .participant_id()
        .ok_or("missing admin participant")?;
    f.node(&node, &asker, "agent").await?;
    let question = f.inbound_question(&node, &asker, None, recipient).await?;
    let input = answer_for(&node, &asker, &question);

    let http = f.listen().await?;
    let key = "answer-with-offline-bridge";
    let (status, first) = http.post_message(key, &input).await?;
    assert_eq!(status, 201);
    assert_eq!(first.delivery_state, DeliveryState::Unavailable);
    assert_eq!(first.in_reply_to.as_ref(), Some(&question));

    let (status, replay) = http.post_message(key, &input).await?;
    assert_eq!(status, 201);
    assert_eq!(replay.delivery_state, DeliveryState::Unavailable);
    assert_eq!(replay.message_id, first.message_id);
    assert_eq!(replay.operation_id, first.operation_id);
    drop(http);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn restart_recovers_persisted_answer_acceptance_before_real_http_replay() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let asker = ParticipantId::generate()?;
    let recipient = f
        .admin
        .participant_id()
        .ok_or("missing admin participant")?;
    f.node(&node, &asker, "agent").await?;
    let question = f.inbound_question(&node, &asker, None, recipient).await?;
    let input = answer_for(&node, &asker, &question);
    let key = "recover-accepted-answer";

    let first_hub = Arc::new(f.listen().await?);
    let caller = first_hub.clone();
    let request = input.clone();
    let active = tokio::spawn(async move { caller.post_message(key, &request).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;
    let (op_id, msg_id, operation_state, message_state): (String, String, String, String) = f
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT o.operation_id, m.message_id, o.state, m.delivery_state
                   FROM operations o JOIN hub_messages m ON m.operation_id = o.operation_id
                  WHERE o.operation_key = ?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?)
        })
        .await?;
    assert_eq!(operation_state, "accepted");
    assert_eq!(message_state, "accepted");

    // Stop the writer while the first delivery is stalled after its accepted
    // commit. Once released, that owner can no longer persist a terminal state.
    let Fixture {
        engine,
        state,
        dir,
        shutdown,
        ..
    } = f;
    state.background.close();
    engine.shutdown().await?;
    bridge.shared.resume.notify_one();
    let (status, first) = active.await??;
    assert_eq!(status, 201);
    assert_eq!(first.operation_id.as_str(), op_id);
    assert_eq!(first.message_id.as_str(), msg_id);
    assert_eq!(first.delivery_state, DeliveryState::Unknown);
    state.background.drain().await;
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);
    drop(first_hub);
    drop(shutdown);

    let restart_bridge = FakeBridge::new();
    let f = Fixture::open(dir, restart_bridge.clone()).await?;
    let http = f.listen().await?;
    let (status, replay) = http.post_message(key, &input).await?;
    assert_eq!(status, 201);
    assert_eq!(replay.delivery_state, DeliveryState::Unknown);
    assert_eq!(replay.in_reply_to.as_ref(), Some(&question));
    assert_eq!(replay.operation_id.as_str(), op_id);
    assert_eq!(replay.message_id.as_str(), msg_id);
    let view = get_message(&f.state, &f.admin, replay.message_id).await?;
    assert_eq!(view.delivery_state, DeliveryState::Unknown);
    assert_eq!(restart_bridge.shared.reserves.load(Ordering::SeqCst), 0);
    assert_eq!(restart_bridge.shared.submits.load(Ordering::SeqCst), 0);
    drop(http);
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn control_commands_resource_id_validation_and_result_budget() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.set(CommandOutcome::Succeeded, None)?;
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;

    // Blank agent selectors are rejected before bridge dispatch.
    for payload in [
        json!({"target_node_id": node, "client_kind": "   "}),
        json!({"target_node_id": node, "template_id": "\t"}),
    ] {
        result_code(
            create_agent(
                &f.state,
                &f.admin,
                "blank-selector",
                serde_json::from_value(payload)?,
            )
            .await,
            ErrorCode::InvalidRequest,
        )?;
    }

    // CreateAgent validates ParticipantId resource_id and drops oversized >64 KiB result.
    let created_agent = ParticipantId::generate()?;
    bridge.set_payload(
        Some(created_agent.to_string()),
        Some(json!({"blob": "x".repeat(70 * 1024)})),
    )?;
    let receipt = create_agent(
        &f.state,
        &f.admin,
        "create-oversized-result",
        serde_json::from_value(json!({"target_node_id": node, "client_kind": "codex"}))?,
    )
    .await?;
    assert_eq!(receipt.resource_id.as_deref(), Some(created_agent.as_str()));
    assert!(receipt.result.is_none());

    // Invalid resource_id on CreateAgent or RunWorkflow turns into unknown.
    bridge.set_payload(Some("not-a-participant".into()), None)?;
    result_code(
        create_agent(
            &f.state,
            &f.admin,
            "create-bad-resource-id",
            serde_json::from_value(json!({"target_node_id": node, "client_kind": "codex"}))?,
        )
        .await,
        ErrorCode::DeliveryUnknown,
    )?;

    let run_id = RunId::generate()?;
    bridge.set_payload(Some("bad\nrun".into()), None)?;
    result_code(
        run_workflow(
            &f.state,
            &f.admin,
            "run-bad-resource-id",
            serde_json::from_value(json!({
                "target_node_id": node,
                "workflow_name": "deploy",
                "workspace_dir": "."
            }))?,
        )
        .await,
        ErrorCode::DeliveryUnknown,
    )?;

    // Happy path + replay for stop_agent, respond_approval, run_workflow, cancel_workflow.
    bridge.set_payload(Some(run_id.to_string()), Some(json!({"ok": true})))?;
    let run_req = || -> TestResult<RunWorkflowRequest> {
        Ok(serde_json::from_value(json!({
            "target_node_id": node,
            "workflow_name": "deploy",
            "workspace_dir": "."
        }))?)
    };
    let ran = run_workflow(&f.state, &f.admin, "run-workflow-ok", run_req()?).await?;
    assert_eq!(ran.resource_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(ran.result, Some(json!({"ok": true})));
    assert_eq!(
        run_workflow(&f.state, &f.admin, "run-workflow-ok", run_req()?).await?,
        ran
    );

    bridge.set_payload(None, Some(json!({"stopped": true})))?;
    let stop_req = || -> TestResult<StopAgentRequest> {
        Ok(serde_json::from_value(json!({"target_node_id": node}))?)
    };
    let stopped = stop_agent(
        &f.state,
        &f.admin,
        "stop-agent-ok",
        agent.clone(),
        stop_req()?,
    )
    .await?;
    assert_eq!(
        stop_agent(
            &f.state,
            &f.admin,
            "stop-agent-ok",
            agent.clone(),
            stop_req()?
        )
        .await?,
        stopped
    );

    let approval_req = || -> TestResult<RespondApprovalRequest> {
        Ok(serde_json::from_value(json!({
            "target_node_id": node,
            "approval_id": "apr_123",
            "action": "approve"
        }))?)
    };
    let approved = respond_approval(
        &f.state,
        &f.admin,
        "approval-ok",
        agent.clone(),
        approval_req()?,
    )
    .await?;
    assert_eq!(
        respond_approval(&f.state, &f.admin, "approval-ok", agent, approval_req()?).await?,
        approved
    );

    let cancel_req = || -> TestResult<CancelWorkflowRequest> {
        Ok(serde_json::from_value(json!({"target_node_id": node}))?)
    };
    let cancelled = cancel_workflow(
        &f.state,
        &f.admin,
        "cancel-workflow-ok",
        run_id.clone(),
        cancel_req()?,
    )
    .await?;
    assert_eq!(
        cancel_workflow(
            &f.state,
            &f.admin,
            "cancel-workflow-ok",
            run_id,
            cancel_req()?
        )
        .await?,
        cancelled
    );

    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn submit_unavailable_and_in_progress_finish_and_extension_scoping() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;

    // SubmitError::Unavailable maps identically to CommandOutcome::Unavailable.
    bridge.set_submit_error(Some(SubmitError::Unavailable))?;
    result_code(
        send_message(
            &f.state,
            &f.admin,
            "submit-unavailable-msg",
            direct(&node, &agent)?,
        )
        .await,
        ErrorCode::BridgeUnavailable,
    )?;
    result_code(
        send_message(
            &f.state,
            &f.admin,
            "submit-unavailable-msg",
            direct(&node, &agent)?,
        )
        .await,
        ErrorCode::BridgeUnavailable,
    )?;

    let recipient = f
        .admin
        .participant_id()
        .ok_or("missing admin participant")?;
    let q_unavail = f.inbound_question(&node, &agent, None, recipient).await?;
    let (status, answer) = send_message(
        &f.state,
        &f.admin,
        "submit-unavailable-ans",
        serde_json::from_value(answer_for(&node, &agent, &q_unavail))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(answer.delivery_state, DeliveryState::Unavailable);

    // CommandOutcome::Queued on default direct_now answer_question is classified as unknown.
    bridge.set(CommandOutcome::Queued, None)?;
    let q_queued = f.inbound_question(&node, &agent, None, recipient).await?;
    let (status, answer) = send_message(
        &f.state,
        &f.admin,
        "queued-answer-anomaly",
        serde_json::from_value(answer_for(&node, &agent, &q_queued))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(answer.delivery_state, DeliveryState::Unknown);

    // copy_extensions scopes fields per ErrorCode: NotFound must not carry approval_id or existing_answer_id.
    let mut mixed = ProblemDetails::new(ErrorCode::NotFound, "Not found");
    mixed
        .extensions
        .insert("approval_id".into(), json!("apr_leak"));
    mixed
        .extensions
        .insert("existing_answer_id".into(), json!("msg_leak"));
    bridge.set(CommandOutcome::Rejected, Some(mixed))?;
    let problem = send_message(
        &f.state,
        &f.admin,
        "rejected-not-found-ext",
        direct(&node, &agent)?,
    )
    .await
    .err()
    .ok_or("expected rejected error")?;
    assert_eq!(problem.problem().code, ErrorCode::NotFound);
    assert!(!problem.problem().extensions.contains_key("approval_id"));
    assert!(
        !problem
            .problem()
            .extensions
            .contains_key("existing_answer_id")
    );

    // journal::finish transitions an 'in_progress' operation cleanly to terminal.
    bridge.set(CommandOutcome::Delivered, None)?;
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let state = f.state.clone();
    let admin = f.admin.clone();
    let msg = direct(&node, &agent)?;
    let in_flight =
        tokio::spawn(async move { send_message(&state, &admin, "finish-in-progress", msg).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;
    f.state
        .db
        .write_sqlite(|tx| {
            tx.execute(
                "UPDATE operations SET state = 'in_progress' WHERE operation_key = 'finish-in-progress'",
                [],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    bridge.shared.resume.notify_one();
    let (status, delivered) = in_flight.await??;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(delivered.delivery_state, DeliveryState::Delivered);

    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn storage_full_during_finalization_falls_back_to_compact_unknown() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.set(CommandOutcome::Delivered, None)?;
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;

    let state = f.state.clone();
    let admin = f.admin.clone();
    let msg = direct(&node, &agent)?;
    let key = "disk-full-tx2";
    let pending = tokio::spawn(async move { send_message(&state, &admin, key, msg).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;

    // Trigger SQLITE_FULL on the next events INSERT (message.delivery_updated in Tx 2).
    f.state
        .db
        .write_sqlite(|tx| {
            tx.execute(
                "UPDATE sqlite_sequence SET seq = 9223372036854775807 WHERE name = 'events'",
                [],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    bridge.shared.resume.notify_one();
    result_code(pending.await?, ErrorCode::DeliveryUnknown)?;
    // Replay must return 503 delivery_unknown rather than permanent 409 operation_in_progress.
    result_code(
        send_message(&f.state, &f.admin, key, direct(&node, &agent)?).await,
        ErrorCode::DeliveryUnknown,
    )?;
    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_rejects_new_commands_but_commits_inflight_confirmed_outcomes() -> TestResult {
    let bridge = FakeBridge::new();
    bridge.set(CommandOutcome::Delivered, None)?;
    bridge.shared.stalled.store(true, Ordering::SeqCst);
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;

    // Start one command before shutdown; signal shutdown while it is in submit().
    let state = f.state.clone();
    let admin = f.admin.clone();
    let msg = direct(&node, &agent)?;
    let inflight =
        tokio::spawn(async move { send_message(&state, &admin, "inflight-shutdown", msg).await });
    tokio::time::timeout(Duration::from_secs(2), bridge.shared.submitted.notified()).await?;

    f.shutdown.send_replace(());
    // A new command arriving after shutdown fails closed before Tx 1.
    result_code(
        send_message(&f.state, &f.admin, "after-shutdown", direct(&node, &agent)?).await,
        ErrorCode::Overloaded,
    )?;
    assert_eq!(bridge.shared.submits.load(Ordering::SeqCst), 1);

    // Release the already-submitted command while DbActor is still alive: Attempt 1 commits.
    bridge.shared.resume.notify_one();
    let (status, receipt) = inflight.await??;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt.delivery_state, DeliveryState::Delivered);

    // Bounded BackgroundWrites rejects overflow with 503 Overloaded.
    let bounded = crate::BackgroundWrites::new(1);
    let (hold_tx, hold_rx) = oneshot::channel::<()>();
    let first = bounded.spawn(async move {
        let _ = hold_rx.await;
    })?;
    result_code(bounded.spawn(async {}), ErrorCode::Overloaded)?;
    let _ = hold_tx.send(());
    first.await?;
    bounded.drain().await;

    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn attachment_cap_valid_attach_question_expiry_and_human_member_acl() -> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;

    // More than 32 attachments is rejected statically before touching the database.
    let too_many: Vec<_> = (0..33)
        .map(|i| {
            Ok::<_, Box<dyn Error + Send + Sync>>(json!({
                "blob_id": BlobId::generate()?,
                "name": format!("f{i}.txt"),
                "mime_type": "text/plain",
                "size_bytes": 1,
                "sha256": "0".repeat(64),
            }))
        })
        .collect::<Result<_, _>>()?;
    result_code(
        send_message(
            &f.state,
            &f.admin,
            "too-many-attachments",
            serde_json::from_value(json!({
                "target_node_id": node,
                "target_participant_id": agent,
                "body": "Files",
                "attachments": too_many,
            }))?,
        )
        .await,
        ErrorCode::InvalidRequest,
    )?;

    // Valid send_message with an uploaded blob succeeds and attaches the blob to target_node.
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain"),
    );
    headers.insert(
        cooplet_client::X_COOPLET_FILENAME,
        http::HeaderValue::from_static("note.txt"),
    );
    let (_, uploaded) = crate::blobs::upload(
        &f.state,
        &f.admin,
        &headers,
        axum::body::Body::from("hello"),
    )
    .await?;
    let (status, sent) = send_message(
        &f.state,
        &f.admin,
        "valid-attachment-msg",
        serde_json::from_value(json!({
            "target_node_id": node,
            "target_participant_id": agent,
            "body": "See attachment",
            "attachments": [uploaded],
        }))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(sent.delivery_state, DeliveryState::Delivered);
    let attached_node: Option<String> = f
        .state
        .reads
        .read(move |db| {
            Ok(db.query_row(
                "SELECT scope_node_id FROM blobs WHERE blob_id = ?1",
                [uploaded.blob_id.as_str()],
                |row| row.get(0),
            )?)
        })
        .await?;
    assert_eq!(attached_node.as_deref(), Some(node.as_str()));

    // Expired question returns 409 question_expired.
    let expired_q = MessageId::generate()?;
    let q_id = expired_q.clone();
    let n_id = node.clone();
    let a_id = agent.clone();
    let admin_part = f
        .admin
        .participant_id()
        .ok_or("missing admin participant")?
        .clone();
    let ap_id = admin_part.clone();
    f.state
        .db
        .write_sqlite(move |tx| {
            let now = now_ms();
            let q_json = serde_json::to_string(&json!({
                "mode": "text",
                "prompt": "Expired?",
                "expires_at_ms": now - 1_000,
            }))
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            tx.execute(
                "INSERT INTO hub_messages (message_id, sender_actor_id, sender_node_id,
                    target_node_id, target_participant_id, body, question_json,
                    delivery_mode, delivery_state, created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, NULL, ?4, '', ?5, 'direct_now', 'delivered', ?6, ?7)",
                rusqlite::params![
                    q_id.as_str(),
                    a_id.as_str(),
                    n_id.as_str(),
                    ap_id.as_str(),
                    q_json,
                    now - 2_000,
                    now + 600_000
                ],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    result_code(
        send_message(
            &f.state,
            &f.admin,
            "answer-expired-q",
            serde_json::from_value(answer_for(&node, &agent, &expired_q))?,
        )
        .await,
        ErrorCode::QuestionExpired,
    )?;

    // HumanMember answer ACL: allowed for an agent target on allowed_nodes, forbidden for another human.
    let member_user = cooplet_protocol::UserId::generate()?;
    let member_part = ParticipantId::generate()?;
    let member_tok_id = TokenId::generate()?;
    let member_tok = mint_bearer(&f.state.master, member_tok_id.clone())?;
    let member_verifier = member_tok.verifier;
    let m_user = member_user.clone();
    let m_part = member_part.clone();
    let m_tok_id = member_tok_id.clone();
    let m_nodes = serde_json::to_string(&[&node])?;
    f.state
        .db
        .write_sqlite(move |tx| {
            let now = now_ms();
            tx.execute(
                "INSERT INTO principals (principal_id, kind, username, display_name,
                    participant_id, scopes_json, allowed_nodes_json, created_at_ms, updated_at_ms)
                 VALUES (?1, 'human_member', 'member1', 'Member', ?2, '[\"collaborate\"]', ?3, ?4, ?4)",
                rusqlite::params![m_user.as_str(), m_part.as_str(), m_nodes, now],
            )?;
            tx.execute(
                "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                    scopes_json, allowed_nodes_json, expires_at_ms, created_at_ms)
                 VALUES (?1, ?2, 'bearer_api', ?3, '[\"collaborate\"]', NULL, ?4, ?5)",
                rusqlite::params![m_tok_id.as_str(), m_user.as_str(), member_verifier, now + 60_000, now],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let member = authenticate(&f.state.reads, &f.state.master, member_tok.secret.as_str()).await?;

    let q_for_admin = f.inbound_question(&node, &agent, None, &admin_part).await?;
    result_code(
        send_message(
            &f.state,
            &member,
            "member-for-other-human",
            serde_json::from_value(answer_for(&node, &agent, &q_for_admin))?,
        )
        .await,
        ErrorCode::Forbidden,
    )?;

    let other_agent = ParticipantId::generate()?;
    f.state
        .db
        .write_sqlite({
            let node = node.clone();
            let other_agent = other_agent.clone();
            move |tx| {
                tx.execute(
                    "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                     VALUES (?1, ?2, 'agent', 'Other agent', 'idle', 0, ?3)",
                    rusqlite::params![node.as_str(), other_agent.as_str(), now_ms()],
                )?;
                Ok(WriteOutcome::new(()))
            }
        })
        .await?;
    let q_for_agent = f
        .inbound_question(&node, &agent, Some(&node), &other_agent)
        .await?;
    let (status, answered) = send_message(
        &f.state,
        &member,
        "member-for-agent",
        serde_json::from_value(answer_for(&node, &agent, &q_for_agent))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(answered.delivery_state, DeliveryState::Delivered);

    f.close().await?;
    Ok(())
}

#[tokio::test]
async fn service_principal_cannot_hijack_human_question_and_bridge_preserves_provenance()
-> TestResult {
    let bridge = FakeBridge::new();
    let f = Fixture::new(bridge.clone()).await?;
    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    let other_agent = ParticipantId::generate()?;
    f.node(&node, &agent, "agent").await?;
    f.state
        .db
        .write_sqlite({
            let node = node.clone();
            let other_agent = other_agent.clone();
            move |tx| {
                tx.execute(
                    "INSERT INTO node_participants (node_id, participant_id, kind, name, state, synced_epoch, updated_at_ms)
                     VALUES (?1, ?2, 'agent', 'Other Agent', 'idle', 0, ?3)",
                    rusqlite::params![node.as_str(), other_agent.as_str(), now_ms()],
                )?;
                Ok(WriteOutcome::new(()))
            }
        })
        .await?;

    // Create a ServicePrincipal with Collaborate + Manage scopes and a participant_id.
    let sp_id = cooplet_protocol::ServicePrincipalId::generate()?;
    let sp_part = ParticipantId::generate()?;
    let sp_tok_id = TokenId::generate()?;
    let sp_tok = crate::credentials::mint_service(&f.state.master, &sp_id)?;
    let sp_verifier = sp_tok.verifier;
    f.state
        .db
        .write_sqlite({
            let sp_id = sp_id.clone();
            let sp_part = sp_part.clone();
            let sp_tok_id = sp_tok_id.clone();
            move |tx| {
                let now = now_ms();
                tx.execute(
                    "INSERT INTO principals (principal_id, kind, username, display_name,
                        participant_id, scopes_json, allowed_nodes_json, created_at_ms, updated_at_ms)
                     VALUES (?1, 'service', 'automation', 'Automation Bot', ?2, '[\"manage\"]', NULL, ?3, ?3)",
                    rusqlite::params![sp_id.as_str(), sp_part.as_str(), now],
                )?;
                tx.execute(
                    "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                        scopes_json, allowed_nodes_json, expires_at_ms, created_at_ms)
                     VALUES (?1, ?2, 'service_token', ?3, '[\"manage\"]', NULL, ?4, ?5)",
                    rusqlite::params![sp_tok_id.as_str(), sp_id.as_str(), sp_verifier, now + 60_000, now],
                )?;
                Ok(WriteOutcome::new(()))
            }
        })
        .await?;
    let service = authenticate(&f.state.reads, &f.state.master, sp_tok.secret.as_str()).await?;

    // 1. HumanAdmin message carries AuthorKind::Human + display_name on BridgeCommand::SendMessage.
    let (_, admin_msg) =
        send_message(&f.state, &f.admin, "prov-admin-msg", direct(&node, &agent)?).await?;
    assert!(matches!(
        bridge.shared.last_command.lock().map_err(|_| "poison")?.as_ref(),
        Some(BridgeCommand::SendMessage {
            sender_kind: AuthorKind::Human,
            sender_name,
            ..
        }) if sender_name == "Hub Admin"
    ));

    // 2. ServicePrincipal message carries AuthorKind::Service + display_name on BridgeCommand::SendMessage.
    let (_, service_msg) =
        send_message(&f.state, &service, "prov-sp-msg", direct(&node, &agent)?).await?;
    assert!(matches!(
        bridge.shared.last_command.lock().map_err(|_| "poison")?.as_ref(),
        Some(BridgeCommand::SendMessage {
            sender_kind: AuthorKind::Service,
            sender_name,
            ..
        }) if sender_name == "Automation Bot"
    ));

    // 3. ServicePrincipal with Manage CANNOT answer a human's ask_user question (403 Forbidden),
    //    and the human can still answer it afterwards.
    let admin_part = f
        .admin
        .participant_id()
        .ok_or("missing admin participant")?
        .clone();
    let q_for_human = f.inbound_question(&node, &agent, None, &admin_part).await?;
    result_code(
        send_message(
            &f.state,
            &service,
            "service-hijack-human-q",
            serde_json::from_value(answer_for(&node, &agent, &q_for_human))?,
        )
        .await,
        ErrorCode::Forbidden,
    )?;
    let (status, human_ans) = send_message(
        &f.state,
        &f.admin,
        "human-answers-own-q",
        serde_json::from_value(answer_for(&node, &agent, &q_for_human))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(human_ans.delivery_state, DeliveryState::Delivered);

    // 4. ServicePrincipal CAN answer a question addressed directly to itself.
    let q_for_service = f.inbound_question(&node, &agent, None, &sp_part).await?;
    let (status, sp_ans) = send_message(
        &f.state,
        &service,
        "service-answers-own-q",
        serde_json::from_value(answer_for(&node, &agent, &q_for_service))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(sp_ans.delivery_state, DeliveryState::Delivered);

    // 5. Non-answer in_reply_to enforces dialogue-party affinity:
    //    - ServicePrincipal cannot reply to Admin's dialogue -> 403 Forbidden.
    result_code(
        send_message(
            &f.state,
            &service,
            "service-reply-admin-dialogue",
            serde_json::from_value(json!({
                "target_node_id": node,
                "target_participant_id": agent,
                "in_reply_to": admin_msg.message_id,
                "body": "hijack reply"
            }))?,
        )
        .await,
        ErrorCode::Forbidden,
    )?;
    //    - Party cannot attach in_reply_to to a message sent to an unrelated third participant -> 400 InvalidRequest.
    result_code(
        send_message(
            &f.state,
            &service,
            "service-reply-wrong-target",
            serde_json::from_value(json!({
                "target_node_id": node,
                "target_participant_id": other_agent,
                "in_reply_to": service_msg.message_id,
                "body": "wrong recipient"
            }))?,
        )
        .await,
        ErrorCode::InvalidRequest,
    )?;
    //    - Original party replying in the same dialogue succeeds.
    let (status, valid_reply) = send_message(
        &f.state,
        &service,
        "service-reply-same-dialogue",
        serde_json::from_value(json!({
            "target_node_id": node,
            "target_participant_id": agent,
            "in_reply_to": service_msg.message_id,
            "body": "follow up"
        }))?,
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(valid_reply.in_reply_to, Some(service_msg.message_id));

    f.close().await?;
    Ok(())
}
