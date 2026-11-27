use super::*;
use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{
    EventId, ParticipantState, ThreadCreateRequest, ThreadMemberJoinRequest,
    ThreadMemberResumeRequest, ThreadPostReceipt, ThreadPostRequest, UserId,
};
use cooplet_storage::{DEFAULT_MAX_BLOB_BYTES, StorageEngine};
use futures_util::future::BoxFuture;
use rusqlite::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::{
    auth::{self, AuthenticatedCaller},
    bridge::{BridgeRegistry, CommandPermit},
    storage, threads,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[test]
fn queue_coalesces_once_idle_and_cannot_accept_an_old_generation_ack() -> TestResult {
    let mut db = Connection::open_in_memory()?;
    db.execute_batch(include_str!("../../storage/v1.sql"))?;
    let node = NodeId::generate()?;
    let hub = HubId::generate()?;
    let owner = UserId::generate()?;
    let participant = ParticipantId::generate()?;
    let thread = ThreadId::generate()?;
    let key = format!("{node}/{participant}");
    let tx = db.transaction()?;
    tx.execute(
        "INSERT INTO hub_meta VALUES(1,?1,'Hub','fingerprint',0,0,1,1)",
        [hub.as_str()],
    )?;
    tx.execute(
        "INSERT INTO principals(principal_id,kind,username,display_name,created_at_ms,updated_at_ms)
         VALUES(?1,'human_admin','owner','Owner',1,1)", [owner.as_str()],
    )?;
    tx.execute(
        "INSERT INTO nodes(node_id,display_name,owner_principal_id,credential_hmac,session_epoch,status,enrolled_at_ms)
         VALUES(?1,'Worker',?2,'credential',5,'online',1)", params![node.as_str(), owner.as_str()],
    )?;
    tx.execute(
        "INSERT INTO node_participants(node_id,participant_id,kind,name,state,synced_epoch,updated_at_ms)
         VALUES(?1,?2,'agent','Worker','working',5,1)", params![node.as_str(), participant.as_str()],
    )?;
    tx.execute(
        "INSERT INTO threads(thread_id,title,created_by_actor_id,status,head_seq,created_at_ms,updated_at_ms)
         VALUES(?1,'Dispatch',?2,'open',5,1,5)", params![thread.as_str(), owner.as_str()],
    )?;
    tx.execute(
        "INSERT INTO thread_members(thread_id,member_key,participant_id,node_id,handle,role,wake_policy,joined_at_ms)
         VALUES(?1,?2,?3,?4,'worker','member','mentions_only',1)",
        params![thread.as_str(), key, participant.as_str(), node.as_str()],
    )?;
    let worker_mention = serde_json::to_string(&[&key])?;
    for seq in 1..=5 {
        let mentions = if matches!(seq, 1 | 4) {
            worker_mention.as_str()
        } else {
            "[]"
        };
        tx.execute(
            "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,author_handle,
               body,mentions_json,created_at_ms,expires_at_ms)
             VALUES(?1,?2,?3,?4,'human','owner','post',?5,?2,100)",
            params![thread.as_str(), seq, cooplet_protocol::MessageId::generate()?.as_str(), owner.as_str(), mentions],
        )?;
    }
    let attempt = || {
        stage(
            &tx,
            &node,
            (5, 50),
            &Mode::Idle,
            None,
            &HashMap::new(),
            OperationId::generate()?,
        )
    };
    assert!(attempt()?.is_none());
    tx.execute("UPDATE node_participants SET state='idle'", [])?;
    let payload = attempt()?.ok_or("no delivery after idle")?;
    assert_eq!(
        (payload.from_seq, payload.to_seq, payload.posts.len()),
        (1, 5, 5)
    );
    assert!(attempt()?.is_none());
    let delivery = Delivery::from_payload(&node, 5, &payload);
    assert!(
        !finish(
            &tx,
            &hub,
            &Delivery {
                epoch: 4,
                ..delivery.clone()
            },
            ThreadDeliveryOutcome::Delivered,
            51
        )?
        .value
    );
    assert!(finish(&tx, &hub, &delivery, ThreadDeliveryOutcome::Unknown, 51)?.value);

    tx.execute(
        "UPDATE thread_members SET state='active',delivery_generation=1,confirmed_seq=5,
         pending_delivery_id=NULL,pending_from_seq=NULL,pending_to_seq=NULL",
        [],
    )?;
    assert!(!finish(&tx, &hub, &delivery, ThreadDeliveryOutcome::Delivered, 52)?.value);
    reconcile_fences(
        &tx,
        &hub,
        &node,
        &[ThreadFenceReport {
            thread_id: thread.clone(),
            participant_id: participant.clone(),
            delivery_generation: 0,
            highest_dispatched_seq: 5,
            last_delivery_id: Some(payload.delivery_id),
            last_from_seq: Some(1),
        }],
        52,
        &mut Vec::new(),
    )?;
    assert_eq!(
        members::member(&tx, &thread, &key)?
            .ok_or("member absent")?
            .state,
        cooplet_protocol::ThreadMemberState::Active
    );
    assert!(attempt()?.is_none());
    Ok(())
}

const TEST_EPOCH: u64 = 5;
const TEST_NOW: i64 = 1_800_000_000_000;
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

struct AttemptOnWire {
    payload: ThreadDeliverPayload,
    reply: oneshot::Sender<ThreadAckPayload>,
}

impl AttemptOnWire {
    fn complete(self, outcome: ThreadDeliveryOutcome) -> TestResult {
        let ThreadDeliverPayload {
            delivery_generation,
            delivery_id,
            thread_id,
            member_id,
            to_seq,
            ..
        } = self.payload;
        self.reply
            .send(ThreadAckPayload {
                delivery_generation,
                delivery_id,
                thread_id,
                member_id,
                acked_seq: to_seq,
                outcome,
                detail: None,
            })
            .map_err(|_| "scheduler dropped the ACK waiter".into())
    }
}

struct ControlledBridge {
    online: AtomicBool,
    free_slot: Arc<Semaphore>,
    sent: mpsc::UnboundedSender<AttemptOnWire>,
    saturated: mpsc::UnboundedSender<()>,
}

impl BridgeRegistry for ControlledBridge {
    fn reserve(&self, _: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        Err(ReserveError::Offline)
    }

    fn reserve_thread<'a>(
        &'a self,
        _: &'a NodeId,
    ) -> BoxFuture<'a, Result<Box<dyn ThreadPermit>, ReserveError>> {
        Box::pin(async move {
            if !self.online.load(Ordering::Acquire) {
                return Err(ReserveError::Offline);
            }
            if let Ok(slot) = self.free_slot.clone().try_acquire_owned() {
                return Ok(Box::new(ControlledPermit {
                    slot,
                    sent: self.sent.clone(),
                }) as Box<dyn ThreadPermit>);
            }
            let _ = self.saturated.send(());
            Err(ReserveError::RateLimited)
        })
    }

    fn disconnect_revoked(&self, _: &NodeId) {
        self.online.store(false, Ordering::Release);
    }
}

struct ControlledPermit {
    slot: OwnedSemaphorePermit,
    sent: mpsc::UnboundedSender<AttemptOnWire>,
}

impl ThreadPermit for ControlledPermit {
    fn epoch(&self) -> u64 {
        TEST_EPOCH
    }

    fn submit(
        self: Box<Self>,
        payload: ThreadDeliverPayload,
    ) -> BoxFuture<'static, Result<ThreadAckPayload, SubmitError>> {
        Box::pin(async move {
            let _slot = self.slot;
            let (reply, wait) = oneshot::channel();
            self.sent
                .send(AttemptOnWire { payload, reply })
                .map_err(|_| SubmitError::Unavailable)?;
            wait.await.map_err(|_| SubmitError::Unknown)
        })
    }
}

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    scratch: Scratch,
    engine: StorageEngine,
    shutdown: watch::Sender<()>,
    state: AppState,
    wall: Arc<AtomicI64>,
    admin: AuthenticatedCaller,
    bridge: Arc<ControlledBridge>,
    attempts: mpsc::UnboundedReceiver<AttemptOnWire>,
    saturated: mpsc::UnboundedReceiver<()>,
    node: NodeId,
    worker: ParticipantId,
}

impl Fixture {
    async fn new(online: bool) -> TestResult<Self> {
        let dir = std::env::temp_dir().join(EventId::generate()?.as_str());
        let scratch = Scratch(PrivateFs::ensure_private_dir(&dir)?);
        let init = storage::init(&dir, "Thread dispatch").await?;
        let admin_token =
            PrivateFs::read_private_file(&init.token_path.ok_or("missing admin token")?, 512)?;
        let storage = storage::open(
            &dir,
            DEFAULT_MAX_BLOB_BYTES,
            10 * 1024 * 1024 * 1024,
            1_048_576,
        )
        .await?;
        let (shutdown, signal) = watch::channel(());
        let (sent, attempts) = mpsc::unbounded_channel();
        let (saturated_send, saturated) = mpsc::unbounded_channel();
        let bridge = Arc::new(ControlledBridge {
            online: AtomicBool::new(online),
            free_slot: Arc::new(Semaphore::new(1)),
            sent,
            saturated: saturated_send,
        });
        let mut state = AppState::new(
            &storage.engine,
            storage.meta,
            storage.master,
            bridge.clone(),
            signal,
            DEFAULT_MAX_BLOB_BYTES,
        )?;
        let wall = Arc::new(AtomicI64::new(TEST_NOW));
        let injected = wall.clone();
        state.wall_clock = Arc::new(move || injected.load(Ordering::Relaxed));
        let admin = auth::authenticate(
            &state.reads,
            &state.master,
            std::str::from_utf8(&admin_token)?.trim(),
        )
        .await?;
        let node = NodeId::generate()?;
        let worker = ParticipantId::generate()?;
        let insert_node = node.clone();
        let insert_worker = worker.clone();
        let owner = admin.actor_id().to_owned();
        state.db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO nodes(node_id,display_name,owner_principal_id,credential_hmac,session_epoch,status,enrolled_at_ms)
                 VALUES(?1,'Worker',?2,'credential',?3,?4,1)",
                params![insert_node.as_str(), owner, TEST_EPOCH, if online { "online" } else { "offline" }],
            )?;
            tx.execute(
                "INSERT INTO node_participants(node_id,participant_id,kind,name,state,synced_epoch,updated_at_ms)
                 VALUES(?1,?2,'agent','Worker','working',?3,1)",
                params![insert_node.as_str(), insert_worker.as_str(), TEST_EPOCH],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(Self {
            scratch,
            engine: storage.engine,
            state,
            wall,
            admin,
            shutdown,
            bridge,
            attempts,
            saturated,
            node,
            worker,
        })
    }

    async fn board(&self, mode: DeliveryMode) -> TestResult<ThreadId> {
        let created = threads::create(
            &self.state,
            &self.admin,
            "create-test-thread",
            ThreadCreateRequest {
                title: "Review regression".into(),
                topic: None,
                creator_handle: None,
                members: vec![ThreadMemberJoinRequest {
                    member_key: self.member_key(),
                    handle: Some("worker".into()),
                    role: None,
                    wake_policy: None,
                    delivery_mode: Some(mode),
                }],
            },
        )
        .await?;
        Ok(created.thread_id)
    }

    fn member_key(&self) -> String {
        format!("{}/{}", self.node, self.worker)
    }

    async fn resume_through(&self, thread: &ThreadId) -> TestResult<Option<u64>> {
        let thread = thread.clone();
        let key = self.member_key();
        Ok(self.state.reads.read(move |db| {
            Ok(db.query_row(
                "SELECT resume_through_seq FROM thread_members WHERE thread_id=?1 AND member_key=?2",
                params![thread.as_str(), key], |row| row.get(0),
            )?)
        }).await?)
    }

    async fn post(
        &self,
        thread: &ThreadId,
        key: &str,
        body: &str,
    ) -> TestResult<ThreadPostReceipt> {
        Ok(threads::post(
            &self.state,
            &self.admin,
            thread.clone(),
            key,
            ThreadPostRequest {
                body: body.into(),
                mentions: Vec::new(),
                attachments: Vec::new(),
                question: None,
                answer: None,
                in_reply_to: None,
            },
        )
        .await?)
    }

    async fn set_worker(&self, state: ParticipantState) -> TestResult {
        let node = self.node.clone();
        let participant = self.worker.clone();
        self.state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE node_participants SET state=?1 WHERE node_id=?2 AND participant_id=?3",
                    params![state.as_str(), node.as_str(), participant.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        if state == ParticipantState::Idle {
            wake_participant(&self.state, &self.node, &self.worker);
        }
        Ok(())
    }

    async fn reconnect_working(&self) -> TestResult {
        let node = self.node.clone();
        self.state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE nodes SET status='online' WHERE node_id=?1",
                    [node.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        self.bridge.online.store(true, Ordering::Release);
        wake(&self.state, &self.node);
        Ok(())
    }

    async fn attempt(&mut self) -> TestResult<AttemptOnWire> {
        Ok(tokio::time::timeout(STEP_TIMEOUT, self.attempts.recv())
            .await?
            .ok_or("wire receiver closed")?)
    }

    async fn blocked_scan(&mut self) -> TestResult {
        tokio::time::timeout(STEP_TIMEOUT, self.saturated.recv())
            .await?
            .ok_or("scheduler was not blocked by its in-flight slot")?;
        Ok(())
    }

    async fn settle(&mut self) -> TestResult {
        tokio::time::timeout(STEP_TIMEOUT, self.state.background.drain()).await?;
        assert!(matches!(
            self.attempts.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        Ok(())
    }

    async fn close(self) -> TestResult {
        let Self {
            scratch,
            engine,
            state,
            shutdown,
            ..
        } = self;
        let _ = shutdown.send(());
        drop(state);
        engine.shutdown().await?;
        drop(scratch);
        Ok(())
    }
}

#[tokio::test]
async fn idle_before_busy_ack_retries_once_for_the_affected_participant() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let board = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture
        .post(&board, "busy-regression-first", "@worker please review")
        .await?;
    fixture
        .post(&board, "busy-regression-tail", "Here is the full context")
        .await?;
    fixture.settle().await?;

    fixture.set_worker(ParticipantState::Idle).await?;
    let first = fixture.attempt().await?;
    assert_eq!((first.payload.from_seq, first.payload.to_seq), (1, 2));
    fixture.blocked_scan().await?;

    // The scheduler has already handled this projection before the first ACK arrives.
    fixture.set_worker(ParticipantState::Working).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.blocked_scan().await?;
    first.complete(ThreadDeliveryOutcome::Busy)?;

    let second = fixture.attempt().await?;
    assert_eq!((second.payload.from_seq, second.payload.to_seq), (1, 2));
    fixture.blocked_scan().await?;
    wake_participant(&fixture.state, &fixture.node, &ParticipantId::generate()?);
    fixture.blocked_scan().await?;
    second.complete(ThreadDeliveryOutcome::AwaitingApproval)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn offline_direct_resume_delivers_once_after_idle_and_preserves_normal_direct_mode()
-> TestResult {
    let mut fixture = Fixture::new(false).await?;
    let board = fixture.board(DeliveryMode::DirectNow).await?;
    fixture
        .post(&board, "resume-regression-first", "@worker please review")
        .await?;
    fixture
        .post(&board, "resume-regression-context", "First context")
        .await?;
    let key = fixture.member_key();
    let disputed = OperationId::generate()?;
    let write_board = board.clone();
    let write_key = key.clone();
    fixture.state.db.write_sqlite(move |tx| {
        tx.execute(
            "UPDATE thread_members SET state='paused_unknown',
             pending_delivery_id=?1,pending_from_seq=1,pending_to_seq=2 WHERE thread_id=?2 AND member_key=?3",
            params![disputed.as_str(), write_board.as_str(), write_key],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;

    let resumed = threads::resume(
        &fixture.state,
        &fixture.admin,
        board.clone(),
        "resume-regression-explicit",
        ThreadMemberResumeRequest {
            member_key: Some(key.clone()),
            after_seq: 0,
        },
    )
    .await?;
    assert_eq!(resumed.delivery_generation, 1);
    assert!(resumed.pending.is_none());
    fixture.settle().await?;

    fixture.reconnect_working().await?;
    fixture.settle().await?;
    fixture
        .post(
            &board,
            "resume-regression-later",
            "@worker has more context",
        )
        .await?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    let recovery = fixture.attempt().await?;
    assert_eq!(recovery.payload.delivery_generation, 1);
    assert_eq!(recovery.payload.delivery_mode, DeliveryMode::DirectNow);
    assert_eq!((recovery.payload.from_seq, recovery.payload.to_seq), (1, 3));
    recovery.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;

    fixture.set_worker(ParticipantState::Working).await?;
    fixture
        .post(&board, "resume-regression-normal", "@worker check once")
        .await?;
    let direct = fixture.attempt().await?;
    assert_eq!((direct.payload.from_seq, direct.payload.to_seq), (4, 4));
    direct.complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;

    assert_eq!(fixture.resume_through(&board).await?, None);
    fixture.close().await
}

#[tokio::test]
async fn deferred_direct_wake_survives_expiry_of_the_original_recovery_range() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::DirectNow).await?;
    let first = fixture
        .post(&thread, "expiry-recovery-first", "@worker first")
        .await?;
    fixture
        .attempt()
        .await?
        .complete(ThreadDeliveryOutcome::Unknown)?;
    fixture.settle().await?;

    let expiry = first.created_at_ms + threads::RETENTION_MS;
    fixture.wall.store(expiry - 1, Ordering::Relaxed);
    let resumed = threads::resume(
        &fixture.state,
        &fixture.admin,
        thread.clone(),
        "expiry-recovery-resume",
        ThreadMemberResumeRequest {
            member_key: Some(fixture.member_key()),
            after_seq: 0,
        },
    )
    .await?;
    assert_eq!(resumed.delivery_generation, 1);
    assert_eq!(fixture.resume_through(&thread).await?, Some(1));

    fixture
        .post(&thread, "expiry-recovery-context", "More context")
        .await?;
    fixture.settle().await?;
    assert_eq!(fixture.resume_through(&thread).await?, Some(1));
    let deferred = fixture
        .post(&thread, "expiry-recovery-deferred", "@worker new request")
        .await?;
    fixture.settle().await?;
    assert_eq!(fixture.resume_through(&thread).await?, Some(deferred.seq));

    fixture.wall.store(expiry + 1, Ordering::Relaxed);
    assert_eq!(
        threads::get(&fixture.state, &fixture.admin, thread.clone())
            .await?
            .floor_seq,
        1
    );
    fixture.set_worker(ParticipantState::Idle).await?;
    let recovered = fixture.attempt().await?;
    assert_eq!(
        (recovered.payload.from_seq, recovered.payload.to_seq),
        (1, deferred.seq)
    );
    assert_eq!(recovered.payload.delivery_generation, 1);
    assert!(recovered.payload.truncated_earlier);
    assert_eq!(
        recovered
            .payload
            .posts
            .iter()
            .map(|post| post.seq)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    recovered.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    assert_eq!(fixture.resume_through(&thread).await?, None);
    fixture.close().await
}
