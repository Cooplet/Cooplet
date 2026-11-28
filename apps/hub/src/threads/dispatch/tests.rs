use super::*;
use std::{
    error::Error,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::{
    DeliveryMode, EventId, EventKind, HubId, ParticipantState, ThreadAckPayload,
    ThreadCreateRequest, ThreadFenceReport, ThreadMemberJoinRequest, ThreadMemberResumeRequest,
    ThreadPostReceipt, ThreadPostRequest, UserId,
};
use cooplet_storage::{DEFAULT_MAX_BLOB_BYTES, StorageEngine};
use futures_util::future::BoxFuture;
use rusqlite::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::{
    auth::{self, AuthenticatedCaller},
    bridge::{BridgeRegistry, CommandPermit},
    storage,
    threads::{self, members},
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
        "INSERT INTO thread_members(thread_id,member_key,participant_id,node_id,handle,role,wake_policy,last_wake_seq,joined_at_ms)
         VALUES(?1,?2,?3,?4,'worker','member','mentions_only',4,1)",
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
    let empty_retries = RetryMap::new();
    let attempt = || {
        stage(
            &tx,
            &node,
            (5, 50),
            None,
            |c, e, g, w| retry_number(&empty_retries, c, e, g, w),
            &[],
            OperationId::generate()?,
        )
    };
    assert!(matches!(attempt()?, StageResult::None));
    tx.execute("UPDATE node_participants SET state='idle'", [])?;
    let StageResult::Ready(payload, _) = attempt()? else {
        return Err("no delivery after idle".into());
    };
    assert_eq!(
        (payload.from_seq, payload.to_seq, payload.posts.len()),
        (1, 5, 5)
    );
    assert!(matches!(attempt()?, StageResult::None));
    let delivery = Delivery::from_payload(&node, 5, &payload, true);
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
    assert!(matches!(attempt()?, StageResult::None));
    Ok(())
}

const TEST_EPOCH: u64 = 5;
const TEST_NOW: i64 = 1_800_000_000_000;
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::test]
async fn queue_wake_is_recomputed_after_a_policy_change_or_resume() -> TestResult {
    use cooplet_protocol::{ThreadWaitPolicy, ThreadWaitRequest, ThreadWaitResponse};

    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture
        .post(&thread, "ordinary-update", "An ordinary update")
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;

    let node = fixture.node.clone();
    let worker = fixture.worker.clone();
    let actor = storage::sql::read_tx(&fixture.state.reads, move |db| {
        auth::attest_node_participant(db, &node, &worker, Some((1, TEST_EPOCH)))
    })
    .await?;
    let caught_up = threads::wait(
        &fixture.state,
        &actor,
        thread.clone(),
        ThreadWaitRequest {
            since_seq: 0,
            wake_policy: Some(ThreadWaitPolicy::All),
        },
    )
    .await?;
    assert!(matches!(
        caught_up,
        ThreadWaitResponse::Ready { head_seq: 1, .. }
    ));
    let first = fixture.attempt().await?;
    assert_eq!((first.payload.from_seq, first.payload.to_seq), (1, 1));
    first.complete(ThreadDeliveryOutcome::Unknown)?;
    fixture.settle().await?;

    fixture
        .post(&thread, "paused-update", "Another update")
        .await?;
    fixture.settle().await?;
    threads::resume(
        &fixture.state,
        &fixture.admin,
        thread,
        "resume-queued-member",
        ThreadMemberResumeRequest {
            member_key: Some(fixture.member_key()),
            after_seq: 1,
        },
    )
    .await?;
    let resumed = fixture.attempt().await?;
    assert_eq!((resumed.payload.from_seq, resumed.payload.to_seq), (2, 2));
    resumed.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

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
    transient_disconnects: AtomicUsize,
}

impl BridgeRegistry for ControlledBridge {
    fn reserve(&self, _: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        Err(ReserveError::Offline)
    }

    fn reserve_thread<'a>(
        &'a self,
        _: &'a NodeId,
        wait_for_slot: bool,
    ) -> BoxFuture<'a, Result<Box<dyn ThreadPermit>, ReserveError>> {
        Box::pin(async move {
            if !self.online.load(Ordering::Acquire) {
                return Err(ReserveError::Offline);
            }
            let slot = match self.free_slot.clone().try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => {
                    let _ = self.saturated.send(());
                    if !wait_for_slot {
                        return Err(ReserveError::RateLimited);
                    }
                    self.free_slot
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| ReserveError::Offline)?
                }
            };
            Ok(Box::new(ControlledPermit {
                slot,
                sent: self.sent.clone(),
            }) as Box<dyn ThreadPermit>)
        })
    }

    fn disconnect_revoked(&self, _: &NodeId) {
        self.online.store(false, Ordering::Release);
    }

    fn disconnect_transient(&self, _: &NodeId) {
        self.transient_disconnects.fetch_add(1, Ordering::SeqCst);
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
            transient_disconnects: AtomicUsize::new(0),
        });
        let mut state = AppState::new(
            &storage.engine,
            storage.meta,
            storage.master,
            storage.thread_acl_watch,
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
            EventId::generate()?.as_str(),
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

    async fn add_peer(&self, board: &ThreadId, mode: DeliveryMode) -> TestResult<ParticipantId> {
        let peer = ParticipantId::generate()?;
        let node = self.node.clone();
        let participant = peer.clone();
        self.state.db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO node_participants(node_id,participant_id,kind,name,state,synced_epoch,updated_at_ms)
                 VALUES(?1,?2,'agent','Peer','idle',?3,1)",
                params![node.as_str(), participant.as_str(), TEST_EPOCH],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        threads::join(
            &self.state,
            &self.admin,
            board.clone(),
            "join-peer",
            ThreadMemberJoinRequest {
                member_key: format!("{}/{}", self.node, peer),
                handle: Some("peer".into()),
                role: None,
                wake_policy: None,
                delivery_mode: Some(mode),
            },
        )
        .await?;
        Ok(peer)
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

    async fn arm_direct_recovery(&mut self, thread: &ThreadId) -> TestResult<i64> {
        let disputed = self
            .post(thread, "expiry-recovery-first", "@worker first")
            .await?;
        self.attempt()
            .await?
            .complete(ThreadDeliveryOutcome::Unknown)?;
        self.settle().await?;

        let expiry = disputed.created_at_ms + threads::RETENTION_MS;
        self.wall.store(expiry - 1, Ordering::Relaxed);
        let resumed = threads::resume(
            &self.state,
            &self.admin,
            thread.clone(),
            "expiry-recovery-resume",
            ThreadMemberResumeRequest {
                member_key: Some(self.member_key()),
                after_seq: 0,
            },
        )
        .await?;
        assert_eq!(resumed.delivery_generation, 1);
        assert_eq!(self.resume_through(thread).await?, Some(1));
        self.settle().await?;
        Ok(expiry)
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
                if state == ParticipantState::Idle {
                    participant_idle(tx, &node, &participant)?;
                }
                Ok(WriteOutcome::new(()))
            })
            .await?;
        if state == ParticipantState::Idle {
            on_participants_idle(&self.state, &self.node, std::slice::from_ref(&self.worker));
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
async fn direct_fanout_waits_for_bridge_capacity_but_attempts_each_recipient_once() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let board = fixture.board(DeliveryMode::DirectNow).await?;
    let peer = fixture.add_peer(&board, DeliveryMode::DirectNow).await?;
    let occupied = fixture.bridge.free_slot.clone().try_acquire_owned()?;
    fixture.post(&board, "fanout-direct", "@all review").await?;
    fixture.blocked_scan().await?;
    drop(occupied);

    let first = fixture.attempt().await?;
    let first_member = first.payload.member_id.clone();
    first.complete(ThreadDeliveryOutcome::Busy)?;
    let second = fixture.attempt().await?;
    assert_ne!(first_member, second.payload.member_id);
    assert!([first_member, second.payload.member_id.clone()].contains(&peer));
    second.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn a_rejection_retires_only_covered_work_and_releases_the_slot_for_other_boards() -> TestResult
{
    let mut fixture = Fixture::new(true).await?;
    let mut boards = [
        fixture.board(DeliveryMode::QueueUntilIdle).await?,
        fixture.board(DeliveryMode::QueueUntilIdle).await?,
    ];
    boards.sort();
    let [earlier, later] = boards;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;

    fixture
        .post(&later, "rejected-range", "@worker later board")
        .await?;
    let rejected = fixture.attempt().await?;
    fixture
        .post(&earlier, "waiting-board", "@worker earlier board")
        .await?;
    rejected.complete(ThreadDeliveryOutcome::Rejected)?;
    let next = fixture.attempt().await?;
    assert_eq!(next.payload.thread_id, earlier);
    next.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture
        .post(&later, "unrelated-after-rejection", "Just context")
        .await?;
    fixture.settle().await?;

    let node = fixture.node.clone();
    let worker = fixture.worker.clone();
    let caller = storage::sql::read_tx(&fixture.state.reads, move |db| {
        auth::attest_node_participant(db, &node, &worker, Some((1, TEST_EPOCH)))
    })
    .await?;
    let waiting = threads::wait(
        &fixture.state,
        &caller,
        later.clone(),
        cooplet_protocol::ThreadWaitRequest {
            since_seq: 0,
            wake_policy: None,
        },
    )
    .await?;
    assert!(matches!(
        waiting,
        cooplet_protocol::ThreadWaitResponse::Ready { .. }
    ));
    fixture.settle().await?;

    fixture
        .post(&later, "fresh-after-rejection", "@worker new request")
        .await?;
    let pending = fixture.attempt().await?;
    assert_eq!((pending.payload.from_seq, pending.payload.to_seq), (1, 3));
    fixture
        .post(&later, "while-rejection-pending", "@worker one more")
        .await?;
    pending.complete(ThreadDeliveryOutcome::Rejected)?;
    let newer = fixture.attempt().await?;
    assert_eq!((newer.payload.from_seq, newer.payload.to_seq), (1, 4));
    newer.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn a_quiet_post_skips_confirmed_busy_and_expired_queue_wakes() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let board = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture
        .post(&board, "queue-addressed", "@worker review")
        .await?;
    fixture
        .attempt()
        .await?
        .complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;

    // Holding every bridge slot makes even an empty scheduler pass observable.
    let occupied = fixture.bridge.free_slot.clone().try_acquire_owned()?;
    fixture
        .post(&board, "quiet-confirmed", "Progress update")
        .await?;
    fixture.settle().await?;
    drop(occupied);

    fixture
        .post(&board, "queue-addressed-again", "@worker review again")
        .await?;
    fixture
        .attempt()
        .await?
        .complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    let occupied = fixture.bridge.free_slot.clone().try_acquire_owned()?;
    fixture
        .post(&board, "quiet-while-busy", "More context")
        .await?;
    fixture.settle().await?;
    drop(occupied);

    fixture.set_worker(ParticipantState::Idle).await?;
    let retry = fixture.attempt().await?;
    assert_eq!(retry.payload.to_seq, 4);
    retry.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;

    fixture.set_worker(ParticipantState::Working).await?;
    let post = fixture
        .post(&board, "expiring-wake", "@worker later")
        .await?;
    fixture.wall.store(
        post.created_at_ms + threads::RETENTION_MS,
        Ordering::Relaxed,
    );
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    let occupied = fixture.bridge.free_slot.clone().try_acquire_owned()?;
    fixture
        .post(&board, "quiet-after-expiry", "More context")
        .await?;
    fixture.settle().await?;
    drop(occupied);
    fixture.close().await
}

#[tokio::test]
async fn a_post_using_the_last_write_slot_still_wakes_its_node() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.state.background = crate::BackgroundWrites::new(1);
    let board = fixture.board(DeliveryMode::DirectNow).await?;
    fixture
        .post(&board, "last-write-slot", "@worker review")
        .await?;
    let attempt = fixture.attempt().await?;
    assert_eq!(attempt.payload.to_seq, 1);
    attempt.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn direct_posts_accepted_during_an_attempt_each_get_one_ordered_turn() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::DirectNow).await?;
    fixture
        .post(&thread, "post-initial", "@worker first")
        .await?;
    let first = fixture.attempt().await?;
    assert_eq!((first.payload.from_seq, first.payload.to_seq), (1, 1));
    fixture
        .post(&thread, "post-second", "@worker second")
        .await?;
    fixture
        .post(&thread, "post-context", "Unaddressed context")
        .await?;
    fixture.post(&thread, "post-third", "@worker third").await?;
    first.complete(ThreadDeliveryOutcome::Delivered)?;
    let second = fixture.attempt().await?;
    assert_eq!((second.payload.from_seq, second.payload.to_seq), (2, 2));
    second.complete(ThreadDeliveryOutcome::Busy)?;
    let third = fixture.attempt().await?;
    assert_eq!((third.payload.from_seq, third.payload.to_seq), (2, 4));
    third.complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture
        .post(&thread, "post-quiet", "No new direct wake")
        .await?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn direct_wakes_end_at_unknown_or_at_the_end_of_the_online_session() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::DirectNow).await?;
    fixture.post(&thread, "uncertain", "@worker first").await?;
    let first = fixture.attempt().await?;
    fixture
        .post(&thread, "before-unknown", "@worker second")
        .await?;
    first.complete(ThreadDeliveryOutcome::Unknown)?;
    fixture.settle().await?;
    threads::resume(
        &fixture.state,
        &fixture.admin,
        thread.clone(),
        "skip-uncertain-generation",
        ThreadMemberResumeRequest {
            member_key: Some(fixture.member_key()),
            after_seq: 2,
        },
    )
    .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;

    let occupied = fixture.bridge.free_slot.clone().try_acquire_owned()?;
    fixture
        .post(&thread, "before-reconnect", "@worker third")
        .await?;
    let node = fixture.node.clone();
    let hub = fixture.state.meta.hub_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            pause_node_pending(tx, &hub, &node, TEST_NOW, &mut Vec::new())?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    drop(occupied);
    fixture.settle().await?;
    fixture
        .post(&thread, "fresh-post", "@worker after reconnect")
        .await?;
    let fresh = fixture.attempt().await?;
    assert_eq!((fresh.payload.from_seq, fresh.payload.to_seq), (3, 4));
    assert_eq!(fresh.payload.delivery_generation, 1);
    fresh.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn busy_and_awaiting_approval_wait_for_the_next_matching_idle_after_the_worker_exits()
-> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.post(&thread, "post-first", "@worker first").await?;
    fixture
        .attempt()
        .await?
        .complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    fixture
        .post(&thread, "post-context", "Unaddressed context")
        .await?;
    fixture
        .post(&thread, "post-second", "@worker second")
        .await?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    let retry = fixture.attempt().await?;
    assert_eq!((retry.payload.from_seq, retry.payload.to_seq), (1, 3));
    retry.complete(ThreadDeliveryOutcome::AwaitingApproval)?;
    fixture.settle().await?;
    fixture
        .post(&thread, "after-approval", "More context")
        .await?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    let resumed = fixture.attempt().await?;
    assert_eq!((resumed.payload.from_seq, resumed.payload.to_seq), (1, 4));
    resumed.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn a_busy_participant_waits_across_boards_while_its_peer_can_receive() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let one = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let two = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let peer = fixture.add_peer(&two, DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.post(&one, "first-board", "@worker review").await?;
    fixture
        .attempt()
        .await?
        .complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    fixture.post(&two, "second-board", "@all review").await?;
    let peer_turn = fixture.attempt().await?;
    assert_eq!(peer_turn.payload.member_id, peer);
    peer_turn.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    let a = fixture.attempt().await?;
    assert_eq!(a.payload.member_id, fixture.worker);
    let first_board = a.payload.thread_id.clone();
    a.complete(ThreadDeliveryOutcome::Delivered)?;
    let b = fixture.attempt().await?;
    assert_eq!(b.payload.member_id, fixture.worker);
    assert_ne!(first_board, b.payload.thread_id);
    b.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn leaving_during_an_idle_turn_releases_only_its_own_gate() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let one = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let two = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.post(&one, "before-leave", "@worker review").await?;
    let original = fixture.attempt().await?;
    fixture.post(&two, "other-board", "@worker second").await?;
    threads::leave(
        &fixture.state,
        &fixture.admin,
        one,
        "leave-during-delivery",
        cooplet_protocol::ThreadMemberLeaveRequest {
            member_key: Some(fixture.member_key()),
        },
    )
    .await?;
    // The session cancels the old waiter once the membership changes.
    let resumed = fixture.attempt().await?;
    assert_eq!(resumed.payload.thread_id, two);
    assert!(original.complete(ThreadDeliveryOutcome::Busy).is_err());
    resumed.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn new_posts_do_not_restart_a_saturated_idle_sweep_at_the_first_member() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let board = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let peer = fixture
        .add_peer(&board, DeliveryMode::QueueUntilIdle)
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture.post(&board, "fanout-idle", "@all review").await?;
    let first = fixture.attempt().await?;
    fixture.blocked_scan().await?;
    let first_member = first.payload.member_id.clone();
    let next_post = if first_member == peer {
        "@peer"
    } else {
        "@worker"
    };
    fixture.post(&board, "wake-first-again", next_post).await?;
    fixture.blocked_scan().await?;
    first.complete(ThreadDeliveryOutcome::Delivered)?;

    let second = fixture.attempt().await?;
    assert_ne!(first_member, second.payload.member_id);
    second.complete(ThreadDeliveryOutcome::Delivered)?;
    let follow_up = fixture.attempt().await?;
    assert_eq!(follow_up.payload.member_id, first_member);
    assert_eq!(
        (follow_up.payload.from_seq, follow_up.payload.to_seq),
        (2, 2)
    );
    follow_up.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn an_idle_participant_gets_one_pending_turn_across_boards() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let one = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let two = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.post(&one, "one-board", "@worker review").await?;
    let first = fixture.attempt().await?;
    fixture
        .post(&two, "two-boards", "@worker follow up")
        .await?;
    // Exercise staging even if another bridge slot were available now.
    let node = fixture.node.clone();
    let overlap = fixture
        .state
        .db
        .write(move |tx| {
            let empty_retries = RetryMap::new();
            let job = stage(
                tx,
                &node,
                (TEST_EPOCH, TEST_NOW),
                None,
                |c, e, g, w| retry_number(&empty_retries, c, e, g, w),
                &[],
                OperationId::generate().map_err(HubError::from)?,
            )?;
            Ok::<_, WriteError<HubError>>(WriteOutcome::new(job))
        })
        .await
        .map_err(HubError::from)?;
    assert!(matches!(overlap, StageResult::None));
    first.complete(ThreadDeliveryOutcome::Delivered)?;
    let next = fixture.attempt().await?;
    assert_eq!(next.payload.thread_id, two);
    next.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
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
    wake(&fixture.state, &fixture.node);
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
    let first = fixture
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

    let expiry = first.created_at_ms + threads::RETENTION_MS;
    fixture.wall.store(expiry - 1, Ordering::Relaxed);
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

    fixture
        .post(
            &board,
            "resume-regression-later",
            "@worker has more context",
        )
        .await?;
    fixture.settle().await?;
    assert_eq!(fixture.resume_through(&board).await?, Some(3));
    fixture.wall.store(expiry + 1, Ordering::Relaxed);
    fixture.reconnect_working().await?;
    fixture.settle().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    let recovery = fixture.attempt().await?;
    assert_eq!(recovery.payload.delivery_generation, 1);
    assert_eq!(recovery.payload.delivery_mode, DeliveryMode::DirectNow);
    assert_eq!((recovery.payload.from_seq, recovery.payload.to_seq), (1, 3));
    assert!(recovery.payload.truncated_earlier);
    assert_eq!(recovery.payload.posts.len(), 1);
    assert_eq!(recovery.payload.posts[0].seq, 3);
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
    let expiry = fixture.arm_direct_recovery(&thread).await?;

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

#[tokio::test]
async fn ack_for_a_recovery_prefix_preserves_a_newer_direct_wake() -> TestResult {
    use ThreadDeliveryOutcome as Outcome;

    for (outcome, expected_from, expected_posts) in [
        (Outcome::Busy, 1, &[2_u64][..]),
        (Outcome::Delivered, 2, &[2][..]),
        (Outcome::Rejected, 1, &[1, 2][..]),
    ] {
        let mut fixture = Fixture::new(true).await?;
        let thread = fixture.board(DeliveryMode::DirectNow).await?;
        let expiry = fixture.arm_direct_recovery(&thread).await?;

        fixture.set_worker(ParticipantState::Idle).await?;
        let prefix = fixture.attempt().await?;
        assert_eq!((prefix.payload.from_seq, prefix.payload.to_seq), (1, 1));
        fixture.set_worker(ParticipantState::Working).await?;
        fixture
            .post(&thread, "pending-recovery-tail", "@worker new request")
            .await?;
        assert_eq!(fixture.resume_through(&thread).await?, Some(2));
        prefix.complete(outcome)?;
        fixture.settle().await?;
        assert_eq!(fixture.resume_through(&thread).await?, Some(2));

        if outcome == Outcome::Busy {
            fixture.wall.store(expiry + 1, Ordering::Relaxed);
        }
        fixture.set_worker(ParticipantState::Idle).await?;
        let tail = fixture.attempt().await?;
        assert_eq!(
            (tail.payload.from_seq, tail.payload.to_seq),
            (expected_from, 2)
        );
        assert_eq!(tail.payload.delivery_generation, 1);
        assert_eq!(tail.payload.truncated_earlier, outcome == Outcome::Busy);
        assert_eq!(
            tail.payload
                .posts
                .iter()
                .map(|post| post.seq)
                .collect::<Vec<_>>()
                .as_slice(),
            expected_posts
        );
        tail.complete(Outcome::Delivered)?;
        fixture.settle().await?;
        assert_eq!(fixture.resume_through(&thread).await?, None);
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test]
async fn direct_follow_up_turns_use_the_free_slot_while_a_peer_ack_is_outstanding() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.bridge.free_slot.add_permits(1);
    let thread = fixture.board(DeliveryMode::DirectNow).await?;
    let peer = fixture.add_peer(&thread, DeliveryMode::DirectNow).await?;
    fixture.post(&thread, "fanout-start", "@all review").await?;
    let first = fixture.attempt().await?;
    let other = fixture.attempt().await?;
    assert_ne!(first.payload.member_id, other.payload.member_id);
    fixture.blocked_scan().await?;

    let member = first.payload.member_id.clone();
    let mention = if member == peer { "@peer" } else { "@worker" };
    fixture.post(&thread, "fanout-follow-1", mention).await?;
    fixture.blocked_scan().await?;
    fixture.post(&thread, "fanout-follow-2", mention).await?;
    fixture.blocked_scan().await?;
    first.complete(ThreadDeliveryOutcome::Delivered)?;

    for seq in [2, 3] {
        let follow_up = fixture.attempt().await?;
        assert_eq!(follow_up.payload.member_id, member);
        assert_eq!(
            (follow_up.payload.from_seq, follow_up.payload.to_seq),
            (seq, seq)
        );
        follow_up.complete(ThreadDeliveryOutcome::Delivered)?;
    }
    other.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn transient_retry_runs_after_backoff_while_a_peer_ack_is_outstanding() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.bridge.free_slot.add_permits(1);
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture
        .add_peer(&thread, DeliveryMode::QueueUntilIdle)
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture.post(&thread, "fanout-retry", "@all review").await?;
    let first = fixture.attempt().await?;
    let other = fixture.attempt().await?;
    assert_ne!(first.payload.member_id, other.payload.member_id);
    fixture.blocked_scan().await?;

    let member = first.payload.member_id.clone();
    let start = tokio::time::Instant::now();
    first.complete(ThreadDeliveryOutcome::Unavailable)?;
    let retry = fixture.attempt().await?;
    assert!(start.elapsed() >= INITIAL_RETRY);
    assert_eq!(retry.payload.member_id, member);
    assert_eq!((retry.payload.from_seq, retry.payload.to_seq), (1, 1));
    retry.complete(ThreadDeliveryOutcome::Delivered)?;
    other.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test(start_paused = true)]
async fn retry_clocks_and_limits_belong_to_the_delivery_address() -> TestResult {
    let thread = ThreadId::generate()?;
    let alpha = Candidate {
        member_id: ParticipantId::generate()?,
        thread_id: thread.clone(),
    };
    let beta = Candidate {
        member_id: ParticipantId::generate()?,
        thread_id: thread,
    };
    let mut sweep = Sweep::new(INITIAL_RETRY);
    let attempt = |number| RetryAttempt {
        epoch: TEST_EPOCH,
        generation: 2,
        wake_seq: 3,
        number,
    };
    for number in 0..=RETRIES {
        let retry = attempt(number);
        assert_eq!(
            retry_number(&sweep.retries, &alpha, TEST_EPOCH, 2, 3),
            Some(number)
        );
        Arc::make_mut(&mut sweep.retries).insert(
            alpha.clone(),
            (retry, RetryPhase::InFlight { idle_reset: false }),
        );
        let alpha_copy = alpha.clone();
        sweep
            .pending
            .spawn(async move { (alpha_copy, Some(retry), Completion::Transient) });
        // A newly committed post cannot slip between the DB finish and its JoinSet outcome.
        assert_eq!(retry_number(&sweep.retries, &alpha, TEST_EPOCH, 2, 4), None);
        assert!(sweep.next(false).await);
        if number == RETRIES {
            assert!(sweep.deadline().is_none());
            break;
        }

        let due = Instant::now() + retry_delay(INITIAL_RETRY, number);
        assert_eq!(sweep.deadline(), Some(due));
        assert_eq!(
            retry_number(&sweep.retries, &beta, TEST_EPOCH, 2, 3),
            Some(0)
        );
        // A replacement generation or session need not wait for the old address.
        assert_eq!(
            retry_number(&sweep.retries, &alpha, TEST_EPOCH, 3, 3),
            Some(0)
        );
        assert_eq!(
            retry_number(&sweep.retries, &alpha, TEST_EPOCH + 1, 2, 3),
            Some(0)
        );
        tokio::time::advance(retry_delay(INITIAL_RETRY, number) - Duration::from_millis(1)).await;
        sweep.release_due(Instant::now());
        assert_eq!(retry_number(&sweep.retries, &alpha, TEST_EPOCH, 2, 3), None);
        tokio::time::advance(Duration::from_millis(1)).await;
        sweep.release_due(Instant::now());
    }
    let retry = attempt(0);
    Arc::make_mut(&mut sweep.retries).insert(
        beta.clone(),
        (retry, RetryPhase::InFlight { idle_reset: false }),
    );
    sweep
        .pending
        .spawn(async move { (beta, Some(retry), Completion::Transient) });
    assert!(sweep.next(false).await);
    assert_eq!(sweep.deadline(), Some(Instant::now() + INITIAL_RETRY));
    Ok(())
}

#[tokio::test]
async fn peer_ack_cannot_shorten_a_live_queue_backoff() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.bridge.free_slot.add_permits(1);
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture
        .add_peer(&thread, DeliveryMode::QueueUntilIdle)
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture
        .post(&thread, "retry-both-members", "@all review")
        .await?;
    let first = fixture.attempt().await?;
    let peer = fixture.attempt().await?;
    let member = first.payload.member_id.clone();
    let start = Instant::now();
    first.complete(ThreadDeliveryOutcome::Unavailable)?;
    peer.complete(ThreadDeliveryOutcome::Delivered)?;
    let retry = fixture.attempt().await?;
    assert!(start.elapsed() >= INITIAL_RETRY);
    assert_eq!(retry.payload.member_id, member);
    retry.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn exhausted_member_stays_quiet_across_workers_until_its_own_idle() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_millis(5));
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let peer = fixture
        .add_peer(&thread, DeliveryMode::QueueUntilIdle)
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture
        .post(&thread, "exhaust-worker", "@worker review")
        .await?;
    for _ in 0..=RETRIES {
        let wire = tokio::time::timeout(MAX_RETRY + STEP_TIMEOUT, fixture.attempts.recv())
            .await?
            .ok_or("wire receiver closed")?;
        assert_eq!(wire.payload.member_id, fixture.worker);
        assert_eq!(wire.payload.to_seq, 1);
        wire.complete(ThreadDeliveryOutcome::Unavailable)?;
    }
    fixture.settle().await?;

    fixture
        .post(&thread, "exhaust-peer", "@peer review")
        .await?;
    let first = fixture.attempt().await?;
    assert_eq!(first.payload.member_id, peer);
    first.complete(ThreadDeliveryOutcome::Expired)?;
    let retry = fixture.attempt().await?;
    assert_eq!(retry.payload.member_id, peer);
    retry.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;

    fixture.set_worker(ParticipantState::Idle).await?;
    let fresh = fixture.attempt().await?;
    assert_eq!(fresh.payload.member_id, fixture.worker);
    assert_eq!((fresh.payload.from_seq, fresh.payload.to_seq), (1, 2));
    fresh.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn concurrent_idle_prevents_stale_exhaustion_and_resets_retry_backoff() -> TestResult {
    // Part 1: Drive all `0..RETRIES` failures so the final attempt (`number == RETRIES`,
    // where `exhausted_wake.is_some()`) races with a concurrent Idle. The concurrent Idle
    // must prevent `retry_exhausted_seq` from sticking and schedule an immediate fresh retry.
    let mut fixture = Fixture::new(true).await?;
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_millis(1));
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture.post(&thread, "race-idle", "@worker review").await?;

    for _ in 0..RETRIES {
        let attempt = fixture.attempt().await?;
        attempt.complete(ThreadDeliveryOutcome::Unavailable)?;
    }
    // Attempt `number == RETRIES` carries `exhausted_wake = Some(1)`.
    let final_attempt = fixture.attempt().await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    final_attempt.complete(ThreadDeliveryOutcome::Unavailable)?;

    let fresh = tokio::time::timeout(Duration::from_secs(2), fixture.attempts.recv())
        .await?
        .ok_or("expected immediate fresh retry after concurrent Idle on final attempt")?;
    assert_eq!((fresh.payload.from_seq, fresh.payload.to_seq), (1, 1));
    fresh.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    let exhausted_seq: u64 = fixture
        .state
        .reads
        .read({
            let t = thread.clone();
            let k = fixture.member_key();
            move |db| {
                Ok(db.query_row(
                    "SELECT retry_exhausted_seq FROM thread_members WHERE thread_id=?1 AND member_key=?2",
                    params![t.as_str(), k],
                    |row| row.get(0),
                )?)
            }
        })
        .await?;
    assert_eq!(exhausted_seq, 0);
    fixture.close().await?;

    // Part 2: Even when `finish(Unavailable)` commits before `on_participants_idle` runs
    // (returning `Completion::Transient`), an in-flight `idle_reset: true` prevents `Sweep::next()`
    // from putting the entry into a 30s `RetryPhase::Sleeping`.
    let candidate = Candidate {
        member_id: ParticipantId::generate()?,
        thread_id: ThreadId::generate()?,
    };
    let retry = RetryAttempt {
        epoch: TEST_EPOCH,
        generation: 0,
        wake_seq: 1,
        number: 0,
    };
    let mut sweep = Sweep::new(Duration::from_secs(30));
    Arc::make_mut(&mut sweep.retries).insert(
        candidate.clone(),
        (retry, RetryPhase::InFlight { idle_reset: true }),
    );
    let task_candidate = candidate.clone();
    sweep
        .pending
        .spawn(async move { (task_candidate, Some(retry), Completion::Transient) });
    assert!(sweep.next(false).await);
    assert!(
        !sweep.retries.contains_key(&candidate),
        "idle_reset during InFlight must drop retry entry instead of sleeping"
    );
    assert!(sweep.deadline().is_none());
    Ok(())
}

#[tokio::test]
async fn stage_clears_expired_wake_wait_policy_resets_exhaustion_and_finish_avoids_event_spam()
-> TestResult {
    use cooplet_protocol::{ThreadWaitPolicy, ThreadWaitRequest};

    let mut fixture = Fixture::new(true).await?;
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_millis(2));
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    let posted = fixture
        .post(&thread, "exhaust-and-wait", "@worker check")
        .await?;

    let count_member_events = |state: &AppState, thread_id: ThreadId| {
        let reads = state.reads.clone();
        async move {
            reads
                .read(move |db| {
                    Ok(db.query_row(
                        "SELECT COUNT(*) FROM events WHERE kind = ?1 AND aggregate_id = ?2",
                        params![EventKind::ThreadMemberUpdated.as_str(), thread_id.as_str()],
                        |row| row.get::<_, u64>(0),
                    )?)
                })
                .await
        }
    };

    let before = count_member_events(&fixture.state, thread.clone()).await?;
    for _ in 0..=RETRIES {
        let wire = fixture.attempt().await?;
        wire.complete(ThreadDeliveryOutcome::Unavailable)?;
    }
    fixture.settle().await?;
    // Unavailable outcomes never emit ThreadMemberUpdated.
    assert_eq!(
        count_member_events(&fixture.state, thread.clone()).await?,
        before
    );

    // Policy change via wait clears retry_exhausted_seq and resumes delivery.
    let node = fixture.node.clone();
    let worker = fixture.worker.clone();
    let actor = storage::sql::read_tx(&fixture.state.reads, move |db| {
        auth::attest_node_participant(db, &node, &worker, Some((1, TEST_EPOCH)))
    })
    .await?;
    let _ = threads::wait(
        &fixture.state,
        &actor,
        thread.clone(),
        ThreadWaitRequest {
            since_seq: 0,
            wake_policy: Some(ThreadWaitPolicy::All),
        },
    )
    .await?;
    let retried = fixture.attempt().await?;
    retried.complete(ThreadDeliveryOutcome::Busy)?;
    fixture.settle().await?;
    // Only the policy change emitted 1 event; Busy emitted 0.
    assert_eq!(
        count_member_events(&fixture.state, thread.clone()).await?,
        before + 1
    );

    // Logically expired last_wake_seq is cleared to 0 during stage().
    fixture.wall.store(
        posted.created_at_ms + threads::RETENTION_MS + 1,
        Ordering::Relaxed,
    );
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    let wake_after_expiry: u64 = fixture
        .state
        .reads
        .read({
            let t = thread.clone();
            let k = fixture.member_key();
            move |db| {
                Ok(db.query_row(
                    "SELECT last_wake_seq FROM thread_members WHERE thread_id=?1 AND member_key=?2",
                    params![t.as_str(), k],
                    |row| row.get(0),
                )?)
            }
        })
        .await?;
    assert_eq!(wake_after_expiry, 0);

    // reconcile_fences ignores state = 'left' (never resurrects a departed member).
    let left = threads::leave(
        &fixture.state,
        &fixture.admin,
        thread.clone(),
        "leave-before-fence",
        cooplet_protocol::ThreadMemberLeaveRequest {
            member_key: Some(fixture.member_key()),
        },
    )
    .await?;
    assert_eq!(left.state, cooplet_protocol::ThreadMemberState::Left);
    let hub_id = fixture.state.meta.hub_id.clone();
    let node = fixture.node.clone();
    let t = thread.clone();
    let w = fixture.worker.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            reconcile_fences(
                tx,
                &hub_id,
                &node,
                &[ThreadFenceReport {
                    thread_id: t,
                    participant_id: w,
                    delivery_generation: 0,
                    highest_dispatched_seq: 1,
                    last_delivery_id: None,
                    last_from_seq: None,
                }],
                TEST_NOW,
                &mut Vec::new(),
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let member_after = fixture
        .state
        .reads
        .read({
            let k = fixture.member_key();
            move |db| Ok(members::member(db, &thread, &k)?)
        })
        .await?
        .ok_or("member row missing")?;
    assert_eq!(
        member_after.state,
        cooplet_protocol::ThreadMemberState::Left
    );
    fixture.close().await
}

#[tokio::test]
async fn long_direct_mentions_span_prescans_on_read_pool_without_ping_pong() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    let thread = fixture.board(DeliveryMode::DirectNow).await?;
    // Seed 140 posts directly so BOTH `through` (span > 64) and `next_direct` (span > 64)
    // require ReadPool prescanning in `stage()`.
    let key = fixture.member_key();
    let owner = fixture.admin.actor_id().to_owned();
    let t = thread.clone();
    let mention = serde_json::to_string(&[&key])?;
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            for seq in 1..=140_u64 {
                let mentions = if matches!(seq, 70 | 140) {
                    mention.as_str()
                } else {
                    "[]"
                };
                tx.execute(
                    "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,
                        author_handle,body,mentions_json,created_at_ms,expires_at_ms)
                     VALUES(?1,?2,?3,?4,'human','owner','post',?5,?6,?7)",
                    params![
                        t.as_str(),
                        seq,
                        cooplet_protocol::MessageId::generate()
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                            .as_str(),
                        owner,
                        mentions,
                        TEST_NOW,
                        TEST_NOW + threads::RETENTION_MS
                    ],
                )?;
            }
            tx.execute(
                "UPDATE threads SET head_seq=140 WHERE thread_id=?1",
                [t.as_str()],
            )?;
            tx.execute(
                "UPDATE thread_members SET direct_wake_seq=1 WHERE thread_id=?1 AND member_key=?2",
                params![t.as_str(), key],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    wake(&fixture.state, &fixture.node);
    let first = fixture.attempt().await?;
    assert_eq!((first.payload.from_seq, first.payload.to_seq), (1, 70));
    first.complete(ThreadDeliveryOutcome::Delivered)?;
    let second = fixture.attempt().await?;
    assert_eq!((second.payload.from_seq, second.payload.to_seq), (71, 140));
    second.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn deliver_recycles_bridge_when_fallback_unknown_write_fails() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_millis(1));
    let thread = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture
        .post(&thread, "fallback-fail", "@worker test")
        .await?;
    let wire = fixture.attempt().await?;
    let payload = wire.payload.clone();
    // Drop the live attempt and stop the DB actor while keeping Hub shutdown un-signalled
    // so finish() retries and fallback finish(Unknown) all fail and recycle the bridge.
    drop(wire);
    let state = fixture.state.clone();
    let bridge = fixture.bridge.clone();
    let node = fixture.node.clone();
    let acl = state.thread_acl_watch.subscribe();
    let _keep_shutdown = fixture.shutdown;
    fixture.state.background.close();
    fixture.state.background.drain().await;
    fixture.engine.shutdown().await?;

    let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
    let permit = Box::new(ControlledPermit {
        slot: Arc::new(Semaphore::new(1)).try_acquire_owned()?,
        sent: sent_tx,
    }) as Box<dyn ThreadPermit>;
    let deliver_task =
        tokio::spawn(async move { deliver(&state, &node, permit, payload, acl, true, None).await });
    let on_wire = tokio::time::timeout(STEP_TIMEOUT, sent_rx.recv())
        .await?
        .ok_or("permit did not submit")?;
    on_wire.complete(ThreadDeliveryOutcome::Delivered)?;
    let completion = tokio::time::timeout(STEP_TIMEOUT, deliver_task).await??;
    assert!(matches!(completion, Completion::Finished));
    assert_eq!(bridge.transient_disconnects.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn many_stale_prescan_candidates_yield_without_stalling_live_delivery() -> TestResult {
    let mut fixture = Fixture::new(true).await?;
    // Long retry proves stale prescans use Attempt::Yield without triggering Retry backoff.
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_secs(30));
    let mut threads_list = Vec::new();
    for _ in 0..17 {
        threads_list.push(fixture.board(DeliveryMode::DirectNow).await?);
    }
    threads_list.sort();
    let live_thread = threads_list.pop().ok_or("missing live thread")?;
    let key = fixture.member_key();
    let owner = fixture.admin.actor_id().to_owned();
    let mention = serde_json::to_string(&[&key])?;
    let stale_threads = threads_list.clone();
    let live_clone = live_thread.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            // Seed 16 stale DirectNow MentionsOnly threads (>64 unmentioned posts each)
            // sorted strictly ahead of the live thread.
            for t in &stale_threads {
                for seq in 1..=70_u64 {
                    tx.execute(
                        "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,
                            author_handle,body,mentions_json,created_at_ms,expires_at_ms)
                         VALUES(?1,?2,?3,?4,'human','owner','noise','[]',?5,?6)",
                        params![
                            t.as_str(),
                            seq,
                            cooplet_protocol::MessageId::generate()
                                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                                .as_str(),
                            owner,
                            TEST_NOW,
                            TEST_NOW + threads::RETENTION_MS
                        ],
                    )?;
                }
                tx.execute(
                    "UPDATE threads SET head_seq=70 WHERE thread_id=?1",
                    [t.as_str()],
                )?;
                tx.execute(
                    "UPDATE thread_members SET direct_wake_seq=1 WHERE thread_id=?1 AND member_key=?2",
                    params![t.as_str(), key],
                )?;
            }
            for seq in 1..=70_u64 {
                let mentions = if seq == 70 { mention.as_str() } else { "[]" };
                tx.execute(
                    "INSERT INTO thread_posts(thread_id,seq,post_id,author_actor_id,author_kind,
                        author_handle,body,mentions_json,created_at_ms,expires_at_ms)
                     VALUES(?1,?2,?3,?4,'human','owner','live',?5,?6,?7)",
                    params![
                        live_clone.as_str(),
                        seq,
                        cooplet_protocol::MessageId::generate()
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?
                            .as_str(),
                        owner,
                        mentions,
                        TEST_NOW,
                        TEST_NOW + threads::RETENTION_MS
                    ],
                )?;
            }
            tx.execute(
                "UPDATE threads SET head_seq=70 WHERE thread_id=?1",
                [live_clone.as_str()],
            )?;
            tx.execute(
                "UPDATE thread_members SET direct_wake_seq=1 WHERE thread_id=?1 AND member_key=?2",
                params![live_clone.as_str(), key],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    wake(&fixture.state, &fixture.node);
    let delivered = tokio::time::timeout(Duration::from_secs(3), fixture.attempts.recv())
        .await?
        .ok_or("live thread stalled behind stale prescan candidates")?;
    assert_eq!(delivered.payload.thread_id, live_thread);
    assert_eq!(
        (delivered.payload.from_seq, delivered.payload.to_seq),
        (1, 70)
    );
    delivered.complete(ThreadDeliveryOutcome::Delivered)?;
    fixture.settle().await?;
    fixture.close().await
}

#[tokio::test]
async fn drive_queue_exits_promptly_when_inflight_fails_transiently_during_shutdown() -> TestResult
{
    let mut fixture = Fixture::new(true).await?;
    // Long backoff proves shutdown prevents a second RetryPhase::Sleeping after shutdown.changed().
    fixture.state.thread_dispatch = Dispatch::with_initial_retry(Duration::from_secs(30));
    fixture.bridge.free_slot.add_permits(1);
    let board_a = fixture.board(DeliveryMode::QueueUntilIdle).await?;
    let peer = fixture
        .add_peer(&board_a, DeliveryMode::QueueUntilIdle)
        .await?;
    fixture.set_worker(ParticipantState::Idle).await?;
    fixture.settle().await?;
    fixture
        .post(&board_a, "shutdown-two-members", "@all check")
        .await?;

    let first = fixture.attempt().await?;
    let second = fixture.attempt().await?;
    assert_ne!(first.payload.member_id, second.payload.member_id);
    assert!(first.payload.member_id == peer || second.payload.member_id == peer);

    // Put the first member into RetryPhase::Sleeping(30s) while the second remains InFlight.
    first.complete(ThreadDeliveryOutcome::Unavailable)?;
    tokio::time::sleep(Duration::from_millis(30)).await;

    // Trigger Hub shutdown once (consuming shutdown.changed() while deadline.is_some()),
    // then finish the still-in-flight second member transiently.
    fixture.state.background.close();
    let _ = fixture.shutdown.send(());
    tokio::time::sleep(Duration::from_millis(20)).await;
    second.complete(ThreadDeliveryOutcome::Unavailable)?;

    tokio::time::timeout(Duration::from_secs(2), fixture.state.background.drain())
        .await
        .map_err(|_| "drive_queue slept on RetryPhase::Sleeping after shutdown")?;
    fixture.engine.shutdown().await?;
    Ok(())
}
