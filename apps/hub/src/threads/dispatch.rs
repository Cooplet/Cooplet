use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use cooplet_protocol::{
    AggregateType, DeliveryMode, EventKind, HubId, NodeId, OperationId, ParticipantId,
    ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadFenceReport, ThreadId,
};
use cooplet_storage::{EventNotice, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{Transaction, params};
use tokio::{sync::watch, task::JoinSet};

use crate::{
    AppState,
    bridge::{ReserveError, SubmitError, ThreadPermit},
    error::HubError,
    storage::{
        events::{self, EventAudience, NewEvent},
        sql::{json_byte_len, parse_col, read_tx},
    },
};

use super::{members, posts, store};

const MAX_DELIVERY_BYTES: usize = 128 * 1024;
const MAX_DELIVERY_POSTS: usize = 20;
const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(5);
const RETRIES: usize = 6;

#[derive(Clone, Default)]
pub(crate) struct Dispatch(Arc<Mutex<HashMap<NodeId, watch::Sender<Wake>>>>);

#[derive(Default)]
struct Wake {
    revision: u64,
    last_idle_revision: u64,
    roster_revision: u64,
    participant_idle: HashMap<ParticipantId, u64>,
}

impl Wake {
    fn advance(&mut self, trigger: WakeTrigger<'_>) {
        self.revision = self.revision.saturating_add(1);
        match trigger {
            WakeTrigger::Work => return,
            WakeTrigger::Roster => self.roster_revision = self.revision,
            WakeTrigger::Idle(participant) => {
                self.participant_idle
                    .insert(participant.clone(), self.revision);
            }
        }
        self.last_idle_revision = self.revision;
    }

    fn newer_idle(&self, participant: &ParticipantId, staged_revision: u64) -> bool {
        self.roster_revision > staged_revision
            || self
                .participant_idle
                .get(participant)
                .is_some_and(|revision| *revision > staged_revision)
    }
}

enum WakeTrigger<'a> {
    Work,
    Roster,
    Idle(&'a ParticipantId),
}

impl Dispatch {
    fn lock(&self) -> MutexGuard<'_, HashMap<NodeId, watch::Sender<Wake>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn wake(state: &AppState, node_id: &NodeId) {
    signal(state, node_id, WakeTrigger::Roster);
}

pub(crate) fn wake_participant(state: &AppState, node_id: &NodeId, participant_id: &ParticipantId) {
    signal(state, node_id, WakeTrigger::Idle(participant_id));
}

fn signal(state: &AppState, node_id: &NodeId, trigger: WakeTrigger<'_>) {
    let mut nodes = state.thread_dispatch.lock();
    if let Some(wake) = nodes.get(node_id) {
        wake.send_modify(|wake| wake.advance(trigger));
        return;
    }
    let (send, receive) = watch::channel(Wake::default());
    let task = drive_queue(state.clone(), node_id.clone(), receive);
    if state.background.spawn(task).is_ok() {
        nodes.insert(node_id.clone(), send);
    }
}

pub(super) async fn wake_thread(state: &AppState, thread_id: ThreadId, direct_seq: Option<u64>) {
    let id = thread_id.clone();
    let nodes = read_tx(&state.reads, move |db| {
        let mut stmt = db.prepare_cached(
            "SELECT m.node_id, m.delivery_mode, MAX(m.resume_through_seq IS NOT NULL)
             FROM thread_members m JOIN nodes n USING(node_id)
             WHERE m.thread_id=?1 AND m.state='active' AND m.node_id IS NOT NULL
               AND m.role!='observer' AND m.wake_policy!='muted' AND n.status='online'
               AND (m.delivery_mode='queue_until_idle' OR m.resume_through_seq IS NOT NULL OR ?2 IS NOT NULL)
             GROUP BY m.node_id, m.delivery_mode",
        )?;
        Ok(stmt
            .query_map(params![id.as_str(), direct_seq], |row| {
                Ok((
                    parse_col::<NodeId>(row, 0)?,
                    parse_col::<DeliveryMode>(row, 1)?,
                    row.get::<_, bool>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
    .await;
    match nodes {
        Ok(nodes) => {
            for (node_id, mode, has_resume) in nodes {
                if mode == DeliveryMode::QueueUntilIdle || has_resume {
                    signal(state, &node_id, WakeTrigger::Work);
                }
                if let (DeliveryMode::DirectNow, Some(seq)) = (mode, direct_seq) {
                    let state_copy = state.clone();
                    let thread_id = thread_id.clone();
                    let _ = state.background.spawn(async move {
                        drive_direct(state_copy, node_id, thread_id, seq).await;
                    });
                }
            }
        }
        Err(error) => {
            tracing::warn!(%error, %thread_id, "Could not locate thread delivery candidates")
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct Candidate {
    member_id: ParticipantId,
    thread_id: ThreadId,
}

#[derive(Clone)]
enum Mode {
    Idle,
    Direct { thread_id: ThreadId, post_seq: u64 },
}

impl Mode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "queue_until_idle",
            Self::Direct { .. } => "direct_now",
        }
    }
}

enum Attempt {
    Started,
    NoCandidate,
    Offline,
    Saturated,
    Retry,
}

enum Completion {
    Delivered,
    WaitForIdle,
    Transient,
    Ignore,
}

struct Sweep {
    cursor: Option<Candidate>,
    suppressed: HashMap<Candidate, u64>,
    pending: JoinSet<FinishedAttempt>,
}

struct FinishedAttempt {
    candidate: Candidate,
    staged_revision: u64,
    outcome: Completion,
}

impl Sweep {
    fn new() -> Self {
        Self {
            cursor: None,
            suppressed: HashMap::new(),
            pending: JoinSet::new(),
        }
    }

    async fn start(
        &mut self,
        state: &AppState,
        node_id: &NodeId,
        mode: Mode,
        staged_revision: u64,
    ) -> Attempt {
        let mut shutdown = state.shutdown.clone();
        let reserved = tokio::select! {
            biased;
            _ = shutdown.changed() => return Attempt::Offline,
            reserved = state.bridge.reserve_thread(node_id) => reserved,
        };
        let permit = match reserved {
            Ok(permit) => permit,
            Err(ReserveError::Offline) => return Attempt::Offline,
            Err(ReserveError::RateLimited) => return Attempt::Saturated,
        };
        let epoch = permit.epoch();
        let delivery_id = match OperationId::generate() {
            Ok(id) => id,
            Err(error) => {
                tracing::warn!(%error, "Could not generate a thread delivery ID");
                return Attempt::Retry;
            }
        };
        let write_id = delivery_id.clone();
        let write_node = node_id.clone();
        let cursor = self.cursor.clone();
        let suppressed = self.suppressed.clone();
        let clock = state.wall_clock.clone();
        let acl = state.thread_acl_watch.subscribe();
        let staged = state
            .db
            .write(move |tx| {
                Ok::<_, WriteError<HubError>>(WriteOutcome::new(stage(
                    tx,
                    &write_node,
                    (epoch, clock()),
                    &mode,
                    cursor.as_ref(),
                    &suppressed,
                    write_id,
                )?))
            })
            .await;

        let payload = match staged {
            Ok(Some(payload)) => payload,
            Ok(None) => return Attempt::NoCandidate,
            Err(error) => {
                if matches!(
                    error,
                    WriteFailure::Storage(
                        StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)
                    )
                ) {
                    clear_unsent(state, node_id, delivery_id).await;
                }
                let error = HubError::from(error);
                tracing::warn!(%error, %node_id, "Could not stage a thread delivery");
                return Attempt::Retry;
            }
        };
        let candidate = Candidate {
            member_id: payload.member_id.clone(),
            thread_id: payload.thread_id.clone(),
        };
        let output = candidate.clone();
        let state = state.clone();
        let node_id = node_id.clone();
        self.pending.spawn(async move {
            let outcome = deliver(&state, &node_id, permit, payload, acl).await;
            FinishedAttempt {
                candidate: output,
                staged_revision,
                outcome,
            }
        });
        self.cursor = Some(candidate);
        Attempt::Started
    }

    async fn next(&mut self) -> Option<FinishedAttempt> {
        while let Some(result) = self.pending.join_next().await {
            match result {
                Ok(completion) => return Some(completion),
                Err(error) => tracing::warn!(%error, "Thread delivery task ended unexpectedly"),
            }
        }
        None
    }
}

async fn drive_direct(state: AppState, node_id: NodeId, thread_id: ThreadId, post_seq: u64) {
    let mut sweep = Sweep::new();
    let mode = Mode::Direct {
        thread_id,
        post_seq,
    };
    while let Attempt::Started = sweep.start(&state, &node_id, mode.clone(), 0).await {}
    while sweep.next().await.is_some() {}
    signal(&state, &node_id, WakeTrigger::Work);
}

async fn drive_queue(state: AppState, node_id: NodeId, mut wake: watch::Receiver<Wake>) {
    let mut sweep = Sweep::new();
    let mut observed = 0;
    let mut observed_idle = 0;
    let mut scan = true;
    let mut retry = false;
    let mut follow_up = false;
    let mut retries = 0;
    loop {
        {
            let current = wake.borrow_and_update();
            if observed != current.revision {
                if observed_idle != current.last_idle_revision {
                    observed_idle = current.last_idle_revision;
                    sweep.suppressed.retain(|candidate, staged_revision| {
                        !current.newer_idle(&candidate.member_id, *staged_revision)
                    });
                    retries = 0;
                }
                observed = current.revision;
                if scan {
                    // Finish this keyset sweep before considering new work on earlier keys.
                    follow_up = true;
                } else {
                    sweep.cursor = None;
                    scan = true;
                }
            }
        }
        if scan {
            match sweep.start(&state, &node_id, Mode::Idle, observed).await {
                Attempt::Started => continue,
                Attempt::NoCandidate | Attempt::Offline | Attempt::Saturated => scan = false,
                Attempt::Retry => {
                    scan = false;
                    retry = true;
                }
            }
        }
        if !sweep.pending.is_empty() {
            tokio::select! {
                change = wake.changed() => {
                    if change.is_err() { return; }
                    continue;
                },
                completion = sweep.next() => if let Some(FinishedAttempt {candidate, staged_revision, outcome}) = completion {
                    match outcome {
                        Completion::Delivered => follow_up = true,
                        Completion::WaitForIdle => {
                            // A projected Idle can commit before the preceding Busy ACK arrives.
                            if wake.borrow().newer_idle(&candidate.member_id, staged_revision) {
                                follow_up = true;
                            } else {
                                sweep.suppressed.insert(candidate, staged_revision);
                            }
                        }
                        Completion::Transient => retry = true,
                        Completion::Ignore => {}
                    }
                }
            }
            scan = true;
            continue;
        }
        if std::mem::take(&mut follow_up) {
            sweep.cursor = None;
            scan = true;
            continue;
        }
        if std::mem::take(&mut retry) && retries < RETRIES {
            let delay = (INITIAL_RETRY * (1 << retries)).min(MAX_RETRY);
            retries += 1;
            let mut shutdown = state.shutdown.clone();
            tokio::select! {
                _ = shutdown.changed() => {},
                change = wake.changed() => {
                    if change.is_err() {
                        return;
                    }
                    continue;
                }
                () = tokio::time::sleep(delay) => {
                    sweep.cursor = None;
                    scan = true;
                }
            }
            if scan {
                continue;
            }
        }
        let mut nodes = state.thread_dispatch.lock();
        if wake.borrow().revision == observed {
            nodes.remove(&node_id);
            return;
        }
    }
}

fn stage(
    tx: &Transaction<'_>,
    node_id: &NodeId,
    (epoch, now): (u64, i64),
    mode: &Mode,
    cursor: Option<&Candidate>,
    suppressed: &HashMap<Candidate, u64>,
    delivery_id: OperationId,
) -> Result<Option<ThreadDeliverPayload>, HubError> {
    let only_thread = match mode {
        Mode::Idle => None,
        Mode::Direct { thread_id, .. } => Some(thread_id.as_str()),
    };
    let mut candidates = tx.prepare_cached(
        "SELECT m.participant_id,m.thread_id,m.member_key,m.resume_through_seq
         FROM thread_members m JOIN nodes n USING(node_id)
         JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
         JOIN threads t USING(thread_id)
         WHERE m.node_id=?1 AND n.session_epoch=?2 AND n.status='online' AND n.revoked_at_ms IS NULL
           AND t.status='open' AND m.state='active' AND m.pending_delivery_id IS NULL AND m.role!='observer'
           AND m.wake_policy!='muted' AND m.confirmed_seq<t.head_seq
           AND p.synced_epoch=?2 AND p.kind='agent'
           AND ((?3='direct_now' AND m.delivery_mode='direct_now')
                OR (?3='queue_until_idle' AND p.state='idle'
                    AND (m.delivery_mode='queue_until_idle' OR m.resume_through_seq IS NOT NULL)))
           AND (?4 IS NULL OR m.thread_id=?4) AND (m.participant_id,m.thread_id)>(?5,?6)
         ORDER BY m.participant_id,m.thread_id",
    )?;
    let mut rows = candidates.query(params![
        node_id.as_str(),
        epoch,
        mode.as_str(),
        only_thread,
        cursor.map_or("", |c| c.member_id.as_str()),
        cursor.map_or("", |c| c.thread_id.as_str()),
    ])?;
    while let Some(row) = rows.next()? {
        let candidate = Candidate {
            member_id: parse_col(row, 0)?,
            thread_id: parse_col(row, 1)?,
        };
        if suppressed.contains_key(&candidate) {
            continue;
        }
        let key: String = row.get(2)?;
        let resume_through: Option<u64> = row.get(3)?;
        let member =
            members::member(tx, &candidate.thread_id, &key)?.ok_or_else(store::not_found)?;
        let board = store::board(tx, &candidate.thread_id, now)?;
        let floor = member.confirmed_seq.max(board.floor_seq);
        let recovery_ready = if let Some(through) = resume_through {
            let relevant = floor < through
                && posts::last_relevant_seq(tx, &candidate.thread_id, floor, through, &member)?
                    .is_some();
            if !relevant {
                tx.execute(
                    "UPDATE thread_members SET resume_through_seq=NULL WHERE thread_id=?1 AND member_key=?2",
                    params![candidate.thread_id.as_str(), key],
                )?;
            }
            relevant
        } else {
            false
        };
        let through = match mode {
            Mode::Idle if member.delivery_mode == DeliveryMode::DirectNow => {
                recovery_ready.then_some(board.head_seq)
            }
            Mode::Idle => {
                posts::last_relevant_seq(tx, &candidate.thread_id, floor, board.head_seq, &member)?
                    .map(|_| board.head_seq)
            }
            Mode::Direct { post_seq, .. }
                if !recovery_ready && *post_seq > floor && *post_seq <= board.head_seq =>
            {
                posts::last_relevant_seq(
                    tx,
                    &candidate.thread_id,
                    post_seq - 1,
                    *post_seq,
                    &member,
                )?
            }
            Mode::Direct { .. } => None,
        };
        if let Some(through) = through {
            let payload = materialize(tx, candidate, &member, floor, through, delivery_id)?;
            tx.execute(
                "UPDATE thread_members SET pending_delivery_id=?1,pending_from_seq=?2,pending_to_seq=?3
                 WHERE thread_id=?4 AND member_key=?5",
                params![payload.delivery_id.as_str(), payload.from_seq, payload.to_seq, payload.thread_id.as_str(), key],
            )?;
            return Ok(Some(payload));
        }
    }
    Ok(None)
}

fn materialize(
    tx: &Transaction<'_>,
    candidate: Candidate,
    member: &cooplet_protocol::MemberView,
    floor: u64,
    through: u64,
    delivery_id: OperationId,
) -> Result<ThreadDeliverPayload, HubError> {
    let from = member.confirmed_seq + 1;
    let mut payload = ThreadDeliverPayload {
        thread_id: candidate.thread_id,
        member_id: candidate.member_id,
        delivery_generation: member.delivery_generation,
        delivery_id,
        from_seq: from,
        to_seq: through,
        posts: Vec::new(),
        truncated_earlier: false,
        delivery_mode: member.delivery_mode,
        admission_budget_ms: 30_000,
    };
    let mut bytes = json_byte_len(&payload)?;
    for post in posts::delivery_posts(tx, &payload.thread_id, floor, through, MAX_DELIVERY_POSTS)? {
        let size = json_byte_len(&post)? + usize::from(!payload.posts.is_empty());
        if bytes + size > MAX_DELIVERY_BYTES {
            break;
        }
        bytes += size;
        payload.posts.push(post);
    }
    payload.posts.reverse();
    let first = payload
        .posts
        .first()
        .ok_or_else(|| HubError::internal("Thread delivery has no live posts."))?;
    payload.truncated_earlier = first.seq > from;
    if payload.posts.iter().all(|post| post.attachments.is_empty()) {
        payload.admission_budget_ms = 5_000;
    }
    Ok(payload)
}

async fn clear_unsent(state: &AppState, node_id: &NodeId, delivery_id: OperationId) {
    let node_id = node_id.clone();
    if let Err(error) = state.db.write_sqlite(move |tx| {
        tx.execute(
            "UPDATE thread_members SET pending_delivery_id=NULL,pending_from_seq=NULL,pending_to_seq=NULL
             WHERE node_id=?1 AND pending_delivery_id=?2 AND state='active'",
            params![node_id.as_str(), delivery_id.as_str()],
        )?;
        Ok(WriteOutcome::new(()))
    }).await {
        tracing::warn!(%error, "Could not clear an unsent thread delivery");
    }
}

async fn deliver(
    state: &AppState,
    node_id: &NodeId,
    permit: Box<dyn ThreadPermit>,
    payload: ThreadDeliverPayload,
    acl: watch::Receiver<u64>,
) -> Completion {
    use ThreadDeliveryOutcome as Outcome;
    let epoch = permit.epoch();
    let mark = Delivery::from_payload(node_id, epoch, &payload);
    let result = tokio::select! {
        biased;
        () = until_ineligible(state, &mark, acl) => Err(SubmitError::Unknown),
        result = permit.submit(payload) => result,
    };
    let outcome = match result {
        Ok(ack) if mark.matches(&ack) => ack.outcome,
        Ok(_) | Err(SubmitError::Unknown) => Outcome::Unknown,
        Err(SubmitError::Unavailable) => Outcome::Unavailable,
    };
    let mut retry = INITIAL_RETRY;
    for attempt in 0..RETRIES {
        let copy = mark.clone();
        let hub_id = state.meta.hub_id.clone();
        let clock = state.wall_clock.clone();
        match state
            .db
            .write_sqlite(move |tx| finish(tx, &hub_id, &copy, outcome, clock()))
            .await
        {
            Ok(false) => return Completion::Ignore,
            Ok(true) => {
                return match outcome {
                    Outcome::Delivered => Completion::Delivered,
                    Outcome::Busy | Outcome::AwaitingApproval => Completion::WaitForIdle,
                    Outcome::Unavailable | Outcome::Expired => Completion::Transient,
                    Outcome::Unknown | Outcome::Rejected => Completion::Ignore,
                };
            }
            Err(error) => {
                tracing::warn!(%error, %node_id, "Could not finalize the thread delivery")
            }
        }
        if attempt + 1 < RETRIES {
            tokio::time::sleep(retry).await;
            retry = (retry * 2).min(MAX_RETRY);
        }
    }
    // Fortify the unknown boundary if the terminal write could not be confirmed.
    let hub_id = state.meta.hub_id.clone();
    let clock = state.wall_clock.clone();
    let _ = state
        .db
        .write_sqlite(move |tx| finish(tx, &hub_id, &mark, Outcome::Unknown, clock()))
        .await;
    Completion::Ignore
}

async fn until_ineligible(state: &AppState, delivery: &Delivery, mut acl: watch::Receiver<u64>) {
    while acl.changed().await.is_ok() {
        let delivery = delivery.clone();
        let eligible = read_tx(&state.reads, move |tx| {
            Ok(tx.query_row(
                "SELECT EXISTS (
                   SELECT 1 FROM thread_members m JOIN threads t USING(thread_id) JOIN nodes n USING(node_id)
                   WHERE m.thread_id=?1 AND m.member_key=?2 AND m.node_id=?3
                     AND m.delivery_generation=?4 AND m.pending_delivery_id=?5 AND m.state='active'
                     AND t.status='open' AND n.session_epoch=?6 AND n.revoked_at_ms IS NULL
                 )",
                params![delivery.candidate.thread_id.as_str(), delivery.member_key,
                    delivery.node_id.as_str(), delivery.generation, delivery.delivery_id.as_str(), delivery.epoch],
                |row| row.get::<_, bool>(0),
            )?)
        }).await;
        match eligible {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                tracing::warn!(%error, "Could not verify an in-flight thread delivery after membership changed");
                return;
            }
        }
    }
}

#[derive(Clone)]
struct Delivery {
    node_id: NodeId,
    epoch: u64,
    candidate: Candidate,
    member_key: String,
    generation: u64,
    delivery_id: OperationId,
    to_seq: u64,
}

impl Delivery {
    fn from_payload(node_id: &NodeId, epoch: u64, payload: &ThreadDeliverPayload) -> Self {
        Self {
            node_id: node_id.clone(),
            epoch,
            candidate: Candidate {
                member_id: payload.member_id.clone(),
                thread_id: payload.thread_id.clone(),
            },
            member_key: format!("{node_id}/{}", payload.member_id),
            generation: payload.delivery_generation,
            delivery_id: payload.delivery_id.clone(),
            to_seq: payload.to_seq,
        }
    }

    fn matches(&self, ack: &ThreadAckPayload) -> bool {
        ack.delivery_generation == self.generation
            && ack.delivery_id == self.delivery_id
            && ack.thread_id == self.candidate.thread_id
            && ack.member_id == self.candidate.member_id
            && ack.acked_seq == self.to_seq
    }
}

fn finish(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    delivery: &Delivery,
    outcome: ThreadDeliveryOutcome,
    now: i64,
) -> rusqlite::Result<WriteOutcome<bool>> {
    let delivered = outcome == ThreadDeliveryOutcome::Delivered;
    let unknown = outcome == ThreadDeliveryOutcome::Unknown;
    let rejected = outcome == ThreadDeliveryOutcome::Rejected;
    let changed = tx.execute(
        "UPDATE thread_members SET state=CASE WHEN ?1 THEN 'paused_unknown' ELSE state END,
           confirmed_seq=CASE WHEN ?2 THEN MAX(confirmed_seq,?3) ELSE confirmed_seq END,
           pending_delivery_id=CASE WHEN ?1 THEN pending_delivery_id ELSE NULL END,
           pending_from_seq=CASE WHEN ?1 THEN pending_from_seq ELSE NULL END,
           pending_to_seq=CASE WHEN ?1 THEN pending_to_seq ELSE NULL END,
           resume_through_seq=CASE WHEN ?1 OR ?2 OR ?10 THEN NULL ELSE resume_through_seq END
         WHERE thread_id=?4 AND member_key=?5 AND node_id=?6 AND state='active' AND delivery_generation=?7
           AND pending_delivery_id=?8 AND pending_to_seq=?3
           AND EXISTS(SELECT 1 FROM nodes WHERE node_id=?6 AND session_epoch=?9)",
        params![unknown, delivered, delivery.to_seq, delivery.candidate.thread_id.as_str(),
            delivery.member_key, delivery.node_id.as_str(), delivery.generation,
            delivery.delivery_id.as_str(), delivery.epoch, rejected],
    )?;
    let mut notices = Vec::new();
    if changed != 0 {
        member_event(
            tx,
            hub_id,
            &delivery.candidate.thread_id,
            &delivery.member_key,
            now,
            &mut notices,
        )?;
    }
    if let Some(last) = notices.last() {
        events::advance_head(tx, last.seq)?;
    }
    Ok(WriteOutcome::with_events(changed != 0, notices))
}

pub(crate) fn recover_pending(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    pause_pending(tx, hub_id, None, now, notices)
}

pub(crate) fn pause_node_pending(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: &NodeId,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let previous = notices.len();
    pause_pending(tx, hub_id, Some(node_id), now, notices)?;
    if notices.len() > previous
        && let Some(last) = notices.last()
    {
        events::advance_head(tx, last.seq)?;
    }
    Ok(())
}

fn pause_pending(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: Option<&NodeId>,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let mut statement = tx.prepare_cached(if node_id.is_some() {
        "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
         WHERE node_id=?1 AND state='active' AND pending_delivery_id IS NOT NULL
         RETURNING thread_id,member_key"
    } else {
        "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
         WHERE ?1 IS NULL AND state='active' AND pending_delivery_id IS NOT NULL
         RETURNING thread_id,member_key"
    })?;
    let keys = statement
        .query_map([node_id.map(NodeId::as_str)], |row| {
            Ok((parse_col::<ThreadId>(row, 0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (thread_id, key) in keys {
        member_event(tx, hub_id, &thread_id, &key, now, notices)?;
    }
    Ok(())
}

pub(crate) fn reconcile_fences(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: &NodeId,
    reports: &[ThreadFenceReport],
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    pause_pending(tx, hub_id, Some(node_id), now, notices)?;
    for report in reports {
        let key = format!("{node_id}/{}", report.participant_id);
        let changed = tx.execute(
            "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
             WHERE thread_id=?1 AND member_key=?2 AND node_id=?3 AND delivery_generation=?4 AND state='active'",
            params![report.thread_id.as_str(), key, node_id.as_str(), report.delivery_generation],
        )?;
        if changed != 0 {
            member_event(tx, hub_id, &report.thread_id, &key, now, notices)?;
        }
    }
    if let Some(last) = notices.last() {
        events::advance_head(tx, last.seq)?;
    }
    Ok(())
}

fn member_event(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    thread_id: &ThreadId,
    key: &str,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    if let Some(member) = members::member(tx, thread_id, key)? {
        notices.push(events::record(
            tx,
            hub_id,
            NewEvent {
                kind: EventKind::ThreadMemberUpdated,
                aggregate_type: AggregateType::Thread,
                aggregate_id: thread_id.as_str(),
                actor_id: None,
                created_at_ms: now,
                data: members::event_data(thread_id, &member),
                audience: EventAudience::Thread {
                    thread_id: thread_id.clone(),
                },
            },
        )?);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
