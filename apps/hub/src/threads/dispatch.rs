use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use cooplet_protocol::{
    AggregateType, DeliveryMode, EventKind, HubId, MemberView, NodeId, OperationId, ParticipantId,
    ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadFenceReport, ThreadId,
    ThreadRole, ThreadWakePolicy,
};
use cooplet_storage::{EventNotice, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};
use tokio::{sync::watch, task::JoinSet, time::Instant};

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

struct NodeQueue {
    wake: watch::Sender<u64>,
    idle_reset: Mutex<HashSet<ParticipantId>>,
}

impl NodeQueue {
    fn new(wake: watch::Sender<u64>) -> Self {
        Self {
            wake,
            idle_reset: Mutex::new(HashSet::new()),
        }
    }

    fn record_idle(&self, participants: &[ParticipantId]) {
        let mut reset = self
            .idle_reset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        reset.extend(participants.iter().cloned());
    }

    fn take_idle_resets(&self) -> HashSet<ParticipantId> {
        let mut reset = self
            .idle_reset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::take(&mut *reset)
    }
}

struct DispatchInner {
    nodes: Mutex<HashMap<NodeId, Arc<NodeQueue>>>,
    initial_retry: Duration,
}

#[derive(Clone)]
pub(crate) struct Dispatch(Arc<DispatchInner>);

impl Default for Dispatch {
    fn default() -> Self {
        Self(Arc::new(DispatchInner {
            nodes: Mutex::new(HashMap::new()),
            initial_retry: INITIAL_RETRY,
        }))
    }
}

impl Dispatch {
    #[cfg(test)]
    pub(crate) fn with_initial_retry(initial_retry: Duration) -> Self {
        Self(Arc::new(DispatchInner {
            nodes: Mutex::new(HashMap::new()),
            initial_retry,
        }))
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<NodeId, Arc<NodeQueue>>> {
        self.0
            .nodes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn retry_delay(&self, number: usize) -> Duration {
        let shift = u32::try_from(number).unwrap_or(u32::MAX);
        let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
        self.0.initial_retry.saturating_mul(factor).min(MAX_RETRY)
    }
}

pub(crate) fn wake(state: &AppState, node_id: &NodeId) {
    signal(state, node_id);
}

/// Clear in-memory backoff timers for participants that just transitioned to
/// `idle` so their refreshed retry budget can execute without waiting on an
/// old sleep deadline.
pub(crate) fn on_participants_idle(
    state: &AppState,
    node_id: &NodeId,
    participants: &[ParticipantId],
) {
    if participants.is_empty() {
        return;
    }
    let nodes = state.thread_dispatch.lock();
    if let Some(queue) = nodes.get(node_id) {
        queue.record_idle(participants);
        queue
            .wake
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

pub(crate) fn participant_idle(
    tx: &Transaction<'_>,
    node: &NodeId,
    participant: &ParticipantId,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND participant_id=?2
         AND idle_delivery_id IS NOT NULL",
    )?
    .execute(params![node.as_str(), participant.as_str()])?;
    tx.prepare_cached(
        "UPDATE thread_members SET retry_exhausted_seq=0 WHERE node_id=?1 AND participant_id=?2
         AND state='active' AND retry_exhausted_seq>0",
    )?
    .execute(params![node.as_str(), participant.as_str()])?;
    Ok(())
}

pub(super) fn record_post_wakes(
    tx: &Transaction<'_>,
    thread_id: &ThreadId,
    seq: u64,
    author_actor_id: &str,
    mentions_json: &str,
    mentions_all: bool,
) -> rusqlite::Result<()> {
    // Record durable queue wakes even while offline. Session-scoped direct wakes
    // keep the earliest unattempted relevant post in `direct_wake_seq` while the
    // node is online, or extend `resume_through_seq` during explicit recovery.
    tx.prepare_cached(
        "UPDATE thread_members AS m SET
           last_wake_seq=CASE WHEN m.delivery_mode='queue_until_idle' THEN ?2
                              ELSE m.last_wake_seq END,
           direct_wake_seq=CASE WHEN m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL
                                     AND m.direct_wake_seq=0
                                     AND EXISTS(SELECT 1 FROM nodes WHERE node_id=m.node_id
                                                AND status='online' AND revoked_at_ms IS NULL)
                                THEN ?2 ELSE m.direct_wake_seq END,
           resume_through_seq=CASE WHEN m.resume_through_seq IS NOT NULL THEN ?2 ELSE NULL END
         WHERE m.thread_id=?1 AND m.state='active' AND m.node_id IS NOT NULL
           AND (m.delivery_mode='queue_until_idle' OR m.resume_through_seq IS NOT NULL
                OR EXISTS(SELECT 1 FROM nodes WHERE node_id=m.node_id AND status='online' AND revoked_at_ms IS NULL))
           AND m.role!='observer' AND m.wake_policy!='muted' AND m.member_key!=?3
           AND (m.wake_policy='all' OR ?5 OR m.member_key IN (SELECT value FROM json_each(?4)))",
    )?
    .execute(params![
        thread_id.as_str(),
        seq,
        author_actor_id,
        mentions_json,
        mentions_all
    ])?;
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub(super) fn record_wait(
    tx: &Transaction<'_>,
    thread_id: &ThreadId,
    member_key: &str,
    since_seq: u64,
    head_seq: u64,
    wake_policy: ThreadWakePolicy,
    policy_changed: bool,
    relevant_last: Option<u64>,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE thread_members SET
           confirmed_seq=MAX(confirmed_seq,?1),
           wake_policy=?2,
           last_wake_seq=CASE WHEN delivery_mode='queue_until_idle' AND ?7 THEN ?5
                              WHEN delivery_mode='queue_until_idle' AND ?1>=?6 THEN 0
                              ELSE last_wake_seq END,
           direct_wake_seq=CASE WHEN delivery_mode='direct_now' AND (?1>=?6 OR ?2='muted') THEN 0
                                ELSE direct_wake_seq END,
           retry_exhausted_seq=CASE WHEN ?7 THEN 0 ELSE retry_exhausted_seq END,
           resume_through_seq=CASE WHEN resume_through_seq<=?1 THEN NULL ELSE resume_through_seq END
         WHERE thread_id=?3 AND member_key=?4",
    )?
    .execute(params![
        since_seq,
        wake_policy.as_str(),
        thread_id.as_str(),
        member_key,
        relevant_last.unwrap_or(0),
        head_seq,
        policy_changed
    ])?;
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub(super) fn reset_member_on_rejoin(
    tx: &Transaction<'_>,
    thread_id: &ThreadId,
    member_key: &str,
    node_id: Option<&NodeId>,
    role: ThreadRole,
    wake_policy: ThreadWakePolicy,
    delivery_mode: DeliveryMode,
    disputed: bool,
    confirmed_seq: u64,
    now_ms: i64,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE thread_members SET node_id=?1, role=?2, wake_policy=?3, delivery_mode=?4, state=?5,
         confirmed_seq=?6, last_wake_seq=0, direct_wake_seq=0, retry_exhausted_seq=0,
         resume_through_seq=NULL, joined_at_ms=?7, left_at_ms=NULL
         WHERE thread_id=?8 AND member_key=?9",
    )?
    .execute(params![
        node_id.map(NodeId::as_str),
        role.as_str(),
        wake_policy.as_str(),
        delivery_mode.as_str(),
        if disputed { "paused_unknown" } else { "active" },
        confirmed_seq,
        now_ms,
        thread_id.as_str(),
        member_key,
    ])?;
    Ok(())
}

pub(super) fn reset_member_on_leave(
    tx: &Transaction<'_>,
    thread_id: &ThreadId,
    member_key: &str,
    now_ms: i64,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE thread_members SET state='left', left_at_ms=?1, resume_through_seq=NULL,
         last_wake_seq=0, direct_wake_seq=0, retry_exhausted_seq=0
         WHERE thread_id=?2 AND member_key=?3",
    )?
    .execute(params![now_ms, thread_id.as_str(), member_key])?;
    Ok(())
}

#[expect(clippy::too_many_arguments)]
pub(super) fn apply_resume_schedule(
    tx: &Transaction<'_>,
    thread_id: &ThreadId,
    member_key: &str,
    delivery_mode: DeliveryMode,
    generation: u64,
    after_seq: u64,
    head_seq: u64,
    relevant_last: Option<u64>,
) -> rusqlite::Result<()> {
    let (last_wake, resume_through) = match delivery_mode {
        DeliveryMode::QueueUntilIdle => (relevant_last.unwrap_or(0), None),
        DeliveryMode::DirectNow => (0, relevant_last.map(|_| head_seq)),
    };
    tx.prepare_cached(
        "UPDATE thread_members SET state='active', delivery_generation=?1, confirmed_seq=?2,
         pending_delivery_id=NULL, pending_from_seq=NULL, pending_to_seq=NULL,
         resume_through_seq=?3, last_wake_seq=?6, direct_wake_seq=0, retry_exhausted_seq=0
         WHERE thread_id=?4 AND member_key=?5",
    )?
    .execute(params![
        generation,
        after_seq,
        resume_through,
        thread_id.as_str(),
        member_key,
        last_wake,
    ])?;
    Ok(())
}

fn signal(state: &AppState, node_id: &NodeId) {
    let mut nodes = state.thread_dispatch.lock();
    if let Some(queue) = nodes.get(node_id) {
        queue
            .wake
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        return;
    }
    let (send, receive) = watch::channel(0);
    let queue = Arc::new(NodeQueue::new(send));
    let task = drive_queue(state.clone(), node_id.clone(), queue.clone(), receive);
    if state.background.spawn_tracked(task).is_ok() {
        nodes.insert(node_id.clone(), queue);
    }
}

pub(super) async fn wake_thread(state: &AppState, thread_id: ThreadId) {
    let id = thread_id.clone();
    let now = (state.wall_clock)();
    let nodes = read_tx(&state.reads, move |db| {
        let mut stmt = db.prepare_cached(
            "SELECT DISTINCT m.node_id FROM thread_members m JOIN nodes n USING(node_id)
             JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
             JOIN threads t USING(thread_id)
             WHERE m.thread_id=?1 AND m.state='active' AND m.node_id IS NOT NULL
               AND m.role!='observer' AND m.wake_policy!='muted' AND n.status='online'
               AND p.synced_epoch=n.session_epoch AND p.kind='agent'
               AND ((m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL AND m.direct_wake_seq>0)
                    OR (p.state='idle' AND p.idle_delivery_id IS NULL
                        AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)>m.retry_exhausted_seq
                        AND (m.resume_through_seq IS NOT NULL
                             OR (m.delivery_mode='queue_until_idle' AND m.last_wake_seq>m.confirmed_seq
                                 AND m.last_wake_seq>t.floor_seq
                                 AND EXISTS(SELECT 1 FROM thread_posts post WHERE post.thread_id=m.thread_id
                                            AND post.seq=m.last_wake_seq AND post.expires_at_ms>?2)))))",
        )?;
        Ok(stmt
            .query_map(params![id.as_str(), now], |row| parse_col::<NodeId>(row, 0))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
    .await;
    match nodes {
        Ok(nodes) => {
            for node in nodes {
                signal(state, &node);
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

enum Attempt {
    Started,
    NoCandidate,
    Offline,
    Saturated,
    Retry,
}

enum Completion {
    Finished,
    Transient,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct RetryAttempt {
    epoch: u64,
    generation: u64,
    wake_seq: u64,
    number: usize,
}

#[derive(Clone, Copy)]
enum RetryPhase {
    InFlight,
    Sleeping(Instant),
    Ready,
}

type RetryMap = HashMap<Candidate, (RetryAttempt, RetryPhase)>;

fn retry_number(
    retries: &RetryMap,
    candidate: &Candidate,
    epoch: u64,
    generation: u64,
    wake_seq: u64,
) -> Option<usize> {
    match retries.get(candidate) {
        Some((prior, phase)) if prior.epoch == epoch && prior.generation == generation => {
            match phase {
                RetryPhase::InFlight | RetryPhase::Sleeping(_) => None,
                RetryPhase::Ready if prior.wake_seq == wake_seq => Some(prior.number + 1),
                RetryPhase::Ready => Some(0),
            }
        }
        _ => Some(0),
    }
}

fn retry_delay(initial: Duration, number: usize) -> Duration {
    let shift = u32::try_from(number).unwrap_or(u32::MAX);
    let factor = 1u32.checked_shl(shift).unwrap_or(u32::MAX);
    initial.saturating_mul(factor).min(MAX_RETRY)
}

struct Sweep {
    cursor: Option<Candidate>,
    pending: JoinSet<(Candidate, Option<RetryAttempt>, Completion)>,
    // Only the serial staging transaction borrows a snapshot. It drops its
    // reference before the writer replies, so scheduler updates mutate in place.
    retries: Arc<RetryMap>,
    last_epoch: Option<u64>,
    initial_retry: Duration,
}

impl Sweep {
    fn new(initial_retry: Duration) -> Self {
        Self {
            cursor: None,
            pending: JoinSet::new(),
            retries: Arc::default(),
            last_epoch: None,
            initial_retry,
        }
    }

    async fn start(&mut self, state: &AppState, node_id: &NodeId) -> Attempt {
        let mut shutdown = state.shutdown.clone();
        let reserved = tokio::select! {
            biased;
            _ = shutdown.changed() => return Attempt::Offline,
            reserved = state.bridge.reserve_thread(node_id, self.pending.is_empty()) => reserved,
        };
        let permit = match reserved {
            Ok(permit) => permit,
            Err(ReserveError::Offline) => return Attempt::Offline,
            Err(ReserveError::RateLimited) => return Attempt::Saturated,
        };
        let epoch = permit.epoch();
        if self.last_epoch != Some(epoch) {
            self.last_epoch = Some(epoch);
            Arc::make_mut(&mut self.retries)
                .retain(|_, (_, phase)| matches!(phase, RetryPhase::InFlight));
        }
        let delivery_id = match OperationId::generate() {
            Ok(id) => id,
            Err(error) => {
                tracing::warn!(%error, "Could not generate a thread delivery ID");
                return Attempt::Retry;
            }
        };
        let from_start = self.cursor.is_none();
        let write_id = delivery_id.clone();
        let write_node = node_id.clone();
        let cursor = self.cursor.clone();
        let clock = state.wall_clock.clone();
        let acl = state.thread_acl_watch.subscribe();
        let retries = self.retries.clone();
        let staged = state
            .db
            .write(move |tx| {
                let now = clock();
                let job = stage(
                    tx,
                    &write_node,
                    (epoch, now),
                    cursor.as_ref(),
                    &retries,
                    write_id,
                )?;
                Ok::<_, WriteError<HubError>>(WriteOutcome::new(job))
            })
            .await;

        let (payload, retry) = match staged {
            Ok(Some(job)) => job,
            Ok(None) => {
                if from_start && self.pending.is_empty() {
                    Arc::make_mut(&mut self.retries)
                        .retain(|_, (_, phase)| !matches!(phase, RetryPhase::Ready));
                }
                return Attempt::NoCandidate;
            }
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
        let state = state.clone();
        let node_id = node_id.clone();
        let has_idle_gate = retry.is_some();
        if let Some(attempt) = retry {
            Arc::make_mut(&mut self.retries)
                .insert(candidate.clone(), (attempt, RetryPhase::InFlight));
        }
        self.cursor = Some(candidate.clone());
        self.pending.spawn(async move {
            let exhausted = retry.filter(|r| r.number == RETRIES).map(|r| r.wake_seq);
            let outcome = deliver(
                &state,
                &node_id,
                permit,
                payload,
                acl,
                has_idle_gate,
                exhausted,
            )
            .await;
            (candidate, retry, outcome)
        });
        Attempt::Started
    }

    fn deadline(&self) -> Option<Instant> {
        self.retries
            .values()
            .filter_map(|(_, phase)| match phase {
                RetryPhase::Sleeping(at) => Some(*at),
                _ => None,
            })
            .min()
    }

    fn release_due(&mut self, now: Instant) {
        for (_, phase) in Arc::make_mut(&mut self.retries).values_mut() {
            if matches!(*phase, RetryPhase::Sleeping(at) if at <= now) {
                *phase = RetryPhase::Ready;
            }
        }
    }

    async fn next(&mut self) -> bool {
        while let Some(result) = self.pending.join_next().await {
            match result {
                Ok((candidate, retry, outcome)) => {
                    let entries = Arc::make_mut(&mut self.retries);
                    if let Some(attempt) = retry
                        && let Some((current, phase)) = entries.get_mut(&candidate)
                        && *current == attempt
                    {
                        if matches!(outcome, Completion::Transient) && attempt.number < RETRIES {
                            *phase = RetryPhase::Sleeping(
                                Instant::now() + retry_delay(self.initial_retry, attempt.number),
                            );
                        } else {
                            entries.remove(&candidate);
                        }
                    }
                    // The released slot can serve a different board on either side of the cursor.
                    return true;
                }
                Err(error) => tracing::warn!(%error, "Thread delivery task ended unexpectedly"),
            }
        }
        false
    }
}

async fn drive_queue(
    state: AppState,
    node_id: NodeId,
    queue: Arc<NodeQueue>,
    mut wake: watch::Receiver<u64>,
) {
    let mut sweep = Sweep::new(state.thread_dispatch.0.initial_retry);
    let mut observed = 0;
    let mut scan = true;
    let mut follow_up = false;
    let mut stage_failures = 0;
    let mut stage_retry_at = None;
    let mut shutdown = state.shutdown.clone();
    loop {
        let idle_reset = queue.take_idle_resets();
        if !idle_reset.is_empty() {
            Arc::make_mut(&mut sweep.retries).retain(|candidate, (_, phase)| {
                matches!(phase, RetryPhase::InFlight) || !idle_reset.contains(&candidate.member_id)
            });
        }
        let current = *wake.borrow_and_update();
        if observed != current {
            observed = current;
            if stage_failures > RETRIES {
                stage_failures = 0;
            }
            if scan {
                // Finish this keyset sweep before considering new work on earlier keys.
                follow_up = true;
            } else {
                sweep.cursor = None;
                scan = true;
            }
        }
        if scan && stage_retry_at.is_none() && stage_failures <= RETRIES {
            match sweep.start(&state, &node_id).await {
                Attempt::Started => {
                    stage_failures = 0;
                    continue;
                }
                Attempt::NoCandidate => {
                    stage_failures = 0;
                    scan = false;
                }
                Attempt::Offline => {
                    Arc::make_mut(&mut sweep.retries)
                        .retain(|_, (_, p)| matches!(p, RetryPhase::InFlight));
                    scan = false;
                }
                // A full bridge pauses this sweep mid-keyset. A new wake adds
                // a follow-up pass and must not restart at the earliest member.
                Attempt::Saturated => {}
                Attempt::Retry => {
                    if stage_failures < RETRIES {
                        stage_retry_at = Some(
                            Instant::now() + state.thread_dispatch.retry_delay(stage_failures),
                        );
                    }
                    stage_failures += 1;
                    scan = false;
                }
            }
        }
        if !scan && std::mem::take(&mut follow_up) {
            sweep.cursor = None;
            scan = true;
            continue;
        }

        let deadline = sweep.deadline().into_iter().chain(stage_retry_at).min();
        if !sweep.pending.is_empty() || deadline.is_some() {
            tokio::select! {
                change = wake.changed() => {
                    if change.is_err() { return; }
                }
                rescan = sweep.next(), if !sweep.pending.is_empty() => {
                    follow_up |= rescan;
                    scan = true;
                }
                () = async {
                    if let Some(at) = deadline { tokio::time::sleep_until(at).await; }
                }, if deadline.is_some() => {
                    let now = Instant::now();
                    sweep.release_due(now);
                    if stage_retry_at.is_some_and(|at| at <= now) {
                        stage_retry_at = None;
                    }
                    follow_up = true;
                }
                _ = shutdown.changed(), if deadline.is_some() => {
                    Arc::make_mut(&mut sweep.retries).retain(|_, (_, p)| matches!(p, RetryPhase::InFlight));
                    stage_retry_at = None;
                    stage_failures = RETRIES + 1;
                }
            }
            continue;
        }
        let mut nodes = state.thread_dispatch.lock();
        if *wake.borrow() == observed {
            nodes.remove(&node_id);
            return;
        }
    }
}

fn stage(
    tx: &Transaction<'_>,
    node_id: &NodeId,
    (epoch, now): (u64, i64),
    cursor: Option<&Candidate>,
    retries: &RetryMap,
    delivery_id: OperationId,
) -> Result<Option<(ThreadDeliverPayload, Option<RetryAttempt>)>, HubError> {
    let mut candidates = tx.prepare_cached(
        "SELECT m.participant_id,m.thread_id,m.member_key,m.handle,m.role,m.wake_policy,
                m.delivery_mode,m.state,m.delivery_generation,m.confirmed_seq,
                m.resume_through_seq,m.last_wake_seq,m.direct_wake_seq,t.head_seq,t.floor_seq
         FROM thread_members m JOIN nodes n USING(node_id)
         JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
         JOIN threads t USING(thread_id)
         WHERE m.node_id=?1 AND n.session_epoch=?2 AND n.status='online' AND n.revoked_at_ms IS NULL
           AND t.status='open' AND m.state='active' AND m.pending_delivery_id IS NULL AND m.role!='observer'
           AND m.wake_policy!='muted' AND m.confirmed_seq<t.head_seq
           AND p.synced_epoch=?2 AND p.kind='agent'
           AND ((m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL AND m.direct_wake_seq>0)
                OR (p.state='idle' AND p.idle_delivery_id IS NULL
                        AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)>m.retry_exhausted_seq
                    AND m.participant_id NOT IN (
                        SELECT participant_id FROM thread_members WHERE node_id=?1
                          AND state='active' AND role!='observer' AND wake_policy!='muted'
                          AND pending_delivery_id IS NOT NULL)
                    AND ((m.delivery_mode='queue_until_idle' AND m.last_wake_seq>m.confirmed_seq AND m.last_wake_seq>t.floor_seq)
                         OR m.resume_through_seq IS NOT NULL)))
           AND (m.participant_id,m.thread_id)>(?3,?4)
         ORDER BY m.participant_id,m.thread_id",
    )?;
    let mut rows = candidates.query(params![
        node_id.as_str(),
        epoch,
        cursor.map_or("", |c| c.member_id.as_str()),
        cursor.map_or("", |c| c.thread_id.as_str()),
    ])?;
    while let Some(row) = rows.next()? {
        let candidate = Candidate {
            member_id: parse_col(row, 0)?,
            thread_id: parse_col(row, 1)?,
        };
        let key: String = row.get(2)?;
        let delivery_mode: DeliveryMode = parse_col(row, 6)?;
        let generation: u64 = row.get(8)?;
        let resume_through: Option<u64> = row.get(10)?;
        let last_wake: u64 = row.get(11)?;
        let direct_wake: u64 = row.get(12)?;
        let head_seq: u64 = row.get(13)?;
        let raw_floor_seq: u64 = row.get(14)?;
        let direct = delivery_mode == DeliveryMode::DirectNow && resume_through.is_none();
        let retry = if direct {
            None
        } else {
            let wake_seq = last_wake.max(resume_through.unwrap_or(0));
            let Some(number) = retry_number(retries, &candidate, epoch, generation, wake_seq)
            else {
                continue;
            };
            Some(RetryAttempt {
                epoch,
                generation,
                wake_seq,
                number,
            })
        };
        let member = MemberView {
            member_key: key.clone(),
            node_id: Some(node_id.clone()),
            participant_id: candidate.member_id.as_str().to_owned(),
            handle: row.get(3)?,
            role: parse_col(row, 4)?,
            wake_policy: parse_col(row, 5)?,
            delivery_mode,
            state: parse_col(row, 7)?,
            delivery_generation: generation,
            confirmed_seq: row.get(9)?,
            pending: None,
        };
        let effective_floor = store::effective_floor(tx, &candidate.thread_id, raw_floor_seq, now)?;
        let floor = member.confirmed_seq.max(effective_floor);
        let recovery_ready = if let Some(through) = resume_through {
            let relevant = floor < through
                && posts::last_relevant_seq(tx, &candidate.thread_id, floor, through, &member)?
                    .is_some();
            if !relevant {
                tx.prepare_cached(
                    "UPDATE thread_members SET resume_through_seq=NULL WHERE thread_id=?1 AND member_key=?2",
                )?
                .execute(params![candidate.thread_id.as_str(), key])?;
            }
            relevant
        } else {
            false
        };
        let through = if direct {
            posts::first_relevant_seq(
                tx,
                &candidate.thread_id,
                floor.max(direct_wake.saturating_sub(1)),
                head_seq,
                &member,
            )?
        } else if recovery_ready {
            Some(head_seq)
        } else if delivery_mode == DeliveryMode::QueueUntilIdle {
            if last_wake > floor {
                Some(head_seq)
            } else {
                // Clear logically expired queued wake so future sweeps do not re-examine it.
                tx.prepare_cached(
                    "UPDATE thread_members SET last_wake_seq=0 WHERE thread_id=?1 AND member_key=?2",
                )?
                .execute(params![candidate.thread_id.as_str(), key])?;
                None
            }
        } else {
            None
        };
        if direct {
            // Consume this one direct wake at admission. Other posts accepted
            // while it is pending retain their own chance in sequence order.
            let next = if let Some(seq) = through {
                posts::first_relevant_seq(tx, &candidate.thread_id, seq, head_seq, &member)?
            } else {
                None
            };
            tx.prepare_cached(
                "UPDATE thread_members SET direct_wake_seq=?3 WHERE thread_id=?1 AND member_key=?2",
            )?
            .execute(params![
                candidate.thread_id.as_str(),
                key,
                next.unwrap_or(0)
            ])?;
        }
        if let Some(through) = through {
            let payload = materialize(tx, candidate, &member, floor, through, delivery_id)?;
            if !direct {
                tx.prepare_cached(
                    "UPDATE node_participants SET idle_delivery_id=?3 WHERE node_id=?1 AND participant_id=?2",
                )?
                .execute(params![
                    node_id.as_str(),
                    payload.member_id.as_str(),
                    payload.delivery_id.as_str()
                ])?;
            }
            tx.prepare_cached(
                "UPDATE thread_members SET pending_delivery_id=?1,pending_from_seq=?2,pending_to_seq=?3
                 WHERE thread_id=?4 AND member_key=?5",
            )?
            .execute(params![
                payload.delivery_id.as_str(),
                payload.from_seq,
                payload.to_seq,
                payload.thread_id.as_str(),
                key
            ])?;
            return Ok(Some((payload, retry)));
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
    for attempt in 0..RETRIES {
        let node = node_id.clone();
        let id = delivery_id.clone();
        match state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND idle_delivery_id=?2",
                    params![node.as_str(), id.as_str()],
                )?;
                tx.execute(
                    "UPDATE thread_members SET
                       direct_wake_seq=CASE WHEN delivery_mode='direct_now' AND resume_through_seq IS NULL
                           AND (direct_wake_seq=0 OR direct_wake_seq>pending_to_seq)
                           THEN pending_to_seq ELSE direct_wake_seq END,
                       pending_delivery_id=NULL,pending_from_seq=NULL,pending_to_seq=NULL
                     WHERE node_id=?1 AND pending_delivery_id=?2 AND state='active'",
                    params![node.as_str(), id.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await
        {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(%error, %node_id, attempt, "Could not clear an unsent thread delivery");
            }
        }
        if attempt + 1 < RETRIES {
            tokio::time::sleep(state.thread_dispatch.retry_delay(attempt)).await;
        }
    }
    tracing::error!(
        %node_id,
        "Could not clear an unsent thread delivery after retries; recycling bridge session"
    );
    state.bridge.disconnect_transient(node_id);
}

async fn deliver(
    state: &AppState,
    node_id: &NodeId,
    permit: Box<dyn ThreadPermit>,
    payload: ThreadDeliverPayload,
    acl: watch::Receiver<u64>,
    has_idle_gate: bool,
    exhausted_wake: Option<u64>,
) -> Completion {
    use ThreadDeliveryOutcome as Outcome;
    let epoch = permit.epoch();
    let mut mark = Delivery::from_payload(node_id, epoch, &payload, has_idle_gate);
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
    if matches!(outcome, Outcome::Expired | Outcome::Unavailable) {
        mark.exhausted_wake = exhausted_wake;
    }
    for attempt in 0..RETRIES {
        let copy = mark.clone();
        let hub_id = state.meta.hub_id.clone();
        let clock = state.wall_clock.clone();
        match state
            .db
            .write_sqlite(move |tx| finish(tx, &hub_id, &copy, outcome, clock()))
            .await
        {
            Ok(true) if matches!(outcome, Outcome::Unavailable | Outcome::Expired) => {
                return Completion::Transient;
            }
            Ok(_) => return Completion::Finished,
            Err(error) => {
                tracing::warn!(%error, %node_id, "Could not finalize the thread delivery")
            }
        }
        if attempt + 1 < RETRIES {
            tokio::time::sleep(state.thread_dispatch.retry_delay(attempt)).await;
        }
    }
    // Fortify the unknown boundary if the terminal write could not be confirmed.
    let hub_id = state.meta.hub_id.clone();
    let clock = state.wall_clock.clone();
    let _ = state
        .db
        .write_sqlite(move |tx| finish(tx, &hub_id, &mark, Outcome::Unknown, clock()))
        .await;
    Completion::Finished
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
    has_idle_gate: bool,
    exhausted_wake: Option<u64>,
}

impl Delivery {
    fn from_payload(
        node_id: &NodeId,
        epoch: u64,
        payload: &ThreadDeliverPayload,
        has_idle_gate: bool,
    ) -> Self {
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
            has_idle_gate,
            exhausted_wake: None,
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
    let idle_gate_held = if delivery.has_idle_gate {
        tx.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM node_participants WHERE node_id=?1 AND participant_id=?2
             AND synced_epoch=?3 AND idle_delivery_id=?4)",
        )?
        .query_row(
            params![
                delivery.node_id.as_str(),
                delivery.candidate.member_id.as_str(),
                delivery.epoch,
                delivery.delivery_id.as_str()
            ],
            |row| row.get::<_, bool>(0),
        )?
    } else {
        true
    };
    let prev_confirmed: Option<u64> = tx
        .prepare_cached(
            "SELECT confirmed_seq FROM thread_members
             WHERE thread_id=?1 AND member_key=?2 AND state='active' AND pending_delivery_id=?3",
        )?
        .query_row(
            params![
                delivery.candidate.thread_id.as_str(),
                delivery.member_key,
                delivery.delivery_id.as_str()
            ],
            |row| row.get(0),
        )
        .optional()?;
    // A concurrent Idle clears `idle_delivery_id` and refreshes the retry budget.
    // Never overwrite that reset with a stale exhaustion marker from this attempt.
    let exhausted_wake = if idle_gate_held {
        delivery.exhausted_wake
    } else {
        None
    };
    let changed = tx
        .prepare_cached(
            "UPDATE thread_members SET state=CASE WHEN ?1 THEN 'paused_unknown' ELSE state END,
               retry_exhausted_seq=COALESCE(?11,retry_exhausted_seq),
               last_wake_seq=CASE WHEN ?10 AND delivery_mode='queue_until_idle' AND last_wake_seq<=?3 THEN 0
                                  ELSE last_wake_seq END,
               direct_wake_seq=CASE WHEN ?1 AND delivery_mode='direct_now' THEN 0
                                    ELSE direct_wake_seq END,
               confirmed_seq=CASE WHEN ?2 THEN MAX(confirmed_seq,?3) ELSE confirmed_seq END,
               pending_delivery_id=CASE WHEN ?1 THEN pending_delivery_id ELSE NULL END,
               pending_from_seq=CASE WHEN ?1 THEN pending_from_seq ELSE NULL END,
               pending_to_seq=CASE WHEN ?1 THEN pending_to_seq ELSE NULL END,
               resume_through_seq=CASE WHEN ?1 OR ((?2 OR ?10) AND resume_through_seq<=?3) THEN NULL
                                       ELSE resume_through_seq END
             WHERE thread_id=?4 AND member_key=?5 AND node_id=?6 AND state='active' AND delivery_generation=?7
               AND pending_delivery_id=?8 AND pending_to_seq=?3
               AND EXISTS(SELECT 1 FROM nodes WHERE node_id=?6 AND session_epoch=?9)",
        )?
        .execute(params![
            unknown,
            delivered,
            delivery.to_seq,
            delivery.candidate.thread_id.as_str(),
            delivery.member_key,
            delivery.node_id.as_str(),
            delivery.generation,
            delivery.delivery_id.as_str(),
            delivery.epoch,
            rejected,
            exhausted_wake
        ])?;
    // Release only this delivery's gate, even when the member CAS no longer matches.
    let blocked = matches!(
        outcome,
        ThreadDeliveryOutcome::Busy | ThreadDeliveryOutcome::AwaitingApproval
    );
    if (changed == 0 || !blocked) && idle_gate_held && delivery.has_idle_gate {
        tx.prepare_cached(
            "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND participant_id=?2
             AND synced_epoch=?3 AND idle_delivery_id=?4",
        )?
        .execute(params![
            delivery.node_id.as_str(),
            delivery.candidate.member_id.as_str(),
            delivery.epoch,
            delivery.delivery_id.as_str()
        ])?;
    }
    let mut notices = Vec::new();
    let emit_event = changed != 0
        && (unknown || (delivered && prev_confirmed.is_some_and(|prev| delivery.to_seq > prev)));
    if emit_event {
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
    // Only request transient backoff when the member CAS matched AND no concurrent
    // Idle cleared the participant's gate while this attempt was in flight.
    Ok(WriteOutcome::with_events(
        changed != 0 && idle_gate_held,
        notices,
    ))
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
    // Direct wakes belong to one online session. A new epoch starts with only
    // durable queue / explicit recovery work, and can acknowledge a fresh Idle.
    let node_filter = if node_id.is_some() {
        "node_id=?1"
    } else {
        "?1 IS NULL"
    };
    tx.prepare_cached(&format!(
        "UPDATE thread_members SET direct_wake_seq=0, retry_exhausted_seq=0
         WHERE {node_filter} AND state='active' AND node_id IS NOT NULL
           AND (direct_wake_seq>0 OR retry_exhausted_seq>0)",
    ))?
    .execute([node_id.map(NodeId::as_str)])?;
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
    let mut pause_fence = tx.prepare_cached(
        "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
         WHERE thread_id=?1 AND member_key=?2 AND node_id=?3 AND delivery_generation=?4 AND state='active'",
    )?;
    for report in reports {
        let key = format!("{node_id}/{}", report.participant_id);
        let changed = pause_fence.execute(params![
            report.thread_id.as_str(),
            key,
            node_id.as_str(),
            report.delivery_generation
        ])?;
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
