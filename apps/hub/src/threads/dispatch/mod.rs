mod plan;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use cooplet_protocol::{
    NodeId, OperationId, ParticipantId, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadId,
};
use cooplet_storage::{StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::params;
use tokio::{sync::watch, task::JoinSet, time::Instant};

use crate::{
    AppState,
    bridge::{ReserveError, SubmitError, ThreadPermit},
    error::HubError,
    storage::sql::{parse_col, read_tx},
};

use super::posts;
use plan::{
    Candidate, Delivery, PrescanHint, PrescanKind, PrescanRequest, RetryAttempt, StageResult,
    clear_unsent_tx, finish, stage,
};

#[cfg(test)]
pub(crate) use plan::{PAUSE_ALL_PENDING_SQL, STAGE_CANDIDATES_SQL};
pub(in crate::threads) use plan::{
    RejoinSchedule, ResumeSchedule, WaitSchedule, apply_resume_schedule, record_post_wakes,
    record_wait, reset_member_on_leave, reset_member_on_rejoin,
};
pub(crate) use plan::{participant_idle, pause_node_pending, reconcile_fences, recover_pending};

const INITIAL_RETRY: Duration = Duration::from_millis(250);
const MAX_RETRY: Duration = Duration::from_secs(5);
const RETRIES: usize = 6;
const MAX_STAGE_PRESCANS: usize = 3;

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
        retry_delay(self.0.initial_retry, number)
    }
}

pub(crate) fn wake(state: &AppState, node_id: &NodeId) {
    signal(state, node_id);
}

/// Clear in-memory backoff timers for participants that just transitioned to
/// `idle` and wake the node's dispatch loop in a single notification.
pub(crate) fn on_participants_idle(
    state: &AppState,
    node_id: &NodeId,
    participants: &[ParticipantId],
) {
    if participants.is_empty() {
        return;
    }
    let mut nodes = state.thread_dispatch.lock();
    if let Some(queue) = nodes.get(node_id) {
        queue.record_idle(participants);
        queue
            .wake
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        return;
    }
    spawn_queue(&mut nodes, state, node_id);
}

fn signal(state: &AppState, node_id: &NodeId) {
    let mut nodes = state.thread_dispatch.lock();
    if let Some(queue) = nodes.get(node_id) {
        queue
            .wake
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        return;
    }
    spawn_queue(&mut nodes, state, node_id);
}

fn spawn_queue(nodes: &mut HashMap<NodeId, Arc<NodeQueue>>, state: &AppState, node_id: &NodeId) {
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

#[derive(Clone, Copy)]
enum RetryPhase {
    InFlight { idle_reset: bool },
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
                RetryPhase::InFlight { .. } | RetryPhase::Sleeping(_) => None,
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

async fn run_prescan(state: &AppState, request: PrescanRequest) -> Result<PrescanHint, HubError> {
    let PrescanRequest {
        candidate,
        member,
        kind,
    } = request;
    let thread_id = candidate.thread_id.clone();
    let found = read_tx(&state.reads, move |db| match kind {
        PrescanKind::ResumeLast { floor, through } => Ok(posts::last_relevant_seq(
            db, &thread_id, floor, through, &member,
        )?),
        PrescanKind::DirectFirst {
            after_seq,
            head_seq,
        } => Ok(posts::first_relevant_seq(
            db, &thread_id, after_seq, head_seq, &member,
        )?),
    })
    .await?;
    Ok(PrescanHint {
        candidate,
        kind,
        found,
    })
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
        let mut prescans: Vec<PrescanHint> = Vec::new();
        for _ in 0..MAX_STAGE_PRESCANS {
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
                    .retain(|_, (_, phase)| matches!(phase, RetryPhase::InFlight { .. }));
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
            let hints = prescans.clone();
            let staged = state
                .db
                .write(move |tx| {
                    let now = clock();
                    let job = stage(
                        tx,
                        &write_node,
                        (epoch, now),
                        cursor.as_ref(),
                        |candidate, ep, generation, wake_seq| {
                            retry_number(&retries, candidate, ep, generation, wake_seq)
                        },
                        &hints,
                        write_id,
                    )?;
                    Ok::<_, WriteError<HubError>>(WriteOutcome::new(job))
                })
                .await;

            match staged {
                Ok(StageResult::Ready(payload, retry)) => {
                    let candidate = Candidate {
                        member_id: payload.member_id.clone(),
                        thread_id: payload.thread_id.clone(),
                    };
                    let state = state.clone();
                    let node_id = node_id.clone();
                    let has_idle_gate = retry.is_some();
                    if let Some(attempt) = retry {
                        Arc::make_mut(&mut self.retries).insert(
                            candidate.clone(),
                            (attempt, RetryPhase::InFlight { idle_reset: false }),
                        );
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
                    return Attempt::Started;
                }
                Ok(StageResult::NeedsPrescan(request)) => {
                    // Release the bridge slot and authority guard before scanning on ReadPool.
                    drop(permit);
                    match run_prescan(state, *request).await {
                        Ok(found) => {
                            prescans.push(found);
                            continue;
                        }
                        Err(error) => {
                            tracing::warn!(%error, %node_id, "Could not prescan thread delivery posts");
                            return Attempt::Retry;
                        }
                    }
                }
                Ok(StageResult::None) => {
                    if from_start && self.pending.is_empty() {
                        Arc::make_mut(&mut self.retries)
                            .retain(|_, (_, phase)| !matches!(phase, RetryPhase::Ready));
                    }
                    return Attempt::NoCandidate;
                }
                Err(error) => {
                    // Release the bridge slot and authority guard before retrying SQLite cleanup.
                    drop(permit);
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
            }
        }
        Attempt::Retry
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
                        if matches!(outcome, Completion::Transient)
                            && attempt.number < RETRIES
                            && matches!(*phase, RetryPhase::InFlight { idle_reset: false })
                        {
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
                if !idle_reset.contains(&candidate.member_id) {
                    return true;
                }
                if let RetryPhase::InFlight { idle_reset } = phase {
                    *idle_reset = true;
                    true
                } else {
                    false
                }
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
                        .retain(|_, (_, p)| matches!(p, RetryPhase::InFlight { .. }));
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
                    Arc::make_mut(&mut sweep.retries)
                        .retain(|_, (_, p)| matches!(p, RetryPhase::InFlight { .. }));
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

async fn clear_unsent(state: &AppState, node_id: &NodeId, delivery_id: OperationId) {
    let mut shutdown = state.shutdown.clone();
    for attempt in 0..RETRIES {
        if shutdown.has_changed().unwrap_or(true) {
            return;
        }
        let node = node_id.clone();
        let id = delivery_id.clone();
        match state
            .db
            .write_sqlite(move |tx| clear_unsent_tx(tx, &node, &id))
            .await
        {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(%error, %node_id, attempt, "Could not clear an unsent thread delivery");
            }
        }
        if attempt + 1 < RETRIES {
            tokio::select! {
                biased;
                _ = shutdown.changed() => return,
                () = tokio::time::sleep(state.thread_dispatch.retry_delay(attempt)) => {}
            }
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
    let mut shutdown = state.shutdown.clone();
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
            tokio::select! {
                biased;
                _ = shutdown.changed() => return Completion::Finished,
                () = tokio::time::sleep(state.thread_dispatch.retry_delay(attempt)) => {}
            }
        }
    }
    // Fortify the unknown boundary if the terminal write could not be confirmed.
    // If even the unknown boundary fails, recycle the bridge so the new epoch pauses pending ranges.
    let hub_id = state.meta.hub_id.clone();
    let clock = state.wall_clock.clone();
    if let Err(error) = state
        .db
        .write_sqlite(move |tx| finish(tx, &hub_id, &mark, Outcome::Unknown, clock()))
        .await
    {
        tracing::error!(
            %error,
            %node_id,
            "Could not mark unconfirmed thread delivery unknown; recycling bridge session"
        );
        state.bridge.disconnect_transient(node_id);
    }
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
                params![
                    delivery.candidate.thread_id.as_str(),
                    delivery.member_key,
                    delivery.node_id.as_str(),
                    delivery.generation,
                    delivery.delivery_id.as_str(),
                    delivery.epoch
                ],
                |row| row.get::<_, bool>(0),
            )?)
        })
        .await;
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

#[cfg(test)]
mod tests;
