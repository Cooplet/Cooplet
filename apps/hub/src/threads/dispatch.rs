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

#[derive(Clone, Default)]
pub(crate) struct Dispatch(Arc<Mutex<HashMap<NodeId, watch::Sender<u64>>>>);

impl Dispatch {
    fn lock(&self) -> MutexGuard<'_, HashMap<NodeId, watch::Sender<u64>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn wake(state: &AppState, node_id: &NodeId) {
    signal(state, node_id);
}

pub(crate) fn participant_idle(
    tx: &Transaction<'_>,
    node: &NodeId,
    participant: &ParticipantId,
) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND participant_id=?2
         AND idle_delivery_id IS NOT NULL",
        params![node.as_str(), participant.as_str()],
    )?;
    tx.execute(
        "UPDATE thread_members SET retry_exhausted_seq=0 WHERE node_id=?1 AND participant_id=?2
         AND state='active' AND retry_exhausted_seq>0",
        params![node.as_str(), participant.as_str()],
    )?;
    Ok(())
}

fn signal(state: &AppState, node_id: &NodeId) {
    let mut nodes = state.thread_dispatch.lock();
    if let Some(wake) = nodes.get(node_id) {
        wake.send_modify(|revision| *revision = revision.wrapping_add(1));
        return;
    }
    let (send, receive) = watch::channel(0);
    let task = drive_queue(state.clone(), node_id.clone(), receive);
    if state.background.spawn_tracked(task).is_ok() {
        nodes.insert(node_id.clone(), send);
    }
}

pub(super) async fn wake_thread(state: &AppState, thread_id: ThreadId) {
    let id = thread_id.clone();
    let now = (state.wall_clock)();
    let nodes = read_tx(&state.reads, move |db| {
        let mut stmt = db.prepare_cached(
            "SELECT DISTINCT m.node_id FROM thread_members m JOIN nodes n USING(node_id)
             JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
             WHERE m.thread_id=?1 AND m.state='active' AND m.node_id IS NOT NULL
               AND m.role!='observer' AND m.wake_policy!='muted' AND n.status='online'
               AND p.synced_epoch=n.session_epoch AND p.kind='agent'
               AND ((m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL AND m.last_wake_seq>0)
                    OR (p.state='idle' AND p.idle_delivery_id IS NULL
                        AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)>m.retry_exhausted_seq
                        AND (m.resume_through_seq IS NOT NULL
                             OR (m.delivery_mode='queue_until_idle' AND m.last_wake_seq>m.confirmed_seq
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

fn retry_delay(number: usize) -> Duration {
    (INITIAL_RETRY * (1 << number)).min(MAX_RETRY)
}

struct Sweep {
    cursor: Option<Candidate>,
    pending: JoinSet<(Candidate, Option<RetryAttempt>, Completion)>,
    // Only the serial staging transaction borrows a snapshot. It drops its
    // reference before the writer replies, so scheduler updates mutate in place.
    retries: Arc<RetryMap>,
    prune_ready: bool,
    acl_revision: u64,
}

impl Sweep {
    fn new() -> Self {
        Self {
            cursor: None,
            pending: JoinSet::new(),
            retries: Arc::default(),
            prune_ready: false,
            acl_revision: 0,
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
        let clock = state.wall_clock.clone();
        let acl = state.thread_acl_watch.subscribe();
        let retries = self.retries.clone();
        let revision = *acl.borrow();
        let prune = self.prune_ready || revision != self.acl_revision;
        let staged = state
            .db
            .write(move |tx| {
                let now = clock();
                let retired = if prune {
                    retired_retries(tx, &write_node, epoch, now, &retries)?
                } else {
                    Vec::new()
                };
                let job = stage(
                    tx,
                    &write_node,
                    (epoch, now),
                    cursor.as_ref(),
                    &retries,
                    write_id,
                )?;
                Ok::<_, WriteError<HubError>>(WriteOutcome::new((job, retired)))
            })
            .await;

        let (payload, retry) = match staged {
            Ok((job, retired)) => {
                self.prune_ready = false;
                self.acl_revision = revision;
                for candidate in retired {
                    Arc::make_mut(&mut self.retries).remove(&candidate);
                }
                let Some(job) = job else {
                    return Attempt::NoCandidate;
                };
                job
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
        if let Some(attempt) = retry {
            Arc::make_mut(&mut self.retries)
                .insert(candidate.clone(), (attempt, RetryPhase::InFlight));
        }
        self.cursor = Some(candidate.clone());
        self.pending.spawn(async move {
            let exhausted = retry.filter(|r| r.number == RETRIES).map(|r| r.wake_seq);
            let outcome = deliver(&state, &node_id, permit, payload, acl, exhausted).await;
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
                self.prune_ready = true;
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
                            *phase =
                                RetryPhase::Sleeping(Instant::now() + retry_delay(attempt.number));
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

async fn drive_queue(state: AppState, node_id: NodeId, mut wake: watch::Receiver<u64>) {
    let mut sweep = Sweep::new();
    let mut observed = 0;
    let mut scan = true;
    let mut follow_up = false;
    let mut stage_failures = 0;
    let mut stage_retry_at = None;
    let mut shutdown = state.shutdown.clone();
    loop {
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
                        stage_retry_at = Some(Instant::now() + retry_delay(stage_failures));
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

// Check deferred addresses only at their deadline or after membership changes.
// Temporary unavailability of a slot or participant does not renew its budget.
fn retired_retries(
    tx: &Transaction<'_>,
    node: &NodeId,
    epoch: u64,
    now: i64,
    retries: &RetryMap,
) -> rusqlite::Result<Vec<Candidate>> {
    let mut retired = Vec::new();
    for (candidate, (attempt, phase)) in retries {
        if !matches!(phase, RetryPhase::Ready) {
            continue;
        }
        let live = attempt.epoch == epoch && tx.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM thread_members m JOIN threads t USING(thread_id)
             JOIN thread_posts p ON p.thread_id=m.thread_id AND p.seq=?4
             WHERE m.thread_id=?1 AND m.member_key=?2 AND m.delivery_generation=?3
               AND t.status='open' AND m.state='active' AND m.role!='observer' AND m.wake_policy!='muted'
               AND (m.delivery_mode='queue_until_idle' OR m.resume_through_seq IS NOT NULL)
               AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)=?4
               AND m.confirmed_seq<?4 AND p.expires_at_ms>?5)"
        )?.query_row(params![candidate.thread_id.as_str(), format!("{node}/{}",candidate.member_id),
            attempt.generation, attempt.wake_seq, now], |row| row.get::<_, bool>(0))?;
        if !live {
            retired.push(candidate.clone());
        }
    }
    Ok(retired)
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
        "SELECT m.participant_id,m.thread_id,m.member_key,m.resume_through_seq,m.last_wake_seq,
                m.delivery_generation,m.delivery_mode='direct_now'
         FROM thread_members m JOIN nodes n USING(node_id)
         JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
         JOIN threads t USING(thread_id)
         WHERE m.node_id=?1 AND n.session_epoch=?2 AND n.status='online' AND n.revoked_at_ms IS NULL
           AND t.status='open' AND m.state='active' AND m.pending_delivery_id IS NULL AND m.role!='observer'
           AND m.wake_policy!='muted' AND m.confirmed_seq<t.head_seq
           AND p.synced_epoch=?2 AND p.kind='agent'
           AND ((m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL AND m.last_wake_seq>0)
                OR (p.state='idle' AND p.idle_delivery_id IS NULL
                        AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)>m.retry_exhausted_seq
                    AND m.participant_id NOT IN (
                        SELECT participant_id FROM thread_members WHERE node_id=?1
                          AND state='active' AND role!='observer' AND wake_policy!='muted'
                          AND pending_delivery_id IS NOT NULL)
                    AND (m.last_wake_seq>m.confirmed_seq AND m.delivery_mode='queue_until_idle'
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
        let resume_through: Option<u64> = row.get(3)?;
        let last_wake: u64 = row.get(4)?;
        let direct = row.get::<_, bool>(6)? && resume_through.is_none();
        let retry = if direct {
            None
        } else {
            let wake_seq = last_wake.max(resume_through.unwrap_or(0));
            let generation = row.get(5)?;
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
        let through = if direct {
            posts::first_relevant_seq(
                tx,
                &candidate.thread_id,
                floor.max(last_wake.saturating_sub(1)),
                board.head_seq,
                &member,
            )?
        } else if recovery_ready
            || (member.delivery_mode == DeliveryMode::QueueUntilIdle && last_wake > floor)
        {
            Some(board.head_seq)
        } else {
            None
        };
        if direct {
            // Consume this one direct wake at admission. Other posts accepted
            // while it is pending retain their own chance in sequence order.
            let next = if let Some(seq) = through {
                posts::first_relevant_seq(tx, &candidate.thread_id, seq, board.head_seq, &member)?
            } else {
                None
            };
            tx.execute(
                "UPDATE thread_members SET last_wake_seq=?3 WHERE thread_id=?1 AND member_key=?2",
                params![candidate.thread_id.as_str(), key, next.unwrap_or(0)],
            )?;
        }
        if let Some(through) = through {
            let payload = materialize(tx, candidate, &member, floor, through, delivery_id)?;
            if !direct {
                tx.execute(
                    "UPDATE node_participants SET idle_delivery_id=?3 WHERE node_id=?1 AND participant_id=?2",
                    params![node_id.as_str(), payload.member_id.as_str(), payload.delivery_id.as_str()],
                )?;
            }
            tx.execute(
                "UPDATE thread_members SET pending_delivery_id=?1,pending_from_seq=?2,pending_to_seq=?3
                 WHERE thread_id=?4 AND member_key=?5",
                params![payload.delivery_id.as_str(), payload.from_seq, payload.to_seq, payload.thread_id.as_str(), key],
            )?;
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
    let node_id = node_id.clone();
    if let Err(error) = state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND idle_delivery_id=?2",
                params![node_id.as_str(), delivery_id.as_str()],
            )?;
            tx.execute(
                "UPDATE thread_members SET
                 last_wake_seq=CASE WHEN delivery_mode='direct_now' AND resume_through_seq IS NULL
                     THEN pending_to_seq ELSE last_wake_seq END,
                 pending_delivery_id=NULL,pending_from_seq=NULL,pending_to_seq=NULL
             WHERE node_id=?1 AND pending_delivery_id=?2 AND state='active'",
                params![node_id.as_str(), delivery_id.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await
    {
        tracing::warn!(%error, "Could not clear an unsent thread delivery");
    }
}

async fn deliver(
    state: &AppState,
    node_id: &NodeId,
    permit: Box<dyn ThreadPermit>,
    payload: ThreadDeliverPayload,
    acl: watch::Receiver<u64>,
    exhausted_wake: Option<u64>,
) -> Completion {
    use ThreadDeliveryOutcome as Outcome;
    let epoch = permit.epoch();
    let mut mark = Delivery::from_payload(node_id, epoch, &payload);
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
            Ok(true) if matches!(outcome, Outcome::Unavailable | Outcome::Expired) => {
                return Completion::Transient;
            }
            Ok(_) => return Completion::Finished,
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
    exhausted_wake: Option<u64>,
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
    let changed = tx.execute(
        "UPDATE thread_members SET state=CASE WHEN ?1 THEN 'paused_unknown' ELSE state END,
           retry_exhausted_seq=COALESCE(?11,retry_exhausted_seq),
           last_wake_seq=CASE WHEN ?1 AND delivery_mode='direct_now' THEN 0
                              WHEN ?10 AND delivery_mode='queue_until_idle' AND last_wake_seq<=?3 THEN 0
                              ELSE last_wake_seq END,
           confirmed_seq=CASE WHEN ?2 THEN MAX(confirmed_seq,?3) ELSE confirmed_seq END,
           pending_delivery_id=CASE WHEN ?1 THEN pending_delivery_id ELSE NULL END,
           pending_from_seq=CASE WHEN ?1 THEN pending_from_seq ELSE NULL END,
           pending_to_seq=CASE WHEN ?1 THEN pending_to_seq ELSE NULL END,
           resume_through_seq=CASE WHEN ?1 OR ((?2 OR ?10) AND resume_through_seq<=?3) THEN NULL
                                   ELSE resume_through_seq END
         WHERE thread_id=?4 AND member_key=?5 AND node_id=?6 AND state='active' AND delivery_generation=?7
           AND pending_delivery_id=?8 AND pending_to_seq=?3
           AND EXISTS(SELECT 1 FROM nodes WHERE node_id=?6 AND session_epoch=?9)",
        params![unknown, delivered, delivery.to_seq, delivery.candidate.thread_id.as_str(),
            delivery.member_key, delivery.node_id.as_str(), delivery.generation,
            delivery.delivery_id.as_str(), delivery.epoch, rejected, delivery.exhausted_wake],
    )?;
    // A leave, close or resume can retire the range while its ACK is in flight.
    // Release only this delivery's gate, even when the member CAS no longer matches.
    let blocked = matches!(
        outcome,
        ThreadDeliveryOutcome::Busy | ThreadDeliveryOutcome::AwaitingApproval
    );
    let released = if changed == 0 || !blocked {
        tx.execute(
            "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND participant_id=?2
             AND synced_epoch=?3 AND idle_delivery_id=?4",
            params![delivery.node_id.as_str(), delivery.candidate.member_id.as_str(),
                delivery.epoch, delivery.delivery_id.as_str()],
        )?
    } else {
        0
    };
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
    Ok(WriteOutcome::with_events(
        changed != 0 || released != 0,
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
        "UPDATE thread_members SET last_wake_seq=0 WHERE {node_filter} AND state='active'
           AND delivery_mode='direct_now' AND last_wake_seq>0",
    ))?
    .execute([node_id.map(NodeId::as_str)])?;
    tx.prepare_cached(&format!(
        "UPDATE thread_members SET retry_exhausted_seq=0 WHERE {node_filter}
         AND state='active' AND node_id IS NOT NULL AND retry_exhausted_seq>0",
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
