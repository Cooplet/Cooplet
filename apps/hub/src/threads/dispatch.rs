use std::{
    collections::{HashMap, HashSet},
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

#[derive(Clone, Copy, Default)]
struct Wake {
    revision: u64,
    idle_revision: u64,
}

impl Dispatch {
    fn lock(&self) -> MutexGuard<'_, HashMap<NodeId, watch::Sender<Wake>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) fn wake(state: &AppState, node_id: &NodeId) {
    signal(state, node_id, true);
}

fn signal(state: &AppState, node_id: &NodeId, projected_idle: bool) {
    let mut nodes = state.thread_dispatch.lock();
    if let Some(wake) = nodes.get(node_id) {
        wake.send_modify(|wake| {
            wake.revision = wake.revision.wrapping_add(1);
            if projected_idle {
                wake.idle_revision = wake.idle_revision.wrapping_add(1);
            }
        });
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
            "SELECT m.node_id, m.delivery_mode FROM thread_members m JOIN nodes n USING(node_id)
             WHERE m.thread_id=?1 AND m.state='active' AND m.node_id IS NOT NULL
               AND m.role!='observer' AND m.wake_policy!='muted' AND n.status='online'
               AND (m.delivery_mode='queue_until_idle' OR ?2 IS NOT NULL)
             GROUP BY m.node_id, m.delivery_mode",
        )?;
        Ok(stmt
            .query_map(params![id.as_str(), direct_seq], |row| {
                Ok((
                    parse_col::<NodeId>(row, 0)?,
                    parse_col::<DeliveryMode>(row, 1)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    })
    .await;
    match nodes {
        Ok(nodes) => {
            for (node_id, mode) in nodes {
                match (mode, direct_seq) {
                    (DeliveryMode::DirectNow, Some(seq)) => {
                        let state_copy = state.clone();
                        let thread_id = thread_id.clone();
                        let _ = state.background.spawn(async move {
                            drive_direct(state_copy, node_id, thread_id, seq).await;
                        });
                    }
                    (DeliveryMode::QueueUntilIdle, _) => signal(state, &node_id, false),
                    _ => {}
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
    Queue,
    Direct { thread_id: ThreadId, post_seq: u64 },
}

impl Mode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Queue => "queue_until_idle",
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
    suppressed: HashSet<Candidate>,
    pending: JoinSet<(Candidate, Completion)>,
}

impl Sweep {
    fn new() -> Self {
        Self {
            cursor: None,
            suppressed: HashSet::new(),
            pending: JoinSet::new(),
        }
    }

    async fn start(&mut self, state: &AppState, node_id: &NodeId, mode: Mode) -> Attempt {
        if state.shutdown.has_changed().unwrap_or(true) {
            return Attempt::Offline;
        }
        let permit = match state.bridge.reserve_thread(node_id) {
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
            let completion = deliver(&state, &node_id, permit, payload, acl).await;
            (output, completion)
        });
        self.cursor = Some(candidate);
        Attempt::Started
    }

    async fn next(&mut self) -> Option<(Candidate, Completion)> {
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
    while let Attempt::Started = sweep.start(&state, &node_id, mode.clone()).await {}
    while sweep.next().await.is_some() {}
    signal(&state, &node_id, false);
}

async fn drive_queue(state: AppState, node_id: NodeId, mut wake: watch::Receiver<Wake>) {
    let mut sweep = Sweep::new();
    let mut observed = *wake.borrow_and_update();
    let mut scan = true;
    let mut retry = false;
    let mut follow_up = false;
    let mut retries = 0;
    loop {
        let current = *wake.borrow_and_update();
        if observed.revision != current.revision {
            if observed.idle_revision != current.idle_revision {
                sweep.suppressed.clear();
                retries = 0;
            }
            observed = current;
            if scan {
                // Finish this keyset sweep before considering new work on earlier keys.
                follow_up = true;
            } else {
                sweep.cursor = None;
                scan = true;
            }
        }
        if scan {
            match sweep.start(&state, &node_id, Mode::Queue).await {
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
                completion = sweep.next() => if let Some((candidate, outcome)) = completion {
                    match outcome {
                        Completion::Delivered => follow_up = true,
                        Completion::WaitForIdle => { sweep.suppressed.insert(candidate); }
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
        if wake.borrow().revision == observed.revision {
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
    suppressed: &HashSet<Candidate>,
    delivery_id: OperationId,
) -> Result<Option<ThreadDeliverPayload>, HubError> {
    let only_thread = match mode {
        Mode::Queue => None,
        Mode::Direct { thread_id, .. } => Some(thread_id.as_str()),
    };
    let mut candidates = tx.prepare_cached(
        "SELECT m.participant_id,m.thread_id,m.member_key
         FROM thread_members m JOIN nodes n USING(node_id)
         JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
         JOIN threads t USING(thread_id)
         WHERE m.node_id=?1 AND n.session_epoch=?2 AND n.status='online' AND n.revoked_at_ms IS NULL
           AND t.status='open' AND m.state='active' AND m.pending_delivery_id IS NULL AND m.role!='observer'
           AND m.wake_policy!='muted' AND m.confirmed_seq<t.head_seq AND m.delivery_mode=?3
           AND p.synced_epoch=?2 AND p.kind='agent' AND (?3='direct_now' OR p.state='idle')
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
        if suppressed.contains(&candidate) {
            continue;
        }
        let key: String = row.get(2)?;
        let member =
            members::member(tx, &candidate.thread_id, &key)?.ok_or_else(store::not_found)?;
        let board = store::board(tx, &candidate.thread_id, now)?;
        let floor = member.confirmed_seq.max(board.floor_seq);
        let relevant = match mode {
            Mode::Queue => {
                posts::last_relevant_seq(tx, &candidate.thread_id, floor, board.head_seq, &member)?
            }
            Mode::Direct { post_seq, .. } if *post_seq > floor && *post_seq <= board.head_seq => {
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
        if let Some(through) = relevant {
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
    let changed = tx.execute(
        "UPDATE thread_members SET state=CASE WHEN ?1 THEN 'paused_unknown' ELSE state END,
           confirmed_seq=CASE WHEN ?2 THEN MAX(confirmed_seq,?3) ELSE confirmed_seq END,
           pending_delivery_id=CASE WHEN ?1 THEN pending_delivery_id ELSE NULL END,
           pending_from_seq=CASE WHEN ?1 THEN pending_from_seq ELSE NULL END,
           pending_to_seq=CASE WHEN ?1 THEN pending_to_seq ELSE NULL END
         WHERE thread_id=?4 AND member_key=?5 AND node_id=?6 AND state='active' AND delivery_generation=?7
           AND pending_delivery_id=?8 AND pending_to_seq=?3
           AND EXISTS(SELECT 1 FROM nodes WHERE node_id=?6 AND session_epoch=?9)",
        params![unknown, delivered, delivery.to_seq, delivery.candidate.thread_id.as_str(),
            delivery.member_key, delivery.node_id.as_str(), delivery.generation,
            delivery.delivery_id.as_str(), delivery.epoch],
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

fn pause_pending(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: Option<&NodeId>,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let mut statement = tx.prepare_cached(
        "UPDATE thread_members SET state='paused_unknown'
          WHERE state='active' AND pending_delivery_id IS NOT NULL AND (?1 IS NULL OR node_id=?1)
          RETURNING thread_id,member_key",
    )?;
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
            "UPDATE thread_members SET state='paused_unknown'
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
mod tests {
    use super::*;
    use cooplet_protocol::UserId;
    use rusqlite::Connection;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn queue_coalesces_once_idle_and_cannot_accept_an_old_generation_ack() -> TestResult {
        let mut db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("../storage/v1.sql"))?;
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
                &Mode::Queue,
                None,
                &HashSet::new(),
                OperationId::generate()?,
            )
        };
        assert!(attempt()?.is_none());
        tx.execute("UPDATE node_participants SET state='idle'", [])?;
        let payload = attempt()?.ok_or("no delivery after idle")?;
        assert_eq!(
            (payload.from_seq, payload.to_seq, payload.posts.len()),
            (1, 4, 4)
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
            "UPDATE thread_members SET state='active',delivery_generation=1,confirmed_seq=4,
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
                highest_dispatched_seq: 4,
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
}
