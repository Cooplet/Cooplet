use cooplet_protocol::{
    AggregateType, DeliveryMode, EventKind, HubId, MemberView, NodeId, OperationId, ParticipantId,
    ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadFenceReport, ThreadId,
    ThreadRole, ThreadWakePolicy,
};
use cooplet_storage::{EventNotice, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};

use crate::{
    error::HubError,
    storage::{
        events::{self, EventAudience, NewEvent},
        sql::{json_byte_len, parse_col},
    },
};

use crate::threads::{members, posts, store};

const MAX_DELIVERY_BYTES: usize = 128 * 1024;
const MAX_DELIVERY_POSTS: usize = 20;
const INLINE_SCAN_LIMIT: u64 = 64;

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct Candidate {
    pub(super) member_id: ParticipantId,
    pub(super) thread_id: ThreadId,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct RetryAttempt {
    pub(super) epoch: u64,
    pub(super) generation: u64,
    pub(super) wake_seq: u64,
    pub(super) number: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PrescanKind {
    ResumeLast { floor: u64, through: u64 },
    DirectFirst { after_seq: u64, head_seq: u64 },
}

pub(super) struct PrescanRequest {
    pub(super) candidate: Candidate,
    pub(super) member: MemberView,
    pub(super) kind: PrescanKind,
}

#[derive(Clone)]
pub(super) struct PrescanHint {
    pub(super) candidate: Candidate,
    pub(super) kind: PrescanKind,
    pub(super) found: Option<u64>,
}

pub(super) enum StageResult {
    Ready(ThreadDeliverPayload, Option<RetryAttempt>),
    NeedsPrescan(Box<PrescanRequest>),
    None,
}

pub(in crate::threads) struct WaitSchedule<'a> {
    pub thread_id: &'a ThreadId,
    pub member_key: &'a str,
    pub since_seq: u64,
    pub head_seq: u64,
    pub wake_policy: ThreadWakePolicy,
    pub policy_changed: bool,
    pub relevant_last: Option<u64>,
}

pub(in crate::threads) struct RejoinSchedule<'a> {
    pub thread_id: &'a ThreadId,
    pub member_key: &'a str,
    pub node_id: Option<&'a NodeId>,
    pub role: ThreadRole,
    pub wake_policy: ThreadWakePolicy,
    pub delivery_mode: DeliveryMode,
    pub disputed: bool,
    pub confirmed_seq: u64,
    pub now_ms: i64,
}

pub(in crate::threads) struct ResumeSchedule<'a> {
    pub thread_id: &'a ThreadId,
    pub member_key: &'a str,
    pub delivery_mode: DeliveryMode,
    pub generation: u64,
    pub after_seq: u64,
    pub head_seq: u64,
    pub relevant_last: Option<u64>,
}

#[derive(Clone)]
pub(super) struct Delivery {
    pub(super) node_id: NodeId,
    pub(super) epoch: u64,
    pub(super) candidate: Candidate,
    pub(super) member_key: String,
    pub(super) generation: u64,
    pub(super) delivery_id: OperationId,
    pub(super) to_seq: u64,
    pub(super) has_idle_gate: bool,
    pub(super) exhausted_wake: Option<u64>,
}

impl Delivery {
    pub(super) fn from_payload(
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

    pub(super) fn matches(&self, ack: &ThreadAckPayload) -> bool {
        ack.delivery_generation == self.generation
            && ack.delivery_id == self.delivery_id
            && ack.thread_id == self.candidate.thread_id
            && ack.member_id == self.candidate.member_id
            && ack.acked_seq == self.to_seq
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

pub(in crate::threads) fn record_post_wakes(
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

pub(in crate::threads) fn record_wait(
    tx: &Transaction<'_>,
    schedule: WaitSchedule<'_>,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE thread_members SET
           confirmed_seq=MAX(confirmed_seq,?1),
           wake_policy=?2,
           last_wake_seq=CASE WHEN delivery_mode='queue_until_idle' AND ?3 THEN ?4
                              WHEN delivery_mode='queue_until_idle' AND ?1>=?5 THEN 0
                              ELSE last_wake_seq END,
           direct_wake_seq=CASE WHEN delivery_mode='direct_now' AND (?1>=?5 OR ?2='muted') THEN 0
                                ELSE direct_wake_seq END,
           retry_exhausted_seq=CASE WHEN ?3 THEN 0 ELSE retry_exhausted_seq END,
           resume_through_seq=CASE WHEN resume_through_seq<=?1 THEN NULL ELSE resume_through_seq END
         WHERE thread_id=?6 AND member_key=?7",
    )?
    .execute(params![
        schedule.since_seq,
        schedule.wake_policy.as_str(),
        schedule.policy_changed,
        schedule.relevant_last.unwrap_or(0),
        schedule.head_seq,
        schedule.thread_id.as_str(),
        schedule.member_key,
    ])?;
    Ok(())
}

pub(in crate::threads) fn reset_member_on_rejoin(
    tx: &Transaction<'_>,
    schedule: RejoinSchedule<'_>,
) -> rusqlite::Result<()> {
    tx.prepare_cached(
        "UPDATE thread_members SET node_id=?1, role=?2, wake_policy=?3, delivery_mode=?4, state=?5,
         confirmed_seq=?6, last_wake_seq=0, direct_wake_seq=0, retry_exhausted_seq=0,
         resume_through_seq=NULL, joined_at_ms=?7, left_at_ms=NULL
         WHERE thread_id=?8 AND member_key=?9",
    )?
    .execute(params![
        schedule.node_id.map(NodeId::as_str),
        schedule.role.as_str(),
        schedule.wake_policy.as_str(),
        schedule.delivery_mode.as_str(),
        if schedule.disputed {
            "paused_unknown"
        } else {
            "active"
        },
        schedule.confirmed_seq,
        schedule.now_ms,
        schedule.thread_id.as_str(),
        schedule.member_key,
    ])?;
    Ok(())
}

pub(in crate::threads) fn reset_member_on_leave(
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

pub(in crate::threads) fn apply_resume_schedule(
    tx: &Transaction<'_>,
    schedule: ResumeSchedule<'_>,
) -> rusqlite::Result<()> {
    let (last_wake, resume_through) = match schedule.delivery_mode {
        DeliveryMode::QueueUntilIdle => (schedule.relevant_last.unwrap_or(0), None),
        DeliveryMode::DirectNow => (0, schedule.relevant_last.map(|_| schedule.head_seq)),
    };
    tx.prepare_cached(
        "UPDATE thread_members SET state='active', delivery_generation=?1, confirmed_seq=?2,
         pending_delivery_id=NULL, pending_from_seq=NULL, pending_to_seq=NULL,
         resume_through_seq=?3, last_wake_seq=?4, direct_wake_seq=0, retry_exhausted_seq=0
         WHERE thread_id=?5 AND member_key=?6",
    )?
    .execute(params![
        schedule.generation,
        schedule.after_seq,
        resume_through,
        last_wake,
        schedule.thread_id.as_str(),
        schedule.member_key,
    ])?;
    Ok(())
}

pub(super) fn clear_unsent_tx(
    tx: &Transaction<'_>,
    node_id: &NodeId,
    delivery_id: &OperationId,
) -> rusqlite::Result<WriteOutcome<()>> {
    tx.prepare_cached(
        "UPDATE node_participants SET idle_delivery_id=NULL WHERE node_id=?1 AND idle_delivery_id=?2",
    )?
    .execute(params![node_id.as_str(), delivery_id.as_str()])?;
    tx.prepare_cached(
        "UPDATE thread_members SET
           direct_wake_seq=CASE WHEN delivery_mode='direct_now' AND resume_through_seq IS NULL
               AND (direct_wake_seq=0 OR direct_wake_seq>pending_to_seq)
               THEN pending_to_seq ELSE direct_wake_seq END,
           pending_delivery_id=NULL,pending_from_seq=NULL,pending_to_seq=NULL
         WHERE node_id=?1 AND pending_delivery_id=?2 AND state='active'",
    )?
    .execute(params![node_id.as_str(), delivery_id.as_str()])?;
    Ok(WriteOutcome::new(()))
}

pub(crate) const STAGE_CANDIDATES_SQL: &str = "SELECT m.participant_id,m.thread_id,m.member_key,m.handle,m.role,m.wake_policy,
        m.delivery_mode,m.state,m.delivery_generation,m.confirmed_seq,
        m.resume_through_seq,m.last_wake_seq,m.direct_wake_seq,t.head_seq,t.floor_seq
 FROM thread_members m JOIN nodes n USING(node_id)
 JOIN node_participants p ON p.node_id=m.node_id AND p.participant_id=m.participant_id
 JOIN threads t USING(thread_id)
 WHERE m.node_id=?1 AND n.session_epoch=?2 AND n.status='online' AND n.revoked_at_ms IS NULL
   AND t.status='open' AND m.state='active' AND m.node_id IS NOT NULL AND m.pending_delivery_id IS NULL
   AND m.role!='observer' AND m.wake_policy!='muted' AND m.confirmed_seq<t.head_seq
   AND (m.direct_wake_seq>0 OR m.last_wake_seq>m.confirmed_seq OR m.resume_through_seq IS NOT NULL)
   AND p.synced_epoch=?2 AND p.kind='agent'
   AND ((m.delivery_mode='direct_now' AND m.resume_through_seq IS NULL AND m.direct_wake_seq>0)
        OR (p.state='idle' AND p.idle_delivery_id IS NULL
                AND MAX(COALESCE(m.resume_through_seq,0),m.last_wake_seq)>m.retry_exhausted_seq
            AND m.participant_id NOT IN (
                SELECT participant_id FROM thread_members WHERE node_id=?1
                  AND state='active' AND pending_delivery_id IS NOT NULL)
            AND ((m.delivery_mode='queue_until_idle' AND m.last_wake_seq>m.confirmed_seq AND m.last_wake_seq>t.floor_seq)
                 OR m.resume_through_seq IS NOT NULL)))
   AND (m.participant_id,m.thread_id)>(?3,?4)
 ORDER BY m.participant_id,m.thread_id";

pub(super) fn stage(
    tx: &Transaction<'_>,
    node_id: &NodeId,
    (epoch, now): (u64, i64),
    cursor: Option<&Candidate>,
    retry_for: impl Fn(&Candidate, u64, u64, u64) -> Option<usize>,
    prescans: &[PrescanHint],
    delivery_id: OperationId,
) -> Result<StageResult, HubError> {
    let mut candidates = tx.prepare_cached(STAGE_CANDIDATES_SQL)?;
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
            let Some(number) = retry_for(&candidate, epoch, generation, wake_seq) else {
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
            let relevant = if floor < through {
                match resolve_resume_last(tx, &candidate, &member, floor, through, prescans)? {
                    ScanOutcome::Done(found) => found.is_some(),
                    ScanOutcome::NeedsPrescan(req) => return Ok(StageResult::NeedsPrescan(req)),
                }
            } else {
                false
            };
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
            let after = floor.max(direct_wake.saturating_sub(1));
            match resolve_direct_first(tx, &candidate, &member, after, head_seq, prescans)? {
                ScanOutcome::Done(found) => found,
                ScanOutcome::NeedsPrescan(req) => return Ok(StageResult::NeedsPrescan(req)),
            }
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
        let next_direct = if direct {
            if let Some(seq) = through {
                match resolve_direct_first(tx, &candidate, &member, seq, head_seq, prescans)? {
                    ScanOutcome::Done(found) => found,
                    ScanOutcome::NeedsPrescan(req) => return Ok(StageResult::NeedsPrescan(req)),
                }
            } else {
                None
            }
        } else {
            None
        };
        if direct {
            // Consume this one direct wake at admission. Other posts accepted
            // while it is pending retain their own chance in sequence order.
            tx.prepare_cached(
                "UPDATE thread_members SET direct_wake_seq=?3 WHERE thread_id=?1 AND member_key=?2",
            )?
            .execute(params![
                candidate.thread_id.as_str(),
                key,
                next_direct.unwrap_or(0)
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
            return Ok(StageResult::Ready(payload, retry));
        }
    }
    Ok(StageResult::None)
}

enum ScanOutcome {
    Done(Option<u64>),
    NeedsPrescan(Box<PrescanRequest>),
}

fn resolve_resume_last(
    tx: &Transaction<'_>,
    candidate: &Candidate,
    member: &MemberView,
    floor: u64,
    through: u64,
    prescans: &[PrescanHint],
) -> Result<ScanOutcome, HubError> {
    if member.wake_policy != ThreadWakePolicy::MentionsOnly
        || through.saturating_sub(floor) <= INLINE_SCAN_LIMIT
    {
        return Ok(ScanOutcome::Done(posts::last_relevant_seq(
            tx,
            &candidate.thread_id,
            floor,
            through,
            member,
        )?));
    }
    for hint in prescans.iter().rev() {
        if hint.candidate == *candidate
            && let PrescanKind::ResumeLast {
                floor: pre_floor,
                through: pre_through,
            } = hint.kind
            && pre_floor == floor
            && through >= pre_through
            && through.saturating_sub(pre_through) <= INLINE_SCAN_LIMIT
        {
            let delta = if through > pre_through {
                posts::last_relevant_seq(tx, &candidate.thread_id, pre_through, through, member)?
            } else {
                None
            };
            return Ok(ScanOutcome::Done(delta.or(hint.found)));
        }
    }
    Ok(ScanOutcome::NeedsPrescan(Box::new(PrescanRequest {
        candidate: candidate.clone(),
        member: member.clone(),
        kind: PrescanKind::ResumeLast { floor, through },
    })))
}

fn resolve_direct_first(
    tx: &Transaction<'_>,
    candidate: &Candidate,
    member: &MemberView,
    after_seq: u64,
    head_seq: u64,
    prescans: &[PrescanHint],
) -> Result<ScanOutcome, HubError> {
    if after_seq >= head_seq {
        return Ok(ScanOutcome::Done(None));
    }
    if member.wake_policy != ThreadWakePolicy::MentionsOnly
        || head_seq.saturating_sub(after_seq) <= INLINE_SCAN_LIMIT
    {
        return Ok(ScanOutcome::Done(posts::first_relevant_seq(
            tx,
            &candidate.thread_id,
            after_seq,
            head_seq,
            member,
        )?));
    }
    for hint in prescans.iter().rev() {
        if hint.candidate == *candidate
            && let PrescanKind::DirectFirst {
                after_seq: pre_after,
                head_seq: pre_head,
            } = hint.kind
            && pre_after == after_seq
            && head_seq >= pre_head
        {
            if hint.found.is_some() {
                return Ok(ScanOutcome::Done(hint.found));
            }
            if head_seq.saturating_sub(pre_head) <= INLINE_SCAN_LIMIT {
                let delta = if head_seq > pre_head {
                    posts::first_relevant_seq(tx, &candidate.thread_id, pre_head, head_seq, member)?
                } else {
                    None
                };
                return Ok(ScanOutcome::Done(delta));
            }
        }
    }
    Ok(ScanOutcome::NeedsPrescan(Box::new(PrescanRequest {
        candidate: candidate.clone(),
        member: member.clone(),
        kind: PrescanKind::DirectFirst {
            after_seq,
            head_seq,
        },
    })))
}

fn materialize(
    tx: &Transaction<'_>,
    candidate: Candidate,
    member: &MemberView,
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

pub(super) fn finish(
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

pub(crate) const PAUSE_ALL_PENDING_SQL: &str =
    "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
     WHERE state='active' AND pending_delivery_id IS NOT NULL
     RETURNING thread_id,member_key";

fn pause_pending(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: Option<&NodeId>,
    now: i64,
    notices: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    // Direct wakes belong to one online session. A new epoch starts with only
    // durable queue / explicit recovery work, and can acknowledge a fresh Idle.
    if let Some(node_id) = node_id {
        tx.prepare_cached(
            "UPDATE thread_members SET direct_wake_seq=0, retry_exhausted_seq=0
             WHERE node_id=?1 AND state='active' AND node_id IS NOT NULL
               AND (direct_wake_seq>0 OR retry_exhausted_seq>0)",
        )?
        .execute([node_id.as_str()])?;
    } else {
        tx.prepare_cached(
            "UPDATE thread_members SET direct_wake_seq=0, retry_exhausted_seq=0
             WHERE state='active' AND node_id IS NOT NULL
               AND (direct_wake_seq>0 OR retry_exhausted_seq>0)",
        )?
        .execute([])?;
    }
    let keys = if let Some(node_id) = node_id {
        let mut statement = tx.prepare_cached(
            "UPDATE thread_members SET state='paused_unknown',resume_through_seq=NULL
             WHERE node_id=?1 AND state='active' AND pending_delivery_id IS NOT NULL
             RETURNING thread_id,member_key",
        )?;
        statement
            .query_map([node_id.as_str()], |row| {
                Ok((parse_col::<ThreadId>(row, 0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    } else {
        let mut statement = tx.prepare_cached(PAUSE_ALL_PENDING_SQL)?;
        statement
            .query_map([], |row| {
                Ok((parse_col::<ThreadId>(row, 0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
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
