use std::{sync::Arc, time::Duration};

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{AttachmentRef, DeliveryState, MessageId, ParticipantId, ParticipantState};
use cooplet_storage::{EventNotice, WriteOutcome};
use rusqlite::{OptionalExtension, Transaction, params};

use super::{
    DeliveryService, SubmitFence,
    envelope::decode_queue_envelope,
    queries::{sql_error, update_delivery},
    send::{outcome_detail, submit_state},
};
use crate::{
    clients::{SubmitOutcome, SubmitTurnRequest, apply_submit_outcome},
    error::{NodeError, Result},
    storage::QUEUE_EXPIRED_DETAIL,
};

const QUEUE_LIMIT: u8 = 64;
pub(super) const ATTACHMENT_UNAVAILABLE: &str = "attachment_unavailable";
const CORRUPT_ENVELOPE: &str = "corrupt_envelope";
const RECEIPT_UNCONFIRMED: &str = "receipt_unconfirmed";

#[derive(Clone, Copy, PartialEq, Eq)]
enum PreClaim {
    Ready,
    Expired,
    CorruptEnvelope,
    AttachmentUnavailable,
}

impl DeliveryService {
    pub(super) fn wake(self: &Arc<Self>, id: ParticipantId) {
        if self.ensure_accepting().is_err() {
            return;
        }
        {
            let mut workers = self.lock_workers();
            if let Some(again) = workers.get_mut(&id) {
                *again = true;
                return;
            }
            workers.insert(id.clone(), false);
        }
        let service = self.clone();
        tokio::spawn(async move {
            let _admission = service.activity.read().await;
            loop {
                service.drain(&id).await;
                let mut workers = service.lock_workers();
                if service.ensure_accepting().is_ok()
                    && let Some(again) = workers.get_mut(&id)
                    && std::mem::take(again)
                {
                    continue;
                }
                workers.remove(&id);
                break;
            }
        });
    }

    async fn drain(&self, id: &ParticipantId) {
        let initial_delay = Duration::from_millis(20);
        let mut delay = initial_delay;
        let mut closing = self.closing.subscribe();
        while self.ensure_accepting().is_ok() {
            match self.drain_one(id).await {
                Ok(true) => delay = initial_delay,
                Ok(false) => break,
                Err(error) => {
                    // Re-read only durable queued work. A dispatched row can never
                    // be selected by this retry, even after an uncertain commit.
                    tracing::warn!(participant_id = %id, %error, "Retrying participant intake");
                    tokio::select! {
                        _ = closing.wait_for(|stopping| *stopping) => break,
                        () = tokio::time::sleep(delay) => {}
                    }
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
            }
        }
    }

    async fn drain_one(&self, id: &ParticipantId) -> Result<bool> {
        let Some(session) = self.agents.live_session(id) else {
            return Ok(false);
        };
        if session.state() != ParticipantState::Idle {
            return Ok(false);
        }
        let Some(item) = self.oldest(id).await? else {
            return Ok(false);
        };
        let decoded_input = decode_queue_envelope(&item.envelope, &item.message_id);
        let Ok(mut slot) = session.try_submit(item.message_id.as_str()) else {
            return Ok(false);
        };
        let generation = slot.generation();
        let staged = async {
            let now = (self.clock)()?;
            let pre_claim = if now >= item.deadline {
                PreClaim::Expired
            } else if decoded_input.is_err() {
                PreClaim::CorruptEnvelope
            } else if self.attachments_available(&item.attachments, now).await? {
                PreClaim::Ready
            } else {
                PreClaim::AttachmentUnavailable
            };
            Ok::<_, NodeError>((self.claim(id, generation, &item, pre_claim).await?, now))
        }
        .await;
        let (claim, now) = match staged {
            Ok(staged) => staged,
            Err(error) => {
                slot.release_without_wakeup();
                return Err(error);
            }
        };
        let admission = match claim {
            Claim::Dispatched(guard) => guard,
            Claim::Resolved => {
                slot.release_without_wakeup();
                return Ok(true);
            }
            Claim::Unavailable => {
                slot.release_without_wakeup();
                // An Idle event committed while this slot was reserved could
                // not wake the worker. Recheck after releasing it. Later Idle
                // events will observe the now-open slot and wake normally.
                return self.persisted_idle(id, generation).await;
            }
        };
        let Ok(input) = decoded_input else {
            slot.release_without_wakeup();
            return Ok(false);
        };
        let QueueItem {
            queue_id,
            message_id,
            deadline,
            ..
        } = item;
        let outcome = match (self.clock)() {
            Ok(now)
                if now < deadline && !admission.is_expired() && self.ensure_accepting().is_ok() =>
            {
                slot.submit(SubmitTurnRequest {
                    operation_key: message_id.to_string(),
                    message_id: Some(message_id.clone()),
                    input,
                    output_schema: None,
                    steer: false,
                    admission,
                })
                .await
            }
            Ok(now) if now >= deadline || admission.is_expired() => SubmitOutcome::Expired,
            _ => SubmitOutcome::Unavailable("The admission window closed before dispatch".into()),
        };
        let fence = SubmitFence {
            generation,
            after_seq: slot.submitted_after_seq(),
        };
        let meta = QueueMeta {
            queue_id,
            message_id,
            deadline,
        };
        // A successful claim has a durable fence. A receipt write may be retried,
        // but the native request is never repeated for this claim.
        let result = self
            .finish_queued(id, fence, &meta, &outcome, outcome_detail(&outcome), now)
            .await;
        let approval_current = match result {
            Ok(approval_current) => approval_current,
            Err(error) => {
                tracing::warn!(message_id = %meta.message_id, %error, "Queue delivery receipt was not confirmed");
                match self.is_dispatched(meta.queue_id).await {
                    Ok(false) => slot.finish(&outcome, false).await,
                    _ => {
                        let unknown = SubmitOutcome::Unknown(
                            "The queue delivery receipt could not be confirmed".into(),
                        );
                        slot.finish(&unknown, false).await;
                        self.resolve_unconfirmed_dispatch(id, fence, &meta, &unknown, now)
                            .await;
                    }
                }
                return Ok(false);
            }
        };
        slot.finish(&outcome, approval_current).await;
        // Busy and approval wait for a new adapter Idle event. A completed
        // turn is released by the slot machine, including events before ACK.
        Ok(matches!(
            outcome,
            SubmitOutcome::Expired | SubmitOutcome::Unavailable(_)
        ))
    }

    async fn resolve_unconfirmed_dispatch(
        &self,
        participant: &ParticipantId,
        fence: SubmitFence,
        meta: &QueueMeta,
        unknown: &SubmitOutcome,
        now: i64,
    ) {
        // Keep the worker registered for this participant until the durable
        // dispatch is resolved. Only the receipt transaction is retried.
        let mut closing = self.closing.subscribe();
        let mut delay = Duration::from_millis(20);
        while self.ensure_accepting().is_ok() {
            if self
                .finish_queued_once(
                    participant,
                    fence,
                    meta,
                    unknown,
                    Some(RECEIPT_UNCONFIRMED),
                    now,
                )
                .await
                .is_ok()
                || self
                    .is_dispatched(meta.queue_id)
                    .await
                    .is_ok_and(|dispatched| !dispatched)
            {
                break;
            }
            tokio::select! {
                biased;
                _ = closing.wait_for(|stopping| *stopping) => break,
                () = tokio::time::sleep(delay) => {}
            }
            delay = (delay * 2).min(Duration::from_millis(250));
        }
    }

    async fn persisted_idle(&self, participant: &ParticipantId, generation: u64) -> Result<bool> {
        let participant = participant.clone();
        Ok(self
            .reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM participants WHERE participant_id = ?1
                               AND generation = ?2 AND state = 'idle')",
                    params![participant.as_str(), generation],
                    |row| row.get(0),
                )?)
            })
            .await?)
    }

    async fn oldest(&self, participant: &ParticipantId) -> Result<Option<QueueItem>> {
        let participant = participant.clone();
        Ok(self.reads.read(move |conn| {
            Ok(conn.query_row(
                "SELECT q.queue_id,q.message_id,q.deadline_at_ms,q.payload_envelope,q.attachments_json
                 FROM intake_queue q
                 WHERE q.participant_id = ?1 AND q.state = 'queued'
                 ORDER BY q.queue_id LIMIT 1",
                [participant.as_str()],
                |row| Ok(QueueItem {
                    queue_id: row.get(0)?,
                    message_id: row.get::<_, String>(1)?.parse().map_err(sql_error)?,
                    deadline: row.get(2)?,
                    envelope: row.get(3)?,
                    attachments: serde_json::from_str(&row.get::<_, String>(4)?).map_err(sql_error)?,
                }),
            ).optional()?)
        }).await?)
    }

    async fn claim(
        &self,
        participant: &ParticipantId,
        generation: u64,
        item: &QueueItem,
        pre_claim: PreClaim,
    ) -> Result<Claim> {
        let participant = participant.clone();
        let queue_id = item.queue_id;
        let message_id = item.message_id.clone();
        let deadline = item.deadline;
        let attachments = item.attachments.clone();
        let clock = self.clock.clone();
        self.operations
            .finalize(move |tx| {
                let now = clock().map_err(NodeError::from)?;
                let terminal = if pre_claim == PreClaim::Expired || now >= deadline {
                    Some(("expired", DeliveryState::Expired, QUEUE_EXPIRED_DETAIL))
                } else if pre_claim == PreClaim::CorruptEnvelope {
                    Some(("cancelled", DeliveryState::Unavailable, CORRUPT_ENVELOPE))
                } else if pre_claim == PreClaim::AttachmentUnavailable
                    || !still_live(tx, &attachments, now)?
                {
                    Some((
                        "cancelled",
                        DeliveryState::Unavailable,
                        ATTACHMENT_UNAVAILABLE,
                    ))
                } else {
                    None
                };
                if let Some((queue, message, detail)) = terminal {
                    let updated = tx.execute(
                        "UPDATE intake_queue SET state = ?1, resolved_at_ms = ?2, payload_envelope = ''
                     WHERE queue_id = ?3 AND state = 'queued'",
                        params![queue, now, queue_id],
                    )?;
                    let mut events = Vec::new();
                    if updated == 1 {
                        events.push(update_delivery(
                            tx,
                            now,
                            message_id.as_str(),
                            message,
                            Some(detail),
                        )?);
                    }
                    return Ok(WriteOutcome::with_events(Claim::Resolved, events));
                }
                let updated = tx.execute(
                    "UPDATE intake_queue SET state = 'dispatched', dispatched_at_ms = ?1
                 WHERE queue_id = ?2 AND state = 'queued'
                   AND EXISTS (SELECT 1 FROM participants WHERE participant_id = ?3
                               AND generation = ?4 AND state = 'idle')",
                    params![now, queue_id, participant.as_str(), generation],
                )?;
                if updated == 0 {
                    return Ok(WriteOutcome::new(Claim::Unavailable));
                }
                let remaining =
                    u64::try_from(deadline.saturating_sub(now)).map_err(NodeError::internal)?;
                Ok(WriteOutcome::new(Claim::Dispatched(AdmissionGuard::new(
                    Duration::from_millis(remaining),
                ))))
            })
            .await
    }

    async fn finish_queued(
        &self,
        participant: &ParticipantId,
        fence: SubmitFence,
        meta: &QueueMeta,
        outcome: &SubmitOutcome,
        detail: Option<&'static str>,
        last_clock: i64,
    ) -> Result<bool> {
        let participant = participant.clone();
        let queue_id = meta.queue_id;
        let message_id = meta.message_id.clone();
        let deadline = meta.deadline;
        let outcome = outcome.clone();
        let clock = self.clock.clone();
        self.operations
            .finalize(move |tx| {
                finish_queued_tx(
                    tx,
                    &participant,
                    fence,
                    queue_id,
                    &message_id,
                    deadline,
                    &outcome,
                    detail,
                    clock.as_ref(),
                    last_clock,
                )
            })
            .await
    }

    async fn finish_queued_once(
        &self,
        participant: &ParticipantId,
        fence: SubmitFence,
        meta: &QueueMeta,
        outcome: &SubmitOutcome,
        detail: Option<&'static str>,
        last_clock: i64,
    ) -> Result<bool> {
        let participant = participant.clone();
        let queue_id = meta.queue_id;
        let message_id = meta.message_id.clone();
        let deadline = meta.deadline;
        let outcome = outcome.clone();
        let clock = self.clock.clone();
        self.db
            .write(move |tx| {
                finish_queued_tx(
                    tx,
                    &participant,
                    fence,
                    queue_id,
                    &message_id,
                    deadline,
                    &outcome,
                    detail,
                    clock.as_ref(),
                    last_clock,
                )
            })
            .await
            .map_err(Into::into)
    }

    async fn is_dispatched(&self, queue_id: i64) -> Result<bool> {
        Ok(self.reads.read(move |conn| {
            Ok(conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM intake_queue WHERE queue_id = ?1 AND state = 'dispatched')",
                [queue_id], |row| row.get(0),
            )?)
        }).await?)
    }
}

#[allow(clippy::too_many_arguments)]
fn finish_queued_tx(
    tx: &Transaction<'_>,
    participant: &ParticipantId,
    fence: SubmitFence,
    queue_id: i64,
    message_id: &MessageId,
    deadline: i64,
    outcome: &SubmitOutcome,
    detail: Option<&'static str>,
    clock: &super::Clock,
    last_clock: i64,
) -> std::result::Result<WriteOutcome<bool>, cooplet_storage::WriteError<NodeError>> {
    let now = clock().unwrap_or(last_clock);
    let awaiting_idle = matches!(
        outcome,
        SubmitOutcome::Busy | SubmitOutcome::AwaitingApproval(_)
    );
    let (queue, state, detail) = match outcome {
        SubmitOutcome::Accepted { .. } => ("delivered", DeliveryState::Delivered, detail),
        SubmitOutcome::Unknown(_) => ("unknown", DeliveryState::Unknown, detail),
        SubmitOutcome::Expired => ("expired", DeliveryState::Expired, detail),
        SubmitOutcome::Unavailable(_) => ("cancelled", DeliveryState::Unavailable, detail),
        _ if now >= deadline => (
            "expired",
            DeliveryState::Expired,
            Some(QUEUE_EXPIRED_DETAIL),
        ),
        _ => ("queued", submit_state(outcome), None),
    };
    if tx.execute(
        "UPDATE intake_queue
         SET state = ?1,
             resolved_at_ms = ?2,
             payload_envelope = CASE WHEN ?1 = 'queued' THEN payload_envelope ELSE '' END
         WHERE queue_id = ?3 AND state = 'dispatched'",
        params![queue, (queue != "queued").then_some(now), queue_id],
    )? != 1
    {
        return Err(rusqlite::Error::QueryReturnedNoRows.into());
    }
    let mut events = Vec::new();
    let approval = apply_submit_outcome(
        tx,
        now,
        participant,
        fence.generation,
        fence.after_seq,
        outcome,
        &mut events,
    )?;
    if !awaiting_idle || queue != "queued" {
        events.push(update_delivery(
            tx,
            now,
            message_id.as_str(),
            state,
            detail,
        )?);
    }
    Ok(WriteOutcome::with_events(approval.is_some(), events))
}

struct QueueMeta {
    queue_id: i64,
    message_id: MessageId,
    deadline: i64,
}

struct QueueItem {
    queue_id: i64,
    message_id: MessageId,
    deadline: i64,
    envelope: String,
    attachments: Vec<AttachmentRef>,
}

enum Claim {
    Dispatched(AdmissionGuard),
    Resolved,
    Unavailable,
}

fn still_live(
    tx: &Transaction<'_>,
    attachments: &[AttachmentRef],
    now: i64,
) -> rusqlite::Result<bool> {
    if attachments.is_empty() {
        return Ok(true);
    }
    let mut live = tx.prepare_cached(
        "SELECT EXISTS(SELECT 1 FROM blobs WHERE blob_id = ?1
           AND deleted_at_ms IS NULL AND expires_at_ms > ?2)",
    )?;
    for attachment in attachments {
        if !live.query_row(params![attachment.blob_id.as_str(), now], |row| {
            row.get::<_, bool>(0)
        })? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn enqueue(
    tx: &Transaction<'_>,
    participant: &ParticipantId,
    message: &MessageId,
    envelope: &str,
    attachments_json: &str,
    now: i64,
    ttl_ms: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO intake_queue (participant_id,message_id,payload_envelope,attachments_json,
                                   state,enqueued_at_ms,deadline_at_ms)
         VALUES (?1,?2,?3,?4,'queued',?5,?6)",
        params![
            participant.as_str(),
            message.as_str(),
            envelope,
            attachments_json,
            now,
            now.saturating_add(ttl_ms)
        ],
    )?;
    Ok(())
}

pub(super) fn queue_has_room(
    tx: &Transaction<'_>,
    participant: &ParticipantId,
) -> rusqlite::Result<bool> {
    let occupied: u64 = tx.query_row(
        "SELECT COUNT(*) FROM intake_queue WHERE participant_id = ?1 AND state IN ('queued','dispatched')",
        [participant.as_str()],
        |row| row.get(0),
    )?;
    Ok(occupied < u64::from(QUEUE_LIMIT))
}

pub(super) fn expire_for_recipient(
    tx: &Transaction<'_>,
    participant: &ParticipantId,
    now: i64,
    events: &mut Vec<EventNotice>,
) -> rusqlite::Result<()> {
    let mut statement = tx.prepare_cached(
        "UPDATE intake_queue SET state = 'expired', resolved_at_ms = ?1, payload_envelope = ''
         WHERE participant_id = ?2 AND state = 'queued' AND deadline_at_ms <= ?1
         RETURNING message_id",
    )?;
    let expired = statement
        .query_map(params![now, participant.as_str()], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in expired {
        events.push(update_delivery(
            tx,
            now,
            &id,
            DeliveryState::Expired,
            Some(QUEUE_EXPIRED_DETAIL),
        )?);
    }
    Ok(())
}
