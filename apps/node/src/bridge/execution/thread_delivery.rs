use cooplet_client::CoopletClient;
use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    ErrorCode, NodeId, OperationId, ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome,
    ThreadFenceReport,
};
use cooplet_storage::{WriteError, WriteOutcome};
use rusqlite::{Connection, OptionalExtension, params};

use crate::{
    clients::{
        InboundThreadBatch, InboundThreadPost, SlotDenial, SubmitOutcome, SubmitTurnRequest,
        TurnInput, VerifiedAuthor, apply_submit_outcome,
    },
    delivery::relay::{HubAttachment, cache_changed, refresh_hub_attachments},
    error::{NodeError, Result},
    http::NodeState,
    operations::{ADMISSION_EXPIRED, admission_expired},
};

const FENCE_TTL_MS: i64 = 90 * 24 * 60 * 60 * 1000;
type FenceResult = (ThreadDeliveryOutcome, Option<&'static str>);

pub(in crate::bridge) async fn thread(
    state: &NodeState,
    hub: &CoopletClient,
    request: ThreadDeliverPayload,
    admission: AdmissionGuard,
) -> ThreadAckPayload {
    let (outcome, detail) = match receive(state, hub, &request, &admission).await {
        Ok(outcome) => outcome,
        Err(error) if error.code() == ErrorCode::DeliveryUnknown => {
            tracing::warn!(delivery_id = %request.delivery_id, %error, "Thread delivery outcome is unknown");
            (
                ThreadDeliveryOutcome::Unknown,
                Some("fence_outcome_unknown"),
            )
        }
        Err(error)
            if error
                .0
                .extensions
                .get("reason")
                .and_then(serde_json::Value::as_str)
                == Some(ADMISSION_EXPIRED) =>
        {
            (ThreadDeliveryOutcome::Expired, Some(ADMISSION_EXPIRED))
        }
        Err(error) => {
            tracing::warn!(delivery_id = %request.delivery_id, %error, "Thread delivery could not be admitted");
            (
                ThreadDeliveryOutcome::Unavailable,
                Some("delivery_unavailable"),
            )
        }
    };
    ThreadAckPayload {
        delivery_generation: request.delivery_generation,
        delivery_id: request.delivery_id,
        thread_id: request.thread_id,
        member_id: request.member_id,
        acked_seq: request.to_seq,
        outcome,
        detail: detail.map(str::to_owned),
    }
}

async fn receive(
    state: &NodeState,
    hub: &CoopletClient,
    request: &ThreadDeliverPayload,
    admission: &AdmissionGuard,
) -> Result<FenceResult> {
    let query = request.clone();
    if let Some(previous) = state
        .reads
        .read(move |conn| Ok(read_fence(conn, &query)?))
        .await?
        .and_then(|fence| fence.replay(request))
    {
        return Ok(previous);
    }

    let Some(live) = state.agents.live_session(&request.member_id) else {
        return Ok((ThreadDeliveryOutcome::Unavailable, Some("no_live_session")));
    };
    let attachments = prepare_attachments(state, hub, request, admission).await?;
    let mut slot = match live.try_thread_submit(request.delivery_id.as_str()) {
        Ok(slot) => slot,
        Err(SlotDenial::Busy) => {
            return Ok((ThreadDeliveryOutcome::Busy, Some("participant_busy")));
        }
        Err(SlotDenial::AwaitingApproval) => {
            return Ok((
                ThreadDeliveryOutcome::AwaitingApproval,
                Some("awaiting_approval"),
            ));
        }
        Err(SlotDenial::Unavailable) => {
            return Ok((
                ThreadDeliveryOutcome::Unavailable,
                Some("intake_unavailable"),
            ));
        }
    };
    let Some(_gate) = state
        .delivery
        .thread_locks
        .try_lock((request.thread_id.clone(), request.member_id.clone()))
    else {
        // A prior session can still be settling a fence after its live slot was
        // replaced. Retry this transient conflict without waiting for Idle.
        return Ok((
            ThreadDeliveryOutcome::Unavailable,
            Some("thread_delivery_in_progress"),
        ));
    };

    let (admitted, attachments) = match begin(state, request, admission, attachments).await {
        Err(error) if cache_changed(&error) => {
            // GC may have won after we opened a cached file. The failed transaction
            // left the fence unchanged, so refetch once under the original guard.
            let attachments = prepare_attachments(state, hub, request, admission).await?;
            begin(state, request, admission, attachments).await?
        }
        result => result?,
    };
    let previous = match admitted {
        FenceAdmission::New(previous) => previous,
        FenceAdmission::Replay(result) => return Ok(result),
    };
    let input = make_input(request, attachments);
    let outcome = if admission.is_expired() {
        SubmitOutcome::Expired
    } else {
        slot.submit(SubmitTurnRequest {
            operation_key: request.delivery_id.to_string(),
            message_id: None,
            input,
            output_schema: None,
            steer: false,
            admission: admission.clone(),
        })
        .await
    };
    let generation = slot.generation();
    let event_seq = slot.submitted_after_seq();
    let finished = finish(state, request, previous, &outcome, generation, event_seq).await;
    match finished {
        Ok(approval_current) => {
            slot.finish(&outcome, approval_current).await;
            Ok(acknowledgment(&outcome))
        }
        Err(error) => {
            tracing::warn!(delivery_id = %request.delivery_id, %error, "Thread fence finalization could not be confirmed");
            slot.finish(
                &SubmitOutcome::Unknown("thread fence finalization failed".into()),
                false,
            )
            .await;
            Ok((
                ThreadDeliveryOutcome::Unknown,
                Some("fence_outcome_unknown"),
            ))
        }
    }
}

async fn prepare_attachments(
    state: &NodeState,
    hub: &CoopletClient,
    request: &ThreadDeliverPayload,
    admission: &AdmissionGuard,
) -> Result<Vec<Vec<HubAttachment>>> {
    let mut attachments = Vec::with_capacity(request.posts.len());
    for post in &request.posts {
        attachments.push(
            state
                .delivery
                .load_hub_attachments(hub, &post.attachments, admission, state.max_blob_bytes)
                .await?,
        );
    }
    Ok(attachments)
}

fn make_input(request: &ThreadDeliverPayload, attachments: Vec<Vec<HubAttachment>>) -> TurnInput {
    let posts = request
        .posts
        .iter()
        .zip(attachments)
        .map(|(post, attachments)| InboundThreadPost {
            seq: post.seq,
            post_id: post.post_id.clone(),
            author: VerifiedAuthor {
                kind: post.author_kind,
                actor_id: post.author_actor_id.clone(),
                name: post.author_handle.clone(),
                source_node_id: post
                    .author_actor_id
                    .split_once('/')
                    .and_then(|(node, _)| NodeId::parse(node).ok()),
            },
            in_reply_to_post_id: post.in_reply_to_post_id.clone(),
            body: post.body.clone(),
            mentions: post.mentions.clone(),
            attachments: attachments.iter().map(HubAttachment::resolved).collect(),
            question: post.question.clone(),
            answer: post.answer.clone(),
        })
        .collect();
    TurnInput::Thread(InboundThreadBatch {
        thread_id: request.thread_id.clone(),
        from_seq: request.from_seq,
        to_seq: request.to_seq,
        truncated_earlier: request.truncated_earlier,
        posts,
    })
}

#[derive(Clone)]
struct Fence {
    generation: u64,
    highest: u64,
    delivery_id: Option<String>,
    first: Option<u64>,
    state: String,
    updated_at: i64,
    expires_at: i64,
}

impl Fence {
    fn replay(&self, delivery: &ThreadDeliverPayload) -> Option<FenceResult> {
        use std::cmp::Ordering;
        match delivery.delivery_generation.cmp(&self.generation) {
            Ordering::Less => Some((ThreadDeliveryOutcome::Rejected, Some("stale_generation"))),
            Ordering::Greater => None,
            Ordering::Equal if self.state != "confirmed" => Some((
                ThreadDeliveryOutcome::Unknown,
                Some("prior_delivery_unknown"),
            )),
            Ordering::Equal
                if self.delivery_id.as_deref() == Some(delivery.delivery_id.as_str())
                    && self.highest == delivery.to_seq
                    && self.first == Some(delivery.from_seq) =>
            {
                Some((ThreadDeliveryOutcome::Delivered, None))
            }
            Ordering::Equal if delivery.from_seq <= self.highest => {
                Some((ThreadDeliveryOutcome::Unknown, Some("overlapping_range")))
            }
            Ordering::Equal => None,
        }
    }
}

fn read_fence(
    conn: &Connection,
    request: &ThreadDeliverPayload,
) -> rusqlite::Result<Option<Fence>> {
    conn.query_row(
        "SELECT delivery_generation, highest_dispatched_seq, last_delivery_id, last_from_seq,
                state, updated_at_ms, expires_at_ms
         FROM thread_inject_fences WHERE thread_id = ?1 AND participant_id = ?2",
        params![request.thread_id.as_str(), request.member_id.as_str()],
        |row| {
            Ok(Fence {
                generation: row.get(0)?,
                highest: row.get(1)?,
                delivery_id: row.get(2)?,
                first: row.get(3)?,
                state: row.get(4)?,
                updated_at: row.get(5)?,
                expires_at: row.get(6)?,
            })
        },
    )
    .optional()
}

enum FenceAdmission {
    New(Option<Fence>),
    Replay(FenceResult),
}

async fn begin(
    state: &NodeState,
    request: &ThreadDeliverPayload,
    admission: &AdmissionGuard,
    attachments: Vec<Vec<HubAttachment>>,
) -> Result<(FenceAdmission, Vec<Vec<HubAttachment>>)> {
    let request = request.clone();
    let admission = admission.clone();
    state
        .db
        .write(move |tx| -> std::result::Result<_, WriteError<NodeError>> {
            let previous = read_fence(tx, &request)?;
            if let Some(replay) = previous.as_ref().and_then(|fence| fence.replay(&request)) {
                return Ok(WriteOutcome::new((
                    FenceAdmission::Replay(replay),
                    attachments,
                )));
            }
            let now = crate::now_ms().map_err(NodeError::from)?;
            for post in &attachments {
                refresh_hub_attachments(tx, post, now)?;
            }
            if admission.is_expired() {
                return Err(admission_expired().into());
            }
            tx.execute(
                "INSERT INTO thread_inject_fences (
                thread_id, participant_id, delivery_generation, highest_dispatched_seq,
                last_delivery_id, last_from_seq, state, updated_at_ms, expires_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'injecting', ?7, ?8)
             ON CONFLICT(thread_id, participant_id) DO UPDATE SET
                delivery_generation = excluded.delivery_generation,
                highest_dispatched_seq = excluded.highest_dispatched_seq,
                last_delivery_id = excluded.last_delivery_id,
                last_from_seq = excluded.last_from_seq, state = excluded.state,
                updated_at_ms = excluded.updated_at_ms, expires_at_ms = excluded.expires_at_ms",
                params![
                    request.thread_id.as_str(),
                    request.member_id.as_str(),
                    request.delivery_generation,
                    request.to_seq,
                    request.delivery_id.as_str(),
                    request.from_seq,
                    now,
                    now.saturating_add(FENCE_TTL_MS)
                ],
            )?;
            Ok(WriteOutcome::new((
                FenceAdmission::New(previous),
                attachments,
            )))
        })
        .await
        .map_err(Into::into)
}

const CURRENT: &str = "WHERE thread_id = ?1 AND participant_id = ?2
    AND delivery_generation = ?3 AND last_delivery_id = ?4 AND last_from_seq = ?5
    AND highest_dispatched_seq = ?6 AND state = 'injecting'";

async fn finish(
    state: &NodeState,
    delivery: &ThreadDeliverPayload,
    previous: Option<Fence>,
    outcome: &SubmitOutcome,
    generation: u64,
    event_seq: u64,
) -> Result<bool> {
    let delivery = delivery.clone();
    let outcome = outcome.clone();
    state.operations.finalize(move |tx| {
        let now = crate::now_ms().map_err(NodeError::from)?;
        let key = (
            delivery.thread_id.as_str(), delivery.member_id.as_str(), delivery.delivery_generation,
            delivery.delivery_id.as_str(), delivery.from_seq, delivery.to_seq,
        );
        let count = match &outcome {
            SubmitOutcome::Accepted { .. } | SubmitOutcome::Unknown(_) => {
                let next = if matches!(outcome, SubmitOutcome::Accepted { .. }) { "confirmed" } else { "unknown" };
                tx.execute(
                    &format!("UPDATE thread_inject_fences SET state = ?7, updated_at_ms = ?8 {CURRENT}"),
                    params![key.0, key.1, key.2, key.3, key.4, key.5, next, now],
                )?
            }
            _ => match &previous {
                Some(prior) => tx.execute(
                    &format!("UPDATE thread_inject_fences SET delivery_generation = ?7,
                    highest_dispatched_seq = ?8, last_delivery_id = ?9, last_from_seq = ?10,
                    state = ?11, updated_at_ms = ?12, expires_at_ms = ?13 {CURRENT}"),
                    params![key.0, key.1, key.2, key.3, key.4, key.5, prior.generation,
                        prior.highest, prior.delivery_id, prior.first, prior.state, prior.updated_at, prior.expires_at],
                )?,
                None => tx.execute(
                    &format!("DELETE FROM thread_inject_fences {CURRENT}"),
                    params![key.0, key.1, key.2, key.3, key.4, key.5],
                )?,
            }
        };
        if count != 1 {
            return Err(NodeError::new(ErrorCode::DeliveryUnknown, "The thread delivery fence changed").into());
        }
        let mut events = Vec::new();
        let approval = apply_submit_outcome(
            tx, now, &delivery.member_id, generation, event_seq, &outcome, &mut events,
        )?;
        Ok(WriteOutcome::with_events(approval.is_some(), events))
    }).await
}

fn acknowledgment(outcome: &SubmitOutcome) -> FenceResult {
    match outcome {
        SubmitOutcome::Accepted { .. } => (ThreadDeliveryOutcome::Delivered, None),
        SubmitOutcome::Busy => (ThreadDeliveryOutcome::Busy, Some("participant_busy")),
        SubmitOutcome::AwaitingApproval(_) => (
            ThreadDeliveryOutcome::AwaitingApproval,
            Some("awaiting_approval"),
        ),
        SubmitOutcome::Unavailable(_) => (
            ThreadDeliveryOutcome::Unavailable,
            Some("adapter_unavailable"),
        ),
        SubmitOutcome::Expired => (ThreadDeliveryOutcome::Expired, Some(ADMISSION_EXPIRED)),
        SubmitOutcome::Unknown(_) => (
            ThreadDeliveryOutcome::Unknown,
            Some("adapter_outcome_unknown"),
        ),
    }
}

pub(in crate::bridge) fn visit_fences(
    conn: &Connection,
    mut visit: impl FnMut(ThreadFenceReport) -> Result<()>,
) -> Result<()> {
    let mut statement = conn.prepare_cached(
        "SELECT thread_id, participant_id, delivery_generation, highest_dispatched_seq,
                last_delivery_id, last_from_seq
         FROM thread_inject_fences WHERE state IN ('injecting', 'unknown')",
    )?;
    let rows = statement.query_map([], |row| {
        let parse_error = |error| rusqlite::Error::ToSqlConversionFailure(Box::new(error));
        Ok(ThreadFenceReport {
            thread_id: row.get::<_, String>(0)?.parse().map_err(parse_error)?,
            participant_id: row.get::<_, String>(1)?.parse().map_err(parse_error)?,
            delivery_generation: row.get(2)?,
            highest_dispatched_seq: row.get(3)?,
            last_delivery_id: row
                .get::<_, Option<String>>(4)?
                .map(|id| OperationId::parse(&id))
                .transpose()
                .map_err(parse_error)?,
            last_from_seq: row.get(5)?,
        })
    })?;
    for row in rows {
        visit(row?)?;
    }
    Ok(())
}
