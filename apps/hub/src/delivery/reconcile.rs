use std::collections::HashSet;

use cooplet_protocol::{
    CommandOutcome, CommandResPayload, DeliveryMode, HubId, MAX_PENDING_OP_IDS, MessageId, NodeId,
    OperationId,
};
use cooplet_storage::EventNotice;
use rusqlite::{OptionalExtension, Transaction, params};

use crate::{
    AppState,
    error::HubError,
    storage::{
        operations::{self, FinishOperation},
        sql::{parse_col, parse_json, parse_opt_col},
    },
};

use super::{
    Kind, ReceiptContext,
    journal::{self, RecordedResult},
    message::{self, DeliveryUpdate},
    outcome::{self, TerminalState},
};

pub(crate) async fn pending_op_ids(
    state: &AppState,
    node_id: &NodeId,
) -> Result<Vec<OperationId>, HubError> {
    let node_id = node_id.clone();
    let wall_clock = state.wall_clock.clone();
    let ids = state
        .reads
        .read(move |db| {
            let mut query = db.prepare_cached(
                "SELECT operation_id FROM operations
             WHERE target_node_id = ?1 AND state IN ('accepted', 'in_progress', 'unknown')
               AND expires_at_ms > ?2
             ORDER BY updated_at_ms, operation_id LIMIT ?3",
            )?;
            query
                .query_map(
                    params![node_id.as_str(), wall_clock(), MAX_PENDING_OP_IDS],
                    |row| parse_col(row, 0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(Into::into)
        })
        .await?;
    Ok(ids)
}

struct PendingReceipt {
    actor_id: String,
    kind: String,
    resource_id: Option<String>,
    recorded: RecordedResult,
    live_delivery_mode: Option<DeliveryMode>,
}

pub(crate) fn reconcile_receipts(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: &NodeId,
    receipts: &[CommandResPayload],
    pending: &[OperationId],
    now: i64,
    events: &mut Vec<EventNotice>,
) -> Result<(), HubError> {
    let requested: HashSet<_> = pending.iter().collect();
    let mut seen = HashSet::with_capacity(receipts.len());
    let mut query = tx.prepare_cached(
        "SELECT o.actor_id, o.kind, o.resource_id, o.result_json, m.delivery_mode
           FROM operations o
           LEFT JOIN hub_messages m ON m.message_id = o.resource_id AND m.operation_id = o.operation_id
          WHERE o.operation_id = ?1 AND o.target_node_id = ?2 AND o.expires_at_ms > ?3
            AND o.state IN ('accepted', 'in_progress', 'unknown')",
    )?;
    for receipt in receipts {
        if !requested.contains(&receipt.op_id)
            || !seen.insert(&receipt.op_id)
            || receipt.outcome == CommandOutcome::Unknown
        {
            return Err(HubError::invalid(
                "Roster contains an invalid command receipt.",
            ));
        }
        let Some(saved) = query
            .query_row(
                params![receipt.op_id.as_str(), node_id.as_str(), now],
                |row| {
                    Ok(PendingReceipt {
                        actor_id: row.get(0)?,
                        kind: row.get(1)?,
                        resource_id: row.get(2)?,
                        recorded: parse_json(row, 3)?,
                        live_delivery_mode: parse_opt_col(row, 4)?,
                    })
                },
            )
            .optional()?
        else {
            // A live receipt may have won the race while the roster was in flight.
            continue;
        };
        if let Some(event) = reconcile_one(tx, hub_id, node_id, receipt, saved, now)? {
            events.push(event);
        }
    }
    Ok(())
}

fn reconcile_one(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    node_id: &NodeId,
    receipt: &CommandResPayload,
    mut saved: PendingReceipt,
    now: i64,
) -> Result<Option<EventNotice>, HubError> {
    let kind = Kind::from_operation(&saved.kind)
        .ok_or_else(|| HubError::internal("persisted bridge operation has an unknown kind"))?;
    let message_id = if matches!(kind, Kind::Message | Kind::Answer) {
        let resource_id = saved
            .resource_id
            .as_deref()
            .ok_or_else(|| HubError::internal("persisted bridge message has no resource id"))?;
        Some(MessageId::parse(resource_id).map_err(HubError::internal)?)
    } else {
        None
    };
    let context = ReceiptContext {
        id: &receipt.op_id,
        kind,
        message_id: message_id.as_ref(),
        in_reply_to: saved.recorded.in_reply_to.as_ref(),
        delivery_mode: saved.recorded.delivery_mode.or(saved.live_delivery_mode),
    };
    let terminal = outcome::classify(&context, Ok(receipt.clone()))?;
    if matches!(terminal.state, TerminalState::Unknown) {
        return Ok(None);
    }
    saved.recorded.set_success(terminal.success.as_ref());
    let problem = journal::encode_problem(&terminal)?;
    let result_json = serde_json::to_string(&saved.recorded)?;
    let resource_id = message_id
        .as_ref()
        .map(MessageId::as_str)
        .or(terminal.resource_id.as_deref());
    let changed = operations::reconcile(
        tx,
        node_id,
        FinishOperation {
            operation_id: &receipt.op_id,
            state: terminal.state.as_str(),
            resource_id,
            result_json: Some(&result_json),
            error_json: problem.as_deref(),
            now_ms: now,
        },
    )?;
    match (changed, message_id.as_ref(), terminal.delivery) {
        (true, Some(message_id), Some(delivery_state)) => message::finalize_reconciled(
            tx,
            hub_id,
            DeliveryUpdate {
                message_id,
                target_node_id: Some(node_id),
                required_nodes: &saved.recorded.required_node_ids,
                actor_id: &saved.actor_id,
                delivery_state,
                detail: terminal.delivery_detail.as_deref(),
                now,
            },
            &receipt.op_id,
        ),
        _ => Ok(None),
    }
}
