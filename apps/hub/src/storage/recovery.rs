use cooplet_protocol::{
    AggregateType, DeliveryState, ErrorCode, EventEnvelope, EventKind, HubId, MessageId, NodeId,
    ProblemDetails,
};
use cooplet_storage::{StorageEngine, StorageError, WriteOutcome};
use rusqlite::{Transaction, params};

use super::{
    events::{
        EventAudience, NewEvent, advance_head, encode_error, message_delivery_updated_data,
        node_disconnected_data, record,
    },
    sql::{dialog_node_ids, nodes_audience, parse_col, parse_opt_col},
};

const RESTARTED: &str = "hub restarted before delivery confirmation";
pub(super) const RECOVERABLE_MESSAGES: &str =
    "SELECT msg.message_id, msg.sender_node_id, msg.target_node_id,
            parent.sender_node_id, parent.target_node_id
     FROM hub_messages msg LEFT JOIN hub_messages parent ON parent.message_id = msg.in_reply_to
     WHERE msg.delivery_state IN ('accepted','dispatched')";

pub(super) async fn recover(engine: &StorageEngine, hub_id: &HubId) -> Result<(), StorageError> {
    let hub_id = hub_id.clone();
    let result = engine
        .db()
        .write_sqlite(move |tx| recover_state(tx, &hub_id, crate::now_ms()))
        .await;
    if matches!(
        result,
        Err(StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_))
    ) {
        let settled = engine.reads().read(|connection| {
            Ok(connection.query_row(
                "SELECT NOT (EXISTS (SELECT 1 FROM nodes WHERE status = 'online')
                          OR EXISTS (SELECT 1 FROM operations WHERE state IN ('accepted','in_progress'))
                          OR EXISTS (SELECT 1 FROM hub_messages WHERE delivery_state IN ('accepted','dispatched')))",
                [],
                |row| row.get::<_, bool>(0),
            )?)
        }).await;
        if matches!(settled, Ok(true)) {
            return Ok(());
        }
    }
    result
}

fn recover_state(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    now: i64,
) -> rusqlite::Result<WriteOutcome<()>> {
    let mut events = Vec::new();
    recover_nodes(tx, hub_id, now, &mut events)?;
    let unknown =
        serde_json::to_string(&ProblemDetails::new(ErrorCode::DeliveryUnknown, RESTARTED))
            .map_err(encode_error)?;
    tx.execute(
        "UPDATE operations SET state = 'unknown', updated_at_ms = ?1, error_json = ?2
         WHERE state IN ('accepted','in_progress')",
        params![now, unknown],
    )?;
    recover_messages(tx, hub_id, now, &mut events)?;
    if let Some(last) = events.last() {
        advance_head(tx, last.seq)?;
    }
    Ok(WriteOutcome::with_events((), events))
}

fn recover_nodes(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    now: i64,
    events: &mut Vec<EventEnvelope>,
) -> rusqlite::Result<()> {
    let mut statement =
        tx.prepare("SELECT node_id, session_epoch FROM nodes WHERE status = 'online'")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let node_id: NodeId = parse_col(row, 0)?;
        let session_epoch: u64 = row.get(1)?;
        events.push(record(
            tx,
            hub_id,
            NewEvent {
                kind: EventKind::NodeDisconnected,
                aggregate_type: AggregateType::Node,
                aggregate_id: node_id.as_str(),
                actor_id: None,
                created_at_ms: now,
                data: node_disconnected_data(&node_id, "offline", session_epoch),
                audience: EventAudience::Nodes {
                    node_ids: vec![node_id.clone()],
                },
            },
        )?);
    }
    tx.execute(
        "UPDATE nodes SET status = 'offline' WHERE status = 'online'",
        [],
    )?;
    Ok(())
}

fn recover_messages(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    now: i64,
    events: &mut Vec<EventEnvelope>,
) -> rusqlite::Result<()> {
    let mut statement = tx.prepare(RECOVERABLE_MESSAGES)?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let message_id: MessageId = parse_col(row, 0)?;
        let target: Option<NodeId> = parse_opt_col(row, 2)?;
        let nodes = dialog_node_ids(
            parse_opt_col(row, 1)?,
            target.clone(),
            parse_opt_col(row, 3)?,
            parse_opt_col(row, 4)?,
        );
        events.push(record(
            tx,
            hub_id,
            NewEvent {
                kind: EventKind::MessageDeliveryUpdated,
                aggregate_type: AggregateType::Message,
                aggregate_id: message_id.as_str(),
                actor_id: None,
                created_at_ms: now,
                data: message_delivery_updated_data(
                    &message_id,
                    target.as_ref(),
                    DeliveryState::Unknown,
                    Some(RESTARTED),
                    None,
                ),
                audience: nodes_audience(&nodes),
            },
        )?);
    }
    tx.execute(
        "UPDATE hub_messages SET delivery_state = 'unknown', delivery_detail = ?1
         WHERE delivery_state IN ('accepted','dispatched')",
        [RESTARTED],
    )?;
    Ok(())
}
