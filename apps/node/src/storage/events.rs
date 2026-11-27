use std::io;

use cooplet_protocol::{
    AggregateType, EventEnvelope, EventId, EventKind, NodeId, ParticipantState, SourceId,
    SourceKind,
};
use cooplet_storage::EventNotice;
use rusqlite::{Row, Transaction, params};
use serde::Serialize;
use serde_json::{Value, json};

use super::bad_data;

pub const MAX_EVENT_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, thiserror::Error)]
#[error("event frame exceeds the wire size limit")]
struct EventFrameTooLarge;

pub fn event_frame_too_large(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::ToSqlConversionFailure(cause) if cause.is::<EventFrameTooLarge>())
}

struct ByteCounter(usize);

impl io::Write for ByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Measure the serialized JSON length of `value` without allocating an output buffer.
pub(crate) fn json_encoded_len<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<usize> {
    let mut counter = ByteCounter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BorrowedEventFrame<'a> {
    Event { event: &'a EventEnvelope },
}

/// Append a validated event and advance the head in the caller's transaction.
/// Publish the returned notice through the transaction's write outcome.
pub fn append_event(
    tx: &Transaction<'_>,
    now_ms: i64,
    kind: EventKind,
    aggregate_type: AggregateType,
    aggregate_id: &str,
    actor_id: Option<&str>,
    payload: Value,
) -> rusqlite::Result<EventNotice> {
    let node_id = read_node_id(tx)?;
    insert_event(
        tx,
        node_id,
        now_ms,
        kind,
        aggregate_type,
        aggregate_id,
        actor_id,
        payload,
    )
}

/// Check the frame the next insert would produce without advancing the outbox.
pub(crate) fn check_next_event(
    tx: &Transaction<'_>,
    now_ms: i64,
    kind: EventKind,
    aggregate_type: AggregateType,
    aggregate_id: &str,
    actor_id: Option<&str>,
    payload: Value,
) -> rusqlite::Result<()> {
    let seq = next_event_seq(tx)?;
    check_event_frame(&EventEnvelope {
        seq,
        event_id: EventId::from_parts(0, [0; 10]).map_err(bad_data)?,
        source_kind: SourceKind::Node,
        source_id: SourceId::Node(read_node_id(tx)?),
        kind,
        aggregate_type,
        aggregate_id: aggregate_id.into(),
        actor_id: actor_id.map(str::to_owned),
        created_at_ms: now_ms,
        payload,
    })
}

fn next_event_seq(tx: &Transaction<'_>) -> rusqlite::Result<u64> {
    tx.query_row(
        "SELECT events_head_seq + 1 FROM node_meta WHERE id = 1",
        [],
        |row| row.get(0),
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_event(
    tx: &Transaction<'_>,
    node_id: NodeId,
    now_ms: i64,
    kind: EventKind,
    aggregate_type: AggregateType,
    aggregate_id: &str,
    actor_id: Option<&str>,
    payload: Value,
) -> rusqlite::Result<EventNotice> {
    let event_id = EventId::generate().map_err(bad_data)?;
    let expected_seq = next_event_seq(tx)?;
    let mut event = EventEnvelope {
        seq: expected_seq,
        event_id,
        source_kind: SourceKind::Node,
        source_id: SourceId::Node(node_id),
        kind,
        aggregate_type,
        aggregate_id: aggregate_id.into(),
        actor_id: actor_id.map(str::to_owned),
        created_at_ms: now_ms,
        payload,
    };
    check_event_frame(&event)?;
    let payload_json = serde_json::to_string(&event.payload).map_err(bad_data)?;
    let seq: u64 = tx.query_row(
        "INSERT INTO events (event_id, kind, aggregate_type, aggregate_id, actor_id, payload_json, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         RETURNING seq",
        params![
            event.event_id.as_str(),
            kind.as_str(),
            aggregate_type.as_str(),
            aggregate_id,
            actor_id,
            payload_json,
            now_ms
        ],
        |row| row.get(0),
    )?;
    if seq != expected_seq {
        event.seq = seq;
        check_event_frame(&event)?;
    }
    tx.execute(
        "UPDATE node_meta SET events_head_seq = ?1 WHERE id = 1",
        [seq],
    )?;
    Ok(EventNotice { seq, kind })
}

fn check_event_frame(event: &EventEnvelope) -> rusqlite::Result<()> {
    event.validate().map_err(bad_data)?;
    if json_encoded_len(&BorrowedEventFrame::Event { event }).map_err(bad_data)?
        > MAX_EVENT_FRAME_BYTES
    {
        return Err(bad_data(EventFrameTooLarge));
    }
    Ok(())
}

pub(crate) fn read_node_id(tx: &Transaction<'_>) -> rusqlite::Result<NodeId> {
    let id: String = tx.query_row("SELECT node_id FROM node_meta WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    id.parse().map_err(bad_data)
}

/// Decode a row projected as (seq, event_id, kind, aggregate_type, aggregate_id,
/// actor_id, payload_json, created_at_ms).
pub fn read_event(row: &Row<'_>, node_id: &NodeId) -> rusqlite::Result<EventEnvelope> {
    let event = EventEnvelope {
        seq: row.get(0)?,
        event_id: row.get::<_, String>(1)?.parse().map_err(bad_data)?,
        source_kind: SourceKind::Node,
        source_id: SourceId::Node(node_id.clone()),
        kind: row.get::<_, String>(2)?.parse().map_err(bad_data)?,
        aggregate_type: row.get::<_, String>(3)?.parse().map_err(bad_data)?,
        aggregate_id: row.get(4)?,
        actor_id: row.get(5)?,
        payload: serde_json::from_str(&row.get::<_, String>(6)?).map_err(bad_data)?,
        created_at_ms: row.get(7)?,
    };
    event.validate().map_err(bad_data)?;
    Ok(event)
}

pub(super) fn participant_event(
    tx: &Transaction<'_>,
    now_ms: i64,
    participant_id: &str,
    name: &str,
    state: ParticipantState,
    reason: &'static str,
) -> rusqlite::Result<EventNotice> {
    let node_id = read_node_id(tx)?;
    let kind = if state == ParticipantState::Stopped {
        EventKind::ParticipantStopped
    } else {
        EventKind::ParticipantStateChanged
    };
    let payload = json!({
        "node_id": node_id,
        "participant_id": participant_id,
        "name": name,
        "state": state,
        "intake_state": state.intake_state(),
        "reason": reason,
    });
    insert_event(
        tx,
        node_id,
        now_ms,
        kind,
        AggregateType::Participant,
        participant_id,
        None,
        payload,
    )
}

pub(super) fn delivery_event(
    tx: &Transaction<'_>,
    now: i64,
    message_id: &str,
    state: &'static str,
    detail: &'static str,
) -> rusqlite::Result<EventNotice> {
    append_event(
        tx,
        now,
        EventKind::MessageDeliveryUpdated,
        AggregateType::Message,
        message_id,
        None,
        json!({
            "message_id": message_id,
            "delivery_state": state,
            "delivery_detail": detail,
        }),
    )
}
