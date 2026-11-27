use std::error::Error;

use cooplet_protocol::{
    AggregateType, EventEnvelope, EventId, EventKind, EventStreamFrame, HubId, NodeId, SourceKind,
    ThreadId,
};
use rusqlite::{Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum EventAudience {
    OwnerOnly { actor_id: String },
    Nodes { node_ids: Vec<NodeId> },
    Thread { thread_id: ThreadId },
    GlobalArchiveOnly,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredEvent<T = Value> {
    pub(crate) data: T,
    pub(crate) audience: EventAudience,
}

pub(crate) struct NewEvent<'a> {
    pub(crate) kind: EventKind,
    pub(crate) aggregate_type: AggregateType,
    pub(crate) aggregate_id: &'a str,
    pub(crate) actor_id: Option<&'a str>,
    pub(crate) created_at_ms: i64,
    pub(crate) data: Value,
    pub(crate) audience: EventAudience,
}

#[derive(Debug, thiserror::Error)]
#[error("event frame exceeds 1 MiB")]
pub(crate) struct FrameTooLarge;

/// Append one event and update the durable watermark. Propagate errors to roll back the write.
pub(crate) fn append(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    new: NewEvent<'_>,
) -> rusqlite::Result<EventEnvelope> {
    let event = record(tx, hub_id, new)?;
    advance_head(tx, event.seq)?;
    Ok(event)
}

/// Insert without advancing the watermark so a batch can advance it once at the end.
pub(super) fn record(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    new: NewEvent<'_>,
) -> rusqlite::Result<EventEnvelope> {
    if matches!(&new.audience, EventAudience::Nodes { node_ids } if node_ids.is_empty()) {
        return Err(encode_error(std::io::Error::other(
            "empty event node audience",
        )));
    }
    let payload_json = serde_json::to_string(&StoredEvent {
        data: &new.data,
        audience: new.audience,
    })
    .map_err(encode_error)?;
    let event_id = EventId::generate().map_err(encode_error)?;
    let seq = tx.query_row(
        "INSERT INTO events (event_id, kind, aggregate_type, aggregate_id, actor_id,
                             payload_json, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING seq",
        params![
            event_id.as_str(),
            new.kind.as_str(),
            new.aggregate_type.as_str(),
            new.aggregate_id,
            new.actor_id,
            payload_json,
            new.created_at_ms,
        ],
        |row| row.get(0),
    )?;
    let event = EventEnvelope {
        seq,
        event_id,
        source_kind: SourceKind::Hub,
        source_id: hub_id.clone().into(),
        kind: new.kind,
        aggregate_type: new.aggregate_type,
        aggregate_id: new.aggregate_id.to_owned(),
        actor_id: new.actor_id.map(str::to_owned),
        created_at_ms: new.created_at_ms,
        payload: new.data,
    };
    event.validate().map_err(encode_error)?;
    let bytes = serde_json::to_vec(&EventStreamFrame::Event {
        event: event.clone(),
    })
    .map_err(encode_error)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(encode_error(FrameTooLarge));
    }
    Ok(event)
}

pub(super) fn advance_head(tx: &Transaction<'_>, seq: u64) -> rusqlite::Result<()> {
    let changed = tx.execute(
        "UPDATE hub_meta SET events_head_seq = MAX(events_head_seq, ?1) WHERE id = 1",
        [seq],
    )?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

pub(crate) fn blob_audience(
    owner: &str,
    attached_at_ms: Option<i64>,
    node_id: Option<NodeId>,
    thread_id: Option<ThreadId>,
) -> EventAudience {
    match (attached_at_ms, node_id, thread_id) {
        (None, _, _) => EventAudience::OwnerOnly {
            actor_id: owner.to_owned(),
        },
        (Some(_), Some(node_id), None) => EventAudience::Nodes {
            node_ids: vec![node_id],
        },
        (Some(_), None, Some(thread_id)) => EventAudience::Thread { thread_id },
        (Some(_), _, _) => EventAudience::GlobalArchiveOnly,
    }
}

pub(super) fn encode_error(error: impl Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}
