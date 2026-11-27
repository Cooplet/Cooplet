use std::error::Error;

use cooplet_protocol::{
    AggregateType, AnswerPayload, AttachmentRef, DeliveryState, EventEnvelope, EventId, EventKind,
    HubId, MessageId, NodeId, ParticipantId, SourceKind, ThreadId,
};
use rusqlite::{Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::sql::json_byte_len;

const MAX_FRAME_BYTES: usize = 1024 * 1024;
// SQLite AUTOINCREMENT sequences fit in i64 (at most 19 decimal digits vs 1 for seq=1).
const MAX_SEQ_DIGIT_GROWTH: usize = 18;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BlobScope {
    Unattached,
    Node(NodeId),
    Thread(ThreadId),
    GlobalArchiveOnly,
}

impl BlobScope {
    pub(crate) fn from_columns(
        attached: Option<i64>,
        node: Option<NodeId>,
        thread: Option<ThreadId>,
    ) -> Self {
        match (attached, node, thread) {
            (None, None, None) => Self::Unattached,
            (Some(_), Some(node), None) => Self::Node(node),
            (Some(_), None, Some(thread)) => Self::Thread(thread),
            _ => Self::GlobalArchiveOnly,
        }
    }

    pub(crate) fn event_audience(self, owner: &str) -> EventAudience {
        match self {
            Self::Unattached => EventAudience::OwnerOnly {
                actor_id: owner.to_owned(),
            },
            Self::Node(node_id) => EventAudience::Nodes {
                node_ids: vec![node_id],
            },
            Self::Thread(thread_id) => EventAudience::Thread { thread_id },
            Self::GlobalArchiveOnly => EventAudience::GlobalArchiveOnly,
        }
    }
}

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

#[derive(Serialize)]
struct BorrowedFrame<'a> {
    #[serde(rename = "type")]
    frame_type: &'static str,
    event: &'a EventEnvelope,
}

fn frame_byte_len(event: &EventEnvelope) -> serde_json::Result<usize> {
    json_byte_len(&BorrowedFrame {
        frame_type: "event",
        event,
    })
}

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
pub(crate) fn record(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    new: NewEvent<'_>,
) -> rusqlite::Result<EventEnvelope> {
    if matches!(&new.audience, EventAudience::Nodes { node_ids } if node_ids.is_empty()) {
        return Err(encode_error(std::io::Error::other(
            "empty event node audience",
        )));
    }
    let event_id = EventId::generate().map_err(encode_error)?;
    let mut event = EventEnvelope {
        seq: 1,
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
    let min_frame_bytes = frame_byte_len(&event).map_err(encode_error)?;
    if min_frame_bytes > MAX_FRAME_BYTES {
        return Err(encode_error(FrameTooLarge));
    }

    let payload_json = serde_json::to_string(&StoredEvent {
        data: &event.payload,
        audience: new.audience,
    })
    .map_err(encode_error)?;
    let mut statement = tx.prepare_cached(
        "INSERT INTO events (event_id, kind, aggregate_type, aggregate_id, actor_id,
                             payload_json, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING seq",
    )?;
    let seq: u64 = statement.query_row(
        params![
            event.event_id.as_str(),
            event.kind.as_str(),
            event.aggregate_type.as_str(),
            event.aggregate_id.as_str(),
            event.actor_id.as_deref(),
            payload_json,
            event.created_at_ms,
        ],
        |row| row.get(0),
    )?;
    event.seq = seq;
    if min_frame_bytes + MAX_SEQ_DIGIT_GROWTH > MAX_FRAME_BYTES
        && frame_byte_len(&event).map_err(encode_error)? > MAX_FRAME_BYTES
    {
        return Err(encode_error(FrameTooLarge));
    }
    Ok(event)
}

pub(crate) fn advance_head(tx: &Transaction<'_>, seq: u64) -> rusqlite::Result<()> {
    let mut statement = tx.prepare_cached(
        "UPDATE hub_meta SET events_head_seq = MAX(events_head_seq, ?1) WHERE id = 1",
    )?;
    let changed = statement.execute([seq])?;
    if changed != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    Ok(())
}

pub(crate) fn node_disconnected_data(node_id: &NodeId, status: &str, session_epoch: u64) -> Value {
    json!({
        "node_id": node_id,
        "status": status,
        "session_epoch": session_epoch,
    })
}

pub(crate) fn blob_event_data(attachment: &AttachmentRef) -> Value {
    json!({
        "blob_id": attachment.blob_id,
        "original_name": attachment.name,
        "mime_type": attachment.mime_type,
        "size_bytes": attachment.size_bytes,
        "sha256": attachment.sha256,
    })
}

pub(crate) fn message_delivery_updated_data(
    message_id: &MessageId,
    target_node_id: Option<&NodeId>,
    delivery_state: DeliveryState,
    delivery_detail: Option<&str>,
    delivered_at_ms: Option<i64>,
) -> Value {
    let mut data = json!({
        "message_id": message_id,
        "target_node_id": target_node_id,
        "delivery_state": delivery_state,
    });
    if let Some(detail) = delivery_detail {
        data["delivery_detail"] = json!(detail);
    }
    if let Some(delivered_at_ms) = delivered_at_ms {
        data["delivered_at_ms"] = json!(delivered_at_ms);
    }
    data
}

pub(crate) fn question_answered_data(
    question_message_id: &MessageId,
    answer_message_id: &MessageId,
    answered_by: &ParticipantId,
    answer: &AnswerPayload,
) -> Value {
    json!({
        "question_message_id": question_message_id,
        "answer_message_id": answer_message_id,
        "answered_by": answered_by,
        "answer": answer,
    })
}

pub(super) fn encode_error(error: impl Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

#[cfg(test)]
mod tests {
    use cooplet_protocol::EventStreamFrame;

    use super::*;

    #[test]
    fn borrowed_frame_len_matches_serialized_frame() -> Result<(), Box<dyn Error>> {
        let hub_id = HubId::generate()?;
        for (seq, actor_id) in [(1, None), (9_223_372_036_854_775_807, Some("usr_actor"))] {
            let event = EventEnvelope {
                seq,
                event_id: EventId::generate()?,
                source_kind: SourceKind::Hub,
                source_id: hub_id.clone().into(),
                kind: EventKind::NodeStatus,
                aggregate_type: AggregateType::Node,
                aggregate_id: "nod_example".into(),
                actor_id: actor_id.map(str::to_owned),
                created_at_ms: 1_700_000_000_000,
                payload: json!({"status": "offline", "note": "café"}),
            };
            let measured = frame_byte_len(&event)?;
            let actual = serde_json::to_vec(&EventStreamFrame::Event { event })?.len();
            assert_eq!(measured, actual);
        }
        Ok(())
    }
}
