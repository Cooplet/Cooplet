use cooplet_protocol::{
    AggregateType, DeliveryState, ErrorCode, EventKind, MessageId, ParticipantId, ParticipantKind,
    ParticipantState,
};
use cooplet_storage::EventNotice;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::model::{
    MessagePage, MessageView, SendMessageRequest, SendMessageResponse, invalid, message_missing,
};
use crate::{
    clients::{AdapterBinding, PendingApprovalRecord},
    error::{NodeError, Result},
    storage::{append_event, check_next_event, event_frame_too_large, json_encoded_len},
};

const PAGE_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const CACHED_MESSAGE_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;
const PROJECTION: &str =
    "m.message_id,m.sender_id,m.recipient_id,m.body,m.attachments_json,m.delivery_mode,m.delivery_state,
     m.created_at_ms, CASE WHEN m.hub_parent_message_id IS NULL THEN m.in_reply_to ELSE parent.message_id END,
     m.question_json,m.answer_json,m.operation_id,m.delivery_detail,m.delivered_at_ms";
const JOIN_PARENT: &str = "FROM messages m LEFT JOIN messages parent
    ON parent.hub_message_id = m.hub_parent_message_id AND parent.expires_at_ms > ?4
    AND (?5 OR parent.sender_id = ?3 OR parent.recipient_id = ?3)";

pub(super) struct HubProjection<'a> {
    pub message: &'a MessageId,
    pub parent: Option<&'a MessageId>,
}

pub(super) struct Recipient {
    pub id: ParticipantId,
    pub kind: ParticipantKind,
    pub state: ParticipantState,
    pub generation: u64,
    pub approval: Option<PendingApprovalRecord>,
}

pub(super) fn recipient(conn: &Connection, to: &str) -> Result<Recipient> {
    let missing = || NodeError::new(ErrorCode::NotFound, "Recipient not found");
    let id = ParticipantId::parse(to).map_err(|_| missing())?;
    conn.query_row(
        "SELECT kind, state, generation,
                CASE WHEN state = 'awaiting_approval' THEN adapter_binding_json END
         FROM participants WHERE participant_id = ?1",
        [id.as_str()],
        |row| {
            let binding: Option<AdapterBinding> = decode_optional(row.get(3)?)?;
            Ok(Recipient {
                id: id.clone(),
                kind: parse(row.get(0)?)?,
                state: parse(row.get(1)?)?,
                generation: row.get(2)?,
                approval: binding.and_then(|binding| binding.core.pending_approval),
            })
        },
    )
    .optional()?
    .ok_or_else(missing)
}

pub(super) fn read_one(
    conn: &Connection,
    id: &MessageId,
    actor: &str,
    can_read_all: bool,
    now: i64,
) -> Result<MessageView> {
    let sql = format!(
        "SELECT {PROJECTION} {JOIN_PARENT}
         WHERE m.message_id = ?1 AND (?5 OR m.sender_id = ?3 OR m.recipient_id = ?3)",
    );
    conn.query_row(
        &sql,
        params![id.as_str(), rusqlite::types::Null, actor, now, can_read_all],
        read_view,
    )
    .optional()?
    .ok_or_else(message_missing)
}

pub(super) fn committed_pending_answer(
    conn: &Connection,
    operation_id: &str,
) -> rusqlite::Result<Option<SendMessageResponse>> {
    conn.query_row(
        "SELECT m.message_id, m.in_reply_to FROM messages m
         JOIN operations o ON o.operation_id = m.operation_id
         WHERE o.operation_id = ?1 AND o.kind = 'message.send'
           AND o.state IN ('accepted', 'in_progress') AND o.bridge_origin IS NULL
           AND m.answer_json IS NOT NULL",
        [operation_id],
        |row| {
            Ok(SendMessageResponse {
                operation_id: operation_id.parse().map_err(sql_error)?,
                message_id: Some(parse(row.get(0)?)?),
                in_reply_to: parse_optional(row.get(1)?)?,
                delivery_state: DeliveryState::Unknown,
            })
        },
    )
    .optional()
}

pub(crate) fn scoped_messages_sql() -> String {
    format!(
        "SELECT {PROJECTION} {JOIN_PARENT}
         WHERE m.message_id IN (
             SELECT message_id FROM messages WHERE sender_id = ?3 AND message_id > ?1
             UNION
             SELECT message_id FROM messages WHERE recipient_id = ?3 AND message_id > ?1
             ORDER BY message_id LIMIT ?2
         )
         ORDER BY m.message_id",
    )
}

pub(super) fn read_page(
    conn: &Connection,
    actor: &str,
    can_read_all: bool,
    after: Option<&MessageId>,
    limit: usize,
    now: i64,
) -> Result<MessagePage> {
    let cursor = after.map_or("", MessageId::as_str);
    let size = i64::try_from(limit + 1).map_err(NodeError::internal)?;
    // Keep the one-owner path indexable without fetching large bodies to sort.
    let sql = if can_read_all {
        format!(
            "SELECT {PROJECTION} {JOIN_PARENT} WHERE m.message_id > ?1 ORDER BY m.message_id LIMIT ?2"
        )
    } else {
        scoped_messages_sql()
    };
    let mut statement = conn.prepare_cached(&sql)?;
    let mut rows = statement.query(params![cursor, size, actor, now, can_read_all])?;
    let mut items = Vec::new();
    let mut bytes = br#"{"items":[]}"#.len();
    let mut current = rows.next()?.map(read_view).transpose()?;
    let mut has_more = false;
    while let Some(item) = current {
        let item_len = json_encoded_len(&item).map_err(NodeError::internal)?;
        let upcoming = if items.len() + 1 < limit {
            rows.next()?.map(read_view).transpose()?
        } else if rows.next()?.is_some() {
            has_more = true;
            None
        } else {
            None
        };
        let next_bytes = bytes + usize::from(!items.is_empty()) + item_len;
        let cursor_bytes = if upcoming.is_some() || has_more {
            br#","next_after":"""#.len() + item.message_id.as_str().len()
        } else {
            0
        };
        if next_bytes + cursor_bytes > PAGE_LIMIT_BYTES {
            has_more = true;
            break;
        }
        bytes = next_bytes;
        items.push(item);
        current = upcoming;
    }
    let next_after = if has_more {
        Some(
            items
                .last()
                .ok_or_else(|| NodeError::internal("One message exceeds the page budget"))?
                .message_id
                .clone(),
        )
    } else {
        None
    };
    Ok(MessagePage { items, next_after })
}

pub(super) fn checked_page_limit(limit: Option<u16>) -> Result<usize> {
    let limit = limit.unwrap_or(100);
    if !(1..=500).contains(&limit) {
        return Err(invalid("limit must be between 1 and 500"));
    }
    Ok(usize::from(limit))
}

fn read_view(row: &Row<'_>) -> rusqlite::Result<MessageView> {
    Ok(MessageView {
        message_id: parse(row.get(0)?)?,
        sender_id: row.get(1)?,
        recipient_id: row.get(2)?,
        body: row.get(3)?,
        attachments: serde_json::from_str(&row.get::<_, String>(4)?).map_err(sql_error)?,
        delivery_mode: parse(row.get(5)?)?,
        delivery_state: parse(row.get(6)?)?,
        created_at_ms: row.get(7)?,
        in_reply_to: parse_optional(row.get(8)?)?,
        question: decode_optional(row.get(9)?)?,
        answer: decode_optional(row.get(10)?)?,
        operation_id: parse_optional(row.get(11)?)?,
        delivery_detail: row.get(12)?,
        delivered_at_ms: row.get(13)?,
    })
}

fn parse<T: std::str::FromStr>(text: String) -> rusqlite::Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    text.parse().map_err(sql_error)
}

fn parse_optional<T: std::str::FromStr>(text: Option<String>) -> rusqlite::Result<Option<T>>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    text.map(parse).transpose()
}

pub(super) fn decode_optional<T: DeserializeOwned>(
    text: Option<String>,
) -> rusqlite::Result<Option<T>> {
    text.map(|json| serde_json::from_str(&json).map_err(sql_error))
        .transpose()
}

pub(super) fn sql_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

pub(super) fn store_message(
    tx: &Transaction<'_>,
    view: &MessageView,
    attachments_json: &str,
    hub: Option<HubProjection<'_>>,
) -> Result<EventNotice> {
    tx.execute(
        "INSERT INTO messages
           (message_id,sender_id,recipient_id,in_reply_to,body,attachments_json,
            question_json,answer_json,delivery_mode,delivery_state,operation_id,
            created_at_ms,delivered_at_ms,expires_at_ms,hub_message_id,hub_parent_message_id)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
        params![
            view.message_id.as_str(),
            view.sender_id,
            view.recipient_id,
            view.in_reply_to.as_ref().map(MessageId::as_str),
            view.body,
            attachments_json,
            view.question
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(sql_error)?,
            view.answer
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(sql_error)?,
            view.delivery_mode.as_str(),
            view.delivery_state.as_str(),
            view.operation_id.as_ref().map(|id| id.as_str()),
            view.created_at_ms,
            view.delivered_at_ms,
            view.created_at_ms.saturating_add(CACHED_MESSAGE_TTL_MS),
            hub.as_ref().map(|projection| projection.message.as_str()),
            hub.and_then(|projection| projection.parent)
                .map(MessageId::as_str),
        ],
    )?;
    append_event(
        tx,
        view.created_at_ms,
        EventKind::MessageCreated,
        AggregateType::Message,
        view.message_id.as_str(),
        Some(&view.sender_id),
        message_payload(view),
    )
    .map_err(message_frame_error)
}

pub(super) fn check_message_frame_fields(
    tx: &Transaction<'_>,
    now: i64,
    message_id: &MessageId,
    sender_id: &str,
    input: &SendMessageRequest,
    delivery_state: DeliveryState,
) -> Result<()> {
    let mut payload = json!({
        "message_id": message_id,
        "sender_id": sender_id,
        "recipient_id": &input.to,
        "body": &input.body,
        "attachments": &input.attachments,
        "delivery_state": delivery_state,
    });
    if let Some(parent) = &input.in_reply_to {
        payload["in_reply_to"] = json!(parent);
    }
    if let Some(question) = &input.question {
        payload["question"] = json!(question);
    }
    if let Some(answer) = &input.answer {
        payload["answer"] = json!(answer);
    }
    check_next_event(
        tx,
        now,
        EventKind::MessageCreated,
        AggregateType::Message,
        message_id.as_str(),
        Some(sender_id),
        payload,
    )
    .map_err(message_frame_error)
}

fn message_payload(view: &MessageView) -> Value {
    let mut payload = json!({
        "message_id": &view.message_id,
        "sender_id": &view.sender_id,
        "recipient_id": &view.recipient_id,
        "body": &view.body,
        "attachments": &view.attachments,
        "delivery_state": view.delivery_state,
    });
    if let Some(parent) = &view.in_reply_to {
        payload["in_reply_to"] = json!(parent);
    }
    if let Some(question) = &view.question {
        payload["question"] = json!(question);
    }
    if let Some(answer) = &view.answer {
        payload["answer"] = json!(answer);
    }
    payload
}

fn message_frame_error(error: rusqlite::Error) -> NodeError {
    if event_frame_too_large(&error) {
        NodeError::new(
            ErrorCode::PayloadTooLarge,
            "Message exceeds the event frame limit",
        )
    } else {
        error.into()
    }
}

pub(super) fn update_delivery(
    tx: &Transaction<'_>,
    now: i64,
    id: &str,
    state: DeliveryState,
    detail: Option<&str>,
) -> rusqlite::Result<EventNotice> {
    let delivered_at_ms = (state == DeliveryState::Delivered).then_some(now);
    if tx.execute(
        "UPDATE messages SET delivery_state = ?1, delivery_detail = ?2, delivered_at_ms = ?3
         WHERE message_id = ?4",
        params![state.as_str(), detail, delivered_at_ms, id],
    )? != 1
    {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    let mut payload = json!({ "message_id": id, "delivery_state": state });
    if let Some(detail) = detail {
        payload["delivery_detail"] = Value::String(detail.to_owned());
    }
    if let Some(delivered) = delivered_at_ms {
        payload["delivered_at_ms"] = json!(delivered);
    }
    append_event(
        tx,
        now,
        EventKind::MessageDeliveryUpdated,
        AggregateType::Message,
        id,
        None,
        payload,
    )
}
