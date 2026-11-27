use cooplet_protocol::{
    AggregateType, AnswerPayload, ErrorCode, EventKind, MessageId, QuestionPayload,
};
use cooplet_storage::EventNotice;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::json;

use super::model::{SendMessageRequest, invalid, message_missing};
use super::queries::decode_optional;
use crate::{
    bridge::require_local_authority,
    error::{NodeError, Result},
    storage::append_event,
};

pub(super) fn validate_reply(
    conn: &Connection,
    input: &SendMessageRequest,
    sender: &str,
    can_read_all: bool,
) -> Result<Option<QuestionPayload>> {
    let Some(id) = &input.in_reply_to else {
        return Ok(None);
    };
    let (from, to, question, hub_message_id) = conn
        .query_row(
            "SELECT sender_id, recipient_id, question_json, hub_message_id FROM messages
         WHERE message_id = ?1 AND (?2 OR sender_id = ?3 OR recipient_id = ?3)",
            params![id.as_str(), can_read_all, sender],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?
        .ok_or_else(message_missing)?;
    require_local_authority(hub_message_id.as_deref())?;
    let reversed = sender == to && input.to == from;
    if let Some(answer) = &input.answer {
        if !reversed {
            return Err(invalid(
                "Only the original recipient may answer a question to its author",
            ));
        }
        let question = decode_optional::<QuestionPayload>(question)?
            .ok_or_else(|| invalid("in_reply_to does not identify a question"))?;
        answer
            .validate_against(&question)
            .map_err(|error| invalid(error.to_string()))?;
        return Ok(Some(question));
    }
    if !reversed && (sender != from || input.to != to) {
        return Err(invalid(
            "A reply must remain between the original sender and recipient",
        ));
    }
    Ok(None)
}

pub(super) fn answer_denial(
    tx: &Transaction<'_>,
    question_id: &MessageId,
    question: &QuestionPayload,
    now: i64,
) -> rusqlite::Result<Option<NodeError>> {
    if question.expires_at_ms.is_some_and(|expiry| expiry <= now) {
        return Ok(Some(NodeError::new(
            ErrorCode::QuestionExpired,
            "The question has expired",
        )));
    }
    let answer = tx
        .query_row(
            "SELECT message_id FROM messages WHERE in_reply_to = ?1 AND answer_json IS NOT NULL",
            [question_id.as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    Ok(answer.map(|id| {
        NodeError::new(
            ErrorCode::QuestionAlreadyAnswered,
            "The question already has an answer",
        )
        .with_extension("existing_answer_id", id)
    }))
}

pub(super) fn answered_event(
    tx: &Transaction<'_>,
    now: i64,
    question_id: &MessageId,
    answer_id: &MessageId,
    sender: &str,
    answer: &AnswerPayload,
) -> rusqlite::Result<EventNotice> {
    append_event(
        tx,
        now,
        EventKind::QuestionAnswered,
        AggregateType::Message,
        question_id.as_str(),
        Some(sender),
        json!({
            "question_message_id": question_id,
            "answer_message_id": answer_id,
            "answered_by": sender,
            "answer": answer,
        }),
    )
}
