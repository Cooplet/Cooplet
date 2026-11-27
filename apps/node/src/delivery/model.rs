use std::collections::HashSet;

use cooplet_protocol::{
    AnswerPayload, AttachmentRef, DeliveryMode, DeliveryState, ErrorCode, MessageId, OperationId,
    ParticipantId, QuestionPayload, RunId, ServicePrincipalId, UserId,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::error::{NodeError, Result};

pub const MAX_MESSAGE_ATTACHMENTS: usize = 64;
const MIN_QUEUE_TTL_MS: u64 = 300_000;
const MAX_QUEUE_TTL_MS: u64 = 3_600_000;

fn direct_mode() -> DeliveryMode {
    DeliveryMode::DirectNow
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SendMessageRequest {
    #[schema(pattern = r"^(part|usr|sp|run)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$")]
    pub to: String,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,
    #[serde(default)]
    #[schema(max_items = 64)]
    pub attachments: Vec<AttachmentRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<QuestionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerPayload>,
    #[serde(default = "direct_mode")]
    pub delivery_mode: DeliveryMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 300_000, maximum = 3_600_000)]
    pub queue_ttl_ms: Option<u64>,
}

impl SendMessageRequest {
    pub(super) fn normalize(&mut self) -> Result<()> {
        if ParticipantId::parse(&self.to).is_err()
            && UserId::parse(&self.to).is_err()
            && ServicePrincipalId::parse(&self.to).is_err()
            && RunId::parse(&self.to).is_err()
        {
            return Err(invalid("to must be a valid actor ID"));
        }
        if self.question.is_some() && self.answer.is_some() {
            return Err(invalid(
                "A message cannot contain both a question and an answer",
            ));
        }
        if self.body.is_empty()
            && self.attachments.is_empty()
            && self.question.is_none()
            && self.answer.is_none()
        {
            return Err(invalid(
                "A message needs a body, attachment, question, or answer",
            ));
        }
        if let Some(question) = &self.question {
            question
                .validate()
                .map_err(|error| invalid(error.to_string()))?;
        }
        if let Some(answer) = &self.answer {
            if self.in_reply_to.is_none() {
                return Err(invalid("An answer requires in_reply_to"));
            }
            answer
                .validate()
                .map_err(|error| invalid(error.to_string()))?;
        }
        if self.attachments.len() > MAX_MESSAGE_ATTACHMENTS {
            return Err(invalid(format!(
                "A message may include at most {MAX_MESSAGE_ATTACHMENTS} attachments"
            )));
        }
        let mut seen_blobs = HashSet::with_capacity(self.attachments.len());
        for attachment in &self.attachments {
            attachment
                .validate()
                .map_err(|error| invalid(error.to_string()))?;
            if !seen_blobs.insert(attachment.blob_id.as_str()) {
                return Err(invalid("Duplicate attachment blob_id"));
            }
        }
        match self.delivery_mode {
            DeliveryMode::DirectNow if self.queue_ttl_ms.is_some() => {
                return Err(invalid("queue_ttl_ms requires queue_until_idle"));
            }
            DeliveryMode::QueueUntilIdle => {
                let ttl = self.queue_ttl_ms.unwrap_or(MIN_QUEUE_TTL_MS);
                if !(MIN_QUEUE_TTL_MS..=MAX_QUEUE_TTL_MS).contains(&ttl) {
                    return Err(invalid("queue_ttl_ms must be between 300000 and 3600000"));
                }
                self.queue_ttl_ms = Some(ttl);
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MessageView {
    pub message_id: MessageId,
    pub sender_id: String,
    pub recipient_id: String,
    pub body: String,
    pub attachments: Vec<AttachmentRef>,
    pub delivery_mode: DeliveryMode,
    pub delivery_state: DeliveryState,
    pub created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub question: Option<QuestionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub answer: Option<AnswerPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<OperationId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct MessagePage {
    pub items: Vec<MessageView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<MessageId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SendMessageResponse {
    pub operation_id: OperationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_id: Option<MessageId>,
    pub delivery_state: DeliveryState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<MessageId>,
}

impl SendMessageResponse {
    pub fn status(&self) -> StatusCode {
        match self.delivery_state {
            DeliveryState::Queued => StatusCode::ACCEPTED,
            DeliveryState::Unavailable if self.message_id.is_none() => StatusCode::OK,
            _ => StatusCode::CREATED,
        }
    }

    pub(super) fn decode(value: Value) -> Result<Self> {
        serde_json::from_value(value).map_err(NodeError::internal)
    }
}

pub(super) fn invalid(detail: impl Into<String>) -> NodeError {
    NodeError::new(ErrorCode::InvalidRequest, detail)
}

pub(super) fn message_missing() -> NodeError {
    NodeError::new(ErrorCode::NotFound, "Message not found")
}
