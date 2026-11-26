use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    AggregateType, AnswerPayload, EventId, MessageId, ParticipantState, ProtocolError, SourceId,
    SourceKind, ThreadId, ValidationError, domain::validate_approval_fields,
};

macro_rules! event_kinds {
    ($($variant:ident => $value:literal,)*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
        pub enum EventKind {
            $(#[serde(rename = $value)] $variant,)*
        }

        impl EventKind {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)*];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value,)*
                }
            }
        }

        impl FromStr for EventKind {
            type Err = ValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)*
                    _ => Err(ValidationError("event kind")),
                }
            }
        }
    };
}

event_kinds! {
    NodeConnected => "node.connected",
    NodeDisconnected => "node.disconnected",
    NodeStatus => "node.status",
    ParticipantCreated => "participant.created",
    ParticipantStateChanged => "participant.state_changed",
    ParticipantAwaitingApproval => "participant.awaiting_approval",
    ParticipantStopped => "participant.stopped",
    MessageCreated => "message.created",
    MessageDeliveryUpdated => "message.delivery_updated",
    QuestionAnswered => "question.answered",
    ThreadCreated => "thread.created",
    ThreadUpdated => "thread.updated",
    ThreadClosed => "thread.closed",
    ThreadMemberUpdated => "thread.member_updated",
    ThreadPostCreated => "thread.post_created",
    ThreadPinUpdated => "thread.pin_updated",
    BlobStored => "blob.stored",
    BlobDeleted => "blob.deleted",
    WorkflowRunStarted => "workflow.run_started",
    WorkflowRunFinished => "workflow.run_finished",
    WorkflowStepStarted => "workflow.step_started",
    WorkflowStepCompleted => "workflow.step_completed",
    WorkflowLog => "workflow.log",
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope {
    pub seq: u64,
    pub event_id: EventId,
    pub source_kind: SourceKind,
    pub source_id: SourceId,
    pub kind: EventKind,
    pub aggregate_type: AggregateType,
    pub aggregate_id: String,
    pub actor_id: Option<String>,
    pub created_at_ms: i64,
    pub payload: Value,
}

impl EventEnvelope {
    /// Validate the shared correlation and approval fields used by event consumers.
    /// Applications validate fields specific to their producers and consumers.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.source_kind != self.source_id.kind() {
            return Err(ValidationError("event source kind").into());
        }
        match self.kind {
            EventKind::ParticipantAwaitingApproval | EventKind::ParticipantStateChanged => {
                #[derive(Deserialize)]
                struct ApprovalFields<'a> {
                    state: ParticipantState,
                    approval_id: Option<&'a str>,
                    can_respond_remotely: Option<bool>,
                }
                let fields = ApprovalFields::deserialize(&self.payload)?;
                if self.kind == EventKind::ParticipantAwaitingApproval
                    && fields.state != ParticipantState::AwaitingApproval
                {
                    return Err(ValidationError("state of awaiting approval event").into());
                }
                validate_approval_fields(
                    fields.state,
                    fields.can_respond_remotely,
                    fields.approval_id,
                )?;
            }
            EventKind::QuestionAnswered => {
                #[derive(Deserialize)]
                struct AnsweredFields {
                    question_message_id: MessageId,
                    answer_message_id: MessageId,
                    thread_id: Option<ThreadId>,
                    answered_by: String,
                    answer: AnswerPayload,
                }
                let AnsweredFields {
                    question_message_id: _question_id,
                    answer_message_id: _answer_id,
                    thread_id: _thread_id,
                    answered_by,
                    answer,
                } = AnsweredFields::deserialize(&self.payload)?;
                if answered_by.trim().is_empty()
                    || (answer.selected_option_ids.is_empty()
                        && answer
                            .text
                            .as_deref()
                            .is_none_or(|text| text.trim().is_empty()))
                {
                    return Err(ValidationError("question answer event").into());
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventStreamFrame {
    Event {
        event: EventEnvelope,
    },
    Ping {},
    ResyncRequired {
        oldest_available_seq: u64,
        latest_seq: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EventsPage {
    pub items: Vec<EventEnvelope>,
    pub next_seq: u64,
    pub oldest_available_seq: u64,
    pub latest_seq: u64,
    pub has_more: bool,
}
