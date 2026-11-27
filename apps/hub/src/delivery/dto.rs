use cooplet_protocol::{
    AnswerPayload, ApprovalAction, AttachmentRef, DeliveryMode, DeliveryState, MessageId, NodeId,
    OperationId, ParticipantId, QuestionPayload,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessageRequest {
    pub(super) target_node_id: Option<NodeId>,
    pub(super) target_participant_id: ParticipantId,
    pub(super) body: String,
    pub(super) delivery_mode: Option<DeliveryMode>,
    pub(super) queue_ttl_ms: Option<u64>,
    #[serde(default)]
    #[schema(max_items = 32)]
    pub(super) attachments: Vec<AttachmentRef>,
    pub(super) question: Option<QuestionPayload>,
    pub(super) in_reply_to: Option<MessageId>,
    pub(super) answer: Option<AnswerPayload>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct MessageReceipt {
    pub(super) operation_id: OperationId,
    pub(super) message_id: MessageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) in_reply_to: Option<MessageId>,
    pub(super) delivery_state: DeliveryState,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommandReceipt {
    pub(super) operation_id: OperationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) result: Option<Value>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HubMessageView {
    pub(super) message_id: MessageId,
    pub(super) sender_actor_id: ParticipantId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sender_node_id: Option<NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) target_node_id: Option<NodeId>,
    pub(super) target_participant_id: ParticipantId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) in_reply_to: Option<MessageId>,
    pub(super) body: String,
    pub(super) attachments: Vec<AttachmentRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) question: Option<QuestionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) answer: Option<AnswerPayload>,
    pub(super) delivery_mode: DeliveryMode,
    pub(super) delivery_state: DeliveryState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delivery_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) operation_id: Option<OperationId>,
    pub(super) created_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) delivered_at_ms: Option<i64>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateAgentRequest {
    pub(super) target_node_id: NodeId,
    pub(super) client_kind: Option<String>,
    pub(super) template_id: Option<String>,
    pub(super) name: Option<String>,
    pub(super) workspace_dir: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub(super) overrides: Option<serde_json::Map<String, Value>>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StopAgentRequest {
    pub(super) target_node_id: NodeId,
    #[serde(default)]
    pub(super) cancel_turn_only: bool,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RespondApprovalRequest {
    pub(super) target_node_id: NodeId,
    pub(super) approval_id: String,
    pub(super) action: ApprovalAction,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunWorkflowRequest {
    pub(super) target_node_id: NodeId,
    pub(super) workflow_name: String,
    pub(super) workspace_dir: String,
    #[serde(default)]
    #[schema(value_type = Object)]
    pub(super) params: serde_json::Map<String, Value>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CancelWorkflowRequest {
    pub(super) target_node_id: NodeId,
}
