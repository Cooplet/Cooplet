use std::collections::HashSet;
use std::time::Duration;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    AnswerPayload, ApprovalDecision, ApprovalWaitKind, AttachmentRef, DeliveryMode, HubId,
    MessageId, NodeId, OperationId, ParticipantId, ParticipantKind, ParticipantState,
    ProblemDetails, ProtocolError, QuestionPayload, RunId, ThreadId, ValidationError,
    domain::validate_approval_fields,
};

pub const BRIDGE_PROTOCOL_VERSION: u8 = 1;
pub const MAX_WS_FRAME_BYTES: usize = 256 * 1024;
pub const MAX_ROSTER_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROSTER_FRAMES: u32 = 512;
pub const ROSTER_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_PENDING_OP_IDS: usize = 512;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    HelloAck,
    CommandReq,
    ThreadDeliver,
    RosterSync,
    CommandRes,
    ThreadAck,
    NodeEvent,
}

crate::wire::wire_object! {
    /// The envelope is parsed before selecting a typed payload. Bounded WebSocket
    /// receivers should use the original byte length, including JSON whitespace.
    #[serde(deny_unknown_fields)]
    pub struct BridgeFrame {
        pub v: u8,
        pub epoch: u64,
        pub kind: FrameKind,
        pub payload: Value,
    }
}

pub trait BridgePayload: Serialize + DeserializeOwned {
    const KIND: FrameKind;

    fn validate(&self) -> Result<(), ValidationError> {
        Ok(())
    }
}

impl BridgeFrame {
    pub fn encode<P: BridgePayload>(epoch: u64, payload: &P) -> Result<Self, ProtocolError> {
        payload.validate()?;
        let frame = Self {
            v: BRIDGE_PROTOCOL_VERSION,
            epoch,
            kind: P::KIND,
            payload: serde_json::to_value(payload)?,
        };
        frame.to_json()?;
        Ok(frame)
    }

    pub fn decode_payload<P: BridgePayload>(&self) -> Result<P, ProtocolError> {
        self.validate_version()?;
        if self.kind != P::KIND {
            return Err(ProtocolError::BridgeKind {
                expected: P::KIND,
                actual: self.kind,
            });
        }
        let payload = P::deserialize(&self.payload)?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, ProtocolError> {
        check_frame_size(bytes.len())?;
        let frame: Self = serde_json::from_slice(bytes)?;
        frame.validate_version()?;
        Ok(frame)
    }

    pub fn to_json(&self) -> Result<Vec<u8>, ProtocolError> {
        self.validate_version()?;
        let bytes = serde_json::to_vec(self)?;
        check_frame_size(bytes.len())?;
        Ok(bytes)
    }

    fn validate_version(&self) -> Result<(), ProtocolError> {
        if self.v == BRIDGE_PROTOCOL_VERSION {
            Ok(())
        } else {
            Err(ProtocolError::BridgeVersion(self.v))
        }
    }
}

fn check_frame_size(actual: usize) -> Result<(), ProtocolError> {
    if actual > MAX_WS_FRAME_BYTES {
        Err(ProtocolError::BridgeFrameTooLarge { actual })
    } else {
        Ok(())
    }
}

macro_rules! payload_kind {
    ($($payload:ty => $kind:ident),+ $(,)?) => {
        $(impl BridgePayload for $payload {
            const KIND: FrameKind = FrameKind::$kind;
        })+
    };
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct HelloAckPayload {
        pub hub_id: HubId,
        pub node_id: NodeId,
        pub max_inflight_commands: u32,
        pub max_inflight_threads: u32,
        pub pending_op_ids: Vec<OperationId>,
    }
}

impl BridgePayload for HelloAckPayload {
    const KIND: FrameKind = FrameKind::HelloAck;

    fn validate(&self) -> Result<(), ValidationError> {
        if self.pending_op_ids.len() > MAX_PENDING_OP_IDS {
            return Err(ValidationError("pending operation count"));
        }
        let mut seen = HashSet::with_capacity(self.pending_op_ids.len());
        if !self.pending_op_ids.iter().all(|id| seen.insert(id)) {
            return Err(ValidationError("duplicate pending operation ID"));
        }
        Ok(())
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct CommandReqPayload {
        pub op_id: OperationId,
        pub actor_id: String,
        pub command: BridgeCommand,
        pub delivery_mode: DeliveryMode,
        pub admission_budget_ms: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub ttl_ms: Option<u64>,
    }
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum BridgeCommand {
        SendMessage {
            message_id: MessageId,
            recipient_id: ParticipantId,
            #[serde(skip_serializing_if = "Option::is_none")]
            in_reply_to: Option<MessageId>,
            body: String,
            #[serde(default)]
            attachments: Vec<AttachmentRef>,
            #[serde(skip_serializing_if = "Option::is_none")]
            question: Option<QuestionPayload>,
            #[serde(skip_serializing_if = "Option::is_none")]
            answer: Option<AnswerPayload>,
        },
        CreateAgent {
            #[serde(skip_serializing_if = "Option::is_none")]
            client_kind: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            template_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            name: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            workspace_dir: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            overrides: Option<Value>,
        },
        StopAgent {
            participant_id: ParticipantId,
            #[serde(default)]
            cancel_turn_only: bool,
        },
        RespondApproval {
            participant_id: ParticipantId,
            decision: ApprovalDecision,
        },
        RunWorkflow {
            workflow_name: String,
            workspace_dir: String,
            #[serde(default)]
            params: Value,
        },
        CancelWorkflow {
            run_id: RunId,
        },
        InspectAgent {
            participant_id: ParticipantId,
        },
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CommandOutcome {
    Delivered,
    Succeeded,
    Queued,
    Busy,
    AwaitingApproval,
    Unavailable,
    Rejected,
    Unknown,
    Expired,
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct CommandResPayload {
        pub op_id: OperationId,
        pub outcome: CommandOutcome,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub resource_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub error: Option<ProblemDetails>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadDeliverPayload {
        pub delivery_id: OperationId,
        pub thread_id: ThreadId,
        pub member_id: ParticipantId,
        pub from_seq: u64,
        pub to_seq: u64,
        pub posts: Vec<ThreadPostWire>,
        #[serde(default)]
        pub truncated_earlier: bool,
        pub delivery_mode: DeliveryMode,
        pub admission_budget_ms: u64,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPostWire {
        pub seq: u64,
        pub post_id: MessageId,
        pub author_actor_id: String,
        pub author_handle: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub in_reply_to_post_id: Option<MessageId>,
        pub body: String,
        #[serde(default)]
        pub mentions: Vec<String>,
        #[serde(default)]
        pub attachments: Vec<AttachmentRef>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub question: Option<QuestionPayload>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub answer: Option<AnswerPayload>,
        pub created_at_ms: i64,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ThreadDeliveryOutcome {
    Delivered,
    Busy,
    AwaitingApproval,
    Unavailable,
    Rejected,
    Unknown,
    Expired,
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadAckPayload {
        pub delivery_id: OperationId,
        pub thread_id: ThreadId,
        pub member_id: ParticipantId,
        pub acked_seq: u64,
        pub outcome: ThreadDeliveryOutcome,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub detail: Option<String>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct RosterSyncPayload {
        pub chunk_index: u32,
        pub is_final: bool,
        pub participants: Vec<ParticipantWire>,
        pub templates: Vec<TemplateSummary>,
        pub workflows: Vec<WorkflowSummary>,
        pub unresolved_thread_fences: Vec<ThreadFenceReport>,
        pub resolved_op_receipts: Vec<CommandResPayload>,
    }
}

impl BridgePayload for RosterSyncPayload {
    const KIND: FrameKind = FrameKind::RosterSync;

    fn validate(&self) -> Result<(), ValidationError> {
        if self.chunk_index >= MAX_ROSTER_FRAMES
            || (self.chunk_index == MAX_ROSTER_FRAMES - 1 && !self.is_final)
        {
            return Err(ValidationError("roster chunk index"));
        }
        for participant in &self.participants {
            validate_approval_fields(
                participant.state,
                participant.can_respond_remotely,
                participant.approval_id.as_deref(),
            )?;
        }
        Ok(())
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ParticipantWire {
        pub participant_id: ParticipantId,
        pub kind: ParticipantKind,
        pub name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub client_kind: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub workspace_dir: Option<String>,
        pub state: ParticipantState,
        #[serde(default)]
        pub capabilities: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub wait_kind: Option<ApprovalWaitKind>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub approval_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub action_hint: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub can_respond_remotely: Option<bool>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct TemplateSummary {
        pub template_id: String,
        pub title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
        pub client_kind: String,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct WorkflowSummary {
        pub workflow_name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub title: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub description: Option<String>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadFenceReport {
        pub thread_id: ThreadId,
        pub participant_id: ParticipantId,
        pub highest_dispatched_seq: u64,
    }
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum NodeEventPayload {
        ParticipantStateChanged {
            participant_id: ParticipantId,
            state: ParticipantState,
            #[serde(skip_serializing_if = "Option::is_none")]
            approval_id: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            action_hint: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            summary: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            wait_kind: Option<ApprovalWaitKind>,
            #[serde(skip_serializing_if = "Option::is_none")]
            can_respond_remotely: Option<bool>,
        },
        AgentReplyCreated {
            message_id: MessageId,
            sender_id: ParticipantId,
            recipient_id: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            in_reply_to: Option<MessageId>,
            body: String,
            #[serde(default)]
            attachments: Vec<AttachmentRef>,
            #[serde(skip_serializing_if = "Option::is_none")]
            question: Option<QuestionPayload>,
            #[serde(skip_serializing_if = "Option::is_none")]
            answer: Option<AnswerPayload>,
        },
        WorkflowRunStarted {
            run_id: RunId,
            workflow_name: String,
        },
        WorkflowStepStarted {
            run_id: RunId,
            step_key: String,
        },
        WorkflowStepCompleted {
            run_id: RunId,
            step_key: String,
            status: String,
        },
        WorkflowRunFinished {
            run_id: RunId,
            workflow_name: String,
            state: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            result: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<Value>,
        },
        WorkflowLog {
            run_id: RunId,
            level: String,
            message: String,
        },
    }
}

impl BridgePayload for NodeEventPayload {
    const KIND: FrameKind = FrameKind::NodeEvent;

    fn validate(&self) -> Result<(), ValidationError> {
        if let Self::ParticipantStateChanged {
            state,
            can_respond_remotely,
            approval_id,
            ..
        } = self
        {
            validate_approval_fields(*state, *can_respond_remotely, approval_id.as_deref())?;
        }
        Ok(())
    }
}

payload_kind!(
    CommandReqPayload => CommandReq,
    CommandResPayload => CommandRes,
    ThreadDeliverPayload => ThreadDeliver,
    ThreadAckPayload => ThreadAck,
);
