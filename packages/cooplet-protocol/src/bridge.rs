use std::collections::HashSet;
use std::io;
use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

use crate::{
    AnswerPayload, ApprovalDecision, ApprovalWaitKind, AttachmentRef, DeliveryMode, HubId,
    MessageId, NodeId, OperationId, ParticipantId, ParticipantKind, ParticipantState,
    ProblemDetails, ProtocolError, QuestionPayload, RunId, ThreadId, ValidationError,
    WorkflowLogLevel, WorkflowRunState, WorkflowStepStatus, domain::validate_approval_fields,
    validate_step_key,
};

pub const BRIDGE_PROTOCOL_VERSION: u8 = 1;
pub const MAX_WS_FRAME_BYTES: usize = 256 * 1024;
pub const MAX_ROSTER_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROSTER_FRAMES: u32 = 512;
pub const ROSTER_TIMEOUT: Duration = Duration::from_secs(30);
pub const MAX_PENDING_OP_IDS: usize = 512;

fn default_empty_object() -> Value {
    Value::Object(Map::new())
}

crate::wire::wire_string_enum! {
    pub enum FrameKind {
        HelloAck => "hello_ack",
        CommandReq => "command_req",
        ThreadDeliver => "thread_deliver",
        RosterSync => "roster_sync",
        CommandRes => "command_res",
        ThreadAck => "thread_ack",
        NodeEvent => "node_event",
    }
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

#[derive(Serialize)]
struct BorrowedFrame<'a, P: ?Sized> {
    v: u8,
    epoch: u64,
    kind: FrameKind,
    payload: &'a P,
}

struct BoundedWriter {
    buffer: Vec<u8>,
    written: usize,
    limit: usize,
    store: bool,
}

impl BoundedWriter {
    fn counting(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            written: 0,
            limit,
            store: false,
        }
    }

    fn buffering(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            written: 0,
            limit,
            store: true,
        }
    }
}

impl io::Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let next = self.written.saturating_add(buf.len());
        if next > self.limit {
            self.written = next;
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "bridge frame exceeds limit",
            ));
        }
        if self.store {
            self.buffer.extend_from_slice(buf);
        }
        self.written = next;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialize_bounded<T: Serialize>(
    value: &T,
    mut writer: BoundedWriter,
) -> Result<Vec<u8>, ProtocolError> {
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(writer.buffer),
        Err(_) if writer.written > writer.limit => Err(ProtocolError::BridgeFrameTooLarge {
            actual: writer.written,
        }),
        Err(error) => Err(ProtocolError::Json(error)),
    }
}

impl BridgeFrame {
    /// Validate and serialize a typed bridge frame directly into bounded JSON bytes
    /// in a single pass without building an intermediate `serde_json::Value`.
    pub fn encode_json<P: BridgePayload>(
        epoch: u64,
        payload: &P,
    ) -> Result<Vec<u8>, ProtocolError> {
        payload.validate()?;
        let envelope = BorrowedFrame {
            v: BRIDGE_PROTOCOL_VERSION,
            epoch,
            kind: P::KIND,
            payload,
        };
        serialize_bounded(&envelope, BoundedWriter::buffering(MAX_WS_FRAME_BYTES))
    }

    pub fn encode<P: BridgePayload>(epoch: u64, payload: &P) -> Result<Self, ProtocolError> {
        payload.validate()?;
        let envelope = BorrowedFrame {
            v: BRIDGE_PROTOCOL_VERSION,
            epoch,
            kind: P::KIND,
            payload,
        };
        // Check the serialized byte limit before allocating a DOM tree for oversized payloads.
        serialize_bounded(&envelope, BoundedWriter::counting(MAX_WS_FRAME_BYTES))?;
        Ok(Self {
            v: BRIDGE_PROTOCOL_VERSION,
            epoch,
            kind: P::KIND,
            payload: serde_json::to_value(payload)?,
        })
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
        serialize_bounded(self, BoundedWriter::buffering(MAX_WS_FRAME_BYTES))
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

fn validate_message_extras(
    attachments: &[AttachmentRef],
    question: Option<&QuestionPayload>,
    answer: Option<&AnswerPayload>,
) -> Result<(), ValidationError> {
    if question.is_some() && answer.is_some() {
        return Err(ValidationError(
            "message cannot carry both question and answer",
        ));
    }
    for attachment in attachments {
        attachment.validate()?;
    }
    if let Some(question) = question {
        question.validate()?;
    }
    if let Some(answer) = answer {
        answer.validate()?;
    }
    Ok(())
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

impl BridgePayload for CommandReqPayload {
    const KIND: FrameKind = FrameKind::CommandReq;

    fn validate(&self) -> Result<(), ValidationError> {
        if self.actor_id.trim().is_empty() {
            return Err(ValidationError("command actor ID"));
        }
        self.command.validate()
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
            #[serde(default = "default_empty_object")]
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

impl BridgeCommand {
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::SendMessage {
                attachments,
                question,
                answer,
                ..
            } => validate_message_extras(attachments, question.as_ref(), answer.as_ref()),
            Self::RespondApproval { decision, .. } => decision.validate(),
            Self::RunWorkflow {
                workflow_name,
                workspace_dir,
                params,
            } => {
                if workflow_name.trim().is_empty() {
                    return Err(ValidationError("workflow name"));
                }
                if workspace_dir.trim().is_empty() {
                    return Err(ValidationError("workspace dir"));
                }
                if !params.is_object() {
                    return Err(ValidationError("workflow params object"));
                }
                Ok(())
            }
            Self::CreateAgent { .. }
            | Self::StopAgent { .. }
            | Self::CancelWorkflow { .. }
            | Self::InspectAgent { .. } => Ok(()),
        }
    }
}

crate::wire::wire_string_enum! {
    pub enum CommandOutcome {
        Delivered => "delivered",
        Succeeded => "succeeded",
        Queued => "queued",
        Busy => "busy",
        AwaitingApproval => "awaiting_approval",
        Unavailable => "unavailable",
        Rejected => "rejected",
        Unknown => "unknown",
        Expired => "expired",
    }
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

impl BridgePayload for ThreadDeliverPayload {
    const KIND: FrameKind = FrameKind::ThreadDeliver;

    fn validate(&self) -> Result<(), ValidationError> {
        if self.from_seq == 0 || self.from_seq > self.to_seq {
            return Err(ValidationError("thread delivery sequence range"));
        }
        let mut previous = None;
        for post in &self.posts {
            if post.seq < self.from_seq || post.seq > self.to_seq {
                return Err(ValidationError("thread post sequence out of range"));
            }
            if previous.is_some_and(|prev| post.seq <= prev) {
                return Err(ValidationError("thread post sequence order"));
            }
            previous = Some(post.seq);
            post.validate()?;
        }
        Ok(())
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

impl ThreadPostWire {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.author_actor_id.trim().is_empty() {
            return Err(ValidationError("thread post author actor ID"));
        }
        if self.author_handle.trim().is_empty() {
            return Err(ValidationError("thread post author handle"));
        }
        validate_message_extras(
            &self.attachments,
            self.question.as_ref(),
            self.answer.as_ref(),
        )
    }
}

crate::wire::wire_string_enum! {
    pub enum ThreadDeliveryOutcome {
        Delivered => "delivered",
        Busy => "busy",
        AwaitingApproval => "awaiting_approval",
        Unavailable => "unavailable",
        Rejected => "rejected",
        Unknown => "unknown",
        Expired => "expired",
    }
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
            if !participant.capabilities.is_object() {
                return Err(ValidationError("participant capabilities object"));
            }
            validate_approval_fields(
                participant.state,
                participant.can_respond_remotely,
                participant.approval_id.as_deref(),
                participant.wait_kind.is_some()
                    || participant.action_hint.is_some()
                    || participant.summary.is_some(),
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
        #[serde(default = "default_empty_object")]
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
            status: WorkflowStepStatus,
        },
        WorkflowRunFinished {
            run_id: RunId,
            workflow_name: String,
            state: WorkflowRunState,
            #[serde(skip_serializing_if = "Option::is_none")]
            result: Option<Value>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<Value>,
        },
        WorkflowLog {
            run_id: RunId,
            level: WorkflowLogLevel,
            message: String,
        },
    }
}

impl BridgePayload for NodeEventPayload {
    const KIND: FrameKind = FrameKind::NodeEvent;

    fn validate(&self) -> Result<(), ValidationError> {
        match self {
            Self::ParticipantStateChanged {
                state,
                can_respond_remotely,
                approval_id,
                wait_kind,
                action_hint,
                summary,
                ..
            } => validate_approval_fields(
                *state,
                *can_respond_remotely,
                approval_id.as_deref(),
                wait_kind.is_some() || action_hint.is_some() || summary.is_some(),
            ),
            Self::AgentReplyCreated {
                recipient_id,
                attachments,
                question,
                answer,
                ..
            } => {
                if recipient_id.trim().is_empty() {
                    return Err(ValidationError("agent reply recipient ID"));
                }
                validate_message_extras(attachments, question.as_ref(), answer.as_ref())
            }
            Self::WorkflowRunStarted { workflow_name, .. } => {
                if workflow_name.trim().is_empty() {
                    return Err(ValidationError("workflow name"));
                }
                Ok(())
            }
            Self::WorkflowRunFinished {
                workflow_name,
                state,
                ..
            } => {
                if workflow_name.trim().is_empty() {
                    return Err(ValidationError("workflow name"));
                }
                if *state == WorkflowRunState::Running {
                    return Err(ValidationError("workflow run finished state"));
                }
                Ok(())
            }
            Self::WorkflowStepStarted { step_key, .. }
            | Self::WorkflowStepCompleted { step_key, .. } => validate_step_key(step_key),
            Self::WorkflowLog { .. } => Ok(()),
        }
    }
}

payload_kind!(
    CommandResPayload => CommandRes,
    ThreadAckPayload => ThreadAck,
);
