//! Calls initiated by a Node over its existing bridge.

use std::time::Duration;

use crate::{
    AnswerPayload, AttachmentRef, BridgePayload, DeliveryMode, DeliveryState, FrameKind,
    MemberView, MessageId, NodeId, OperationId, ParticipantId, ProblemDetails, QuestionPayload,
    ThreadCreateRequest, ThreadId, ThreadListQuery, ThreadListResponse, ThreadMemberJoinRequest,
    ThreadMemberLeaveRequest, ThreadMemberResumeRequest, ThreadMembersQuery, ThreadMembersResponse,
    ThreadPinDeleteQuery, ThreadPinDeleteResponse, ThreadPinSetRequest, ThreadPinView,
    ThreadPostReceipt, ThreadPostRequest, ThreadReadQuery, ThreadReadResponse, ThreadSummary,
    ThreadWaitRequest, ThreadWaitResponse, ValidationError,
};

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct NodeRequestPayload {
        pub request_id: OperationId,
        pub caller: NodeRequestCaller,
        pub operation: NodeRpcOperation,
    }
}

impl BridgePayload for NodeRequestPayload {
    const KIND: FrameKind = FrameKind::NodeRequest;
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum NodeRequestCaller {
        Participant { participant_id: ParticipantId },
    }
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum NodeRpcOperation {
        ThreadCreate { operation_key: String, body: ThreadCreateRequest },
        ThreadList { query: ThreadListQuery },
        ThreadGet { thread_id: ThreadId },
        ThreadClose { thread_id: ThreadId, operation_key: String },
        ThreadPost { thread_id: ThreadId, operation_key: String, body: ThreadPostRequest },
        ThreadRead { thread_id: ThreadId, query: ThreadReadQuery },
        ThreadPinSet {
            thread_id: ThreadId,
            pin_key: String,
            operation_key: String,
            body: ThreadPinSetRequest,
        },
        ThreadPinDelete {
            thread_id: ThreadId,
            pin_key: String,
            operation_key: String,
            query: ThreadPinDeleteQuery,
        },
        ThreadWait { thread_id: ThreadId, body: ThreadWaitRequest },
        ThreadMembersList { thread_id: ThreadId, query: ThreadMembersQuery },
        ThreadMemberJoin { thread_id: ThreadId, operation_key: String, body: ThreadMemberJoinRequest },
        ThreadMemberLeave { thread_id: ThreadId, operation_key: String, body: ThreadMemberLeaveRequest },
        ThreadMemberResume { thread_id: ThreadId, operation_key: String, body: ThreadMemberResumeRequest },
        SendDirectMessage { operation_key: String, body: DirectMessageRequest },
    }
}

impl NodeRpcOperation {
    /// A wait can change the confirmed sequence even though it never waits on the socket.
    pub fn is_mutating(&self) -> bool {
        !matches!(
            self,
            Self::ThreadList { .. }
                | Self::ThreadGet { .. }
                | Self::ThreadRead { .. }
                | Self::ThreadMembersList { .. },
        )
    }

    pub fn correlation_timeout(&self) -> Duration {
        let seconds = match self {
            Self::SendDirectMessage { body, .. } if !body.attachments.is_empty() => 75,
            Self::SendDirectMessage { .. } => 45,
            _ => 30,
        };
        Duration::from_secs(seconds)
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct NodeResponsePayload {
        pub request_id: OperationId,
        pub outcome: NodeRpcOutcome,
    }
}

impl BridgePayload for NodeResponsePayload {
    const KIND: FrameKind = FrameKind::NodeResponse;

    fn validate(&self) -> Result<(), ValidationError> {
        if let NodeRpcOutcome::Problem { problem } = &self.outcome
            && !(400..=599).contains(&problem.status)
        {
            return Err(ValidationError("node response problem status"));
        }
        Ok(())
    }
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum NodeRpcOutcome {
        Result { result: NodeRpcResult },
        Problem { problem: ProblemDetails },
    }
}

crate::wire::wire_object! {
    #[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum NodeRpcResult {
        ThreadCreate { body: ThreadSummary },
        ThreadList { body: ThreadListResponse },
        ThreadGet { body: ThreadSummary },
        ThreadClose { body: ThreadSummary },
        ThreadPost { body: ThreadPostReceipt },
        ThreadRead { body: ThreadReadResponse },
        ThreadPinSet { body: ThreadPinView },
        ThreadPinDelete { body: ThreadPinDeleteResponse },
        ThreadWait { body: ThreadWaitResponse },
        ThreadMembersList { body: ThreadMembersResponse },
        ThreadMemberJoin { body: MemberView },
        ThreadMemberLeave { body: MemberView },
        ThreadMemberResume { body: MemberView },
        SendDirectMessage { body: DirectMessageReceipt },
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct DirectMessageRequest {
        pub target_node_id: Option<NodeId>,
        pub target_participant_id: ParticipantId,
        pub body: String,
        pub delivery_mode: Option<DeliveryMode>,
        pub queue_ttl_ms: Option<u64>,
        #[serde(default)]
        #[schema(max_items = 64)]
        pub attachments: Vec<AttachmentRef>,
        pub question: Option<QuestionPayload>,
        pub in_reply_to: Option<MessageId>,
        pub answer: Option<AnswerPayload>,
    } schema_fields
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct DirectMessageReceipt {
        pub operation_id: OperationId,
        pub message_id: MessageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub in_reply_to: Option<MessageId>,
        pub delivery_state: DeliveryState,
    }
}
