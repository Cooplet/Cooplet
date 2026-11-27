//! Public thread requests and views, shared by the Hub and its Node proxy.

use serde_json::Value;

use crate::{
    AnswerPayload, AttachmentRef, DeliveryMode, MessageId, NodeId, OperationId, QuestionPayload,
    ThreadId, ThreadPostWire, ValidationError,
};

crate::wire::wire_string_enum! {
    pub enum ThreadStatus {
        Open => "open",
        Closed => "closed",
    }
}

crate::wire::wire_string_enum! {
    pub enum ThreadRole {
        Owner => "owner",
        Member => "member",
        Observer => "observer",
    }
}

crate::wire::wire_string_enum! {
    pub enum ThreadWakePolicy {
        MentionsOnly => "mentions_only",
        All => "all",
        Muted => "muted",
    }
}

crate::wire::wire_string_enum! {
    pub enum ThreadMemberState {
        Active => "active",
        PausedUnknown => "paused_unknown",
        Left => "left",
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadCreateRequest {
        pub title: String,
        pub topic: Option<String>,
        pub creator_handle: Option<String>,
        #[serde(default)]
        pub members: Vec<ThreadMemberJoinRequest>,
    }
}

crate::wire::wire_object! {
    #[derive(utoipa::IntoParams, Default)]
    #[into_params(parameter_in = Query)]
    #[serde(deny_unknown_fields)]
    pub struct ThreadListQuery {
        #[param(minimum = 1, maximum = 100, default = 20)]
        pub limit: Option<u32>,
        pub after: Option<ThreadId>,
        pub status: Option<ThreadStatus>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadCloseRequest {}
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPostRequest {
        pub body: String,
        #[serde(default)]
        pub mentions: Vec<String>,
        #[serde(default)]
        #[schema(max_items = 64)]
        pub attachments: Vec<AttachmentRef>,
        pub question: Option<QuestionPayload>,
        pub answer: Option<AnswerPayload>,
        pub in_reply_to: Option<MessageId>,
    } schema_fields
}

crate::wire::wire_object! {
    #[derive(utoipa::IntoParams, Default)]
    #[into_params(parameter_in = Query)]
    #[serde(deny_unknown_fields)]
    pub struct ThreadReadQuery {
        pub since_seq: Option<u64>,
        #[param(minimum = 1, maximum = 100, default = 20)]
        pub limit: Option<u32>,
        #[serde(default)]
        pub include_pins: bool,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPinSetRequest {
        pub value_json: Value,
        pub post_id: Option<MessageId>,
        pub expected_version: Option<u64>,
    }
}

crate::wire::wire_object! {
    #[derive(utoipa::IntoParams, Default)]
    #[into_params(parameter_in = Query)]
    #[serde(deny_unknown_fields)]
    pub struct ThreadPinDeleteQuery {
        pub expected_version: Option<u64>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadWaitRequest {
        pub since_seq: u64,
        pub wake_policy: Option<ThreadWakePolicy>,
    }
}

crate::wire::wire_object! {
    #[derive(utoipa::IntoParams, Default)]
    #[into_params(parameter_in = Query)]
    #[serde(deny_unknown_fields)]
    pub struct ThreadMembersQuery {
        #[param(minimum = 1, maximum = 100, default = 20)]
        pub limit: Option<u32>,
        pub after: Option<String>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadMemberJoinRequest {
        pub member_key: String,
        pub handle: Option<String>,
        pub role: Option<ThreadRole>,
        pub wake_policy: Option<ThreadWakePolicy>,
        pub delivery_mode: Option<DeliveryMode>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadMemberLeaveRequest {
        pub member_key: Option<String>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadMemberResumeRequest {
        pub member_key: Option<String>,
        pub after_seq: u64,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadSummary {
        pub thread_id: ThreadId,
        pub title: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub topic: Option<String>,
        pub created_by_actor_id: String,
        pub status: ThreadStatus,
        pub head_seq: u64,
        pub floor_seq: u64,
        pub created_at_ms: i64,
        pub updated_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub closed_at_ms: Option<i64>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadListResponse {
        pub items: Vec<ThreadSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub next_after: Option<ThreadId>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPostReceipt {
        pub post_id: MessageId,
        pub seq: u64,
        pub created_at_ms: i64,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadReadResponse {
        pub thread: ThreadSummary,
        pub head_seq: u64,
        pub floor_seq: u64,
        pub pins: Vec<ThreadPinView>,
        pub posts: Vec<ThreadPostWire>,
        pub has_more: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub next_since_seq: Option<u64>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPinView {
        pub thread_id: ThreadId,
        pub pin_key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub post_id: Option<MessageId>,
        pub value_json: Value,
        pub version: u64,
        pub updated_by_actor_id: String,
        pub updated_at_ms: i64,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadPinDeleteResponse {
        pub thread_id: ThreadId,
        pub pin_key: String,
        pub deleted: bool,
        pub version: u64,
    }
}

crate::wire::wire_object! {
    #[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
    pub enum ThreadWaitResponse {
        Ready {
            head_seq: u64,
            floor_seq: u64,
            posts: Vec<ThreadPostWire>,
            has_more: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            next_since_seq: Option<u64>,
        },
        Waiting {
            head_seq: u64,
        },
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ThreadMembersResponse {
        pub items: Vec<MemberView>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub next_after: Option<String>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct MemberView {
        pub member_key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub node_id: Option<NodeId>,
        pub participant_id: String,
        pub handle: String,
        pub role: ThreadRole,
        pub wake_policy: ThreadWakePolicy,
        pub delivery_mode: DeliveryMode,
        pub state: ThreadMemberState,
        pub delivery_generation: u64,
        pub confirmed_seq: u64,
        pub pending: Option<PendingThreadDelivery>,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct PendingThreadDelivery {
        pub delivery_id: OperationId,
        pub from_seq: u64,
        pub to_seq: u64,
    }
}

pub fn validate_pin_key(key: &str) -> Result<(), ValidationError> {
    if (1..=64).contains(&key.len())
        && !matches!(key, "." | "..")
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        Ok(())
    } else {
        Err(ValidationError("pin key"))
    }
}
