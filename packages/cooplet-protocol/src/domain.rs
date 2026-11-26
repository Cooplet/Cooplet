use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{BlobId, HubId, NodeId, ParticipantId, ValidationError, validate_media_type};

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum ParticipantKind {
        Agent => "agent",
        Human => "human",
        System => "system",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum ParticipantState {
        Starting => "starting",
        Idle => "idle",
        Working => "working",
        AwaitingApproval => "awaiting_approval",
        Stopping => "stopping",
        Stopped => "stopped",
        Unavailable => "unavailable",
        Unknown => "unknown",
    }
}

impl ParticipantState {
    pub const fn intake_state(&self) -> IntakeState {
        match self {
            Self::Idle => IntakeState::Ready,
            Self::Working => IntakeState::Busy,
            Self::AwaitingApproval => IntakeState::AwaitingApproval,
            Self::Starting | Self::Stopping | Self::Stopped | Self::Unavailable => {
                IntakeState::Unavailable
            }
            Self::Unknown => IntakeState::Unknown,
        }
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum IntakeState {
        Ready => "ready",
        Busy => "busy",
        AwaitingApproval => "awaiting_approval",
        Unavailable => "unavailable",
        Unknown => "unknown",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum DeliveryMode {
        DirectNow => "direct_now",
        QueueUntilIdle => "queue_until_idle",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum DeliveryState {
        Accepted => "accepted",
        Queued => "queued",
        Dispatched => "dispatched",
        Delivered => "delivered",
        Busy => "busy",
        AwaitingApproval => "awaiting_approval",
        Expired => "expired",
        Unavailable => "unavailable",
        Unknown => "unknown",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum WorkflowRunState {
        Running => "running",
        Completed => "completed",
        Failed => "failed",
        Cancelled => "cancelled",
        Interrupted => "interrupted",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum WorkflowStepStatus {
        Completed => "completed",
        Failed => "failed",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum WorkflowLogLevel {
        Debug => "debug",
        Info => "info",
        Warn => "warn",
        Error => "error",
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct AttachmentRef {
        pub blob_id: BlobId,
        pub name: String,
        pub mime_type: String,
        pub size_bytes: u64,
        pub sha256: String,
    }
}

impl AttachmentRef {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.name.is_empty()
            || self.name.len() > 255
            || self.name.trim().is_empty()
            || self.name.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
        {
            return Err(ValidationError("attachment name"));
        }
        validate_media_type(&self.mime_type)
            .map_err(|_| ValidationError("attachment MIME type"))?;
        validate_sha256(&self.sha256)
    }
}

pub fn validate_sha256(sha256: &str) -> Result<(), ValidationError> {
    if sha256.len() == 64
        && sha256
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(ValidationError("SHA-256 digest"))
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum ApprovalWaitKind {
        Permission => "permission",
        Question => "question",
        Unspecified => "unspecified",
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ApprovalWait {
        #[serde(skip_serializing_if = "Option::is_none")]
        pub approval_id: Option<String>,
        pub kind: ApprovalWaitKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub summary: Option<String>,
        pub can_respond_remotely: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub action_hint: Option<String>,
    }
}

impl ApprovalWait {
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_approval_fields(
            ParticipantState::AwaitingApproval,
            Some(self.can_respond_remotely),
            self.approval_id.as_deref(),
        )
    }
}

pub(crate) fn validate_approval_fields(
    state: ParticipantState,
    can_respond_remotely: Option<bool>,
    approval_id: Option<&str>,
) -> Result<(), ValidationError> {
    if approval_id.is_some_and(|id| id.trim().is_empty()) {
        return Err(ValidationError("approval ID"));
    }
    if state == ParticipantState::AwaitingApproval {
        let Some(remote) = can_respond_remotely else {
            return Err(ValidationError("missing can_respond_remotely"));
        };
        if remote && approval_id.is_none() {
            return Err(ValidationError("approval ID for remote response"));
        }
    } else if can_respond_remotely.is_some() || approval_id.is_some() {
        return Err(ValidationError("approval fields outside awaiting_approval"));
    }
    Ok(())
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum ApprovalAction {
        Approve => "approve",
        Deny => "deny",
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ApprovalDecision {
        pub approval_id: String,
        pub action: ApprovalAction,
    }
}

impl ApprovalDecision {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.approval_id.trim().is_empty() {
            return Err(ValidationError("approval decision ID"));
        }
        Ok(())
    }
}

crate::wire::wire_string_enum! {
    #[derive(PartialOrd, Ord, Hash)]
    pub enum ServiceScope {
        Read => "read",
        Collaborate => "collaborate",
        Manage => "manage",
    }
}

crate::wire::wire_string_enum! {
    #[derive(Hash)]
    pub enum PrincipalKind {
        Participant => "participant",
        Operator => "operator",
        Adapter => "adapter",
        WorkflowRun => "workflow_run",
        HumanAdmin => "human_admin",
        HumanMember => "human_member",
        Service => "service",
        Node => "node",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(untagged, deny_unknown_fields)]
pub enum InfoResponse {
    Hub(#[schema(inline)] HubInfoResponse),
    Node(#[schema(inline)] NodeInfoResponse),
}

impl InfoResponse {
    pub const fn protocol_version(&self) -> u8 {
        match self {
            Self::Hub(info) => info.protocol_version,
            Self::Node(info) => info.protocol_version,
        }
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct HubInfoResponse {
        pub hub_id: HubId,
        pub display_name: String,
        pub protocol_version: u8,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct NodeInfoResponse {
        pub node_id: NodeId,
        pub display_name: String,
        pub protocol_version: u8,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct HealthResponse {
        pub status: String,
    }
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct WhoamiResponse {
        pub actor_id: String,
        pub principal_kind: PrincipalKind,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub node_id: Option<NodeId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub participant_id: Option<ParticipantId>,
        pub name: String,
        pub scopes: Vec<ServiceScope>,
    }
}
