use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{BlobId, HubId, NodeId, ParticipantId, ValidationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantKind {
    Agent,
    Human,
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParticipantState {
    Starting,
    Idle,
    Working,
    AwaitingApproval,
    Stopping,
    Stopped,
    Unavailable,
    Unknown,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum IntakeState {
    Ready,
    Busy,
    AwaitingApproval,
    Unavailable,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    DirectNow,
    QueueUntilIdle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Accepted,
    Queued,
    Dispatched,
    Delivered,
    Busy,
    AwaitingApproval,
    Expired,
    Unavailable,
    Unknown,
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
        if self.name.is_empty() || self.name.len() > 255 || self.name.contains('\0') {
            return Err(ValidationError("attachment name"));
        }
        if self.mime_type.trim().is_empty() {
            return Err(ValidationError("attachment MIME type"));
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalWaitKind {
    Permission,
    Question,
    Unspecified,
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
    if state == ParticipantState::AwaitingApproval && can_respond_remotely.is_none() {
        return Err(ValidationError("missing can_respond_remotely"));
    }
    if can_respond_remotely == Some(true) && approval_id.is_none_or(|id| id.trim().is_empty()) {
        return Err(ValidationError("approval ID for remote response"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalAction {
    Approve,
    Deny,
}

crate::wire::wire_object! {
    #[serde(deny_unknown_fields)]
    pub struct ApprovalDecision {
        pub approval_id: String,
        pub action: ApprovalAction,
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ServiceScope {
    Read,
    Collaborate,
    Manage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Participant,
    Operator,
    Adapter,
    WorkflowRun,
    HumanAdmin,
    HumanMember,
    Service,
    Node,
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
