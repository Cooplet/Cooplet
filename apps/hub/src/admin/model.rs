use cooplet_protocol::{
    HubId, IntakeState, NodeId, ParticipantId, ParticipantKind, ParticipantState, ServiceScope,
    TokenId,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IdentityKind {
    HumanAdmin,
    HumanMember,
    Service,
}

impl IdentityKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::HumanAdmin => "human_admin",
            Self::HumanMember => "human_member",
            Self::Service => "service",
        }
    }

    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "human_admin" => Some(Self::HumanAdmin),
            "human_member" => Some(Self::HumanMember),
            "service" => Some(Self::Service),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ApiTokenKind {
    BearerApi,
    ServiceToken,
}

impl ApiTokenKind {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::BearerApi => "bearer_api",
            Self::ServiceToken => "service_token",
        }
    }

    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "bearer_api" => Some(Self::BearerApi),
            "service_token" => Some(Self::ServiceToken),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeStatus {
    Online,
    Offline,
    Revoked,
}

impl NodeStatus {
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "online" => Some(Self::Online),
            "offline" => Some(Self::Offline),
            "revoked" => Some(Self::Revoked),
            _ => None,
        }
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreatePrincipalRequest {
    pub kind: IdentityKind,
    pub username: String,
    pub display_name: String,
    pub scopes: Vec<ServiceScope>,
    #[serde(deserialize_with = "explicit_option")]
    #[schema(required = true, max_items = 512)]
    pub allowed_nodes: Option<Vec<NodeId>>,
    pub with_participant: Option<bool>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct PrincipalSummary {
    pub principal_id: String,
    pub kind: IdentityKind,
    pub username: String,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub participant_id: Option<ParticipantId>,
    pub scopes: Vec<ServiceScope>,
    #[schema(required = true)]
    pub allowed_nodes: Option<Vec<NodeId>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct IssueTokenRequest {
    pub label: String,
    pub scopes: Vec<ServiceScope>,
    #[serde(deserialize_with = "explicit_option")]
    #[schema(required = true, max_items = 512)]
    pub allowed_nodes: Option<Vec<NodeId>>,
    #[serde(default)]
    pub is_admin: bool,
    pub expires_at_ms: Option<i64>,
}

fn explicit_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct TokenSummary {
    pub token_id: TokenId,
    pub principal_id: String,
    pub token_kind: ApiTokenKind,
    pub is_admin: bool,
    pub label: String,
    pub scopes: Vec<ServiceScope>,
    #[schema(required = true)]
    pub allowed_nodes: Option<Vec<NodeId>>,
    pub durable: bool,
    #[schema(required = true)]
    pub expires_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct IssuedTokenResponse {
    #[serde(flatten)]
    pub summary: TokenSummary,
    pub already_issued: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct IssueEnrollmentRequest {
    pub name: String,
    #[schema(minimum = 1, maximum = 86_400_000, default = 900_000)]
    pub ttl_ms: Option<i64>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct EnrollmentIssueSummary {
    pub token_id: TokenId,
    pub node_id: NodeId,
    pub label: String,
    pub expires_at_ms: i64,
    pub created_at_ms: i64,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct IssuedEnrollmentResponse {
    #[serde(flatten)]
    pub summary: EnrollmentIssueSummary,
    pub already_issued: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct EnrollmentTokenSummary {
    #[serde(flatten)]
    pub summary: EnrollmentIssueSummary,
    pub principal_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumed_at_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<i64>,
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct RedeemEnrollmentRequest {
    pub token: String,
    pub name: Option<String>,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct RedeemEnrollmentResponse {
    pub hub_id: HubId,
    pub node_id: NodeId,
    pub generation: u64,
    pub node_token: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct RevokedEnrollment {
    pub token_id: TokenId,
    pub node_id: NodeId,
    pub revoked: bool,
    pub already_consumed: bool,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct Revoked<T> {
    pub id: T,
    pub revoked: bool,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub(crate) struct DisabledPrincipal {
    pub id: String,
    pub disabled: bool,
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct CatalogQuery {
    #[param(minimum = 1, maximum = 500, default = 100)]
    pub limit: Option<usize>,
    pub after: Option<String>,
}

#[derive(Deserialize, IntoParams)]
#[serde(deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub(crate) struct NodeCatalogQuery {
    #[param(minimum = 1, maximum = 500, default = 100)]
    pub limit: Option<usize>,
    pub after: Option<String>,
    pub node_id: Option<NodeId>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CatalogPage<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct NodeSummary {
    pub node_id: NodeId,
    pub display_name: String,
    pub status: NodeStatus,
    pub generation: u64,
    pub session_epoch: u64,
    pub capabilities: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_ms: Option<i64>,
    pub enrolled_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<i64>,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HubParticipantSummary {
    pub node_id: NodeId,
    pub participant_id: ParticipantId,
    pub kind: ParticipantKind,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_dir: Option<String>,
    pub state: ParticipantState,
    pub intake_state: IntakeState,
    pub capabilities: Value,
    pub synced_epoch: u64,
    pub updated_at_ms: i64,
}
