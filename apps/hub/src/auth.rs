use std::collections::BTreeSet;

use cooplet_protocol::{
    ErrorCode, NodeId, ParticipantId, PrincipalKind, ServicePrincipalId, ServiceScope, TokenId,
    UserId, WhoamiResponse,
};
use cooplet_storage::ReadPool;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};
use tokio::sync::watch;

use crate::{
    credentials::{ApiCredential, Master},
    error::HubError,
    now_ms,
};

#[derive(Clone)]
pub(crate) struct AuthenticatedCaller {
    identity: Identity,
    display_name: String,
}

#[derive(Clone, PartialEq, Eq)]
enum Identity {
    Human {
        id: UserId,
        participant: ParticipantId,
        admin_role: bool,
        token: TokenAuthority,
    },
    Service {
        id: ServicePrincipalId,
        participant: Option<ParticipantId>,
        token: TokenAuthority,
    },
    Node {
        id: NodeId,
        generation: u64,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct TokenAuthority {
    id: TokenId,
    is_admin_token: bool,
    scope: BTreeSet<ServiceScope>,
    nodes: Option<BTreeSet<NodeId>>,
    expires_at_ms: i64,
}

impl AuthenticatedCaller {
    pub(crate) fn actor_id(&self) -> &str {
        match &self.identity {
            Identity::Human { id, .. } => id.as_str(),
            Identity::Service { id, .. } => id.as_str(),
            Identity::Node { id, .. } => id.as_str(),
        }
    }

    pub(crate) fn participant_id(&self) -> Option<&ParticipantId> {
        match &self.identity {
            Identity::Human { participant, .. } => Some(participant),
            Identity::Service { participant, .. } => participant.as_ref(),
            Identity::Node { .. } => None,
        }
    }

    pub(crate) fn is_hub_recipient(
        &self,
        node: Option<&NodeId>,
        participant: &ParticipantId,
    ) -> bool {
        node.is_none() && self.participant_id() == Some(participant)
    }

    pub(crate) fn node_id(&self) -> Option<&NodeId> {
        match &self.identity {
            Identity::Node { id, .. } => Some(id),
            _ => None,
        }
    }

    pub(crate) fn is_admin(&self) -> bool {
        matches!(
            &self.identity,
            Identity::Human { admin_role: true, token, .. }
                if token.is_admin_token && token.nodes.is_none() && token.scope.contains(&ServiceScope::Manage)
        )
    }

    pub(crate) fn is_service(&self) -> bool {
        matches!(self.identity, Identity::Service { .. })
    }

    pub(crate) fn is_global_reader(&self) -> bool {
        self.is_admin()
            || matches!(&self.identity, Identity::Service { token, .. }
                if token.nodes.is_none() && token.scope.contains(&ServiceScope::Read))
    }

    pub(crate) fn require_admin(&self) -> Result<(), HubError> {
        if self.is_admin() {
            Ok(())
        } else {
            Err(HubError::new(
                ErrorCode::AdminHumanRequired,
                "A full human administrator bearer token is required.",
            ))
        }
    }

    pub(crate) fn require_scope(&self, scope: ServiceScope) -> Result<(), HubError> {
        if self
            .token()
            .is_some_and(|token| token.scope.contains(&scope))
        {
            Ok(())
        } else {
            Err(HubError::new(
                ErrorCode::Forbidden,
                "The bearer token does not grant the required scope.",
            ))
        }
    }

    pub(crate) fn allowed_nodes(&self) -> Option<&BTreeSet<NodeId>> {
        self.token().and_then(|token| token.nodes.as_ref())
    }

    pub(crate) fn allows_node(&self, node_id: &NodeId) -> bool {
        match &self.identity {
            Identity::Node { id, .. } => id == node_id,
            _ => self
                .allowed_nodes()
                .is_none_or(|nodes| nodes.contains(node_id)),
        }
    }

    pub(crate) fn expires_at_ms(&self) -> Option<i64> {
        self.token().map(|token| token.expires_at_ms)
    }

    pub(crate) fn whoami(&self) -> WhoamiResponse {
        let principal_kind = match &self.identity {
            Identity::Human { .. } if self.is_admin() => PrincipalKind::HumanAdmin,
            Identity::Human { .. } => PrincipalKind::HumanMember,
            Identity::Service { .. } => PrincipalKind::Service,
            Identity::Node { .. } => PrincipalKind::Node,
        };
        WhoamiResponse {
            actor_id: self.actor_id().into(),
            principal_kind,
            node_id: self.node_id().cloned(),
            participant_id: self.participant_id().cloned(),
            name: self.display_name.clone(),
            scopes: self
                .token()
                .map_or_else(Vec::new, |token| token.scope.iter().copied().collect()),
        }
    }

    fn token(&self) -> Option<&TokenAuthority> {
        match &self.identity {
            Identity::Human { token, .. } | Identity::Service { token, .. } => Some(token),
            Identity::Node { .. } => None,
        }
    }
}

const SELECT_BEARER_TOKEN: &str = "
    SELECT t.token_id, t.token_kind, t.verifier_hmac, t.is_admin, t.scopes_json,
           t.allowed_nodes_json, t.expires_at_ms, t.revoked_at_ms,
           p.principal_id, p.kind, p.participant_id, p.scopes_json,
           p.allowed_nodes_json, p.disabled_at_ms, p.display_name
    FROM auth_tokens t JOIN principals p ON p.principal_id = t.principal_id
    WHERE t.token_id = ?1 AND t.token_kind = 'bearer_api'";

const SELECT_SERVICE_TOKEN: &str = "
    SELECT t.token_id, t.token_kind, t.verifier_hmac, t.is_admin, t.scopes_json,
           t.allowed_nodes_json, t.expires_at_ms, t.revoked_at_ms,
           p.principal_id, p.kind, p.participant_id, p.scopes_json,
           p.allowed_nodes_json, p.disabled_at_ms, p.display_name
    FROM auth_tokens t JOIN principals p ON p.principal_id = t.principal_id
    WHERE t.verifier_hmac = ?1 AND t.principal_id = ?2 AND t.token_kind = 'service_token'";

const SELECT_REVERIFY_TOKEN: &str = "
    SELECT t.token_id, t.token_kind, t.verifier_hmac, t.is_admin, t.scopes_json,
           t.allowed_nodes_json, t.expires_at_ms, t.revoked_at_ms,
           p.principal_id, p.kind, p.participant_id, p.scopes_json,
           p.allowed_nodes_json, p.disabled_at_ms, p.display_name
    FROM auth_tokens t JOIN principals p ON p.principal_id = t.principal_id
    WHERE t.token_id = ?1 AND t.principal_id = ?2";

const SELECT_NODE: &str = "
    SELECT credential_hmac, generation, status = 'revoked' OR revoked_at_ms IS NOT NULL, display_name
    FROM nodes WHERE node_id = ?1";

struct TokenRow {
    token_id: String,
    token_kind: String,
    verifier: String,
    is_admin_token: bool,
    token_scopes: String,
    token_nodes: Option<String>,
    expires_at_ms: i64,
    revoked_at_ms: Option<i64>,
    principal_id: String,
    principal_kind: String,
    participant_id: Option<String>,
    principal_scopes: String,
    principal_nodes: Option<String>,
    disabled_at_ms: Option<i64>,
    display_name: String,
}

impl TokenRow {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            token_id: row.get(0)?,
            token_kind: row.get(1)?,
            verifier: row.get(2)?,
            is_admin_token: row.get(3)?,
            token_scopes: row.get(4)?,
            token_nodes: row.get(5)?,
            expires_at_ms: row.get(6)?,
            revoked_at_ms: row.get(7)?,
            principal_id: row.get(8)?,
            principal_kind: row.get(9)?,
            participant_id: row.get(10)?,
            principal_scopes: row.get(11)?,
            principal_nodes: row.get(12)?,
            disabled_at_ms: row.get(13)?,
            display_name: row.get(14)?,
        })
    }

    fn authorize(self, now: i64) -> Result<AuthenticatedCaller, HubError> {
        if self.expires_at_ms <= now
            || self.revoked_at_ms.is_some()
            || self.disabled_at_ms.is_some()
        {
            return Err(unauthorized());
        }
        let principal_scope = decode_scope(&self.principal_scopes)?;
        let token_scope = decode_scope(&self.token_scopes)?;
        let principal_nodes = decode_nodes(self.principal_nodes.as_deref())?;
        let token_nodes = decode_nodes(self.token_nodes.as_deref())?;
        let admin_role = self.principal_kind == "human_admin";
        let valid_admin_token = self.token_kind == "bearer_api"
            && admin_role
            && principal_scope.contains(&ServiceScope::Manage)
            && token_scope.contains(&ServiceScope::Manage)
            && principal_nodes.is_none()
            && token_nodes.is_none();
        if self.is_admin_token && !valid_admin_token {
            return Err(unauthorized());
        }

        let token = TokenAuthority {
            id: TokenId::parse(&self.token_id).map_err(HubError::internal)?,
            is_admin_token: self.is_admin_token,
            scope: principal_scope
                .intersection(&token_scope)
                .copied()
                .collect(),
            nodes: match (principal_nodes, token_nodes) {
                (Some(principal), Some(token)) => {
                    Some(principal.intersection(&token).cloned().collect())
                }
                (principal, token) => principal.or(token),
            },
            expires_at_ms: self.expires_at_ms,
        };
        let participant = self
            .participant_id
            .as_deref()
            .map(ParticipantId::parse)
            .transpose()
            .map_err(HubError::internal)?;
        let identity = match (self.token_kind.as_str(), self.principal_kind.as_str()) {
            ("bearer_api", "human_admin" | "human_member") => Identity::Human {
                id: UserId::parse(&self.principal_id).map_err(HubError::internal)?,
                participant: participant
                    .ok_or_else(|| HubError::internal("Human principal lacks a participant"))?,
                admin_role,
                token,
            },
            ("service_token", "service") => Identity::Service {
                id: ServicePrincipalId::parse(&self.principal_id).map_err(HubError::internal)?,
                participant,
                token,
            },
            _ => return Err(unauthorized()),
        };
        Ok(AuthenticatedCaller {
            identity,
            display_name: self.display_name,
        })
    }
}

fn expand_scopes(highest: Option<ServiceScope>) -> BTreeSet<ServiceScope> {
    [
        ServiceScope::Read,
        ServiceScope::Collaborate,
        ServiceScope::Manage,
    ]
    .into_iter()
    .filter(|scope| highest.is_some_and(|maximum| *scope <= maximum))
    .collect()
}

fn decode_scope(json: &str) -> Result<BTreeSet<ServiceScope>, HubError> {
    let scopes: Vec<ServiceScope> = serde_json::from_str(json)?;
    Ok(expand_scopes(scopes.into_iter().max()))
}

fn decode_nodes(json: Option<&str>) -> Result<Option<BTreeSet<NodeId>>, HubError> {
    json.map(serde_json::from_str)
        .transpose()
        .map_err(HubError::from)
}

struct NodeRow {
    verifier: String,
    generation: u64,
    revoked: bool,
    display_name: String,
}

fn node(conn: &Connection, id: &NodeId) -> Result<NodeRow, HubError> {
    let mut statement = conn.prepare_cached(SELECT_NODE)?;
    statement
        .query_row([id.as_str()], |row| {
            Ok(NodeRow {
                verifier: row.get(0)?,
                generation: row.get(1)?,
                revoked: row.get(2)?,
                display_name: row.get(3)?,
            })
        })
        .optional()?
        .ok_or_else(unauthorized)
}

fn node_caller(id: NodeId, row: NodeRow) -> Result<AuthenticatedCaller, HubError> {
    if row.revoked {
        return Err(HubError::new(
            ErrorCode::NodeRevoked,
            "The Hub has revoked this node credential.",
        ));
    }
    Ok(AuthenticatedCaller {
        identity: Identity::Node {
            id,
            generation: row.generation,
        },
        display_name: row.display_name,
    })
}

pub(crate) async fn authenticate(
    reads: &ReadPool,
    master: &std::sync::Arc<Master>,
    secret: &str,
) -> Result<AuthenticatedCaller, HubError> {
    // Reject malformed Bearer syntax before touching the blocking read pool.
    let credential = ApiCredential::parse(secret).ok_or_else(unauthorized)?;
    let master = master.clone();
    reads
        .read(move |conn| Ok(authenticate_on(conn, &master, &credential)))
        .await?
}

fn authenticate_on(
    conn: &Connection,
    master: &Master,
    credential: &ApiCredential,
) -> Result<AuthenticatedCaller, HubError> {
    let token = match credential {
        ApiCredential::Bearer(id, _) => {
            let mut statement = conn.prepare_cached(SELECT_BEARER_TOKEN)?;
            statement
                .query_row([id.as_str()], TokenRow::read)
                .optional()?
        }
        ApiCredential::Service(id, _) => {
            let mut statement = conn.prepare_cached(SELECT_SERVICE_TOKEN)?;
            statement
                .query_row(
                    params![credential.service_lookup(master)?, id.as_str()],
                    TokenRow::read,
                )
                .optional()?
        }
        ApiCredential::Node(id, _) => {
            let row = node(conn, id)?;
            if !credential.verify(master, &row.verifier, row.generation)? {
                return Err(unauthorized());
            }
            return node_caller(id.clone(), row);
        }
    };
    let token = token.ok_or_else(unauthorized)?;
    if !credential.verify(master, &token.verifier, 0)? {
        return Err(unauthorized());
    }
    token.authorize(now_ms())
}

pub(crate) fn reverify_caller_tx(
    tx: &Transaction<'_>,
    caller: &AuthenticatedCaller,
    now: i64,
) -> Result<(), HubError> {
    reverify_on(tx, caller, now)
}

pub(crate) async fn reverify_caller(
    reads: &ReadPool,
    caller: &AuthenticatedCaller,
) -> Result<(), HubError> {
    let caller = caller.clone();
    reads
        .read(move |conn| Ok(reverify_on(conn, &caller, now_ms())))
        .await?
}

fn reverify_on(conn: &Connection, caller: &AuthenticatedCaller, now: i64) -> Result<(), HubError> {
    let current = match &caller.identity {
        Identity::Human { token, .. } | Identity::Service { token, .. } => {
            let mut statement = conn.prepare_cached(SELECT_REVERIFY_TOKEN)?;
            statement
                .query_row(
                    params![token.id.as_str(), caller.actor_id()],
                    TokenRow::read,
                )
                .optional()?
                .ok_or_else(unauthorized)?
                .authorize(now)?
        }
        Identity::Node { id, generation } => {
            let row = node(conn, id)?;
            if row.generation != *generation {
                return Err(unauthorized());
            }
            node_caller(id.clone(), row)?
        }
    };
    if current.identity != caller.identity {
        return Err(HubError::new(
            ErrorCode::Forbidden,
            "The caller's authority has changed.",
        ));
    }
    Ok(())
}

fn unauthorized() -> HubError {
    HubError::new(
        ErrorCode::Unauthorized,
        "A valid Hub bearer token is required.",
    )
}

#[derive(Clone)]
pub(crate) struct RevocationWatch(watch::Sender<u64>);

impl Default for RevocationWatch {
    fn default() -> Self {
        Self(watch::channel(0).0)
    }
}

impl RevocationWatch {
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.subscribe()
    }

    pub(crate) fn bump(&self) {
        self.0
            .send_modify(|version| *version = version.saturating_add(1));
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn Error>>;

    fn sample_token(
        principal_scopes: &str,
        token_scopes: &str,
        principal_nodes: Option<String>,
        token_nodes: Option<String>,
        expires_at_ms: i64,
    ) -> TestResult<TokenRow> {
        Ok(TokenRow {
            token_id: TokenId::generate()?.to_string(),
            token_kind: "service_token".into(),
            verifier: "0".repeat(64),
            is_admin_token: false,
            token_scopes: token_scopes.into(),
            token_nodes,
            expires_at_ms,
            revoked_at_ms: None,
            principal_id: ServicePrincipalId::generate()?.to_string(),
            principal_kind: "service".into(),
            participant_id: None,
            principal_scopes: principal_scopes.into(),
            principal_nodes,
            disabled_at_ms: None,
            display_name: "Service".into(),
        })
    }

    #[test]
    fn expired_tokens_are_rejected_at_or_after_deadline() -> TestResult {
        let row = sample_token(r#"["read"]"#, r#"["read"]"#, None, None, 1_000)?;
        let err = row
            .authorize(1_000)
            .err()
            .ok_or("accepted token at exact expiry")?;
        assert_eq!(err.problem().code, ErrorCode::Unauthorized);

        let row = sample_token(r#"["read"]"#, r#"["read"]"#, None, None, 999)?;
        let err = row.authorize(1_000).err().ok_or("accepted expired token")?;
        assert_eq!(err.problem().code, ErrorCode::Unauthorized);

        let row = sample_token(r#"["read"]"#, r#"["read"]"#, None, None, 1_001)?;
        assert!(row.authorize(1_000).is_ok());
        Ok(())
    }

    #[test]
    fn allowed_nodes_and_scopes_intersect_principal_and_token_grants() -> TestResult {
        let a = NodeId::generate()?;
        let b = NodeId::generate()?;
        let c = NodeId::generate()?;

        let overlapped = sample_token(
            r#"["collaborate"]"#,
            r#"["manage"]"#,
            Some(serde_json::to_string(&[&a, &b])?),
            Some(serde_json::to_string(&[&b, &c])?),
            5_000,
        )?
        .authorize(1_000)?;
        assert!(!overlapped.allows_node(&a));
        assert!(overlapped.allows_node(&b));
        assert!(!overlapped.allows_node(&c));
        assert_eq!(
            overlapped.whoami().scopes,
            vec![ServiceScope::Read, ServiceScope::Collaborate]
        );
        assert!(overlapped.require_scope(ServiceScope::Collaborate).is_ok());
        assert!(overlapped.require_scope(ServiceScope::Manage).is_err());

        let disjoint = sample_token(
            r#"["read"]"#,
            r#"["read"]"#,
            Some(serde_json::to_string(&[&a])?),
            Some(serde_json::to_string(&[&c])?),
            5_000,
        )?
        .authorize(1_000)?;
        assert_eq!(disjoint.allowed_nodes(), Some(&BTreeSet::new()));
        assert!(!disjoint.allows_node(&a));
        assert!(!disjoint.allows_node(&c));
        assert!(!disjoint.is_global_reader());

        let principal_only = sample_token(
            r#"["read"]"#,
            r#"["read"]"#,
            Some(serde_json::to_string(&[&a])?),
            None,
            5_000,
        )?
        .authorize(1_000)?;
        assert!(principal_only.allows_node(&a));
        assert!(!principal_only.allows_node(&b));

        let token_only = sample_token(
            r#"["read"]"#,
            r#"["read"]"#,
            None,
            Some(serde_json::to_string(&[&c])?),
            5_000,
        )?
        .authorize(1_000)?;
        assert!(token_only.allows_node(&c));
        assert!(!token_only.allows_node(&a));
        Ok(())
    }
}
