use cooplet_protocol::{
    AggregateType, ErrorCode, EventKind, NodeId, OperationId, ParticipantId, ServicePrincipalId,
    ServiceScope, TokenId, UserId, canonical_sha256, validate_idempotency_key,
};
use cooplet_storage::{EventNotice, StorageError, WriteError, WriteFailure, WriteOutcome};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, reverify_caller_tx},
    credentials::{
        EnrollmentCredential, Master, SecretToken, enrollment_node_id, mint_bearer,
        mint_enrollment, mint_node, mint_service,
    },
    error::HubError,
    now_ms,
    storage::{
        events::{EventAudience, NewEvent, append, node_disconnected_data},
        operations::{self, NewOperation},
    },
};

use super::{
    catalog::valid_principal_id,
    model::{
        ApiTokenKind, CreatePrincipalRequest, DisabledPrincipal, EnrollmentIssueSummary,
        IdentityKind, IssueEnrollmentRequest, IssueTokenRequest, IssuedEnrollmentResponse,
        IssuedTokenResponse, PrincipalSummary, RedeemEnrollmentRequest, RedeemEnrollmentResponse,
        Revoked, RevokedEnrollment, TokenSummary,
    },
};

const DEFAULT_ENROLLMENT_TTL_MS: i64 = 15 * 60 * 1_000;
const MAX_ENROLLMENT_TTL_MS: i64 = 24 * 60 * 60 * 1_000;
const MAX_SAFE_JSON_INTEGER: i64 = 9_007_199_254_740_991;
const INITIAL_NODE_GENERATION: u64 = 1;
const SELECT_ENROLLMENT_FOR_REDEEM: &str = "
    SELECT t.principal_id, t.label, t.verifier_hmac,
           t.revoked_at_ms IS NULL AND t.consumed_at_ms IS NULL
             AND t.expires_at_ms > ?2 AND p.disabled_at_ms IS NULL
      FROM auth_tokens t JOIN principals p USING (principal_id)
     WHERE t.token_id = ?1 AND t.token_kind = 'enrollment_token'";

pub(crate) async fn create_principal(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    mut request: CreatePrincipalRequest,
) -> Result<PrincipalSummary, HubError> {
    caller.require_admin()?;
    validate_username(&request.username)?;
    validate_display_name(&request.display_name)?;
    normalize_grants(&mut request.scopes, &mut request.allowed_nodes)?;
    if matches!(request.kind, IdentityKind::HumanAdmin)
        && (!request.scopes.contains(&ServiceScope::Manage) || request.allowed_nodes.is_some())
    {
        return Err(HubError::invalid(
            "An administrator principal requires manage and all nodes",
        ));
    }
    let with_participant = match request.kind {
        IdentityKind::Service => request.with_participant.unwrap_or(false),
        _ if request.with_participant == Some(false) => {
            return Err(HubError::invalid(
                "A human principal requires a participant ID",
            ));
        }
        _ => true,
    };
    request.with_participant = Some(with_participant);

    let operation = AdminOperation::new(key, "principal.create", &request)?;
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let username_exists = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM principals WHERE username = ?1)",
            [&request.username],
            |row| row.get::<_, bool>(0),
        )?;
        if username_exists {
            return Err(conflict("Username already exists", "username_exists"));
        }

        let principal_id = match request.kind {
            IdentityKind::Service => ServicePrincipalId::generate()?.to_string(),
            _ => UserId::generate()?.to_string(),
        };
        let participant_id = with_participant.then(ParticipantId::generate).transpose()?;
        let scopes_json = serde_json::to_string(&request.scopes)?;
        let allowed_nodes_json = optional_json(request.allowed_nodes.as_ref())?;
        tx.execute(
            "INSERT INTO principals (principal_id, kind, username, display_name, participant_id,
                                     scopes_json, allowed_nodes_json, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                principal_id,
                request.kind.as_str(),
                request.username,
                request.display_name,
                participant_id.as_ref().map(ParticipantId::as_str),
                scopes_json,
                allowed_nodes_json,
                now,
            ],
        )?;

        let summary = PrincipalSummary {
            principal_id: principal_id.clone(),
            kind: request.kind,
            username: request.username,
            display_name: request.display_name,
            participant_id,
            scopes: request.scopes,
            allowed_nodes: request.allowed_nodes,
            disabled_at_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        Ok(AdminCommit::new(summary, principal_id))
    })
    .await?;
    Ok(commit.data)
}

pub(crate) async fn issue_token(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    principal_id: &str,
    mut request: IssueTokenRequest,
) -> Result<IssuedTokenResponse, HubError> {
    caller.require_admin()?;
    valid_principal_id(principal_id)?;
    validate_label(&request.label)?;
    normalize_grants(&mut request.scopes, &mut request.allowed_nodes)?;
    if request.expires_at_ms.is_none() && !request.is_admin {
        return Err(HubError::invalid(
            "Non-administrator tokens require an expiration time",
        ));
    }
    if request
        .expires_at_ms
        .is_some_and(|expiry| !(1..=MAX_SAFE_JSON_INTEGER).contains(&expiry))
    {
        return Err(HubError::invalid("Invalid token expiration time"));
    }
    if request.is_admin
        && (!request.scopes.contains(&ServiceScope::Manage) || request.allowed_nodes.is_some())
    {
        return Err(HubError::invalid(
            "An admin token requires manage and all nodes",
        ));
    }

    let operation = AdminOperation::new(key, "principal.token.issue", &(principal_id, &request))?;
    let principal_id = principal_id.to_owned();
    let master = state.master.clone();
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let (kind, disabled_at, scopes_json, allowed_nodes_json): (String, Option<i64>, String, Option<String>) =
            tx.query_row(
                "SELECT kind, disabled_at_ms, scopes_json, allowed_nodes_json
                   FROM principals WHERE principal_id = ?1",
                [&principal_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?
            .ok_or_else(|| not_found("Principal not found"))?;
        if disabled_at.is_some() {
            return Err(conflict("Principal is disabled", "principal_disabled"));
        }
        if request.expires_at_ms.is_some_and(|expiry| expiry <= now) {
            return Err(HubError::invalid("Token expiration must be in the future"));
        }
        let kind = IdentityKind::parse(&kind).ok_or_else(|| HubError::internal("Invalid principal kind"))?;
        if request.is_admin {
            let scopes: Vec<ServiceScope> = serde_json::from_str(&scopes_json)?;
            if !matches!(kind, IdentityKind::HumanAdmin)
                || !scopes.contains(&ServiceScope::Manage)
                || allowed_nodes_json.is_some()
            {
                return Err(HubError::invalid("Principal cannot receive an administrator token"));
            }
        }

        let token_id = TokenId::generate()?;
        let (token_kind, minted) = match kind {
            IdentityKind::Service => {
                let id = ServicePrincipalId::parse(&principal_id)?;
                (ApiTokenKind::ServiceToken, mint_service(&master, &id)?)
            }
            _ => (ApiTokenKind::BearerApi, mint_bearer(&master, token_id.clone())?),
        };
        let scopes_json = serde_json::to_string(&request.scopes)?;
        let allowed_nodes_json = optional_json(request.allowed_nodes.as_ref())?;
        tx.execute(
            "INSERT INTO auth_tokens (token_id, principal_id, token_kind, is_admin, verifier_hmac,
                                      label, scopes_json, allowed_nodes_json, expires_at_ms, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                token_id.as_str(),
                principal_id,
                token_kind.as_str(),
                request.is_admin,
                minted.verifier,
                request.label,
                scopes_json,
                allowed_nodes_json,
                request.expires_at_ms.unwrap_or(i64::MAX),
                now,
            ],
        )?;

        let resource_id = token_id.to_string();
        let summary = TokenSummary {
            token_id,
            principal_id,
            token_kind,
            is_admin: request.is_admin,
            label: request.label,
            scopes: request.scopes,
            allowed_nodes: request.allowed_nodes,
            durable: request.expires_at_ms.is_none(),
            expires_at_ms: request.expires_at_ms,
            revoked_at_ms: None,
            created_at_ms: now,
        };
        let mut commit = AdminCommit::new(summary, resource_id);
        commit.secret = Some(minted.secret);
        Ok(commit)
    })
    .await?;
    Ok(IssuedTokenResponse {
        summary: commit.data,
        already_issued: commit.secret.is_none(),
        token: commit.secret.map(SecretToken::into_string),
    })
}

pub(crate) async fn revoke_token(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    principal_id: &str,
    token_id: TokenId,
) -> Result<Revoked<TokenId>, HubError> {
    caller.require_admin()?;
    valid_principal_id(principal_id)?;
    let mut operation =
        AdminOperation::new(key, "principal.token.revoke", &(principal_id, &token_id))?;
    operation.notify = Notify::Revocation;
    let principal_id = principal_id.to_owned();
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let revoked_at: Option<i64> = tx.query_row(
            "SELECT revoked_at_ms FROM auth_tokens
              WHERE token_id = ?1 AND principal_id = ?2 AND token_kind != 'enrollment_token'",
            params![token_id.as_str(), principal_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| not_found("Token not found for principal"))?;
        let changed = revoked_at.is_none();
        if changed {
            require_remaining_admin(tx, None, Some(&token_id))?;
            tx.execute(
                "UPDATE auth_tokens SET revoked_at_ms = ?1 WHERE token_id = ?2 AND revoked_at_ms IS NULL",
                params![now, token_id.as_str()],
            )?;
        }
        let resource_id = token_id.to_string();
        let mut commit = AdminCommit::new(Revoked { id: token_id, revoked: true }, resource_id);
        commit.changed = changed;
        Ok(commit)
    })
    .await?;
    Ok(commit.data)
}

pub(crate) async fn disable_principal(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    principal_id: &str,
) -> Result<DisabledPrincipal, HubError> {
    caller.require_admin()?;
    valid_principal_id(principal_id)?;
    let mut operation = AdminOperation::new(key, "principal.disable", &principal_id)?;
    operation.notify = Notify::Revocation;
    let principal_id = principal_id.to_owned();
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let disabled_at: Option<i64> = tx
            .query_row(
                "SELECT disabled_at_ms FROM principals WHERE principal_id = ?1",
                [&principal_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| not_found("Principal not found"))?;
        let changed = disabled_at.is_none();
        if changed {
            require_remaining_admin(tx, Some(&principal_id), None)?;
            tx.execute(
                "UPDATE principals SET disabled_at_ms = ?1, updated_at_ms = ?1
                  WHERE principal_id = ?2 AND disabled_at_ms IS NULL",
                params![now, principal_id],
            )?;
            tx.execute(
                "UPDATE auth_tokens SET revoked_at_ms = ?1
                  WHERE principal_id = ?2 AND revoked_at_ms IS NULL AND consumed_at_ms IS NULL",
                params![now, principal_id],
            )?;
        }

        let resource_id = principal_id.clone();
        let mut commit = AdminCommit::new(
            DisabledPrincipal {
                id: principal_id,
                disabled: true,
            },
            resource_id,
        );
        commit.changed = changed;
        Ok(commit)
    })
    .await?;
    Ok(commit.data)
}

pub(crate) async fn issue_enrollment(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    mut request: IssueEnrollmentRequest,
) -> Result<IssuedEnrollmentResponse, HubError> {
    caller.require_admin()?;
    validate_display_name(&request.name)?;
    let ttl_ms = request.ttl_ms.unwrap_or(DEFAULT_ENROLLMENT_TTL_MS);
    if !(1..=MAX_ENROLLMENT_TTL_MS).contains(&ttl_ms) {
        return Err(HubError::invalid(
            "Enrollment token lifetime must be between 1 ms and 24 hours",
        ));
    }
    request.ttl_ms = Some(ttl_ms);

    let operation = AdminOperation::new(key, "enrollment.issue", &request)?;
    let master = state.master.clone();
    let issuer = caller.actor_id().to_owned();
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let (token_id, node_id) = reserve_enrollment_id(tx)?;
        let minted = mint_enrollment(&master, token_id.clone())?;
        let expiry = now.saturating_add(ttl_ms);
        tx.execute(
            "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac, label,
                                      expires_at_ms, created_at_ms)
             VALUES (?1, ?2, 'enrollment_token', ?3, ?4, ?5, ?6)",
            params![
                token_id.as_str(),
                issuer,
                minted.verifier,
                request.name,
                expiry,
                now
            ],
        )?;

        let resource_id = token_id.to_string();
        let summary = EnrollmentIssueSummary {
            token_id,
            node_id,
            label: request.name,
            expires_at_ms: expiry,
            created_at_ms: now,
        };
        let mut commit = AdminCommit::new(summary, resource_id);
        commit.secret = Some(minted.secret);
        Ok(commit)
    })
    .await?;
    Ok(IssuedEnrollmentResponse {
        summary: commit.data,
        already_issued: commit.secret.is_none(),
        token: commit.secret.map(SecretToken::into_string),
    })
}

pub(crate) async fn revoke_enrollment(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    token_id: TokenId,
) -> Result<RevokedEnrollment, HubError> {
    caller.require_admin()?;
    let operation = AdminOperation::new(key, "enrollment.revoke", &token_id)?;
    let node_id = enrollment_node_id(&token_id)?;
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let consumed_at: Option<i64> = tx
            .query_row(
                "SELECT consumed_at_ms FROM auth_tokens
              WHERE token_id = ?1 AND token_kind = 'enrollment_token'",
                [token_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| not_found("Enrollment token not found"))?;
        let already_consumed = consumed_at.is_some();
        if !already_consumed {
            tx.execute(
                "UPDATE auth_tokens SET revoked_at_ms = ?1
                  WHERE token_id = ?2 AND revoked_at_ms IS NULL",
                params![now, token_id.as_str()],
            )?;
        }
        let resource_id = token_id.to_string();
        let response = RevokedEnrollment {
            token_id,
            node_id,
            revoked: !already_consumed,
            already_consumed,
        };
        Ok(AdminCommit::new(response, resource_id))
    })
    .await?;
    Ok(commit.data)
}

pub(crate) async fn redeem_enrollment(
    state: &AppState,
    request: RedeemEnrollmentRequest,
) -> Result<RedeemEnrollmentResponse, HubError> {
    if let Some(name) = &request.name {
        validate_display_name(name)?;
    }
    // Parse syntax and run an RO check before touching the single writer thread so
    // unauthenticated invalid tokens cannot saturate DbActor.
    let credential =
        EnrollmentCredential::parse(&request.token).ok_or_else(unauthorized_enrollment)?;
    let precheck_master = state.master.clone();
    let credential = state
        .reads
        .read(move |conn| {
            Ok(
                verify_enrollment_on(conn, &precheck_master, &credential, now_ms())
                    .map(|_| credential),
            )
        })
        .await??;

    let master = state.master.clone();
    let (node_id, secret) = state
        .db
        .write(move |tx| {
            let now = now_ms();
            let (owner, label) = verify_enrollment_on(tx, &master, &credential, now)?;
            let token_id = credential.token_id.as_str();
            let node_id = enrollment_node_id(&credential.token_id)?;
            let minted = mint_node(&master, &node_id, INITIAL_NODE_GENERATION)?;
            let name = request.name.unwrap_or(label);
            tx.prepare_cached("UPDATE auth_tokens SET consumed_at_ms = ?1 WHERE token_id = ?2")?
                .execute(params![now, token_id])?;
            tx.prepare_cached(
                "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac,
                                generation, session_epoch, status, enrolled_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, 'offline', ?6)",
            )?
            .execute(params![
                node_id.as_str(),
                name,
                owner,
                minted.verifier,
                INITIAL_NODE_GENERATION,
                now,
            ])?;
            Ok::<_, WriteError<HubError>>(WriteOutcome::new((node_id, minted.secret)))
        })
        .await?;
    Ok(RedeemEnrollmentResponse {
        hub_id: state.meta.hub_id.clone(),
        node_id,
        generation: INITIAL_NODE_GENERATION,
        node_token: secret.into_string(),
    })
}

fn verify_enrollment_on(
    conn: &Connection,
    master: &Master,
    credential: &EnrollmentCredential,
    now: i64,
) -> Result<(String, String), HubError> {
    let mut statement = conn.prepare_cached(SELECT_ENROLLMENT_FOR_REDEEM)?;
    let enrollment: Option<(String, String, String, bool)> = statement
        .query_row(params![credential.token_id.as_str(), now], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .optional()?;
    let Some((owner, label, verifier, available)) = enrollment else {
        return Err(unauthorized_enrollment());
    };
    if !credential.verify(master, &verifier)? || !available {
        return Err(unauthorized_enrollment());
    }
    Ok((owner, label))
}

pub(crate) async fn revoke_node(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    node_id: NodeId,
) -> Result<Revoked<NodeId>, HubError> {
    caller.require_admin()?;
    let mut operation = AdminOperation::new(key, "node.revoke", &node_id)?;
    operation.notify = Notify::Node(node_id.clone());
    let hub_id = state.meta.hub_id.clone();
    let actor_id = caller.actor_id().to_owned();
    let commit = admin_write(state, caller, operation, move |tx, now| {
        let (status, revoked_at, session_epoch): (String, Option<i64>, u64) = tx
            .query_row(
                "SELECT status, revoked_at_ms, session_epoch FROM nodes WHERE node_id = ?1",
                [node_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| not_found("Node not found"))?;
        let changed = revoked_at.is_none() && status != "revoked";
        let mut events = Vec::new();
        if changed {
            tx.execute(
                "UPDATE nodes SET status = 'revoked', revoked_at_ms = ?1 WHERE node_id = ?2",
                params![now, node_id.as_str()],
            )?;
            tx.execute(
                "DELETE FROM node_participants WHERE node_id = ?1",
                [node_id.as_str()],
            )?;
            if status == "online" {
                events.push(append(
                    tx,
                    &hub_id,
                    NewEvent {
                        kind: EventKind::NodeDisconnected,
                        aggregate_type: AggregateType::Node,
                        aggregate_id: node_id.as_str(),
                        actor_id: Some(&actor_id),
                        created_at_ms: now,
                        data: node_disconnected_data(&node_id, "revoked", session_epoch),
                        audience: EventAudience::Nodes {
                            node_ids: vec![node_id.clone()],
                        },
                    },
                )?);
            }
        }
        let resource_id = node_id.to_string();
        let mut commit = AdminCommit::new(
            Revoked {
                id: node_id,
                revoked: true,
            },
            resource_id,
        );
        commit.changed = changed;
        commit.events = events;
        Ok(commit)
    })
    .await?;
    Ok(commit.data)
}

fn reserve_enrollment_id(tx: &Transaction<'_>) -> Result<(TokenId, NodeId), HubError> {
    for _ in 0..4 {
        let token_id = TokenId::generate()?;
        let node_id = enrollment_node_id(&token_id)?;
        let occupied = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM auth_tokens WHERE token_id = ?1)
                 OR EXISTS(SELECT 1 FROM nodes WHERE node_id = ?2)",
            params![token_id.as_str(), node_id.as_str()],
            |row| row.get::<_, bool>(0),
        )?;
        if !occupied {
            return Ok((token_id, node_id));
        }
    }
    Err(HubError::internal(
        "Could not allocate an enrollment token identifier",
    ))
}

fn require_remaining_admin(
    tx: &Transaction<'_>,
    excluded_principal: Option<&str>,
    excluded_token: Option<&TokenId>,
) -> Result<(), HubError> {
    let remaining = tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM principals p JOIN auth_tokens t USING (principal_id)
             WHERE p.kind = 'human_admin' AND p.disabled_at_ms IS NULL AND p.allowed_nodes_json IS NULL
               AND t.token_kind = 'bearer_api' AND t.is_admin = 1 AND t.revoked_at_ms IS NULL
               AND t.consumed_at_ms IS NULL AND t.expires_at_ms = ?1 AND t.allowed_nodes_json IS NULL
               AND EXISTS (SELECT 1 FROM json_each(p.scopes_json) WHERE value = 'manage')
               AND EXISTS (SELECT 1 FROM json_each(t.scopes_json) WHERE value = 'manage')
               AND (?2 IS NULL OR p.principal_id != ?2)
               AND (?3 IS NULL OR t.token_id != ?3)
        )",
        params![i64::MAX, excluded_principal, excluded_token.map(TokenId::as_str)],
        |row| row.get::<_, bool>(0),
    )?;
    if !remaining {
        return Err(conflict(
            "At least one durable administrator token must remain",
            "last_durable_admin",
        ));
    }
    Ok(())
}

fn validate_username(value: &str) -> Result<(), HubError> {
    if !(1..=64).contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(HubError::invalid("Invalid username"));
    }
    Ok(())
}

fn validate_display_name(value: &str) -> Result<(), HubError> {
    cooplet_protocol::validate_display_name(value).map_err(|_| {
        HubError::invalid(
            "Name must be non-blank, at most 128 UTF-8 bytes, and free of control, bidirectional, or zero-width characters",
        )
    })
}

fn validate_label(value: &str) -> Result<(), HubError> {
    cooplet_protocol::validate_label(value).map_err(|_| {
        HubError::invalid(
            "Label exceeds 128 UTF-8 bytes or contains a forbidden control, bidirectional, or zero-width character",
        )
    })
}

fn normalize_grants(
    scopes: &mut Vec<ServiceScope>,
    allowed_nodes: &mut Option<Vec<NodeId>>,
) -> Result<(), HubError> {
    scopes.sort_unstable();
    scopes.dedup();
    if let Some(nodes) = allowed_nodes {
        if nodes.len() > 512 {
            return Err(HubError::invalid("At most 512 node IDs may be allowed"));
        }
        nodes.sort_unstable();
        if nodes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(HubError::invalid("Allowed node IDs must be unique"));
        }
    }
    Ok(())
}

fn optional_json(value: Option<&impl Serialize>) -> Result<Option<String>, HubError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn conflict(detail: &'static str, reason: &'static str) -> HubError {
    HubError::new(ErrorCode::OperationConflict, detail).with_extension("reason", reason)
}

fn not_found(detail: &'static str) -> HubError {
    HubError::new(ErrorCode::NotFound, detail)
}

fn unauthorized_enrollment() -> HubError {
    HubError::new(
        ErrorCode::Unauthorized,
        "Invalid or unavailable enrollment token",
    )
}

#[derive(Clone)]
struct AdminOperation {
    key: String,
    kind: &'static str,
    digest: String,
    notify: Notify,
}

impl AdminOperation {
    fn new(key: &str, kind: &'static str, payload: &impl Serialize) -> Result<Self, HubError> {
        validate_idempotency_key(key).map_err(|error| HubError::invalid(error.to_string()))?;
        Ok(Self {
            key: key.to_owned(),
            kind,
            digest: canonical_sha256(payload)?,
            notify: Notify::None,
        })
    }
}

#[derive(Clone)]
enum Notify {
    None,
    Revocation,
    Node(NodeId),
}

struct AdminCommit<T> {
    data: T,
    resource_id: Option<String>,
    secret: Option<SecretToken>,
    changed: bool,
    events: Vec<EventNotice>,
}

impl<T> AdminCommit<T> {
    fn new(data: T, resource_id: String) -> Self {
        Self {
            data,
            resource_id: Some(resource_id),
            secret: None,
            changed: false,
            events: Vec::new(),
        }
    }

    fn replay(data: T) -> Self {
        Self {
            data,
            resource_id: None,
            secret: None,
            changed: false,
            events: Vec::new(),
        }
    }
}

async fn admin_write<T, F>(
    state: &AppState,
    caller: &AuthenticatedCaller,
    operation: AdminOperation,
    apply: F,
) -> Result<AdminCommit<T>, HubError>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    F: FnOnce(&Transaction<'_>, i64) -> Result<AdminCommit<T>, HubError> + Send + 'static,
{
    let detached = !matches!(operation.notify, Notify::None);
    let apply = perform_admin_write(state.clone(), caller.clone(), operation, apply);
    if detached {
        state
            .background
            .spawn(apply)?
            .await
            .map_err(HubError::internal)?
    } else {
        apply.await
    }
}

async fn perform_admin_write<T, F>(
    state: AppState,
    caller: AuthenticatedCaller,
    operation: AdminOperation,
    apply: F,
) -> Result<AdminCommit<T>, HubError>
where
    T: Serialize + DeserializeOwned + Send + 'static,
    F: FnOnce(&Transaction<'_>, i64) -> Result<AdminCommit<T>, HubError> + Send + 'static,
{
    let _transition = match (&operation.notify, state.bridge.live()) {
        (Notify::Node(node_id), Some(bridge)) => Some(bridge.transition(node_id).await),
        _ => None,
    };
    let actor_id = caller.actor_id().to_owned();
    let write_actor = actor_id.clone();
    let write_operation = operation.clone();
    let result = state
        .db
        .write(move |tx| {
            let now = now_ms();
            reverify_caller_tx(tx, &caller, now)?;
            caller.require_admin()?;
            if let Some(data) = find_receipt(tx, &write_actor, &write_operation)? {
                return Ok(WriteOutcome::new(AdminCommit::replay(data)));
            }

            let mut commit = apply(tx, now)?;
            let receipt = serde_json::to_string(&commit.data).map_err(HubError::from)?;
            let operation_id = OperationId::generate().map_err(HubError::from)?;
            operations::insert(
                tx,
                NewOperation {
                    operation_id: &operation_id,
                    actor_id: &write_actor,
                    key: &write_operation.key,
                    kind: write_operation.kind,
                    request_sha256: &write_operation.digest,
                    target_node_id: None,
                    state: "succeeded",
                    resource_id: commit.resource_id.as_deref(),
                    result_json: Some(&receipt),
                    now_ms: now,
                },
            )?;
            let events = std::mem::take(&mut commit.events);
            Ok::<_, WriteError<HubError>>(WriteOutcome::with_events(commit, events))
        })
        .await;

    match result {
        Ok(commit) => {
            if commit.changed {
                notify_revocation(&state, &operation.notify, false).await;
            }
            Ok(commit)
        }
        Err(WriteFailure::Storage(
            error @ (StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)),
        )) => {
            notify_revocation(&state, &operation.notify, true).await;
            // The read can confirm a commit and its public receipt. It can never recover a secret.
            let read = state
                .reads
                .read(move |conn| Ok(find_receipt(conn, &actor_id, &operation)))
                .await;
            match read {
                Ok(Ok(Some(receipt))) => Ok(AdminCommit::replay(receipt)),
                Ok(Err(reconciled)) => Err(reconciled),
                Ok(Ok(None)) | Err(_) => Err(error.into()),
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn find_receipt<T: DeserializeOwned>(
    conn: &Connection,
    actor_id: &str,
    operation: &AdminOperation,
) -> Result<Option<T>, HubError> {
    let Some(saved) = operations::load(conn, actor_id, &operation.key)? else {
        return Ok(None);
    };
    saved.check_match(operation.kind, &operation.digest)?;
    match saved.state.as_str() {
        "succeeded" => {
            let body = saved.result_json.as_deref().ok_or_else(|| {
                HubError::internal("successful administrator receipt has no body")
            })?;
            Ok(Some(serde_json::from_str(body)?))
        }
        "failed" => Err(saved.failed_error()?),
        "unknown" => {
            Err(saved.unknown_error("The Hub could not confirm the write outcome.", false)?)
        }
        "accepted" | "in_progress" => {
            Err(saved.in_progress_error("The operation is still in progress.")?)
        }
        _ => Err(HubError::internal(
            "Invalid persisted administrator operation receipt",
        )),
    }
}

async fn notify_revocation(state: &AppState, notify: &Notify, verify: bool) {
    if matches!(notify, Notify::None) {
        return;
    }
    state.revocations.bump();
    if let Notify::Node(node_id) = notify {
        if verify {
            let check_id = node_id.clone();
            let revoked = state
                .reads
                .read(move |conn| {
                    Ok(conn.query_row(
                        "SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id = ?1
                                    AND (status = 'revoked' OR revoked_at_ms IS NOT NULL))",
                        [check_id.as_str()],
                        |row| row.get::<_, bool>(0),
                    )?)
                })
                .await;
            match revoked {
                Ok(true) => {}
                Ok(false) => return,
                Err(error) => {
                    tracing::warn!(%error, %node_id, "Could not confirm node revocation");
                    state.bridge.disconnect_transient(node_id);
                    return;
                }
            }
        }
        state.bridge.disconnect_revoked(node_id);
    }
}
