use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use cooplet_protocol::{NodeId, TokenId};

use crate::{
    AppState,
    admin::{
        self, CatalogPage, CatalogQuery, CreatePrincipalRequest, DisabledPrincipal,
        EnrollmentTokenSummary, HubParticipantSummary, IssueEnrollmentRequest, IssueTokenRequest,
        IssuedEnrollmentResponse, IssuedTokenResponse, NodeCatalogQuery, NodeSummary,
        PrincipalSummary, RedeemEnrollmentRequest, RedeemEnrollmentResponse, Revoked,
        RevokedEnrollment, TokenSummary,
    },
    error::HubError,
};

use super::{
    super::extract::{Caller, EmptyJson, StrictJson, StrictPath, StrictQuery, idempotency_key},
    json,
};

#[utoipa::path(
    get, path = "/v1/principals", tag = "Principals", params(CatalogQuery),
    responses(
        (status = 200, description = "Page of active and disabled principals.", body = inline(CatalogPage<PrincipalSummary>)),
        (status = 403, description = "A full human administrator credential is required."),
    )
)]
pub(in crate::http) async fn list_principals(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<CatalogQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        admin::list_principals(&state, &caller, query).await?,
    )
}

#[utoipa::path(
    post, path = "/v1/principals", tag = "Principals",
    request_body = CreatePrincipalRequest,
    responses(
        (status = 201, description = "Principal created.", body = PrincipalSummary),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 409, description = "Hub identity mismatch, username conflict, or conflicting operation receipt."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn create_principal(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(request): StrictJson<CreatePrincipalRequest>,
) -> Result<Response, HubError> {
    let principal =
        admin::create_principal(&state, &caller, idempotency_key(&headers)?, request).await?;
    json(StatusCode::CREATED, principal)
}

#[utoipa::path(
    get, path = "/v1/principals/{id}/tokens", tag = "Principals",
    params(CatalogQuery, ("id" = String, Path, pattern = "^(usr|sp)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$")),
    responses(
        (status = 200, description = "Page of credential metadata.", body = inline(CatalogPage<TokenSummary>)),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The principal does not exist."),
    )
)]
pub(in crate::http) async fn list_tokens(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<String>,
    StrictQuery(query): StrictQuery<CatalogQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        admin::list_tokens(&state, &caller, &id, query).await?,
    )
}

#[utoipa::path(
    post, path = "/v1/principals/{id}/tokens", tag = "Principals",
    request_body = IssueTokenRequest,
    params(("id" = String, Path, pattern = "^(usr|sp)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$")),
    responses(
        (status = 200, description = "Existing issuance receipt. The secret is omitted.", body = IssuedTokenResponse),
        (status = 201, description = "Token issued. Save the one-time secret.", body = IssuedTokenResponse),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The principal does not exist."),
        (status = 409, description = "Hub identity mismatch, disabled principal, or conflicting operation receipt."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn issue_token(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<String>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<IssueTokenRequest>,
) -> Result<Response, HubError> {
    let result =
        admin::issue_token(&state, &caller, idempotency_key(&headers)?, &id, request).await?;
    json(
        if result.already_issued {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        result,
    )
}

#[utoipa::path(
    post, path = "/v1/principals/{id}/tokens/{token_id}/revoke", tag = "Principals",
    request_body = EmptyJson,
    params(
        ("id" = String, Path, pattern = "^(usr|sp)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$"),
        ("token_id" = TokenId, Path),
    ),
    responses(
        (status = 200, description = "Credential revoked.", body = inline(Revoked<TokenId>)),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The credential does not belong to the principal."),
        (status = 409, description = "Hub identity mismatch, last durable administrator, or conflicting operation receipt."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn revoke_token(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath((id, token_id)): StrictPath<(String, TokenId)>,
    headers: HeaderMap,
    _: EmptyJson,
) -> Result<Response, HubError> {
    let result =
        admin::revoke_token(&state, &caller, idempotency_key(&headers)?, &id, token_id).await?;
    json(StatusCode::OK, result)
}

#[utoipa::path(
    post, path = "/v1/principals/{id}/disable", tag = "Principals",
    request_body = EmptyJson,
    params(("id" = String, Path, pattern = "^(usr|sp)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$")),
    responses(
        (status = 200, description = "Principal disabled and its unused credentials revoked.", body = DisabledPrincipal),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The principal does not exist."),
        (status = 409, description = "Hub identity mismatch, last durable administrator, or conflicting operation receipt."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn disable_principal(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<String>,
    headers: HeaderMap,
    _: EmptyJson,
) -> Result<Response, HubError> {
    let result = admin::disable_principal(&state, &caller, idempotency_key(&headers)?, &id).await?;
    json(StatusCode::OK, result)
}

#[utoipa::path(
    get, path = "/v1/enrollment-tokens", tag = "Enrollment", params(CatalogQuery),
    responses(
        (status = 200, description = "Page of enrollment credential metadata.", body = inline(CatalogPage<EnrollmentTokenSummary>)),
        (status = 403, description = "A full human administrator credential is required."),
    )
)]
pub(in crate::http) async fn list_enrollment(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<CatalogQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        admin::list_enrollment(&state, &caller, query).await?,
    )
}

#[utoipa::path(
    post, path = "/v1/enrollment-tokens", tag = "Enrollment",
    request_body = IssueEnrollmentRequest,
    responses(
        (status = 200, description = "Existing issuance receipt. The secret is omitted.", body = IssuedEnrollmentResponse),
        (status = 201, description = "Single-use token issued. Save the one-time secret.", body = IssuedEnrollmentResponse),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn issue_enrollment(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(request): StrictJson<IssueEnrollmentRequest>,
) -> Result<Response, HubError> {
    let result =
        admin::issue_enrollment(&state, &caller, idempotency_key(&headers)?, request).await?;
    json(
        if result.already_issued {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        result,
    )
}

#[utoipa::path(
    post, path = "/v1/enrollment-tokens/{token_id}/revoke", tag = "Enrollment",
    request_body = EmptyJson,
    params(("token_id" = TokenId, Path)),
    responses(
        (status = 200, description = "Enrollment token revoked or already consumed.", body = RevokedEnrollment),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The enrollment token does not exist."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn revoke_enrollment(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(token_id): StrictPath<TokenId>,
    headers: HeaderMap,
    _: EmptyJson,
) -> Result<Response, HubError> {
    let result =
        admin::revoke_enrollment(&state, &caller, idempotency_key(&headers)?, token_id).await?;
    json(StatusCode::OK, result)
}

#[utoipa::path(
    post, path = "/v1/enrollment/token", tag = "Enrollment", security(()),
    description = "Redeem a single-use enrollment credential. The HTTP Authorization header must be absent.",
    request_body = RedeemEnrollmentRequest,
    responses(
        (status = 201, description = "Node enrolled with its own bearer credential.", body = RedeemEnrollmentResponse),
        (status = 401, description = "The enrollment credential is invalid, expired, already consumed, or revoked, or Authorization was supplied."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn redeem_enrollment(
    State(state): State<AppState>,
    StrictJson(request): StrictJson<RedeemEnrollmentRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::CREATED,
        admin::redeem_enrollment(&state, request).await?,
    )
}

#[utoipa::path(
    post, path = "/v1/nodes/{node_id}/revoke", tag = "Nodes",
    request_body = EmptyJson,
    params(("node_id" = NodeId, Path)),
    responses(
        (status = 200, description = "Node credential revoked.", body = inline(Revoked<NodeId>)),
        (status = 403, description = "A full human administrator credential is required."),
        (status = 404, description = "The node does not exist."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn revoke_node(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(node_id): StrictPath<NodeId>,
    headers: HeaderMap,
    _: EmptyJson,
) -> Result<Response, HubError> {
    let result = admin::revoke_node(&state, &caller, idempotency_key(&headers)?, node_id).await?;
    json(StatusCode::OK, result)
}

#[utoipa::path(
    get, path = "/v1/nodes", tag = "Nodes", params(NodeCatalogQuery),
    responses(
        (status = 200, description = "Page of nodes within the credential's scope.", body = inline(CatalogPage<NodeSummary>)),
        (status = 403, description = "The credential lacks the read scope or access to the requested node."),
    )
)]
pub(in crate::http) async fn list_nodes(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<NodeCatalogQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        admin::list_nodes(&state, &caller, query).await?,
    )
}

#[utoipa::path(
    get, path = "/v1/participants", tag = "Nodes", params(NodeCatalogQuery),
    responses(
        (status = 200, description = "Page of participants on visible, unrevoked nodes.", body = inline(CatalogPage<HubParticipantSummary>)),
        (status = 403, description = "The credential lacks the read scope or access to the requested node."),
    )
)]
pub(in crate::http) async fn list_participants(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<NodeCatalogQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        admin::list_participants(&state, &caller, query).await?,
    )
}
