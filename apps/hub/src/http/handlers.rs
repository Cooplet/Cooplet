use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use cooplet_protocol::{
    AttachmentRef, BlobId, EventStreamFrame, EventsPage, HealthResponse, HubInfoResponse,
    WhoamiResponse,
};
use serde::Serialize;

use crate::{
    AppState, blobs,
    error::HubError,
    event_bus::{self, EventsQuery},
    openapi::openapi_bytes,
};

use super::extract::{Caller, Revocations, StrictPath, StrictQuery};

pub(crate) mod admin_routes;
pub(crate) mod command_routes;

pub(super) fn json<T: Serialize>(status: StatusCode, data: T) -> Result<Response, HubError> {
    Ok((
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&data)?,
    )
        .into_response())
}

#[utoipa::path(
    get, path = "/v1/info", tag = "Hub", security(()),
    responses((status = 200, description = "Hub identity and protocol version.", body = HubInfoResponse))
)]
pub(super) async fn info(State(state): State<AppState>) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        HubInfoResponse {
            hub_id: state.meta.hub_id,
            display_name: state.meta.display_name,
            protocol_version: 1,
        },
    )
}

#[utoipa::path(
    get, path = "/v1/openapi.json", tag = "Hub", security(()),
    responses((status = 200, description = "OpenAPI 3.1 document for this Hub.", content(("application/json"))))
)]
pub(super) async fn openapi() -> Result<Response, HubError> {
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        openapi_bytes()?.clone(),
    )
        .into_response())
}

#[utoipa::path(
    get, path = "/v1/health", tag = "Hub",
    responses((status = 200, description = "Hub is ready to serve authenticated requests.", body = HealthResponse))
)]
pub(super) async fn health() -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        HealthResponse {
            status: "ok".into(),
        },
    )
}

#[utoipa::path(
    get, path = "/v1/whoami", tag = "Hub",
    responses((status = 200, description = "Authenticated caller and its effective scopes.", body = WhoamiResponse))
)]
pub(super) async fn whoami(Caller(caller): Caller) -> Result<Response, HubError> {
    json(StatusCode::OK, caller.whoami())
}

#[utoipa::path(
    post, path = "/v1/blobs", tag = "Files",
    description = "Upload raw file bytes using their concrete Content-Type. Requires collaborate scope or a Node credential.",
    request_body(
        content(("application/octet-stream"), ("*/*")),
        description = "Raw binary body. The default byte limit is 50 MiB.",
    ),
    params(
        ("X-Cooplet-Filename" = String, Header, description = "Percent-encoded UTF-8 filename."),
        ("X-Cooplet-Sha256" = Option<String>, Header, description = "Lowercase SHA-256 hex digest.", pattern = "^[0-9a-f]{64}$"),
        ("Content-Length" = Option<u64>, Header, description = "Length of the file body in bytes."),
    ),
    responses(
        (status = 200, description = "Previously stored attachment.", body = AttachmentRef),
        (status = 201, description = "File stored.", body = AttachmentRef),
        (status = 403, description = "The credential cannot upload the file or read its existing scope."),
        (status = 413, description = "The file exceeds the configured byte limit."),
        (status = 507, description = "The Hub storage quota or database capacity was reached."),
    )
)]
pub(super) async fn upload_blob(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, HubError> {
    let (status, attachment) = blobs::upload(&state, &caller, &headers, body).await?;
    json(status, attachment)
}

#[utoipa::path(
    get, path = "/v1/blobs/{blob_id}", tag = "Files",
    params(("blob_id" = BlobId, Path)),
    responses(
        (status = 200, description = "Raw file bytes with the stored media type.",
            content(("application/octet-stream"), ("*/*")),
            headers(
                ("Content-Length" = u64, description = "Length of the file body in bytes."),
                ("Content-Disposition" = String, description = "Attachment disposition with the original filename."),
                ("ETag" = String, description = "Quoted lowercase SHA-256 digest."),
                ("X-Content-Type-Options" = String, description = "Always nosniff."),
            ),
        ),
        (status = 403, description = "The credential cannot read the file scope."),
        (status = 404, description = "The file is missing or expired."),
    )
)]
pub(super) async fn download_blob(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(blob_id): StrictPath<BlobId>,
) -> Result<Response, HubError> {
    blobs::download(&state, &caller, blob_id).await
}

#[utoipa::path(
    get, path = "/v1/events", tag = "Events",
    description = "Read a page of events visible to the caller. Resume from next_seq, including when a filtered page is empty.",
    params(EventsQuery),
    responses(
        (status = 200, description = "Visible event page and cursor watermarks.", body = EventsPage),
        (status = 403, description = "The credential lacks the read scope."),
        (status = 410, description = "The cursor lies outside the retained event range."),
    )
)]
pub(super) async fn events_page(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<EventsQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        event_bus::page(&state, &caller, query).await?,
    )
}

#[utoipa::path(
    get, path = "/v1/events/stream", tag = "Events",
    description = "Follow an NDJSON stream of visible events and keepalive frames. A pruned cursor during the stream produces one resync frame followed by EOF.",
    params(EventsQuery),
    responses(
        (status = 200, description = "One event stream frame per line.", body = EventStreamFrame, content_type = "application/x-ndjson"),
        (status = 403, description = "The credential lacks the read scope."),
        (status = 410, description = "The initial cursor lies outside the retained event range."),
    )
)]
pub(super) async fn events_stream(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<EventsQuery>,
    Revocations(revocations): Revocations,
) -> Result<Response, HubError> {
    event_bus::stream(&state, &caller, query, revocations).await
}
