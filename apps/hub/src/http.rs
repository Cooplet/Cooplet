use std::sync::Arc;

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use cooplet_protocol::{ErrorCode, HubId};
use tokio::sync::Semaphore;

use crate::{AppState, error::HubError};

mod admission;
mod extract;
pub(crate) mod handlers;
pub(crate) use extract::EmptyJson;
use handlers::{admin_routes::*, command_routes::*, *};

const JSON_BODY_LIMIT: usize = 64 * 1024;
const ENROLLMENT_BODY_LIMIT: usize = 16 * 1024;
pub(crate) const BEARER_CHALLENGE: &str = "Bearer realm=\"cooplet-hub\"";

pub(crate) struct RequestLimits {
    requests: usize,
    event_streams: usize,
    blob_transfers: usize,
}

impl RequestLimits {
    pub(crate) fn new(
        max_concurrent_requests: usize,
        max_active_event_streams: usize,
        max_concurrent_blob_transfers: usize,
    ) -> Result<Self, HubError> {
        if [
            max_concurrent_requests,
            max_active_event_streams,
            max_concurrent_blob_transfers,
        ]
        .into_iter()
        .any(|limit| !(1..=Semaphore::MAX_PERMITS).contains(&limit))
        {
            return Err(HubError::invalid(
                "HTTP concurrency limits must be positive and bounded.",
            ));
        }
        Ok(Self {
            requests: max_concurrent_requests,
            event_streams: max_active_event_streams,
            blob_transfers: max_concurrent_blob_transfers,
        })
    }
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            requests: 256,
            event_streams: 64,
            blob_transfers: 32,
        }
    }
}

pub(crate) fn router(state: AppState, limits: RequestLimits) -> Router {
    use admission::Access;

    let requests = Arc::new(Semaphore::new(limits.requests));
    let protect = |router: Router<AppState>, access: Access| {
        apply_guard(router, &state, access, requests.clone())
    };

    let public = Router::new()
        .route("/v1/info", get(info))
        .route("/v1/openapi.json", get(openapi));
    let admin = Router::new()
        .route(
            "/v1/principals",
            get(list_principals).post(create_principal),
        )
        .route(
            "/v1/principals/{id}/tokens",
            get(list_tokens).post(issue_token),
        )
        .route(
            "/v1/principals/{id}/tokens/{token_id}/revoke",
            post(revoke_token),
        )
        .route("/v1/principals/{id}/disable", post(disable_principal))
        .route(
            "/v1/enrollment-tokens",
            get(list_enrollment).post(issue_enrollment),
        )
        .route(
            "/v1/enrollment-tokens/{token_id}/revoke",
            post(revoke_enrollment),
        )
        .route("/v1/nodes/{node_id}/revoke", post(revoke_node));
    let protected = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/whoami", get(whoami))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/participants", get(list_participants))
        .route("/v1/messages", post(send_message))
        .route("/v1/messages/{message_id}", get(get_message))
        .route("/v1/agents", post(create_agent))
        .route("/v1/agents/{participant_id}/stop", post(stop_agent))
        .route(
            "/v1/participants/{participant_id}/approval",
            post(respond_approval),
        )
        .route("/v1/workflows/runs", post(run_workflow))
        .route("/v1/workflows/runs/{run_id}/cancel", post(cancel_workflow))
        .route("/v1/events", get(events_page));
    let streams = apply_guard(
        Router::new().route("/v1/events/stream", get(events_stream)),
        &state,
        Access::EventStream,
        Arc::new(Semaphore::new(limits.event_streams)),
    );

    let json = protect(public, Access::Public)
        .merge(protect(admin, Access::Admin))
        .merge(protect(protected, Access::Bearer))
        .merge(streams)
        .layer(DefaultBodyLimit::max(JSON_BODY_LIMIT));

    let enrollment = protect(
        Router::new().route("/v1/enrollment/token", post(redeem_enrollment)),
        Access::Enrollment,
    )
    .layer(DefaultBodyLimit::max(ENROLLMENT_BODY_LIMIT));

    let blobs = apply_guard(
        Router::new()
            .route("/v1/blobs", post(upload_blob))
            .route("/v1/blobs/{blob_id}", get(download_blob)),
        &state,
        Access::Blob,
        Arc::new(Semaphore::new(limits.blob_transfers)),
    )
    .layer(DefaultBodyLimit::disable());

    json.merge(enrollment)
        .merge(blobs)
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.meta.hub_id.clone(),
            response_headers,
        ))
        .with_state(state)
}

fn apply_guard(
    router: Router<AppState>,
    state: &AppState,
    access: admission::Access,
    permits: Arc<Semaphore>,
) -> Router<AppState> {
    router.route_layer(middleware::from_fn_with_state(
        admission::Gate::new(state.clone(), access, permits),
        admission::check_request,
    ))
}

async fn not_found() -> HubError {
    HubError::new(ErrorCode::NotFound, "No Hub route matches this path.")
}

async fn method_not_allowed() -> HubError {
    HubError::method_not_allowed()
}

async fn response_headers(State(hub_id): State<HubId>, request: Request, next: Next) -> Response {
    use axum::http::{HeaderValue, header};
    use cooplet_client::X_COOPLET_HUB;

    let mut response = next.run(request).await;
    // Branded Hub IDs contain only ASCII characters allowed in HTTP headers.
    if let Ok(pin) = HeaderValue::from_str(hub_id.as_str()) {
        response.headers_mut().insert(X_COOPLET_HUB, pin);
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
