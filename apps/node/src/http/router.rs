use axum::{
    Extension, Router,
    body::{Body, HttpBody},
    extract::{DefaultBodyLimit, Request, State},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use cooplet_protocol::ErrorCode;
use http::{
    HeaderValue, Method, StatusCode,
    header::{CACHE_CONTROL, CONTENT_LENGTH, TRANSFER_ENCODING, X_CONTENT_TYPE_OPTIONS},
};

use crate::error::{NodeError, Result};

use super::{
    NodeState, blobs, events,
    extract::{BodyLimit, buffer},
    problem::{invalid_request, payload_too_large},
    routes::*,
};

const CONTROL_BODY_LIMIT: usize = 64 * 1024;
const MESSAGE_BODY_LIMIT: usize = 2 * 1024 * 1024;

pub fn router(state: NodeState) -> Router {
    let messages = Router::new()
        .route("/v1/messages", post(send_message))
        .layer(DefaultBodyLimit::max(MESSAGE_BODY_LIMIT))
        .layer(Extension(BodyLimit(MESSAGE_BODY_LIMIT)));
    let control = Router::new()
        .route("/v1/info", get(info))
        .route("/v1/openapi.json", get(openapi))
        .route("/v1/health", get(health))
        .route("/v1/whoami", get(whoami))
        .route("/v1/participants", get(participants))
        .route("/v1/participants/{id}", get(participant))
        .route("/v1/agents", post(create_agent))
        .route("/v1/agents/{id}/resume", post(resume_agent))
        .route("/v1/agents/{id}/stop", post(stop_agent))
        .route("/v1/participants/{id}/approval", post(approval))
        .route("/v1/participants/{id}/attach-plan", get(attach_plan))
        .route("/v1/messages", get(messages_list))
        .route("/v1/messages/{id}", get(message_get))
        .route("/v1/events", get(events::list))
        .route("/v1/events/stream", get(events::stream))
        .route("/v1/blobs/{id}", get(blobs::get))
        .route("/v1/node/stop", post(stop_node))
        .layer(middleware::from_fn_with_state(state.clone(), bound_get))
        .layer(DefaultBodyLimit::max(CONTROL_BODY_LIMIT))
        .layer(Extension(BodyLimit(CONTROL_BODY_LIMIT)))
        .merge(messages);
    let blobs = Router::new()
        .route("/v1/blobs", post(blobs::put))
        .layer(DefaultBodyLimit::disable());
    control
        .merge(blobs)
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn(no_store))
        .with_state(state)
}

async fn bound_get(
    State(state): State<NodeState>,
    request: Request,
    next: Next,
) -> Result<Response> {
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return Ok(next.run(request).await);
    }
    let limit = request
        .extensions()
        .get::<BodyLimit>()
        .map_or(CONTROL_BODY_LIMIT, |limit| limit.0) as u64;
    if request.body().size_hint().lower() > limit {
        return Err(payload_too_large());
    }
    let mut lengths = request.headers().get_all(CONTENT_LENGTH).iter();
    let non_empty_length = match (lengths.next(), lengths.next()) {
        (None, _) => false,
        (Some(_), Some(_)) => true,
        (Some(value), None) => value.as_bytes() != b"0",
    };
    if request.headers().contains_key(TRANSFER_ENCODING) || non_empty_length {
        return Err(invalid_request(
            "GET and HEAD requests must not include a body",
        ));
    }
    let (parts, body) = request.into_parts();
    let bounded = Request::from_parts(parts.clone(), body);
    let bytes = buffer(bounded, &state).await?;
    if !bytes.is_empty() {
        return Err(invalid_request(
            "GET and HEAD requests must not include a body",
        ));
    }
    Ok(next.run(Request::from_parts(parts, Body::empty())).await)
}

async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

async fn not_found() -> NodeError {
    NodeError::new(ErrorCode::NotFound, "The Node route does not exist")
}

async fn method_not_allowed() -> NodeError {
    let mut error = invalid_request("The route does not support this HTTP method");
    error.0.status = StatusCode::METHOD_NOT_ALLOWED.as_u16();
    error
}
