use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post, put},
};
use cooplet_protocol::{
    MemberView, ThreadCreateRequest, ThreadId, ThreadListQuery, ThreadListResponse,
    ThreadMemberJoinRequest, ThreadMemberLeaveRequest, ThreadMemberResumeRequest,
    ThreadMembersQuery, ThreadMembersResponse, ThreadPinDeleteQuery, ThreadPinDeleteResponse,
    ThreadPinSetRequest, ThreadPinView, ThreadPostReceipt, ThreadPostRequest, ThreadReadQuery,
    ThreadReadResponse, ThreadSummary, ThreadWaitRequest, ThreadWaitResponse,
};

use crate::{
    AppState,
    error::HubError,
    http::extract::{Caller, EmptyJson, StrictJson, StrictPath, StrictQuery, idempotency_key},
    threads,
};

use super::json;

pub(in crate::http) fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/threads", get(list).post(create))
        .route("/v1/threads/{id}", get(read_thread))
        .route("/v1/threads/{id}/close", post(close))
        .route("/v1/threads/{id}/posts", get(read_posts).post(post_message))
        .route(
            "/v1/threads/{id}/pins/{pin_key}",
            put(set_pin).delete(delete_pin),
        )
        .route("/v1/threads/{id}/wait", post(wait))
        .route("/v1/threads/{id}/members", get(list_members))
        .route("/v1/threads/{id}/members/join", post(join))
        .route("/v1/threads/{id}/members/leave", post(leave))
        .route("/v1/threads/{id}/members/resume", post(resume))
}

#[utoipa::path(post, path = "/v1/threads", tag = "Threads", request_body = ThreadCreateRequest,
    responses(
        (status = 201, description = "Thread created.", body = ThreadSummary),
        (status = 403, description = "The caller cannot create threads or add a listed node."),
    )
)]
pub(in crate::http) async fn create(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadCreateRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::CREATED,
        threads::create(&state, &caller, idempotency_key(&headers)?, body).await?,
    )
}

#[utoipa::path(get, path = "/v1/threads", tag = "Threads", params(ThreadListQuery),
    responses(
        (status = 200, description = "Visible threads in thread ID order.", body = ThreadListResponse),
        (status = 403, description = "The caller lacks read scope."),
    )
)]
pub(in crate::http) async fn list(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictQuery(query): StrictQuery<ThreadListQuery>,
) -> Result<Response, HubError> {
    json(StatusCode::OK, threads::list(&state, &caller, query).await?)
}

#[utoipa::path(get, path = "/v1/threads/{id}", tag = "Threads", params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Thread and retained sequence range.", body = ThreadSummary),
        (status = 404, description = "Thread missing or not visible."),
    )
)]
pub(in crate::http) async fn read_thread(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
) -> Result<Response, HubError> {
    json(StatusCode::OK, threads::get(&state, &caller, id).await?)
}

#[utoipa::path(post, path = "/v1/threads/{id}/close", tag = "Threads", request_body = EmptyJson,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Thread closed.", body = ThreadSummary),
        (status = 403, description = "The caller cannot close this thread."),
        (status = 404, description = "Thread missing or not visible."),
        (status = 409, description = "The thread is already closed or the idempotency key conflicts."),
    )
)]
pub(in crate::http) async fn close(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    headers: HeaderMap,
    _: EmptyJson,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::close(&state, &caller, id, idempotency_key(&headers)?).await?,
    )
}

#[utoipa::path(post, path = "/v1/threads/{id}/posts", tag = "Threads", request_body = ThreadPostRequest,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 201, description = "Post committed to the thread.", body = ThreadPostReceipt),
        (status = 403, description = "The caller cannot post to this thread."),
        (status = 404, description = "The thread or referenced resource is missing or not visible."),
        (status = 409, description = "The thread is closed, the question already has an answer, or the request conflicts."),
    )
)]
pub(in crate::http) async fn post_message(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadPostRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::CREATED,
        threads::post(&state, &caller, id, idempotency_key(&headers)?, body).await?,
    )
}

#[utoipa::path(get, path = "/v1/threads/{id}/posts", tag = "Threads", params(("id" = ThreadId, Path), ThreadReadQuery),
    responses(
        (status = 200, description = "Retained posts in sequence order.", body = ThreadReadResponse),
        (status = 404, description = "Thread missing or not visible."),
        (status = 410, description = "The cursor predates the retained range."),
    )
)]
pub(in crate::http) async fn read_posts(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    StrictQuery(query): StrictQuery<ThreadReadQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::read(&state, &caller, id, query).await?,
    )
}

#[utoipa::path(put, path = "/v1/threads/{id}/pins/{pin_key}", tag = "Threads", request_body = ThreadPinSetRequest,
    params(("id" = ThreadId, Path), ("pin_key" = String, Path)),
    responses(
        (status = 200, description = "Pin committed with a new version.", body = ThreadPinView),
        (status = 403, description = "The caller cannot modify pins on this thread."),
        (status = 404, description = "The thread or referenced post is missing or not visible."),
        (status = 409, description = "The pin version or request conflicts."),
    )
)]
pub(in crate::http) async fn set_pin(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath((id, pin_key)): StrictPath<(ThreadId, String)>,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadPinSetRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::set_pin(
            &state,
            &caller,
            id,
            pin_key,
            idempotency_key(&headers)?,
            body,
        )
        .await?,
    )
}

#[utoipa::path(delete, path = "/v1/threads/{id}/pins/{pin_key}", tag = "Threads",
    params(("id" = ThreadId, Path), ("pin_key" = String, Path), ThreadPinDeleteQuery),
    responses(
        (status = 200, description = "Pin deleted or already absent.", body = ThreadPinDeleteResponse),
        (status = 403, description = "The caller cannot modify pins on this thread."),
        (status = 404, description = "Thread missing or not visible."),
        (status = 409, description = "The pin version or request conflicts."),
    )
)]
pub(in crate::http) async fn delete_pin(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath((id, pin_key)): StrictPath<(ThreadId, String)>,
    headers: HeaderMap,
    StrictQuery(query): StrictQuery<ThreadPinDeleteQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::delete_pin(
            &state,
            &caller,
            id,
            pin_key,
            idempotency_key(&headers)?,
            query,
        )
        .await?,
    )
}

#[utoipa::path(post, path = "/v1/threads/{id}/wait", tag = "Threads", request_body = ThreadWaitRequest,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Current context or a registered wait.", body = ThreadWaitResponse),
        (status = 403, description = "The caller cannot wait on this thread."),
        (status = 404, description = "Thread missing or not visible."),
        (status = 410, description = "The cursor predates the retained range."),
    )
)]
pub(in crate::http) async fn wait(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    StrictJson(body): StrictJson<ThreadWaitRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::wait(&state, &caller, id, body).await?,
    )
}

#[utoipa::path(get, path = "/v1/threads/{id}/members", tag = "Threads", params(("id" = ThreadId, Path), ThreadMembersQuery),
    responses(
        (status = 200, description = "Thread membership and delivery state.", body = ThreadMembersResponse),
        (status = 404, description = "Thread missing or not visible."),
    )
)]
pub(in crate::http) async fn list_members(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    StrictQuery(query): StrictQuery<ThreadMembersQuery>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::list_members(&state, &caller, id, query).await?,
    )
}

#[utoipa::path(post, path = "/v1/threads/{id}/members/join", tag = "Threads", request_body = ThreadMemberJoinRequest,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Member joined.", body = MemberView),
        (status = 403, description = "The caller cannot add a member."),
        (status = 404, description = "Thread missing or not visible."),
        (status = 409, description = "The member or request conflicts."),
    )
)]
pub(in crate::http) async fn join(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadMemberJoinRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::join(&state, &caller, id, idempotency_key(&headers)?, body).await?,
    )
}

#[utoipa::path(post, path = "/v1/threads/{id}/members/leave", tag = "Threads", request_body = ThreadMemberLeaveRequest,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Member left.", body = MemberView),
        (status = 403, description = "The caller cannot remove this member."),
        (status = 404, description = "The thread or member is missing or not visible."),
        (status = 409, description = "The thread still needs its last owner or the request conflicts."),
    )
)]
pub(in crate::http) async fn leave(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadMemberLeaveRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::leave(&state, &caller, id, idempotency_key(&headers)?, body).await?,
    )
}

#[utoipa::path(post, path = "/v1/threads/{id}/members/resume", tag = "Threads", request_body = ThreadMemberResumeRequest,
    params(("id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Uncertain delivery resolved at the supplied cursor.", body = MemberView),
        (status = 403, description = "The caller cannot resume this member."),
        (status = 404, description = "The thread or member is missing or not visible."),
        (status = 409, description = "The member is not paused or the request conflicts."),
        (status = 410, description = "The cursor predates the retained range."),
    )
)]
pub(in crate::http) async fn resume(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(id): StrictPath<ThreadId>,
    headers: HeaderMap,
    StrictJson(body): StrictJson<ThreadMemberResumeRequest>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        threads::resume(&state, &caller, id, idempotency_key(&headers)?, body).await?,
    )
}
