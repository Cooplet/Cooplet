use axum::{Json as ResponseJson, extract::State};
use cooplet_protocol::{
    MemberView, NodeRpcOperation, NodeRpcResult, ProblemDetails, ThreadCloseRequest,
    ThreadCreateRequest, ThreadId, ThreadListQuery, ThreadListResponse, ThreadMemberJoinRequest,
    ThreadMemberLeaveRequest, ThreadMemberResumeRequest, ThreadMembersQuery, ThreadMembersResponse,
    ThreadPinDeleteQuery, ThreadPinDeleteResponse, ThreadPinSetRequest, ThreadPinView,
    ThreadPostReceipt, ThreadPostRequest, ThreadReadQuery, ThreadReadResponse, ThreadSummary,
    ThreadWaitRequest, ThreadWaitResponse,
};
use http::{HeaderMap, StatusCode};

use crate::error::Result;

use super::{
    Caller, Json, NodeState, Path, Query,
    bridge::{rpc, unexpected_result},
    extract::required_key,
};

type ThreadResponse<T> = Result<(StatusCode, ResponseJson<T>)>;

#[utoipa::path(
    post, path = "/v1/threads", security(("bearer" = [])),
    request_body = ThreadCreateRequest,
    params(("Idempotency-Key" = String, Header)),
    responses(
        (status = 201, description = "Created", body = ThreadSummary),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn create(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    Json(body): Json<ThreadCreateRequest>,
) -> ThreadResponse<ThreadSummary> {
    let operation = NodeRpcOperation::ThreadCreate {
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadCreate { body } => Ok((StatusCode::CREATED, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    get, path = "/v1/threads", security(("bearer" = [])),
    params(ThreadListQuery),
    responses(
        (status = 200, description = "Success", body = ThreadListResponse),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn list(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Query(query): Query<ThreadListQuery>,
) -> ThreadResponse<ThreadListResponse> {
    let operation = NodeRpcOperation::ThreadList { query };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadList { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(false)),
    }
}

#[utoipa::path(
    get, path = "/v1/threads/{thread_id}", security(("bearer" = [])),
    params(("thread_id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Success", body = ThreadSummary),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn get(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
) -> ThreadResponse<ThreadSummary> {
    let operation = NodeRpcOperation::ThreadGet { thread_id };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadGet { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(false)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/close", security(("bearer" = [])),
    request_body = ThreadCloseRequest,
    params(("thread_id" = ThreadId, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = ThreadSummary),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn close(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    headers: HeaderMap,
    Json(_): Json<ThreadCloseRequest>,
) -> ThreadResponse<ThreadSummary> {
    let operation = NodeRpcOperation::ThreadClose {
        thread_id,
        operation_key: required_key(&headers)?,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadClose { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/posts", security(("bearer" = [])),
    request_body = ThreadPostRequest,
    params(("thread_id" = ThreadId, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 201, description = "Created", body = ThreadPostReceipt),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn post(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    headers: HeaderMap,
    Json(body): Json<ThreadPostRequest>,
) -> ThreadResponse<ThreadPostReceipt> {
    let operation = NodeRpcOperation::ThreadPost {
        thread_id,
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadPost { body } => Ok((StatusCode::CREATED, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    get, path = "/v1/threads/{thread_id}/posts", security(("bearer" = [])),
    params(("thread_id" = ThreadId, Path), ThreadReadQuery),
    responses(
        (status = 200, description = "Success", body = ThreadReadResponse),
        (status = 410, description = "Thread history requires resynchronization", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn read(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    Query(query): Query<ThreadReadQuery>,
) -> ThreadResponse<ThreadReadResponse> {
    let operation = NodeRpcOperation::ThreadRead { thread_id, query };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadRead { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(false)),
    }
}

#[utoipa::path(
    put, path = "/v1/threads/{thread_id}/pins/{pin_key}", security(("bearer" = [])),
    request_body = ThreadPinSetRequest,
    params(("thread_id" = ThreadId, Path), ("pin_key" = String, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = ThreadPinView),
        (status = 409, description = "Pin version conflict", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn pin_set(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path((thread_id, pin_key)): Path<(ThreadId, String)>,
    headers: HeaderMap,
    Json(body): Json<ThreadPinSetRequest>,
) -> ThreadResponse<ThreadPinView> {
    let operation = NodeRpcOperation::ThreadPinSet {
        thread_id,
        pin_key,
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadPinSet { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    delete, path = "/v1/threads/{thread_id}/pins/{pin_key}", security(("bearer" = [])),
    params(("thread_id" = ThreadId, Path), ("pin_key" = String, Path), ThreadPinDeleteQuery, ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = ThreadPinDeleteResponse),
        (status = 409, description = "Pin version conflict", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn pin_delete(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path((thread_id, pin_key)): Path<(ThreadId, String)>,
    headers: HeaderMap,
    Query(query): Query<ThreadPinDeleteQuery>,
) -> ThreadResponse<ThreadPinDeleteResponse> {
    let operation = NodeRpcOperation::ThreadPinDelete {
        thread_id,
        pin_key,
        operation_key: required_key(&headers)?,
        query,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadPinDelete { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/wait", security(("bearer" = [])),
    request_body = ThreadWaitRequest,
    params(("thread_id" = ThreadId, Path)),
    responses(
        (status = 200, description = "Success", body = ThreadWaitResponse),
        (status = 410, description = "Thread history requires resynchronization", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn wait(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    Json(body): Json<ThreadWaitRequest>,
) -> ThreadResponse<ThreadWaitResponse> {
    let operation = NodeRpcOperation::ThreadWait { thread_id, body };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadWait { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    get, path = "/v1/threads/{thread_id}/members", security(("bearer" = [])),
    params(("thread_id" = ThreadId, Path), ThreadMembersQuery),
    responses(
        (status = 200, description = "Success", body = ThreadMembersResponse),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn members(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    Query(query): Query<ThreadMembersQuery>,
) -> ThreadResponse<ThreadMembersResponse> {
    let operation = NodeRpcOperation::ThreadMembersList { thread_id, query };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadMembersList { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(false)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/members/join", security(("bearer" = [])),
    request_body = ThreadMemberJoinRequest,
    params(("thread_id" = ThreadId, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = MemberView),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn join(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    headers: HeaderMap,
    Json(body): Json<ThreadMemberJoinRequest>,
) -> ThreadResponse<MemberView> {
    let operation = NodeRpcOperation::ThreadMemberJoin {
        thread_id,
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadMemberJoin { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/members/leave", security(("bearer" = [])),
    request_body = ThreadMemberLeaveRequest,
    params(("thread_id" = ThreadId, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = MemberView),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn leave(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    headers: HeaderMap,
    Json(body): Json<ThreadMemberLeaveRequest>,
) -> ThreadResponse<MemberView> {
    let operation = NodeRpcOperation::ThreadMemberLeave {
        thread_id,
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadMemberLeave { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}

#[utoipa::path(
    post, path = "/v1/threads/{thread_id}/members/resume", security(("bearer" = [])),
    request_body = ThreadMemberResumeRequest,
    params(("thread_id" = ThreadId, Path), ("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "Success", body = MemberView),
        (status = 410, description = "Thread history requires resynchronization", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, description = "Too many requests in flight", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Hub bridge unavailable", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn resume(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(thread_id): Path<ThreadId>,
    headers: HeaderMap,
    Json(body): Json<ThreadMemberResumeRequest>,
) -> ThreadResponse<MemberView> {
    let operation = NodeRpcOperation::ThreadMemberResume {
        thread_id,
        operation_key: required_key(&headers)?,
        body,
    };
    match rpc(state, caller, operation).await? {
        NodeRpcResult::ThreadMemberResume { body } => Ok((StatusCode::OK, ResponseJson(body))),
        _ => Err(unexpected_result(true)),
    }
}
