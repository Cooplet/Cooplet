use std::convert::Infallible;

use axum::{
    Json as ResponseJson,
    body::Body,
    extract::State,
    response::{IntoResponse, Response},
};
use cooplet_protocol::{
    ApprovalDecision, ErrorCode, HealthResponse, MessageId, NodeInfoResponse, OperationId,
    ParticipantId, PrincipalKind, ProblemDetails, WhoamiResponse,
};
use cooplet_storage::WriteOutcome;
use http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    clients::{
        AgentResponse, ApprovalResponse, CreateAgentRequest, ParticipantPage, ParticipantView,
        StopAgentRequest, StopAgentResponse, TuiAttachPlan,
    },
    delivery::{MessagePage, MessageView, SendMessageRequest, SendMessageResponse},
    error::{NodeError, Result},
    now_ms,
};

use super::{
    NodeState,
    extract::{Caller, Json, OptionalJson, Path, Query, required_key},
    openapi_json,
    problem::forbidden,
};

#[derive(Default, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EmptyRequest {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParticipantQuery {
    after: Option<ParticipantId>,
    limit: Option<u16>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageQuery {
    after: Option<MessageId>,
    limit: Option<u16>,
}

#[derive(Serialize, Deserialize, ToSchema)]
pub struct StopNodeResponse {
    operation_id: OperationId,
    stopping: bool,
}

#[utoipa::path(get, path = "/v1/info", responses((status = 200, body = NodeInfoResponse)))]
pub async fn info(State(state): State<NodeState>) -> ResponseJson<NodeInfoResponse> {
    ResponseJson(state.info)
}

#[utoipa::path(get, path = "/v1/openapi.json", responses((status = 200, description = "OpenAPI 3.1 document", content_type = "application/json")))]
pub async fn openapi() -> Result<Response> {
    Ok((
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        openapi_json().map_err(NodeError::internal)?,
    )
        .into_response())
}

#[utoipa::path(
    get, path = "/v1/health", security(("bearer" = [])),
    responses(
        (status = 200, body = HealthResponse),
        (status = 401, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 503, description = "Node is shutting down", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub async fn health(
    State(state): State<NodeState>,
    Caller(_): Caller,
) -> Result<ResponseJson<HealthResponse>> {
    if state.tasks.is_closed() {
        return Err(NodeError::new(
            ErrorCode::Overloaded,
            "The Node is shutting down",
        ));
    }
    Ok(ResponseJson(HealthResponse {
        status: "ok".into(),
    }))
}

#[utoipa::path(get, path = "/v1/whoami", security(("bearer" = [])), responses((status = 200, body = WhoamiResponse), (status = 401, body = ProblemDetails, content_type = "application/problem+json")))]
pub async fn whoami(Caller(caller): Caller) -> ResponseJson<WhoamiResponse> {
    ResponseJson(caller.identity)
}

#[utoipa::path(
    get, path = "/v1/participants", security(("bearer" = [])),
    params(("after" = Option<ParticipantId>, Query), ("limit" = Option<u16>, Query, minimum = 1, maximum = 500)),
    responses((status = 200, body = ParticipantPage), (status = 403, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn participants(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Query(query): Query<ParticipantQuery>,
) -> Result<ResponseJson<ParticipantPage>> {
    let page = state
        .agents
        .participants(
            &caller,
            query.after,
            usize::from(query.limit.unwrap_or(100)),
        )
        .await?;
    Ok(ResponseJson(page))
}

#[utoipa::path(
    get, path = "/v1/participants/{id}", security(("bearer" = [])), params(("id" = ParticipantId, Path)),
    responses((status = 200, body = ParticipantView), (status = 404, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn participant(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<ParticipantId>,
) -> Result<ResponseJson<ParticipantView>> {
    Ok(ResponseJson(state.agents.participant(&caller, id).await?))
}

#[utoipa::path(
    post, path = "/v1/agents", security(("bearer" = [])), request_body = CreateAgentRequest,
    params(("Idempotency-Key" = String, Header)),
    responses((status = 201, body = AgentResponse), (status = 400, body = ProblemDetails, content_type = "application/problem+json"), (status = 409, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn create_agent(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    Json(input): Json<CreateAgentRequest>,
) -> Result<(StatusCode, ResponseJson<AgentResponse>)> {
    if !caller.can_manage() {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let service = state.agents.clone();
    let result = state
        .tasks
        .run(async move { service.create(caller.actor_id(), &key, input, None).await })
        .await?;
    Ok((StatusCode::CREATED, ResponseJson(result)))
}

#[utoipa::path(
    post, path = "/v1/agents/{id}/resume", security(("bearer" = [])), request_body = Option<EmptyRequest>,
    params(("id" = ParticipantId, Path), ("Idempotency-Key" = String, Header)),
    responses((status = 200, body = AgentResponse), (status = 400, body = ProblemDetails, content_type = "application/problem+json"), (status = 409, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn resume_agent(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<ParticipantId>,
    headers: HeaderMap,
    OptionalJson(_): OptionalJson<EmptyRequest>,
) -> Result<ResponseJson<AgentResponse>> {
    if !caller.can_manage() {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let service = state.agents.clone();
    let result = state
        .tasks
        .run(async move { service.resume(caller.actor_id(), &key, id).await })
        .await?;
    Ok(ResponseJson(result))
}

#[utoipa::path(
    post, path = "/v1/agents/{id}/stop", security(("bearer" = [])), request_body = Option<StopAgentRequest>,
    params(("id" = ParticipantId, Path), ("Idempotency-Key" = String, Header)),
    responses((status = 200, body = StopAgentResponse), (status = 400, body = ProblemDetails, content_type = "application/problem+json"), (status = 409, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn stop_agent(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<ParticipantId>,
    headers: HeaderMap,
    OptionalJson(input): OptionalJson<StopAgentRequest>,
) -> Result<ResponseJson<StopAgentResponse>> {
    if !caller.can_manage() {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let service = state.agents.clone();
    let result = state
        .tasks
        .run(async move { service.stop(caller.actor_id(), &key, id, input).await })
        .await?;
    Ok(ResponseJson(result))
}

#[utoipa::path(
    post, path = "/v1/participants/{id}/approval", security(("bearer" = [])), request_body = ApprovalDecision,
    params(("id" = ParticipantId, Path), ("Idempotency-Key" = String, Header)),
    responses((status = 200, body = ApprovalResponse), (status = 400, body = ProblemDetails, content_type = "application/problem+json"), (status = 409, body = ProblemDetails, content_type = "application/problem+json"), (status = 503, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn approval(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<ParticipantId>,
    headers: HeaderMap,
    Json(input): Json<ApprovalDecision>,
) -> Result<ResponseJson<ApprovalResponse>> {
    if !caller.can_manage() {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let service = state.agents.clone();
    let result = state
        .tasks
        .run(async move {
            service
                .respond_approval(caller.actor_id(), &key, id, input)
                .await
        })
        .await?;
    Ok(ResponseJson(result))
}

#[utoipa::path(
    get, path = "/v1/participants/{id}/attach-plan", security(("bearer" = [])), params(("id" = ParticipantId, Path)),
    responses((status = 200, body = TuiAttachPlan), (status = 403, body = ProblemDetails, content_type = "application/problem+json"), (status = 404, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn attach_plan(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<ParticipantId>,
) -> Result<ResponseJson<TuiAttachPlan>> {
    if !caller.can_manage() {
        return Err(forbidden());
    }
    Ok(ResponseJson(state.agents.attach_plan(id).await?))
}

#[utoipa::path(
    get, path = "/v1/messages", security(("bearer" = [])),
    params(("after" = Option<MessageId>, Query), ("limit" = Option<u16>, Query, minimum = 1, maximum = 500)),
    responses((status = 200, body = MessagePage), (status = 403, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn messages_list(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Query(query): Query<MessageQuery>,
) -> Result<ResponseJson<MessagePage>> {
    Ok(ResponseJson(
        state
            .delivery
            .list(&caller, query.after, query.limit)
            .await?,
    ))
}

#[utoipa::path(
    get, path = "/v1/messages/{id}", security(("bearer" = [])), params(("id" = MessageId, Path)),
    responses((status = 200, body = MessageView), (status = 404, body = ProblemDetails, content_type = "application/problem+json"))
)]
pub async fn message_get(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Path(id): Path<MessageId>,
) -> Result<ResponseJson<MessageView>> {
    Ok(ResponseJson(state.delivery.show(&caller, id).await?))
}

#[utoipa::path(
    post, path = "/v1/messages", security(("bearer" = [])), request_body = SendMessageRequest,
    params(("Idempotency-Key" = String, Header)),
    responses(
        (status = 200, description = "No available session", body = SendMessageResponse),
        (status = 201, description = "Message created", body = SendMessageResponse),
        (status = 202, description = "Queued until idle", body = SendMessageResponse),
        (status = 400, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 404, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 409, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 413, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 429, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 503, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 507, body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub async fn send_message(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    Json(input): Json<SendMessageRequest>,
) -> Result<(StatusCode, ResponseJson<SendMessageResponse>)> {
    if !caller.can_collaborate() {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let service = state.delivery.clone();
    let bridge = state.bridge.clone();
    let result = state
        .tasks
        .run(async move { service.send_routed(&bridge, caller, key, input).await })
        .await?;
    Ok((result.status(), ResponseJson(result)))
}

#[utoipa::path(
    post, path = "/v1/node/stop", security(("bearer" = [])), request_body = Option<EmptyRequest>,
    params(("Idempotency-Key" = String, Header)),
    responses(
        (status = 202, body = StopNodeResponse),
        (status = 400, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 401, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 403, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 409, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 413, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 503, body = ProblemDetails, content_type = "application/problem+json"),
        (status = 507, body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub async fn stop_node(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    OptionalJson(input): OptionalJson<EmptyRequest>,
) -> Result<Response> {
    if !caller.can_manage() || caller.kind() != PrincipalKind::Operator {
        return Err(forbidden());
    }
    let key = required_key(&headers)?;
    let tasks = state.tasks.clone();
    let (outcome, signal) = tasks
        .run(async move {
            let guard = state
                .operations
                .guard(caller.actor_id(), &key, "node.stop", &input)?;
            let outcome = state
                .db
                .write(move |tx| {
                    let operation = &guard.operation;
                    if let Some(previous) = operation.lookup(tx)? {
                        let reply =
                            serde_json::from_value(previous).map_err(NodeError::internal)?;
                        return Ok(WriteOutcome::new((reply, false)));
                    }
                    let now = now_ms().map_err(NodeError::from)?;
                    let reply = StopNodeResponse {
                        operation_id: operation.id.clone(),
                        stopping: true,
                    };
                    operation.reserve(tx, now, None)?;
                    operation.succeed(tx, now, &reply)?;
                    Ok(WriteOutcome::new((reply, true)))
                })
                .await?;
            let signal = outcome.1.then(|| {
                state.tasks.close();
                NotifyOnDrop(state.shutdown)
            });
            Ok((outcome.0, signal))
        })
        .await?;

    if let Some(signal) = signal {
        let bytes = serde_json::to_vec(&outcome).map_err(NodeError::internal)?;
        let frames =
            futures_util::stream::unfold((Some(bytes), signal), |(bytes, signal)| async move {
                bytes.map(|bytes| {
                    (
                        Ok::<_, Infallible>(axum::body::Bytes::from(bytes)),
                        (None, signal),
                    )
                })
            });
        return Ok((
            StatusCode::ACCEPTED,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Body::from_stream(frames),
        )
            .into_response());
    }
    Ok((StatusCode::ACCEPTED, ResponseJson(outcome)).into_response())
}

struct NotifyOnDrop(tokio::sync::watch::Sender<bool>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}
