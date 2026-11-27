use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use cooplet_protocol::{MessageId, ParticipantId, RunId};

use crate::{
    AppState,
    delivery::{
        self, CancelWorkflowRequest, CommandReceipt, CreateAgentRequest, HubMessageView,
        MessageReceipt, MessageRequest, RespondApprovalRequest, RunWorkflowRequest,
        StopAgentRequest,
    },
    error::HubError,
};

use super::{
    super::extract::{Caller, StrictJson, StrictPath, idempotency_key},
    json,
};

#[utoipa::path(
    post, path = "/v1/messages", tag = "Messages",
    description = "Send to an allowed node participant or answer an existing question. Requires collaborate scope and the caller's own participant identity.",
    request_body = MessageRequest,
    responses(
        (status = 201, description = "Message delivered or answer accepted by the Hub.", body = MessageReceipt),
        (status = 202, description = "Message queued by the destination node.", body = MessageReceipt),
        (status = 403, description = "The credential cannot collaborate in the destination or dialog scope."),
        (status = 404, description = "Node, participant, or referenced message not found."),
        (status = 409, description = "Hub identity, operation, question, or destination state conflict."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn send_message(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(request): StrictJson<MessageRequest>,
) -> Result<Response, HubError> {
    let (status, receipt) =
        delivery::send_message(&state, &caller, idempotency_key(&headers)?, request).await?;
    json(status, receipt)
}

#[utoipa::path(
    get, path = "/v1/messages/{message_id}", tag = "Messages",
    params(("message_id" = MessageId, Path)),
    responses(
        (status = 200, description = "Message and its delivery and question state.", body = HubMessageView),
        (status = 403, description = "The credential cannot read the dialog scope."),
        (status = 404, description = "Message missing or expired."),
    )
)]
pub(in crate::http) async fn get_message(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(message_id): StrictPath<MessageId>,
) -> Result<Response, HubError> {
    json(
        StatusCode::OK,
        delivery::get_message(&state, &caller, message_id).await?,
    )
}

#[utoipa::path(
    post, path = "/v1/agents", tag = "Agents",
    description = "Create an agent on the target node. Requires manage scope on that node.",
    request_body = CreateAgentRequest,
    responses(
        (status = 201, description = "Agent created.", body = CommandReceipt),
        (status = 403, description = "The credential lacks manage access to the target node."),
        (status = 404, description = "The target node or referenced resource does not exist."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn create_agent(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(request): StrictJson<CreateAgentRequest>,
) -> Result<Response, HubError> {
    let receipt =
        delivery::create_agent(&state, &caller, idempotency_key(&headers)?, request).await?;
    json(StatusCode::CREATED, receipt)
}

#[utoipa::path(
    post, path = "/v1/agents/{participant_id}/stop", tag = "Agents",
    description = "Stop an agent or cancel its current turn. Requires manage scope on the target node.",
    request_body = StopAgentRequest,
    params(("participant_id" = ParticipantId, Path)),
    responses(
        (status = 200, description = "Agent stop or turn cancellation confirmed.", body = CommandReceipt),
        (status = 403, description = "The credential lacks manage access to the target node."),
        (status = 404, description = "The target node or agent does not exist."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn stop_agent(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(participant_id): StrictPath<ParticipantId>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<StopAgentRequest>,
) -> Result<Response, HubError> {
    let receipt = delivery::stop_agent(
        &state,
        &caller,
        idempotency_key(&headers)?,
        participant_id,
        request,
    )
    .await?;
    json(StatusCode::OK, receipt)
}

#[utoipa::path(
    post, path = "/v1/participants/{participant_id}/approval", tag = "Agents",
    description = "Respond to a pending approval. Requires manage scope on the target node.",
    request_body = RespondApprovalRequest,
    params(("participant_id" = ParticipantId, Path)),
    responses(
        (status = 200, description = "Approval decision confirmed.", body = CommandReceipt),
        (status = 403, description = "The credential lacks manage access to the target node."),
        (status = 404, description = "The target node, participant, or approval does not exist."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn respond_approval(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(participant_id): StrictPath<ParticipantId>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<RespondApprovalRequest>,
) -> Result<Response, HubError> {
    let receipt = delivery::respond_approval(
        &state,
        &caller,
        idempotency_key(&headers)?,
        participant_id,
        request,
    )
    .await?;
    json(StatusCode::OK, receipt)
}

#[utoipa::path(
    post, path = "/v1/workflows/runs", tag = "Workflows",
    description = "Request a workflow run on a node. Requires manage scope on the target node.",
    request_body = RunWorkflowRequest,
    responses(
        (status = 201, description = "Workflow run created.", body = CommandReceipt),
        (status = 403, description = "The credential lacks manage access to the target node."),
        (status = 404, description = "The target node or workflow does not exist."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn run_workflow(
    State(state): State<AppState>,
    Caller(caller): Caller,
    headers: HeaderMap,
    StrictJson(request): StrictJson<RunWorkflowRequest>,
) -> Result<Response, HubError> {
    let receipt =
        delivery::run_workflow(&state, &caller, idempotency_key(&headers)?, request).await?;
    json(StatusCode::CREATED, receipt)
}

#[utoipa::path(
    post, path = "/v1/workflows/runs/{run_id}/cancel", tag = "Workflows",
    description = "Cancel the addressed workflow run. Requires manage scope on the target node.",
    request_body = CancelWorkflowRequest,
    params(("run_id" = RunId, Path)),
    responses(
        (status = 200, description = "Workflow cancellation confirmed.", body = CommandReceipt),
        (status = 403, description = "The credential lacks manage access to the target node."),
        (status = 404, description = "The target node or workflow run does not exist."),
        (status = 429, description = "The destination has no available command slots."),
        (status = 502, description = "The destination is unavailable."),
        (status = 507, description = "The Hub database capacity was reached."),
    )
)]
pub(in crate::http) async fn cancel_workflow(
    State(state): State<AppState>,
    Caller(caller): Caller,
    StrictPath(run_id): StrictPath<RunId>,
    headers: HeaderMap,
    StrictJson(request): StrictJson<CancelWorkflowRequest>,
) -> Result<Response, HubError> {
    let receipt =
        delivery::cancel_workflow(&state, &caller, idempotency_key(&headers)?, run_id, request)
            .await?;
    json(StatusCode::OK, receipt)
}
