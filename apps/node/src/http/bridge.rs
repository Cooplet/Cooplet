use axum::{Json as ResponseJson, extract::State};
use cooplet_protocol::{
    ErrorCode, NodeRequestCaller, NodeRpcOperation, NodeRpcOutcome, NodeRpcResult, ParticipantId,
    PrincipalKind, ProblemDetails,
};
use http::StatusCode;

use crate::{
    auth::Principal,
    bridge::{EnrollRequest, EnrollResponse},
    error::{NodeError, Result},
};

use super::{Caller, Json, NodeState, problem::forbidden};

#[utoipa::path(
    post, path = "/v1/bridge/enroll", security(("bearer" = [])),
    request_body = EnrollRequest,
    responses(
        (status = 201, description = "Node enrolled", body = EnrollResponse),
        (status = 200, description = "Node already enrolled at this Hub", body = EnrollResponse),
        (status = 409, description = "Conflicting binding or Hub pin", body = ProblemDetails, content_type = "application/problem+json"),
        (status = 502, description = "Could not contact the Hub", body = ProblemDetails, content_type = "application/problem+json")
    )
)]
pub(super) async fn enroll(
    State(state): State<NodeState>,
    Caller(caller): Caller,
    Json(input): Json<EnrollRequest>,
) -> Result<(StatusCode, ResponseJson<EnrollResponse>)> {
    if caller.kind() != PrincipalKind::Operator || !caller.can_manage() {
        return Err(forbidden());
    }
    let (status, response) = state.bridge.enroll(&state.tasks, input).await?;
    Ok((status, ResponseJson(response)))
}

pub(super) fn bridge_participant(caller: &Principal) -> Result<ParticipantId> {
    if (caller.kind() != PrincipalKind::Operator || !caller.can_collaborate())
        && !caller.is_active_participant()
    {
        return Err(forbidden());
    }
    caller.identity.participant_id.clone().ok_or_else(forbidden)
}

pub(super) async fn rpc(
    state: NodeState,
    caller: Principal,
    operation: NodeRpcOperation,
) -> Result<NodeRpcResult> {
    let caller = NodeRequestCaller::Participant {
        participant_id: bridge_participant(&caller)?,
    };
    let response = state.bridge.request(caller, operation).await?;
    match response.outcome {
        NodeRpcOutcome::Result { result } => Ok(result),
        NodeRpcOutcome::Problem { problem } => Err(NodeError(Box::new(problem))),
    }
}

pub(super) fn unexpected_result(mutation: bool) -> NodeError {
    let code = if mutation {
        ErrorCode::DeliveryUnknown
    } else {
        ErrorCode::BridgeUnavailable
    };
    NodeError::new(
        code,
        "The Hub returned an unexpected result for the operation",
    )
}
