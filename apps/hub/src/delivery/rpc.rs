use cooplet_protocol::{
    NodeId, NodeRequestCaller, NodeRequestPayload, NodeResponsePayload, NodeRpcOperation,
    NodeRpcOutcome, NodeRpcResult,
};

use crate::{AppState, auth::attest_node_participant, error::HubError, storage::sql::read_tx};

pub(crate) async fn dispatch_node_request(
    state: &AppState,
    node_id: &NodeId,
    generation: u64,
    epoch: u64,
    request: NodeRequestPayload,
) -> NodeResponsePayload {
    let outcome = match dispatch(
        state,
        node_id,
        generation,
        epoch,
        request.caller,
        request.operation,
    )
    .await
    {
        Ok(result) => NodeRpcOutcome::Result { result },
        Err(error) => NodeRpcOutcome::Problem {
            problem: error.into_problem(),
        },
    };
    NodeResponsePayload {
        request_id: request.request_id,
        outcome,
    }
}

async fn dispatch(
    state: &AppState,
    node_id: &NodeId,
    generation: u64,
    epoch: u64,
    caller: NodeRequestCaller,
    operation: NodeRpcOperation,
) -> Result<NodeRpcResult, HubError> {
    let NodeRequestCaller::Participant { participant_id } = caller;
    let node_id = node_id.clone();
    let caller = read_tx(&state.reads, move |db| {
        attest_node_participant(db, &node_id, &participant_id, Some((generation, epoch)))
    })
    .await?;
    match operation {
        NodeRpcOperation::SendDirectMessage {
            operation_key,
            body,
        } => {
            let (_, body) =
                super::send_message(state, &caller, &operation_key, body.into()).await?;
            Ok(NodeRpcResult::SendDirectMessage { body: body.into() })
        }
        thread_op => crate::threads::dispatch_rpc(state, &caller, thread_op).await,
    }
}
