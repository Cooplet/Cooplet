use cooplet_client::CoopletClient;
use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    BridgeCommand, BridgeFrame, CommandOutcome, CommandReqPayload, CommandResPayload,
    DeliveryState, ErrorCode, HubId, NodeId, OperationId, ParticipantId, ProblemDetails,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::Value;

use crate::{
    clients::{
        AgentResponse, ApprovalResponse, CreateAgentRequest, StopAgentRequest, StopAgentResponse,
        VerifiedAuthor,
    },
    delivery::{InboundHubMessage, SendMessageRequest, SendMessageResponse},
    error::{NodeError, Result},
    http::NodeState,
    operations::{ADMISSION_EXPIRED, DelegatedOperation, admission_expired, bound_problem},
};

#[cfg(test)]
#[path = "../../../tests/common/mock_adapter.rs"]
mod mock_adapter;
#[cfg(test)]
#[path = "commands_timing_tests.rs"]
mod timing_tests;

pub(in crate::bridge) async fn command(
    state: &NodeState,
    hub_id: &HubId,
    issued_node_id: &NodeId,
    hub: &CoopletClient,
    request: CommandReqPayload,
    admission: AdmissionGuard,
) -> CommandResPayload {
    let id = request.op_id.clone();
    match dispatch(state, hub_id, issued_node_id, hub, request, admission).await {
        Ok(result) => result,
        Err(error) => failure_response(id, error),
    }
}

async fn dispatch(
    state: &NodeState,
    hub_id: &HubId,
    issued_node_id: &NodeId,
    hub: &CoopletClient,
    request: CommandReqPayload,
    admission: AdmissionGuard,
) -> Result<CommandResPayload> {
    let mut delegated = DelegatedOperation {
        id: request.op_id,
        actor_id: request.actor_id,
        origin: bridge_origin(hub_id, issued_node_id),
        admission,
    };

    match request.command {
        BridgeCommand::SendMessage {
            message_id,
            sender_kind,
            sender_name,
            sender_node_id,
            recipient_id,
            in_reply_to,
            body,
            attachments,
            question,
            answer,
        } => {
            let sender = ParticipantId::parse(&delegated.actor_id).map_err(|_| {
                NodeError::new(
                    ErrorCode::InvalidRequest,
                    "Invalid delegated message sender",
                )
            })?;
            let principal = sender_node_id
                .as_ref()
                .map_or(hub_id.as_str(), NodeId::as_str);
            delegated.actor_id = format!("{principal}/{sender}");
            let sender_id = if sender_node_id.as_ref() == Some(issued_node_id) {
                sender.to_string()
            } else {
                delegated.actor_id.clone()
            };
            let message = InboundHubMessage {
                message_id,
                sender_id,
                author: VerifiedAuthor {
                    kind: sender_kind,
                    actor_id: delegated.actor_id.clone(),
                    name: sender_name,
                    source_node_id: sender_node_id,
                },
                input: SendMessageRequest {
                    to: recipient_id.to_string(),
                    body,
                    in_reply_to,
                    attachments,
                    question,
                    answer,
                    delivery_mode: request.delivery_mode,
                    queue_ttl_ms: request.ttl_ms,
                },
            };
            let sent = state
                .delivery
                .send_delegated(&delegated, hub, state.max_blob_bytes, message)
                .await?;
            Ok(message_response(sent))
        }
        BridgeCommand::CreateAgent {
            client_kind,
            template_id,
            name,
            workspace_dir,
            overrides,
        } => {
            let overrides = overrides
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| {
                    NodeError::new(ErrorCode::InvalidRequest, "Invalid agent overrides")
                })?;
            let request = CreateAgentRequest {
                template_id,
                client_kind,
                name,
                workspace_dir,
                overrides,
            };
            let created = state.agents.create_delegated(&delegated, request).await?;
            create_response(created)
        }
        BridgeCommand::StopAgent {
            participant_id,
            cancel_turn_only,
        } => {
            let stopped = state
                .agents
                .stop_delegated(
                    &delegated,
                    participant_id,
                    StopAgentRequest {
                        cancel_turn_only,
                        acknowledge_manual_cleanup: false,
                    },
                )
                .await?;
            succeeded(
                delegated.id,
                Some(stopped.participant_id.to_string()),
                &stopped,
            )
        }
        BridgeCommand::RespondApproval {
            participant_id,
            decision,
        } => {
            let response = state
                .agents
                .respond_approval_delegated(&delegated, participant_id, decision)
                .await?;
            succeeded(
                delegated.id,
                Some(response.participant_id.to_string()),
                &response,
            )
        }
        BridgeCommand::InspectAgent { participant_id } => {
            if delegated.admission.is_expired() {
                return Err(admission_expired());
            }
            let view = state.agents.inspect_delegated(participant_id).await?;
            succeeded(delegated.id, None, &view)
        }
        BridgeCommand::RunWorkflow { .. } | BridgeCommand::CancelWorkflow { .. } => {
            Ok(response(delegated.id, CommandOutcome::Unavailable))
        }
    }
}

fn bridge_origin(hub_id: &HubId, node_id: &NodeId) -> String {
    format!("{hub_id}/{node_id}")
}

fn response(op_id: OperationId, outcome: CommandOutcome) -> CommandResPayload {
    CommandResPayload {
        op_id,
        outcome,
        resource_id: None,
        result: None,
        error: None,
    }
}

fn succeeded(
    id: OperationId,
    resource_id: Option<String>,
    payload: &impl Serialize,
) -> Result<CommandResPayload> {
    Ok(CommandResPayload {
        resource_id,
        result: Some(serde_json::to_value(payload).map_err(NodeError::internal)?),
        ..response(id, CommandOutcome::Succeeded)
    })
}

fn create_response(created: AgentResponse) -> Result<CommandResPayload> {
    #[derive(Serialize)]
    struct CreatedAgent {
        operation_id: OperationId,
        participant_id: ParticipantId,
        name: String,
        client_kind: String,
    }
    succeeded(
        created.operation_id.clone(),
        Some(created.participant_id.to_string()),
        &CreatedAgent {
            operation_id: created.operation_id,
            participant_id: created.participant_id,
            name: created.name,
            client_kind: created.client_kind,
        },
    )
}

fn message_response(receipt: SendMessageResponse) -> CommandResPayload {
    let outcome = match receipt.delivery_state {
        DeliveryState::Delivered => CommandOutcome::Delivered,
        DeliveryState::Queued => CommandOutcome::Queued,
        DeliveryState::Busy => CommandOutcome::Busy,
        DeliveryState::AwaitingApproval => CommandOutcome::AwaitingApproval,
        DeliveryState::Unavailable => CommandOutcome::Unavailable,
        DeliveryState::Expired => CommandOutcome::Expired,
        DeliveryState::Accepted | DeliveryState::Dispatched | DeliveryState::Unknown => {
            CommandOutcome::Unknown
        }
    };
    response(receipt.operation_id, outcome)
}

fn failure_response(id: OperationId, error: NodeError) -> CommandResPayload {
    if error.0.extensions.get("reason").and_then(Value::as_str) == Some(ADMISSION_EXPIRED) {
        return response(id, CommandOutcome::Expired);
    }
    let outcome = match error.code() {
        ErrorCode::DeliveryUnknown | ErrorCode::OperationInProgress => CommandOutcome::Unknown,
        ErrorCode::BridgeUnavailable
        | ErrorCode::Overloaded
        | ErrorCode::StorageFull
        | ErrorCode::InternalError => CommandOutcome::Unavailable,
        ErrorCode::ParticipantBusy => CommandOutcome::Busy,
        ErrorCode::AwaitingApproval => CommandOutcome::AwaitingApproval,
        _ => CommandOutcome::Rejected,
    };
    let mut response = response(id, outcome);
    if matches!(
        outcome,
        CommandOutcome::Rejected | CommandOutcome::AwaitingApproval
    ) {
        let code = error.code();
        response.error = Some(
            bound_problem(*error.0)
                .unwrap_or_else(|_| ProblemDetails::new(code, "The Node rejected the command")),
        );
    }
    response
}

pub(in crate::bridge) fn receipts(
    conn: &Connection,
    hub_id: &HubId,
    issued_node_id: &NodeId,
    pending: &[OperationId],
    now: i64,
) -> rusqlite::Result<Vec<CommandResPayload>> {
    let origin = bridge_origin(hub_id, issued_node_id);
    let mut statement = conn.prepare_cached(
        "SELECT kind, state, result_json, error_json FROM operations
         WHERE operation_id = ?1 AND operation_key = ?1 AND bridge_origin = ?2
           AND expires_at_ms > ?3 AND state IN ('failed','succeeded')",
    )?;
    let mut receipts = Vec::new();
    for id in pending {
        let stored = statement
            .query_row(params![id.as_str(), origin, now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })
            .optional()?;
        let Some((kind, state, result, error)) = stored else {
            continue;
        };
        let receipt = match state.as_str() {
            "failed" => error
                .and_then(|value| serde_json::from_str::<ProblemDetails>(&value).ok())
                .map(|problem| failure_response(id.clone(), NodeError(Box::new(problem)))),
            "succeeded" => result.and_then(|serialized| success_receipt(&kind, &serialized).ok()),
            _ => None,
        };
        if let Some(receipt) = receipt
            && &receipt.op_id == id
            && receipt.outcome != CommandOutcome::Unknown
            && BridgeFrame::encode_json(1, &receipt).is_ok()
        {
            receipts.push(receipt);
        }
    }
    Ok(receipts)
}

fn success_receipt(kind: &str, json: &str) -> Result<CommandResPayload> {
    let invalid = || NodeError::internal("Unexpected delegated operation kind");
    match kind {
        "message.send" => {
            let receipt =
                serde_json::from_str::<SendMessageResponse>(json).map_err(NodeError::internal)?;
            Ok(message_response(receipt))
        }
        "agent.create" => {
            let receipt =
                serde_json::from_str::<AgentResponse>(json).map_err(NodeError::internal)?;
            create_response(receipt)
        }
        "agent.stop" => {
            let stopped =
                serde_json::from_str::<StopAgentResponse>(json).map_err(NodeError::internal)?;
            succeeded(
                stopped.operation_id.clone(),
                Some(stopped.participant_id.to_string()),
                &stopped,
            )
        }
        "approval.respond" => {
            let approval =
                serde_json::from_str::<ApprovalResponse>(json).map_err(NodeError::internal)?;
            succeeded(
                approval.operation_id.clone(),
                Some(approval.participant_id.to_string()),
                &approval,
            )
        }
        _ => Err(invalid()),
    }
}

#[cfg(test)]
mod tests {
    use cooplet_protocol::{DeliveryState, MessageId};

    use super::*;

    #[test]
    fn roster_receipts_only_prove_terminal_operations_from_this_binding()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let db = Connection::open_in_memory()?;
        db.execute_batch(include_str!("../../storage/001-node.sql"))?;
        let hub = HubId::generate()?;
        let node = NodeId::generate()?;
        let origin = bridge_origin(&hub, &node);
        let mut pending = Vec::new();
        for (state, delivery_state, owned, expired) in [
            ("succeeded", DeliveryState::Delivered, true, false),
            ("succeeded", DeliveryState::Busy, true, false),
            ("succeeded", DeliveryState::Unknown, true, false),
            ("unknown", DeliveryState::Delivered, true, false),
            ("succeeded", DeliveryState::Delivered, false, false),
            ("succeeded", DeliveryState::Delivered, true, true),
        ] {
            let id = OperationId::generate()?;
            let receipt = SendMessageResponse {
                operation_id: id.clone(),
                message_id: Some(MessageId::generate()?),
                delivery_state,
                in_reply_to: None,
            };
            db.execute(
                "INSERT INTO operations (
                    operation_id, actor_id, operation_key, kind, request_sha256, state,
                    result_json, bridge_origin, created_at_ms, updated_at_ms, expires_at_ms
                 ) VALUES (?1, 'remote', ?1, 'message.send', 'hash', ?2, ?3, ?4, 1, 1, ?5)",
                params![
                    id.as_str(),
                    state,
                    serde_json::to_string(&receipt)?,
                    owned.then_some(origin.as_str()),
                    if expired { 4 } else { 10 }
                ],
            )?;
            pending.push(id);
        }

        let receipts = receipts(&db, &hub, &node, &pending, 5)?;
        assert_eq!(receipts.len(), 2);
        assert_eq!(
            (&receipts[0].op_id, receipts[0].outcome),
            (&pending[0], CommandOutcome::Delivered)
        );
        assert_eq!(
            (&receipts[1].op_id, receipts[1].outcome),
            (&pending[1], CommandOutcome::Busy)
        );
        Ok(())
    }

    #[test]
    fn command_create_receipt_has_no_local_node_identity()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let agent = AgentResponse {
            operation_id: OperationId::generate()?,
            participant_id: ParticipantId::generate()?,
            name: "coordinator".into(),
            client_kind: "mock".into(),
            node_id: NodeId::generate()?,
        };
        let resource_id = agent.participant_id.to_string();
        let response = create_response(agent)?;
        let result = response.result.ok_or("missing result")?;
        assert_eq!(response.resource_id.as_deref(), Some(resource_id.as_str()));
        assert_eq!(result["participant_id"], resource_id);
        assert!(result.get("node_id").is_none());
        Ok(())
    }
}
