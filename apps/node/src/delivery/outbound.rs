use std::sync::Arc;

use cooplet_protocol::{
    DeliveryState, DirectMessageReceipt, DirectMessageRequest, ErrorCode, HubId, MessageId, NodeId,
    NodeRequestCaller, NodeRpcOperation, NodeRpcOutcome, NodeRpcResult, ParticipantId,
};
use cooplet_storage::WriteOutcome;
use rusqlite::{OptionalExtension, params};

use super::{
    DeliveryService, MessageView, SendMessageRequest, SendMessageResponse,
    model::{invalid, message_missing},
    queries::{HubProjection, sql_error, store_message},
};
use crate::{
    auth::Principal,
    bridge::NodeBridge,
    error::{NodeError, Result},
};

impl DeliveryService {
    /// Select the authoritative store before reserving a key or a local intake slot.
    pub(crate) async fn send_routed(
        self: &Arc<Self>,
        bridge: &NodeBridge,
        caller: Principal,
        key: String,
        mut input: SendMessageRequest,
    ) -> Result<SendMessageResponse> {
        if !caller.can_collaborate() {
            return Err(NodeError::new(
                ErrorCode::Forbidden,
                "Collaborate scope is required",
            ));
        }
        self.ensure_accepting()?;
        input.normalize()?;
        let qualified = input.to.contains('/');
        let parent = self
            .message_authority(&caller, input.in_reply_to.as_ref())
            .await?;
        let hub_parent = match parent {
            Some(Some(parent)) => Some(parent),
            Some(None) if qualified => return Err(invalid("A Hub reply requires a Hub message")),
            None if qualified => input.in_reply_to.clone(),
            _ => None,
        };
        if !qualified && hub_parent.is_none() {
            return self.send_in_place(caller, key, input).await;
        }
        let connection = bridge.connection()?;
        let participant_id = caller
            .identity
            .participant_id
            .filter(|id| id.as_str() == caller.identity.actor_id)
            .ok_or_else(|| {
                NodeError::new(
                    ErrorCode::Forbidden,
                    "A local participant identity is required",
                )
            })?;
        let (target_node_id, target_participant_id) =
            target(&input.to, connection.hub_id(), connection.node_id())?;
        let projection_to = if target_node_id.as_ref() == Some(connection.node_id()) {
            target_participant_id.to_string()
        } else {
            input.to.clone()
        };
        let operation = NodeRpcOperation::SendDirectMessage {
            operation_key: key,
            body: DirectMessageRequest {
                target_node_id,
                target_participant_id,
                body: input.body.clone(),
                delivery_mode: Some(input.delivery_mode),
                queue_ttl_ms: input.queue_ttl_ms,
                attachments: input.attachments.clone(),
                question: input.question.clone(),
                in_reply_to: hub_parent.clone(),
                answer: input.answer.clone(),
            },
        };
        let response = bridge
            .request(
                NodeRequestCaller::Participant {
                    participant_id: participant_id.clone(),
                },
                operation,
            )
            .await?;
        match response.outcome {
            NodeRpcOutcome::Result {
                result: NodeRpcResult::SendDirectMessage { body },
            } => {
                self.project_outbound(participant_id, projection_to, input, body)
                    .await
            }
            NodeRpcOutcome::Problem { problem } => Err(NodeError(Box::new(problem))),
            _ => Err(NodeError::new(
                ErrorCode::DeliveryUnknown,
                "The Hub returned an unexpected message receipt",
            )),
        }
    }

    async fn message_authority(
        &self,
        caller: &Principal,
        id: Option<&MessageId>,
    ) -> Result<Option<Option<MessageId>>> {
        let Some(id) = id.cloned() else {
            return Ok(None);
        };
        let actor = caller.actor_id().to_owned();
        let can_read_all = caller.can_read_all();
        let now = (self.clock)()?;
        self.reads
            .read(move |conn| {
                Ok(conn
                    .query_row(
                        "SELECT hub_message_id FROM messages
                 WHERE message_id = ?1 AND expires_at_ms > ?4
                   AND (?2 OR sender_id = ?3 OR recipient_id = ?3)",
                        params![id.as_str(), can_read_all, actor, now],
                        |row| {
                            row.get::<_, Option<String>>(0)?
                                .map(|id| id.parse().map_err(sql_error))
                                .transpose()
                        },
                    )
                    .optional()?)
            })
            .await
            .map_err(Into::into)
    }

    async fn project_outbound(
        &self,
        sender: ParticipantId,
        recipient: String,
        input: SendMessageRequest,
        receipt: DirectMessageReceipt,
    ) -> Result<SendMessageResponse> {
        let operation = receipt.operation_id.clone();
        let clock = self.clock.clone();
        let result = self
            .operations
            .finalize(move |tx| {
                let now = clock().map_err(NodeError::from)?;
                let parent = receipt
                    .in_reply_to
                    .as_ref()
                    .map(|parent| {
                        tx.query_row(
                            "SELECT message_id FROM messages
                     WHERE hub_message_id = ?1 AND expires_at_ms > ?2
                       AND (sender_id = ?3 OR recipient_id = ?3)",
                            params![parent.as_str(), now, sender.as_str()],
                            |row| row.get::<_, String>(0)?.parse().map_err(sql_error),
                        )
                        .optional()
                    })
                    .transpose()?
                    .flatten();
                let prior = tx
                    .query_row(
                        "SELECT message_id FROM messages
                 WHERE hub_message_id = ?1 AND (sender_id = ?2 OR recipient_id = ?2)",
                        params![receipt.message_id.as_str(), sender.as_str()],
                        |row| row.get::<_, String>(0)?.parse().map_err(sql_error),
                    )
                    .optional()?;
                let local_id = match prior {
                    Some(id) => {
                        return Ok(WriteOutcome::new(SendMessageResponse {
                            operation_id: receipt.operation_id.clone(),
                            message_id: Some(id),
                            in_reply_to: parent,
                            delivery_state: receipt.delivery_state,
                        }));
                    }
                    None => MessageId::generate().map_err(NodeError::from)?,
                };
                let view = MessageView {
                    message_id: local_id.clone(),
                    sender_id: sender.to_string(),
                    recipient_id: recipient.clone(),
                    body: input.body.clone(),
                    attachments: input.attachments.clone(),
                    delivery_mode: input.delivery_mode,
                    delivery_state: receipt.delivery_state,
                    created_at_ms: now,
                    in_reply_to: parent.clone(),
                    question: input.question.clone(),
                    answer: input.answer.clone(),
                    operation_id: None,
                    delivery_detail: None,
                    delivered_at_ms: (receipt.delivery_state == DeliveryState::Delivered)
                        .then_some(now),
                };
                let attachments = serde_json::to_string(&view.attachments).map_err(sql_error)?;
                let event = store_message(
                    tx,
                    &view,
                    &attachments,
                    Some(HubProjection {
                        message: &receipt.message_id,
                        parent: receipt.in_reply_to.as_ref(),
                    }),
                )?;
                Ok(WriteOutcome::with_events(
                    SendMessageResponse {
                        operation_id: receipt.operation_id.clone(),
                        message_id: Some(local_id),
                        in_reply_to: parent,
                        delivery_state: receipt.delivery_state,
                    },
                    vec![event],
                ))
            })
            .await;
        result.map_err(|error| {
            tracing::warn!(operation_id = %operation, %error, "A Hub message receipt could not be projected locally");
            NodeError::new(ErrorCode::DeliveryUnknown, "The Hub accepted the message but the local receipt could not be confirmed")
                .with_extension("operation_id", operation.as_str())
        })
    }
}

fn target(address: &str, hub: &HubId, local: &NodeId) -> Result<(Option<NodeId>, ParticipantId)> {
    let parse_participant = |id: &str| {
        ParticipantId::parse(id).map_err(|_| invalid("A Hub message requires a participant ID"))
    };
    let Some((prefix, participant)) = address.split_once('/') else {
        return Ok((Some(local.clone()), parse_participant(address)?));
    };
    let recipient = parse_participant(participant)?;
    if prefix == hub.as_str() {
        return Ok((None, recipient));
    }
    let destination = NodeId::parse(prefix)
        .map_err(|_| message_missing().with_extension("reason", "unknown_conversation"))?;
    Ok((Some(destination), recipient))
}
