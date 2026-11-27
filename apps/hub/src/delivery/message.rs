use cooplet_protocol::{
    AggregateType, AnswerPayload, AttachmentRef, AuthorKind, BridgeCommand, DeliveryMode,
    DeliveryState, ErrorCode, EventKind, HubId, MessageId, NodeId, OperationId, ParticipantId,
    QuestionPayload, ServiceScope,
};
use cooplet_storage::EventNotice;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;

use crate::{
    AppState,
    auth::AuthenticatedCaller,
    blobs::{attach_to_node, preflight_node_attachments},
    error::HubError,
    storage::{
        events::{self, NewEvent, message_delivery_updated_data, question_answered_data},
        sql::{
            dialog_node_ids, nodes_audience, parse_col, parse_json, parse_opt_col, parse_opt_json,
            read_verified_tx_with_clock,
        },
    },
};

use super::{
    HubMessageView, MessageRequest, Target, active_node, check_node_access, participant_kind,
};

const MESSAGE_TTL_MS: i64 = 90 * 24 * 60 * 60 * 1_000;
pub(super) const MAX_MESSAGE_ATTACHMENTS: usize = 64;

impl MessageRequest {
    pub(super) fn normalize(&mut self) -> Result<(), HubError> {
        if self.question.is_some() && self.answer.is_some() {
            return Err(HubError::invalid(
                "A message cannot carry both a question and an answer.",
            ));
        }
        if self.attachments.len() > MAX_MESSAGE_ATTACHMENTS {
            return Err(HubError::invalid(
                "A message may carry at most 64 attachments.",
            ));
        }
        for attachment in &self.attachments {
            attachment
                .validate()
                .map_err(|error| HubError::invalid(error.to_string()))?;
        }
        if let Some(question) = &self.question {
            question
                .validate()
                .map_err(|error| HubError::invalid(error.to_string()))?;
        }
        if let Some(answer) = &self.answer {
            answer
                .validate()
                .map_err(|error| HubError::invalid(error.to_string()))?;
        }
        if self.body.trim().is_empty()
            && self.attachments.is_empty()
            && self.question.is_none()
            && self.answer.is_none()
        {
            return Err(HubError::invalid(
                "A message needs text, an attachment, a question, or an answer.",
            ));
        }
        if self.answer.is_some() && self.in_reply_to.is_none() {
            return Err(HubError::invalid(
                "An answer requires the message ID of its question.",
            ));
        }
        let delivery_mode = self.delivery_mode.unwrap_or(DeliveryMode::DirectNow);
        self.queue_ttl_ms = match delivery_mode {
            DeliveryMode::QueueUntilIdle => {
                let ttl = self.queue_ttl_ms.unwrap_or(300_000);
                if !(300_000..=3_600_000).contains(&ttl) {
                    return Err(HubError::invalid(
                        "Queue TTL must be between 300000 and 3600000 milliseconds.",
                    ));
                }
                Some(ttl)
            }
            DeliveryMode::DirectNow if self.queue_ttl_ms.is_some() => {
                return Err(HubError::invalid(
                    "Queue TTL applies to queue_until_idle delivery.",
                ));
            }
            DeliveryMode::DirectNow => None,
        };
        self.delivery_mode = Some(delivery_mode);
        Ok(())
    }

    pub(super) fn bridge_command(
        &self,
        message_id: &MessageId,
        sender_kind: AuthorKind,
        sender_name: &str,
        sender_node_id: Option<NodeId>,
    ) -> BridgeCommand {
        BridgeCommand::SendMessage {
            message_id: message_id.clone(),
            sender_kind,
            sender_name: sender_name.to_owned(),
            sender_node_id,
            recipient_id: self.target_participant_id.clone(),
            in_reply_to: self.in_reply_to.clone(),
            body: self.body.clone(),
            attachments: self.attachments.clone(),
            question: self.question.clone(),
            answer: self.answer.clone(),
        }
    }
}

pub(super) fn preflight(
    db: &Connection,
    caller: &AuthenticatedCaller,
    request: &MessageRequest,
    now: i64,
) -> Result<Target, HubError> {
    if let Some(answer) = &request.answer {
        return answer_target(db, caller, request, answer, now);
    }

    let parent = request
        .in_reply_to
        .as_ref()
        .map(|id| load(db, id, now))
        .transpose()?;
    if let Some(parent) = &parent {
        parent.authorize(caller)?;
        let sender = caller.is_party(
            parent.view.sender_node_id.as_ref(),
            &parent.view.sender_actor_id,
        );
        let recipient = caller.is_party(
            parent.view.target_node_id.as_ref(),
            &parent.view.target_participant_id,
        );
        let targets_sender = matches_target(
            request,
            parent.view.sender_node_id.as_ref(),
            &parent.view.sender_actor_id,
        );
        let targets_recipient = matches_target(
            request,
            parent.view.target_node_id.as_ref(),
            &parent.view.target_participant_id,
        );
        if !sender && !recipient && !caller.is_admin() {
            return Err(HubError::new(
                ErrorCode::Forbidden,
                "This caller is not a participant in the referenced dialogue.",
            ));
        }
        if !(sender && targets_recipient
            || recipient && targets_sender
            || caller.is_admin() && (targets_sender || targets_recipient))
        {
            return Err(HubError::invalid(
                "A reply must remain between the original dialogue participants.",
            ));
        }
    }
    match &request.target_node_id {
        Some(node_id) => {
            if !active_node(db, node_id)? {
                return Err(HubError::new(
                    ErrorCode::NotFound,
                    "The target node is unavailable.",
                ));
            }
            participant_kind(db, node_id, &request.target_participant_id)?;
        }
        None if parent.is_none() => {
            if caller.source_node_id().is_none() {
                return Err(HubError::invalid(
                    "A direct message requires a target node.",
                ));
            }
            let active: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM principals
                   WHERE participant_id = ?1 AND disabled_at_ms IS NULL)",
                [request.target_participant_id.as_str()],
                |row| row.get(0),
            )?;
            if !active {
                return Err(HubError::new(
                    ErrorCode::NotFound,
                    "The target participant is unavailable.",
                ));
            }
        }
        None => {}
    }
    if let Some(question) = &request.question
        && question.expires_at_ms.is_some_and(|expires| expires <= now)
    {
        return Err(HubError::invalid("Question expiry must be in the future."));
    }
    let target = message_target(
        caller,
        request,
        parent.as_ref(),
        request.target_node_id.clone(),
        now.saturating_add(MESSAGE_TTL_MS),
    );
    target.validate_attachments(db, caller, request, now)?;
    Ok(target)
}

fn matches_target(request: &MessageRequest, node: Option<&NodeId>, id: &ParticipantId) -> bool {
    request.target_node_id.as_ref() == node && &request.target_participant_id == id
}

fn message_target(
    caller: &AuthenticatedCaller,
    request: &MessageRequest,
    parent: Option<&MessageRecord>,
    notify: Option<NodeId>,
    expires_at_ms: i64,
) -> Target {
    let mut required_nodes = dialog_node_ids(
        caller.source_node_id().cloned(),
        request.target_node_id.clone(),
        parent.and_then(|p| p.view.sender_node_id.clone()),
        parent.and_then(|p| p.view.target_node_id.clone()),
    );
    if let Some(parent) = parent {
        required_nodes.extend(parent.node_ids.iter().cloned());
        required_nodes.sort_unstable();
        required_nodes.dedup();
    }
    let attachment_node = request
        .target_node_id
        .clone()
        .or_else(|| required_nodes.first().cloned());
    let attachment_peer = required_nodes
        .iter()
        .find(|node| Some(*node) != attachment_node.as_ref())
        .cloned();
    Target {
        notify,
        attachment_node,
        attachment_peer,
        required_nodes,
        message_expires_at_ms: expires_at_ms,
    }
}

impl Target {
    fn validate_attachments(
        &self,
        db: &Connection,
        caller: &AuthenticatedCaller,
        request: &MessageRequest,
        now: i64,
    ) -> Result<(), HubError> {
        if request.attachments.is_empty() {
            return Ok(());
        }
        let node = self
            .attachment_node
            .as_ref()
            .ok_or_else(no_attachment_scope)?;
        preflight_node_attachments(
            db,
            caller,
            node,
            self.attachment_peer.as_ref(),
            &request.attachments,
            now,
        )
    }
}

fn no_attachment_scope() -> HubError {
    HubError::new(ErrorCode::Forbidden, "The message has no attachment scope.")
}

fn answer_target(
    db: &Connection,
    caller: &AuthenticatedCaller,
    request: &MessageRequest,
    answer: &AnswerPayload,
    now: i64,
) -> Result<Target, HubError> {
    let parent_id = request
        .in_reply_to
        .as_ref()
        .ok_or_else(|| HubError::invalid("An answer requires the message ID of its question."))?;
    let parent = load(db, parent_id, now)?;
    parent.authorize(caller)?;
    let question =
        parent.view.question.as_ref().ok_or_else(|| {
            HubError::invalid("The referenced message does not contain a question.")
        })?;

    let addressed_to_caller = caller.is_party(
        parent.view.target_node_id.as_ref(),
        &parent.view.target_participant_id,
    );
    if !addressed_to_caller && !caller.is_admin() {
        return Err(HubError::new(
            ErrorCode::Forbidden,
            "This caller cannot answer the question.",
        ));
    }
    if request.target_node_id != parent.view.sender_node_id
        || request.target_participant_id != parent.view.sender_actor_id
    {
        return Err(HubError::invalid(
            "An answer must be addressed to the question's author.",
        ));
    }
    if question.expires_at_ms.is_some_and(|expires| expires <= now) {
        return Err(HubError::new(
            ErrorCode::QuestionExpired,
            "The question has expired.",
        ));
    }
    let mut existing_query = db.prepare_cached(
        "SELECT message_id FROM hub_messages WHERE in_reply_to = ?1 AND answer_json IS NOT NULL",
    )?;
    let existing = existing_query
        .query_row([parent_id.as_str()], |row| parse_col::<MessageId>(row, 0))
        .optional()?;
    if let Some(existing) = existing {
        return Err(HubError::new(
            ErrorCode::QuestionAlreadyAnswered,
            "The question already has an answer.",
        )
        .with_extension("existing_answer_id", existing.as_str()));
    }
    answer
        .validate_against(question)
        .map_err(|error| HubError::invalid(error.to_string()))?;
    let notify = match &parent.view.sender_node_id {
        Some(node_id) if active_node(db, node_id)? => Some(node_id.clone()),
        _ => None,
    };
    let target = message_target(
        caller,
        request,
        Some(&parent),
        notify,
        now.saturating_add(MESSAGE_TTL_MS).min(parent.expires_at_ms),
    );
    target.validate_attachments(db, caller, request, now)?;
    Ok(target)
}

pub(super) fn find_participant_kind(
    db: &Connection,
    node: &NodeId,
    id: &ParticipantId,
) -> rusqlite::Result<Option<String>> {
    let mut statement = db.prepare_cached(
        "SELECT kind FROM node_participants WHERE node_id = ?1 AND participant_id = ?2",
    )?;
    statement
        .query_row(params![node.as_str(), id.as_str()], |row| row.get(0))
        .optional()
}

pub(crate) async fn get_message(
    state: &AppState,
    caller: &AuthenticatedCaller,
    id: MessageId,
) -> Result<HubMessageView, HubError> {
    caller.require_scope(ServiceScope::Read)?;
    read_verified_tx_with_clock(
        &state.reads,
        caller,
        state.wall_clock.clone(),
        move |db, caller, now| {
            let message = load(db, &id, now)?;
            message.authorize(caller)?;
            Ok(message.view)
        },
    )
    .await
}

struct MessageRecord {
    view: HubMessageView,
    node_ids: Vec<NodeId>,
    expires_at_ms: i64,
}

impl MessageRecord {
    fn authorize(&self, caller: &AuthenticatedCaller) -> Result<(), HubError> {
        let party = caller.is_party(
            self.view.sender_node_id.as_ref(),
            &self.view.sender_actor_id,
        ) || caller.is_party(
            self.view.target_node_id.as_ref(),
            &self.view.target_participant_id,
        );
        if caller.source_node_id().is_some() {
            if party {
                return Ok(());
            }
        } else if !self.node_ids.is_empty() {
            return check_node_access(caller, &self.node_ids);
        } else if caller.is_global_reader() || party {
            return Ok(());
        }
        Err(HubError::new(
            ErrorCode::Forbidden,
            "The message is outside this caller's scope.",
        ))
    }
}

fn load(db: &Connection, id: &MessageId, now: i64) -> Result<MessageRecord, HubError> {
    let mut statement = db.prepare_cached(
        "SELECT m.message_id, m.sender_actor_id, m.sender_node_id, m.target_node_id,
                m.target_participant_id, m.in_reply_to, m.body, m.attachments_json,
                m.question_json, m.answer_json, m.delivery_mode, m.delivery_state,
                m.delivery_detail, m.operation_id, m.created_at_ms, m.delivered_at_ms,
                m.expires_at_ms, parent.sender_node_id, parent.target_node_id
         FROM hub_messages m
         LEFT JOIN hub_messages parent ON parent.message_id = m.in_reply_to
         WHERE m.message_id = ?1 AND m.expires_at_ms > ?2",
    )?;
    statement
        .query_row(params![id.as_str(), now], |row| {
            let sender_node_id = parse_opt_col::<NodeId>(row, 2)?;
            let target_node_id = parse_opt_col::<NodeId>(row, 3)?;
            let node_ids = dialog_node_ids(
                sender_node_id.clone(),
                target_node_id.clone(),
                parse_opt_col::<NodeId>(row, 17)?,
                parse_opt_col::<NodeId>(row, 18)?,
            );
            Ok(MessageRecord {
                view: HubMessageView {
                    message_id: parse_col(row, 0)?,
                    sender_actor_id: parse_col(row, 1)?,
                    sender_node_id,
                    target_node_id,
                    target_participant_id: parse_col(row, 4)?,
                    in_reply_to: parse_opt_col(row, 5)?,
                    body: row.get(6)?,
                    attachments: parse_json(row, 7)?,
                    question: parse_opt_json(row, 8)?,
                    answer: parse_opt_json(row, 9)?,
                    delivery_mode: parse_col(row, 10)?,
                    delivery_state: parse_col(row, 11)?,
                    delivery_detail: row.get(12)?,
                    operation_id: parse_opt_col(row, 13)?,
                    created_at_ms: row.get(14)?,
                    delivered_at_ms: row.get(15)?,
                },
                node_ids,
                expires_at_ms: row.get(16)?,
            })
        })
        .optional()?
        .ok_or_else(|| HubError::new(ErrorCode::NotFound, "The message does not exist."))
}

pub(super) struct NewMessage<'a> {
    pub(super) id: &'a MessageId,
    pub(super) operation_id: &'a OperationId,
    pub(super) sender: &'a ParticipantId,
    pub(super) body: &'a MessageRequest,
    pub(super) state: DeliveryState,
}

pub(super) fn insert(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    caller: &AuthenticatedCaller,
    target: &Target,
    message: NewMessage<'_>,
    now: i64,
) -> Result<Vec<EventNotice>, HubError> {
    let NewMessage {
        id,
        operation_id,
        sender,
        body,
        state,
    } = message;
    if !body.attachments.is_empty() {
        let node_id = target
            .attachment_node
            .as_ref()
            .ok_or_else(no_attachment_scope)?;
        attach_to_node(
            tx,
            caller,
            node_id,
            target.attachment_peer.as_ref(),
            &body.attachments,
            now,
        )?;
    }
    let question_json = body
        .question
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    let answer_json = body
        .answer
        .as_ref()
        .map(serde_json::to_string)
        .transpose()?;
    tx.prepare_cached(
        "INSERT INTO hub_messages (
             message_id, sender_actor_id, sender_node_id, target_node_id, target_participant_id, in_reply_to,
             body, attachments_json, question_json, answer_json, delivery_mode, delivery_state,
             operation_id, created_at_ms, delivered_at_ms, expires_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
    )?
    .execute(params![
        id.as_str(),
        sender.as_str(),
        caller.source_node_id().map(NodeId::as_str),
        body.target_node_id.as_ref().map(NodeId::as_str),
        body.target_participant_id.as_str(),
        body.in_reply_to.as_ref().map(MessageId::as_str),
        body.body,
        serde_json::to_string(&body.attachments)?,
        question_json,
        answer_json,
        body.delivery_mode
            .unwrap_or(DeliveryMode::DirectNow)
            .as_str(),
        state.as_str(),
        operation_id.as_str(),
        now,
        (state == DeliveryState::Delivered).then_some(now),
        target.message_expires_at_ms,
    ])?;

    let audience = nodes_audience(&target.required_nodes);
    let mut committed = Vec::with_capacity(2);
    if let (Some(question_id), Some(answer)) = (&body.in_reply_to, &body.answer) {
        committed.push(events::record(
            tx,
            hub_id,
            NewEvent {
                kind: EventKind::QuestionAnswered,
                aggregate_type: AggregateType::Message,
                aggregate_id: question_id.as_str(),
                actor_id: Some(caller.actor_id()),
                created_at_ms: now,
                data: question_answered_data(question_id, id, caller.actor_id(), answer, None),
                audience: audience.clone(),
            },
        )?);
    }

    let data = serde_json::to_value(MessageCreated {
        message_id: id,
        sender_id: sender,
        sender_node_id: caller.source_node_id(),
        recipient_id: &body.target_participant_id,
        target_node_id: body.target_node_id.as_ref(),
        in_reply_to: body.in_reply_to.as_ref(),
        body: &body.body,
        attachments: &body.attachments,
        question: body.question.as_ref(),
        answer: body.answer.as_ref(),
        delivery_state: state,
    })?;
    committed.push(events::record(
        tx,
        hub_id,
        NewEvent {
            kind: EventKind::MessageCreated,
            aggregate_type: AggregateType::Message,
            aggregate_id: id.as_str(),
            actor_id: Some(caller.actor_id()),
            created_at_ms: now,
            data,
            audience,
        },
    )?);
    if let Some(last) = committed.last() {
        events::advance_head(tx, last.seq)?;
    }
    Ok(committed)
}

#[derive(Serialize)]
struct MessageCreated<'a> {
    message_id: &'a MessageId,
    sender_id: &'a ParticipantId,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_node_id: Option<&'a NodeId>,
    recipient_id: &'a ParticipantId,
    target_node_id: Option<&'a NodeId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<&'a MessageId>,
    body: &'a str,
    attachments: &'a [AttachmentRef],
    #[serde(skip_serializing_if = "Option::is_none")]
    question: Option<&'a QuestionPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    answer: Option<&'a AnswerPayload>,
    delivery_state: DeliveryState,
}

pub(super) struct DeliveryUpdate<'a> {
    pub(super) message_id: &'a MessageId,
    pub(super) target_node_id: Option<&'a NodeId>,
    pub(super) required_nodes: &'a [NodeId],
    pub(super) actor_id: &'a str,
    pub(super) delivery_state: DeliveryState,
    pub(super) detail: Option<&'a str>,
    pub(super) now: i64,
}

pub(super) fn finalize(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    update: DeliveryUpdate<'_>,
) -> Result<Option<EventNotice>, HubError> {
    finalize_inner(tx, hub_id, update, None)
}

pub(super) fn finalize_reconciled(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    update: DeliveryUpdate<'_>,
    operation_id: &OperationId,
) -> Result<Option<EventNotice>, HubError> {
    finalize_inner(tx, hub_id, update, Some(operation_id))
}

fn finalize_inner(
    tx: &Transaction<'_>,
    hub_id: &HubId,
    update: DeliveryUpdate<'_>,
    reconciled_operation: Option<&OperationId>,
) -> Result<Option<EventNotice>, HubError> {
    let delivered_at_ms = (update.delivery_state == DeliveryState::Delivered).then_some(update.now);
    let changed = tx
        .prepare_cached(
            "UPDATE hub_messages SET delivery_state = ?2, delivery_detail = ?3, delivered_at_ms = ?4
             WHERE message_id = ?1 AND (
                 (?5 IS NULL AND delivery_state IN ('accepted', 'dispatched'))
                 OR (operation_id = ?5 AND delivery_state IN ('accepted', 'dispatched', 'unknown'))
             )",
        )?
        .execute(params![
            update.message_id.as_str(),
            update.delivery_state.as_str(),
            update.detail,
            delivered_at_ms,
            reconciled_operation.map(OperationId::as_str)
        ])?;
    if changed == 0 {
        return Ok(None);
    }
    let data = message_delivery_updated_data(
        update.message_id,
        update.target_node_id,
        update.delivery_state,
        update.detail,
        delivered_at_ms,
    );
    Ok(Some(events::append(
        tx,
        hub_id,
        NewEvent {
            kind: EventKind::MessageDeliveryUpdated,
            aggregate_type: AggregateType::Message,
            aggregate_id: update.message_id.as_str(),
            actor_id: Some(update.actor_id),
            created_at_ms: update.now,
            data,
            audience: nodes_audience(update.required_nodes),
        },
    )?))
}

pub(super) fn finalize_compact_unknown(
    tx: &Transaction<'_>,
    message_id: &MessageId,
) -> Result<(), HubError> {
    tx.prepare_cached(
        "UPDATE hub_messages SET delivery_state = 'unknown', delivery_detail = NULL
         WHERE message_id = ?1 AND delivery_state IN ('accepted', 'dispatched')",
    )?
    .execute([message_id.as_str()])?;
    Ok(())
}
