mod dto;
mod journal;
mod message;
mod outcome;
#[cfg(test)]
mod tests;

use std::{sync::Arc, time::Duration};

use cooplet_protocol::{
    ApprovalDecision, BridgeCommand, CommandReqPayload, DeliveryMode, DeliveryState, ErrorCode,
    MessageId, NodeId, OperationId, ParticipantId, RunId, ServiceScope, canonical_sha256,
    validate_idempotency_key,
};
use cooplet_storage::{StorageError, WriteFailure, WriteOutcome};
use http::StatusCode;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, reverify_caller_tx},
    bridge::{CommandPermit, ReserveError, SubmitError},
    error::HubError,
    now_ms,
};

pub(crate) use dto::{
    CancelWorkflowRequest, CommandReceipt, CreateAgentRequest, HubMessageView, MessageReceipt,
    MessageRequest, RespondApprovalRequest, RunWorkflowRequest, StopAgentRequest,
};
pub(crate) use message::get_message;

const ANSWER_KIND: &str = "answer_question";

pub(crate) async fn send_message(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    mut input: MessageRequest,
) -> Result<(StatusCode, MessageReceipt), HubError> {
    caller.require_scope(ServiceScope::Collaborate)?;
    let sender = caller.participant_id().cloned().ok_or_else(|| {
        HubError::new(
            ErrorCode::Forbidden,
            "Sending a message requires a participant identity.",
        )
    })?;
    let message_id = MessageId::generate()?;
    input.normalize(&message_id)?;
    let reply = execute(
        state,
        caller,
        key,
        Action::Message {
            message_id,
            sender,
            input: Box::new(input),
        },
    )
    .await?;
    match reply.body {
        Reply::Message(receipt) => Ok((reply.status, receipt)),
        Reply::Command(_) => Err(HubError::internal("message returned a command receipt")),
    }
}

pub(crate) async fn create_agent(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    request: CreateAgentRequest,
) -> Result<CommandReceipt, HubError> {
    caller.require_scope(ServiceScope::Manage)?;
    if request.client_kind.is_none() && request.template_id.is_none() {
        return Err(HubError::invalid(
            "Specify a client kind or an agent template.",
        ));
    }
    if request
        .overrides
        .as_ref()
        .is_some_and(|value| !value.is_object())
    {
        return Err(HubError::invalid("Agent overrides must be a JSON object."));
    }
    control(
        state,
        caller,
        key,
        request.target_node_id,
        BridgeCommand::CreateAgent {
            client_kind: request.client_kind,
            template_id: request.template_id,
            name: request.name,
            workspace_dir: request.workspace_dir,
            overrides: request.overrides,
        },
    )
    .await
}

pub(crate) async fn stop_agent(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    participant_id: ParticipantId,
    request: StopAgentRequest,
) -> Result<CommandReceipt, HubError> {
    control(
        state,
        caller,
        key,
        request.target_node_id,
        BridgeCommand::StopAgent {
            participant_id,
            cancel_turn_only: request.cancel_turn_only,
        },
    )
    .await
}

pub(crate) async fn respond_approval(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    participant_id: ParticipantId,
    request: RespondApprovalRequest,
) -> Result<CommandReceipt, HubError> {
    control(
        state,
        caller,
        key,
        request.target_node_id,
        BridgeCommand::RespondApproval {
            participant_id,
            decision: ApprovalDecision {
                approval_id: request.approval_id,
                action: request.action,
            },
        },
    )
    .await
}

pub(crate) async fn run_workflow(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    request: RunWorkflowRequest,
) -> Result<CommandReceipt, HubError> {
    control(
        state,
        caller,
        key,
        request.target_node_id,
        BridgeCommand::RunWorkflow {
            workflow_name: request.workflow_name,
            workspace_dir: request.workspace_dir,
            params: request.params,
        },
    )
    .await
}

pub(crate) async fn cancel_workflow(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    run_id: RunId,
    request: CancelWorkflowRequest,
) -> Result<CommandReceipt, HubError> {
    control(
        state,
        caller,
        key,
        request.target_node_id,
        BridgeCommand::CancelWorkflow { run_id },
    )
    .await
}

async fn control(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    node: NodeId,
    command: BridgeCommand,
) -> Result<CommandReceipt, HubError> {
    caller.require_scope(ServiceScope::Manage)?;
    command
        .validate()
        .map_err(|error| HubError::invalid(error.to_string()))?;
    let reply = execute(state, caller, key, Action::Control { node, command }).await?;
    match reply.body {
        Reply::Command(receipt) => Ok(receipt),
        Reply::Message(_) => Err(HubError::internal(
            "control command returned a message receipt",
        )),
    }
}

enum Action {
    Control {
        node: NodeId,
        command: BridgeCommand,
    },
    Message {
        message_id: MessageId,
        sender: ParticipantId,
        input: Box<MessageRequest>,
    },
}

#[derive(Clone, Copy)]
enum Kind {
    CreateAgent,
    StopAgent,
    RespondApproval,
    RunWorkflow,
    CancelWorkflow,
    Message,
    Answer,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreateAgent => "agent.create",
            Self::StopAgent => "agent.stop",
            Self::RespondApproval => "participant.approval",
            Self::RunWorkflow => "workflow.run",
            Self::CancelWorkflow => "workflow.cancel",
            Self::Message => "message.send",
            Self::Answer => ANSWER_KIND,
        }
    }

    fn default_status(self) -> StatusCode {
        match self {
            Self::CreateAgent | Self::RunWorkflow | Self::Message | Self::Answer => {
                StatusCode::CREATED
            }
            _ => StatusCode::OK,
        }
    }

    fn admission_budget_ms(self) -> u64 {
        match self {
            Self::CreateAgent | Self::RunWorkflow => 30_000,
            _ => 5_000,
        }
    }
}

impl Action {
    fn kind(&self) -> Result<Kind, HubError> {
        match self {
            Self::Message { input, .. } if input.answer.is_some() => Ok(Kind::Answer),
            Self::Message { .. } => Ok(Kind::Message),
            Self::Control { command, .. } => match command {
                BridgeCommand::CreateAgent { .. } => Ok(Kind::CreateAgent),
                BridgeCommand::StopAgent { .. } => Ok(Kind::StopAgent),
                BridgeCommand::RespondApproval { .. } => Ok(Kind::RespondApproval),
                BridgeCommand::RunWorkflow { .. } => Ok(Kind::RunWorkflow),
                BridgeCommand::CancelWorkflow { .. } => Ok(Kind::CancelWorkflow),
                _ => Err(HubError::internal("invalid control bridge command")),
            },
        }
    }

    fn target_node(&self) -> Option<&NodeId> {
        match self {
            Self::Control { node, .. } => Some(node),
            Self::Message { input, .. } => input.target_node_id.as_ref(),
        }
    }

    fn fingerprint(&self, kind: Kind) -> Result<String, HubError> {
        let kind = kind.as_str();
        match self {
            Self::Control { node, command } => {
                let path_target_id = match command {
                    BridgeCommand::StopAgent { participant_id, .. }
                    | BridgeCommand::RespondApproval { participant_id, .. } => {
                        Some(participant_id.as_str())
                    }
                    BridgeCommand::CancelWorkflow { run_id } => Some(run_id.as_str()),
                    _ => None,
                };
                Ok(canonical_sha256(&Fingerprint {
                    kind,
                    path_target_ids: path_target_id.as_slice(),
                    normalized_body: (node, command),
                })?)
            }
            Self::Message { input, .. } => Ok(canonical_sha256(&Fingerprint {
                kind,
                path_target_ids: &[],
                normalized_body: input,
            })?),
        }
    }
}

#[derive(Serialize)]
struct Fingerprint<'a, T> {
    kind: &'static str,
    path_target_ids: &'a [&'a str],
    normalized_body: T,
}

struct Request {
    id: OperationId,
    key: String,
    fingerprint: String,
    kind: Kind,
    action: Action,
    caller: AuthenticatedCaller,
}

impl Request {
    fn is_answer(&self) -> bool {
        matches!(self.kind, Kind::Answer)
    }

    fn message_id(&self) -> Option<&MessageId> {
        match &self.action {
            Action::Message { message_id, .. } => Some(message_id),
            Action::Control { .. } => None,
        }
    }

    fn in_reply_to(&self) -> Option<&MessageId> {
        match &self.action {
            Action::Message { input, .. } => input.in_reply_to.as_ref(),
            Action::Control { .. } => None,
        }
    }

    fn preflight(&self, db: &Connection, now: i64) -> Result<Target, HubError> {
        match &self.action {
            Action::Message { input, .. } => message::preflight(db, &self.caller, input, now),
            Action::Control { node, command } => {
                if !active_node(db, node)? {
                    return Err(HubError::new(
                        ErrorCode::NotFound,
                        "The target node is unavailable.",
                    ));
                }
                if let BridgeCommand::StopAgent { participant_id, .. }
                | BridgeCommand::RespondApproval { participant_id, .. } = command
                    && participant_kind(db, node, participant_id)? != "agent"
                {
                    return Err(HubError::invalid(
                        "The target participant must be an agent.",
                    ));
                }
                Ok(Target {
                    notify: Some(node.clone()),
                    required_nodes: vec![node.clone()],
                    attachment_node: None,
                    message_expires_at_ms: 0,
                })
            }
        }
    }

    fn payload(&self) -> CommandReqPayload {
        let (actor_id, command, delivery_mode, ttl_ms) = match &self.action {
            Action::Control { command, .. } => (
                self.caller.actor_id(),
                command.clone(),
                DeliveryMode::DirectNow,
                None,
            ),
            Action::Message {
                message_id,
                sender,
                input,
            } => (
                sender.as_str(),
                input.bridge_command(message_id),
                input.delivery_mode.unwrap_or(DeliveryMode::DirectNow),
                input.queue_ttl_ms,
            ),
        };
        CommandReqPayload {
            op_id: self.id.clone(),
            actor_id: actor_id.into(),
            command,
            delivery_mode,
            admission_budget_ms: self.kind.admission_budget_ms(),
            ttl_ms,
        }
    }

    fn message_reply(&self, state: DeliveryState) -> Result<Success, HubError> {
        let message_id = self
            .message_id()
            .ok_or_else(|| HubError::internal("expected a message ID"))?;
        let status = if state == DeliveryState::Queued && !self.is_answer() {
            StatusCode::ACCEPTED
        } else {
            StatusCode::CREATED
        };
        Ok(Success {
            status,
            body: Reply::Message(MessageReceipt {
                operation_id: self.id.clone(),
                message_id: message_id.clone(),
                in_reply_to: self.in_reply_to().cloned(),
                delivery_state: state,
            }),
        })
    }
}

struct Target {
    notify: Option<NodeId>,
    required_nodes: Vec<NodeId>,
    attachment_node: Option<NodeId>,
    message_expires_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Reply {
    Message(MessageReceipt),
    Command(CommandReceipt),
}

#[derive(Clone)]
struct Success {
    status: StatusCode,
    body: Reply,
}

pub(super) fn active_node(db: &Connection, id: &NodeId) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS (
            SELECT 1 FROM nodes WHERE node_id = ?1 AND status <> 'revoked' AND revoked_at_ms IS NULL
        )",
        [id.as_str()],
        |row| row.get(0),
    )
}

fn participant_kind(
    db: &Connection,
    node: &NodeId,
    id: &ParticipantId,
) -> Result<String, HubError> {
    message::find_participant_kind(db, node, id)?.ok_or_else(|| {
        HubError::new(
            ErrorCode::NotFound,
            "The node does not have this participant.",
        )
    })
}

fn check_node_access(caller: &AuthenticatedCaller, nodes: &[NodeId]) -> Result<(), HubError> {
    if nodes.iter().all(|node| caller.allows_node(node)) {
        Ok(())
    } else {
        Err(HubError::new(
            ErrorCode::Forbidden,
            "The target node is outside this caller's scope.",
        ))
    }
}

enum Probe {
    Replay(Success),
    New(Target),
}

async fn execute(
    state: &AppState,
    caller: &AuthenticatedCaller,
    key: &str,
    action: Action,
) -> Result<Success, HubError> {
    check_node_access(
        caller,
        action.target_node().map_or(&[], std::slice::from_ref),
    )?;
    validate_idempotency_key(key).map_err(|error| HubError::invalid(error.to_string()))?;
    let kind = action.kind()?;
    let request = Arc::new(Request {
        id: OperationId::generate()?,
        key: key.into(),
        fingerprint: action.fingerprint(kind)?,
        kind,
        action,
        caller: caller.clone(),
    });
    let target = match probe(state, request.clone()).await? {
        Probe::Replay(reply) => return Ok(reply),
        Probe::New(target) => target,
    };

    let reservation = match target.notify {
        Some(node) => match state.bridge.reserve(&node) {
            Ok(permit) => Some(Reservation { node, permit }),
            Err(_) if request.is_answer() => None,
            Err(error) => {
                // A concurrent caller may have already accepted this key.
                if let Some(replay) = replay_only(state, request).await? {
                    return Ok(replay);
                }
                return Err(reserve_error(error));
            }
        },
        None => None,
    };

    let state_for_task = state.clone();
    state
        .background
        .spawn(async move { deliver_owned(&state_for_task, request, reservation).await })?
        .await
        .map_err(HubError::internal)?
}

struct Reservation {
    node: NodeId,
    permit: Box<dyn CommandPermit>,
}

async fn probe(state: &AppState, request: Arc<Request>) -> Result<Probe, HubError> {
    state
        .reads
        .read(move |conn| {
            let tx = conn.unchecked_transaction()?;
            Ok(
                journal::replay(&tx, &request).and_then(|reply| match reply {
                    Some(reply) => Ok(Probe::Replay(reply)),
                    None => request.preflight(&tx, now_ms()).map(Probe::New),
                }),
            )
        })
        .await?
}

async fn replay_only(state: &AppState, request: Arc<Request>) -> Result<Option<Success>, HubError> {
    state
        .reads
        .read(move |conn| Ok(journal::replay(conn, &request)))
        .await?
}

fn reserve_error(error: ReserveError) -> HubError {
    match error {
        ReserveError::Offline => HubError::new(
            ErrorCode::BridgeUnavailable,
            "The node has no active bridge session.",
        ),
        ReserveError::RateLimited => HubError::new(
            ErrorCode::RateLimited,
            "The node has no free bridge command slots.",
        ),
    }
}

enum Accepted {
    Done(Success),
    Dispatch(Target),
}

async fn deliver_owned(
    state: &AppState,
    request: Arc<Request>,
    reservation: Option<Reservation>,
) -> Result<Success, HubError> {
    let tx_request = request.clone();
    let hub_id = state.meta.hub_id.clone();
    let reserved_node = reservation
        .as_ref()
        .map(|reservation| reservation.node.clone());
    let accepted = state
        .db
        .write(move |tx| {
            let now = now_ms();
            reverify_caller_tx(tx, &tx_request.caller, now)?;
            if let Some(reply) = journal::replay(tx, &tx_request)? {
                return Ok(WriteOutcome::new(Accepted::Done(reply)));
            }
            let target = tx_request.preflight(tx, now)?;
            let can_dispatch = target.notify.is_some() && target.notify == reserved_node;
            if !can_dispatch && !tx_request.is_answer() {
                return Err(reserve_error(ReserveError::Offline).into());
            }
            let local = if can_dispatch {
                None
            } else if tx_request.action.target_node().is_some() {
                Some(tx_request.message_reply(DeliveryState::Unavailable)?)
            } else {
                Some(tx_request.message_reply(DeliveryState::Delivered)?)
            };
            journal::insert(tx, &tx_request, &target, local.as_ref(), now)?;
            let events = match &tx_request.action {
                Action::Message {
                    message_id,
                    sender,
                    input,
                } => {
                    let delivery_state = match local.as_ref().map(|success| &success.body) {
                        Some(Reply::Message(receipt)) => receipt.delivery_state,
                        _ => DeliveryState::Accepted,
                    };
                    message::insert(
                        tx,
                        &hub_id,
                        &tx_request.caller,
                        &target,
                        message::NewMessage {
                            id: message_id,
                            operation_id: &tx_request.id,
                            sender,
                            body: input,
                            state: delivery_state,
                        },
                        now,
                    )?
                }
                Action::Control { .. } => Vec::new(),
            };
            Ok::<_, cooplet_storage::WriteError<HubError>>(WriteOutcome::with_events(
                match local {
                    Some(reply) => Accepted::Done(reply),
                    None => Accepted::Dispatch(target),
                },
                events,
            ))
        })
        .await;

    match accepted {
        Ok(Accepted::Done(reply)) => Ok(reply),
        Ok(Accepted::Dispatch(target)) => {
            let response = match reservation {
                Some(reservation) => reservation.permit.submit(request.payload()).await,
                None => Err(SubmitError::Unknown),
            };
            let terminal = outcome::classify(&request, response)?;
            finish(state, request, target.required_nodes, terminal).await
        }
        Err(WriteFailure::Storage(
            StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_),
        )) => {
            // A failed acknowledgement cannot authorize the first bridge send.
            drop(reservation);
            recover_acceptance(state, request).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn recover_acceptance(state: &AppState, request: Arc<Request>) -> Result<Success, HubError> {
    let lookup = request.clone();
    let observed = state
        .reads
        .read(move |conn| Ok(journal::observe_acceptance(conn, &lookup)))
        .await;
    match observed {
        Ok(Ok(journal::Observed::Terminal(reply))) => reply,
        Ok(Ok(journal::Observed::Accepted(required_nodes))) => {
            let terminal = outcome::unknown_terminal(&request)?;
            finish(state, request, required_nodes, terminal).await
        }
        _ => Err(outcome::unknown(&request)),
    }
}

async fn finish(
    state: &AppState,
    request: Arc<Request>,
    required_nodes: Vec<NodeId>,
    terminal: outcome::Terminal,
) -> Result<Success, HubError> {
    let terminal = Arc::new(terminal);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut delay = Duration::from_millis(10);
    loop {
        let hub_id = state.meta.hub_id.clone();
        let tx_request = request.clone();
        let tx_terminal = terminal.clone();
        let required_nodes = required_nodes.clone();
        let result = state
            .db
            .write(move |tx| {
                let now = now_ms();
                let receipt = journal::finish(tx, &tx_request, &required_nodes, &tx_terminal, now)?;
                if !receipt {
                    return Ok::<_, cooplet_storage::WriteError<HubError>>(WriteOutcome::new(
                        false,
                    ));
                }
                let event = match (tx_request.message_id(), tx_terminal.delivery) {
                    (Some(message_id), Some(delivery_state)) => message::finalize(
                        tx,
                        &hub_id,
                        message::DeliveryUpdate {
                            message_id,
                            target_node_id: tx_request.action.target_node(),
                            required_nodes: &required_nodes,
                            actor_id: tx_request.caller.actor_id(),
                            delivery_state,
                            detail: tx_terminal.delivery_detail.as_deref(),
                            now,
                        },
                    )?,
                    _ => None,
                };
                Ok(WriteOutcome::with_events(true, event.into_iter().collect()))
            })
            .await;
        match result {
            Ok(true) => return terminal.response(),
            Err(WriteFailure::Storage(StorageError::Overloaded))
                if tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_millis(200));
            }
            result => {
                tracing::warn!(operation_id = %request.id, ?result, "Could not confirm a delivery receipt");
                // Tx 1 completed. A duplicate or unacknowledged Tx 2 may have
                // recorded a terminal result, which a fresh read can prove.
                let check_request = request.clone();
                if let Ok(Ok(journal::Observed::Terminal(result))) = state
                    .reads
                    .read(move |conn| Ok(journal::observe_acceptance(conn, &check_request)))
                    .await
                {
                    return result;
                }
                return if request.is_answer() {
                    request.message_reply(DeliveryState::Unknown)
                } else {
                    Err(outcome::unknown(&request))
                };
            }
        }
    }
}
