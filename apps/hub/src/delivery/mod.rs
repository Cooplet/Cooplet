mod dto;
mod journal;
mod message;
mod outcome;
#[cfg(test)]
mod tests;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use cooplet_protocol::{
    ApprovalDecision, BridgeCommand, CommandReqPayload, DeliveryMode, DeliveryState, ErrorCode,
    MessageId, NodeId, OperationId, ParticipantId, RunId, ServiceScope, canonical_sha256,
    validate_idempotency_key,
};
use cooplet_storage::{StorageError, WriteFailure, WriteOutcome};
use http::StatusCode;
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};

use crate::{
    AppState,
    auth::{AuthenticatedCaller, reverify_caller_tx},
    bridge::{CommandPermit, ReserveError, SubmitError},
    error::HubError,
    now_ms,
    storage::sql::read_verified_tx,
};

pub(crate) use dto::{
    CancelWorkflowRequest, CommandReceipt, CreateAgentRequest, HubMessageView, MessageReceipt,
    MessageRequest, RespondApprovalRequest, RunWorkflowRequest, StopAgentRequest,
};
pub(crate) use message::get_message;

const ANSWER_KIND: &str = "answer_question";
const FINALIZATION_REPLY_TIMEOUT: Duration = Duration::from_secs(2);
const FINALIZATION_RETRY_BUDGET: Duration = Duration::from_secs(75);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(5);
static JITTER_COUNTER: AtomicU64 = AtomicU64::new(1);

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
    input.normalize()?;
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
    let blank =
        |value: &Option<String>| value.as_deref().is_some_and(|text| text.trim().is_empty());
    if blank(&request.client_kind) || blank(&request.template_id) {
        return Err(HubError::invalid("Agent selectors must not be blank."));
    }
    if request.client_kind.is_none() && request.template_id.is_none() {
        return Err(HubError::invalid(
            "Specify a client kind or an agent template.",
        ));
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
            overrides: request.overrides.map(serde_json::Value::Object),
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
            params: serde_json::Value::Object(request.params),
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
    let reply = execute(
        state,
        caller,
        key,
        Action::Control {
            node,
            command: Box::new(command),
        },
    )
    .await?;
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
        command: Box<BridgeCommand>,
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
            Self::Control { command, .. } => match command.as_ref() {
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
                let path_target_id = match command.as_ref() {
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

    fn delivery_mode(&self) -> Option<DeliveryMode> {
        match &self.action {
            Action::Message { input, .. } => {
                Some(input.delivery_mode.unwrap_or(DeliveryMode::DirectNow))
            }
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
                | BridgeCommand::RespondApproval { participant_id, .. } = command.as_ref()
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
                command.as_ref().clone(),
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

    fn unconfirmed_delivery(&self) -> Result<Success, HubError> {
        if self.is_answer() {
            self.message_reply(DeliveryState::Unknown)
        } else {
            Err(outcome::unknown(self))
        }
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
    let mut statement = db.prepare_cached(
        "SELECT EXISTS (
            SELECT 1 FROM nodes WHERE node_id = ?1 AND status <> 'revoked' AND revoked_at_ms IS NULL
        )",
    )?;
    statement.query_row([id.as_str()], |row| row.get(0))
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
    let (sender, response) = oneshot::channel();
    state.background.spawn(async move {
        let mut client = ClientReply(Some(sender));
        let result = deliver_owned(&state_for_task, request, reservation, &mut client).await;
        client.send(result);
    })?;
    response.await.map_err(HubError::internal)?
}

struct ClientReply(Option<oneshot::Sender<Result<Success, HubError>>>);

impl ClientReply {
    fn send(&mut self, result: Result<Success, HubError>) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(result);
        }
    }
}

struct Reservation {
    node: NodeId,
    permit: Box<dyn CommandPermit>,
}

async fn probe(state: &AppState, request: Arc<Request>) -> Result<Probe, HubError> {
    let caller = request.caller.clone();
    read_verified_tx(
        &state.reads,
        &caller,
        move |tx, _, now| match journal::replay(tx, &request)? {
            Some(reply) => Ok(Probe::Replay(reply)),
            None => request.preflight(tx, now).map(Probe::New),
        },
    )
    .await
}

async fn replay_only(state: &AppState, request: Arc<Request>) -> Result<Option<Success>, HubError> {
    let caller = request.caller.clone();
    read_verified_tx(&state.reads, &caller, move |tx, _, _| {
        journal::replay(tx, &request)
    })
    .await
}

fn authorized_replay(
    tx: &Transaction<'_>,
    request: &Request,
    now: i64,
) -> Result<Option<Success>, HubError> {
    reverify_caller_tx(tx, &request.caller, now)?;
    journal::replay(tx, request)
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
    client: &mut ClientReply,
) -> Result<Success, HubError> {
    if state.shutdown.has_changed().unwrap_or(true) {
        drop(reservation);
        return Err(HubError::new(
            ErrorCode::Overloaded,
            "The Hub is shutting down.",
        ));
    }
    let reserved_node = reservation
        .as_ref()
        .map(|reservation| reservation.node.clone());
    match accept_request(state, request.clone(), reserved_node).await {
        Ok(Accepted::Done(reply)) => Ok(reply),
        Ok(Accepted::Dispatch(target)) => {
            let response = match reservation {
                Some(reservation) => reservation.permit.submit(request.payload()).await,
                None => Err(SubmitError::Unknown),
            };
            let terminal = outcome::classify(&request, response)?;
            finish(state, request, target.required_nodes, terminal, client).await
        }
        Err(WriteFailure::Storage(
            StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_),
        )) => {
            // A failed acknowledgement cannot authorize the first bridge send.
            drop(reservation);
            recover_acceptance(state, request, client).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn accept_request(
    state: &AppState,
    request: Arc<Request>,
    reserved_node: Option<NodeId>,
) -> Result<Accepted, WriteFailure<HubError>> {
    let hub_id = state.meta.hub_id.clone();
    state
        .db
        .write(move |tx| {
            let now = now_ms();
            if let Some(reply) = authorized_replay(tx, &request, now)? {
                return Ok(WriteOutcome::new(Accepted::Done(reply)));
            }
            let target = request.preflight(tx, now)?;
            let can_dispatch = target.notify.is_some() && target.notify == reserved_node;
            if !can_dispatch && !request.is_answer() {
                return Err(reserve_error(ReserveError::Offline).into());
            }
            let local = if can_dispatch {
                None
            } else if request.action.target_node().is_some() {
                Some(request.message_reply(DeliveryState::Unavailable)?)
            } else {
                Some(request.message_reply(DeliveryState::Delivered)?)
            };
            journal::insert(tx, &request, &target, local.as_ref(), now)?;
            let events = match &request.action {
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
                        &request.caller,
                        &target,
                        message::NewMessage {
                            id: message_id,
                            operation_id: &request.id,
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
        .await
}

async fn recover_acceptance(
    state: &AppState,
    request: Arc<Request>,
    client: &mut ClientReply,
) -> Result<Success, HubError> {
    let lookup = request.clone();
    let observed = state
        .reads
        .read(move |conn| Ok(journal::observe_acceptance(conn, &lookup)))
        .await;
    match observed {
        Ok(Ok(journal::Observed::Terminal(reply))) => reply,
        Ok(Ok(journal::Observed::Accepted(required_nodes))) => {
            let terminal = outcome::unknown_terminal(&request)?;
            finish(state, request, required_nodes, terminal, client).await
        }
        _ => Err(outcome::unknown(&request)),
    }
}

async fn finish(
    state: &AppState,
    request: Arc<Request>,
    required_nodes: Vec<NodeId>,
    terminal: outcome::Terminal,
    client: &mut ClientReply,
) -> Result<Success, HubError> {
    let shutdown = state.shutdown.clone();
    let finalization = persist_terminal(
        state,
        &request,
        required_nodes,
        terminal,
        shutdown,
        FINALIZATION_RETRY_BUDGET,
    );
    tokio::pin!(finalization);
    tokio::select! {
        biased;
        result = &mut finalization => result,
        () = tokio::time::sleep(FINALIZATION_REPLY_TIMEOUT) => {
            client.send(request.unconfirmed_delivery());
            // Releasing the HTTP request does not release ownership of the
            // accepted operation. Retry only the SQLite receipt within budget.
            finalization.await
        }
    }
}

async fn persist_terminal(
    state: &AppState,
    request: &Arc<Request>,
    required_nodes: Vec<NodeId>,
    terminal: outcome::Terminal,
    mut shutdown: watch::Receiver<()>,
    retry_budget: Duration,
) -> Result<Success, HubError> {
    let terminal = Arc::new(terminal);
    let deadline = Instant::now() + retry_budget;
    let mut delay = Duration::from_millis(10);
    let mut storage_full_fallback = false;
    loop {
        // Always allow at least one write attempt even if shutdown arrived while
        // the bridge call was in flight: DbActor is still open during drain.
        let result = if storage_full_fallback {
            write_compact_unknown(state, request, &required_nodes).await
        } else {
            write_terminal(state, request, &required_nodes, &terminal).await
        };
        match result {
            Ok(true) => {
                return if storage_full_fallback {
                    request.unconfirmed_delivery()
                } else {
                    terminal.response()
                };
            }
            Err(error) if is_storage_full(&error) && !storage_full_fallback => {
                // Immediately attempt an in-place shrinking transition to `unknown`
                // so the receipt does not stay stranded in `accepted` (409).
                storage_full_fallback = true;
                continue;
            }
            Err(WriteFailure::Storage(StorageError::Overloaded)) => {}
            Err(error) if is_storage_full(&error) => {}
            result => {
                // Tx 1 completed. A duplicate or unacknowledged Tx 2 may have
                // recorded a terminal result, which a fresh read can prove.
                let ambiguous = matches!(
                    &result,
                    Err(WriteFailure::Storage(
                        StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)
                    ))
                );
                let check_request = request.clone();
                let observed = state
                    .reads
                    .read(move |conn| Ok(journal::observe_acceptance(conn, &check_request)))
                    .await;
                match observed {
                    Ok(Ok(journal::Observed::Terminal(result))) => return result,
                    Ok(Ok(journal::Observed::Accepted(_))) if ambiguous => {}
                    _ => {
                        tracing::warn!(operation_id = %request.id, ?result, "Could not confirm a delivery receipt");
                        return request.unconfirmed_delivery();
                    }
                }
            }
        }
        if shutdown.has_changed().unwrap_or(true) || Instant::now() >= deadline {
            tracing::warn!(
                operation_id = %request.id,
                "Stopped delivery receipt finalization after shutdown or retry budget"
            );
            return request.unconfirmed_delivery();
        }
        let wait = jittered_delay(delay).min(deadline.saturating_duration_since(Instant::now()));
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                tracing::warn!(
                    operation_id = %request.id,
                    "Stopped delivery receipt retries on Hub shutdown"
                );
                return request.unconfirmed_delivery();
            }
            () = tokio::time::sleep(wait) => {}
        }
        delay = (delay * 2).min(MAX_RETRY_BACKOFF);
    }
}

async fn write_terminal(
    state: &AppState,
    request: &Arc<Request>,
    required_nodes: &[NodeId],
    terminal: &Arc<outcome::Terminal>,
) -> Result<bool, WriteFailure<HubError>> {
    let hub_id = state.meta.hub_id.clone();
    let tx_request = request.clone();
    let tx_terminal = terminal.clone();
    let required_nodes = required_nodes.to_vec();
    state
        .db
        .write(move |tx| {
            let now = now_ms();
            let receipt = journal::finish(tx, &tx_request, &required_nodes, &tx_terminal, now)?;
            if !receipt {
                return Ok::<_, cooplet_storage::WriteError<HubError>>(WriteOutcome::new(false));
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
        .await
}

async fn write_compact_unknown(
    state: &AppState,
    request: &Arc<Request>,
    required_nodes: &[NodeId],
) -> Result<bool, WriteFailure<HubError>> {
    let tx_request = request.clone();
    let required_nodes = required_nodes.to_vec();
    state
        .db
        .write(move |tx| {
            let now = now_ms();
            let receipt = journal::finish_compact_unknown(tx, &tx_request, &required_nodes, now)?;
            if !receipt {
                return Ok::<_, cooplet_storage::WriteError<HubError>>(WriteOutcome::new(false));
            }
            if let Some(message_id) = tx_request.message_id() {
                message::finalize_compact_unknown(tx, message_id)?;
            }
            Ok::<_, cooplet_storage::WriteError<HubError>>(WriteOutcome::new(true))
        })
        .await
}

fn is_storage_full(error: &WriteFailure<HubError>) -> bool {
    matches!(error, WriteFailure::Storage(StorageError::StorageFull))
        || matches!(error, WriteFailure::App(err) if err.problem().code == ErrorCode::StorageFull)
}

fn jittered_delay(base: Duration) -> Duration {
    let max_jitter_ms = u64::try_from(base.as_millis() / 2).unwrap_or(0);
    if max_jitter_ms == 0 {
        return base;
    }
    let mut x = JITTER_COUNTER.fetch_add(1, Ordering::Relaxed)
        ^ u64::try_from(now_ms()).unwrap_or(0).rotate_left(13);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    base + Duration::from_millis(x % (max_jitter_ms + 1))
}
