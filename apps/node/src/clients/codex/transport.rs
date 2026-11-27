use std::{
    collections::BTreeMap,
    future::poll_fn,
    path::Path,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    task::Poll,
    time::Duration,
};

use cooplet_platform::{AdmissionGuard, ProcessIdentity};
use futures_util::{Sink, Stream};
use serde_json::{Value, json};
use tokio::{
    net::UnixStream,
    sync::{mpsc, oneshot, watch},
    time::{Instant, sleep, sleep_until, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Error as WireError, Message, protocol::WebSocketConfig},
};

use super::{
    approvals::ReplyError,
    events::{Observations, ProtocolResult, Snapshot, SubmittedInput, native_id},
};
use crate::clients::{
    AdapterEvent, ApprovalDecision, ApprovalWait, ParticipantRuntimeState as State,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const TURN_ACK_TIMEOUT: Duration = Duration::from_secs(12);
pub(super) const MAX_PENDING: usize = 16;
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

const QUEUED: u8 = 0;
const STARTED: u8 = 1;
const CANCELLED: u8 = 2;

type Wire = WebSocketStream<UnixStream>;

#[derive(Debug)]
pub(super) enum Failure {
    NotSent(&'static str),
    Uncertain(&'static str),
    Rejected(Rejection),
    Expired,
    Busy,
    Waiting(ApprovalWait),
    Stale,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    SchemaMismatch,
    NoIntake,
    MissingThread,
    Mcp,
    NoRollout,
    StaleTurn,
    UnsupportedMethod,
    Other,
}

impl Rejection {
    fn classify(error: &Value) -> Self {
        let code = error["code"].as_i64();
        let message = error["message"].as_str().unwrap_or_default();
        match (code, message) {
            (Some(-32603), "failed to submit turn input: ActiveTurnOutputSchemaMismatch") => {
                Self::SchemaMismatch
            }
            (Some(-32001), "Server overloaded; retry later.") => Self::NoIntake,
            (Some(-32600), "Server is draining; retry after reconnecting")
                if error["data"]["reason"] == "serverShuttingDown" =>
            {
                Self::NoIntake
            }
            (Some(-32600), msg) if msg.starts_with("thread not found: ") => Self::MissingThread,
            (Some(-32600), "no active turn to interrupt") => Self::StaleTurn,
            (Some(-32600), msg)
                if msg.starts_with("expected active turn id ") && msg.contains(" but found ") =>
            {
                Self::StaleTurn
            }
            (Some(-32601), _) => Self::UnsupportedMethod,
            (Some(-32603), msg) if msg.contains("required MCP servers failed to initialize") => {
                Self::Mcp
            }
            (Some(-32600 | -32603), msg)
                if msg.contains("rollout") || msg.contains("not materialized") =>
            {
                Self::NoRollout
            }
            _ => Self::Other,
        }
    }
}

pub(super) enum Operation {
    Rpc {
        method: &'static str,
        params: Value,
    },
    Initialized,
    Turn {
        operation_key: String,
        rendered: String,
        tool: Option<(&'static str, u64)>,
        schema: Option<Value>,
        admission: AdmissionGuard,
    },
    Approval {
        decision: ApprovalDecision,
        admission: Option<AdmissionGuard>,
    },
    Interrupt {
        turn_id: String,
        admission: AdmissionGuard,
    },
}

impl Operation {
    fn admission(&self) -> Option<&AdmissionGuard> {
        match self {
            Self::Turn { admission, .. } | Self::Interrupt { admission, .. } => Some(admission),
            Self::Approval { admission, .. } => admission.as_ref(),
            _ => None,
        }
    }
}

struct Command {
    operation: Operation,
    progress: Arc<AtomicU8>,
    result: oneshot::Sender<Result<Value, Failure>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Exchange {
    Plain,
    Open,
    Read,
    Turn,
    Interrupt,
}

struct Pending {
    exchange: Exchange,
    result: oneshot::Sender<Result<Value, Failure>>,
}

pub(super) struct Session {
    requests: mpsc::Sender<Command>,
    stop: watch::Sender<()>,
    snapshot: watch::Receiver<Snapshot>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop.send_replace(());
    }
}

impl Session {
    pub(super) fn start(
        wire: Wire,
        generation: u64,
        hint: String,
    ) -> (Self, mpsc::Receiver<AdapterEvent>) {
        let (requests, commands) = mpsc::channel(MAX_PENDING);
        let (event_tx, events) = mpsc::channel(128);
        let (stop, stopping) = watch::channel(());
        let (snapshots, snapshot) = watch::channel(Snapshot::default());
        let observations = Observations::new(generation, hint, event_tx, snapshots);
        tokio::spawn(drive(wire, commands, stopping, observations, generation));
        (
            Self {
                requests,
                stop,
                snapshot,
            },
            events,
        )
    }

    pub(super) fn snapshot(&self) -> Snapshot {
        self.snapshot.borrow().clone()
    }

    pub(super) async fn rpc(&self, method: &'static str, params: Value) -> Result<Value, Failure> {
        self.request(Operation::Rpc { method, params }).await
    }

    pub(super) async fn request(&self, operation: Operation) -> Result<Value, Failure> {
        let deadline = match operation {
            Operation::Turn { .. } => TURN_ACK_TIMEOUT,
            Operation::Interrupt { .. } => Duration::from_millis(1500),
            _ => RPC_TIMEOUT,
        };
        let admission = operation.admission().cloned();
        let progress = Arc::new(AtomicU8::new(QUEUED));
        let _ticket = Ticket(progress.clone());
        let (result, receiver) = oneshot::channel();
        self.requests
            .try_send(Command {
                operation,
                progress: progress.clone(),
                result,
            })
            .map_err(|_| Failure::NotSent("Codex transport is closed or busy"))?;
        match timeout(deadline, receiver).await {
            Ok(Ok(result)) => result,
            timed_out => {
                let started = progress.swap(CANCELLED, Ordering::AcqRel) == STARTED;
                match (started, timed_out.is_err()) {
                    (true, true) => Err(Failure::Uncertain("Codex acknowledgement timed out")),
                    (true, false) => Err(Failure::Uncertain(
                        "Codex disconnected after a possible write",
                    )),
                    (false, _) if admission.is_some_and(|guard| guard.is_expired()) => {
                        Err(Failure::Expired)
                    }
                    (false, _) => Err(Failure::NotSent(
                        "Codex request ended before the first write",
                    )),
                }
            }
        }
    }

    pub(super) async fn stop(&self) {
        self.stop.send_replace(());
        self.requests.closed().await;
    }
}

struct Ticket(Arc<AtomicU8>);

impl Drop for Ticket {
    fn drop(&mut self) {
        // Synchronize with the first write even if the caller cancels its future.
        let _ = self
            .0
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire);
    }
}

pub(super) async fn connect(socket: &Path, process: &ProcessIdentity) -> ProtocolResult<Wire> {
    timeout(CONNECT_TIMEOUT, async {
        let stream = loop {
            match UnixStream::connect(socket).await {
                Ok(stream) => break stream,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    if process.probe().ok() != Some(cooplet_platform::ProcessProbe::Running) {
                        return Err("codex_process_exited");
                    }
                    sleep(Duration::from_millis(20)).await;
                }
                Err(_) => return Err("codex_socket_connect_failed"),
            }
        };
        let peer = stream
            .peer_cred()
            .map_err(|_| "codex_peer_credentials_unavailable")?;
        let pid = peer
            .pid()
            .and_then(|pid| u32::try_from(pid).ok())
            .ok_or("codex_peer_pid_unavailable")?;
        if !process.owns_peer(pid, peer.uid()).unwrap_or(false) {
            return Err("codex_peer_identity_mismatch");
        }

        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MESSAGE_BYTES))
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_MESSAGE_BYTES * 2);
        let (wire, _) = client_async_with_config("ws://localhost/", stream, Some(config))
            .await
            .map_err(|_| "codex_websocket_handshake_failed")?;
        Ok(wire)
    })
    .await
    .map_err(|_| "codex_socket_connect_timeout")?
}

async fn drive(
    mut wire: Wire,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<()>,
    mut observed: Observations,
    generation: u64,
) {
    let mut pending = BTreeMap::new();
    let mut next_id = 1;
    let mut staged = None;
    let mut flush = None;
    let reason = loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break "codex_transport_stopped",
            _ = async { if let Some(deadline) = flush { sleep_until(deadline).await } }, if flush.is_some() => {
                break "codex_write_timeout";
            }
            _ = poll_fn(|cx| {
                if pending.values_mut().any(|entry: &mut Pending| entry.result.poll_closed(cx).is_ready()) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }), if !pending.is_empty() => {
                if pending.values().any(|entry| entry.exchange == Exchange::Turn && entry.result.is_closed()) {
                    break "codex_turn_acknowledgement_lost";
                }
                pending.retain(|_, entry| !entry.result.is_closed());
            }
            progress = poll_fn(|cx| {
                // Read ready notifications before admitting the next write. Use the
                // actual WebSocket sink: a split sink can defer the first possible
                // write to a later poll, after these observations become obsolete.
                let mut wire = Pin::new(&mut wire);
                if let Poll::Ready(frame) = wire.as_mut().poll_next(cx) {
                    return Poll::Ready(WireProgress::Read(frame));
                }
                if flush.is_some() {
                    return wire.poll_flush(cx).map(WireProgress::Flushed);
                }
                if staged.is_some() {
                    return wire.poll_ready(cx).map(WireProgress::Writable);
                }
                Poll::Pending
            }) => match progress {
                WireProgress::Read(frame) => match frame {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(reason) = receive(text.as_str(), &mut pending, &mut observed) {
                            break reason;
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        flush.get_or_insert_with(|| Instant::now() + WRITE_TIMEOUT);
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break "codex_event_stream_closed",
                    _ => break "codex_websocket_protocol_error",
                },
                WireProgress::Flushed(Ok(())) => flush = None,
                WireProgress::Writable(Ok(())) => {
                    if let Some(command) = staged.take() {
                        if let Err(reason) = send(command, &mut wire, &mut pending, &mut observed, &mut next_id) {
                            break reason;
                        }
                        flush = Some(Instant::now() + WRITE_TIMEOUT);
                    }
                }
                WireProgress::Writable(Err(_)) | WireProgress::Flushed(Err(_)) => break "codex_websocket_writer_failed",
            },
            command = commands.recv(), if staged.is_none() => match command {
                Some(command) => staged = Some(command),
                None => break "codex_transport_stopped",
            }
        }
    };
    observed.closed(reason);
    if reason != "codex_transport_stopped" {
        tracing::warn!(generation, reason, "Codex transport ended");
    }
    drop(observed);
    drop(pending);
    drop(staged);
    // Complete an intentional disconnect before Node terminates the group.
    if reason == "codex_transport_stopped" {
        let _ = timeout(Duration::from_millis(250), wire.close(None)).await;
    }
}

enum WireProgress {
    Read(Option<Result<Message, WireError>>),
    Flushed(Result<(), WireError>),
    Writable(Result<(), WireError>),
}

fn send(
    command: Command,
    wire: &mut Wire,
    pending: &mut BTreeMap<u64, Pending>,
    observed: &mut Observations,
    next_id: &mut u64,
) -> ProtocolResult<()> {
    let Command {
        operation,
        progress,
        result,
    } = command;
    if result.is_closed() {
        return Ok(());
    }
    let prepared = match prepare(&operation, observed, *next_id) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = result.send(Err(error));
            return Ok(());
        }
    };
    let named_output = prepared.packet.pointer("/params/toolOutput").is_some();
    let text = prepared.packet.to_string();
    if text.len() > MAX_MESSAGE_BYTES || pending.len() >= MAX_PENDING {
        let _ = result.send(Err(Failure::NotSent("Codex request capacity exceeded")));
        return Ok(());
    }
    if operation
        .admission()
        .is_some_and(AdmissionGuard::is_expired)
    {
        let _ = result.send(Err(Failure::Expired));
        return Ok(());
    }
    if progress
        .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(());
    }
    Pin::new(wire)
        .start_send(Message::Text(text.into()))
        .map_err(|_| "codex_websocket_write_failed")?;

    match operation {
        Operation::Turn {
            operation_key,
            schema,
            ..
        } => {
            let input = if named_output {
                SubmittedInput::NamedOutput
            } else {
                SubmittedInput::UserMessage(operation_key)
            };
            observed.begin_submit(input, schema.is_some());
        }
        Operation::Approval { .. } => {
            observed.approvals.consume_current();
            // A response is a JSON-RPC result, with no acknowledgement for the answering client.
            let _ = result.send(Err(Failure::Uncertain(
                "Codex does not confirm which approval response it applied",
            )));
            return observed.publish_state();
        }
        _ => {}
    }
    if let Some(exchange) = prepared.exchange {
        pending.insert(*next_id, Pending { exchange, result });
        *next_id += 1;
    } else {
        let _ = result.send(Ok(Value::Null));
    }
    Ok(())
}

struct Prepared {
    packet: Value,
    exchange: Option<Exchange>,
}

fn prepare(operation: &Operation, observed: &Observations, id: u64) -> Result<Prepared, Failure> {
    let rpc = |method: &str, params: Value, exchange| Prepared {
        packet: json!({"id": id, "method": method, "params": params}),
        exchange: Some(exchange),
    };
    match operation {
        Operation::Rpc { method, params } => {
            let exchange = match *method {
                "thread/start" | "thread/resume" => Exchange::Open,
                "thread/read" => Exchange::Read,
                _ => Exchange::Plain,
            };
            Ok(rpc(method, params.clone(), exchange))
        }
        Operation::Initialized => Ok(Prepared {
            packet: json!({"method": "initialized"}),
            exchange: None,
        }),
        Operation::Turn {
            operation_key,
            rendered,
            tool,
            schema,
            admission,
        } => {
            if admission.is_expired() {
                return Err(Failure::Expired);
            }
            match &observed.snapshot.state {
                State::AwaitingApproval(wait) => return Err(Failure::Waiting(wait.clone())),
                State::Working => return Err(Failure::Busy),
                State::Idle
                    if observed.submission_pending() || observed.snapshot.active_turn.is_some() =>
                {
                    return Err(Failure::Busy);
                }
                State::Idle => {}
                _ => return Err(Failure::NotSent("Codex thread is not ready")),
            }
            let mut params = json!({"threadId": observed.thread_id});
            if let Some((name, revision)) = tool
                && Some(*revision) == observed.snapshot.route_revision
            {
                params["input"] = json!([]);
                params["toolOutput"] = json!({"namespace": "cooplet", "name": name, "output": rendered, "retain": false});
            } else {
                params["input"] = json!([{"type": "text", "text": rendered}]);
                params["clientUserMessageId"] = json!(operation_key);
            }
            if let Some(schema) = schema {
                params["outputSchema"] = schema.clone();
            }
            Ok(rpc("turn/start", params, Exchange::Turn))
        }
        Operation::Approval {
            decision,
            admission,
        } => {
            if admission.as_ref().is_some_and(AdmissionGuard::is_expired) {
                return Err(Failure::Expired);
            }
            let callback = observed
                .approvals
                .current()
                .ok_or(match observed.snapshot.state {
                    State::AwaitingApproval(_) => Failure::Unsupported,
                    _ => Failure::Stale,
                })?;
            if !matches!(observed.snapshot.state, State::AwaitingApproval(_))
                || observed.snapshot.active_turn.as_deref() != Some(&callback.turn_id)
            {
                return Err(Failure::Stale);
            }
            let packet = callback
                .response(&decision.approval_id, decision.action)
                .map_err(|error| match error {
                    ReplyError::Stale => Failure::Stale,
                    ReplyError::Unsupported => Failure::Unsupported,
                })?;
            Ok(Prepared {
                packet,
                exchange: None,
            })
        }
        Operation::Interrupt { turn_id, .. } => {
            if observed.snapshot.active_turn.as_ref() != Some(turn_id)
                || !matches!(
                    observed.snapshot.state,
                    State::Working | State::AwaitingApproval(_)
                )
            {
                return Err(Failure::Stale);
            }
            Ok(rpc(
                "turn/interrupt",
                json!({"threadId": observed.thread_id, "turnId": turn_id}),
                Exchange::Interrupt,
            ))
        }
    }
}

fn receive(
    text: &str,
    pending: &mut BTreeMap<u64, Pending>,
    observed: &mut Observations,
) -> ProtocolResult<()> {
    let mut packet: Value = serde_json::from_str(text).map_err(|_| "invalid_codex_json")?;
    if let Some(method) = packet["method"].as_str() {
        let params = &packet["params"];
        return if packet.get("id").is_some() {
            let id = serde_json::from_value(packet["id"].clone())
                .map_err(|_| "invalid_server_request_id")?;
            observed.request(id, method, params)
        } else {
            observed.notify(method, params)
        };
    }
    let Some(id) = packet["id"].as_u64() else {
        return Err("invalid_codex_response_id");
    };
    let Some(entry) = pending.remove(&id) else {
        // A diagnostic request can expire before its otherwise harmless response arrives.
        return Ok(());
    };
    if packet["error"].is_object() && packet.get("result").is_none() {
        let _ = entry.result.send(Err(Failure::Rejected(Rejection::classify(
            &packet["error"],
        ))));
        if entry.exchange == Exchange::Turn {
            return observed.acknowledge(None);
        }
        return Ok(());
    }
    let Some(value) = packet.get_mut("result") else {
        return Err("invalid_codex_response");
    };
    match entry.exchange {
        Exchange::Open => observed.bind(value)?,
        Exchange::Read => observed.refresh(value)?,
        Exchange::Turn => {
            let turn_id = native_id(&value["turn"]["id"])?.to_owned();
            // Preserve the positive acknowledgement even if event delivery fails.
            let _ = entry.result.send(Ok(value.take()));
            return observed.acknowledge(Some(&turn_id));
        }
        Exchange::Plain | Exchange::Interrupt => {}
    }
    let _ = entry.result.send(Ok(value.take()));
    Ok(())
}
