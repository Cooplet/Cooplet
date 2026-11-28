use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::{Future, pending},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use bytes::Bytes;
use cooplet_platform::{AdmissionGuard, MonoClock, MonoInstant};
use cooplet_protocol::{
    BridgeFrame, BridgePayload, CommandResPayload, ErrorCode, FrameKind, HelloAckPayload,
    NodeEventPayload, NodeRequestPayload, NodeResponsePayload, NodeRpcOutcome, OperationId,
    ParticipantId, ProblemDetails, ProtocolError, ROSTER_TIMEOUT, RosterReadyPayload,
    RosterSyncPayload, ThreadAckPayload, ThreadId,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tokio_tungstenite::tungstenite;

#[cfg(test)]
mod timing_tests;

use crate::{AppState, delivery, error::HubError, threads};

use super::{
    LiveBridge, projection,
    registry::{
        Admission, COMMAND_SLOTS, CloseReason, Outbound, Reply, SESSION_QUEUE, SessionKey,
        THREAD_SLOTS,
    },
    roster::Roster,
};

const PING_INTERVAL: Duration = Duration::from_secs(15);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const ACK_GRACE_MS: u64 = 35_000;
const REVERSE_SLOTS: usize = 16;
const EVENT_QUEUE: usize = 256;

pub(super) async fn run(
    socket: WebSocket,
    state: AppState,
    bridge: LiveBridge,
    key: SessionKey,
    mut admission: Admission,
    handshake: OwnedSemaphorePermit,
) {
    let (close_tx, close) = watch::channel(None);
    let (outbound, incoming) = mpsc::channel(SESSION_QUEUE);
    let (cancelled_tx, cancelled) = watch::channel(());
    let (sink, stream) = socket.split();
    let mut session = Socket {
        stream,
        sink: Some(sink),
        active_write: None,
        control_writes: VecDeque::new(),
        close,
        authority: admission.authority(),
        clock: bridge.clock().clone(),
        heartbeat: Heartbeat::new(bridge.clock().as_ref()),
        epoch: key.epoch,
    };
    let registered = admission.register(key.identity(), close_tx.clone(), outbound, cancelled_tx);
    if let Err(reason) = registered {
        session.send_close(reason).await;
        drop(handshake);
        return;
    }
    drop(handshake);

    let result = match session.synchronize(&state, &key).await {
        Ok(()) if bridge.ready(&key) => {
            threads::wake(&state, &key.node_id);
            session
                .run_ready(&state, &key, incoming, cancelled, &bridge, &close_tx)
                .await
        }
        Ok(()) => Err(Exit::Close(
            session.close.borrow().unwrap_or(CloseReason::Retry),
        )),
        Err(error) => Err(error),
    };

    bridge.stop_dispatch(&key);
    if let Err(Exit::Close(reason)) = result {
        session.send_close(reason).await;
    }
    drop(session);
    if let Err(error) = projection::mark_offline(&state, key.clone()).await {
        tracing::warn!(node_id = %key.node_id, %error, "Bridge disconnect could not update the node catalog");
    }
    drop(admission);
}

type SessionResult<T> = Result<T, Exit>;
type SinkOutcome = (SplitSink<WebSocket, Message>, Result<(), axum::Error>);

#[derive(Debug)]
enum Exit {
    Peer,
    Close(CloseReason),
}

impl From<ProtocolError> for Exit {
    fn from(error: ProtocolError) -> Self {
        let reason = if matches!(error, ProtocolError::BridgeFrameTooLarge { .. }) {
            CloseReason::TooLarge
        } else {
            CloseReason::InvalidFrame
        };
        Self::Close(reason)
    }
}

enum WriteOp {
    Send(Message),
    Flush,
}

struct ActiveWrite {
    future: Pin<Box<dyn Future<Output = SinkOutcome> + Send>>,
    deadline: MonoInstant,
    _authorized: Option<OwnedRwLockReadGuard<()>>,
}

struct Socket {
    stream: SplitStream<WebSocket>,
    sink: Option<SplitSink<WebSocket, Message>>,
    active_write: Option<ActiveWrite>,
    control_writes: VecDeque<WriteOp>,
    close: watch::Receiver<Option<CloseReason>>,
    authority: Arc<RwLock<()>>,
    clock: Arc<dyn MonoClock>,
    heartbeat: Heartbeat,
    epoch: u64,
}

struct Frame {
    frame: BridgeFrame,
    bytes: usize,
}

impl Socket {
    fn can_write_app(&self) -> bool {
        self.sink.is_some() && self.control_writes.is_empty()
    }

    fn heartbeat_poll_deadline(&self) -> Option<MonoInstant> {
        if self.heartbeat.pong.is_some() || self.sink.is_some() {
            Some(self.heartbeat.deadline())
        } else {
            None
        }
    }

    fn write_deadline(&self) -> Option<MonoInstant> {
        self.active_write.as_ref().map(|write| write.deadline)
    }

    fn transport_write_deadline(&self, phase_deadline: Option<MonoInstant>) -> MonoInstant {
        let mut bound = self.clock.deadline_after(WRITE_TIMEOUT);
        if let Some(pong) = self.heartbeat.pong {
            bound = self.earlier(bound, pong);
        }
        if let Some(phase) = phase_deadline {
            bound = self.earlier(bound, phase);
        }
        bound
    }

    fn start_write(
        &mut self,
        op: WriteOp,
        phase_deadline: Option<MonoInstant>,
        authorized: Option<OwnedRwLockReadGuard<()>>,
    ) {
        let Some(mut sink) = self.sink.take() else {
            return;
        };
        let deadline = self.transport_write_deadline(phase_deadline);
        let future = Box::pin(async move {
            let result = match op {
                WriteOp::Send(message) => sink.send(message).await,
                WriteOp::Flush => sink.flush().await,
            };
            (sink, result)
        });
        self.active_write = Some(ActiveWrite {
            future,
            deadline,
            _authorized: authorized,
        });
    }

    fn queue_control_write(&mut self, op: WriteOp, phase_deadline: Option<MonoInstant>) {
        if matches!(op, WriteOp::Flush)
            && self
                .control_writes
                .iter()
                .any(|queued| matches!(queued, WriteOp::Flush))
        {
            return;
        }
        if self.sink.is_some() {
            self.start_write(op, phase_deadline, None);
        } else {
            self.control_writes.push_back(op);
        }
    }

    fn flush_control_queue(&mut self, phase_deadline: Option<MonoInstant>) {
        if self.sink.is_some()
            && let Some(op) = self.control_writes.pop_front()
        {
            self.start_write(op, phase_deadline, None);
        }
    }

    fn finish_write(
        &mut self,
        outcome: SinkOutcome,
        phase_deadline: Option<MonoInstant>,
    ) -> SessionResult<()> {
        self.active_write = None;
        self.sink = Some(outcome.0);
        outcome.1.map_err(|_| Exit::Peer)?;
        self.flush_control_queue(phase_deadline);
        Ok(())
    }

    async fn drain_writes(&mut self, deadline: MonoInstant) -> SessionResult<()> {
        let clock = self.clock.clone();
        while self.active_write.is_some() || !self.control_writes.is_empty() {
            self.flush_control_queue(Some(deadline));
            let write_limit = self.write_deadline();
            let heartbeat_limit = self.heartbeat_poll_deadline();
            tokio::select! {
                biased;
                reason = closed(&mut self.close) => return Err(Exit::Close(reason)),
                () = until(clock.as_ref(), Some(deadline)) => return Err(Exit::Close(CloseReason::Retry)),
                () = until(clock.as_ref(), write_limit), if write_limit.is_some() => {
                    return Err(Exit::Close(CloseReason::Retry));
                }
                () = until(clock.as_ref(), heartbeat_limit), if heartbeat_limit.is_some() => {
                    self.heartbeat_tick(Some(deadline))?;
                }
                outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                    self.finish_write(outcome, Some(deadline))?;
                }
                incoming = self.stream.next() => {
                    if self.read(incoming, Some(deadline)).await?.is_some() {
                        return Err(Exit::Close(CloseReason::InvalidFrame));
                    }
                }
            }
        }
        Ok(())
    }

    async fn synchronize(&mut self, state: &AppState, key: &SessionKey) -> SessionResult<()> {
        let initial = self.clock.deadline_after(ROSTER_TIMEOUT);
        let pending = self
            .wait_controls(initial, delivery::pending_op_ids(state, &key.node_id))
            .await?
            .map_err(|error| projection_error(key, error))?;
        let hello = HelloAckPayload {
            hub_id: state.meta.hub_id.clone(),
            node_id: key.node_id.clone(),
            max_inflight_commands: COMMAND_SLOTS,
            max_inflight_threads: THREAD_SLOTS,
            pending_op_ids: pending.clone(),
        };
        self.send_handshake(encode(self.epoch, &hello)?, initial)
            .await?;

        let clock = self.clock.clone();
        let deadline = clock.deadline_after(ROSTER_TIMEOUT);
        let guard = AdmissionGuard::with_clock(ROSTER_TIMEOUT, clock.clone());
        let mut roster = Roster::default();
        loop {
            self.flush_control_queue(Some(deadline));
            let write_limit = self.write_deadline();
            let heartbeat_limit = self.heartbeat_poll_deadline();
            let packet = tokio::select! {
                biased;
                reason = closed(&mut self.close) => return Err(Exit::Close(reason)),
                () = until(clock.as_ref(), Some(deadline)) => return Err(Exit::Close(CloseReason::Retry)),
                () = until(clock.as_ref(), write_limit), if write_limit.is_some() => {
                    return Err(Exit::Close(CloseReason::Retry));
                }
                () = until(clock.as_ref(), heartbeat_limit), if heartbeat_limit.is_some() => {
                    self.heartbeat_tick(Some(deadline))?;
                    continue;
                }
                outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                    self.finish_write(outcome, Some(deadline))?;
                    continue;
                }
                packet = self.stream.next() => packet,
            };
            let Some(Frame { frame, bytes }) = self.read(packet, Some(deadline)).await? else {
                continue;
            };
            let chunk: RosterSyncPayload = frame.decode_payload()?;
            if roster
                .append(chunk, bytes)
                .map_err(|error| roster_error(key, error))?
            {
                break;
            }
        }

        let commit = roster.commit(state, key, pending, guard.clone(), self.close.clone());
        tokio::pin!(commit);
        // The actor owns a transaction once queued. Expire the guard and wait
        // for its result before attempting to clean up that same durable epoch.
        let commit_result = match self.wait_controls(deadline, commit.as_mut()).await {
            Ok(result) => result,
            Err(error) => {
                guard.expire();
                if let Exit::Close(reason) = error {
                    self.send_close(reason).await;
                }
                let _ = commit.await;
                return Err(Exit::Peer);
            }
        };
        commit_result.map_err(|error| roster_error(key, error))?;
        self.send_handshake(encode(self.epoch, &RosterReadyPayload {})?, deadline)
            .await
    }

    async fn wait_controls<T>(
        &mut self,
        deadline: MonoInstant,
        future: impl Future<Output = T>,
    ) -> SessionResult<T> {
        let clock = self.clock.clone();
        tokio::pin!(future);
        loop {
            self.flush_control_queue(Some(deadline));
            let write_limit = self.write_deadline();
            let heartbeat_limit = self.heartbeat_poll_deadline();
            let incoming = tokio::select! {
                biased;
                reason = closed(&mut self.close) => return Err(Exit::Close(reason)),
                () = until(clock.as_ref(), Some(deadline)) => return Err(Exit::Close(CloseReason::Retry)),
                () = until(clock.as_ref(), write_limit), if write_limit.is_some() => {
                    return Err(Exit::Close(CloseReason::Retry));
                }
                () = until(clock.as_ref(), heartbeat_limit), if heartbeat_limit.is_some() => {
                    self.heartbeat_tick(Some(deadline))?;
                    continue;
                }
                outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                    self.finish_write(outcome, Some(deadline))?;
                    continue;
                }
                result = &mut future => return Ok(result),
                incoming = self.stream.next() => incoming,
            };
            if self.read(incoming, Some(deadline)).await?.is_some() {
                return Err(Exit::Close(CloseReason::InvalidFrame));
            }
        }
    }

    async fn send_handshake(
        &mut self,
        message: Message,
        deadline: MonoInstant,
    ) -> SessionResult<()> {
        self.drain_writes(deadline).await?;
        let authority = self.authority.clone();
        let authorized = self.wait_controls(deadline, authority.read_owned()).await?;
        self.drain_writes(deadline).await?;
        if self.clock.is_expired(deadline) {
            return Err(Exit::Close(CloseReason::Retry));
        }
        self.start_write(WriteOp::Send(message), Some(deadline), Some(authorized));
        self.drain_writes(deadline).await
    }

    async fn run_ready(
        &mut self,
        state: &AppState,
        key: &SessionKey,
        incoming: mpsc::Receiver<Outbound>,
        cancelled: watch::Receiver<()>,
        bridge: &LiveBridge,
        close_tx: &watch::Sender<Option<CloseReason>>,
    ) -> SessionResult<()> {
        let (events, receive_events) = mpsc::channel(EVENT_QUEUE);
        let (applied_tx, applied) = watch::channel(0);
        let mut worker = tokio::spawn(projection::worker(
            state.clone(),
            key.clone(),
            receive_events,
            applied_tx,
            self.close.clone(),
        ));
        let mut ready = Ready {
            incoming,
            cancelled,
            events,
            applied,
            accepted_events: 0,
            rpc: JoinSet::new(),
            requests: HashSet::new(),
            responses: VecDeque::new(),
            pending: Pending::default(),
        };
        let (result, worker_finished) = loop {
            match self.step(state, key, &mut ready, &mut worker).await {
                Ok(()) => {}
                Err((result, finished)) => break (Err(result), finished),
            }
        };

        bridge.stop_dispatch(key);
        let stop_reason = match &result {
            Err(Exit::Close(reason)) => *reason,
            _ => CloseReason::Retry,
        };
        close_tx.send_if_modified(|reason| {
            if reason.is_some() {
                false
            } else {
                *reason = Some(stop_reason);
                true
            }
        });
        drop(ready.pending);
        drop(ready.incoming);
        drop(ready.events);
        let result = if let Err(Exit::Close(reason)) = result {
            self.send_close(reason).await;
            Err(Exit::Peer)
        } else {
            result
        };
        ready.rpc.shutdown().await;
        if !worker_finished {
            let _ = worker.await;
        }
        result
    }

    async fn step(
        &mut self,
        state: &AppState,
        key: &SessionKey,
        ready: &mut Ready,
        worker: &mut JoinHandle<Result<(), HubError>>,
    ) -> Result<(), (Exit, bool)> {
        if let Some(reason) = *self.close.borrow() {
            return Err((Exit::Close(reason), false));
        }
        self.flush_control_queue(None);
        let clock = self.clock.clone();
        let authority = self.authority.clone();
        let next_ack = ready.pending.deadline(clock.as_ref());
        let write_limit = self.write_deadline();
        let heartbeat_limit = self.heartbeat_poll_deadline();
        let can_write_app = self.can_write_app();
        let result = tokio::select! {
            reason = closed(&mut self.close) => Err(Exit::Close(reason)),
            worker = worker => {
                if let Ok(Err(error)) = worker {
                    tracing::debug!(node_id = %key.node_id, %error, "Bridge event projection ended");
                }
                return Err((Exit::Close(CloseReason::Retry), true));
            }
            () = until(clock.as_ref(), write_limit), if write_limit.is_some() => {
                Err(Exit::Close(CloseReason::Retry))
            }
            outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                self.finish_write(outcome, None)
            }
            () = until(clock.as_ref(), heartbeat_limit), if heartbeat_limit.is_some() => {
                self.heartbeat_tick(None)
            }
            () = until(clock.as_ref(), next_ack) => {
                ready.pending.expire(clock.as_ref());
                Ok(())
            }
            changed = ready.cancelled.changed() => match changed {
                Ok(()) => {
                    ready.pending.remove_cancelled_threads();
                    Ok(())
                }
                Err(_) => Err(Exit::Close(CloseReason::Retry)),
            },
            frame = self.stream.next() => self.receive(state, key, ready, frame).await,
            request = ready.incoming.recv(), if can_write_app => match request {
                Some(request) => self.forward(&mut ready.pending, request),
                None => Err(Exit::Close(CloseReason::Retry)),
            },
            result = ready.rpc.join_next(), if !ready.rpc.is_empty() => match result {
                Some(Ok(response)) => {
                    ready.responses.push_back(response);
                    Ok(())
                }
                _ => Err(Exit::Close(CloseReason::Retry)),
            },
            authorized = authority.read_owned(), if can_write_app && !ready.responses.is_empty() => {
                if let Some(response) = ready.responses.pop_front() {
                    ready.requests.remove(&response.request_id);
                    self.start_response(response, authorized)
                } else {
                    Ok(())
                }
            },
        };
        result.map_err(|error| (error, false))
    }

    async fn receive(
        &mut self,
        state: &AppState,
        key: &SessionKey,
        ready: &mut Ready,
        packet: Option<Result<Message, axum::Error>>,
    ) -> SessionResult<()> {
        let Some(Frame { frame, .. }) = self.read(packet, None).await? else {
            return Ok(());
        };
        match frame.kind {
            FrameKind::CommandRes => {
                let ack: CommandResPayload = frame.decode_payload()?;
                if let Some(pending) = ready.pending.commands.remove(&ack.op_id)
                    && !self.clock.is_expired(pending.deadline)
                {
                    pending.reply.complete(ack);
                }
            }
            FrameKind::ThreadAck => {
                let ack: ThreadAckPayload = frame.decode_payload()?;
                if let Some(pending) = ready.pending.threads.get(&ack.delivery_id)
                    && !pending.correlation.matches(&ack)
                {
                    return Err(Exit::Close(CloseReason::InvalidFrame));
                }
                if let Some(pending) = ready.pending.threads.remove(&ack.delivery_id)
                    && !self.clock.is_expired(pending.deadline)
                {
                    pending.reply.complete(ack);
                }
            }
            FrameKind::NodeEvent => {
                ready.accepted_events = ready.accepted_events.saturating_add(1);
                let event: NodeEventPayload = frame.decode_payload()?;
                ready
                    .events
                    .try_send((ready.accepted_events, event))
                    .map_err(|_| Exit::Close(CloseReason::Retry))?;
            }
            FrameKind::NodeRequest => {
                let req: NodeRequestPayload = frame.decode_payload()?;
                if ready.requests.len() >= REVERSE_SLOTS {
                    if ready.responses.len() < REVERSE_SLOTS * 2 {
                        ready.responses.push_back(problem_response(
                            req.request_id,
                            ErrorCode::RateLimited,
                            "The node has no free reverse request slots.",
                        ));
                    }
                } else if !ready.requests.insert(req.request_id.clone()) {
                    return Err(Exit::Close(CloseReason::InvalidFrame));
                } else {
                    let state = state.clone();
                    let key = key.clone();
                    let mut applied = ready.applied.clone();
                    let required = ready.accepted_events;
                    ready.rpc.spawn(async move {
                        if applied
                            .wait_for(|applied| *applied >= required)
                            .await
                            .is_err()
                        {
                            return problem_response(
                                req.request_id,
                                ErrorCode::BridgeUnavailable,
                                "The bridge closed.",
                            );
                        }
                        delivery::dispatch_node_request(
                            &state,
                            &key.node_id,
                            key.generation,
                            key.epoch,
                            req,
                        )
                        .await
                    });
                }
            }
            _ => return Err(Exit::Close(CloseReason::InvalidFrame)),
        }
        Ok(())
    }

    async fn read(
        &mut self,
        packet: Option<Result<Message, axum::Error>>,
        phase_deadline: Option<MonoInstant>,
    ) -> SessionResult<Option<Frame>> {
        let message = match packet {
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                return if matches!(
                    error.into_inner().downcast_ref::<tungstenite::Error>(),
                    Some(tungstenite::Error::Capacity(_))
                ) {
                    Err(Exit::Close(CloseReason::TooLarge))
                } else {
                    Err(Exit::Peer)
                };
            }
            None => return Err(Exit::Peer),
        };
        match message {
            Message::Text(text) => {
                let bytes = text.as_bytes();
                let frame = BridgeFrame::from_json(bytes)?;
                if frame.epoch != self.epoch {
                    return Err(Exit::Close(CloseReason::EpochMismatch));
                }
                Ok(Some(Frame {
                    frame,
                    bytes: bytes.len(),
                }))
            }
            Message::Binary(_) => Err(Exit::Close(CloseReason::InvalidDataType)),
            Message::Close(_) => {
                self.active_write = None;
                if let Some(mut sink) = self.sink.take() {
                    let _ = time::timeout(CLOSE_TIMEOUT, sink.flush()).await;
                }
                Err(Exit::Peer)
            }
            Message::Ping(_) => {
                // Tungstenite queues the RFC Pong automatically. Flush it without blocking reads.
                self.queue_control_write(WriteOp::Flush, phase_deadline);
                Ok(None)
            }
            Message::Pong(payload) => {
                self.heartbeat.pong(self.clock.as_ref(), &payload)?;
                Ok(None)
            }
        }
    }

    fn heartbeat_tick(&mut self, phase_deadline: Option<MonoInstant>) -> SessionResult<()> {
        if self.heartbeat.pong.is_some() {
            return Err(Exit::Close(CloseReason::Retry));
        }
        let payload = self.heartbeat.ping(self.clock.as_ref());
        self.queue_control_write(WriteOp::Send(Message::Ping(payload)), phase_deadline);
        Ok(())
    }

    fn earlier(&self, left: MonoInstant, right: MonoInstant) -> MonoInstant {
        if self.clock.remaining_ms(left).unwrap_or(0) <= self.clock.remaining_ms(right).unwrap_or(0)
        {
            left
        } else {
            right
        }
    }

    async fn send_close(&mut self, reason: CloseReason) {
        self.active_write = None;
        let Some(mut sink) = self.sink.take() else {
            return;
        };
        let reason = self.close.borrow().unwrap_or(reason);
        let (code, reason) = reason.frame();
        let _ = time::timeout(
            CLOSE_TIMEOUT,
            sink.send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.into(),
            }))),
        )
        .await;
    }

    fn start_response(
        &mut self,
        response: NodeResponsePayload,
        authorized: OwnedRwLockReadGuard<()>,
    ) -> SessionResult<()> {
        let encoded = match encode(self.epoch, &response) {
            Ok(encoded) => encoded,
            Err(_) => encode(
                self.epoch,
                &problem_response(
                    response.request_id,
                    ErrorCode::PayloadTooLarge,
                    "The bridge response exceeds its byte limit.",
                ),
            )?,
        };
        self.start_write(WriteOp::Send(encoded), None, Some(authorized));
        Ok(())
    }

    fn forward(&mut self, pending: &mut Pending, request: Outbound) -> SessionResult<()> {
        match request {
            Outbound::Command(request) => {
                let Ok(authorized) = self.authority.clone().try_read_owned() else {
                    return Ok(());
                };
                let mut request = *request;
                let id = request.payload.op_id.clone();
                if pending.commands.contains_key(&id) || request.reply.is_cancelled() {
                    return Ok(());
                }
                let Some(granted) = self.clock.remaining_ms(request.deadline) else {
                    return Ok(());
                };
                request.payload.admission_budget_ms = granted;
                let message = match encode(self.epoch, &request.payload) {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::warn!(op_id = %id, %error, "Could not encode outbound bridge command");
                        return Ok(());
                    }
                };
                request.reply.in_progress();
                pending.commands.insert(
                    id,
                    Awaiting::new(self.clock.as_ref(), granted, (), request.reply),
                );
                self.start_write(WriteOp::Send(message), None, Some(authorized));
            }
            Outbound::Thread(request, authorized) => {
                pending.remove_cancelled_threads();
                let mut request = *request;
                if request.reply.is_cancelled() {
                    return Ok(());
                }
                let correlation = ThreadCorrelation {
                    thread_id: request.payload.thread_id.clone(),
                    member_id: request.payload.member_id.clone(),
                    generation: request.payload.delivery_generation,
                    to_seq: request.payload.to_seq,
                };
                let id = request.payload.delivery_id.clone();
                if pending.threads.contains_key(&id)
                    || pending
                        .threads
                        .values()
                        .any(|pending| pending.correlation.same_member(&correlation))
                {
                    return Ok(());
                }
                let Some(granted) = self.clock.remaining_ms(request.deadline) else {
                    return Ok(());
                };
                request.payload.admission_budget_ms = granted;
                let message = match encode(self.epoch, &request.payload) {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::warn!(delivery_id = %id, %error, "Could not encode outbound thread delivery");
                        return Ok(());
                    }
                };
                request.reply.in_progress();
                pending.threads.insert(
                    id,
                    Awaiting::new(self.clock.as_ref(), granted, correlation, request.reply),
                );
                self.start_write(WriteOp::Send(message), None, Some(authorized));
            }
        }
        Ok(())
    }
}

async fn poll_active_write(active: &mut Option<ActiveWrite>) -> SinkOutcome {
    match active.as_mut() {
        Some(write) => write.future.as_mut().await,
        None => pending().await,
    }
}

fn encode<P: BridgePayload>(epoch: u64, payload: &P) -> Result<Message, ProtocolError> {
    let bytes = BridgeFrame::encode_json(epoch, payload)?;
    // JSON produced by serde_json is valid UTF-8.
    let text = axum::extract::ws::Utf8Bytes::try_from(bytes).map_err(|error| {
        ProtocolError::Json(serde_json::Error::io(std::io::Error::other(error)))
    })?;
    Ok(Message::Text(text))
}

#[cfg(test)]
async fn bounded<T>(
    close: &mut watch::Receiver<Option<CloseReason>>,
    clock: &dyn MonoClock,
    deadline: MonoInstant,
    future: impl Future<Output = T>,
) -> SessionResult<T> {
    tokio::select! {
        biased;
        reason = closed(close) => Err(Exit::Close(reason)),
        () = until(clock, Some(deadline)) => Err(Exit::Close(CloseReason::Retry)),
        result = future => Ok(result),
    }
}

async fn closed(close: &mut watch::Receiver<Option<CloseReason>>) -> CloseReason {
    match close.wait_for(Option::is_some).await {
        Ok(reason) => reason.unwrap_or(CloseReason::Retry),
        Err(_) => CloseReason::Retry,
    }
}

async fn until(clock: &dyn MonoClock, deadline: Option<MonoInstant>) {
    let Some(deadline) = deadline else {
        return pending().await;
    };
    while let Some(remaining_ms) = clock.remaining_ms(deadline) {
        time::sleep(Duration::from_millis(remaining_ms)).await;
    }
}

struct Heartbeat {
    next_ping: MonoInstant,
    last_nonce: u64,
    pong: Option<MonoInstant>,
}

impl Heartbeat {
    fn new(clock: &dyn MonoClock) -> Self {
        Self {
            next_ping: clock.deadline_after(PING_INTERVAL),
            last_nonce: 0,
            pong: None,
        }
    }

    fn deadline(&self) -> MonoInstant {
        self.pong.unwrap_or(self.next_ping)
    }

    fn ping(&mut self, clock: &dyn MonoClock) -> Bytes {
        self.last_nonce = self.last_nonce.wrapping_add(1);
        self.next_ping = clock.deadline_after(PING_INTERVAL);
        self.pong = Some(clock.deadline_after(PONG_TIMEOUT));
        Bytes::copy_from_slice(&self.last_nonce.to_be_bytes())
    }

    fn pong(&mut self, clock: &dyn MonoClock, payload: &[u8]) -> SessionResult<()> {
        if let Some(deadline) = self.pong {
            if clock.is_expired(deadline) {
                return Err(Exit::Close(CloseReason::Retry));
            }
            if payload == self.last_nonce.to_be_bytes() {
                self.pong = None;
            }
        }
        Ok(())
    }
}

struct Ready {
    incoming: mpsc::Receiver<Outbound>,
    cancelled: watch::Receiver<()>,
    events: mpsc::Sender<(u64, NodeEventPayload)>,
    applied: watch::Receiver<u64>,
    accepted_events: u64,
    requests: HashSet<OperationId>,
    responses: VecDeque<NodeResponsePayload>,
    rpc: JoinSet<NodeResponsePayload>,
    pending: Pending,
}

#[derive(Default)]
struct Pending {
    commands: HashMap<OperationId, Awaiting<CommandResPayload, ()>>,
    threads: HashMap<OperationId, Awaiting<ThreadAckPayload, ThreadCorrelation>>,
}

impl Pending {
    fn remove_cancelled_threads(&mut self) {
        self.threads
            .retain(|_, pending| !pending.reply.is_cancelled());
    }

    fn deadline(&self, clock: &dyn MonoClock) -> Option<MonoInstant> {
        self.commands
            .values()
            .map(|pending| pending.deadline)
            .chain(self.threads.values().map(|pending| pending.deadline))
            .min_by_key(|deadline| clock.remaining_ms(*deadline).unwrap_or(0))
    }

    fn expire(&mut self, clock: &dyn MonoClock) {
        self.commands
            .retain(|_, pending| !clock.is_expired(pending.deadline));
        self.threads
            .retain(|_, pending| !clock.is_expired(pending.deadline));
    }
}

struct Awaiting<T, C> {
    deadline: MonoInstant,
    correlation: C,
    reply: Reply<T>,
}

impl<T, C> Awaiting<T, C> {
    fn new(clock: &dyn MonoClock, granted_ms: u64, correlation: C, reply: Reply<T>) -> Self {
        Self {
            deadline: clock.deadline_after(Duration::from_millis(
                granted_ms.saturating_add(ACK_GRACE_MS),
            )),
            correlation,
            reply,
        }
    }
}

struct ThreadCorrelation {
    thread_id: ThreadId,
    member_id: ParticipantId,
    generation: u64,
    to_seq: u64,
}

impl ThreadCorrelation {
    fn matches(&self, ack: &ThreadAckPayload) -> bool {
        self.thread_id == ack.thread_id
            && self.member_id == ack.member_id
            && self.generation == ack.delivery_generation
            && self.to_seq == ack.acked_seq
    }

    fn same_member(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id && self.member_id == other.member_id
    }
}

fn problem_response(request_id: OperationId, code: ErrorCode, detail: &str) -> NodeResponsePayload {
    NodeResponsePayload {
        request_id,
        outcome: NodeRpcOutcome::Problem {
            problem: ProblemDetails::new(code, detail),
        },
    }
}

fn projection_error(key: &SessionKey, error: HubError) -> Exit {
    tracing::debug!(node_id = %key.node_id, %error, "Bridge projection could not complete");
    Exit::Close(CloseReason::Retry)
}

fn roster_error(key: &SessionKey, error: HubError) -> Exit {
    match error.problem().code {
        ErrorCode::PayloadTooLarge => Exit::Close(CloseReason::TooLarge),
        ErrorCode::InvalidRequest => Exit::Close(CloseReason::InvalidFrame),
        _ => projection_error(key, error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    #[derive(Default)]
    struct ManualClock(AtomicU64);

    impl MonoClock for ManualClock {
        fn now(&self) -> MonoInstant {
            MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
        }
    }

    impl ManualClock {
        fn set(&self, ms: u64) {
            self.0.store(ms, Ordering::Relaxed);
        }
    }

    #[test]
    fn heartbeat_uses_transport_nonce_and_fail_closed_deadlines()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = ManualClock::default();
        let mut heart = Heartbeat::new(&clock);
        assert_eq!(clock.remaining_ms(heart.deadline()), Some(15_000));
        clock.set(15_000);
        assert!(clock.is_expired(heart.deadline()));
        let first = heart.ping(&clock);
        assert_eq!(clock.remaining_ms(heart.deadline()), Some(10_000));

        clock.set(24_999);
        assert!(heart.pong(&clock, b"unrelated").is_ok());
        assert_eq!(clock.remaining_ms(heart.deadline()), Some(1));
        assert!(heart.pong(&clock, &first).is_ok());
        assert_eq!(clock.remaining_ms(heart.deadline()), Some(5_001));

        clock.set(30_000);
        let second = heart.ping(&clock);
        assert_ne!(first, second);
        clock.set(40_000);
        assert!(matches!(
            heart.pong(&clock, &second),
            Err(Exit::Close(CloseReason::Retry))
        ));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn wait_does_not_progress_an_expired_or_closed_session() {
        let clock = ManualClock::default();
        let (close_tx, mut close) = watch::channel(None);
        let deadline = clock.deadline_after(ROSTER_TIMEOUT);
        clock.set(30_000);
        let expired = bounded(&mut close, &clock, deadline, async {}).await;
        assert!(matches!(expired, Err(Exit::Close(CloseReason::Retry))));

        let fresh_deadline = clock.deadline_after(ROSTER_TIMEOUT);
        close_tx.send_replace(Some(CloseReason::Revoked));
        let revoked = bounded(&mut close, &clock, fresh_deadline, async {}).await;
        assert!(matches!(revoked, Err(Exit::Close(CloseReason::Revoked))));
    }
}
