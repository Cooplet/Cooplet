use std::{
    collections::{HashMap, HashSet},
    future::{Future, pending},
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
use futures_util::SinkExt;
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time,
};
use tokio_tungstenite::tungstenite;

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
    let mut session = Socket {
        io: socket,
        close,
        clock: bridge.clock().clone(),
        heartbeat: Heartbeat::new(bridge.clock().as_ref()),
        epoch: key.epoch,
    };
    let registered = admission.register(key.epoch, close_tx.clone(), outbound, cancelled_tx);
    drop(handshake);
    if let Err(reason) = registered {
        session.send_close(reason).await;
        return;
    }

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
    if let Err(error) = projection::mark_offline(&state, key.clone()).await {
        tracing::warn!(node_id = %key.node_id, %error, "Bridge disconnect could not update the node catalog");
    }
    drop(admission);
}

type SessionResult<T> = Result<T, Exit>;

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

struct Socket {
    io: WebSocket,
    close: watch::Receiver<Option<CloseReason>>,
    clock: Arc<dyn MonoClock>,
    heartbeat: Heartbeat,
    epoch: u64,
}

struct Frame {
    frame: BridgeFrame,
    bytes: usize,
}

impl Socket {
    async fn synchronize(&mut self, state: &AppState, key: &SessionKey) -> SessionResult<()> {
        let initial = self.clock.deadline_after(ROSTER_TIMEOUT);
        let pending = bounded(
            &mut self.close,
            self.clock.as_ref(),
            initial,
            delivery::pending_op_ids(state, &key.node_id),
        )
        .await?
        .map_err(|error| projection_error(key, error))?;
        let hello = HelloAckPayload {
            hub_id: state.meta.hub_id.clone(),
            node_id: key.node_id.clone(),
            max_inflight_commands: COMMAND_SLOTS,
            max_inflight_threads: THREAD_SLOTS,
            pending_op_ids: pending.clone(),
        };
        self.send(encode(self.epoch, &hello)?, initial).await?;

        let clock = self.clock.clone();
        let deadline = clock.deadline_after(ROSTER_TIMEOUT);
        let guard = AdmissionGuard::with_clock(ROSTER_TIMEOUT, clock.clone());
        let mut roster = Roster::default();
        loop {
            let packet = tokio::select! {
                biased;
                reason = closed(&mut self.close) => return Err(Exit::Close(reason)),
                () = until(clock.as_ref(), Some(deadline)) => return Err(Exit::Close(CloseReason::Retry)),
                () = until(clock.as_ref(), Some(self.heartbeat.deadline())) => {
                    self.heartbeat_tick().await?;
                    continue;
                }
                packet = self.io.recv() => packet,
            };
            let Some(Frame { frame, bytes }) = self.read(packet).await? else {
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
        let commit_result = tokio::select! {
            biased;
            reason = closed(&mut self.close) => {
                guard.expire();
                self.send_close(reason).await;
                let _ = commit.await;
                return Err(Exit::Peer);
            }
            () = until(clock.as_ref(), Some(deadline)) => {
                guard.expire();
                self.send_close(CloseReason::Retry).await;
                let _ = commit.await;
                return Err(Exit::Peer);
            }
            result = &mut commit => result,
        };
        commit_result.map_err(|error| roster_error(key, error))?;
        self.send(encode(self.epoch, &RosterReadyPayload {})?, deadline)
            .await
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
        let (events, receive_events) = mpsc::channel(SESSION_QUEUE);
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
        let clock = self.clock.clone();
        let next_ack = ready.pending.deadline(clock.as_ref());
        let result = tokio::select! {
            reason = closed(&mut self.close) => Err(Exit::Close(reason)),
            worker = worker => {
                if let Ok(Err(error)) = worker {
                    tracing::debug!(node_id = %key.node_id, %error, "Bridge event projection ended");
                }
                return Err((Exit::Close(CloseReason::Retry), true));
            }
            () = until(clock.as_ref(), Some(self.heartbeat.deadline())) => self.heartbeat_tick().await,
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
            frame = self.io.recv() => self.receive(state, key, ready, frame).await,
            request = ready.incoming.recv() => match request {
                Some(request) => self.forward(&mut ready.pending, request).await,
                None => Err(Exit::Close(CloseReason::Retry)),
            },
            result = ready.rpc.join_next(), if !ready.rpc.is_empty() => match result {
                Some(Ok(response)) => {
                    ready.requests.remove(&response.request_id);
                    self.send_response(response).await
                }
                _ => Err(Exit::Close(CloseReason::Retry)),
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
        let Some(Frame { frame, .. }) = self.read(packet).await? else {
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
                if ready.rpc.len() >= REVERSE_SLOTS {
                    self.send_response(problem_response(
                        req.request_id,
                        ErrorCode::RateLimited,
                        "The node has no free reverse request slots.",
                    ))
                    .await?;
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
                let _ = time::timeout(CLOSE_TIMEOUT, self.io.flush()).await;
                Err(Exit::Peer)
            }
            Message::Ping(_) => {
                // Tungstenite queues the RFC Pong automatically.
                let deadline = self.clock.deadline_after(WRITE_TIMEOUT);
                bounded(
                    &mut self.close,
                    self.clock.as_ref(),
                    deadline,
                    self.io.flush(),
                )
                .await?
                .map_err(|_| Exit::Peer)?;
                Ok(None)
            }
            Message::Pong(payload) => {
                self.heartbeat.pong(self.clock.as_ref(), &payload)?;
                Ok(None)
            }
        }
    }

    async fn heartbeat_tick(&mut self) -> SessionResult<()> {
        if self.heartbeat.pong.is_some() {
            return Err(Exit::Close(CloseReason::Retry));
        }
        let payload = self.heartbeat.ping(self.clock.as_ref());
        self.send(Message::Ping(payload), self.heartbeat.deadline())
            .await
    }

    async fn send(&mut self, message: Message, deadline: MonoInstant) -> SessionResult<()> {
        bounded(
            &mut self.close,
            self.clock.as_ref(),
            deadline,
            self.io.send(message),
        )
        .await?
        .map_err(|_| Exit::Peer)
    }

    async fn send_close(&mut self, reason: CloseReason) {
        let reason = self.close.borrow().unwrap_or(reason);
        let (code, reason) = reason.frame();
        let _ = time::timeout(
            CLOSE_TIMEOUT,
            self.io.send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.into(),
            }))),
        )
        .await;
    }

    async fn send_response(&mut self, response: NodeResponsePayload) -> SessionResult<()> {
        let deadline = self.clock.deadline_after(WRITE_TIMEOUT);
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
        self.send(encoded, deadline).await
    }

    async fn forward(&mut self, pending: &mut Pending, request: Outbound) -> SessionResult<()> {
        match request {
            Outbound::Command(request) => {
                let mut request = *request;
                let id = request.payload.op_id.clone();
                if pending.commands.contains_key(&id) {
                    return Ok(());
                }
                let Some(granted) = self.clock.remaining_ms(request.deadline) else {
                    return Ok(());
                };
                request.payload.admission_budget_ms = granted;
                let Ok(message) = encode(self.epoch, &request.payload) else {
                    return Ok(());
                };
                if !self
                    .forward_wire(message, request.deadline, &mut request.reply)
                    .await?
                {
                    return Ok(());
                }
                pending.commands.insert(
                    id,
                    Awaiting::new(self.clock.as_ref(), granted, (), request.reply),
                );
            }
            Outbound::Thread(request) => {
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
                let Ok(message) = encode(self.epoch, &request.payload) else {
                    return Ok(());
                };
                if !self
                    .forward_wire(message, request.deadline, &mut request.reply)
                    .await?
                {
                    return Ok(());
                }
                pending.threads.insert(
                    id,
                    Awaiting::new(self.clock.as_ref(), granted, correlation, request.reply),
                );
            }
        }
        Ok(())
    }

    async fn forward_wire<T>(
        &mut self,
        message: Message,
        deadline: MonoInstant,
        reply: &mut Reply<T>,
    ) -> SessionResult<bool> {
        let Some(remaining) = self.clock.remaining_ms(deadline) else {
            return Ok(false);
        };
        let deadline = if Duration::from_millis(remaining) > WRITE_TIMEOUT {
            self.clock.deadline_after(WRITE_TIMEOUT)
        } else {
            deadline
        };
        let mut started = false;
        let write = async {
            if reply.is_cancelled() {
                return Ok(());
            }
            started = true;
            reply.in_progress();
            self.io.send(message).await
        };
        match bounded(&mut self.close, self.clock.as_ref(), deadline, write).await {
            Ok(result) => result.map(|()| started).map_err(|_| Exit::Peer),
            Err(Exit::Close(CloseReason::Retry)) if !started => Ok(false),
            Err(error) => Err(error),
        }
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
