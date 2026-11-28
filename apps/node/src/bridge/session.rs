use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::{Future, pending, ready},
    path::Path,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use cooplet_client::{ClientError, X_COOPLET_HUB, build_rustls_client_config};
use cooplet_platform::{AdmissionGuard, MonoClock, MonoInstant};
use cooplet_protocol::{
    BRIDGE_ERROR_HEADER, BRIDGE_SUBPROTOCOL, BridgeFrame, BridgePayload, CommandOutcome,
    CommandReqPayload, CommandResPayload, ErrorCode, FrameKind, HelloAckPayload,
    MAX_WS_FRAME_BYTES, OperationId, ParticipantId, ProblemDetails, ROSTER_TIMEOUT,
    RosterReadyPayload, ThreadAckPayload, ThreadDeliverPayload, ThreadDeliveryOutcome, ThreadId,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use http::{
    HeaderValue, StatusCode,
    header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{
        Error as WsError, Message,
        client::IntoClientRequest,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};
use url::Host;

use crate::http::NodeState;

use super::{
    HubConnection, NodeBridge, events, execution,
    reverse::{ReverseHandle, ReverseRequest, ReverseRequests},
    roster, wait_deadline, wait_stopped,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const PING_INTERVAL: Duration = Duration::from_secs(15);
const PONG_TIMEOUT: Duration = Duration::from_secs(10);
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_COMMANDS: usize = 32;
const MAX_THREAD_DELIVERIES: usize = 16;
const OUTBOX_CAPACITY: usize =
    MAX_COMMANDS + MAX_THREAD_DELIVERIES + super::reverse::MAX_REVERSE_IN_FLIGHT;

type SessionResult<T> = Result<T, Exit>;

trait WebSocketIo: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> WebSocketIo for T {}

type WebSocket = WebSocketStream<Box<dyn WebSocketIo>>;

pub(super) enum Outbound {
    Reverse(ReverseRequest),
    Command(CommandResPayload),
    Thread(ThreadAckPayload),
}

#[derive(Clone, Copy)]
pub(super) enum Exit {
    Stopping,
    ConnectTimeout,
    RosterTimeout,
    SendTimeout,
    PongTimeout,
    Transport,
    Protocol,
    EpochMismatch,
    BinaryUnsupported,
    FrameTooLarge,
    RosterInvalid,
    EventProjection,
    Revoked,
    IdentityMismatch,
    PeerClosed,
}

impl Exit {
    pub(super) fn reason(self) -> &'static str {
        match self {
            Self::Stopping => "node_shutdown",
            Self::ConnectTimeout => "connect_timeout",
            Self::RosterTimeout => "roster_timeout",
            Self::SendTimeout => "send_timeout",
            Self::PongTimeout => "pong_timeout",
            Self::Transport => "transport_disconnected",
            Self::Protocol => "invalid_bridge_frame",
            Self::EpochMismatch => "epoch_mismatch",
            Self::BinaryUnsupported => "binary_unsupported",
            Self::FrameTooLarge => "bridge_frame_too_large",
            Self::RosterInvalid => "invalid_roster",
            Self::EventProjection => "event_projection_failed",
            Self::Revoked => "node_revoked",
            Self::IdentityMismatch => "hub_identity_mismatch",
            Self::PeerClosed => "peer_closed",
        }
    }

    fn close(self) -> Option<CloseCode> {
        match self {
            Self::Stopping => Some(CloseCode::Away),
            Self::Protocol | Self::IdentityMismatch => Some(CloseCode::Protocol),
            Self::EpochMismatch => Some(CloseCode::from(4002)),
            Self::BinaryUnsupported => Some(CloseCode::Unsupported),
            Self::FrameTooLarge => Some(CloseCode::Size),
            Self::RosterInvalid | Self::EventProjection => Some(CloseCode::Error),
            Self::ConnectTimeout | Self::RosterTimeout | Self::SendTimeout | Self::PongTimeout => {
                Some(CloseCode::Away)
            }
            Self::Transport | Self::Revoked | Self::PeerClosed => None,
        }
    }

    pub(super) fn terminal_code(self) -> Option<ErrorCode> {
        match self {
            Self::Revoked => Some(ErrorCode::NodeRevoked),
            Self::IdentityMismatch => Some(ErrorCode::HubIdentityMismatch),
            _ => None,
        }
    }
}

pub(super) struct WorkerLimits {
    commands: Arc<Semaphore>,
    threads: Arc<Semaphore>,
}

impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            commands: Arc::new(Semaphore::new(MAX_COMMANDS)),
            threads: Arc::new(Semaphore::new(MAX_THREAD_DELIVERIES)),
        }
    }
}

struct Attempt<'a> {
    bridge: &'a NodeBridge,
    state: &'a NodeState,
    workspace: &'a Path,
    hub: &'a Arc<HubConnection>,
    limits: &'a WorkerLimits,
}

pub(super) async fn run(
    bridge: &NodeBridge,
    state: &NodeState,
    workspace: &Path,
    hub: &Arc<HubConnection>,
    limits: &WorkerLimits,
    stop: &watch::Receiver<bool>,
) -> (bool, Exit) {
    let clock = bridge.clock.clone();
    let connect_deadline = clock.deadline_after(CONNECT_TIMEOUT);
    let socket = match connect(&*clock, hub, connect_deadline, stop).await {
        Ok(socket) => socket,
        Err(exit) => return (false, exit),
    };
    let mut io = Wire::new(socket, clock);
    let attempt = Attempt {
        bridge,
        state,
        workspace,
        hub,
        limits,
    };
    let (ready, exit) = attempt.serve(&mut io, connect_deadline, stop).await;
    bridge.session.send_replace(None);
    if let Some(code) = exit.close() {
        io.close(code, exit.reason()).await;
    }
    (ready, exit)
}

impl Attempt<'_> {
    async fn serve(
        &self,
        io: &mut Wire,
        connect_deadline: MonoInstant,
        stop: &watch::Receiver<bool>,
    ) -> (bool, Exit) {
        let (epoch, hello) = match self.hello(io, connect_deadline, stop).await {
            Ok(hello) => hello,
            Err(exit) => return (false, exit),
        };
        let roster_deadline = self.bridge.clock.deadline_after(ROSTER_TIMEOUT);
        let notices = self.state.db.subscribe();
        let uncertain_commit = self.state.db.subscribe_uncertain_commits();
        let max_commands = (hello.max_inflight_commands as usize).min(MAX_COMMANDS);
        let max_threads = (hello.max_inflight_threads as usize).min(MAX_THREAD_DELIVERIES);
        let (cursor, participants) = match self
            .sync_roster(io, epoch, hello, roster_deadline, stop)
            .await
        {
            Ok(cursor) => cursor,
            Err(exit) => return (false, exit),
        };
        let (outbox, input) = mpsc::channel(OUTBOX_CAPACITY);
        self.bridge
            .session
            .send_replace(Some(ReverseHandle::new(epoch, outbox.clone())));

        let (event_tx, event_rx) = mpsc::channel(1);
        let reads = self.state.reads.clone();
        let local_id = self.state.info.node_id.clone();
        self.bridge.spawn_worker(async move {
            if let Err(error) = events::pump(
                reads,
                local_id,
                notices,
                uncertain_commit,
                cursor,
                participants,
                event_tx,
            )
            .await
            {
                tracing::warn!(code = %error.code(), "Could not forward Node events");
            }
        });
        let mut online = Online {
            epoch,
            outbox,
            input,
            immediate_replies: VecDeque::new(),
            work: InFlight::new(max_commands, max_threads),
            events: CatchUp::new(cursor, event_rx),
            requests: ReverseRequests::default(),
        };
        let exit = self.online(io, &mut online, stop).await;
        (true, exit)
    }

    async fn hello(
        &self,
        io: &mut Wire,
        deadline: MonoInstant,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<(u64, HelloAckPayload)> {
        let frame = io.next_app(deadline, Exit::ConnectTimeout, stop).await?;
        if frame.epoch == 0 {
            return Err(Exit::EpochMismatch);
        }
        let hello = decode::<HelloAckPayload>(&frame)?;
        if &hello.hub_id != self.hub.hub_id() || &hello.node_id != self.hub.node_id() {
            return Err(Exit::IdentityMismatch);
        }
        Ok((frame.epoch, hello))
    }

    async fn sync_roster(
        &self,
        io: &mut Wire,
        epoch: u64,
        hello: HelloAckPayload,
        deadline: MonoInstant,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<(u64, HashSet<ParticipantId>)> {
        let capture = roster::capture(&self.state.reads, self.workspace, self.hub, epoch, hello);
        let roster = tokio::select! {
            roster = capture => roster.map_err(|error| {
                tracing::warn!(code = %error.code(), "Could not prepare the Node roster");
                Exit::RosterInvalid
            })?,
            frame = io.next_app(deadline, Exit::RosterTimeout, stop) => {
                let frame = frame?;
                return Err(if frame.epoch == epoch { Exit::Protocol } else { Exit::EpochMismatch });
            }
        };

        let phase = Some((deadline, Exit::RosterTimeout));
        let mut frames: VecDeque<_> = roster.frames.into();
        while !frames.is_empty() {
            io.flush_control_queue(phase);
            let write_limit = io.write_deadline();
            let heartbeat = io.heartbeat_poll_deadline();
            let can_write_app = io.can_write_app();
            tokio::select! {
                biased;
                () = wait_stopped(stop) => return Err(Exit::Stopping),
                () = wait_deadline(&*self.bridge.clock, Some(deadline)) => return Err(Exit::RosterTimeout),
                () = wait_deadline(&*self.bridge.clock, write_limit.map(|(at, _)| at)), if write_limit.is_some() => {
                    return Err(write_limit.map_or(Exit::RosterTimeout, |(_, exit)| exit));
                }
                () = wait_deadline(&*self.bridge.clock, heartbeat), if heartbeat.is_some() => {
                    io.heartbeat_tick(phase)?;
                }
                outcome = poll_active_write(&mut io.active_write), if io.active_write.is_some() => {
                    io.finish_write(outcome, phase)?;
                }
                message = io.stream.next() => {
                    if let Some(frame) = io.received(message, phase).await? {
                        return Err(if frame.epoch == epoch { Exit::Protocol } else { Exit::EpochMismatch });
                    }
                }
                () = ready(()), if can_write_app => {
                    if let Some(frame) = frames.pop_front() {
                        io.start_write(WriteOp::Send(Message::Text(frame.into())), phase);
                    }
                }
            }
        }

        let ready = io.next_app(deadline, Exit::RosterTimeout, stop).await?;
        check_epoch(&ready, epoch)?;
        decode::<RosterReadyPayload>(&ready)?;
        Ok((roster.cursor, roster.participants))
    }

    async fn online(
        &self,
        io: &mut Wire,
        online: &mut Online,
        stop: &watch::Receiver<bool>,
    ) -> Exit {
        loop {
            if let Err(exit) = self.online_step(io, online, stop).await {
                return exit;
            }
        }
    }

    async fn online_step(
        &self,
        io: &mut Wire,
        online: &mut Online,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        if *stop.borrow() {
            return Err(Exit::Stopping);
        }
        io.flush_control_queue(None);
        if let Some(heartbeat) = io.heartbeat_poll_deadline()
            && self.bridge.clock.is_expired(heartbeat)
        {
            io.heartbeat_tick(None)?;
        }
        online.requests.expire(&*self.bridge.clock);
        let write_limit = io.write_deadline();
        let heartbeat = io.heartbeat_poll_deadline();
        let correlation = online.requests.earliest_deadline(&*self.bridge.clock);
        let ready_cursor = online.events.ready_cursor();
        let can_send_request = online.requests.has_ready(ready_cursor);
        let can_write_app = io.can_write_app();
        tokio::select! {
            () = wait_stopped(stop) => return Err(Exit::Stopping),
            () = wait_deadline(&*self.bridge.clock, write_limit.map(|(at, _)| at)), if write_limit.is_some() => {
                return Err(write_limit.map_or(Exit::SendTimeout, |(_, exit)| exit));
            }
            outcome = poll_active_write(&mut io.active_write), if io.active_write.is_some() => {
                io.finish_write(outcome, None)?;
            }
            () = wait_deadline(&*self.bridge.clock, heartbeat), if heartbeat.is_some() => io.heartbeat_tick(None)?,
            () = wait_deadline(&*self.bridge.clock, correlation) => online.requests.expire(&*self.bridge.clock),
            socket_message = io.stream.next() => {
                if let Some(frame) = io.received(socket_message, None).await? {
                    self.handle(online, frame)?;
                }
            }
            () = ready(()), if can_write_app && !online.immediate_replies.is_empty() => {
                if let Some(reply) = online.immediate_replies.pop_front() {
                    io.start_write(WriteOp::Send(reply), None);
                }
            }
            outbound = online.input.recv(), if can_write_app && online.immediate_replies.is_empty() => {
                if let Some(outbound) = outbound {
                    online.handle_outbound(io, outbound)?;
                }
            }
            batch = online.events.input.recv(), if online.events.pending.is_empty() => {
                online.events.batch(batch.ok_or(Exit::EventProjection)?);
            }
            () = ready(()), if can_write_app && online.immediate_replies.is_empty() && !online.events.pending.is_empty() => {
                if let Some(event) = online.events.pending.pop_front() {
                    io.start_payload(online.epoch, &event)?;
                }
            }
            () = ready(()), if can_write_app && online.immediate_replies.is_empty() && can_send_request => {
                if let Some(mut request) = online.requests.take_ready(ready_cursor)
                    && request.mark_sent(&*self.bridge.clock)
                {
                    io.start_write(WriteOp::Send(Message::Text(request.take_frame().into())), None);
                    online.requests.sent(request);
                }
            }
        }
        Ok(())
    }

    fn handle(&self, online: &mut Online, frame: BridgeFrame) -> SessionResult<()> {
        check_epoch(&frame, online.epoch)?;
        match frame.kind {
            FrameKind::CommandReq => {
                let request = decode::<CommandReqPayload>(&frame)?;
                let admission = AdmissionGuard::with_clock(
                    Duration::from_millis(request.admission_budget_ms),
                    self.bridge.clock.clone(),
                );
                if let Some(rejected) = self.dispatch_command(online, request, admission) {
                    online
                        .immediate_replies
                        .push_back(encode_payload(online.epoch, &rejected)?);
                }
            }
            FrameKind::ThreadDeliver => {
                let request = decode::<ThreadDeliverPayload>(&frame)?;
                let admission = AdmissionGuard::with_clock(
                    Duration::from_millis(request.admission_budget_ms),
                    self.bridge.clock.clone(),
                );
                if let Some(rejected) = self.dispatch_thread(online, request, admission) {
                    online
                        .immediate_replies
                        .push_back(encode_payload(online.epoch, &rejected)?);
                }
            }
            FrameKind::NodeResponse => online
                .requests
                .respond(decode(&frame)?, &*self.bridge.clock),
            _ => return Err(Exit::Protocol),
        }
        Ok(())
    }

    fn dispatch_command(
        &self,
        online: &mut Online,
        request: CommandReqPayload,
        admission: AdmissionGuard,
    ) -> Option<CommandResPayload> {
        let op_id = request.op_id.clone();
        if online.work.commands.contains_key(&op_id) {
            // Do not send an early second receipt for the operation in progress.
            return None;
        }
        let slot = (online.work.commands.len() < online.work.max_commands)
            .then(|| self.limits.commands.clone().try_acquire_owned().ok())
            .flatten();
        let Some(slot) = slot else {
            return Some(CommandResPayload {
                op_id,
                outcome: CommandOutcome::Rejected,
                resource_id: None,
                result: None,
                error: Some(ProblemDetails::new(
                    ErrorCode::RateLimited,
                    "The Node command limit is reached",
                )),
            });
        };
        online.work.commands.insert(op_id, admission.clone());
        let state = self.state.clone();
        let hub = self.hub.clone();
        let outbox = online.outbox.clone();
        self.bridge.spawn_worker(async move {
            let _slot = slot;
            let reply = execution::command(
                &state,
                hub.hub_id(),
                hub.node_id(),
                hub.client(),
                request,
                admission,
            )
            .await;
            let _ = outbox.send(Outbound::Command(reply)).await;
        });
        None
    }

    fn dispatch_thread(
        &self,
        online: &mut Online,
        request: ThreadDeliverPayload,
        admission: AdmissionGuard,
    ) -> Option<ThreadAckPayload> {
        let key = (request.thread_id.clone(), request.member_id.clone());
        if online
            .work
            .threads
            .get(&key)
            .is_some_and(|pending| pending.delivery == request.delivery_id)
        {
            return None;
        }
        let slot = match online.work.thread_slot(&key, &self.limits.threads) {
            Ok(slot) => slot,
            Err(outcome) => {
                return Some(ThreadAckPayload {
                    delivery_id: request.delivery_id,
                    thread_id: request.thread_id,
                    member_id: request.member_id,
                    delivery_generation: request.delivery_generation,
                    acked_seq: request.to_seq,
                    outcome,
                    detail: Some("thread_delivery_in_flight".into()),
                });
            }
        };
        online.work.threads.insert(
            key,
            ThreadWork {
                delivery: request.delivery_id.clone(),
                admission: admission.clone(),
            },
        );
        let state = self.state.clone();
        let hub = self.hub.clone();
        let outbox = online.outbox.clone();
        self.bridge.spawn_worker(async move {
            let _slot = slot;
            let reply = execution::thread(&state, hub.client(), request, admission).await;
            let _ = outbox.send(Outbound::Thread(reply)).await;
        });
        None
    }
}

struct Online {
    epoch: u64,
    outbox: mpsc::Sender<Outbound>,
    input: mpsc::Receiver<Outbound>,
    immediate_replies: VecDeque<Message>,
    work: InFlight,
    events: CatchUp,
    requests: ReverseRequests,
}

impl Online {
    fn handle_outbound(&mut self, io: &mut Wire, outbound: Outbound) -> SessionResult<()> {
        match outbound {
            Outbound::Reverse(request) => self.requests.enqueue(request),
            Outbound::Command(reply) => {
                self.work.commands.remove(&reply.op_id);
                io.start_payload(self.epoch, &reply)?;
            }
            Outbound::Thread(reply) => {
                self.work
                    .threads
                    .remove(&(reply.thread_id.clone(), reply.member_id.clone()));
                io.start_payload(self.epoch, &reply)?;
            }
        }
        Ok(())
    }
}

struct CatchUp {
    cursor: u64,
    initial_head: Option<u64>,
    pending: VecDeque<cooplet_protocol::NodeEventPayload>,
    input: mpsc::Receiver<events::Batch>,
}

impl CatchUp {
    fn new(cursor: u64, input: mpsc::Receiver<events::Batch>) -> Self {
        Self {
            cursor,
            initial_head: None,
            pending: VecDeque::new(),
            input,
        }
    }

    fn batch(&mut self, batch: events::Batch) {
        self.cursor = batch.through;
        self.initial_head = Some(batch.initial_head);
        self.pending = batch.events.into();
    }

    fn ready_cursor(&self) -> Option<u64> {
        (self.pending.is_empty() && self.initial_head.is_some_and(|head| self.cursor >= head))
            .then_some(self.cursor)
    }
}

struct InFlight {
    max_commands: usize,
    max_threads: usize,
    commands: HashMap<OperationId, AdmissionGuard>,
    threads: HashMap<(ThreadId, ParticipantId), ThreadWork>,
}

struct ThreadWork {
    delivery: OperationId,
    admission: AdmissionGuard,
}

impl InFlight {
    fn thread_slot(
        &self,
        key: &(ThreadId, ParticipantId),
        slots: &Arc<Semaphore>,
    ) -> Result<OwnedSemaphorePermit, ThreadDeliveryOutcome> {
        // These slots can outlive their bridge epoch. Their release does not
        // emit a participant Idle event, so the Hub must retry on backoff.
        if self.threads.len() >= self.max_threads || self.threads.contains_key(key) {
            return Err(ThreadDeliveryOutcome::Unavailable);
        }
        slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ThreadDeliveryOutcome::Unavailable)
    }

    fn new(max_commands: usize, max_threads: usize) -> Self {
        Self {
            max_commands,
            max_threads,
            commands: HashMap::new(),
            threads: HashMap::new(),
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let threads = self.threads.values().map(|work| &work.admission);
        for admission in self.commands.values().chain(threads) {
            admission.expire();
        }
    }
}

async fn connect(
    clock: &dyn MonoClock,
    hub: &HubConnection,
    deadline: MonoInstant,
    stop: &watch::Receiver<bool>,
) -> SessionResult<WebSocket> {
    within(clock, deadline, Exit::ConnectTimeout, stop, async {
        let remaining = clock.remaining_ms(deadline).ok_or(Exit::ConnectTimeout)?;
        hub.client()
            .verify_hub_identity_with_timeout(Duration::from_millis(remaining))
            .await
            .map_err(client_exit)?;
        let url = hub.url();
        let host = match url.host() {
            Some(Host::Domain(host)) => host.to_owned(),
            Some(Host::Ipv4(address)) => address.to_string(),
            Some(Host::Ipv6(address)) => address.to_string(),
            None => return Err(Exit::Protocol),
        };
        let port = url.port_or_known_default().ok_or(Exit::Protocol)?;
        let tcp = TcpStream::connect((host.as_str(), port))
            .await
            .map_err(|_| Exit::Transport)?;
        tcp.set_nodelay(true).map_err(|_| Exit::Transport)?;
        let tls = url.scheme() == "https";
        let stream: Box<dyn WebSocketIo> = if tls {
            let config = build_rustls_client_config(hub.trust()).map_err(client_exit)?;
            let server =
                rustls_pki_types::ServerName::try_from(host).map_err(|_| Exit::Protocol)?;
            let stream = TlsConnector::from(config)
                .connect(server, tcp)
                .await
                .map_err(|_| Exit::Transport)?;
            Box::new(stream)
        } else {
            Box::new(tcp)
        };

        let mut endpoint = url.clone();
        endpoint.set_path("/v1/bridge/connect");
        endpoint
            .set_scheme(if tls { "wss" } else { "ws" })
            .map_err(|()| Exit::Protocol)?;
        let mut request = endpoint.as_str().into_client_request().map_err(ws_error)?;
        let mut bearer = HeaderValue::from_str(&format!("Bearer {}", hub.node_token()))
            .map_err(|_| Exit::Protocol)?;
        bearer.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, bearer);
        request.headers_mut().insert(
            X_COOPLET_HUB,
            HeaderValue::from_str(hub.hub_id().as_str()).map_err(|_| Exit::Protocol)?,
        );
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(BRIDGE_SUBPROTOCOL),
        );
        let config = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_WS_FRAME_BYTES * 2)
            .max_message_size(Some(MAX_WS_FRAME_BYTES))
            .max_frame_size(Some(MAX_WS_FRAME_BYTES));
        let (socket, response) = client_async_with_config(request, stream, Some(config))
            .await
            .map_err(ws_error)?;
        let mut protocols = response.headers().get_all(SEC_WEBSOCKET_PROTOCOL).iter();
        if protocols
            .next()
            .is_none_or(|value| value != BRIDGE_SUBPROTOCOL)
            || protocols.next().is_some()
        {
            return Err(Exit::Protocol);
        }
        Ok(socket)
    })
    .await
}

fn client_exit(error: ClientError) -> Exit {
    match error {
        ClientError::HubIdentityMismatch { .. } => Exit::IdentityMismatch,
        ClientError::Problem(problem) if problem.code == ErrorCode::NodeRevoked => Exit::Revoked,
        _ => Exit::Transport,
    }
}

fn ws_error(error: WsError) -> Exit {
    match error {
        WsError::Capacity(_) => Exit::FrameTooLarge,
        WsError::Utf8(_) => Exit::Protocol,
        WsError::Protocol(_) => Exit::Protocol,
        WsError::Http(response) => {
            let status = response.status();
            // A rejected upgrade may deliver its HTTP body in a separate TCP
            // packet. Tungstenite cannot be relied on to return that body.
            let code = response
                .headers()
                .get(BRIDGE_ERROR_HEADER)
                .and_then(|value| value.to_str().ok())
                .and_then(|code| code.parse().ok())
                .or_else(|| {
                    response
                        .body()
                        .as_deref()
                        .and_then(|body| serde_json::from_slice::<ProblemDetails>(body).ok())
                        .filter(|problem| problem.status == status.as_u16())
                        .map(|problem| problem.code)
                });
            match (status, code) {
                (StatusCode::UNAUTHORIZED, Some(ErrorCode::NodeRevoked)) => Exit::Revoked,
                (StatusCode::CONFLICT, Some(ErrorCode::HubIdentityMismatch)) => {
                    Exit::IdentityMismatch
                }
                _ => Exit::Transport,
            }
        }
        _ => Exit::Transport,
    }
}

type NodeSinkOutcome = (SplitSink<WebSocket, Message>, Result<(), WsError>);

enum WriteOp {
    Send(Message),
    Flush,
}

struct ActiveWrite {
    future: Pin<Box<dyn Future<Output = NodeSinkOutcome> + Send>>,
    deadline: MonoInstant,
    on_timeout: Exit,
}

struct Wire {
    stream: SplitStream<WebSocket>,
    sink: Option<SplitSink<WebSocket, Message>>,
    active_write: Option<ActiveWrite>,
    control_writes: VecDeque<WriteOp>,
    heartbeat: Heartbeat,
    clock: Arc<dyn MonoClock>,
}

impl Wire {
    fn new(socket: WebSocket, clock: Arc<dyn MonoClock>) -> Self {
        let (sink, stream) = socket.split();
        Self {
            stream,
            sink: Some(sink),
            active_write: None,
            control_writes: VecDeque::new(),
            heartbeat: Heartbeat::new(&*clock),
            clock,
        }
    }

    fn can_write_app(&self) -> bool {
        self.sink.is_some() && self.control_writes.is_empty()
    }

    fn heartbeat_poll_deadline(&self) -> Option<MonoInstant> {
        if self.heartbeat.awaiting.is_some() || self.sink.is_some() {
            Some(self.heartbeat.deadline())
        } else {
            None
        }
    }

    fn write_deadline(&self) -> Option<(MonoInstant, Exit)> {
        self.active_write
            .as_ref()
            .map(|write| (write.deadline, write.on_timeout))
    }

    fn transport_write_deadline(
        &self,
        timeout: Duration,
        phase_limit: Option<(MonoInstant, Exit)>,
    ) -> (MonoInstant, Exit) {
        let mut bound = (self.clock.deadline_after(timeout), Exit::SendTimeout);
        if let Some((_, pong_deadline)) = self.heartbeat.awaiting
            && pong_deadline.is_earlier_than(bound.0)
        {
            bound = (pong_deadline, Exit::PongTimeout);
        }
        if let Some(limit) = phase_limit
            && limit.0.is_earlier_than(bound.0)
        {
            bound = limit;
        }
        bound
    }

    fn start_write(&mut self, op: WriteOp, phase_limit: Option<(MonoInstant, Exit)>) {
        let Some(mut sink) = self.sink.take() else {
            return;
        };
        let (deadline, on_timeout) = self.transport_write_deadline(SEND_TIMEOUT, phase_limit);
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
            on_timeout,
        });
    }

    fn queue_control_write(&mut self, op: WriteOp, phase_limit: Option<(MonoInstant, Exit)>) {
        if matches!(op, WriteOp::Flush)
            && self
                .control_writes
                .iter()
                .any(|queued| matches!(queued, WriteOp::Flush))
        {
            return;
        }
        if self.sink.is_some() {
            self.start_write(op, phase_limit);
        } else {
            self.control_writes.push_back(op);
        }
    }

    fn flush_control_queue(&mut self, phase_limit: Option<(MonoInstant, Exit)>) {
        if self.sink.is_some()
            && let Some(op) = self.control_writes.pop_front()
        {
            self.start_write(op, phase_limit);
        }
    }

    fn finish_write(
        &mut self,
        outcome: NodeSinkOutcome,
        phase_limit: Option<(MonoInstant, Exit)>,
    ) -> SessionResult<()> {
        self.active_write = None;
        self.sink = Some(outcome.0);
        outcome.1.map_err(ws_error)?;
        self.flush_control_queue(phase_limit);
        Ok(())
    }

    async fn next_app(
        &mut self,
        deadline: MonoInstant,
        phase_timeout: Exit,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<BridgeFrame> {
        let clock = self.clock.clone();
        let phase = Some((deadline, phase_timeout));
        loop {
            self.flush_control_queue(phase);
            let write_limit = self.write_deadline();
            let heartbeat = self.heartbeat_poll_deadline();
            tokio::select! {
                biased;
                () = wait_stopped(stop) => return Err(Exit::Stopping),
                () = wait_deadline(&*clock, Some(deadline)) => return Err(phase_timeout),
                () = wait_deadline(&*clock, write_limit.map(|(at, _)| at)), if write_limit.is_some() => {
                    return Err(write_limit.map_or(phase_timeout, |(_, exit)| exit));
                }
                () = wait_deadline(&*clock, heartbeat), if heartbeat.is_some() => {
                    self.heartbeat_tick(phase)?;
                }
                outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                    self.finish_write(outcome, phase)?;
                }
                input = self.stream.next() => {
                    if let Some(frame) = self.received(input, phase).await? {
                        return Ok(frame);
                    }
                }
            }
        }
    }

    async fn received(
        &mut self,
        input: Option<Result<Message, WsError>>,
        phase_limit: Option<(MonoInstant, Exit)>,
    ) -> SessionResult<Option<BridgeFrame>> {
        let message = input.ok_or(Exit::Transport)?.map_err(ws_error)?;
        match message {
            Message::Text(text) => BridgeFrame::from_json(text.as_bytes())
                .map(Some)
                .map_err(|_| Exit::Protocol),
            Message::Binary(_) => Err(Exit::BinaryUnsupported),
            Message::Pong(payload) => {
                self.heartbeat.pong(&*self.clock, &payload)?;
                Ok(None)
            }
            Message::Ping(_) => {
                // Tungstenite queues the RFC 6455 Pong while reading a Ping.
                self.queue_control_write(WriteOp::Flush, phase_limit);
                Ok(None)
            }
            Message::Close(close) => {
                // Send the automatically queued close reply before dropping the stream.
                self.active_write = None;
                if let Some(mut sink) = self.sink.take() {
                    let deadline = self.clock.deadline_after(CLOSE_TIMEOUT);
                    tokio::select! {
                        biased;
                        () = wait_deadline(&*self.clock, Some(deadline)) => {},
                        _ = sink.flush() => {},
                    }
                }
                if close.is_some_and(|close| u16::from(close.code) == 4003) {
                    Err(Exit::Revoked)
                } else {
                    Err(Exit::PeerClosed)
                }
            }
            Message::Frame(_) => Err(Exit::Protocol),
        }
    }

    fn heartbeat_tick(&mut self, phase_limit: Option<(MonoInstant, Exit)>) -> SessionResult<()> {
        if self.heartbeat.awaiting.is_some() {
            return Err(Exit::PongTimeout);
        }
        let bytes = self.heartbeat.serial.to_be_bytes();
        self.heartbeat.start_ping(&*self.clock, bytes);
        self.queue_control_write(
            WriteOp::Send(Message::Ping(bytes.to_vec().into())),
            phase_limit,
        );
        Ok(())
    }

    async fn close(&mut self, code: CloseCode, reason: &'static str) {
        self.active_write = None;
        let Some(mut sink) = self.sink.take() else {
            return;
        };
        let deadline = self.clock.deadline_after(CLOSE_TIMEOUT);
        let message = Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }));
        tokio::select! {
            biased;
            () = wait_deadline(&*self.clock, Some(deadline)) => {},
            _ = sink.send(message) => {},
        }
    }

    fn start_payload(&mut self, epoch: u64, payload: &impl BridgePayload) -> SessionResult<()> {
        let message = encode_payload(epoch, payload)?;
        self.start_write(WriteOp::Send(message), None);
        Ok(())
    }

    #[cfg(test)]
    async fn drive_write_to_completion(
        &mut self,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        let clock = self.clock.clone();
        while self.active_write.is_some() {
            let write_limit = self.write_deadline();
            tokio::select! {
                biased;
                () = wait_stopped(stop) => return Err(Exit::Stopping),
                () = wait_deadline(&*clock, write_limit.map(|(at, _)| at)), if write_limit.is_some() => {
                    return Err(write_limit.map_or(Exit::SendTimeout, |(_, exit)| exit));
                }
                outcome = poll_active_write(&mut self.active_write), if self.active_write.is_some() => {
                    self.finish_write(outcome, None)?;
                }
            }
        }
        Ok(())
    }
}

async fn poll_active_write(active: &mut Option<ActiveWrite>) -> NodeSinkOutcome {
    match active.as_mut() {
        Some(write) => write.future.as_mut().await,
        None => pending().await,
    }
}

fn encode_payload(epoch: u64, payload: &impl BridgePayload) -> SessionResult<Message> {
    let bytes = BridgeFrame::encode_json(epoch, payload).map_err(|_| Exit::Protocol)?;
    let text = String::from_utf8(bytes).map_err(|_| Exit::Protocol)?;
    Ok(Message::Text(text.into()))
}

struct Heartbeat {
    next_ping: MonoInstant,
    awaiting: Option<([u8; 8], MonoInstant)>,
    serial: u64,
}

impl Heartbeat {
    fn new(clock: &dyn MonoClock) -> Self {
        Self {
            next_ping: clock.deadline_after(PING_INTERVAL),
            awaiting: None,
            serial: 1,
        }
    }

    fn deadline(&self) -> MonoInstant {
        self.awaiting
            .map_or(self.next_ping, |(_, deadline)| deadline)
    }

    fn start_ping(&mut self, clock: &dyn MonoClock, payload: [u8; 8]) {
        self.awaiting = Some((payload, clock.deadline_after(PONG_TIMEOUT)));
        self.next_ping = clock.deadline_after(PING_INTERVAL);
        self.serial = self.serial.wrapping_add(1);
    }

    fn pong(&mut self, clock: &dyn MonoClock, data: &[u8]) -> SessionResult<()> {
        if let Some((payload, deadline)) = self.awaiting {
            if clock.is_expired(deadline) {
                return Err(Exit::PongTimeout);
            }
            if data == payload {
                self.awaiting = None;
            }
        }
        Ok(())
    }
}

fn check_epoch(frame: &BridgeFrame, epoch: u64) -> SessionResult<()> {
    if frame.epoch == epoch {
        Ok(())
    } else {
        Err(Exit::EpochMismatch)
    }
}

fn decode<P: BridgePayload>(frame: &BridgeFrame) -> SessionResult<P> {
    frame.decode_payload().map_err(|_| Exit::Protocol)
}

async fn within<T>(
    clock: &dyn MonoClock,
    deadline: MonoInstant,
    on_timeout: Exit,
    stop: &watch::Receiver<bool>,
    operation: impl Future<Output = SessionResult<T>>,
) -> SessionResult<T> {
    tokio::select! {
        biased;
        () = wait_stopped(stop) => Err(Exit::Stopping),
        () = wait_deadline(clock, Some(deadline)) => Err(on_timeout),
        result = operation => result,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn,
        pin::{Pin, pin},
        sync::atomic::{AtomicU64, Ordering},
        task::Poll,
    };

    use tokio::io::AsyncReadExt;

    use super::*;

    #[derive(Default)]
    struct Clock(AtomicU64);

    impl Clock {
        fn at(&self, milliseconds: u64) {
            self.0.store(milliseconds, Ordering::Relaxed);
        }
    }

    impl MonoClock for Clock {
        fn now(&self) -> MonoInstant {
            MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
        }
    }

    #[test]
    fn thread_worker_contention_allows_retry_without_a_participant_idle_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let slots = Arc::new(Semaphore::new(1));
        let mut work = InFlight::new(1, 1);
        let key = (ThreadId::generate()?, ParticipantId::generate()?);
        let old_epoch_slot = slots.clone().try_acquire_owned()?;
        assert!(matches!(
            work.thread_slot(&key, &slots),
            Err(ThreadDeliveryOutcome::Unavailable)
        ));
        drop(old_epoch_slot);
        work.threads.insert(
            key.clone(),
            ThreadWork {
                delivery: OperationId::generate()?,
                admission: AdmissionGuard::new(Duration::from_secs(5)),
            },
        );
        assert!(matches!(
            work.thread_slot(&key, &slots),
            Err(ThreadDeliveryOutcome::Unavailable)
        ));
        work.threads.remove(&key);
        assert!(work.thread_slot(&key, &slots).is_ok());
        Ok(())
    }

    #[test]
    fn transport_heartbeat_requires_matching_pong_in_ten_seconds() {
        let clock = Clock::default();
        let mut heartbeat = Heartbeat::new(&clock);
        assert_eq!(clock.remaining_ms(heartbeat.deadline()), Some(15_000));
        clock.at(15_000);
        let challenge = 9_u64.to_be_bytes();
        heartbeat.start_ping(&clock, challenge);
        assert_eq!(clock.remaining_ms(heartbeat.deadline()), Some(10_000));

        clock.at(24_999);
        assert!(heartbeat.pong(&clock, b"unrelated").is_ok());
        assert_eq!(clock.remaining_ms(heartbeat.deadline()), Some(1));
        assert!(heartbeat.pong(&clock, &challenge).is_ok());
        assert_eq!(clock.remaining_ms(heartbeat.deadline()), Some(5_001));

        clock.at(30_000);
        heartbeat.start_ping(&clock, challenge);
        clock.at(40_000);
        assert!(matches!(
            heartbeat.pong(&clock, &challenge),
            Err(Exit::PongTimeout)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_hello_and_roster_have_independent_deadlines() {
        for (duration, expired) in [
            (CONNECT_TIMEOUT, Exit::ConnectTimeout),
            (ROSTER_TIMEOUT, Exit::RosterTimeout),
        ] {
            let clock = Arc::new(Clock::default());
            let (transport, _peer) = tokio::io::duplex(4096);
            let socket = WebSocketStream::from_raw_socket(
                Box::new(transport) as Box<dyn WebSocketIo>,
                tokio_tungstenite::tungstenite::protocol::Role::Client,
                None,
            )
            .await;
            let mut wire = Wire::new(socket, clock.clone());
            let (_stopping, stop) = watch::channel(false);
            let (result, ()) = tokio::join!(
                wire.next_app(clock.deadline_after(duration), expired, &stop),
                async {
                    tokio::task::yield_now().await;
                    clock.at(duration.as_secs() * 1000);
                    tokio::time::advance(duration).await;
                },
            );
            assert!(result.is_err_and(|failure| failure.reason() == expired.reason()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_application_write_respects_pong_deadline_not_next_ping() {
        // When waiting for Pong, a stalled write must abort at PongTimeout.
        let clock = Arc::new(Clock::default());
        let (mut wire, _peer) = blocked_wire(&clock).await;
        wire.heartbeat.start_ping(&*clock, [0; 8]);
        clock.at(9_999);
        let (_stopping, stop) = watch::channel(false);
        wire.start_write(
            WriteOp::Send(Message::Text("blocked".repeat(10).into())),
            None,
        );
        let mut write = pin!(wire.drive_write_to_completion(&stop));
        assert!(poll_once(write.as_mut()).await.is_pending());
        clock.at(10_000);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            poll_once(write.as_mut()).await,
            Poll::Ready(Err(Exit::PongTimeout))
        ));

        // When NOT waiting for Pong, a write crossing next_ping (15s) keeps its SEND_TIMEOUT
        // budget and succeeds once the peer drains, rather than dropping a healthy session.
        let clock = Arc::new(Clock::default());
        let (mut wire, mut peer) = blocked_wire(&clock).await;
        clock.at(14_999);
        wire.start_write(
            WriteOp::Send(Message::Text("healthy".repeat(10).into())),
            None,
        );
        let drain = {
            let mut write = pin!(wire.drive_write_to_completion(&stop));
            assert!(poll_once(write.as_mut()).await.is_pending());
            clock.at(15_005);
            tokio::time::advance(Duration::from_millis(6)).await;
            assert!(poll_once(write.as_mut()).await.is_pending());
            let drain = tokio::spawn(async move {
                let mut buf = [0u8; 256];
                while let Ok(n) = peer.read(&mut buf).await {
                    if n == 0 {
                        break;
                    }
                }
            });
            assert!(write.await.is_ok());
            drain
        };
        drop(wire);
        let _ = drain.await;
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_websocket_flush_cannot_extend_pong_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(Clock::default());
        let (mut wire, _peer) = blocked_wire(&clock).await;
        wire.heartbeat.start_ping(&*clock, [0; 8]);
        wire.sink
            .as_mut()
            .ok_or("missing sink")?
            .feed(Message::Text("buffered".repeat(10).into()))
            .await?;
        clock.at(9_999);
        let (_stopping, stop) = watch::channel(false);
        wire.start_write(WriteOp::Flush, None);
        let mut flush = pin!(wire.drive_write_to_completion(&stop));
        assert!(poll_once(flush.as_mut()).await.is_pending());
        clock.at(10_000);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(matches!(
            poll_once(flush.as_mut()).await,
            Poll::Ready(Err(Exit::PongTimeout))
        ));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn time_waiting_to_write_ping_consumes_its_pong_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(Clock::default());
        let (mut wire, mut peer) = blocked_wire(&clock).await;
        clock.at(15_000);
        let (_stopping, stop) = watch::channel(false);
        wire.heartbeat_tick(None).map_err(|e| e.reason())?;
        {
            let mut sending = pin!(wire.drive_write_to_completion(&stop));
            assert!(poll_once(sending.as_mut()).await.is_pending());

            clock.at(23_000);
            tokio::time::advance(Duration::from_secs(8)).await;
            let mut prefix = [0; 8];
            let mut read = pin!(peer.read_exact(&mut prefix));
            assert!(matches!(poll_once(read.as_mut()).await, Poll::Ready(Ok(8))));
            assert!(matches!(
                poll_once(sending.as_mut()).await,
                Poll::Ready(Ok(()))
            ));
        }
        assert_eq!(clock.remaining_ms(wire.heartbeat.deadline()), Some(2_000));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn close_sends_frame_even_after_pong_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(Clock::default());
        let (transport, mut peer) = tokio::io::duplex(4096);
        let socket = WebSocketStream::from_raw_socket(
            Box::new(transport) as Box<dyn WebSocketIo>,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut wire = Wire::new(socket, clock.clone());
        wire.heartbeat.start_ping(&*clock, [0; 8]);
        clock.at(10_000);
        wire.close(CloseCode::Away, "pong_timeout").await;
        let mut bytes = [0u8; 64];
        let n = peer.read(&mut bytes).await?;
        assert!(
            n > 0,
            "expected RFC 6455 close frame bytes after PongTimeout"
        );
        Ok(())
    }

    #[tokio::test]
    async fn full_duplex_large_frames_do_not_deadlock() -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(Clock::default());
        let (a, b) = tokio::io::duplex(4096);
        let ws_cfg = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(MAX_WS_FRAME_BYTES * 2)
            .max_message_size(Some(MAX_WS_FRAME_BYTES))
            .max_frame_size(Some(MAX_WS_FRAME_BYTES));
        let client_ws = WebSocketStream::from_raw_socket(
            Box::new(a) as Box<dyn WebSocketIo>,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            Some(ws_cfg),
        )
        .await;
        let server_ws = WebSocketStream::from_raw_socket(
            Box::new(b) as Box<dyn WebSocketIo>,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            Some(ws_cfg),
        )
        .await;
        let mut client = Wire::new(client_ws, clock.clone());
        let mut server = Wire::new(server_ws, clock);
        let payload = "x".repeat(200 * 1024);
        client.start_write(WriteOp::Send(Message::Text(payload.clone().into())), None);
        server.start_write(WriteOp::Send(Message::Text(payload.clone().into())), None);

        let drive = async {
            let mut client_got = None;
            let mut server_got = None;
            while client.active_write.is_some()
                || server.active_write.is_some()
                || client_got.is_none()
                || server_got.is_none()
            {
                tokio::select! {
                    outcome = poll_active_write(&mut client.active_write), if client.active_write.is_some() => {
                        client.finish_write(outcome, None).map_err(|e| e.reason())?;
                    }
                    outcome = poll_active_write(&mut server.active_write), if server.active_write.is_some() => {
                        server.finish_write(outcome, None).map_err(|e| e.reason())?;
                    }
                    msg = client.stream.next(), if client_got.is_none() => {
                        if let Some(Ok(Message::Text(text))) = msg {
                            client_got = Some(text.len());
                        }
                    }
                    msg = server.stream.next(), if server_got.is_none() => {
                        if let Some(Ok(Message::Text(text))) = msg {
                            server_got = Some(text.len());
                        }
                    }
                }
            }
            Ok::<_, &'static str>((client_got, server_got))
        };
        let (c_len, s_len) = tokio::time::timeout(Duration::from_secs(5), drive)
            .await
            .map_err(|_| "full duplex deadlocked")??;
        assert_eq!(c_len, Some(payload.len()));
        assert_eq!(s_len, Some(payload.len()));
        Ok(())
    }

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
    }

    async fn blocked_wire(clock: &Arc<Clock>) -> (Wire, tokio::io::DuplexStream) {
        let (transport, peer) = tokio::io::duplex(8);
        let socket = WebSocketStream::from_raw_socket(
            Box::new(transport) as Box<dyn WebSocketIo>,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        (Wire::new(socket, clock.clone()), peer)
    }

    #[test]
    fn terminal_upgrade_errors_do_not_depend_on_body_framing() -> Result<(), http::Error> {
        for (status, code, matches) in [
            (StatusCode::UNAUTHORIZED, ErrorCode::NodeRevoked, true),
            (StatusCode::CONFLICT, ErrorCode::HubIdentityMismatch, true),
            (StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized, false),
            (StatusCode::CONFLICT, ErrorCode::NodeRevoked, false),
        ] {
            let response = http::Response::builder()
                .status(status)
                .header(BRIDGE_ERROR_HEADER, code.as_str())
                .body(None)?;
            let exit = ws_error(WsError::Http(Box::new(response)));
            assert_eq!(exit.terminal_code(), matches.then_some(code));
        }
        Ok(())
    }
}
