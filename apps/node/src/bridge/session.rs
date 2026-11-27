use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::{Future, ready},
    path::Path,
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
use futures_util::{SinkExt, StreamExt};
use http::{
    HeaderValue, StatusCode,
    header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{Semaphore, mpsc, watch},
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
const OUTBOX_CAPACITY: usize = 64;

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
        let (outbox, input) = mpsc::channel(OUTBOX_CAPACITY);
        self.bridge
            .session
            .send_replace(Some(ReverseHandle::new(epoch, outbox.clone())));

        let notices = self.state.db.subscribe();
        let max_commands = (hello.max_inflight_commands as usize).min(MAX_COMMANDS);
        let max_threads = (hello.max_inflight_threads as usize).min(MAX_THREAD_DELIVERIES);
        let (cursor, participants) = match self
            .sync_roster(io, epoch, hello, roster_deadline, stop)
            .await
        {
            Ok(cursor) => cursor,
            Err(exit) => return (false, exit),
        };

        let (event_tx, event_rx) = mpsc::channel(1);
        let reads = self.state.reads.clone();
        let local_id = self.state.info.node_id.clone();
        self.bridge.spawn_worker(async move {
            if let Err(error) =
                events::pump(reads, local_id, notices, cursor, participants, event_tx).await
            {
                tracing::warn!(code = %error.code(), "Could not forward Node events");
            }
        });
        let mut online = Online {
            epoch,
            outbox,
            input,
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

        let mut frames: VecDeque<_> = roster.frames.into();
        while !frames.is_empty() {
            let heartbeat = io.heartbeat.deadline();
            tokio::select! {
                biased;
                () = wait_stopped(stop) => return Err(Exit::Stopping),
                () = wait_deadline(&*self.bridge.clock, Some(deadline)) => return Err(Exit::RosterTimeout),
                () = wait_deadline(&*self.bridge.clock, Some(heartbeat)) => {
                    within(&*self.bridge.clock, deadline, Exit::RosterTimeout, stop, io.heartbeat(stop)).await?;
                }
                message = io.socket.next() => {
                    let frame = within(
                        &*self.bridge.clock, deadline, Exit::RosterTimeout, stop,
                        io.received(message, stop),
                    ).await?;
                    if let Some(frame) = frame {
                        return Err(if frame.epoch == epoch { Exit::Protocol } else { Exit::EpochMismatch });
                    }
                }
                () = ready(()) => {
                    if let Some(frame) = frames.pop_front() {
                        io.send(
                            Message::Text(frame.into()),
                            Some((deadline, Exit::RosterTimeout)),
                            stop,
                        ).await?;
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
        if self.bridge.clock.is_expired(io.heartbeat.deadline()) {
            io.heartbeat(stop).await?;
        }
        online.requests.expire(&*self.bridge.clock);
        let heartbeat = io.heartbeat.deadline();
        let correlation = online.requests.earliest_deadline(&*self.bridge.clock);
        let ready_cursor = online.events.ready_cursor();
        let can_send_request = online.requests.has_ready(ready_cursor);
        tokio::select! {
            () = wait_stopped(stop) => return Err(Exit::Stopping),
            () = wait_deadline(&*self.bridge.clock, Some(heartbeat)) => io.heartbeat(stop).await?,
            () = wait_deadline(&*self.bridge.clock, correlation) => online.requests.expire(&*self.bridge.clock),
            socket_message = io.socket.next() => {
                if let Some(frame) = io.received(socket_message, stop).await? {
                    self.handle(online, io, frame, stop).await?;
                }
            }
            outbound = online.input.recv() => {
                if let Some(outbound) = outbound {
                    online.handle_outbound(io, outbound, stop).await?;
                }
            }
            batch = online.events.input.recv(), if online.events.pending.is_empty() => {
                online.events.batch(batch.ok_or(Exit::EventProjection)?);
            }
            () = ready(()), if !online.events.pending.is_empty() => {
                if let Some(event) = online.events.pending.pop_front() {
                    io.send_payload(online.epoch, &event, stop).await?;
                }
            }
            () = ready(()), if can_send_request => {
                if let Some(mut request) = online.requests.take_ready(ready_cursor)
                    && request.mark_sent(&*self.bridge.clock)
                {
                    io.send(
                        Message::Text(request.take_frame().into()),
                        Some((request.deadline(), Exit::SendTimeout)),
                        stop,
                    ).await?;
                    online.requests.sent(request);
                }
            }
        }
        Ok(())
    }

    async fn handle(
        &self,
        online: &mut Online,
        io: &mut Wire,
        frame: BridgeFrame,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        check_epoch(&frame, online.epoch)?;
        match frame.kind {
            FrameKind::CommandReq => {
                let request = decode::<CommandReqPayload>(&frame)?;
                let admission = AdmissionGuard::with_clock(
                    Duration::from_millis(request.admission_budget_ms),
                    self.bridge.clock.clone(),
                );
                if let Some(rejected) = self.dispatch_command(online, request, admission) {
                    io.send_payload(online.epoch, &rejected, stop).await?;
                }
            }
            FrameKind::ThreadDeliver => {
                let request = decode::<ThreadDeliverPayload>(&frame)?;
                let admission = AdmissionGuard::with_clock(
                    Duration::from_millis(request.admission_budget_ms),
                    self.bridge.clock.clone(),
                );
                if let Some(rejected) = self.dispatch_thread(online, request, admission) {
                    io.send_payload(online.epoch, &rejected, stop).await?;
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
            let _ = outbox.try_send(Outbound::Command(reply));
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
        let slot = (online.work.threads.len() < online.work.max_threads
            && !online.work.threads.contains_key(&key))
        .then(|| self.limits.threads.clone().try_acquire_owned().ok())
        .flatten();
        let Some(slot) = slot else {
            return Some(ThreadAckPayload {
                delivery_id: request.delivery_id,
                thread_id: request.thread_id,
                member_id: request.member_id,
                delivery_generation: request.delivery_generation,
                acked_seq: request.to_seq,
                outcome: ThreadDeliveryOutcome::Busy,
                detail: Some("thread_delivery_in_flight".into()),
            });
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
            let _ = outbox.try_send(Outbound::Thread(reply));
        });
        None
    }
}

struct Online {
    epoch: u64,
    outbox: mpsc::Sender<Outbound>,
    input: mpsc::Receiver<Outbound>,
    work: InFlight,
    events: CatchUp,
    requests: ReverseRequests,
}

impl Online {
    async fn handle_outbound(
        &mut self,
        io: &mut Wire,
        outbound: Outbound,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        match outbound {
            Outbound::Reverse(request) => self.requests.enqueue(request),
            Outbound::Command(reply) => {
                self.work.commands.remove(&reply.op_id);
                io.send_payload(self.epoch, &reply, stop).await?;
            }
            Outbound::Thread(reply) => {
                self.work
                    .threads
                    .remove(&(reply.thread_id.clone(), reply.member_id.clone()));
                io.send_payload(self.epoch, &reply, stop).await?;
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

struct Wire {
    socket: WebSocket,
    heartbeat: Heartbeat,
    clock: Arc<dyn MonoClock>,
}

impl Wire {
    fn new(socket: WebSocket, clock: Arc<dyn MonoClock>) -> Self {
        Self {
            socket,
            heartbeat: Heartbeat::new(&*clock),
            clock,
        }
    }

    async fn next_app(
        &mut self,
        deadline: MonoInstant,
        phase_timeout: Exit,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<BridgeFrame> {
        let clock = self.clock.clone();
        loop {
            let heartbeat = self.heartbeat.deadline();
            tokio::select! {
                biased;
                () = wait_stopped(stop) => return Err(Exit::Stopping),
                () = wait_deadline(&*clock, Some(deadline)) => return Err(phase_timeout),
                () = wait_deadline(&*clock, Some(heartbeat)) => {
                    within(&*clock, deadline, phase_timeout, stop, self.heartbeat(stop)).await?;
                }
                input = self.socket.next() => {
                    let frame = within(&*clock, deadline, phase_timeout, stop, self.received(input, stop)).await?;
                    if let Some(frame) = frame {
                        return Ok(frame);
                    }
                }
            }
        }
    }

    async fn received(
        &mut self,
        input: Option<Result<Message, WsError>>,
        stop: &watch::Receiver<bool>,
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
                self.flush(SEND_TIMEOUT, stop).await?;
                Ok(None)
            }
            Message::Close(close) => {
                // Send the automatically queued close reply before dropping the stream.
                let _ = self.flush(CLOSE_TIMEOUT, stop).await;
                if close.is_some_and(|close| u16::from(close.code) == 4003) {
                    Err(Exit::Revoked)
                } else {
                    Err(Exit::PeerClosed)
                }
            }
            Message::Frame(_) => Err(Exit::Protocol),
        }
    }

    async fn heartbeat(&mut self, stop: &watch::Receiver<bool>) -> SessionResult<()> {
        if self.heartbeat.awaiting.is_some() {
            return Err(Exit::PongTimeout);
        }
        let bytes = self.heartbeat.serial.to_be_bytes();
        self.heartbeat.start_ping(&*self.clock, bytes);
        self.send(Message::Ping(bytes.to_vec().into()), None, stop)
            .await
    }

    async fn send(
        &mut self,
        message: Message,
        limit: Option<(MonoInstant, Exit)>,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        let (deadline, on_timeout) = self.write_deadline(SEND_TIMEOUT, limit);
        within(&*self.clock, deadline, on_timeout, stop, async {
            self.socket.send(message).await.map_err(ws_error)
        })
        .await
    }

    async fn flush(
        &mut self,
        timeout: Duration,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        let (deadline, on_timeout) = self.write_deadline(timeout, None);
        within(&*self.clock, deadline, on_timeout, stop, async {
            self.socket.flush().await.map_err(ws_error)
        })
        .await
    }

    // Cancel a stalled write when the next Ping is due, or when its expected
    // Pong has expired. A partially written frame cannot be interrupted and
    // resumed later without continuing its socket write.
    fn write_deadline(
        &self,
        timeout: Duration,
        limit: Option<(MonoInstant, Exit)>,
    ) -> (MonoInstant, Exit) {
        let heartbeat = self.heartbeat.deadline();
        let local = self.clock.deadline_after(timeout);
        let mut bound = if local.is_earlier_than(heartbeat) {
            (local, Exit::SendTimeout)
        } else {
            let on_timeout = if self.heartbeat.awaiting.is_some() {
                Exit::PongTimeout
            } else {
                Exit::SendTimeout
            };
            (heartbeat, on_timeout)
        };
        if let Some(limit) = limit
            && limit.0.is_earlier_than(bound.0)
        {
            bound = limit;
        }
        bound
    }

    async fn close(&mut self, code: CloseCode, reason: &'static str) {
        let (deadline, _) = self.write_deadline(CLOSE_TIMEOUT, None);
        let message = Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }));
        tokio::select! {
            biased;
            () = wait_deadline(&*self.clock, Some(deadline)) => {},
            _ = self.socket.send(message) => {},
        }
    }

    async fn send_payload(
        &mut self,
        epoch: u64,
        payload: &impl BridgePayload,
        stop: &watch::Receiver<bool>,
    ) -> SessionResult<()> {
        let bytes = BridgeFrame::encode_json(epoch, payload).map_err(|_| Exit::Protocol)?;
        let text = String::from_utf8(bytes).map_err(|_| Exit::Protocol)?;
        self.send(Message::Text(text.into()), None, stop).await
    }
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
        result = operation => if clock.is_expired(deadline) {
            Err(on_timeout)
        } else {
            result
        },
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
    async fn blocked_application_write_cannot_cross_heartbeat_deadline() {
        for waiting_for_pong in [false, true] {
            let clock = Arc::new(Clock::default());
            let (mut wire, _peer) = blocked_wire(&clock).await;
            let (until, reason) = if waiting_for_pong {
                wire.heartbeat.start_ping(&*clock, [0; 8]);
                (10_000, Exit::PongTimeout)
            } else {
                (15_000, Exit::SendTimeout)
            };
            clock.at(until - 1);
            let (_stopping, stop) = watch::channel(false);
            let mut write =
                pin!(wire.send(Message::Text("blocked".repeat(10).into()), None, &stop,));
            assert!(poll_once(write.as_mut()).await.is_pending());
            clock.at(until);
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(matches!(
                poll_once(write.as_mut()).await,
                Poll::Ready(Err(failure)) if failure.reason() == reason.reason()
            ));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_websocket_flush_cannot_extend_pong_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        let clock = Arc::new(Clock::default());
        let (mut wire, _peer) = blocked_wire(&clock).await;
        wire.heartbeat.start_ping(&*clock, [0; 8]);
        wire.socket
            .feed(Message::Text("buffered".repeat(10).into()))
            .await?;
        clock.at(9_999);
        let (_stopping, stop) = watch::channel(false);
        let mut flush = pin!(wire.flush(SEND_TIMEOUT, &stop));
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
        {
            let mut sending = pin!(wire.heartbeat(&stop));
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
