use std::{
    fs, io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as sync_mpsc,
    },
};

use cooplet_protocol::{
    AttachmentRef, BRIDGE_SUBPROTOCOL, BridgeCommand, CommandOutcome, CommandReqPayload,
    DeliveryMode, EventKind, HubId, MemberView, NodeId, ParticipantKind, ParticipantState,
    ParticipantWire, ThreadDeliveryOutcome, ThreadMemberState,
};
use cooplet_storage::{StorageConfig, StorageEngine, WriteOutcome};
use futures_util::StreamExt;
use http::header;
use rusqlite::params;
use serde_json::json;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message as ClientMessage, client::IntoClientRequest},
};

use crate::{
    auth::{AuthenticatedCaller, authenticate},
    bridge::{BridgeRegistry, ReserveError, SubmitError},
    credentials::mint_node,
    http::{RequestLimits, router},
    storage,
};

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;
type CommandResult = JoinHandle<Result<CommandResPayload, SubmitError>>;
const TIMEOUT: Duration = Duration::from_secs(2);
const PROBE: &[u8] = b"clock-probe";

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct Clock(AtomicU64);

impl MonoClock for Clock {
    fn now(&self) -> MonoInstant {
        MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
    }
}

struct Fixture {
    state: AppState,
    registry: Arc<LiveBridge>,
    clock: Arc<Clock>,
    node_id: NodeId,
    server: JoinHandle<io::Result<()>>,
    engine: StorageEngine,
    shutdown: watch::Sender<()>,
    _scratch: Scratch,
}

impl Fixture {
    async fn open() -> TestResult<(Self, Client, u64)> {
        let scratch = Scratch(
            std::env::temp_dir().join(format!("cooplet-session-test-{}", HubId::generate()?)),
        );
        storage::init(&scratch.0, "Bridge Test").await?;
        let storage::HubStorage {
            engine,
            meta,
            master,
            thread_acl_watch,
        } = storage::open(&scratch.0, 1024, 16_384, 16_384).await?;
        let (shutdown, stop) = watch::channel(());
        let clock = Arc::new(Clock::default());
        let registry = Arc::new(LiveBridge::new(1, clock.clone()));
        let state = AppState::new(
            &engine,
            meta,
            master,
            thread_acl_watch,
            registry.clone(),
            stop,
            1024,
        )?;
        let node_id = NodeId::generate()?;
        let credential = mint_node(&state.master, &node_id, 1)?;
        let id = node_id.clone();
        let bearer = credential.secret.into_string();
        let verifier = credential.verifier;
        state.db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO nodes
                   (node_id, display_name, owner_principal_id, credential_hmac, status, enrolled_at_ms)
                 VALUES (?1, 'Worker', (SELECT principal_id FROM principals LIMIT 1),
                        ?2, 'offline', 0)",
                params![id.as_str(), verifier],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let app = router(state.clone(), RequestLimits::default());
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let mut request = format!("ws://{address}/v1/bridge/connect").into_client_request()?;
        request
            .headers_mut()
            .insert("x-cooplet-hub", state.meta.hub_id.as_str().parse()?);
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, format!("Bearer {bearer}").parse()?);
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_PROTOCOL, BRIDGE_SUBPROTOCOL.parse()?);
        let (mut client, _) = time::timeout(TIMEOUT, connect_async(request)).await??;
        let frame = next_frame(&mut client).await?;
        let _: HelloAckPayload = frame.decode_payload()?;
        Ok((
            Self {
                state,
                registry,
                clock,
                node_id,
                server,
                engine,
                shutdown,
                _scratch: scratch,
            },
            client,
            frame.epoch,
        ))
    }

    async fn close(self) -> TestResult {
        self.state.background.close();
        self.shutdown.send_replace(());
        self.registry.shutdown();
        self.server.abort();
        let _ = self.server.await;
        time::timeout(TIMEOUT, self.registry.drain()).await?;
        time::timeout(TIMEOUT, self.state.background.drain()).await?;
        self.engine.shutdown().await?;
        Ok(())
    }

    async fn block_writer(&self) -> TestResult<WriterBarrier> {
        let (entered, wait) = oneshot::channel();
        let (release, blocked) = sync_mpsc::channel();
        let db = self.state.db.clone();
        let job = tokio::spawn(async move {
            db.write_sqlite(move |_| {
                let _ = entered.send(());
                blocked
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                Ok(WriteOutcome::new(()))
            })
            .await
        });
        time::timeout(TIMEOUT, wait).await??;
        Ok(WriterBarrier { release, job })
    }

    async fn heartbeat(&self, client: &mut Client, at_ms: u64, hub_ping: bool) -> TestResult {
        self.clock.0.store(at_ms, Ordering::Relaxed);
        client
            .send(ClientMessage::Ping(Bytes::from_static(PROBE)))
            .await?;
        let mut answered_probe = false;
        let mut saw_hub_ping = false;
        while !answered_probe || hub_ping && !saw_hub_ping {
            match next_message(client).await? {
                ClientMessage::Pong(bytes) if bytes == PROBE => answered_probe = true,
                ClientMessage::Ping(bytes) => {
                    saw_hub_ping = true;
                    client.send(ClientMessage::Pong(bytes)).await?;
                }
                other => {
                    return Err(format!("expected transport control frame, got {other:?}").into());
                }
            }
        }
        if saw_hub_ping {
            // The second probe orders the Hub's receipt of our Pong before the
            // next monotonic time advance.
            client
                .send(ClientMessage::Ping(Bytes::from_static(PROBE)))
                .await?;
            assert_eq!(
                next_message(client).await?,
                ClientMessage::Pong(Bytes::from_static(PROBE))
            );
        }
        assert_eq!(saw_hub_ping, hub_ping);
        Ok(())
    }

    fn command(&self, budget_ms: u64) -> TestResult<(OperationId, CommandResult)> {
        let permit = self
            .registry
            .reserve(&self.node_id)
            .map_err(|err| format!("bridge command unavailable: {err:?}"))?;
        let payload = command(budget_ms)?;
        Ok((payload.op_id.clone(), tokio::spawn(permit.submit(payload))))
    }

    async fn status(&self) -> TestResult<String> {
        let node = self.node_id.clone();
        Ok(self
            .state
            .reads
            .read(move |conn| {
                Ok(conn.query_row(
                    "SELECT status FROM nodes WHERE node_id = ?1",
                    [node.as_str()],
                    |row| row.get(0),
                )?)
            })
            .await?)
    }

    async fn admin(&self) -> TestResult<AuthenticatedCaller> {
        let bytes = cooplet_platform::PrivateFs::read_private_file(
            &self._scratch.0.join("admin.token"),
            128,
        )?;
        let token = std::str::from_utf8(&bytes)?.trim();
        Ok(authenticate(&self.state.reads, &self.state.master, token).await?)
    }

    async fn board(
        &self,
        caller: &AuthenticatedCaller,
        participant: &ParticipantId,
        suffix: &str,
        mode: DeliveryMode,
    ) -> TestResult<ThreadId> {
        let member_key = format!("{}/{participant}", self.node_id);
        let body = serde_json::from_value(json!({
            "title":"Thread timing",
            "members":[{"member_key":member_key,"handle":"coordinator","delivery_mode":mode}],
        }))?;
        Ok(
            threads::create(&self.state, caller, &format!("thread-{suffix}"), body)
                .await?
                .thread_id,
        )
    }

    async fn post(
        &self,
        caller: &AuthenticatedCaller,
        board: &ThreadId,
        suffix: &str,
        attachments: Vec<AttachmentRef>,
    ) -> TestResult {
        let body = serde_json::from_value(json!({
            "body":"@coordinator review","attachments":attachments,
        }))?;
        threads::post(
            &self.state,
            caller,
            board.clone(),
            &format!("post-{suffix}"),
            body,
        )
        .await?;
        Ok(())
    }

    async fn member(
        &self,
        caller: &AuthenticatedCaller,
        board: &ThreadId,
        participant: &ParticipantId,
    ) -> TestResult<MemberView> {
        let page =
            threads::list_members(&self.state, caller, board.clone(), Default::default()).await?;
        Ok(page
            .items
            .into_iter()
            .find(|view| view.participant_id == participant.as_str())
            .ok_or("thread member missing")?)
    }

    async fn wait_member(
        &self,
        caller: &AuthenticatedCaller,
        board: &ThreadId,
        participant: &ParticipantId,
        predicate: impl Fn(&MemberView) -> bool,
    ) -> TestResult<MemberView> {
        let mut updates = self.state.db.subscribe();
        time::timeout(TIMEOUT, async {
            loop {
                let member = self.member(caller, board, participant).await?;
                if predicate(&member) {
                    return Ok(member);
                }
                let _ = updates.recv().await?;
            }
        })
        .await?
    }
}

struct WriterBarrier {
    release: sync_mpsc::Sender<()>,
    job: JoinHandle<Result<(), cooplet_storage::StorageError>>,
}

impl WriterBarrier {
    async fn release(self) -> TestResult {
        self.release.send(())?;
        self.job.await??;
        Ok(())
    }
}

async fn next_message(client: &mut Client) -> TestResult<ClientMessage> {
    Ok(time::timeout(TIMEOUT, client.next())
        .await?
        .ok_or("bridge closed")??)
}

async fn next_frame(client: &mut Client) -> TestResult<BridgeFrame> {
    let ClientMessage::Text(text) = next_message(client).await? else {
        return Err("expected an application text frame".into());
    };
    Ok(BridgeFrame::from_json(text.as_bytes())?)
}

async fn send_payload(client: &mut Client, epoch: u64, payload: &impl BridgePayload) -> TestResult {
    let wire = String::from_utf8(BridgeFrame::encode_json(epoch, payload)?)?;
    client.send(ClientMessage::Text(wire.into())).await?;
    Ok(())
}

async fn finish_roster(client: &mut Client, epoch: u64) -> TestResult {
    send_roster(client, epoch, Vec::new()).await
}

async fn send_roster(
    client: &mut Client,
    epoch: u64,
    participants: Vec<ParticipantWire>,
) -> TestResult {
    send_payload(
        client,
        epoch,
        &RosterSyncPayload {
            chunk_index: 0,
            is_final: true,
            participants,
            templates: Vec::new(),
            workflows: Vec::new(),
            unresolved_thread_fences: Vec::new(),
            resolved_op_receipts: Vec::new(),
        },
    )
    .await
}

async fn ready_agent(
    client: &mut Client,
    epoch: u64,
    state: ParticipantState,
) -> TestResult<ParticipantId> {
    let participant = agent("coordinator", state)?;
    let id = participant.participant_id.clone();
    send_roster(client, epoch, vec![participant]).await?;
    expect_ready(client).await?;
    Ok(id)
}

fn agent(name: &str, state: ParticipantState) -> TestResult<ParticipantWire> {
    Ok(ParticipantWire {
        participant_id: ParticipantId::generate()?,
        kind: ParticipantKind::Agent,
        name: name.into(),
        client_kind: Some("mock".into()),
        workspace_dir: None,
        state,
        capabilities: json!({}),
        wait_kind: None,
        approval_id: None,
        action_hint: None,
        summary: None,
        can_respond_remotely: None,
    })
}

async fn expect_ready(client: &mut Client) -> TestResult {
    let _: RosterReadyPayload = next_frame(client).await?.decode_payload()?;
    Ok(())
}

async fn acknowledge(
    client: &mut Client,
    epoch: u64,
    id: OperationId,
    response: CommandResult,
) -> TestResult {
    send_payload(
        client,
        epoch,
        &CommandResPayload {
            op_id: id.clone(),
            outcome: CommandOutcome::Succeeded,
            resource_id: None,
            result: None,
            error: None,
        },
    )
    .await?;
    let resolved = time::timeout(TIMEOUT, response)
        .await??
        .map_err(|_| "healthy bridge lost its command")?;
    assert_eq!(resolved.op_id, id);
    Ok(())
}

fn command(budget_ms: u64) -> TestResult<CommandReqPayload> {
    Ok(CommandReqPayload {
        op_id: OperationId::generate()?,
        actor_id: "command-caller".into(),
        command: BridgeCommand::InspectAgent {
            participant_id: ParticipantId::generate()?,
        },
        delivery_mode: DeliveryMode::DirectNow,
        admission_budget_ms: budget_ms,
        ttl_ms: None,
    })
}

#[tokio::test]
async fn final_roster_wait_keeps_ping_and_pong_live_during_a_writer_stall() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    let writer = hub.block_writer().await?;
    finish_roster(&mut client, epoch).await?;
    hub.heartbeat(&mut client, 15_000, true).await?;
    hub.heartbeat(&mut client, 26_000, false).await?;
    assert_eq!(hub.status().await?, "offline");
    writer.release().await?;
    expect_ready(&mut client).await?;
    assert_eq!(hub.status().await?, "online");
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn authority_transition_gates_queued_commands_and_roster_ready() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    let transition = hub.registry.transition(&hub.node_id).await;
    finish_roster(&mut client, epoch).await?;
    hub.heartbeat(&mut client, 15_000, true).await?;
    assert!(matches!(
        hub.registry.reserve(&hub.node_id),
        Err(ReserveError::Offline)
    ));
    drop(transition);
    expect_ready(&mut client).await?;

    // The caller already holds a reservation from the current epoch. Beginning
    // a transition still prevents the socket from starting its frame.
    hub.heartbeat(&mut client, 15_000, false).await?;
    let permit = hub
        .registry
        .reserve(&hub.node_id)
        .map_err(|_| "missing ready bridge")?;
    let transition = hub.registry.transition(&hub.node_id).await;
    assert!(matches!(
        hub.registry.reserve(&hub.node_id),
        Err(ReserveError::Offline)
    ));
    let revoked = hub.node_id.clone();
    hub.state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE nodes SET status = 'revoked', revoked_at_ms = 1 WHERE node_id = ?1",
                [revoked.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    // Keep the transition held after the durable change while the registry
    // still has the old session. Its permit must not start a write.
    let result = permit.submit(command(5000)?);
    assert!(matches!(
        time::timeout(TIMEOUT, result).await?,
        Err(SubmitError::Unavailable)
    ));
    hub.registry.disconnect_revoked(&hub.node_id);
    drop(transition);
    let ClientMessage::Close(Some(close)) = next_message(&mut client).await? else {
        return Err("expected node revocation close".into());
    };
    assert_eq!(u16::from(close.code), 4003);
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn command_ack_deadlines_are_40_and_65_seconds_without_closing_the_socket() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    finish_roster(&mut client, epoch).await?;
    expect_ready(&mut client).await?;
    hub.heartbeat(&mut client, 0, false).await?;
    let (short_id, short) = hub.command(5000)?;
    let request: CommandReqPayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(request.op_id, short_id);
    assert_eq!(request.admission_budget_ms, 5000);
    let (short_edge_id, short_edge) = hub.command(5000)?;
    let request: CommandReqPayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(request.op_id, short_edge_id);
    for now in [15_000, 30_000] {
        hub.heartbeat(&mut client, now, true).await?;
    }
    hub.heartbeat(&mut client, 39_999, false).await?;
    acknowledge(&mut client, epoch, short_edge_id, short_edge).await?;
    hub.heartbeat(&mut client, 40_000, false).await?;
    assert!(matches!(
        time::timeout(TIMEOUT, short).await??,
        Err(SubmitError::Unknown)
    ));

    let (long_id, long) = hub.command(30_000)?;
    let request: CommandReqPayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(request.op_id, long_id);
    assert_eq!(request.admission_budget_ms, 30_000);
    let (long_edge_id, long_edge) = hub.command(30_000)?;
    let request: CommandReqPayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(request.op_id, long_edge_id);
    for now in [45_000, 60_000, 75_000, 90_000] {
        hub.heartbeat(&mut client, now, true).await?;
    }
    hub.heartbeat(&mut client, 104_999, false).await?;
    acknowledge(&mut client, epoch, long_edge_id, long_edge).await?;
    hub.heartbeat(&mut client, 105_000, true).await?;
    assert!(matches!(
        time::timeout(TIMEOUT, long).await??,
        Err(SubmitError::Unknown)
    ));

    let (id, later) = hub.command(5000)?;
    let request: CommandReqPayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(request.op_id, id);
    acknowledge(&mut client, epoch, id, later).await?;
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn disconnect_catalog_update_waits_for_full_actor_inbox() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    finish_roster(&mut client, epoch).await?;
    expect_ready(&mut client).await?;
    let writer = hub.block_writer().await?;
    let db = &hub.state.db;
    let mut queued = (0..StorageConfig::new("hub.sqlite3").write_queue_capacity)
        .map(|_| async { db.write_sqlite(|_| Ok(WriteOutcome::new(()))).await })
        .map(Box::pin)
        .collect::<Vec<_>>();
    std::future::poll_fn(|cx| {
        for write in &mut queued {
            assert!(write.as_mut().poll(cx).is_pending());
        }
        std::task::Poll::Ready(())
    })
    .await;
    assert!(matches!(
        db.write_sqlite(|_| Ok(WriteOutcome::new(()))).await,
        Err(cooplet_storage::StorageError::Overloaded)
    ));

    client.send(ClientMessage::Close(None)).await?;
    assert!(matches!(
        next_message(&mut client).await?,
        ClientMessage::Close(_)
    ));
    assert_eq!(hub.status().await?, "online");
    writer.release().await?;
    for write in queued {
        write.await?;
    }
    time::timeout(TIMEOUT, hub.registry.drain()).await?;
    assert_eq!(hub.status().await?, "offline");
    hub.close().await
}

#[tokio::test]
async fn thread_ack_deadlines_pause_only_the_matching_ranges_on_a_live_socket() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    let first = agent("text-worker", ParticipantState::Idle)?;
    let second = agent("file-worker", ParticipantState::Idle)?;
    let text_worker = first.participant_id.clone();
    let file_worker = second.participant_id.clone();
    send_roster(&mut client, epoch, vec![first, second]).await?;
    expect_ready(&mut client).await?;
    let caller = hub.admin().await?;
    let short = hub
        .board(
            &caller,
            &text_worker,
            "short-ack",
            DeliveryMode::QueueUntilIdle,
        )
        .await?;
    let long = hub
        .board(
            &caller,
            &file_worker,
            "long-ack",
            DeliveryMode::QueueUntilIdle,
        )
        .await?;
    let mut headers = http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        http::HeaderValue::from_static("text/plain"),
    );
    headers.insert(
        "x-cooplet-filename",
        http::HeaderValue::from_static("note.txt"),
    );
    let (_, attachment) = crate::blobs::upload(
        &hub.state,
        &caller,
        &headers,
        axum::body::Body::from("A note for the coordinator"),
    )
    .await?;

    hub.post(&caller, &short, "short-ack", Vec::new()).await?;
    let short_wire: cooplet_protocol::ThreadDeliverPayload =
        next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(short_wire.thread_id, short);
    assert_eq!(short_wire.admission_budget_ms, 5_000);
    hub.post(&caller, &long, "long-ack", vec![attachment])
        .await?;
    let long_wire: cooplet_protocol::ThreadDeliverPayload =
        next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(long_wire.thread_id, long);
    assert_eq!(long_wire.admission_budget_ms, 30_000);
    assert!(!long_wire.posts[0].attachments.is_empty());

    for now in [15_000, 30_000] {
        hub.heartbeat(&mut client, now, true).await?;
    }
    hub.heartbeat(&mut client, 39_999, false).await?;
    assert_eq!(
        hub.member(&caller, &short, &text_worker).await?.state,
        ThreadMemberState::Active
    );
    hub.heartbeat(&mut client, 40_000, false).await?;
    let paused = hub
        .wait_member(&caller, &short, &text_worker, |member| {
            member.state == ThreadMemberState::PausedUnknown
        })
        .await?;
    assert_pending_range(&paused, &short_wire)?;
    assert_eq!(
        hub.member(&caller, &long, &file_worker).await?.state,
        ThreadMemberState::Active
    );
    for now in [45_000, 60_000] {
        hub.heartbeat(&mut client, now, true).await?;
    }
    hub.heartbeat(&mut client, 64_999, false).await?;
    assert_eq!(
        hub.member(&caller, &long, &file_worker).await?.state,
        ThreadMemberState::Active
    );
    hub.heartbeat(&mut client, 65_000, false).await?;
    let paused = hub
        .wait_member(&caller, &long, &file_worker, |member| {
            member.state == ThreadMemberState::PausedUnknown
        })
        .await?;
    assert_pending_range(&paused, &long_wire)?;
    hub.heartbeat(&mut client, 65_001, false).await?;
    assert_eq!(hub.status().await?, "online");
    drop(client);
    hub.close().await
}

fn assert_pending_range(
    member: &MemberView,
    wire: &cooplet_protocol::ThreadDeliverPayload,
) -> TestResult {
    assert_eq!(member.confirmed_seq, 0);
    assert_eq!(member.delivery_generation, wire.delivery_generation);
    let pending = member
        .pending
        .as_ref()
        .ok_or("timeout removed its unresolved range")?;
    assert_eq!(pending.delivery_id, wire.delivery_id);
    assert_eq!(
        (pending.from_seq, pending.to_seq),
        (wire.from_seq, wire.to_seq)
    );
    Ok(())
}

#[tokio::test]
async fn direct_post_and_the_only_idle_wake_survive_a_rolled_back_transition() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    let participant = ready_agent(&mut client, epoch, ParticipantState::Working).await?;
    let caller = hub.admin().await?;
    let queued = hub
        .board(
            &caller,
            &participant,
            "queued-gate",
            DeliveryMode::QueueUntilIdle,
        )
        .await?;
    let direct = hub
        .board(
            &caller,
            &participant,
            "direct-gate",
            DeliveryMode::DirectNow,
        )
        .await?;
    hub.post(&caller, &queued, "queued-gate", Vec::new())
        .await?;

    let transition = hub.registry.transition(&hub.node_id).await;
    let mut events = hub.state.db.subscribe();
    send_payload(
        &mut client,
        epoch,
        &NodeEventPayload::ParticipantStateChanged {
            participant_id: participant.clone(),
            state: ParticipantState::Idle,
            approval_id: None,
            action_hint: None,
            summary: None,
            wait_kind: None,
            can_respond_remotely: None,
        },
    )
    .await?;
    time::timeout(TIMEOUT, async {
        loop {
            if events.recv().await?.kind == EventKind::ParticipantStateChanged {
                return Ok::<_, tokio::sync::broadcast::error::RecvError>(());
            }
        }
    })
    .await??;
    hub.post(&caller, &direct, "direct-gate", Vec::new())
        .await?;
    hub.heartbeat(&mut client, 0, false).await?;
    assert!(
        hub.member(&caller, &queued, &participant)
            .await?
            .pending
            .is_none()
    );
    assert!(
        hub.member(&caller, &direct, &participant)
            .await?
            .pending
            .is_none()
    );

    // The authority transition has no durable effect. Release it without
    // sending a fresh post, projecting another Idle or reconnecting the node.
    drop(transition);
    let mut received = HashSet::new();
    for _ in 0..2 {
        let delivery: cooplet_protocol::ThreadDeliverPayload =
            next_frame(&mut client).await?.decode_payload()?;
        assert_eq!(delivery.member_id, participant);
        assert_eq!((delivery.from_seq, delivery.to_seq), (1, 1));
        assert!(received.insert(delivery.thread_id.clone()));
        send_payload(
            &mut client,
            epoch,
            &ThreadAckPayload {
                delivery_id: delivery.delivery_id,
                delivery_generation: delivery.delivery_generation,
                thread_id: delivery.thread_id,
                member_id: delivery.member_id,
                acked_seq: delivery.to_seq,
                outcome: ThreadDeliveryOutcome::Delivered,
                detail: None,
            },
        )
        .await?;
    }
    assert_eq!(received, HashSet::from([queued.clone(), direct.clone()]));
    for board in [queued, direct] {
        let member = hub
            .wait_member(&caller, &board, &participant, |member| {
                member.confirmed_seq == 1
            })
            .await?;
        assert!(member.pending.is_none());
    }
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn cancelled_thread_delivery_does_not_block_replacement_for_same_member() -> TestResult {
    let (hub, mut client, epoch) = Fixture::open().await?;
    let participant = ready_agent(&mut client, epoch, ParticipantState::Idle).await?;
    let thread_id = ThreadId::generate()?;

    let make_post = |seq: u64| -> TestResult<cooplet_protocol::ThreadPostWire> {
        Ok(cooplet_protocol::ThreadPostWire {
            seq,
            post_id: cooplet_protocol::MessageId::generate()?,
            author_actor_id: "usr_admin".into(),
            author_kind: cooplet_protocol::AuthorKind::Human,
            author_handle: "admin".into(),
            in_reply_to_post_id: None,
            body: format!("post {seq}"),
            mentions: Vec::new(),
            attachments: Vec::new(),
            question: None,
            answer: None,
            created_at_ms: 1_000,
        })
    };

    let permit1 = hub
        .registry
        .reserve_thread(&hub.node_id, false)
        .await
        .map_err(|err| format!("reserve 1 failed: {err:?}"))?;
    let delivery1 = OperationId::generate()?;
    let payload1 = cooplet_protocol::ThreadDeliverPayload {
        delivery_generation: 0,
        delivery_id: delivery1.clone(),
        thread_id: thread_id.clone(),
        member_id: participant.clone(),
        from_seq: 1,
        to_seq: 1,
        posts: vec![make_post(1)?],
        truncated_earlier: false,
        delivery_mode: DeliveryMode::QueueUntilIdle,
        admission_budget_ms: 5_000,
    };
    let submit1 = tokio::spawn(permit1.submit(payload1));
    let wire1: cooplet_protocol::ThreadDeliverPayload =
        next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(wire1.delivery_id, delivery1);

    // Cancel delivery 1 (dropping its future sets the cancel flag) and immediately submit a
    // replacement delivery for the same (thread_id, member_id) without acking delivery 1.
    submit1.abort();
    let _ = submit1.await;

    let permit2 = hub
        .registry
        .reserve_thread(&hub.node_id, false)
        .await
        .map_err(|err| format!("reserve 2 failed: {err:?}"))?;
    let delivery2 = OperationId::generate()?;
    let payload2 = cooplet_protocol::ThreadDeliverPayload {
        delivery_generation: 1,
        delivery_id: delivery2.clone(),
        thread_id: thread_id.clone(),
        member_id: participant.clone(),
        from_seq: 2,
        to_seq: 2,
        posts: vec![make_post(2)?],
        truncated_earlier: false,
        delivery_mode: DeliveryMode::QueueUntilIdle,
        admission_budget_ms: 5_000,
    };
    let submit2 = tokio::spawn(permit2.submit(payload2));
    let wire2: cooplet_protocol::ThreadDeliverPayload =
        next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(wire2.delivery_id, delivery2);
    send_payload(
        &mut client,
        epoch,
        &ThreadAckPayload {
            delivery_id: delivery2.clone(),
            delivery_generation: 1,
            thread_id,
            member_id: participant,
            acked_seq: 2,
            outcome: ThreadDeliveryOutcome::Delivered,
            detail: None,
        },
    )
    .await?;
    let ack = time::timeout(TIMEOUT, submit2)
        .await??
        .map_err(|err| format!("replacement delivery failed: {err:?}"))?;
    assert_eq!(ack.delivery_id, delivery2);
    assert_eq!(ack.outcome, ThreadDeliveryOutcome::Delivered);
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn duplicate_reverse_request_at_capacity_closes_socket_and_rate_limit_preserves_slots()
-> TestResult {
    use cooplet_protocol::{
        NodeRequestCaller, NodeRequestPayload, NodeResponsePayload, NodeRpcOperation,
        NodeRpcOutcome,
    };

    let (hub, mut client, epoch) = Fixture::open().await?;
    let participant = ready_agent(&mut client, epoch, ParticipantState::Idle).await?;

    // Send a NodeEvent while the DB writer is stalled so all 16 reverse RPCs block waiting
    // for the projection barrier (`applied >= accepted_events`).
    let writer = hub.block_writer().await?;
    send_payload(
        &mut client,
        epoch,
        &NodeEventPayload::ParticipantStateChanged {
            participant_id: participant.clone(),
            state: ParticipantState::Working,
            approval_id: None,
            action_hint: None,
            summary: None,
            wait_kind: None,
            can_respond_remotely: None,
        },
    )
    .await?;

    let make_req = |request_id: OperationId| NodeRequestPayload {
        request_id,
        caller: NodeRequestCaller::Participant {
            participant_id: participant.clone(),
        },
        operation: NodeRpcOperation::ThreadList {
            query: Default::default(),
        },
    };
    let mut live_ids = Vec::with_capacity(REVERSE_SLOTS);
    for _ in 0..REVERSE_SLOTS {
        let id = OperationId::generate()?;
        send_payload(&mut client, epoch, &make_req(id.clone())).await?;
        live_ids.push(id);
    }

    // 17th distinct request_id exceeds REVERSE_SLOTS and gets synthetic RateLimited without
    // evicting any of the 16 live slots or closing the session.
    let overflow_id = OperationId::generate()?;
    send_payload(&mut client, epoch, &make_req(overflow_id.clone())).await?;
    let resp: NodeResponsePayload = next_frame(&mut client).await?.decode_payload()?;
    assert_eq!(resp.request_id, overflow_id);
    assert!(matches!(
        resp.outcome,
        NodeRpcOutcome::Problem { ref problem } if problem.code == ErrorCode::RateLimited
    ));

    // Reusing one of the 16 live request_ids at capacity must close the session with 1002.
    send_payload(&mut client, epoch, &make_req(live_ids[0].clone())).await?;
    let ClientMessage::Close(Some(close)) = next_message(&mut client).await? else {
        return Err("expected 1002 protocol close on duplicate reverse request_id".into());
    };
    assert_eq!(u16::from(close.code), 1002);

    writer.release().await?;
    drop(client);
    hub.close().await
}

#[tokio::test]
async fn early_node_event_and_revocation_close_with_active_write_behave_cleanly() -> TestResult {
    use tokio::net::TcpSocket;
    use tokio_tungstenite::client_async;

    let (hub, mut client, epoch) = Fixture::open().await?;
    let participant = agent("early-worker", ParticipantState::Working)?;
    let pid = participant.participant_id.clone();

    // Send Ping right before RosterSync to interleave control writes during handshake.
    client
        .send(ClientMessage::Ping(Bytes::from_static(PROBE)))
        .await?;
    send_roster(&mut client, epoch, vec![participant]).await?;
    let mut ready_seen = false;
    while !ready_seen {
        match next_message(&mut client).await? {
            ClientMessage::Pong(_) => {}
            ClientMessage::Text(text) => {
                let _: RosterReadyPayload =
                    BridgeFrame::from_json(text.as_bytes())?.decode_payload()?;
                ready_seen = true;
            }
            other => return Err(format!("unexpected handshake frame: {other:?}").into()),
        }
    }

    // Immediately emit a NodeEvent and verify it is projected cleanly without closing.
    let mut events = hub.state.db.subscribe();
    send_payload(
        &mut client,
        epoch,
        &NodeEventPayload::ParticipantStateChanged {
            participant_id: pid.clone(),
            state: ParticipantState::Idle,
            approval_id: None,
            action_hint: None,
            summary: None,
            wait_kind: None,
            can_respond_remotely: None,
        },
    )
    .await?;
    time::timeout(TIMEOUT, async {
        loop {
            if events.recv().await?.kind == EventKind::ParticipantStateChanged {
                return Ok::<_, tokio::sync::broadcast::error::RecvError>(());
            }
        }
    })
    .await??;
    drop(client);
    time::timeout(TIMEOUT, hub.registry.drain()).await?;

    // Open a second connection with tiny TCP send+recv buffers (4 KiB each) so stopping reads
    // deterministically backpressures Hub writes (`active_write.is_some()`), then verify
    // `disconnect_revoked` still delivers RFC 6455 close code 4003 mid-burst.
    let server_sock = TcpSocket::new_v4()?;
    server_sock.set_send_buffer_size(4096)?;
    server_sock.bind("127.0.0.1:0".parse()?)?;
    let listener = server_sock.listen(16)?;
    let address = listener.local_addr()?;
    let app = router(hub.state.clone(), RequestLimits::default());
    let server2 = tokio::spawn(async move { axum::serve(listener, app).await });
    let cred = mint_node(&hub.state.master, &hub.node_id, 2)?;
    let nid = hub.node_id.clone();
    let verifier = cred.verifier;
    hub.state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE nodes SET generation = 2, credential_hmac = ?2 WHERE node_id = ?1",
                params![nid.as_str(), verifier],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let tcp_sock = TcpSocket::new_v4()?;
    tcp_sock.set_recv_buffer_size(4096)?;
    let tcp = tcp_sock.connect(address).await?;
    let mut request = format!("ws://{address}/v1/bridge/connect").into_client_request()?;
    request
        .headers_mut()
        .insert("x-cooplet-hub", hub.state.meta.hub_id.as_str().parse()?);
    request.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {}", cred.secret.into_string()).parse()?,
    );
    request
        .headers_mut()
        .insert(header::SEC_WEBSOCKET_PROTOCOL, BRIDGE_SUBPROTOCOL.parse()?);
    let (mut bp_client, _) = time::timeout(TIMEOUT, client_async(request, tcp)).await??;
    let hello_frame: BridgeFrame = {
        let ClientMessage::Text(text) = time::timeout(TIMEOUT, bp_client.next())
            .await?
            .ok_or("bp bridge closed")??
        else {
            return Err("expected HelloAck".into());
        };
        BridgeFrame::from_json(text.as_bytes())?
    };
    let epoch2 = hello_frame.epoch;
    let roster_wire = String::from_utf8(BridgeFrame::encode_json(
        epoch2,
        &RosterSyncPayload {
            chunk_index: 0,
            is_final: true,
            participants: Vec::new(),
            templates: Vec::new(),
            workflows: Vec::new(),
            unresolved_thread_fences: Vec::new(),
            resolved_op_receipts: Vec::new(),
        },
    )?)?;
    bp_client
        .send(ClientMessage::Text(roster_wire.into()))
        .await?;
    let ClientMessage::Text(ready_txt) = time::timeout(TIMEOUT, bp_client.next())
        .await?
        .ok_or("bp bridge closed before ready")??
    else {
        return Err("expected RosterReady".into());
    };
    let _: RosterReadyPayload = BridgeFrame::from_json(ready_txt.as_bytes())?.decode_payload()?;

    // Flood 24 large (~48 KiB) CommandReq frames (>1.1 MiB) while NOT reading from bp_client
    // so the Hub socket is backpressured mid-write (`active_write.is_some()`), then revoke.
    let large_body = "z".repeat(48 * 1024);
    let mut submitted = Vec::new();
    for _ in 0..24 {
        let permit = hub
            .registry
            .reserve(&hub.node_id)
            .map_err(|err| format!("reserve failed: {err:?}"))?;
        let payload = CommandReqPayload {
            op_id: OperationId::generate()?,
            actor_id: "usr_admin".into(),
            command: BridgeCommand::SendMessage {
                message_id: cooplet_protocol::MessageId::generate()?,
                sender_kind: cooplet_protocol::AuthorKind::Human,
                sender_name: "admin".into(),
                sender_node_id: None,
                recipient_id: pid.clone(),
                in_reply_to: None,
                body: large_body.clone(),
                attachments: Vec::new(),
                question: None,
                answer: None,
            },
            delivery_mode: DeliveryMode::DirectNow,
            admission_budget_ms: 5_000,
            ttl_ms: None,
        };
        submitted.push(tokio::spawn(permit.submit(payload)));
    }
    time::sleep(Duration::from_millis(50)).await;
    hub.registry.disconnect_revoked(&hub.node_id);

    let mut frames_before_close = 0usize;
    let mut saw_revoked_close = false;
    while let Ok(Some(Ok(msg))) = time::timeout(TIMEOUT, bp_client.next()).await {
        match msg {
            ClientMessage::Text(_) => frames_before_close += 1,
            ClientMessage::Pong(_) | ClientMessage::Ping(_) => {}
            ClientMessage::Close(Some(close)) => {
                assert_eq!(u16::from(close.code), 4003);
                saw_revoked_close = true;
                break;
            }
            other => return Err(format!("expected 4003 close frame, got {other:?}").into()),
        }
    }
    assert!(
        saw_revoked_close,
        "Hub must deliver 4003 close frame even when writes were backpressured"
    );
    assert!(
        frames_before_close < 24,
        "revocation should interrupt the backpressured burst (saw {frames_before_close}/24)"
    );
    for cmd in submitted {
        let _ = time::timeout(TIMEOUT, cmd).await;
    }
    server2.abort();
    let _ = server2.await;
    drop(bp_client);
    hub.close().await
}
