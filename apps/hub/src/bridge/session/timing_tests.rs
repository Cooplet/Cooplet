use std::{
    fs, io,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as sync_mpsc,
    },
};

use cooplet_protocol::{
    BRIDGE_SUBPROTOCOL, BridgeCommand, CommandOutcome, CommandReqPayload, DeliveryMode, HubId,
    NodeId,
};
use cooplet_storage::{StorageConfig, StorageEngine, WriteOutcome};
use futures_util::StreamExt;
use http::header;
use rusqlite::params;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message as ClientMessage, client::IntoClientRequest},
};

use crate::{
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
        } = storage::open(&scratch.0, 1024, 16_384, 16_384).await?;
        let (shutdown, stop) = watch::channel(());
        let clock = Arc::new(Clock::default());
        let registry = Arc::new(LiveBridge::new(1, clock.clone()));
        let state = AppState::new(&engine, meta, master, registry.clone(), stop, 1024)?;
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
    send_payload(
        client,
        epoch,
        &RosterSyncPayload {
            chunk_index: 0,
            is_final: true,
            participants: Vec::new(),
            templates: Vec::new(),
            workflows: Vec::new(),
            unresolved_thread_fences: Vec::new(),
            resolved_op_receipts: Vec::new(),
        },
    )
    .await
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
