use std::{error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use axum::body::BodyDataStream;
use cooplet_platform::PrivateFs;
use cooplet_protocol::{EventStreamFrame, NodeId, ServicePrincipalId, TokenId};
use cooplet_storage::{DEFAULT_MAX_BLOB_BYTES, StorageEngine, WriteOutcome};
use futures_util::StreamExt;
use serde_json::{Value, json};

use super::*;
use crate::{
    bridge::OfflineBridge,
    credentials::mint_service,
    storage::{
        self,
        events::{NewEvent, append},
    },
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    engine: StorageEngine,
    state: AppState,
    admin: AuthenticatedCaller,
    shutdown: watch::Sender<()>,
    _scratch: Scratch,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        let root = std::env::temp_dir().join(EventId::generate()?.as_str());
        let scratch = Scratch(PrivateFs::ensure_private_dir(&root)?);
        let init = storage::init(&root, "EventBus fixture").await?;
        let token =
            PrivateFs::read_private_file(&init.token_path.ok_or("missing admin token")?, 512)?;
        let storage = storage::open(
            &root,
            DEFAULT_MAX_BLOB_BYTES,
            10 * 1024 * 1024 * 1024,
            1_048_576,
        )
        .await?;
        let (shutdown, signal) = watch::channel(());
        let state = AppState::new(
            &storage.engine,
            storage.meta,
            storage.master,
            Arc::new(OfflineBridge),
            signal,
            DEFAULT_MAX_BLOB_BYTES,
        )?;
        let admin = auth::authenticate(
            &state.reads,
            &state.master,
            std::str::from_utf8(&token)?.trim(),
        )
        .await?;
        Ok(Self {
            engine: storage.engine,
            state,
            admin,
            shutdown,
            _scratch: scratch,
        })
    }

    async fn service(&self, nodes: &[NodeId]) -> TestResult<(AuthenticatedCaller, TokenId)> {
        let principal = ServicePrincipalId::generate()?;
        let token_id = TokenId::generate()?;
        let token = mint_service(&self.state.master, &principal)?;
        let verifier = token.verifier;
        let token_for_write = token_id.clone();
        let nodes = serde_json::to_string(nodes)?;
        let now = now_ms();
        self.state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                "INSERT INTO principals (principal_id, kind, username, display_name, scopes_json,
                    allowed_nodes_json, created_at_ms, updated_at_ms)
                 VALUES (?1, 'service', ?1, 'Archive reader', '[\"read\"]', ?2, ?3, ?3)",
                params![principal.as_str(), nodes, now],
            )?;
                tx.execute(
                    "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                    scopes_json, expires_at_ms, created_at_ms)
                 VALUES (?1, ?2, 'service_token', ?3, '[\"read\"]', ?4, ?5)",
                    params![
                        token_for_write.as_str(),
                        principal.as_str(),
                        verifier,
                        now + 60_000,
                        now
                    ],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        let caller =
            auth::authenticate(&self.state.reads, &self.state.master, token.secret.as_str())
                .await?;
        Ok((caller, token_id))
    }

    async fn publish(&self, inputs: Vec<(EventAudience, Value)>) -> TestResult {
        let hub = self.state.meta.hub_id.clone();
        self.state
            .db
            .write_sqlite(move |tx| {
                let now = now_ms();
                let mut events = Vec::with_capacity(inputs.len());
                for (audience, data) in inputs {
                    events.push(append(
                        tx,
                        &hub,
                        NewEvent {
                            kind: EventKind::NodeStatus,
                            aggregate_type: AggregateType::Node,
                            aggregate_id: hub.as_str(),
                            actor_id: None,
                            created_at_ms: now,
                            data,
                            audience,
                        },
                    )?);
                }
                Ok(WriteOutcome::with_events((), events))
            })
            .await?;
        Ok(())
    }

    async fn revoke(&self, token: TokenId) -> TestResult {
        self.state
            .db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE auth_tokens SET revoked_at_ms = ?1 WHERE token_id = ?2",
                    params![now_ms(), token.as_str()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        self.state.revocations.bump();
        Ok(())
    }

    async fn stream(&self, caller: &AuthenticatedCaller, after: u64) -> TestResult<BodyDataStream> {
        Ok(stream(
            &self.state,
            caller,
            query(after),
            self.state.revocations.subscribe(),
        )
        .await?
        .into_body()
        .into_data_stream())
    }

    async fn close(self) -> TestResult {
        let Self {
            engine,
            state,
            shutdown,
            _scratch,
            ..
        } = self;
        let _ = shutdown.send(());
        drop(state);
        engine.shutdown().await?;
        Ok(())
    }
}

fn query(after: u64) -> EventsQuery {
    EventsQuery {
        after,
        limit: None,
        kinds: None,
    }
}

fn nodes(ids: &[NodeId]) -> EventAudience {
    EventAudience::Nodes {
        node_ids: ids.to_vec(),
    }
}

async fn next_frame(body: &mut BodyDataStream) -> TestResult<Option<EventStreamFrame>> {
    let chunk = tokio::time::timeout(Duration::from_secs(5), body.next()).await?;
    chunk
        .map(|bytes| Ok(serde_json::from_slice(&bytes?)?))
        .transpose()
}

#[tokio::test]
async fn event_pages_keep_large_items_and_the_final_watermark() -> TestResult {
    let fixture = Fixture::new().await?;
    let payload = "a".repeat(48 * 1024);
    fixture
        .publish(
            (1..=100)
                .map(|n| {
                    (
                        EventAudience::GlobalArchiveOnly,
                        json!({ "n": n, "payload": payload }),
                    )
                })
                .collect(),
        )
        .await?;

    let page_one = page(&fixture.state, &fixture.admin, query(0)).await?;
    assert!(page_one.has_more);
    assert!(!page_one.items.is_empty());
    assert!(serde_json::to_vec(&page_one)?.len() <= MAX_PAGE_BYTES);
    let page_two = page(&fixture.state, &fixture.admin, query(page_one.next_seq)).await?;
    assert!(!page_two.has_more);
    assert_eq!(page_two.next_seq, 100);
    assert_eq!(page_two.latest_seq, 100);
    assert!(serde_json::to_vec(&page_two)?.len() <= MAX_PAGE_BYTES);

    let seqs: Vec<_> = page_one
        .items
        .iter()
        .chain(&page_two.items)
        .map(|event| event.seq)
        .collect();
    assert_eq!(seqs, (1..=100).collect::<Vec<_>>());
    let error = page(&fixture.state, &fixture.admin, query(u64::MAX))
        .await
        .err()
        .ok_or("accepted a cursor beyond the head")?;
    assert_eq!(error.problem().code, ErrorCode::ResyncRequired);
    assert_eq!(error.problem().extensions["latest_seq"], 100);
    fixture.close().await
}

#[tokio::test]
async fn scoped_page_and_stream_advance_past_hidden_events_and_project_only_public_data()
-> TestResult {
    let fixture = Fixture::new().await?;
    let allowed = NodeId::generate()?;
    let hidden = NodeId::generate()?;
    let (service, _) = fixture.service(std::slice::from_ref(&allowed)).await?;
    let mut inputs: Vec<_> = (1..=520)
        .map(|n| (nodes(std::slice::from_ref(&hidden)), json!({"n": n})))
        .collect();
    inputs.extend([
        (
            nodes(&[allowed.clone(), hidden]),
            json!({ "tag": "cross-node" }),
        ),
        (
            EventAudience::GlobalArchiveOnly,
            json!({ "tag": "archive" }),
        ),
        (
            EventAudience::OwnerOnly {
                actor_id: service.actor_id().to_owned(),
            },
            json!({ "tag": "own" }),
        ),
        (
            nodes(std::slice::from_ref(&allowed)),
            json!({ "tag": "allowed" }),
        ),
        (
            nodes(std::slice::from_ref(&allowed)),
            json!({ "tag": "malformed" }),
        ),
    ]);
    fixture.publish(inputs).await?;
    fixture.state.db.write_sqlite(|tx| {
        tx.execute(
            "UPDATE events SET payload_json = '{\"data\":{\"tag\":\"malformed\"},\"audience\":{\"kind\":\"future\"}}'
             WHERE seq = 525",
            [],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;

    let first = page(&fixture.state, &service, query(0)).await?;
    assert!(first.items.is_empty());
    assert_eq!(first.next_seq, 512);
    assert!(first.has_more);
    let second = page(&fixture.state, &service, query(first.next_seq)).await?;
    assert_eq!(
        second.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [523, 524]
    );
    assert_eq!(second.next_seq, 525);
    assert!(!second.has_more);
    let global = page(&fixture.state, &fixture.admin, query(520)).await?;
    assert_eq!(
        global.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [521, 522, 523, 524]
    );

    let mut body = fixture.stream(&service, 0).await?;
    for expected in second.items {
        assert_eq!(
            next_frame(&mut body).await?,
            Some(EventStreamFrame::Event { event: expected })
        );
    }
    fixture
        .publish(vec![(nodes(&[allowed]), json!({ "tag": "live" }))])
        .await?;
    let Some(EventStreamFrame::Event { event }) = next_frame(&mut body).await? else {
        return Err("no live event".into());
    };
    assert_eq!(event.seq, 526);
    assert_eq!(event.payload, json!({ "tag": "live" }));
    fixture.shutdown.send(())?;
    assert_eq!(next_frame(&mut body).await?, None);
    fixture.close().await
}

#[tokio::test]
async fn idle_and_buffered_streams_close_on_credential_revocation() -> TestResult {
    let fixture = Fixture::new().await?;
    let allowed = NodeId::generate()?;
    let (service, token) = fixture.service(std::slice::from_ref(&allowed)).await?;
    fixture
        .publish(vec![(nodes(&[allowed]), json!({ "tag": "pending" }))])
        .await?;
    let mut idle = fixture.stream(&service, 1).await?;
    let mut buffered = fixture.stream(&service, 0).await?;
    let (next, revocation) = tokio::join!(next_frame(&mut idle), fixture.revoke(token),);
    revocation?;
    assert_eq!(next?, None);
    assert_eq!(next_frame(&mut buffered).await?, None);
    fixture.close().await
}

#[tokio::test]
async fn stream_emits_one_terminal_frame_when_retention_passes_its_scan_cursor() -> TestResult {
    let fixture = Fixture::new().await?;
    fixture
        .publish(
            (1..=300)
                .map(|seq| (EventAudience::GlobalArchiveOnly, json!({ "seq": seq })))
                .collect(),
        )
        .await?;
    let mut body = fixture.stream(&fixture.admin, 0).await?;
    fixture
        .state
        .db
        .write_sqlite(|tx| {
            tx.execute("DELETE FROM events WHERE seq <= 280", [])?;
            tx.execute(
                "UPDATE hub_meta SET events_pruned_through_seq = 280 WHERE id = 1",
                [],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    for seq in 1..=256 {
        let Some(EventStreamFrame::Event { event }) = next_frame(&mut body).await? else {
            return Err("missing prefetched event".into());
        };
        assert_eq!(event.seq, seq);
    }
    assert_eq!(
        next_frame(&mut body).await?,
        Some(EventStreamFrame::ResyncRequired {
            oldest_available_seq: 281,
            latest_seq: 300,
            reason: "cursor_pruned".into(),
        })
    );
    assert_eq!(next_frame(&mut body).await?, None);
    let error = stream(
        &fixture.state,
        &fixture.admin,
        query(279),
        fixture.state.revocations.subscribe(),
    )
    .await
    .err()
    .ok_or("accepted a cursor below the floor")?;
    assert_eq!(error.problem().code, ErrorCode::ResyncRequired);
    assert_eq!(error.problem().extensions["oldest_available_seq"], 281);
    fixture.close().await
}

#[test]
fn event_filter_requires_valid_nonempty_csv_and_deduplicates_labels() -> TestResult {
    for kinds in ["", "node.status,", ",node.status", "unregistered.event"] {
        let query = EventsQuery {
            kinds: Some(kinds.into()),
            ..query(0)
        };
        assert!(query.validated().is_err());
    }
    let query = EventsQuery {
        kinds: Some("node.status,node.status".into()),
        ..query(0)
    };
    assert_eq!(
        query.validated()?.kinds,
        HashSet::from([EventKind::NodeStatus])
    );
    Ok(())
}

#[tokio::test]
async fn quiet_stream_reconciles_a_durable_commit_without_a_wake() -> TestResult {
    let fixture = Fixture::new().await?;
    // Isolate the stream from broadcasts while using the real writer and reader.
    let (_silent, wake) = watch::channel((0, 0));
    let mut body = super::stream::response(
        Reader::new(&fixture.state, &fixture.admin, HashSet::new()),
        0,
        None,
        wake,
        fixture.state.revocations.subscribe(),
        fixture.state.shutdown.clone(),
    )
    .await?
    .into_body()
    .into_data_stream();
    fixture
        .publish(vec![(
            EventAudience::GlobalArchiveOnly,
            json!({"status": "offline"}),
        )])
        .await?;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(31)).await;
    tokio::time::resume();
    let frame = next_frame(&mut body).await?;
    let event = if matches!(frame, Some(EventStreamFrame::Ping {})) {
        next_frame(&mut body).await?
    } else {
        frame
    };
    assert!(matches!(event, Some(EventStreamFrame::Event { event }) if event.seq == 1));
    drop(body);
    fixture.close().await
}
