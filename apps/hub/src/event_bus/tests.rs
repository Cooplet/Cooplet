use std::{error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use axum::body::BodyDataStream;
use cooplet_platform::PrivateFs;
use cooplet_protocol::{EventStreamFrame, NodeId, ServicePrincipalId, TokenId};
use cooplet_storage::{DEFAULT_MAX_BLOB_BYTES, StorageEngine, WriteOutcome};
use futures_util::StreamExt;
use serde_json::{Value, json};

use super::*;
use crate::{
    auth,
    bridge::OfflineBridge,
    credentials::mint_service,
    now_ms,
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
            stream_query(after),
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

fn stream_query(after: u64) -> EventStreamQuery {
    EventStreamQuery { after, kinds: None }
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
        stream_query(279),
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
    assert_eq!(query.validated()?.1, vec![EventKind::NodeStatus]);
    Ok(())
}

#[tokio::test]
async fn quiet_stream_reconciles_a_durable_commit_without_a_wake() -> TestResult {
    let fixture = Fixture::new().await?;
    // Isolate the stream from broadcasts while using the real writer and reader.
    let (_silent, wake) = watch::channel((0, 0));
    let mut body = super::stream::response(
        Reader::new(&fixture.state, &fixture.admin, Vec::new()),
        0,
        wake,
        fixture.state.revocations.subscribe(),
        fixture.state.thread_acl_watch.subscribe(),
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

#[test]
fn event_stream_query_rejects_limit_and_kind_queries_use_index() -> TestResult {
    use axum::extract::Query;
    assert!(
        Query::<EventStreamQuery>::try_from_uri(&http::Uri::from_static(
            "/v1/events/stream?after=0&limit=1"
        ))
        .is_err()
    );
    assert!(
        Query::<EventStreamQuery>::try_from_uri(&http::Uri::from_static(
            "/v1/events/stream?after=12&kinds=node.status"
        ))
        .is_ok()
    );

    let db = rusqlite::Connection::open_in_memory()?;
    db.execute_batch(include_str!("../storage/v1.sql"))?;
    db.execute_batch(
        "WITH RECURSIVE n(seq) AS (VALUES (1) UNION ALL SELECT seq + 1 FROM n WHERE seq < 5000)
         INSERT INTO events (seq, event_id, kind, aggregate_type, aggregate_id, payload_json, created_at_ms)
         SELECT seq, 'evt-' || seq, CASE WHEN seq % 100 = 0 THEN 'blob.stored' ELSE 'node.status' END,
                'node', 'node', '{}', seq FROM n;
         ANALYZE;",
    )?;
    let explain = |sql: &str, bindings: &[&dyn rusqlite::ToSql]| -> rusqlite::Result<String> {
        let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
        let lines = stmt
            .query_map(bindings, |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(lines.join("\n"))
    };
    let single = explain(
        SELECT_SINGLE_KIND_EVENTS,
        &[&"blob.stored", &0u64, &5000u64, &101usize],
    )?;
    assert!(single.contains("idx_hub_events_kind_seq"), "{single}");
    assert!(!single.contains("USE TEMP B-TREE"), "{single}");

    let rare_multi = explain(
        SELECT_KIND_SEQS,
        &[&"blob.stored", &0u64, &5000u64, &101usize],
    )?;
    assert!(
        rare_multi.contains("COVERING INDEX idx_hub_events_kind_seq"),
        "{rare_multi}"
    );
    assert!(!rare_multi.contains("USE TEMP B-TREE"), "{rare_multi}");
    let dense_multi = explain(
        SELECT_KIND_SEQS,
        &[&"node.status", &0u64, &5000u64, &101usize],
    )?;
    assert!(!dense_multi.contains("USE TEMP B-TREE"), "{dense_multi}");
    Ok(())
}

#[tokio::test]
async fn single_and_multi_kind_queries_return_ordered_events_and_advance_watermark() -> TestResult {
    let fixture = Fixture::new().await?;
    let hub = fixture.state.meta.hub_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            let now = now_ms();
            let mut events = Vec::new();
            for (index, kind) in [
                EventKind::NodeStatus,
                EventKind::BlobStored,
                EventKind::BlobDeleted,
                EventKind::NodeStatus,
                EventKind::BlobStored,
            ]
            .into_iter()
            .enumerate()
            {
                events.push(append(
                    tx,
                    &hub,
                    NewEvent {
                        kind,
                        aggregate_type: AggregateType::Node,
                        aggregate_id: hub.as_str(),
                        actor_id: None,
                        created_at_ms: now,
                        data: json!({ "index": index + 1 }),
                        audience: EventAudience::GlobalArchiveOnly,
                    },
                )?);
            }
            Ok(WriteOutcome::with_events((), events))
        })
        .await?;

    let single = page(
        &fixture.state,
        &fixture.admin,
        EventsQuery {
            after: 0,
            limit: Some(10),
            kinds: Some("blob.stored".into()),
        },
    )
    .await?;
    assert_eq!(
        single.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 5]
    );
    assert_eq!(single.next_seq, 5);
    assert!(!single.has_more);

    let multi_first = page(
        &fixture.state,
        &fixture.admin,
        EventsQuery {
            after: 0,
            limit: Some(2),
            kinds: Some("blob.deleted,blob.stored".into()),
        },
    )
    .await?;
    assert_eq!(
        multi_first.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(multi_first.next_seq, 3);
    assert!(multi_first.has_more);

    let multi_second = page(
        &fixture.state,
        &fixture.admin,
        EventsQuery {
            after: multi_first.next_seq,
            limit: Some(10),
            kinds: Some("blob.deleted,blob.stored".into()),
        },
    )
    .await?;
    assert_eq!(
        multi_second.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![5]
    );
    assert_eq!(multi_second.next_seq, 5);
    assert!(!multi_second.has_more);

    fixture.close().await
}

#[tokio::test]
async fn stream_closes_naturally_when_token_ttl_expires_without_revocation_bump() -> TestResult {
    let fixture = Fixture::new().await?;
    let principal = ServicePrincipalId::generate()?;
    let token_id = TokenId::generate()?;
    let token = mint_service(&fixture.state.master, &principal)?;
    let verifier = token.verifier;
    let now = now_ms();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO principals (principal_id, kind, username, display_name, scopes_json,
                    allowed_nodes_json, created_at_ms, updated_at_ms)
                 VALUES (?1, 'service', ?1, 'Short TTL', '[\"read\"]', NULL, ?2, ?2)",
                params![principal.as_str(), now],
            )?;
            tx.execute(
                "INSERT INTO auth_tokens (token_id, principal_id, token_kind, verifier_hmac,
                    scopes_json, expires_at_ms, created_at_ms)
                 VALUES (?1, ?2, 'service_token', ?3, '[\"read\"]', ?4, ?5)",
                params![
                    token_id.as_str(),
                    principal.as_str(),
                    verifier,
                    now + 150,
                    now
                ],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let caller = auth::authenticate(
        &fixture.state.reads,
        &fixture.state.master,
        token.secret.as_str(),
    )
    .await?;
    let mut body = fixture.stream(&caller, 0).await?;
    assert_eq!(next_frame(&mut body).await?, None);
    fixture.close().await
}

#[tokio::test]
async fn thread_retention_hides_logically_expired_posts_before_collection() -> TestResult {
    use std::sync::atomic::{AtomicI64, Ordering};

    use axum::body::{Body, to_bytes};
    use cooplet_client::X_COOPLET_FILENAME;
    use http::{HeaderMap, HeaderValue, header};

    use crate::{blobs, threads};
    use cooplet_protocol::{
        ThreadCreateRequest, ThreadPinSetRequest, ThreadPostRequest, ThreadReadQuery,
    };

    let mut fixture = Fixture::new().await?;
    let initial = now_ms();
    let wall = Arc::new(AtomicI64::new(initial));
    let injected = wall.clone();
    fixture.state.wall_clock = Arc::new(move || injected.load(Ordering::Relaxed));
    let board = threads::create(
        &fixture.state,
        &fixture.admin,
        "thread-test-create",
        ThreadCreateRequest {
            title: "Retention".into(),
            topic: None,
            creator_handle: None,
            members: Vec::new(),
        },
    )
    .await?;
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    headers.insert(X_COOPLET_FILENAME, HeaderValue::from_static("report.txt"));
    let (_, attachment) = blobs::upload(
        &fixture.state,
        &fixture.admin,
        &headers,
        Body::from("retained attachment"),
    )
    .await?;
    let mut receipts = Vec::new();
    for (index, time) in [
        (1, initial),
        (2, initial - 1000),
        (3, initial + 86_400_000),
        (4, initial + threads::RETENTION_MS + 1),
    ] {
        wall.store(time, Ordering::Relaxed);
        receipts.push(
            threads::post(
                &fixture.state,
                &fixture.admin,
                board.thread_id.clone(),
                &format!("thread-test-post-{index}"),
                ThreadPostRequest {
                    body: format!("post {index}"),
                    mentions: vec![],
                    attachments: if matches!(index, 1 | 3) {
                        vec![attachment.clone()]
                    } else {
                        vec![]
                    },
                    question: None,
                    answer: None,
                    in_reply_to: None,
                },
            )
            .await?,
        );
    }
    assert_eq!(receipts[0].created_at_ms, receipts[1].created_at_ms);
    let live = threads::read(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        ThreadReadQuery::default(),
    )
    .await?;
    assert_eq!((live.floor_seq, live.head_seq), (2, 4));
    assert_eq!(live.posts[0].post_id, receipts[2].post_id);
    assert_eq!(
        live.posts[0].attachments.as_slice(),
        std::slice::from_ref(&attachment)
    );
    let file = blobs::download(&fixture.state, &fixture.admin, attachment.blob_id.clone()).await?;
    assert_eq!(
        to_bytes(file.into_body(), 64).await?.as_ref(),
        b"retained attachment"
    );

    let error = threads::read(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        ThreadReadQuery {
            since_seq: Some(1),
            ..ThreadReadQuery::default()
        },
    )
    .await
    .err()
    .ok_or("obsolete cursor was accepted")?;
    assert_eq!(error.problem().code, ErrorCode::ResyncRequired);
    assert_eq!(error.problem().extensions["floor_seq"], 2);
    assert_eq!(error.problem().extensions["head_seq"], 4);
    let error = threads::set_pin(
        &fixture.state,
        &fixture.admin,
        board.thread_id,
        "old".into(),
        "thread-test-old-pin",
        ThreadPinSetRequest {
            value_json: json!("keep"),
            post_id: Some(receipts[0].post_id.clone()),
            expected_version: Some(0),
        },
    )
    .await
    .err()
    .ok_or("logically expired post was pinnable")?;
    assert_eq!(error.problem().code, ErrorCode::NotFound);
    fixture.close().await
}

#[tokio::test]
async fn thread_acl_change_discards_initial_frames_at_the_retained_event_floor() -> TestResult {
    use crate::threads;
    use cooplet_protocol::{
        ThreadCreateRequest, ThreadMemberJoinRequest, ThreadMemberLeaveRequest, ThreadRole,
    };

    let fixture = Fixture::new().await?;
    let (service, _) = fixture.service(&[]).await?;
    let board = threads::create(
        &fixture.state,
        &fixture.admin,
        "acl-board-create",
        ThreadCreateRequest {
            title: "Scoped board".into(),
            topic: None,
            creator_handle: None,
            members: vec![ThreadMemberJoinRequest {
                member_key: service.actor_id().into(),
                handle: Some("reader".into()),
                role: Some(ThreadRole::Observer),
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    fixture
        .state
        .db
        .write_sqlite(|tx| {
            tx.execute(
                "UPDATE hub_meta SET events_pruned_through_seq=1 WHERE id=1",
                [],
            )?;
            tx.execute("DELETE FROM events WHERE seq=1", [])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let mut body = fixture.stream(&service, 0).await?;
    threads::leave(
        &fixture.state,
        &fixture.admin,
        board.thread_id,
        "acl-board-leave",
        ThreadMemberLeaveRequest {
            member_key: Some(service.actor_id().into()),
        },
    )
    .await?;
    fixture
        .publish(vec![(
            EventAudience::OwnerOnly {
                actor_id: service.actor_id().into(),
            },
            json!({"visible":true}),
        )])
        .await?;

    let response = tokio::time::timeout(Duration::from_secs(2), body.next())
        .await?
        .ok_or("stream ended before the visible event")??;
    let EventStreamFrame::Event { event } = serde_json::from_slice(&response)? else {
        return Err("expected the visible event, not a resync frame".into());
    };
    assert_eq!(event.payload["visible"], true);
    drop(body);
    fixture.close().await
}

#[tokio::test]
async fn closed_thread_expiry_and_gc_discard_buffered_frames_for_members() -> TestResult {
    use std::sync::atomic::{AtomicI64, Ordering};

    use crate::threads;
    use cooplet_protocol::{ThreadCreateRequest, ThreadMemberJoinRequest};

    let mut fixture = Fixture::new().await?;
    let (service, _) = fixture.service(&[]).await?;
    let now = now_ms();
    let wall = Arc::new(AtomicI64::new(now));
    let injected = wall.clone();
    fixture.state.wall_clock = Arc::new(move || injected.load(Ordering::Relaxed));
    let board = threads::create(
        &fixture.state,
        &fixture.admin,
        "expiry-board-create",
        ThreadCreateRequest {
            title: "Expiring board".into(),
            topic: None,
            creator_handle: None,
            members: vec![ThreadMemberJoinRequest {
                member_key: service.actor_id().into(),
                handle: Some("reader".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    threads::close(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "expiry-board-close",
    )
    .await?;
    let thread = board.thread_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            tx.execute(
                "UPDATE threads SET expires_at_ms=?1 WHERE thread_id=?2",
                params![now + 50, thread.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let mut member_stream = fixture.stream(&service, 0).await?;
    let mut rollback_stream = fixture.stream(&service, 0).await?;
    let mut archive = fixture.stream(&fixture.admin, 0).await?;
    wall.store(now + 51, Ordering::Relaxed);
    fixture
        .publish(vec![(
            EventAudience::OwnerOnly {
                actor_id: service.actor_id().into(),
            },
            json!({"after_expiry":true}),
        )])
        .await?;
    let first = tokio::time::timeout(Duration::from_secs(2), member_stream.next())
        .await?
        .ok_or("member stream ended early")??;
    let EventStreamFrame::Event { event } = serde_json::from_slice(&first)? else {
        return Err("expected an event after expiry".into());
    };
    assert_eq!(event.payload["after_expiry"], true);

    // Keep the intermediate state from the first committed GC batch. The
    // second member stream is still holding frames from before this write.
    let thread = board.thread_id.clone();
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            let marked = tx.execute(
                "UPDATE threads SET expires_at_ms=0 WHERE thread_id=?1 AND expires_at_ms<=?2",
                params![thread.as_str(), now + 51],
            )?;
            assert_eq!(marked, 1);
            Ok(WriteOutcome::new(()))
        })
        .await?;
    wall.store(now, Ordering::Relaxed);
    let first = tokio::time::timeout(Duration::from_secs(2), rollback_stream.next())
        .await?
        .ok_or("member stream ended on the clock rollback")??;
    let EventStreamFrame::Event { event } = serde_json::from_slice(&first)? else {
        return Err("expected a visible event after rollback".into());
    };
    assert_eq!(event.payload["after_expiry"], true);

    let first = tokio::time::timeout(Duration::from_secs(2), archive.next())
        .await?
        .ok_or("archive stream ended early")??;
    let EventStreamFrame::Event { event } = serde_json::from_slice(&first)? else {
        return Err("expected a retained archive event".into());
    };
    assert_eq!(event.kind, EventKind::ThreadCreated);
    assert_eq!(event.aggregate_id, board.thread_id.as_str());
    drop(member_stream);
    drop(rollback_stream);
    drop(archive);
    fixture.close().await
}

#[tokio::test]
async fn a_nodes_departure_does_not_remove_it_from_historical_thread_acl() -> TestResult {
    use crate::threads;
    use cooplet_protocol::{
        ParticipantId, ThreadCreateRequest, ThreadListQuery, ThreadMemberJoinRequest,
        ThreadMemberLeaveRequest,
    };

    let fixture = Fixture::new().await?;
    let (service, _) = fixture.service(&[]).await?;
    let board = threads::create(
        &fixture.state,
        &fixture.admin,
        "history-board-create",
        ThreadCreateRequest {
            title: "Historical scopes".into(),
            topic: None,
            creator_handle: None,
            members: vec![ThreadMemberJoinRequest {
                member_key: service.actor_id().into(),
                handle: Some("reader".into()),
                role: None,
                wake_policy: None,
                delivery_mode: None,
            }],
        },
    )
    .await?;
    assert_eq!(
        threads::list(&fixture.state, &service, ThreadListQuery::default())
            .await?
            .items
            .len(),
        1
    );

    let node = NodeId::generate()?;
    let agent = ParticipantId::generate()?;
    let key = format!("{node}/{agent}");
    let node_for_write = node.clone();
    let owner = fixture.admin.actor_id().to_owned();
    fixture.state.db.write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO nodes(node_id,display_name,owner_principal_id,credential_hmac,status,enrolled_at_ms)
             VALUES(?1,'Worker',?2,'worker credential','online',1)",
            params![node_for_write.as_str(), owner],
        )?;
        tx.execute(
            "INSERT INTO node_participants(node_id,participant_id,kind,name,state,synced_epoch,updated_at_ms)
             VALUES(?1,?2,'agent','Worker','idle',0,1)",
            params![node_for_write.as_str(), agent.as_str()],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;

    let mut buffered = fixture.stream(&service, 0).await?;
    threads::join(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "history-board-join",
        ThreadMemberJoinRequest {
            member_key: key.clone(),
            handle: Some("worker".into()),
            role: None,
            wake_policy: None,
            delivery_mode: None,
        },
    )
    .await?;
    threads::leave(
        &fixture.state,
        &fixture.admin,
        board.thread_id.clone(),
        "history-board-leave",
        ThreadMemberLeaveRequest {
            member_key: Some(key),
        },
    )
    .await?;
    fixture
        .state
        .db
        .write_sqlite(move |tx| {
            tx.execute("DELETE FROM nodes WHERE node_id=?1", [node.as_str()])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let hidden = threads::get(&fixture.state, &service, board.thread_id).await;
    assert!(hidden.is_err_and(|error| error.problem().code == ErrorCode::NotFound));
    assert!(
        threads::list(&fixture.state, &service, ThreadListQuery::default())
            .await?
            .items
            .is_empty()
    );

    fixture
        .publish(vec![(
            EventAudience::OwnerOnly {
                actor_id: service.actor_id().into(),
            },
            json!({"outside":true}),
        )])
        .await?;
    let bytes = tokio::time::timeout(Duration::from_secs(2), buffered.next())
        .await?
        .ok_or("missing visible event")??;
    let EventStreamFrame::Event { event } = serde_json::from_slice(&bytes)? else {
        return Err("expected the outside event".into());
    };
    assert_eq!(event.payload["outside"], true);
    drop(buffered);
    fixture.close().await
}
