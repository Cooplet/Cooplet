use std::{error::Error, fs, path::PathBuf};

use cooplet_client::{ClientError, EventsQuery};
use cooplet_platform::PrivateFs;
use cooplet_protocol::{AggregateType, EventId, PrincipalKind};
use cooplet_storage::{DbActor, WriteOutcome};
use serde_json::json;

use super::*;
use crate::{
    auth::Credential,
    clients::AdapterRegistry,
    config::{NodeConfig, OpenNode, operator_path},
    daemon::Node,
    storage::append_event,
};

type TestError = Box<dyn Error + Send + Sync>;
type TestResult = std::result::Result<(), TestError>;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> std::result::Result<Self, TestError> {
        let path = std::env::temp_dir().join(EventId::generate()?.as_str());
        Ok(Self(PrivateFs::ensure_private_dir(&path)?))
    }

    async fn open(&self) -> std::result::Result<OpenNode, TestError> {
        Ok(OpenNode::start(NodeConfig::new(self.0.join("state"), self.0.clone())).await?)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn byte_bounded_pages_preserve_the_next_match_and_advance_through_filtered_rows() -> TestResult
{
    let scratch = Scratch::new()?;
    let node = scratch.open().await?;
    node.engine
        .db()
        .write_sqlite(|tx| {
            let mut events = Vec::new();
            for index in 0..5 {
                events.push(append_event(
                    tx,
                    index,
                    EventKind::BlobStored,
                    AggregateType::Blob,
                    "blob",
                    None,
                    json!({ "text": "界".repeat(300 * 1024) }),
                )?);
            }
            events.push(append_event(
                tx,
                5,
                EventKind::BlobDeleted,
                AggregateType::Blob,
                "blob",
                None,
                json!({}),
            )?);
            Ok(WriteOutcome::with_events((), events))
        })
        .await?;
    let kinds = parse_kinds(Some("blob.stored"))?;
    let read = |cursor| {
        read_page(
            node.engine.reads(),
            &node.info.node_id,
            cursor,
            kinds.as_deref(),
            100,
        )
    };
    let Snapshot::Page(page) = read(Cursor::Open(0)).await? else {
        return Err(io_error("The initial page is outside the retained window"));
    };
    assert_eq!(page.items.len(), 4);
    assert_eq!(
        (page.next_seq, page.latest_seq, page.has_more),
        (4, 6, true)
    );
    assert!(json_bytes(&page)? <= PAGE_BYTES);

    let Snapshot::Page(page) = read(Cursor::Open(4)).await? else {
        return Err(io_error("The second page is outside the retained window"));
    };
    assert_eq!(page.items.first().map(|e| e.seq), Some(5));
    assert_eq!(
        (page.next_seq, page.latest_seq, page.has_more),
        (6, 6, false)
    );

    let Snapshot::Gap(window) = read(Cursor::Open(u64::MAX)).await? else {
        return Err(io_error("A future cursor was accepted"));
    };
    assert_eq!(window.problem().code(), ErrorCode::ResyncRequired);
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn pruning_between_stream_batches_reports_resync_without_a_broadcast() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.open().await?;
    node.engine
        .db()
        .write_sqlite(|tx| {
            for index in 0..600 {
                append_event(
                    tx,
                    index,
                    EventKind::BlobStored,
                    AggregateType::Blob,
                    "blob",
                    None,
                    json!({}),
                )?;
            }
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let receiver = node.engine.db().subscribe();
    let Snapshot::Page(page) = read_page(
        node.engine.reads(),
        &node.info.node_id,
        Cursor::Open(0),
        None,
        CATCH_UP_BATCH,
    )
    .await?
    else {
        return Err(io_error("The initial page is outside the retained window"));
    };
    let (_shutdown, stopping) = watch::channel(false);
    let now = Instant::now();
    let mut stream = StreamState {
        reads: node.engine.reads().clone(),
        node: node.info.node_id.clone(),
        kinds: None,
        token: TokenId::generate()?,
        events: receiver,
        shutdown: stopping,
        cursor: page.next_seq,
        pending: page.items.into(),
        catch_up: page.has_more,
        authenticate_at: now + AUTH_INTERVAL,
        ping_at: now + PING_INTERVAL,
    };
    for seq in 1..=256 {
        assert!(matches!(
            stream.next_frame().await?,
            Some(EventStreamFrame::Event { event }) if event.seq == seq
        ));
    }
    node.engine
        .db()
        .write_sqlite(|tx| {
            tx.execute("DELETE FROM events WHERE seq <= 400", [])?;
            tx.execute("UPDATE node_meta SET events_pruned_through_seq = 400", [])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    assert!(matches!(
        stream.next_frame().await?,
        Some(EventStreamFrame::ResyncRequired {
            oldest_available_seq: 401,
            latest_seq: 600,
            reason,
        }) if reason == "cursor_pruned"
    ));
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn broadcast_lag_replays_commits_and_revocation_closes_the_stream() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.open().await?;
    let token = node
        .engine
        .reads()
        .read(|conn| {
            Ok(conn.query_row(
                "SELECT token_id FROM participant_credentials WHERE principal_kind = 'operator'",
                [],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await?
        .parse::<TokenId>()?;
    let (_shutdown, stopping) = watch::channel(false);
    let now = Instant::now();
    let mut stream = StreamState {
        reads: node.engine.reads().clone(),
        node: node.info.node_id.clone(),
        kinds: None,
        token,
        events: node.engine.db().subscribe(),
        shutdown: stopping,
        cursor: 0,
        pending: VecDeque::new(),
        catch_up: false,
        authenticate_at: now,
        ping_at: now + PING_INTERVAL,
    };

    let created_at_ms = now_ms()?;
    node.engine
        .db()
        .write_sqlite(move |tx| {
            let mut events = Vec::new();
            for _ in 0..1025 {
                events.push(append_event(
                    tx,
                    created_at_ms,
                    EventKind::BlobStored,
                    AggregateType::Blob,
                    "lagged",
                    None,
                    json!({}),
                )?);
            }
            Ok(WriteOutcome::with_events((), events))
        })
        .await?;
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), stream.next_frame()).await??,
        Some(EventStreamFrame::Event { event }) if event.seq == 1
    ));

    node.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "UPDATE participant_credentials SET revoked_at_ms = ?1 WHERE principal_kind = 'operator'",
            [created_at_ms],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    stream.authenticate_at = Instant::now();
    assert!(stream.next_frame().await?.is_none());
    node.engine.shutdown().await?;
    Ok(())
}

fn io_error(detail: &str) -> TestError {
    std::io::Error::other(detail).into()
}

#[tokio::test]
async fn client_catches_up_then_receives_live_commits_and_stale_cursors_get_410() -> TestResult {
    let scratch = Scratch::new()?;
    let config = NodeConfig::new(scratch.0.join("state"), scratch.0.clone());
    let credential_path = operator_path(&config.state_dir);
    let node = Node::open(config, AdapterRegistry::default()).await?;
    let db = node.state.db.clone();
    let shutdown = node.state.shutdown.clone();
    publish_blob_event(&db, "first").await?;
    publish_blob_event(&db, "second").await?;
    let task = tokio::spawn(node.serve());
    let assertions = async {
        let (client, _) = Credential::read(&credential_path)?.connect(PrincipalKind::Operator).await?;
        let mut stream = client.stream_events(1, &[EventKind::BlobStored]).await?;
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next_frame()).await?;
        assert!(matches!(
            frame,
            Some(Ok(EventStreamFrame::Event { event })) if event.seq == 2 && event.aggregate_id == "second"
        ));
        publish_blob_event(&db, "live").await?;
        let frame = tokio::time::timeout(Duration::from_secs(5), stream.next_frame()).await?;
        assert!(matches!(
            frame,
            Some(Ok(EventStreamFrame::Event { event })) if event.seq == 3 && event.aggregate_id == "live"
        ));

        db.write_sqlite(|tx| {
            tx.execute("DELETE FROM events WHERE seq <= 2", [])?;
            tx.execute("UPDATE node_meta SET events_pruned_through_seq = 2", [])?;
            Ok(WriteOutcome::new(()))
        }).await?;
        assert!(matches!(
            client.stream_events(1, &[]).await,
            Err(ClientError::ResyncRequired { oldest_available_seq: 3, latest_seq: 3, .. })
        ));
        assert!(matches!(
            client.list_events(&EventsQuery { after: 1, ..EventsQuery::default() }).await,
            Err(ClientError::ResyncRequired { oldest_available_seq: 3, latest_seq: 3, .. })
        ));
        let page = client.list_events(&EventsQuery { after: 2, ..EventsQuery::default() }).await?;
        assert_eq!(page.items.iter().map(|event| event.seq).collect::<Vec<_>>(), [3]);
        assert_eq!((page.oldest_available_seq, page.latest_seq, page.next_seq), (3, 3, 3));
        Ok::<(), TestError>(())
    }.await;
    shutdown.send_replace(true);
    task.await??;
    assertions
}

async fn publish_blob_event(db: &DbActor, name: &'static str) -> TestResult {
    let now = now_ms()?;
    db.write_sqlite(move |tx| {
        let event = append_event(
            tx,
            now,
            EventKind::BlobStored,
            AggregateType::Blob,
            name,
            None,
            json!({}),
        )?;
        Ok(WriteOutcome::with_events((), vec![event]))
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn multi_kind_pages_preserve_strict_seq_order_and_byte_budget() -> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.open().await?;
    node.engine
        .db()
        .write_sqlite(|tx| {
            let mut events = Vec::new();
            let sequence = [
                (EventKind::BlobStored, AggregateType::Blob),
                (EventKind::NodeStatus, AggregateType::Node),
                (EventKind::BlobDeleted, AggregateType::Blob),
                (EventKind::MessageCreated, AggregateType::Message),
                (EventKind::BlobStored, AggregateType::Blob),
                (EventKind::MessageDeliveryUpdated, AggregateType::Message),
            ];
            for (index, (kind, agg)) in sequence.into_iter().enumerate() {
                events.push(append_event(
                    tx,
                    index as i64,
                    kind,
                    agg,
                    &format!("agg-{index}"),
                    None,
                    json!({ "idx": index }),
                )?);
            }
            Ok(WriteOutcome::with_events((), events))
        })
        .await?;
    let kinds = parse_kinds(Some("blob.stored,blob.deleted,message.created"))?;
    let Snapshot::Page(first) = read_page(
        node.engine.reads(),
        &node.info.node_id,
        Cursor::Open(0),
        kinds.as_deref(),
        2,
    )
    .await?
    else {
        return Err(io_error("Expected page 1"));
    };
    assert_eq!(
        first.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [1, 3]
    );
    assert_eq!(
        (first.next_seq, first.latest_seq, first.has_more),
        (3, 6, true)
    );

    let Snapshot::Page(second) = read_page(
        node.engine.reads(),
        &node.info.node_id,
        Cursor::Open(first.next_seq),
        kinds.as_deref(),
        10,
    )
    .await?
    else {
        return Err(io_error("Expected page 2"));
    };
    assert_eq!(
        second.items.iter().map(|e| e.seq).collect::<Vec<_>>(),
        [4, 5]
    );
    assert_eq!(
        (second.next_seq, second.latest_seq, second.has_more),
        (6, 6, false)
    );
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn stream_advances_cursor_across_non_matching_notices_before_reading_matching_event()
-> TestResult {
    let scratch = Scratch::new()?;
    let node = scratch.open().await?;
    let receiver = node.engine.db().subscribe();
    let (_shutdown, stopping) = watch::channel(false);
    let now = Instant::now();
    let mut stream = StreamState {
        reads: node.engine.reads().clone(),
        node: node.info.node_id.clone(),
        kinds: Some(vec![EventKind::MessageCreated]),
        token: TokenId::generate()?,
        events: receiver,
        shutdown: stopping,
        cursor: 0,
        pending: VecDeque::new(),
        catch_up: false,
        authenticate_at: now + AUTH_INTERVAL,
        ping_at: now + Duration::from_millis(50),
    };
    // Directly verify handle_notice advances cursor across consecutive non-matching notices
    // without setting catch_up, and sets catch_up on a matching kind or sequence gap.
    for seq in 1..=3 {
        stream.handle_notice(EventNotice {
            seq,
            kind: EventKind::BlobStored,
        });
        assert_eq!(stream.cursor, seq);
        assert!(!stream.catch_up);
    }
    // Stale notices are ignored.
    stream.handle_notice(EventNotice {
        seq: 2,
        kind: EventKind::MessageCreated,
    });
    assert_eq!(stream.cursor, 3);
    assert!(!stream.catch_up);
    // Reset to 0 and exercise end-to-end live notice flow via broadcast + next_frame.
    stream.cursor = 0;

    node.engine
        .db()
        .write_sqlite(|tx| {
            let mut events = Vec::new();
            for index in 0..3 {
                events.push(append_event(
                    tx,
                    index,
                    EventKind::BlobStored,
                    AggregateType::Blob,
                    "ignored",
                    None,
                    json!({}),
                )?);
            }
            Ok(WriteOutcome::with_events((), events))
        })
        .await?;
    // Drain the 3 live broadcast notices directly and prove cursor advances without SQL catch-up.
    for expected in 1..=3 {
        let notice = tokio::time::timeout(Duration::from_secs(2), stream.events.recv()).await??;
        stream.handle_notice(notice);
        assert_eq!(stream.cursor, expected);
        assert!(!stream.catch_up);
    }
    // Next frame should be Ping (after draining 3 consecutive non-matching EventNotices),
    // with cursor already advanced to 3 without loading ignored events into pending.
    let ping = tokio::time::timeout(Duration::from_secs(2), stream.next_frame()).await??;
    assert!(matches!(ping, Some(EventStreamFrame::Ping {})));
    assert_eq!(stream.cursor, 3);
    assert!(stream.pending.is_empty());

    // A sequence gap or matching notice flips catch_up without advancing cursor prematurely.
    stream.handle_notice(EventNotice {
        seq: 5,
        kind: EventKind::BlobStored,
    });
    assert_eq!(stream.cursor, 3);
    assert!(stream.catch_up);
    stream.catch_up = false;

    // Now publish a matching event at seq 4 and verify it arrives cleanly.
    node.engine
        .db()
        .write_sqlite(|tx| {
            let event = append_event(
                tx,
                4,
                EventKind::MessageCreated,
                AggregateType::Message,
                "matched",
                None,
                json!({}),
            )?;
            Ok(WriteOutcome::with_events((), vec![event]))
        })
        .await?;
    let matched = tokio::time::timeout(Duration::from_secs(2), stream.next_frame()).await??;
    assert!(matches!(
        matched,
        Some(EventStreamFrame::Event { event }) if event.seq == 4 && event.kind == EventKind::MessageCreated
    ));
    assert_eq!(stream.cursor, 4);
    node.engine.shutdown().await?;
    Ok(())
}
