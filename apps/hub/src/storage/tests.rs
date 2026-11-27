use std::{
    convert::Infallible,
    fs::{self, Permissions},
    io,
    os::unix::fs::{PermissionsExt, symlink},
    path::{Path, PathBuf},
};

use cooplet_protocol::{
    AggregateType, BlobId, ErrorCode, EventKind, HubId, NodeId, ProblemDetails,
};
use cooplet_storage::{BlobInput, DEFAULT_MAX_BLOB_BYTES, GcHooks};
use rusqlite::{Transaction, params};
use serde_json::json;

use super::{
    events::{EventAudience, FrameTooLarge, NewEvent, StoredEvent, append},
    gc::HubGcHooks,
    *,
};

type TestResult = StartupResult<()>;

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> StartupResult<Self> {
        let path = std::env::temp_dir().join(format!("cooplet-hub-test-{}", HubId::generate()?));
        Ok(Self(PrivateFs::ensure_private_dir(&path)?))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn open_default(state_dir: &Path) -> StartupResult<HubStorage> {
    open(
        state_dir,
        DEFAULT_MAX_BLOB_BYTES,
        10 * 1024 * 1024 * 1024,
        1_048_576,
    )
    .await
}

#[tokio::test]
async fn gate_sqlite_hygiene() -> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    hub.engine.db().write_sqlite(|tx| {
        let mut statement = tx.prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name <> 'sqlite_sequence' ORDER BY name",
        )?;
        let tables = statement.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert_eq!(tables, [
            "auth_tokens", "blobs", "events", "hub_messages", "hub_meta", "node_participants", "nodes",
            "operations", "principals", "thread_members", "thread_pins", "thread_posts", "threads",
        ]);
        let triggers = tx.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger'", [], |row| row.get::<_, u32>(0))?;
        assert_eq!(triggers, 0);
        let read_pragma = |name| tx.pragma_query_value(None, name, |row| row.get::<_, u32>(0));
        assert_eq!(read_pragma("user_version")?, 1);
        assert_eq!(read_pragma("foreign_keys")?, 1);
        assert_eq!(read_pragma("auto_vacuum")?, 2);
        assert_eq!(read_pragma("max_page_count")?, 1_048_576);
        let journal: String = tx.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
        assert_eq!(journal, "wal");
        Ok(WriteOutcome::new(()))
    }).await?;
    hub.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn repeated_init_opens_a_database_larger_than_its_default_budget() -> TestResult {
    let scratch = Scratch::new()?;
    let initialized = init(&scratch.0, "Original Hub").await?;
    let mut config = StorageConfig::new(scratch.0.join(DATABASE));
    // Scale down the runtime page budget so a freshly migrated Hub exceeds it.
    config.max_page_count = 1;
    assert!(matches!(
        open(&scratch.0, DEFAULT_MAX_BLOB_BYTES, 10 * 1024 * 1024 * 1024, 1).await,
        Err(error) if matches!(error.downcast_ref::<StorageError>(), Some(StorageError::PageLimit { .. }))
    ));

    let engine = open_existing_for_init(config, scratch.0.clone()).await?;
    let repeated = initialize_locked(&scratch.0, "Other Name", &engine).await?;
    assert_eq!(repeated.hub_id, initialized.hub_id);
    assert!(repeated.token_path.is_none());
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn bootstrap_is_private_idempotent_and_cleans_interrupted_staging() -> TestResult {
    let scratch = Scratch::new()?;
    let staging = scratch
        .0
        .join(".tmp.hub.master.key.00000000000000000000000000000000");
    fs::write(&staging, b"interrupted")?;
    let first = init(&scratch.0, "First Hub").await?;
    assert_eq!(first.token_path, Some(scratch.0.join(ADMIN_TOKEN)));
    assert!(!staging.exists());
    for filename in [MASTER_KEY, ADMIN_TOKEN] {
        assert_eq!(
            fs::metadata(scratch.0.join(filename))?.permissions().mode() & 0o7777,
            0o600
        );
    }
    assert_eq!(
        PrivateFs::read_private_file(&scratch.0.join(MASTER_KEY), 32)?.len(),
        32
    );
    let token = PrivateFs::read_private_file(&scratch.0.join(ADMIN_TOKEN), 256)?;
    assert!(token.starts_with(b"cplt_tok_") && token.ends_with(b"\n"));
    fs::remove_file(scratch.0.join(ADMIN_TOKEN))?;
    let second = init(&scratch.0, "Replacement Name").await?;
    assert_eq!(first.hub_id, second.hub_id);
    assert!(second.token_path.is_none());
    assert!(!scratch.0.join(ADMIN_TOKEN).exists());
    let hub = open_default(&scratch.0).await?;
    assert_eq!(hub.meta.display_name, "First Hub");
    assert_eq!(hub.meta.hub_id, first.hub_id);
    hub.engine
        .reads()
        .read(|connection| {
            let (tokens, verifier_len): (u64, u32) = connection.query_row(
                "SELECT COUNT(*), COALESCE(MAX(length(verifier_hmac)), 0) FROM auth_tokens
             WHERE token_kind = 'bearer_api' AND is_admin = 1 AND expires_at_ms = ?1",
                [i64::MAX],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!((tokens, verifier_len), (1, 64));
            Ok(())
        })
        .await?;
    hub.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn init_rejects_partial_state_before_opening_missing_or_empty_database() -> TestResult {
    for (zero_byte_database, state_file) in [
        (false, MASTER_KEY),
        (true, ADMIN_TOKEN),
        (false, "foreign-data"),
        (true, "hub.sqlite3-wal"),
    ] {
        let scratch = Scratch::new()?;
        let database = scratch.0.join(DATABASE);
        if zero_byte_database {
            write_private(&database, b"")?;
        }
        fs::write(scratch.0.join(state_file), b"existing")?;
        assert!(init(&scratch.0, "Test").await.is_err());
        assert_eq!(
            metadata(&database)?.map(|m| m.len()),
            zero_byte_database.then_some(0)
        );
        assert!(!scratch.0.join("hub.lock").exists());
        assert_eq!(fs::read(scratch.0.join(state_file))?, b"existing");
    }
    for path in [
        "database.owner",
        "staging/quarantined",
        "objects/quarantined",
    ] {
        let scratch = Scratch::new()?;
        let blobs = PrivateFs::ensure_private_dir(&scratch.0.join(BLOB_DIR))?;
        let file = blobs.join(path);
        if let Some(parent) = file.parent() {
            PrivateFs::ensure_private_dir(parent)?;
        }
        fs::write(&file, b"left over")?;
        assert!(init(&scratch.0, "Test").await.is_err());
        assert!(!scratch.0.join(DATABASE).exists());
        assert_eq!(fs::read(file)?, b"left over");
    }
    let scratch = Scratch::new()?;
    let actual = PrivateFs::ensure_private_dir(&scratch.0.join("actual"))?;
    let state = PrivateFs::ensure_private_dir(&scratch.0.join("state"))?;
    symlink(&actual, state.join(BLOB_DIR))?;
    assert!(init(&state, "Test").await.is_err());
    assert!(!state.join(DATABASE).exists());
    fs::set_permissions(&actual, Permissions::from_mode(0o755))?;
    assert!(init(&actual, "Test").await.is_err());
    assert!(!actual.join(DATABASE).exists());
    Ok(())
}

#[tokio::test]
async fn init_rejects_foreign_or_populated_uninitialized_databases_without_mutation() -> TestResult
{
    for (version, setup, reason) in [
        (
            0,
            "CREATE TABLE ledger (amount INTEGER); INSERT INTO ledger VALUES (42);",
            "version 0 Hub database must have an empty schema",
        ),
        (
            MIGRATION_V1.version,
            "INSERT INTO principals (principal_id,kind,username,display_name,created_at_ms,updated_at_ms)
             VALUES ('retained','human_member','existing','Existing',0,0);",
            "partial Hub state",
        ),
    ] {
        let scratch = Scratch::new()?;
        let path = scratch.0.join(DATABASE);
        let database = existing_database(&path, version)?;
        database.execute_batch(setup)?;
        drop(database);
        let before = fs::read(&path)?;
        let Err(error) = init(&scratch.0, "Test").await else {
            return Err(io::Error::other("initialized a populated unowned database").into());
        };
        assert!(error.to_string().contains(reason), "{error}");
        assert!(fs::read(&path)? == before);
        assert!(!scratch.0.join(MASTER_KEY).exists());
        assert!(!scratch.0.join(ADMIN_TOKEN).exists());
        assert!(!scratch.0.join(BLOB_DIR).exists());
    }
    Ok(())
}

#[tokio::test]
async fn init_can_resume_an_empty_unfinished_migration() -> TestResult {
    for version in [0, MIGRATION_V1.version] {
        let scratch = Scratch::new()?;
        let database = existing_database(&scratch.0.join(DATABASE), version)?;
        drop(database);
        let initialized = init(&scratch.0, "Test").await?;
        assert_eq!(initialized.token_path, Some(scratch.0.join(ADMIN_TOKEN)));
        let hub = open_default(&scratch.0).await?;
        assert_eq!(hub.meta.hub_id, initialized.hub_id);
        hub.engine.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn both_opens_reject_v1_with_expected_table_names_but_changed_schema() -> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test").await?;
    let path = scratch.0.join(DATABASE);
    let database = Connection::open(&path)?;
    database.execute("ALTER TABLE principals ADD COLUMN extra TEXT", [])?;
    drop(database);
    let before = fs::read(&path)?;
    let master = fs::read(scratch.0.join(MASTER_KEY))?;
    let token = fs::read(scratch.0.join(ADMIN_TOKEN))?;
    let Err(error) = init(&scratch.0, "Test").await else {
        return Err(io::Error::other("accepted an incompatible Hub schema during init").into());
    };
    assert!(
        error
            .to_string()
            .contains("schema does not match migration v1")
    );
    let Err(error) = open_default(&scratch.0).await else {
        return Err(io::Error::other("accepted an incompatible Hub schema during run").into());
    };
    assert!(
        error
            .to_string()
            .contains("schema does not match migration v1")
    );
    assert!(fs::read(&path)? == before);
    assert!(fs::read(scratch.0.join(MASTER_KEY))? == master);
    assert!(fs::read(scratch.0.join(ADMIN_TOKEN))? == token);
    assert!(!scratch.0.join(BLOB_DIR).exists());
    Ok(())
}

fn existing_database(path: &Path, version: u32) -> StartupResult<Connection> {
    let database = Connection::open(path)?;
    fs::set_permissions(path, Permissions::from_mode(0o600))?;
    database.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
    database.execute_batch("VACUUM")?;
    if version == MIGRATION_V1.version {
        database.execute_batch(MIGRATION_V1.sql)?;
        database.pragma_update(None, "user_version", version)?;
    }
    Ok(database)
}

#[tokio::test]
async fn run_checks_identity_before_managed_blob_recovery() -> TestResult {
    for initialized in [true, false] {
        let scratch = Scratch::new()?;
        if initialized {
            init(&scratch.0, "Test Hub").await?;
        } else {
            StorageEngine::open(
                StorageConfig::new(scratch.0.join(DATABASE)),
                &[MIGRATION_V1],
            )
            .await?
            .shutdown()
            .await?;
        }
        let wrong_key = [7; 32];
        write_private(&scratch.0.join(MASTER_KEY), &wrong_key)?;
        let blobs = PrivateFs::ensure_private_dir(&scratch.0.join(BLOB_DIR))?;
        let staging = PrivateFs::ensure_private_dir(&blobs.join("staging"))?;
        let objects = PrivateFs::ensure_private_dir(&blobs.join("objects"))?;
        let staged = staging.join("00000000000000000000000000000000.part");
        let object = objects.join("sentinel");
        fs::write(&staged, b"staging bytes")?;
        fs::write(&object, b"object bytes")?;
        let outcome = open_default(&scratch.0).await;
        let Err(error) = outcome else {
            return Err(io::Error::other("opened an invalid Hub").into());
        };
        let diagnostic = error.to_string();
        assert!(diagnostic.contains(if initialized {
            "master key does not match"
        } else {
            "identity is missing"
        }));
        assert_eq!(fs::read(staged)?, b"staging bytes");
        assert_eq!(fs::read(object)?, b"object bytes");
        assert!(!blobs.join("database-owner.lock").exists());
    }
    Ok(())
}

fn write_private(path: &Path, data: &[u8]) -> io::Result<()> {
    fs::write(path, data)?;
    fs::set_permissions(path, Permissions::from_mode(0o600))
}

fn insert_node(tx: &Transaction<'_>, node_id: &str, status: &str) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO nodes (node_id, display_name, owner_principal_id, credential_hmac, status, session_epoch, enrolled_at_ms)
         SELECT ?1, 'Node', principal_id, ?1, ?2, 7, 0 FROM principals", params![node_id, status],
    )?;
    Ok(())
}

#[tokio::test]
async fn restart_preserves_terminal_facts_and_atomically_recovers_interrupted_delivery()
-> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    let node_a = NodeId::from_parts(1, [1; 10])?;
    let node_b = NodeId::from_parts(1, [2; 10])?;
    let node_a_for_write = node_a.clone();
    let node_b_for_write = node_b.clone();
    hub.engine.db().write_sqlite(move |tx| {
        insert_node(tx, node_a_for_write.as_str(), "online")?;
        insert_node(tx, node_b_for_write.as_str(), "revoked")?;
        tx.execute_batch(
            r#"INSERT INTO operations (operation_id, actor_id, operation_key, kind, request_sha256, state,
                                         resource_id, result_json, created_at_ms, updated_at_ms, expires_at_ms)
               VALUES ('op_accepted', 'actor', 'first-op', 'answer_question', 'hash', 'accepted', 'msg_answer', '{"in_reply_to":"msg_parent"}', 0, 0, 9999999999999),
                      ('op_in_progress', 'actor', 'second-op', 'create', 'hash', 'in_progress', NULL, NULL, 0, 0, 9999999999999),
                      ('op_terminal', 'actor', 'third-op', 'send', 'hash', 'succeeded', 'msg_queued', '{}', 0, 0, 9999999999999);"#,
        )?;
        tx.execute(
            "INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, sender_node_id,
                                       target_node_id, body, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             VALUES ('msg_00000000000000000000000001', 'sender', 'target', ?1, ?2, 'parent', 'direct_now', 'delivered', 0, 9999999999999),
                    ('msg_00000000000000000000000003', 'sender', 'target', NULL, ?2, 'queued', 'queue_until_idle', 'queued', 0, 9999999999999)",
            params![node_a_for_write.as_str(), node_b_for_write.as_str()],
        )?;
        tx.execute(
            "INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, target_node_id,
                                       in_reply_to, body, answer_json, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             VALUES ('msg_00000000000000000000000002', 'sender', 'target', ?1,
                     'msg_00000000000000000000000001', 'answer', '{}', 'direct_now', 'dispatched', 0, 9999999999999)",
            [node_a_for_write.as_str()],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    hub.engine.shutdown().await?;
    let hub = open_default(&scratch.0).await?;
    hub.engine
        .reads()
        .read(move |connection| {
            let mut nodes = connection.prepare("SELECT status FROM nodes ORDER BY node_id")?;
            let actual = nodes
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(actual, ["offline", "revoked"]);
            let mut ops = connection.prepare(
                "SELECT state, result_json, error_json FROM operations ORDER BY operation_id",
            )?;
            let mut rows = ops.query([])?;
            let accepted = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            assert_eq!(accepted.get::<_, String>(0)?, "unknown");
            assert_eq!(
                accepted.get::<_, String>(1)?,
                r#"{"in_reply_to":"msg_parent"}"#
            );
            let details: ProblemDetails = serde_json::from_str(&accepted.get::<_, String>(2)?)
                .map_err(events::encode_error)?;
            assert_eq!(
                (details.code, details.status, details.retryable),
                (ErrorCode::DeliveryUnknown, 503, false)
            );
            let in_progress = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            assert_eq!(in_progress.get::<_, String>(0)?, "unknown");
            let terminal = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            assert_eq!(terminal.get::<_, String>(0)?, "succeeded");
            assert_eq!(terminal.get::<_, Option<String>>(2)?, None);
            let mut messages = connection
                .prepare("SELECT delivery_state FROM hub_messages ORDER BY message_id")?;
            let actual = messages
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            assert_eq!(actual, ["delivered", "unknown", "queued"]);
            let mut events =
                connection.prepare("SELECT kind, payload_json FROM events ORDER BY seq")?;
            let mut rows = events.query([])?;
            let disconnected = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            assert_eq!(
                disconnected.get::<_, String>(0)?,
                EventKind::NodeDisconnected.as_str()
            );
            let delivery = rows.next()?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            assert_eq!(
                delivery.get::<_, String>(0)?,
                EventKind::MessageDeliveryUpdated.as_str()
            );
            let delivery: StoredEvent = serde_json::from_str(&delivery.get::<_, String>(1)?)
                .map_err(super::events::encode_error)?;
            assert_eq!(delivery.data["target_node_id"], json!(node_a));
            assert_eq!(
                delivery.audience,
                EventAudience::Nodes {
                    node_ids: vec![node_a, node_b]
                }
            );
            assert!(rows.next()?.is_none());
            let head: u64 =
                connection
                    .query_row("SELECT events_head_seq FROM hub_meta", [], |row| row.get(0))?;
            assert_eq!(head, 2);
            Ok(())
        })
        .await?;
    hub.engine.shutdown().await?;
    let hub = open_default(&scratch.0).await?;
    let (_, head, rows) = watermarks(&hub).await?;
    assert_eq!((head, rows), (2, 2));
    hub.engine.shutdown().await?;
    Ok(())
}

async fn watermarks(hub: &HubStorage) -> Result<(u64, u64, u64), StorageError> {
    hub.engine.reads().read(|connection| {
        Ok(connection.query_row(
            "SELECT events_pruned_through_seq, events_head_seq, (SELECT COUNT(*) FROM events) FROM hub_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?)
    }).await
}

const DAY: i64 = 86_400_000;

#[test]
fn blob_event_sql_error_rolls_back_the_blob_and_yields_gc_to_event_pruning() -> TestResult {
    let mut database = Connection::open_in_memory()?;
    database.execute_batch(MIGRATION_V1.sql)?;
    let hub_id = HubId::generate()?;
    let blob_id = BlobId::generate()?;
    database.execute(
        "INSERT INTO hub_meta (id, hub_id, display_name, master_key_fingerprint, events_head_seq,
                               created_at_ms, updated_at_ms) VALUES (1, ?1, 'Hub', 'key', 1, 0, 0)",
        [hub_id.as_str()],
    )?;
    database.execute(
        "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name, safe_disk_name,
                            disk_rel_path, owner_actor_id, created_at_ms, expires_at_ms)
         VALUES (?1, 'sha', 1, 'text/plain', 'file', 'file', 'file', 'owner', 0, 1)",
        [blob_id.as_str()],
    )?;
    database.execute_batch(
        "INSERT INTO events (seq, event_id, kind, aggregate_type, aggregate_id, payload_json, created_at_ms)
         VALUES (1, 'old', 'node.status', 'node', 'node', '{}', 0);
         UPDATE sqlite_sequence SET seq = 9223372036854775807 WHERE name = 'events';",
    )?;

    let gc = HubGcHooks::default();
    let failed = {
        let tx = database.transaction()?;
        gc.prune_batch(&tx, 14 * DAY, 200)
    };
    assert!(
        matches!(failed, Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull))
    );
    let before: (Option<i64>, u64, u64) = database.query_row(
        "SELECT (SELECT deleted_at_ms FROM blobs), events_pruned_through_seq, (SELECT COUNT(*) FROM events) FROM hub_meta",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(before, (None, 0, 1));

    let tx = database.transaction()?;
    assert!(gc.prune_batch(&tx, 14 * DAY, 200)?.has_more);
    tx.commit()?;
    let after: (Option<i64>, u64, u64) = database.query_row(
        "SELECT (SELECT deleted_at_ms FROM blobs), events_pruned_through_seq, (SELECT COUNT(*) FROM events) FROM hub_meta",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(after, (None, 1, 0));
    Ok(())
}

#[tokio::test]
async fn prune_advances_floor_only_for_a_committed_contiguous_prefix() -> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    hub.engine.db().write_sqlite(|tx| {
        tx.execute_batch(
            "INSERT INTO events (seq, event_id, kind, aggregate_type, aggregate_id, payload_json, created_at_ms)
             VALUES (1, 'old', 'node.status', 'node', 'node', '{}', 0),
                    (400002, 'live', 'node.status', 'node', 'node', '{}', 864000000),
                    (600003, 'older', 'node.status', 'node', 'node', '{}', -1);
             UPDATE hub_meta SET events_head_seq = 600003;",
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    let result = hub
        .engine
        .db()
        .write_sqlite::<(), _>(|tx| {
            let gc = HubGcHooks::default();
            assert!(gc.prune_batch(tx, 14 * DAY, 200)?.has_more);
            let floor: u64 = tx.query_row(
                "SELECT events_pruned_through_seq FROM hub_meta",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(floor, 1);
            Err(rusqlite::Error::InvalidQuery)
        })
        .await;
    assert!(result.is_err());
    assert_eq!(watermarks(&hub).await?, (0, 600003, 3));
    hub.engine.gc_once(14 * DAY).await?;
    assert_eq!(watermarks(&hub).await?, (1, 600003, 2));
    hub.engine.gc_once(24 * DAY).await?;
    assert_eq!(watermarks(&hub).await?, (600003, 600003, 0));
    hub.engine.gc_once(25 * DAY).await?;
    assert_eq!(watermarks(&hub).await?, (600003, 600003, 0));
    hub.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn invalid_event_frame_rolls_back_event_and_watermark() -> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    let hub_id = hub.meta.hub_id.clone();
    let result = hub
        .engine
        .db()
        .write_sqlite(move |tx| {
            let event = append(
                tx,
                &hub_id,
                NewEvent {
                    kind: EventKind::NodeStatus,
                    aggregate_type: AggregateType::Node,
                    aggregate_id: "node",
                    actor_id: None,
                    created_at_ms: 1,
                    data: json!({"status": "x".repeat(1024 * 1024)}),
                    audience: EventAudience::GlobalArchiveOnly,
                },
            )?;
            Ok(WriteOutcome::with_events((), vec![event]))
        })
        .await;
    assert!(
        matches!(result, Err(StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(error))) if error.is::<FrameTooLarge>())
    );
    assert_eq!(watermarks(&hub).await?, (0, 0, 0));
    hub.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn gc_preserves_single_answer_after_receipt_expiry_and_bounds_reply_detachment() -> TestResult
{
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    let node = NodeId::generate()?;
    hub.engine.db().write_sqlite(move |tx| {
        insert_node(tx, node.as_str(), "offline")?;
        tx.execute_batch(
            "INSERT INTO operations (operation_id, actor_id, operation_key, kind, request_sha256, state,
                                     created_at_ms, updated_at_ms, expires_at_ms)
             VALUES ('receipt', 'actor', 'key', 'answer_question', 'hash', 'succeeded', 0, 0, 10);
             INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, target_node_id,
                                       body, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             SELECT 'question', 'sender', 'target', node_id, '', 'direct_now', 'delivered', 0, 100 FROM nodes;
             INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, in_reply_to,
                                       body, answer_json, operation_id, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             VALUES ('answer', 'sender', 'target', 'question', '', '{}', 'receipt', 'direct_now', 'delivered', 0, 100);
             WITH RECURSIVE numbers(i) AS (VALUES (1) UNION ALL SELECT i + 1 FROM numbers WHERE i < 250)
             INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, target_node_id,
                                       in_reply_to, body, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             SELECT 'reply_' || i, 'sender', 'target', node_id, 'question', '', 'direct_now', 'delivered', 0, 1000
             FROM numbers CROSS JOIN nodes;",
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    hub.engine.gc_once(11).await?;
    hub.engine.db().write_sqlite(|tx| {
        let answer_receipt: Option<String> = tx.query_row("SELECT operation_id FROM hub_messages WHERE message_id = 'answer'", [], |row| row.get(0))?;
        assert!(answer_receipt.is_none());
        let duplicate = tx.execute(
            "INSERT INTO hub_messages (message_id, sender_actor_id, target_participant_id, in_reply_to,
                                       body, answer_json, delivery_mode, delivery_state, created_at_ms, expires_at_ms)
             VALUES ('duplicate', 'sender', 'target', 'question', '', '{}', 'direct_now', 'delivered', 0, 100)",
            [],
        );
        assert!(matches!(duplicate, Err(error) if error.sqlite_error_code() == Some(rusqlite::ErrorCode::ConstraintViolation)));
        let batch = HubGcHooks::default().prune_batch(tx, 100, 200)?;
        assert!(batch.has_more);
        let children: u64 = tx.query_row("SELECT COUNT(*) FROM hub_messages WHERE in_reply_to = 'question'", [], |row| row.get(0))?;
        assert!(children >= 150);
        Ok(WriteOutcome::new(()))
    }).await?;
    hub.engine.gc_once(100).await?;
    hub.engine
        .reads()
        .read(|db| {
            let (rows, links): (u64, u64) = db.query_row(
                "SELECT COUNT(*), COUNT(in_reply_to) FROM hub_messages",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!((rows, links), (250, 0));
            Ok(())
        })
        .await?;
    hub.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn expired_blobs_survive_startup_until_gc_commits_their_scoped_event() -> TestResult {
    let scratch = Scratch::new()?;
    init(&scratch.0, "Test Hub").await?;
    let hub = open_default(&scratch.0).await?;
    let node = NodeId::generate()?;
    let node_for_write = node.clone();
    hub.engine
        .db()
        .write_sqlite(move |tx| {
            insert_node(tx, node_for_write.as_str(), "offline")?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let mut expected = Vec::new();
    let store = hub
        .engine
        .blobs()
        .ok_or_else(|| io::Error::other("missing blob store"))?;
    for (attached_at_ms, node_id, audience) in [
        (
            Some(0),
            Some(node.to_string()),
            EventAudience::Nodes {
                node_ids: vec![node.clone()],
            },
        ),
        (Some(0), None, EventAudience::GlobalArchiveOnly),
        (
            None,
            None,
            EventAudience::OwnerOnly {
                actor_id: "owner".into(),
            },
        ),
    ] {
        let input = BlobInput {
            blob_id: BlobId::generate()?,
            original_name: "Résumé.txt".into(),
            expected_size: Some(5),
            expected_sha256: None,
        };
        let (stored, ()) = store
            .store::<_, _, Infallible, _>(input, &b"hello"[..], move |tx, stored| {
                tx.execute(
                    "INSERT INTO blobs (blob_id, sha256, size_bytes, mime_type, original_name,
                                    safe_disk_name, disk_rel_path, owner_actor_id, scope_node_id,
                                    attached_at_ms, created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, 'text/plain', ?4, ?5, ?6, 'owner', ?7, ?8, 0, 1)",
                    params![
                        stored.blob_id.as_str(),
                        stored.sha256,
                        stored.size_bytes,
                        stored.original_name,
                        stored.safe_disk_name,
                        stored.disk_rel_path.to_str(),
                        node_id,
                        attached_at_ms
                    ],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        expected.push((stored, audience));
    }
    hub.engine.shutdown().await?;
    let hub = open_default(&scratch.0).await?;
    for (stored, _) in &expected {
        assert_eq!(fs::read(&stored.absolute_path)?, b"hello");
    }
    let mut committed = hub.engine.db().subscribe();
    let sweep = hub.engine.gc_once(2).await?;
    assert_eq!(sweep.tombstones_completed, 3);
    assert_eq!(watermarks(&hub).await?, (0, 3, 3));
    for (stored, audience) in expected {
        assert!(!stored.absolute_path.exists());
        let id = stored.blob_id;
        hub.engine
            .reads()
            .read(move |connection| {
                let count = connection.query_row(
                    "SELECT COUNT(*) FROM blobs WHERE blob_id = ?1",
                    [id.as_str()],
                    |row| row.get::<_, u32>(0),
                )?;
                assert_eq!(count, 0);
                let json: String = connection.query_row(
                "SELECT payload_json FROM events WHERE kind = 'blob.deleted' AND aggregate_id = ?1",
                [id.as_str()],
                |row| row.get(0),
            )?;
                let stored: StoredEvent =
                    serde_json::from_str(&json).map_err(events::encode_error)?;
                assert_eq!(stored.audience, audience);
                assert_eq!(stored.data["original_name"], "Résumé.txt");
                Ok(())
            })
            .await?;
    }
    for seq in 1..=3 {
        let event = committed.try_recv()?;
        assert_eq!((event.seq, event.kind), (seq, EventKind::BlobDeleted));
        assert!(event.payload.get("audience").is_none());
    }
    hub.engine.shutdown().await?;
    Ok(())
}

#[test]
fn gc_filters_use_the_retention_and_recovery_indexes() -> TestResult {
    let mut db = Connection::open_in_memory()?;
    db.execute_batch(MIGRATION_V1.sql)?;
    let tx = db.transaction()?;
    tx.execute("INSERT INTO principals (principal_id,kind,username,display_name,created_at_ms,updated_at_ms) VALUES ('p','human_admin','admin','Admin',0,0)", [])?;
    tx.execute_batch(
        "WITH RECURSIVE n(seq) AS (VALUES (1) UNION ALL SELECT seq+1 FROM n WHERE seq < 100000)
         INSERT INTO auth_tokens (token_id,principal_id,token_kind,verifier_hmac,expires_at_ms,created_at_ms)
         SELECT 'token-'||seq,'p','bearer_api','verifier-'||seq,1000,0 FROM n;
         ANALYZE;",
    )?;
    let tokens = explain(&tx, gc::DELETE_EXPIRED_TOKENS, params![5, 200])?;
    for index in [
        "idx_auth_tokens_gc",
        "idx_auth_tokens_revoked",
        "idx_auth_tokens_consumed",
    ] {
        assert!(tokens.contains(index), "{tokens}");
    }
    let blobs = explain(&tx, gc::PENDING_BLOB_TOMBSTONES, params![5, "", 200])?;
    assert!(
        blobs.contains("USING INDEX idx_hub_blobs_gc_pending"),
        "{blobs}"
    );
    assert!(!blobs.contains("USE TEMP B-TREE"), "{blobs}");
    let messages = explain(&tx, recovery::RECOVERABLE_MESSAGES, [])?;
    assert!(
        messages.contains("USING INDEX idx_hub_messages_recovery"),
        "{messages}"
    );
    assert!(!messages.contains("SCAN"), "{messages}");
    Ok(())
}

fn explain(
    db: &Connection,
    query: &str,
    params: impl rusqlite::Params,
) -> rusqlite::Result<String> {
    let mut statement = db.prepare(&format!("EXPLAIN QUERY PLAN {query}"))?;
    let details = statement
        .query_map(params, |row| row.get::<_, String>(3))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(details.join("\n"))
}
