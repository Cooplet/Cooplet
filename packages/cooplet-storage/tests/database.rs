mod common;

use std::{
    fs::{self, Permissions},
    future::{Future, poll_fn},
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    sync::mpsc,
    task::Poll,
    time::Duration,
};

use cooplet_protocol::{
    AggregateType, EventEnvelope, EventId, EventKind, NodeId, SourceId, SourceKind,
};
use cooplet_storage::{
    Migration, StorageConfig, StorageEngine, StorageError, WriteError, WriteFailure, WriteOutcome,
};
use rusqlite::Connection;
use tokio::sync::oneshot;

use common::{Scratch, TestResult};

const BASE: Migration = Migration {
    version: 1,
    name: "base",
    sql: "CREATE TABLE rows(id INTEGER PRIMARY KEY, body BLOB);
          CREATE TABLE outbox(seq INTEGER PRIMARY KEY);
          CREATE TABLE dependencies(id INTEGER PRIMARY KEY);
          CREATE TABLE dependents(id INTEGER REFERENCES dependencies(id) DEFERRABLE INITIALLY DEFERRED);",
};

#[tokio::test]
async fn pragmas_and_one_sidecar_survive_orderly_restart() -> TestResult {
    let scratch = Scratch::new()?;
    let path = scratch.0.join("node.sqlite3");
    let config = StorageConfig::new(&path);
    let engine = StorageEngine::open(config.clone(), &[]).await?;
    assert!(scratch.0.join("node.lock").is_file());
    let alias = StorageConfig::new(scratch.0.join(".").join("node.sqlite3"));
    assert!(matches!(
        StorageEngine::open(alias, &[]).await,
        Err(StorageError::Platform(
            cooplet_platform::PlatformError::LockBusy
        ))
    ));

    let verify_rw = |db: &Connection| -> rusqlite::Result<()> {
        assert_eq!(
            db.query_row("PRAGMA auto_vacuum", [], |row| row.get::<_, u32>(0))?,
            2
        );
        assert_eq!(
            db.query_row("PRAGMA foreign_keys", [], |row| row.get::<_, u32>(0))?,
            1
        );
        assert_eq!(
            db.query_row("PRAGMA page_size", [], |row| row.get::<_, u32>(0))?,
            4096
        );
        assert_eq!(
            db.query_row("PRAGMA max_page_count", [], |row| row.get::<_, u32>(0))?,
            1_048_576
        );
        assert_eq!(
            db.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?,
            "wal"
        );
        Ok(())
    };
    engine
        .db()
        .write_sqlite(move |tx| {
            verify_rw(tx)?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    engine
        .reads()
        .read(|db| {
            assert_eq!(
                db.query_row("PRAGMA query_only", [], |row| row.get::<_, u32>(0))?,
                1
            );
            assert_eq!(
                db.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
                0
            );
            assert!(
                db.execute_batch("CREATE TABLE forbidden(id INTEGER)")
                    .is_err()
            );
            Ok(())
        })
        .await?;
    let stale_pool = engine.reads().clone();
    let stale_writer = engine.db().clone();
    engine.shutdown().await?;
    assert!(matches!(
        stale_pool.read(|_| Ok(())).await,
        Err(StorageError::Closed)
    ));
    assert!(matches!(
        stale_writer
            .write_sqlite(|_| Ok(WriteOutcome::new(())))
            .await,
        Err(StorageError::Closed)
    ));
    let reopened = StorageEngine::open(config, &[]).await?;
    reopened
        .db()
        .write_sqlite(move |tx| {
            verify_rw(tx)?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    reopened.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn incompatible_preflight_preserves_database_bytes_and_ddl() -> TestResult {
    let scratch = Scratch::new()?;
    for (index, extra) in [
        "PRAGMA user_version=99",
        "CREATE TRIGGER forbidden AFTER INSERT ON data BEGIN SELECT 1; END",
        "CREATE TABLE stale_storage_rows(x)",
        "CREATE TABLE Node_Storage(x)",
        "CREATE TABLE Hub_STORAGE(x)",
        "CREATE TABLE Stale_Storage_Rows(x)",
        "PRAGMA auto_vacuum=NONE; VACUUM",
    ]
    .into_iter()
    .enumerate()
    {
        let path = scratch.0.join(format!("case-{index}.sqlite3"));
        let connection = Connection::open(&path)?;
        connection.execute_batch(
            "PRAGMA auto_vacuum=INCREMENTAL; CREATE TABLE data(x); INSERT INTO data VALUES(42);",
        )?;
        connection.execute_batch(extra)?;
        drop(connection);
        fs::set_permissions(&path, Permissions::from_mode(0o600))?;
        let before = fs::read(&path)?;
        let rejected = StorageEngine::open(StorageConfig::new(&path), &[BASE]).await;
        assert!(
            matches!(rejected, Err(StorageError::IncompatibleDatabase(_))),
            "{extra}"
        );
        assert_eq!(before, fs::read(&path)?);
        let check = Connection::open(&path)?;
        assert_eq!(
            check.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))?,
            "delete"
        );
        assert_eq!(
            check.query_row("SELECT x FROM data", [], |row| row.get::<_, i32>(0))?,
            42
        );
    }
    let path = scratch.0.join("versioned.sqlite3");
    let config = StorageConfig::new(path);
    let version_two = Migration {
        version: 2,
        name: "invalid",
        sql: "CREATE TABLE next(id); CREATE TRIGGER invalid AFTER INSERT ON next BEGIN SELECT 1; END;",
    };
    assert!(matches!(
        StorageEngine::open(config.clone(), &[version_two]).await,
        Err(StorageError::InvalidConfig(_))
    ));
    assert!(matches!(
        StorageEngine::open(config.clone(), &[BASE, BASE]).await,
        Err(StorageError::InvalidConfig(_))
    ));
    assert!(matches!(
        StorageEngine::open(config.clone(), &[BASE, version_two]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    let mixed_case_table = Migration {
        version: 2,
        name: "forbidden mixed-case table",
        sql: "CREATE TABLE Node_Storage(id);",
    };
    assert!(matches!(
        StorageEngine::open(config.clone(), &[BASE, mixed_case_table]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    let engine = StorageEngine::open(config.clone(), &[BASE]).await?;
    engine
        .reads()
        .read(|connection| {
            assert_eq!(
                connection.query_row("PRAGMA user_version", [], |row| row.get::<_, u32>(0))?,
                1
            );
            assert_eq!(
                connection.query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name IN ('next','invalid','Node_Storage')",
                    [],
                    |row| row.get::<_, u32>(0)
                )?,
                0
            );
            Ok(())
        })
        .await?;
    engine.shutdown().await?;
    let mut too_small = config;
    too_small.max_page_count = 1;
    assert!(matches!(
        StorageEngine::open(too_small, &[BASE]).await,
        Err(StorageError::PageLimit { requested: 1, .. })
    ));
    Ok(())
}

#[tokio::test]
async fn cancelled_reader_holds_permit_and_leaves_no_snapshot() -> TestResult {
    let scratch = Scratch::new()?;
    let mut config = StorageConfig::new(scratch.0.join("node.sqlite3"));
    config.read_pool_size = 1;
    let engine = StorageEngine::open(config, &[BASE]).await?;
    let first = engine.reads().clone();
    let (reading, started) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let first = tokio::spawn(async move {
        first
            .read(move |connection| {
                connection.execute_batch("BEGIN")?;
                let count = connection
                    .query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?;
                assert_eq!(count, 0);
                let _ = reading.send(());
                wait.recv().map_err(std::io::Error::other)?;
                Err::<(), _>(StorageError::Closed)
            })
            .await
    });
    started.await?;
    first.abort();
    assert!(first.await.is_err());
    engine
        .db()
        .write_sqlite(|tx| {
            tx.execute("INSERT INTO rows VALUES(1, NULL)", [])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;

    let second = engine.reads().clone();
    let mut second = tokio::spawn(async move {
        second
            .read(|connection| {
                assert!(connection.is_autocommit());
                Ok(connection
                    .query_row("SELECT count(*) FROM rows", [], |row| row.get::<_, i64>(0))?)
            })
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut second)
            .await
            .is_err()
    );
    release.send(())?;
    assert_eq!(second.await??, 1);
    engine.shutdown().await?;
    Ok(())
}

fn event(seq: u64) -> Result<EventEnvelope, cooplet_protocol::IdError> {
    let node = NodeId::from_parts(0, [0; 10])?;
    Ok(EventEnvelope {
        seq,
        event_id: EventId::from_parts(seq, [0; 10])?,
        source_kind: SourceKind::Node,
        source_id: SourceId::Node(node.clone()),
        kind: EventKind::NodeStatus,
        aggregate_type: AggregateType::Node,
        aggregate_id: node.to_string(),
        actor_id: None,
        created_at_ms: 0,
        payload: Default::default(),
    })
}

#[tokio::test]
async fn accepted_write_survives_cancellation_and_broadcast_waits_for_commit() -> TestResult {
    let scratch = Scratch::new()?;
    let mut config = StorageConfig::new(scratch.0.join("node.sqlite3"));
    config.write_queue_capacity = 1;
    let engine = StorageEngine::open(config, &[BASE]).await?;
    let mut events = engine.db().subscribe();
    let writer = engine.db().clone();
    let (active, started) = oneshot::channel();
    let (release, wait) = mpsc::channel();
    let work = tokio::spawn(async move {
        writer
            .write::<_, std::io::Error, _>(move |_| {
                let _ = active.send(());
                wait.recv()
                    .map_err(|error| WriteError::Domain(std::io::Error::other(error)))?;
                Ok(WriteOutcome::new(()))
            })
            .await
    });
    started.await?;
    let committed = event(1)?;
    let mut admitted = Box::pin(engine.db().write_sqlite(move |tx| {
        tx.execute("INSERT INTO rows VALUES(1, NULL)", [])?;
        tx.execute("INSERT INTO outbox VALUES(1)", [])?;
        Ok(WriteOutcome::with_events((), vec![committed]))
    }));
    poll_fn(|context| {
        assert!(matches!(admitted.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    drop(admitted);
    assert!(matches!(
        engine
            .db()
            .write_sqlite(|_| Ok(WriteOutcome::new(())))
            .await,
        Err(StorageError::Overloaded)
    ));
    assert!(events.try_recv().is_err());
    release.send(())?;
    work.await??;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await??
            .seq,
        1
    );
    let rows = engine
        .reads()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT count(*) FROM rows) + (SELECT count(*) FROM outbox)",
                [],
                |row| row.get::<_, i32>(0),
            )?)
        })
        .await?;
    assert_eq!(rows, 2);

    let rejected = engine
        .db()
        .write(|tx| {
            tx.execute("INSERT INTO rows VALUES(2, NULL)", [])?;
            Err::<WriteOutcome<()>, _>(WriteError::Domain("rollback"))
        })
        .await;
    assert!(matches!(rejected, Err(WriteFailure::App("rollback"))));
    let invalid = event(2)?;
    let fail = engine
        .db()
        .write_sqlite(move |tx| {
            tx.execute("INSERT INTO dependents VALUES(900)", [])?;
            Ok(WriteOutcome::with_events((), vec![invalid]))
        })
        .await;
    assert!(matches!(fail, Err(StorageError::CommitFailed(_))));
    assert!(events.try_recv().is_err());
    let rows = engine
        .reads()
        .read(|connection| {
            Ok(connection.query_row(
                "SELECT (SELECT count(*) FROM rows) + (SELECT count(*) FROM dependents)",
                [],
                |row| row.get::<_, i32>(0),
            )?)
        })
        .await?;
    assert_eq!(rows, 1);
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn sqlite_full_inside_composed_write_rolls_back_and_stays_usable() -> TestResult {
    let scratch = Scratch::new()?;
    let mut config = StorageConfig::new(scratch.0.join("node.sqlite3"));
    config.max_page_count = 24;
    let engine = StorageEngine::open(config, &[BASE]).await?;
    let mut events = engine.db().subscribe();
    let wrong = event(1)?;
    let result = engine
        .db()
        .write::<_, &'static str, _>(move |tx| {
            tx.execute("INSERT INTO rows VALUES(1, NULL)", [])?;
            tx.execute("INSERT INTO rows VALUES(2, zeroblob(1048576))", [])?;
            Ok(WriteOutcome::with_events((), vec![wrong]))
        })
        .await;
    assert!(matches!(
        result,
        Err(WriteFailure::Storage(StorageError::StorageFull))
    ));
    assert!(events.try_recv().is_err());
    assert_eq!(
        engine
            .reads()
            .read(|connection| Ok(connection.query_row(
                "SELECT count(*) FROM rows",
                [],
                |row| row.get::<_, i32>(0),
            )?))
            .await?,
        0
    );
    engine
        .db()
        .write_sqlite(|tx| {
            tx.execute("INSERT INTO rows VALUES(1, NULL)", [])?;
            tx.execute("DELETE FROM rows WHERE id=1", [])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn shutdown_checkpoints_wal_after_readonly_queries() -> TestResult {
    let scratch = Scratch::new()?;
    let path = scratch.0.join("node.sqlite3");
    let mut config = StorageConfig::new(&path);
    config.wal_autocheckpoint_pages = 0;
    let engine = StorageEngine::open(config, &[BASE]).await?;
    engine
        .db()
        .write_sqlite(|tx| {
            tx.execute("INSERT INTO rows VALUES(1, zeroblob(8192))", [])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let count = engine
        .reads()
        .read(|db| Ok(db.query_row("SELECT count(*) FROM rows", [], |r| r.get::<_, i64>(0))?))
        .await?;
    assert_eq!(count, 1);
    let wal = path.with_extension("sqlite3-wal");
    assert!(wal.try_exists()? && fs::metadata(&wal)?.len() > 0);
    engine.shutdown().await?;
    assert!(!wal.try_exists()? || fs::metadata(&wal)?.len() == 0);
    assert!(!path.with_extension("sqlite3-shm").try_exists()?);
    let check = Connection::open(&path)?;
    assert_eq!(
        check.query_row("SELECT count(*) FROM rows", [], |r| r.get::<_, i64>(0))?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn rejects_insecure_database_modes_links_and_invalid_pragmas() -> TestResult {
    let scratch = Scratch::new()?;
    let path = scratch.0.join("node.sqlite3");
    let engine = StorageEngine::open(StorageConfig::new(&path), &[BASE]).await?;
    engine.shutdown().await?;
    assert_eq!(fs::metadata(&path)?.mode() & 0o7777, 0o600);

    // Non-0600 database is rejected.
    fs::set_permissions(&path, Permissions::from_mode(0o644))?;
    assert!(matches!(
        StorageEngine::open(StorageConfig::new(&path), &[BASE]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    fs::set_permissions(&path, Permissions::from_mode(0o600))?;

    // Symlink database is rejected.
    let link = scratch.0.join("alias.sqlite3");
    symlink(&path, &link)?;
    assert!(matches!(
        StorageEngine::open(StorageConfig::new(&link), &[BASE]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    fs::remove_file(&link)?;

    // Hardlinked database is rejected.
    let hard = scratch.0.join("hard.sqlite3");
    fs::hard_link(&path, &hard)?;
    assert!(matches!(
        StorageEngine::open(StorageConfig::new(&path), &[BASE]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    fs::remove_file(&hard)?;

    // wal_autocheckpoint_pages > i32::MAX is rejected.
    let mut bad_config = StorageConfig::new(&path);
    bad_config.wal_autocheckpoint_pages = (i32::MAX as u32) + 1;
    assert!(matches!(
        StorageEngine::open(bad_config, &[BASE]).await,
        Err(StorageError::InvalidConfig(_))
    ));

    // Migration ending its transaction early is rejected.
    let early_commit = Migration {
        version: 2,
        name: "early-commit",
        sql: "COMMIT; CREATE TABLE rogue(x INTEGER);",
    };
    assert!(matches!(
        StorageEngine::open(StorageConfig::new(&path), &[BASE, early_commit]).await,
        Err(StorageError::IncompatibleDatabase(_))
    ));
    Ok(())
}

#[tokio::test]
async fn reader_and_writer_panics_recover_cleanly_without_holding_sidecar() -> TestResult {
    let scratch = Scratch::new()?;
    let path = scratch.0.join("node.sqlite3");
    let mut config = StorageConfig::new(&path);
    config.read_pool_size = 1;
    let engine = StorageEngine::open(config.clone(), &[BASE]).await?;

    // A panicking reader discards its connection and returns its permit cleanly.
    let panicked_read = engine
        .reads()
        .read::<(), _>(|_| std::panic::resume_unwind(Box::new("intentional reader panic")))
        .await;
    assert!(matches!(panicked_read, Err(StorageError::Task(_))));
    assert_eq!(
        engine
            .reads()
            .read(|db| Ok(db.query_row("SELECT count(*) FROM rows", [], |r| r.get::<_, i64>(0))?))
            .await?,
        0
    );

    // A panicking writer unwinds Connection before FileLock, reports WriterPanicked on shutdown,
    // and allows immediate clean reopen.
    let panicked_write = engine
        .db()
        .write_sqlite::<(), _>(|_| std::panic::resume_unwind(Box::new("intentional writer panic")))
        .await;
    assert!(matches!(
        panicked_write,
        Err(StorageError::WriteOutcomeUnknown)
    ));
    assert!(matches!(
        engine.shutdown().await,
        Err(StorageError::WriterPanicked)
    ));
    let reopened = StorageEngine::open(config, &[BASE]).await?;
    reopened.shutdown().await?;
    Ok(())
}
