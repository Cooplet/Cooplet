mod common;

use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs, io,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll},
    time::Duration,
};

use cooplet_platform::PrivateFs;
use cooplet_protocol::BlobId;
use cooplet_storage::{
    BlobInput, BlobRecord, BlobState, BlobStoreConfig, BlobTombstone, GcBatchOutcome, GcHooks,
    Migration, StorageConfig, StorageEngine, StorageError, StoredBlob, WriteError, WriteFailure,
    WriteOutcome, safe_disk_name,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tokio::{
    io::{AsyncRead, AsyncWriteExt, ReadBuf},
    sync::oneshot,
};

use common::{Scratch, TestResult};

const SCHEMA: &[Migration] = &[Migration { version: 1, name: "fixtures", sql:
    "CREATE TABLE objects(id TEXT PRIMARY KEY, path TEXT NOT NULL, bytes INTEGER NOT NULL, deleted INTEGER);
     CREATE TABLE expiring(id INTEGER PRIMARY KEY, expires INTEGER NOT NULL, payload BLOB);
     CREATE INDEX expiring_by_expiry ON expiring(expires, id);"
}];

const HELLO_SHA: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

#[derive(Default)]
struct Registry {
    lookup_fails: AtomicBool,
    failure_before_ack: Mutex<Option<AckFailure>>,
    stale_candidate: Mutex<Option<BlobTombstone>>,
}

struct AckFailure {
    entered: oneshot::Sender<()>,
    release: mpsc::Receiver<()>,
}

impl GcHooks for Registry {
    fn prune_batch(
        &self,
        tx: &Transaction<'_>,
        now_ms: i64,
        limit: usize,
    ) -> rusqlite::Result<GcBatchOutcome> {
        let count = tx.execute(
            "DELETE FROM expiring WHERE id IN (SELECT id FROM expiring WHERE expires<=?1 ORDER BY expires,id LIMIT ?2)",
            params![now_ms, limit],
        )?;
        Ok(GcBatchOutcome {
            events: vec![],
            has_more: count == limit,
        })
    }

    fn fetch_pending_blob_tombstones(
        &self,
        tx: &Transaction<'_>,
        limit: usize,
    ) -> rusqlite::Result<Vec<BlobTombstone>> {
        let stale = self
            .stale_candidate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(stale) = stale {
            return Ok(vec![stale]);
        }
        let mut query = tx.prepare("SELECT id,path,bytes,deleted FROM objects WHERE deleted IS NOT NULL ORDER BY deleted,id LIMIT ?")?;
        query
            .query_map([limit], |row| {
                let text: String = row.get(0)?;
                let id = text.parse().map_err(|_| rusqlite::Error::InvalidQuery)?;
                Ok(BlobTombstone {
                    record: BlobRecord {
                        blob_id: id,
                        disk_rel_path: PathBuf::from(row.get::<_, String>(1)?),
                        size_bytes: row.get(2)?,
                    },
                    deleted_at_ms: row.get(3)?,
                })
            })?
            .collect()
    }

    fn ack_blob_tombstone(
        &self,
        tx: &Transaction<'_>,
        observed: &BlobTombstone,
    ) -> rusqlite::Result<bool> {
        let failure = self
            .failure_before_ack
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(failure) = failure {
            let _ = failure.entered.send(());
            let _ = failure.release.recv();
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(tx.execute(
            "DELETE FROM objects WHERE id=?1 AND path=?2 AND bytes=?3 AND deleted=?4",
            params![
                observed.record.blob_id.as_str(),
                observed.record.disk_rel_path.to_str(),
                observed.record.size_bytes,
                observed.deleted_at_ms
            ],
        )? == 1)
    }

    fn lookup_blobs(
        &self,
        connection: &Connection,
        ids: &[BlobId],
    ) -> rusqlite::Result<BTreeMap<BlobId, BlobState>> {
        if self.lookup_fails.load(Ordering::Acquire) {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let mut query = connection.prepare("SELECT path,bytes,deleted FROM objects WHERE id=?")?;
        ids.iter()
            .map(|id| {
                let record = query
                    .query_row([id.as_str()], |row| {
                        let record = BlobRecord {
                            blob_id: id.clone(),
                            disk_rel_path: PathBuf::from(row.get::<_, String>(0)?),
                            size_bytes: row.get(1)?,
                        };
                        let deleted: Option<i64> = row.get(2)?;
                        Ok(match deleted {
                            Some(deleted_at_ms) => BlobState::Tombstone(BlobTombstone {
                                record,
                                deleted_at_ms,
                            }),
                            None => BlobState::Live(record),
                        })
                    })
                    .optional()?
                    .unwrap_or(BlobState::Absent);
                Ok((id.clone(), record))
            })
            .collect()
    }
}

fn register(
    tx: &Transaction<'_>,
    blob: &StoredBlob,
) -> Result<WriteOutcome<()>, WriteError<Infallible>> {
    tx.execute(
        "INSERT INTO objects VALUES (?1,?2,?3,NULL)",
        params![
            blob.blob_id.as_str(),
            blob.disk_rel_path.to_str(),
            blob.size_bytes
        ],
    )?;
    Ok(WriteOutcome::new(()))
}

fn configs(scratch: &Scratch) -> (StorageConfig, BlobStoreConfig) {
    (
        StorageConfig::new(scratch.0.join("node.sqlite3")),
        BlobStoreConfig::new(scratch.0.join("blobs")),
    )
}

fn input(id: BlobId) -> BlobInput {
    BlobInput {
        blob_id: id,
        original_name: "../release-notes.txt".into(),
        expected_size: None,
        expected_sha256: None,
    }
}

async fn tombstone(engine: &StorageEngine, blob: &StoredBlob) -> TestResult {
    let id = blob.blob_id.clone();
    engine
        .db()
        .write_sqlite(move |tx| {
            tx.execute("UPDATE objects SET deleted=16 WHERE id=?", [id.as_str()])?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    Ok(())
}

fn no_parts(config: &BlobStoreConfig) -> io::Result<bool> {
    Ok(fs::read_dir(config.root.join("staging"))?.next().is_none())
}

#[tokio::test]
async fn stores_sealed_contents_only_after_valid_eof_and_checks_bounds() -> TestResult {
    let scratch = Scratch::new()?;
    let (config, mut blobs) = configs(&scratch);
    blobs.max_blob_bytes = 8;
    let registry = Arc::new(Registry::default());
    let engine = StorageEngine::open_managed(config, SCHEMA, registry, Some(blobs.clone())).await?;
    let store = engine.blobs().ok_or("missing blob store")?;

    assert_eq!(safe_disk_name(""), "file.bin");
    assert_eq!(safe_disk_name("\0\u{7f}"), "file.bin");
    assert_eq!(
        safe_disk_name(r"C:\notes\weekly report.md"),
        "weekly report.md"
    );
    assert_eq!(safe_disk_name("-a..b.tar.gz"), "_a._b.tar.gz");
    let name = safe_disk_name(&format!("{}.txt", "🙂".repeat(150)));
    assert!(name.ends_with(".txt"));
    assert!(name.chars().count() <= 120 && name.len() <= 255);
    assert_eq!(safe_disk_name(&name), name);

    let id = BlobId::generate()?;
    let mut valid = input(id.clone());
    valid.expected_size = Some(5);
    valid.expected_sha256 = Some(HELLO_SHA.into());
    let (stored, ()) = store.store(valid, &b"hello"[..], register).await?;
    assert_eq!(stored.original_name, "../release-notes.txt");
    assert_eq!(stored.safe_disk_name, "release-notes.txt");
    assert_eq!(stored.sha256, HELLO_SHA);
    assert_eq!(
        stored.disk_rel_path,
        PathBuf::from(format!("objects/2c/{id}/release-notes.txt"))
    );
    assert_eq!(stored.absolute_path.metadata()?.mode() & 0o777, 0o400);
    assert_eq!(fs::read(&stored.absolute_path)?, b"hello");
    assert!(store.open_blob(&id).await?.is_some());
    assert_eq!(store.used_bytes(), 5);

    let reusable = BlobId::generate()?;
    let mut wrong_hash = input(reusable.clone());
    wrong_hash.expected_sha256 = Some("0".repeat(64));
    assert!(matches!(
        store.store(wrong_hash, &b"hello"[..], register).await,
        Err(WriteFailure::Storage(StorageError::DigestMismatch))
    ));
    let mut short = input(reusable.clone());
    short.expected_size = Some(6);
    assert!(matches!(
        store.store(short, &b"hello"[..], register).await,
        Err(WriteFailure::Storage(StorageError::LengthMismatch { .. }))
    ));
    assert!(matches!(
        store
            .store(input(reusable.clone()), &b"123456789"[..], register)
            .await,
        Err(WriteFailure::Storage(StorageError::BlobTooLarge {
            limit_bytes: 8
        }))
    ));
    assert!(matches!(
        store
            .store(input(reusable.clone()), BrokenReader(false), register)
            .await,
        Err(WriteFailure::Storage(StorageError::Io(_)))
    ));
    assert!(matches!(
        store
            .store(input(id.clone()), &b"hello"[..], register)
            .await,
        Err(WriteFailure::Storage(StorageError::BlobCollision))
    ));
    assert!(store.open_blob(&reusable).await?.is_none());
    assert_eq!(store.used_bytes(), 5);
    assert!(no_parts(&blobs)?);
    engine.shutdown().await?;
    Ok(())
}

struct BrokenReader(bool);

impl AsyncRead for BrokenReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.0 {
            Poll::Ready(Err(io::Error::other("input failed before EOF")))
        } else {
            self.0 = true;
            buffer.put_slice(b"x");
            Poll::Ready(Ok(()))
        }
    }
}

#[tokio::test]
async fn pre_eof_abort_releases_quota_but_accepted_registration_survives_cancellation() -> TestResult
{
    let scratch = Scratch::new()?;
    let (config, mut blobs) = configs(&scratch);
    blobs.max_total_blob_bytes = 5;
    let registry = Arc::new(Registry::default());
    let engine = StorageEngine::open_managed(config, SCHEMA, registry, Some(blobs.clone())).await?;
    let store = engine.blobs().ok_or("missing blob store")?;
    let worker = store.clone();
    let aborted = BlobId::generate()?;
    let before_eof = aborted.clone();
    let (mut tx, rx) = tokio::io::duplex(16);
    let waiting = tokio::spawn(async move { worker.store(input(before_eof), rx, register).await });
    tx.write_all(b"hello").await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.used_bytes() != 5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let competitor = store
        .store(input(BlobId::generate()?), &b"a"[..], register)
        .await;
    assert!(matches!(
        competitor,
        Err(WriteFailure::Storage(StorageError::StorageFull))
    ));
    waiting.abort();
    assert!(waiting.await.is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.used_bytes() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    assert!(store.open_blob(&aborted).await?.is_none());
    assert!(no_parts(&blobs)?);

    let lost_response = BlobId::generate()?;
    let registering = lost_response.clone();
    let (entered, active) = oneshot::channel();
    let (commit, release) = mpsc::channel();
    let worker = store.clone();
    let task = tokio::spawn(async move {
        worker
            .store(input(registering), &b"hello"[..], move |tx, blob| {
                let registered = register(tx, blob)?;
                let _ = entered.send(());
                let _ = release.recv();
                Ok(registered)
            })
            .await
    });
    active.await?;
    assert!(store.open_blob(&lost_response).await?.is_none());
    task.abort();
    assert!(task.await.is_err());
    commit.send(())?;
    let previous = store.clone();
    engine.shutdown().await?;
    assert!(matches!(
        previous.open_blob(&lost_response).await,
        Err(StorageError::Closed)
    ));
    let mut stored_paths =
        fs::read_dir(blobs.root.join("objects/2c").join(lost_response.as_str()))?;
    assert_eq!(
        stored_paths
            .next()
            .transpose()?
            .ok_or("missing object")?
            .path()
            .metadata()?
            .len(),
        5
    );
    Ok(())
}

#[tokio::test]
async fn rollback_removes_its_file_and_repairs_crash_windows_on_reopen() -> TestResult {
    let scratch = Scratch::new()?;
    let (config, blobs) = configs(&scratch);
    let registry = Arc::new(Registry::default());
    let engine = StorageEngine::open_managed(
        config.clone(),
        SCHEMA,
        registry.clone(),
        Some(blobs.clone()),
    )
    .await?;
    let store = engine.blobs().ok_or("missing blob store")?;
    let rollback = BlobId::generate()?;
    let rejected = store
        .store(input(rollback.clone()), &b"hello"[..], |_, _| {
            Err::<WriteOutcome<()>, _>(WriteError::Domain("rejected"))
        })
        .await;
    assert!(matches!(rejected, Err(WriteFailure::App("rejected"))));
    assert!(store.open_blob(&rollback).await?.is_none());
    assert_eq!(store.used_bytes(), 0);
    assert!(
        !blobs
            .root
            .join("objects/2c")
            .join(rollback.as_str())
            .exists()
    );

    let (kept, ()) = store
        .store(input(BlobId::generate()?), &b"hello"[..], register)
        .await?;
    let (delete_after_commit, ()) = store
        .store(input(BlobId::generate()?), &b"hello"[..], register)
        .await?;
    let (ack_after_restart, ()) = store
        .store(input(BlobId::generate()?), &b"hello"[..], register)
        .await?;
    tombstone(&engine, &delete_after_commit).await?;
    tombstone(&engine, &ack_after_restart).await?;
    fs::remove_file(&ack_after_restart.absolute_path)?;
    engine.shutdown().await?;

    let orphan_dir = PrivateFs::ensure_private_dir(
        &blobs
            .root
            .join("objects/2c")
            .join(BlobId::generate()?.as_str()),
    )?;
    let orphan = orphan_dir.join("orphan.txt");
    fs::write(&orphan, b"hello")?;
    PrivateFs::seal_readonly(&fs::File::open(&orphan)?)?;
    let stale_part = blobs
        .root
        .join("staging/00000000000000000000000000000001.part");
    fs::hard_link(&orphan, &stale_part)?;

    let engine = StorageEngine::open_managed(
        config.clone(),
        SCHEMA,
        registry.clone(),
        Some(blobs.clone()),
    )
    .await?;
    assert!(kept.absolute_path.is_file());
    assert!(!delete_after_commit.absolute_path.exists());
    assert!(!ack_after_restart.absolute_path.exists());
    assert!(!orphan.exists());
    assert!(!stale_part.exists());
    let live = engine.blobs().ok_or("missing blob store")?;
    assert_eq!(live.used_bytes(), 5);
    assert_eq!(
        engine
            .reads()
            .read(
                |db| Ok(db.query_row("SELECT count(*) FROM objects", [], |row| row
                    .get::<_, i32>(0))?)
            )
            .await?,
        1
    );
    engine.shutdown().await?;

    registry.lookup_fails.store(true, Ordering::Release);
    assert!(
        StorageEngine::open_managed(
            config.clone(),
            SCHEMA,
            registry.clone(),
            Some(blobs.clone())
        )
        .await
        .is_err()
    );
    assert!(kept.absolute_path.is_file());
    registry.lookup_fails.store(false, Ordering::Release);
    let engine = StorageEngine::open_managed(config, SCHEMA, registry, Some(blobs)).await?;
    assert_eq!(engine.blobs().ok_or("missing blob store")?.used_bytes(), 5);
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn gc_holds_mutation_through_ack_and_revalidates_stale_candidates() -> TestResult {
    let scratch = Scratch::new()?;
    let (config, blobs) = configs(&scratch);
    let registry = Arc::new(Registry::default());
    let engine = Arc::new(
        StorageEngine::open_managed(config, SCHEMA, registry.clone(), Some(blobs.clone())).await?,
    );
    let store = engine.blobs().ok_or("missing blob store")?;
    let id = BlobId::generate()?;
    let (old, ()) = store
        .store(input(id.clone()), &b"hello"[..], register)
        .await?;
    let (other, ()) = store
        .store(input(BlobId::generate()?), &b"hello"[..], register)
        .await?;
    tombstone(&engine, &old).await?;
    let (entered, observed) = oneshot::channel();
    let (release, blocked) = mpsc::channel();
    *registry
        .failure_before_ack
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(AckFailure {
        entered,
        release: blocked,
    });
    let gc = engine.clone();
    let cycle = tokio::spawn(async move { gc.gc_once(20).await });
    observed.await?;
    assert!(!old.absolute_path.exists());
    assert_eq!(store.used_bytes(), 5);
    let worker = store.clone();
    let retry = id.clone();
    let mut competing =
        tokio::spawn(async move { worker.store(input(retry), &b"new"[..], register).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut competing)
            .await
            .is_err()
    );
    release.send(())?;
    assert!(matches!(
        competing.await?,
        Err(WriteFailure::Storage(StorageError::BlobCollision))
    ));
    assert_eq!(cycle.await??.tombstones_deferred, 1);
    assert_eq!(store.used_bytes(), 5);
    assert_eq!(engine.gc_once(21).await?.tombstones_completed, 1);
    assert_eq!(store.used_bytes(), 5);

    let (new, ()) = store.store(input(id), &b"new"[..], register).await?;
    *registry
        .stale_candidate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(BlobTombstone {
        record: old.record(),
        deleted_at_ms: 16,
    });
    let report = engine.gc_once(22).await?;
    assert_eq!(report.tombstones_completed, 0);
    assert_eq!(report.tombstones_deferred, 0);
    assert_eq!(fs::read(new.absolute_path)?, b"new");
    assert_eq!(fs::read(other.absolute_path)?, b"hello");
    assert_eq!(store.used_bytes(), 8);
    let engine = Arc::try_unwrap(engine).map_err(|_| "live engine owner")?;
    engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn checkpoint_observes_pinned_reader_and_gc_batches_at_most_200_rows() -> TestResult {
    let scratch = Scratch::new()?;
    let (mut config, _) = configs(&scratch);
    config.journal_size_limit_bytes = 1;
    config.wal_autocheckpoint_pages = 0;
    let engine =
        StorageEngine::open_managed(config, SCHEMA, Arc::new(Registry::default()), None).await?;
    engine.db().write_sqlite(|tx| {
        tx.execute_batch("WITH RECURSIVE numbers(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM numbers WHERE x<201)
                          INSERT INTO expiring SELECT x, 0, zeroblob(100) FROM numbers")?;
        Ok(WriteOutcome::new(()))
    }).await?;
    let reads = engine.reads().clone();
    let (pin, active) = oneshot::channel();
    let (released, wait) = mpsc::channel();
    let reader = tokio::spawn(async move {
        reads
            .read(move |db| {
                db.execute_batch("BEGIN;")?;
                let count =
                    db.query_row("SELECT count(*) FROM expiring", [], |r| r.get::<_, u32>(0))?;
                let _ = pin.send(());
                wait.recv().map_err(io::Error::other)?;
                Ok(count)
            })
            .await
    });
    active.await?;
    let report = tokio::time::timeout(Duration::from_secs(2), engine.gc_once(0)).await??;
    assert_eq!(report.prune_batches, 2);
    assert!(!report.has_more);
    assert!(report.checkpoint.passive.log_frames > report.checkpoint.passive.checkpointed_frames);
    assert!(
        report
            .checkpoint
            .restart
            .ok_or("missing restart checkpoint")?
            .busy
    );
    released.send(())?;
    assert_eq!(reader.await??, 201);
    let report = engine.gc_once(0).await?;
    assert_eq!(
        report.checkpoint.passive.log_frames,
        report.checkpoint.passive.checkpointed_frames
    );
    assert_eq!(
        engine
            .db()
            .write_sqlite(|tx| Ok(WriteOutcome::new(tx.query_row(
                "PRAGMA busy_timeout",
                [],
                |r| r.get::<_, u32>(0)
            )?,)))
            .await?,
        5000
    );
    engine.shutdown().await?;
    Ok(())
}

#[test]
fn staging_does_not_hold_the_only_blocking_worker_while_waiting_for_async_input() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()?;
    runtime.block_on(async {
        let scratch = Scratch::new()?;
        let (config, blobs) = configs(&scratch);
        let engine =
            StorageEngine::open_managed(config, SCHEMA, Arc::new(Registry::default()), Some(blobs))
                .await?;
        let store = engine.blobs().ok_or("missing blob store")?;
        let (mut writer, reader) = tokio::io::duplex(32);
        let producer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let payload = tokio::task::spawn_blocking(|| b"hello").await?;
            writer.write_all(payload).await?;
            writer.shutdown().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        let (blob, ()) = tokio::time::timeout(
            Duration::from_secs(1),
            store.store(input(BlobId::generate()?), reader, register),
        )
        .await??;
        producer.await??;
        assert_eq!(blob.size_bytes, 5);
        engine.shutdown().await?;
        Ok(())
    })
}
