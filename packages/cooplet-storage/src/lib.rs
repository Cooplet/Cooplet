//! Schema-independent SQLite writer, read pool, and disk blob storage.

mod actor;
mod blob_store;
mod config;
mod error;
mod gc;
mod migrate;
mod pragma;
mod read_pool;

use std::{
    path::PathBuf,
    sync::Arc,
    thread::JoinHandle,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle as Task,
};

pub use actor::{DbActor, EventNotice, WriteOutcome};
pub use blob_store::{BlobDiskStore, BlobInput, StoredBlob, safe_disk_name};
pub use config::{BlobStoreConfig, DEFAULT_MAX_BLOB_BYTES, StorageConfig, SynchronousMode};
pub use error::{StorageError, WriteError, WriteFailure};
pub use gc::{
    BlobRecord, BlobState, BlobTombstone, CheckpointProgress, CheckpointReport, GC_BATCH_SIZE,
    GcBatchOutcome, GcHooks, GcReport,
};
pub use migrate::Migration;
pub use read_pool::ReadPool;

const GC_BATCHES_PER_CYCLE: usize = 50;
const GC_TOMBSTONE_BATCHES_PER_CYCLE: usize = 10;

pub struct StorageEngine {
    db: DbActor,
    reads: ReadPool,
    writer: Option<JoinHandle<()>>,
    blobs: Option<BlobDiskStore>,
    gc: Option<Arc<GcDriver>>,
    stop_gc: Option<watch::Sender<()>>,
    gc_task: Option<Task<()>>,
}

impl StorageEngine {
    pub async fn open(
        config: StorageConfig,
        migrations: &[Migration],
    ) -> Result<Self, StorageError> {
        Self::open_database(config, migrations, None, None).await
    }

    async fn open_database(
        config: StorageConfig,
        migrations: &[Migration],
        blob_root: Option<PathBuf>,
        existing_check: Option<pragma::ExistingDatabaseCheck>,
    ) -> Result<Self, StorageError> {
        let (db, reads, writer) =
            DbActor::start(config, migrations, blob_root, existing_check).await?;
        Ok(Self {
            db,
            reads,
            writer: Some(writer),
            blobs: None,
            gc: None,
            stop_gc: None,
            gc_task: None,
        })
    }

    /// Recover committed tombstones and incomplete publications before
    /// exposing any handles, then start one periodic maintenance driver.
    pub async fn open_managed(
        config: StorageConfig,
        migrations: &[Migration],
        hooks: Arc<dyn GcHooks>,
        blob_config: Option<BlobStoreConfig>,
    ) -> Result<Self, StorageError> {
        Self::open_managed_database(config, migrations, hooks, blob_config, None).await
    }

    /// Open an existing database and verify its application state before startup.
    ///
    /// The check runs once on the writer thread with a read-only connection
    /// and the database sidecar held. Returning an error aborts startup before
    /// read-write open, migrations or blob recovery. Preflight sees committed
    /// WAL data without creating WAL or shared-memory files. On success the
    /// same sidecar remains held for managed startup and normal operation.
    pub async fn open_existing_managed_checked(
        config: StorageConfig,
        migrations: &[Migration],
        hooks: Arc<dyn GcHooks>,
        blob_config: Option<BlobStoreConfig>,
        check: impl FnOnce(&rusqlite::Connection) -> Result<(), StorageError> + Send + 'static,
    ) -> Result<Self, StorageError> {
        Self::open_managed_database(
            config,
            migrations,
            hooks,
            blob_config,
            Some(pragma::ExistingDatabaseCheck {
                require_database: true,
                validate: Box::new(check),
            }),
        )
        .await
    }

    /// Create a managed database or check its existing state before opening it
    /// for writes. The database sidecar protects that choice and the check.
    pub async fn open_managed_checked(
        config: StorageConfig,
        migrations: &[Migration],
        hooks: Arc<dyn GcHooks>,
        blob_config: Option<BlobStoreConfig>,
        check: impl FnOnce(&rusqlite::Connection) -> Result<(), StorageError> + Send + 'static,
    ) -> Result<Self, StorageError> {
        Self::open_managed_database(
            config,
            migrations,
            hooks,
            blob_config,
            Some(pragma::ExistingDatabaseCheck {
                require_database: false,
                validate: Box::new(check),
            }),
        )
        .await
    }

    /// Inspect an offline database under a shared guard on its existing
    /// sidecar. Other inspections can run concurrently. No writer or
    /// maintenance worker is started.
    pub async fn check_existing<T: Send + 'static>(
        config: StorageConfig,
        migrations: &[Migration],
        check: impl FnOnce(&rusqlite::Connection) -> Result<T, StorageError> + Send + 'static,
    ) -> Result<T, StorageError> {
        pragma::validate_config(&config)?;
        let target = migrate::target_version(migrations)?;
        tokio::task::spawn_blocking(move || pragma::check_existing(&config, target, check)).await?
    }

    async fn open_managed_database(
        config: StorageConfig,
        migrations: &[Migration],
        hooks: Arc<dyn GcHooks>,
        blob_config: Option<BlobStoreConfig>,
        existing_check: Option<pragma::ExistingDatabaseCheck>,
    ) -> Result<Self, StorageError> {
        if let Some(blob_config) = &blob_config {
            blob_store::validate_config(blob_config)?;
        }
        let interval = config.gc_interval;
        let blob_root = blob_config.as_ref().map(|config| config.root.clone());
        let mut engine = Self::open_database(config, migrations, blob_root, existing_check).await?;
        if let Some(config) = blob_config {
            let opened = BlobDiskStore::open(
                config,
                engine.db.clone(),
                engine.reads.clone(),
                hooks.clone(),
            )
            .await;
            match opened {
                Ok(blobs) => engine.blobs = Some(blobs),
                Err(error) => {
                    let _ = engine.shutdown().await;
                    return Err(error);
                }
            }
        }
        let driver = Arc::new(GcDriver {
            db: engine.db.clone(),
            hooks,
            blobs: engine.blobs.clone(),
            cycle: Mutex::new(GcState::default()),
        });
        let (stop, stopped) = watch::channel(());
        engine.gc_task = Some(tokio::spawn(driver.clone().run(stopped, interval)));
        engine.gc = Some(driver);
        engine.stop_gc = Some(stop);
        Ok(engine)
    }

    pub fn db(&self) -> &DbActor {
        &self.db
    }

    pub fn reads(&self) -> &ReadPool {
        &self.reads
    }

    pub fn blobs(&self) -> Option<&BlobDiskStore> {
        self.blobs.as_ref()
    }

    pub async fn gc_once(&self, now_ms: i64) -> Result<GcReport, StorageError> {
        let gc = self
            .gc
            .as_ref()
            .ok_or(StorageError::InvalidConfig("GC hooks are not configured"))?;
        gc.cycle(now_ms, None).await
    }

    /// Stop GC, drain accepted blob and SQLite operations, and close the
    /// databases before releasing their sidecar.
    pub async fn shutdown(mut self) -> Result<(), StorageError> {
        if let Some(stop) = self.stop_gc.take() {
            let _ = stop.send(());
        }
        let gc = if let Some(task) = self.gc_task.take() {
            task.await.map_err(StorageError::from)
        } else {
            Ok(())
        };
        if let Some(blobs) = &self.blobs {
            blobs.close().await;
        }
        // Close every read-only handle before the writer so SQLite treats the
        // writer as the final connection and checkpoints the WAL cleanly.
        let reads = self.reads.close().await;
        self.db.close();
        let writer = if let Some(thread) = self.writer.take() {
            tokio::task::spawn_blocking(move || {
                thread.join().map_err(|_| StorageError::WriterPanicked)
            })
            .await?
        } else {
            Ok(())
        };
        gc.and(reads).and(writer)
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        if let Some(stop) = self.stop_gc.take() {
            let _ = stop.send(());
        }
        if let Some(blobs) = &self.blobs {
            blobs.stop_admission();
        }
        self.reads.mark_closed();
        self.db.close();
    }
}

#[derive(Default)]
struct GcState {
    tombstone_cursor: Option<BlobTombstone>,
}

struct GcDriver {
    db: DbActor,
    hooks: Arc<dyn GcHooks>,
    blobs: Option<BlobDiskStore>,
    cycle: Mutex<GcState>,
}

impl GcDriver {
    async fn run(self: Arc<Self>, mut stop: watch::Receiver<()>, interval: Duration) {
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                () = tokio::time::sleep(interval) => {
                    match unix_millis() {
                        Ok(now) => self.run_background_cycle(now, &stop).await,
                        Err(error) => tracing::warn!(%error, "could not obtain storage maintenance time"),
                    }
                }
            }
        }
    }

    async fn run_background_cycle(&self, now: i64, stop: &watch::Receiver<()>) {
        let mut state = self.cycle.lock().await;
        if let Err(error) = self.run_prune(now, Some(stop)).await {
            tracing::warn!(%error, "storage prune maintenance failed");
        }
        if stopping(Some(stop)) {
            return;
        }
        if let Err(error) = self.run_tombstones(&mut state, Some(stop)).await {
            tracing::warn!(%error, "storage blob tombstone maintenance failed");
        }
        if stopping(Some(stop)) {
            return;
        }
        if let Err(error) = self.db.checkpoint().await {
            tracing::warn!(%error, "storage WAL checkpoint maintenance failed");
        }
    }

    async fn cycle(
        &self,
        now: i64,
        stop: Option<&watch::Receiver<()>>,
    ) -> Result<GcReport, StorageError> {
        let mut state = self.cycle.lock().await;
        let (prune_batches, prune_has_more) = self.run_prune(now, stop).await?;
        let (tombstones_completed, tombstones_deferred, tombstones_has_more) =
            self.run_tombstones(&mut state, stop).await?;
        let checkpoint = self.db.checkpoint().await?;
        Ok(GcReport {
            prune_batches,
            has_more: prune_has_more || tombstones_has_more,
            tombstones_completed,
            tombstones_deferred,
            checkpoint,
        })
    }

    async fn run_prune(
        &self,
        now: i64,
        stop: Option<&watch::Receiver<()>>,
    ) -> Result<(usize, bool), StorageError> {
        let mut prune_batches = 0;
        let mut has_more = true;
        while has_more && prune_batches < GC_BATCHES_PER_CYCLE {
            if stopping(stop) {
                break;
            }
            let hooks = self.hooks.clone();
            has_more = self
                .db
                .write_sqlite(move |tx| {
                    let batch = hooks.prune_batch(tx, now, GC_BATCH_SIZE)?;
                    Ok(WriteOutcome::with_events(batch.has_more, batch.events))
                })
                .await?;
            prune_batches += 1;
        }
        Ok((prune_batches, has_more))
    }

    async fn run_tombstones(
        &self,
        state: &mut GcState,
        stop: Option<&watch::Receiver<()>>,
    ) -> Result<(usize, usize, bool), StorageError> {
        let Some(blobs) = &self.blobs else {
            return Ok((0, 0, false));
        };
        let (mut completed, mut deferred) = (0, 0);
        let mut has_more = false;
        for _ in 0..GC_TOMBSTONE_BATCHES_PER_CYCLE {
            if stopping(stop) {
                break;
            }
            let previous_cursor = state.tombstone_cursor.clone();
            let candidates = blobs.pending_tombstones(previous_cursor.clone()).await?;
            if let (Some(prev), Some(first)) = (&previous_cursor, candidates.first())
                && (first.deleted_at_ms, &first.record.blob_id)
                    <= (prev.deleted_at_ms, &prev.record.blob_id)
            {
                state.tombstone_cursor = None;
                has_more = true;
                break;
            }
            let batch_full = candidates.len() == GC_BATCH_SIZE;
            let last = candidates.last().cloned();
            let mut interrupted = false;
            for candidate in candidates {
                if stopping(stop) {
                    interrupted = true;
                    break;
                }
                match blobs.collect_tombstone(candidate).await {
                    Ok(true) => completed += 1,
                    Ok(false) => {}
                    Err(error) => {
                        deferred += 1;
                        tracing::warn!(%error, "blob tombstone cleanup deferred");
                    }
                }
            }
            if interrupted {
                has_more = true;
                break;
            }
            if !batch_full {
                // Reached the end of the pending set; wrap to the oldest entry next cycle.
                state.tombstone_cursor = None;
                has_more = false;
                break;
            }
            let advanced = match (&previous_cursor, &last) {
                (Some(prev), Some(next)) => {
                    (next.deleted_at_ms, &next.record.blob_id)
                        > (prev.deleted_at_ms, &prev.record.blob_id)
                }
                (None, Some(_)) => true,
                (_, None) => false,
            };
            if !advanced {
                state.tombstone_cursor = None;
                has_more = true;
                break;
            }
            state.tombstone_cursor = last;
            has_more = true;
        }
        Ok((completed, deferred, has_more))
    }
}

fn stopping(stop: Option<&watch::Receiver<()>>) -> bool {
    stop.is_some_and(|signal| signal.has_changed().unwrap_or(true))
}

fn unix_millis() -> Result<i64, StorageError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(|error| std::io::Error::other(error).into())
}
