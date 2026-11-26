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

pub use actor::{DbActor, WriteOutcome};
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
    /// read-write open, migrations or blob recovery. On success the same
    /// sidecar remains held for managed startup and normal operation.
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
            Some(Box::new(check)),
        )
        .await
    }

    async fn open_managed_database(
        config: StorageConfig,
        migrations: &[Migration],
        hooks: Arc<dyn GcHooks>,
        blob_config: Option<BlobStoreConfig>,
        existing_check: Option<pragma::ExistingDatabaseCheck>,
    ) -> Result<Self, StorageError> {
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
            cycle: Mutex::new(()),
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
        gc.cycle(now_ms).await
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
        self.db.close();
        let reads = self.reads.close().await;
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
        self.db.close();
    }
}

struct GcDriver {
    db: DbActor,
    hooks: Arc<dyn GcHooks>,
    blobs: Option<BlobDiskStore>,
    cycle: Mutex<()>,
}

impl GcDriver {
    async fn run(self: Arc<Self>, mut stop: watch::Receiver<()>, interval: Duration) {
        loop {
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                () = tokio::time::sleep(interval) => {
                    let result = unix_millis();
                    match result {
                        Ok(now) => {
                            if let Err(error) = self.cycle(now).await {
                                tracing::warn!(%error, "storage maintenance failed");
                            }
                        }
                        Err(error) => tracing::warn!(%error, "could not obtain storage maintenance time"),
                    }
                }
            }
        }
    }

    async fn cycle(&self, now: i64) -> Result<GcReport, StorageError> {
        let _guard = self.cycle.lock().await;
        let mut prune_batches = 0;
        let mut has_more = true;
        while has_more && prune_batches < GC_BATCHES_PER_CYCLE {
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

        let (mut tombstones_completed, mut tombstones_deferred) = (0, 0);
        if let Some(blobs) = &self.blobs {
            for candidate in blobs.pending_tombstones().await? {
                match blobs.collect_tombstone(candidate).await {
                    Ok(true) => tombstones_completed += 1,
                    Ok(false) => {}
                    Err(error) => {
                        tombstones_deferred += 1;
                        tracing::warn!(%error, "blob tombstone cleanup deferred");
                    }
                }
            }
        }
        let checkpoint = self.db.checkpoint().await?;
        Ok(GcReport {
            prune_batches,
            has_more,
            tombstones_completed,
            tombstones_deferred,
            checkpoint,
        })
    }
}

fn unix_millis() -> Result<i64, StorageError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?;
    i64::try_from(duration.as_millis()).map_err(|error| std::io::Error::other(error).into())
}
