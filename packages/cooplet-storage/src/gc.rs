use std::{collections::BTreeMap, fs, io, path::PathBuf, time::Duration};

use cooplet_protocol::{BlobId, EventEnvelope};
use rusqlite::{Connection, Transaction};

use crate::{StorageConfig, StorageError, pragma::wal_path};

pub const GC_BATCH_SIZE: usize = 200;
const RESTART_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRecord {
    pub blob_id: BlobId,
    pub disk_rel_path: PathBuf,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobTombstone {
    pub record: BlobRecord,
    pub deleted_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobState {
    Absent,
    Live(BlobRecord),
    Tombstone(BlobTombstone),
}

#[derive(Debug, Default)]
pub struct GcBatchOutcome {
    pub events: Vec<EventEnvelope>,
    pub has_more: bool,
}

/// Application-owned SQL and retention policy. Lookups must explicitly
/// return one state for every requested ID, including absent IDs.
pub trait GcHooks: Send + Sync + 'static {
    fn prune_batch(
        &self,
        tx: &Transaction<'_>,
        now_ms: i64,
        limit: usize,
    ) -> rusqlite::Result<GcBatchOutcome>;

    fn fetch_pending_blob_tombstones(
        &self,
        tx: &Transaction<'_>,
        limit: usize,
    ) -> rusqlite::Result<Vec<BlobTombstone>>;

    fn ack_blob_tombstone(
        &self,
        tx: &Transaction<'_>,
        observed: &BlobTombstone,
    ) -> rusqlite::Result<bool>;

    fn lookup_blobs(
        &self,
        connection: &Connection,
        blob_ids: &[BlobId],
    ) -> rusqlite::Result<BTreeMap<BlobId, BlobState>>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointProgress {
    pub busy: bool,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointReport {
    pub passive: CheckpointProgress,
    pub restart: Option<CheckpointProgress>,
}

#[derive(Debug)]
pub struct GcReport {
    pub prune_batches: usize,
    pub has_more: bool,
    pub tombstones_completed: usize,
    pub tombstones_deferred: usize,
    pub checkpoint: CheckpointReport,
}

pub(crate) fn checkpoint(
    connection: &Connection,
    config: &StorageConfig,
) -> Result<CheckpointReport, StorageError> {
    {
        let mut vacuum = connection.prepare("PRAGMA incremental_vacuum(64)")?;
        let mut progress = vacuum.query([])?;
        while progress.next()?.is_some() {}
    }
    let passive = read_checkpoint(connection, "PRAGMA wal_checkpoint(PASSIVE)")?;
    let wal_bytes = match fs::metadata(wal_path(&config.db_path)) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    let restart = if wal_bytes > config.journal_size_limit_bytes {
        connection.busy_timeout(RESTART_TIMEOUT)?;
        let result = read_checkpoint(connection, "PRAGMA wal_checkpoint(RESTART)");
        connection.busy_timeout(config.busy_timeout)?;
        Some(result?)
    } else {
        None
    };
    Ok(CheckpointReport { passive, restart })
}

fn read_checkpoint(connection: &Connection, sql: &str) -> rusqlite::Result<CheckpointProgress> {
    connection.query_row(sql, [], |row| {
        Ok(CheckpointProgress {
            busy: row.get::<_, i64>(0)? != 0,
            log_frames: row.get(1)?,
            checkpointed_frames: row.get(2)?,
        })
    })
}
