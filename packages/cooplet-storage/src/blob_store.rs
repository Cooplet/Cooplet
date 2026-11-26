mod disk;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    hash::{BuildHasher, RandomState},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use cooplet_platform::{PlatformError, PublishOutcome};
use cooplet_protocol::{BlobId, validate_sha256};
use rusqlite::Transaction;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{Mutex, OwnedRwLockReadGuard, RwLock, mpsc},
};

use crate::{
    BlobRecord, BlobState, BlobStoreConfig, BlobTombstone, DbActor, GC_BATCH_SIZE, GcHooks,
    ReadPool, StorageError, WriteError, WriteFailure, WriteOutcome,
};
pub use disk::safe_disk_name;
use disk::{CHUNK_BYTES, Disk, InputChunk, StagedBlob, checked_blob_size, validate_path};

const MUTATION_SLOTS: usize = 64;
const STAGING_QUEUE: usize = 8;

#[derive(Clone, Debug)]
pub struct BlobInput {
    pub blob_id: BlobId,
    pub original_name: String,
    pub expected_size: Option<u64>,
    pub expected_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlob {
    pub blob_id: BlobId,
    pub original_name: String,
    pub safe_disk_name: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub disk_rel_path: PathBuf,
    pub absolute_path: PathBuf,
}

impl StoredBlob {
    pub fn record(&self) -> BlobRecord {
        BlobRecord {
            blob_id: self.blob_id.clone(),
            disk_rel_path: self.disk_rel_path.clone(),
            size_bytes: self.size_bytes,
        }
    }
}

#[derive(Clone)]
pub struct BlobDiskStore(Arc<Store>);

struct Store {
    disk: Arc<Disk>,
    db: DbActor,
    reads: ReadPool,
    hooks: Arc<dyn GcHooks>,
    slots: [Mutex<HashSet<BlobId>>; MUTATION_SLOTS],
    hasher: RandomState,
    admission: Arc<RwLock<()>>,
    closed: AtomicBool,
}

impl BlobDiskStore {
    pub(crate) async fn open(
        config: BlobStoreConfig,
        db: DbActor,
        reads: ReadPool,
        hooks: Arc<dyn GcHooks>,
    ) -> Result<Self, StorageError> {
        let startup_reads = reads.clone();
        let disk = tokio::task::spawn_blocking(move || {
            let _sidecar = startup_reads;
            Disk::open(config)
        })
        .await??;
        let store = Self(Arc::new(Store {
            disk: Arc::new(disk),
            db,
            reads,
            hooks,
            slots: std::array::from_fn(|_| Mutex::new(HashSet::new())),
            hasher: RandomState::new(),
            admission: Arc::new(RwLock::new(())),
            closed: AtomicBool::new(false),
        }));
        store.recover().await?;
        Ok(store)
    }

    pub fn used_bytes(&self) -> u64 {
        self.0.disk.used_bytes()
    }

    /// Read a committed live object. Perform synchronous reads from the file
    /// on a blocking thread or convert it to the application's async file type.
    pub async fn open_blob(&self, id: &BlobId) -> Result<Option<(BlobRecord, File)>, StorageError> {
        let admission = self.enter().await?;
        let store = self.0.clone();
        let BlobState::Live(record) = self.0.lookup_one(id.clone()).await? else {
            return Ok(None);
        };
        tokio::task::spawn_blocking(move || {
            let _admission = admission;
            let file = store.disk.open_live(&record)?;
            Ok(Some((record, file)))
        })
        .await?
    }

    /// Stage and hash a stream, publish the complete object, and register it
    /// in one writer transaction. The coordinator keeps ownership after EOF
    /// even when the caller is cancelled.
    pub async fn store<R, T, E, F>(
        &self,
        input: BlobInput,
        mut reader: R,
        register: F,
    ) -> Result<(StoredBlob, T), WriteFailure<E>>
    where
        R: AsyncRead + Unpin,
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&Transaction<'_>, &StoredBlob) -> Result<WriteOutcome<T>, WriteError<E>>
            + Send
            + 'static,
    {
        let admission = self.enter().await?;
        let limit = self.0.disk.max_blob_bytes;
        if let Some(sha256) = &input.expected_sha256 {
            validate_sha256(sha256).map_err(StorageError::from)?;
        }
        let expected = input.expected_size;
        checked_blob_size(0, expected.unwrap_or(0), limit, expected)?;
        let (send, receive) = mpsc::channel(STAGING_QUEUE);
        let store = self.0.clone();
        let worker = tokio::spawn(async move {
            let _admission = admission;
            let staged = store.disk.clone().stage(input, receive).await?;
            store.register(staged, register).await
        });
        let result = feed(&mut reader, send, limit, expected).await;
        let stored = worker.await.map_err(StorageError::from)?;
        match result {
            Ok(()) | Err(StorageError::BlobAborted) => stored,
            Err(error) => Err(error.into()),
        }
    }

    async fn enter(&self) -> Result<OwnedRwLockReadGuard<()>, StorageError> {
        let guard = self.0.admission.clone().read_owned().await;
        if self.0.closed.load(Ordering::Acquire) {
            Err(StorageError::Closed)
        } else {
            Ok(guard)
        }
    }

    pub(crate) fn stop_admission(&self) {
        self.0.closed.store(true, Ordering::Release);
    }

    pub(crate) async fn close(&self) {
        self.stop_admission();
        let _drained = self.0.admission.write().await;
    }

    pub(crate) async fn pending_tombstones(&self) -> Result<Vec<BlobTombstone>, StorageError> {
        let hooks = self.0.hooks.clone();
        let candidates = self
            .0
            .db
            .write_sqlite(move |tx| {
                Ok(WriteOutcome::new(
                    hooks.fetch_pending_blob_tombstones(tx, GC_BATCH_SIZE)?,
                ))
            })
            .await?;
        if candidates.len() > GC_BATCH_SIZE {
            return Err(StorageError::BlobUnverified(
                "tombstone batch exceeds the limit".into(),
            ));
        }
        Ok(candidates)
    }

    pub(crate) async fn collect_tombstone(
        &self,
        candidate: BlobTombstone,
    ) -> Result<bool, StorageError> {
        let admission = self.enter().await?;
        let store = self.0.clone();
        tokio::spawn(async move {
            let _admission = admission;
            store.collect(candidate, true).await
        })
        .await?
    }

    async fn recover(&self) -> Result<(), StorageError> {
        let owner = self.0.clone();
        let mut scanner = tokio::task::spawn_blocking(move || {
            owner.disk.clean_staging()?;
            owner.disk.scan()
        })
        .await??;
        loop {
            let candidates = self.pending_tombstones().await?;
            if candidates.is_empty() {
                break;
            }
            for candidate in candidates {
                if !self.0.collect(candidate, false).await? {
                    return Err(StorageError::BlobUnverified(
                        "startup tombstone changed before acknowledgment".into(),
                    ));
                }
            }
        }
        let mut used = 0u64;
        loop {
            let (next, batch) = tokio::task::spawn_blocking(move || {
                let batch = scanner.next_batch(GC_BATCH_SIZE)?;
                Ok::<_, StorageError>((scanner, batch))
            })
            .await??;
            scanner = next;
            if batch.is_empty() {
                break;
            }
            let ids = batch.iter().map(|record| record.blob_id.clone()).collect();
            let states = self.0.lookup(ids).await?;
            let owner = self.0.clone();
            used = tokio::task::spawn_blocking(move || {
                let mut used = used;
                for candidate in batch {
                    match states.get(&candidate.blob_id) {
                        Some(BlobState::Live(record)) if *record == candidate => {
                            used = used
                                .checked_add(record.size_bytes)
                                .ok_or(StorageError::StorageFull)?;
                        }
                        Some(BlobState::Absent) => owner.disk.unlink(&candidate, false)?,
                        Some(BlobState::Live(record))
                            if record.disk_rel_path != candidate.disk_rel_path =>
                        {
                            owner.disk.prove_separate(&candidate, record)?;
                            owner.disk.unlink(&candidate, false)?;
                        }
                        _ => {
                            return Err(StorageError::BlobUnverified(format!(
                                "registry and disk disagree for {}",
                                candidate.blob_id,
                            )));
                        }
                    }
                }
                Ok::<_, StorageError>(used)
            })
            .await??;
        }
        self.0.disk.initialize_usage(used);
        Ok(())
    }
}

impl Store {
    fn slot(&self, id: &BlobId) -> &Mutex<HashSet<BlobId>> {
        &self.slots[self.hasher.hash_one(id) as usize % MUTATION_SLOTS]
    }

    async fn lookup(
        &self,
        ids: BTreeSet<BlobId>,
    ) -> Result<BTreeMap<BlobId, BlobState>, StorageError> {
        let hooks = self.hooks.clone();
        self.reads
            .read(move |connection| {
                let ids: Vec<_> = ids.into_iter().collect();
                let states = hooks.lookup_blobs(connection, &ids)?;
                if states.len() != ids.len() {
                    return Err(StorageError::BlobUnverified(
                        "incomplete blob registry response".into(),
                    ));
                }
                for id in ids {
                    let state = states.get(&id).ok_or_else(|| {
                        StorageError::BlobUnverified(format!("missing registry result for {id}"))
                    })?;
                    let record = match state {
                        BlobState::Live(record) => record,
                        BlobState::Tombstone(tombstone) => &tombstone.record,
                        BlobState::Absent => continue,
                    };
                    if record.blob_id != id {
                        return Err(StorageError::BlobUnverified(format!(
                            "incorrect registry ID for {id}"
                        )));
                    }
                    validate_path(&record.disk_rel_path, &record.blob_id)?;
                }
                Ok(states)
            })
            .await
    }

    async fn lookup_one(&self, id: BlobId) -> Result<BlobState, StorageError> {
        self.lookup(BTreeSet::from([id]))
            .await?
            .into_values()
            .next()
            .ok_or_else(|| StorageError::BlobUnverified("empty blob registry response".into()))
    }

    async fn register<T, E, F>(
        &self,
        stage: StagedBlob,
        register: F,
    ) -> Result<(StoredBlob, T), WriteFailure<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&Transaction<'_>, &StoredBlob) -> Result<WriteOutcome<T>, WriteError<E>>
            + Send
            + 'static,
    {
        let id = stage.info.blob_id.clone();
        let mut slot = self.slot(&id).lock().await;
        let admitted = if slot.contains(&id) {
            Err(StorageError::BlobUnverified(format!(
                "unresolved prior mutation for {id}"
            )))
        } else {
            match self.lookup_one(id.clone()).await {
                Ok(BlobState::Absent) => Ok(()),
                Ok(_) => Err(StorageError::BlobCollision),
                Err(error) => Err(error),
            }
        };

        let disk = self.disk.clone();
        let publication = tokio::task::spawn_blocking(move || {
            if let Err(error) = admitted {
                disk.discard_stage(stage)?;
                return Err(error);
            }
            match disk.publish(&stage) {
                Ok(PublishOutcome::Published) => Ok(stage.info),
                Ok(PublishOutcome::AlreadyExists) => {
                    disk.discard_stage(stage)?;
                    Err(StorageError::BlobCollision)
                }
                Err(
                    error @ StorageError::Platform(PlatformError::PublishedDurabilityUnknown {
                        ..
                    }),
                ) => Err(error),
                Err(error) => {
                    disk.discard_stage(stage)?;
                    Err(error)
                }
            }
        })
        .await;
        let blob = match publication {
            Ok(Ok(blob)) => blob,
            Ok(Err(
                error @ StorageError::Platform(PlatformError::PublishedDurabilityUnknown { .. }),
            )) => {
                slot.insert(id);
                return Err(error.into());
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(error) => {
                slot.insert(id);
                return Err(StorageError::from(error).into());
            }
        };

        let record = blob.record();
        let callback_blob = blob.clone();
        let outcome = self.db.write(move |tx| register(tx, &callback_blob)).await;
        match outcome {
            Ok(value) => Ok((blob, value)),
            Err(error) => {
                // A lost or failed commit response needs a fresh observation
                // behind the accepted write before compensation is safe.
                if matches!(
                    error,
                    WriteFailure::Storage(
                        StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_)
                    )
                ) {
                    match self.observe_after_writer(id.clone()).await {
                        Ok(BlobState::Absent) => {}
                        Ok(BlobState::Live(current)) if current == record => return Err(error),
                        Ok(BlobState::Tombstone(current)) if current.record == record => {
                            return Err(error);
                        }
                        _ => {
                            slot.insert(id);
                            return Err(error);
                        }
                    }
                }
                if let Err(cleanup) = self.unlink(record, true).await {
                    slot.insert(id);
                    return Err(cleanup.into());
                }
                Err(error)
            }
        }
    }

    async fn collect(&self, candidate: BlobTombstone, account: bool) -> Result<bool, StorageError> {
        let id = candidate.record.blob_id.clone();
        let mut slot = self.slot(&id).lock().await;
        if slot.contains(&id) {
            return Err(StorageError::BlobUnverified(format!(
                "unresolved prior mutation for {id}"
            )));
        }
        if self.lookup_one(id.clone()).await? != BlobState::Tombstone(candidate.clone()) {
            return Ok(false);
        }
        self.unlink(candidate.record.clone(), account).await?;
        let hooks = self.hooks.clone();
        let observed = candidate.clone();
        let ack = self
            .db
            .write_sqlite(move |tx| Ok(WriteOutcome::new(hooks.ack_blob_tombstone(tx, &observed)?)))
            .await;
        match ack {
            Ok(acked) => Ok(acked),
            Err(error @ (StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_))) => {
                match self.observe_after_writer(id.clone()).await {
                    Ok(BlobState::Absent) => Ok(true),
                    Ok(BlobState::Tombstone(current)) if current == candidate => Err(error),
                    _ => {
                        slot.insert(id);
                        Err(error)
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn observe_after_writer(&self, id: BlobId) -> Result<BlobState, StorageError> {
        self.db.barrier().await?;
        self.lookup_one(id).await
    }

    async fn unlink(&self, record: BlobRecord, account: bool) -> Result<(), StorageError> {
        let disk = self.disk.clone();
        let sidecar = self.reads.clone();
        tokio::task::spawn_blocking(move || {
            let _sidecar = sidecar;
            disk.unlink(&record, account)
        })
        .await?
    }
}

async fn feed(
    reader: &mut (impl AsyncRead + Unpin),
    send: mpsc::Sender<InputChunk>,
    limit: u64,
    expected: Option<u64>,
) -> Result<(), StorageError> {
    let mut buffer = vec![0; CHUNK_BYTES];
    let mut bytes = 0;
    loop {
        let count = tokio::select! {
            count = reader.read(&mut buffer) => count?,
            () = send.closed() => return Err(StorageError::BlobAborted),
        };
        if count == 0 {
            break;
        }
        bytes = checked_blob_size(bytes, count as u64, limit, expected)?;
        send.send(InputChunk::Data(Bytes::copy_from_slice(&buffer[..count])))
            .await
            .map_err(|_| StorageError::BlobAborted)?;
    }
    if let Some(expected) = expected
        && expected != bytes
    {
        return Err(StorageError::LengthMismatch {
            expected,
            actual: bytes,
        });
    }
    send.send(InputChunk::Finish)
        .await
        .map_err(|_| StorageError::BlobAborted)
}
