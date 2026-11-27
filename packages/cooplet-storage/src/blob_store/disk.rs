use std::{
    fs::{self, File, ReadDir},
    io::{self, Write},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use cooplet_platform::{
    AtomicPublish, FileLock, PlatformError, PrivateFs, PublishOutcome, current_euid,
};
use cooplet_protocol::BlobId;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::{BlobInput, StoredBlob};
use crate::{BlobRecord, BlobStoreConfig, StorageError};

pub(super) const CHUNK_BYTES: usize = 64 * 1024;

pub(crate) fn validate_config(config: &BlobStoreConfig) -> Result<(), StorageError> {
    if config.max_blob_bytes == 0 {
        return Err(StorageError::InvalidConfig(
            "blob size limit must be positive",
        ));
    }
    Ok(())
}

pub(super) enum InputChunk {
    Data(Bytes),
    Finish,
}

pub(super) struct Disk {
    root: PathBuf,
    staging: PathBuf,
    objects: PathBuf,
    pub(super) max_blob_bytes: u64,
    max_total_bytes: u64,
    used_bytes: AtomicU64,
    synced_shards: [AtomicBool; 256],
}

impl Disk {
    pub(super) fn open(
        config: BlobStoreConfig,
        database_path: &Path,
    ) -> Result<Self, StorageError> {
        validate_config(&config)?;
        validate_root_disjoint(&config.root, database_path)?;
        let root = PrivateFs::ensure_private_dir(&config.root)?;
        validate_root_disjoint(&root, database_path)?;
        check_or_claim_root(&root, database_path, RootUse::Bind)?;
        let staging = PrivateFs::ensure_private_dir(&root.join("staging"))?;
        let objects = PrivateFs::ensure_private_dir(&root.join("objects"))?;
        Ok(Self {
            root,
            staging,
            objects,
            max_blob_bytes: config.max_blob_bytes,
            max_total_bytes: config.max_total_blob_bytes,
            used_bytes: AtomicU64::new(0),
            synced_shards: std::array::from_fn(|_| AtomicBool::new(false)),
        })
    }

    pub(super) fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    pub(super) fn initialize_usage(&self, bytes: u64) {
        self.used_bytes.store(bytes, Ordering::Release);
    }

    fn reserve(&self, bytes: u64) -> Result<(), StorageError> {
        self.used_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|&total| total <= self.max_total_bytes)
            })
            .map_err(|_| StorageError::StorageFull)?;
        Ok(())
    }

    fn release(&self, bytes: u64) -> Result<(), StorageError> {
        self.used_bytes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_sub(bytes)
            })
            .map_err(|_| StorageError::BlobUnverified("blob quota accounting underflow".into()))?;
        Ok(())
    }

    pub(super) fn clean_staging(&self) -> Result<(), StorageError> {
        let dir = PrivateFs::open_dir_nofollow(&self.staging)?;
        let mut removed = false;
        for item in fs::read_dir(&self.staging)? {
            let item = item?;
            let name = item.file_name();
            let Some(name_str) = name.to_str() else {
                return Err(StorageError::InvalidBlobPath(
                    item.path().display().to_string(),
                ));
            };
            let ours = name_str
                .strip_suffix(".part")
                .is_some_and(|stem| stem.len() == 32 && stem.bytes().all(lower_hex));
            if !ours || !item.file_type()?.is_file() {
                return Err(StorageError::InvalidBlobPath(
                    item.path().display().to_string(),
                ));
            }
            // A failed link-and-unlink publication may still share this
            // inode with the destination. Remove the staging name only.
            if PrivateFs::unlink_file_at(&dir, name_str)? {
                removed = true;
            }
        }
        if removed {
            dir.sync_all()?;
        }
        Ok(())
    }

    pub(super) async fn stage(
        self: Arc<Self>,
        input: BlobInput,
        mut chunks: mpsc::Receiver<InputChunk>,
    ) -> Result<StagedBlob, StorageError> {
        let mut writer =
            tokio::task::spawn_blocking(move || StageWriter::new(self, input)).await??;
        let mut batch = Vec::with_capacity(super::STAGING_QUEUE);
        loop {
            let Some(first) = chunks.recv().await else {
                tokio::task::spawn_blocking(move || drop(writer)).await?;
                return Err(StorageError::BlobAborted);
            };
            // Only hand ready input to the blocking pool. A reader can itself
            // need that pool (for example an async file with one blocking slot).
            batch.clear();
            batch.push(first);
            while batch.len() < super::STAGING_QUEUE {
                match chunks.try_recv() {
                    Ok(chunk) => batch.push(chunk),
                    Err(_) => break,
                }
            }
            let ready = std::mem::take(&mut batch);
            match tokio::task::spawn_blocking(move || writer.write_batch(ready)).await?? {
                StageProgress::Receiving(next, reused) => {
                    writer = next;
                    batch = reused;
                }
                StageProgress::Finished(staged) => return Ok(staged),
            }
        }
    }

    pub(super) fn publish(&self, stage: &StagedBlob) -> Result<PublishOutcome, StorageError> {
        let path = validate_path(&stage.info.disk_rel_path, &stage.info.blob_id)?;
        let shard_idx = shard_index(path.shard)?;
        let objects_fd = PrivateFs::open_dir_nofollow(&self.objects)?;
        let shard_fd = if self.synced_shards[shard_idx].load(Ordering::Acquire) {
            match PrivateFs::open_dir_at(&objects_fd, path.shard) {
                Ok(fd) => fd,
                Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                    self.synced_shards[shard_idx].store(false, Ordering::Release);
                    PrivateFs::ensure_private_dir(&self.objects.join(path.shard))?;
                    let fd = PrivateFs::open_dir_at(&objects_fd, path.shard)?;
                    self.synced_shards[shard_idx].store(true, Ordering::Release);
                    fd
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            PrivateFs::ensure_private_dir(&self.objects.join(path.shard))?;
            let fd = PrivateFs::open_dir_at(&objects_fd, path.shard)?;
            self.synced_shards[shard_idx].store(true, Ordering::Release);
            fd
        };
        let dest_path = self
            .objects
            .join(path.shard)
            .join(stage.info.blob_id.as_str());
        PrivateFs::ensure_private_dir(&dest_path)?;
        let dest_dir = PrivateFs::open_dir_at(&shard_fd, stage.info.blob_id.as_str())?;
        let stage_dir = PrivateFs::open_dir_nofollow(&self.staging)?;
        Ok(AtomicPublish::publish_noreplace(
            &stage_dir,
            &stage.name,
            &dest_dir,
            path.name,
        )?)
    }

    pub(super) fn discard_stage(&self, staged: StagedBlob) -> Result<(), StorageError> {
        self.discard_name(&staged.name, staged.info.size_bytes)
    }

    fn discard_name(&self, name: &str, reserved: u64) -> Result<(), StorageError> {
        let stage_dir = PrivateFs::open_dir_nofollow(&self.staging)?;
        PrivateFs::unlink_file_at(&stage_dir, name)?;
        self.release(reserved)
    }

    /// Called after a matching committed tombstone or a proven unpublished
    /// registration. Debit bytes only if this call removed the file.
    pub(super) fn unlink(&self, record: &BlobRecord, account: bool) -> Result<(), StorageError> {
        let path = validate_path(&record.disk_rel_path, &record.blob_id)?;
        let objects_fd = PrivateFs::open_dir_nofollow(&self.objects)?;
        let shard_fd = match PrivateFs::open_dir_at(&objects_fd, path.shard) {
            Ok(fd) => fd,
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                objects_fd.sync_all()?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let object_fd = match PrivateFs::open_dir_at(&shard_fd, record.blob_id.as_str()) {
            Ok(fd) => Some(fd),
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(object_fd) = object_fd {
            match PrivateFs::open_sealed_at(&object_fd, path.name) {
                Ok(file) => {
                    let size = file.metadata()?.len();
                    drop(file);
                    if size != record.size_bytes {
                        return Err(StorageError::BlobUnverified(format!(
                            "object size differs for {}",
                            record.blob_id
                        )));
                    }
                    if PrivateFs::unlink_file_at(&object_fd, path.name)? && account {
                        self.release(size)?;
                    }
                }
                Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            match PrivateFs::remove_dir_at(&shard_fd, record.blob_id.as_str()) {
                Ok(_) => {}
                Err(PlatformError::Io(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::DirectoryNotEmpty | io::ErrorKind::AlreadyExists
                    ) =>
                {
                    // Another entry still lives in this directory; sync the unlink before moving on.
                    object_fd.sync_all()?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        shard_fd.sync_all()?;
        Ok(())
    }

    pub(super) fn open_live(&self, record: &BlobRecord) -> Result<File, StorageError> {
        let path = validate_path(&record.disk_rel_path, &record.blob_id)?;
        let objects_fd = PrivateFs::open_dir_nofollow(&self.objects)?;
        let shard_fd = match PrivateFs::open_dir_at(&objects_fd, path.shard) {
            Ok(fd) => fd,
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(object_missing(record));
            }
            Err(error) => return Err(error.into()),
        };
        let object_fd = match PrivateFs::open_dir_at(&shard_fd, record.blob_id.as_str()) {
            Ok(fd) => fd,
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(object_missing(record));
            }
            Err(error) => return Err(error.into()),
        };
        let file = match PrivateFs::open_sealed_at(&object_fd, path.name) {
            Ok(file) => file,
            Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Err(object_missing(record));
            }
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() != record.size_bytes {
            return Err(StorageError::BlobUnverified(format!(
                "object changed for {}",
                record.blob_id
            )));
        }
        Ok(file)
    }

    pub(super) fn scan(&self) -> Result<ObjectScanner, StorageError> {
        let objects_fd = PrivateFs::open_dir_nofollow(&self.objects)?;
        Ok(ObjectScanner {
            root: self.root.clone(),
            directories: vec![(
                PathBuf::from("objects"),
                objects_fd,
                fs::read_dir(&self.objects)?,
            )],
        })
    }

    pub(super) fn prove_separate(
        &self,
        candidate: &BlobRecord,
        registered: &BlobRecord,
    ) -> Result<(), StorageError> {
        let registered = self.open_live(registered)?.metadata()?;
        let candidate = self.open_live(candidate)?.metadata()?;
        if (registered.dev(), registered.ino()) == (candidate.dev(), candidate.ino()) {
            return Err(StorageError::BlobUnverified(
                "object paths resolve to the same inode".into(),
            ));
        }
        Ok(())
    }

    fn make_part(&self) -> Result<(File, String), StorageError> {
        let stage_dir = PrivateFs::open_dir_nofollow(&self.staging)?;
        for _ in 0..16 {
            let mut random = [0; 16];
            cooplet_platform::random_bytes(&mut random)?;
            let name = format!("{:032x}.part", u128::from_be_bytes(random));
            match PrivateFs::create_private_file_at(&stage_dir, &name) {
                Ok(file) => return Ok((file, name)),
                Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "blob staging name collisions").into())
    }
}

enum RootUse {
    BeforeDatabaseCreate,
    BeforeDatabaseOpen,
    Bind,
}

fn validate_root_disjoint(root: &Path, database_path: &Path) -> Result<(), StorageError> {
    let db_parent = database_path
        .parent()
        .ok_or(StorageError::InvalidConfig("database path has no parent"))?;
    let db_parent_meta = fs::symlink_metadata(db_parent)?;
    let normalized_root: PathBuf = root.components().collect();
    if normalized_root == db_parent
        || db_parent.starts_with(&normalized_root)
        || normalized_root == database_path
        || normalized_root.starts_with(database_path)
    {
        return Err(StorageError::InvalidConfig(
            "blob root must be a dedicated directory distinct from the database directory",
        ));
    }
    if let Ok(root_meta) = fs::symlink_metadata(&normalized_root) {
        if (root_meta.dev(), root_meta.ino()) == (db_parent_meta.dev(), db_parent_meta.ino()) {
            return Err(StorageError::InvalidConfig(
                "blob root must be a dedicated directory distinct from the database directory",
            ));
        }
        if root_meta.is_dir() {
            let canonical_root = fs::canonicalize(&normalized_root)?;
            if canonical_root == db_parent
                || db_parent.starts_with(&canonical_root)
                || canonical_root == database_path
                || canonical_root.starts_with(database_path)
            {
                return Err(StorageError::InvalidConfig(
                    "blob root must be a dedicated directory distinct from the database directory",
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn check_before_database_open(
    root: &Path,
    database_path: &Path,
    fresh: bool,
) -> Result<(), StorageError> {
    validate_root_disjoint(root, database_path)?;
    if checked_dir(root)?.is_some() {
        let canonical = PrivateFs::validate_private_dir(root)?;
        validate_root_disjoint(&canonical, database_path)?;
        let usage = if fresh {
            RootUse::BeforeDatabaseCreate
        } else {
            RootUse::BeforeDatabaseOpen
        };
        check_or_claim_root(&canonical, database_path, usage)?;
    }
    Ok(())
}

fn check_or_claim_root(
    root: &Path,
    database_path: &Path,
    usage: RootUse,
) -> Result<(), StorageError> {
    const OWNER: &str = "database.owner";
    // The DB sidecar excludes another instance of this database. Because blob_root
    // is guaranteed distinct from the database directory, this dedicated claim lock
    // serializes initialization across differently configured databases.
    let claim_path = root.join("database-owner.lock");
    let claim = FileLock::acquire_exclusive(&claim_path)?;
    claim.verify_path()?;
    let marker = root.join(OWNER);
    let owner = database_path.as_os_str().as_encoded_bytes();
    let max_owner_bytes = owner.len().max(4096) as u64;
    let stored = match PrivateFs::read_private_file(&marker, max_owner_bytes) {
        Ok(stored) => stored,
        Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            // A root with data must have a durable owner before recovery may
            // interpret absent rows as permission to remove objects.
            for directory in ["objects", "staging"] {
                let path = root.join(directory);
                if checked_dir(&path)?.is_some()
                    && fs::read_dir(path)?.next().transpose()?.is_some()
                {
                    return Err(StorageError::InvalidConfig(
                        "blob root has data without a database owner",
                    ));
                }
            }
            if matches!(
                usage,
                RootUse::BeforeDatabaseCreate | RootUse::BeforeDatabaseOpen
            ) {
                return Ok(());
            }
            // Remove only interrupted staging files targeting `database.owner`.
            claim.verify_path()?;
            PrivateFs::clean_interrupted_writes(root, OWNER)?;
            PrivateFs::write_private_file_atomic(root, OWNER, owner)?;
            PrivateFs::read_private_file(&marker, max_owner_bytes)?
        }
        Err(error) => return Err(error.into()),
    };
    if stored != owner {
        return Err(StorageError::InvalidConfig(
            "blob root belongs to another database",
        ));
    }
    // The owner marker is only installed after SQLite has a durable header.
    // Recreating its registry would make recovery delete every retained object.
    if matches!(usage, RootUse::BeforeDatabaseCreate) {
        return Err(StorageError::IncompatibleDatabase(
            "missing or empty database for an existing blob root".into(),
        ));
    }
    if matches!(usage, RootUse::Bind) {
        // Also cover a previous claim whose rename was visible but directory
        // sync did not complete before shutdown.
        PrivateFs::open_dir_nofollow(root)?.sync_all()?;
    }
    Ok(())
}

fn object_missing(record: &BlobRecord) -> StorageError {
    StorageError::BlobUnverified(format!("object missing for {}", record.blob_id))
}

struct StageWriter {
    disk: Arc<Disk>,
    input: BlobInput,
    file: File,
    name: Option<String>,
    reserved: u64,
    digest: Sha256,
}

enum StageProgress {
    Receiving(StageWriter, Vec<InputChunk>),
    Finished(StagedBlob),
}

impl StageWriter {
    fn new(disk: Arc<Disk>, input: BlobInput) -> Result<Self, StorageError> {
        let (file, name) = disk.make_part()?;
        Ok(Self {
            disk,
            input,
            file,
            name: Some(name),
            reserved: 0,
            digest: Sha256::new(),
        })
    }

    fn write_batch(mut self, mut batch: Vec<InputChunk>) -> Result<StageProgress, StorageError> {
        let mut batch_bytes = 0u64;
        let mut projected = self.reserved;
        for chunk in &batch {
            if let InputChunk::Data(data) = chunk {
                let bytes = data.len() as u64;
                projected = checked_blob_size(
                    projected,
                    bytes,
                    self.disk.max_blob_bytes,
                    self.input.expected_size,
                )?;
                batch_bytes = batch_bytes
                    .checked_add(bytes)
                    .ok_or(StorageError::BlobTooLarge {
                        limit_bytes: self.disk.max_blob_bytes,
                    })?;
            }
        }
        if batch_bytes > 0 {
            self.disk.reserve(batch_bytes)?;
            self.reserved = projected;
        }
        for chunk in batch.drain(..) {
            match chunk {
                InputChunk::Data(data) => {
                    self.digest.update(&data);
                    self.file.write_all(&data)?;
                }
                InputChunk::Finish => return self.finish().map(StageProgress::Finished),
            }
        }
        Ok(StageProgress::Receiving(self, batch))
    }

    fn finish(mut self) -> Result<StagedBlob, StorageError> {
        if let Some(expected) = self.input.expected_size
            && expected != self.reserved
        {
            return Err(StorageError::LengthMismatch {
                expected,
                actual: self.reserved,
            });
        }
        let digest = format!("{:x}", std::mem::take(&mut self.digest).finalize());
        if self
            .input
            .expected_sha256
            .as_ref()
            .is_some_and(|expected| *expected != digest)
        {
            return Err(StorageError::DigestMismatch);
        }
        PrivateFs::seal_readonly(&self.file)?;
        let name = self.name.take().ok_or(StorageError::BlobAborted)?;
        let safe_disk_name = safe_disk_name(&self.input.original_name);
        let disk_rel_path = PathBuf::from("objects")
            .join(&digest[..2])
            .join(self.input.blob_id.as_str())
            .join(&safe_disk_name);
        let absolute_path = self.disk.root.join(&disk_rel_path);
        Ok(StagedBlob {
            name,
            info: StoredBlob {
                blob_id: self.input.blob_id.clone(),
                disk_rel_path,
                absolute_path,
                size_bytes: self.reserved,
                sha256: digest,
                original_name: std::mem::take(&mut self.input.original_name),
                safe_disk_name,
            },
        })
    }
}

impl Drop for StageWriter {
    fn drop(&mut self) {
        if let Some(name) = self.name.take()
            && let Err(error) = self.disk.discard_name(&name, self.reserved)
        {
            tracing::warn!(%error, "blob staging cleanup deferred");
        }
    }
}

pub(super) struct StagedBlob {
    pub(super) name: String,
    pub(super) info: StoredBlob,
}

pub(super) fn checked_blob_size(
    current: u64,
    additional: u64,
    limit: u64,
    expected: Option<u64>,
) -> Result<u64, StorageError> {
    let size = current
        .checked_add(additional)
        .filter(|&size| size <= limit)
        .ok_or(StorageError::BlobTooLarge { limit_bytes: limit })?;
    if let Some(expected) = expected
        && size > expected
    {
        return Err(StorageError::LengthMismatch {
            expected,
            actual: size,
        });
    }
    Ok(size)
}

#[derive(Debug)]
pub(super) struct VerifiedPath<'a> {
    shard: &'a str,
    name: &'a str,
}

pub(super) fn validate_path<'a>(
    path: &'a Path,
    id: &BlobId,
) -> Result<VerifiedPath<'a>, StorageError> {
    let text = path.to_str().ok_or_else(|| invalid_path(path))?;
    let mut pieces = text.split('/');
    if pieces.next() != Some("objects") {
        return Err(invalid_path(path));
    }
    let shard = pieces
        .next()
        .filter(|shard| valid_shard(shard))
        .ok_or_else(|| invalid_path(path))?;
    if pieces.next() != Some(id.as_str()) {
        return Err(invalid_path(path));
    }
    let name = pieces
        .next()
        .filter(|name| !name.is_empty() && safe_disk_name(name) == *name)
        .ok_or_else(|| invalid_path(path))?;
    if pieces.next().is_some() {
        return Err(invalid_path(path));
    }
    Ok(VerifiedPath { shard, name })
}

fn invalid_path(path: &Path) -> StorageError {
    StorageError::InvalidBlobPath(path.display().to_string())
}

fn lower_hex(byte: u8) -> bool {
    matches!(byte, b'0'..=b'9' | b'a'..=b'f')
}

fn valid_shard(shard: &str) -> bool {
    shard.len() == 2 && shard.bytes().all(lower_hex)
}

fn shard_index(shard: &str) -> Result<usize, StorageError> {
    u8::from_str_radix(shard, 16)
        .map(usize::from)
        .map_err(|_| StorageError::InvalidBlobPath(shard.to_owned()))
}

pub fn safe_disk_name(input: &str) -> String {
    let basename = input.rsplit(['/', '\\']).next().unwrap_or_default();
    let (stem, suffix) = split_extension(basename);

    let mut safe = String::new();
    let mut replaced = false;
    let mut prev_dot = false;
    let max_stem_bytes = 255 - suffix.len();
    let max_stem_chars = 120 - suffix.chars().count();
    for (index, c) in stem.chars().take(max_stem_chars).enumerate() {
        let substitute = unsafe_filename_char(c)
            || (index == 0 && matches!(c, '.' | '-'))
            || (c == '.' && prev_dot);
        let out = if substitute { '_' } else { c };
        if safe.len() + out.len_utf8() > max_stem_bytes {
            break;
        }
        safe.push(out);
        prev_dot = c == '.';
        replaced |= substitute;
    }
    // A dot left at the end of a stem would join the extension as '..'.
    if !suffix.is_empty() && safe.ends_with('.') {
        safe.pop();
        safe.push('_');
        replaced = true;
    }
    if replaced && safe.chars().all(|c| c == '_' || c.is_whitespace()) {
        return "file.bin".into();
    }
    safe.push_str(suffix);
    // Substitution or truncation can turn an invalid extension into a valid
    // one. Apply the empty-stem rule to the final filename that will be read.
    if split_extension(&safe).0.trim().is_empty() {
        return "file.bin".into();
    }
    safe
}

fn split_extension(basename: &str) -> (&str, &str) {
    basename
        .rsplit_once('.')
        .filter(|(stem, ext)| {
            !stem.is_empty()
                && (1..=16).contains(&ext.chars().count())
                && ext
                    .chars()
                    .all(|c| !c.is_whitespace() && !unsafe_filename_char(c))
        })
        .map_or((basename, ""), |(stem, _)| (stem, &basename[stem.len()..]))
}

fn unsafe_filename_char(c: char) -> bool {
    c.is_control()
        || cooplet_protocol::is_bidi_or_line_control(c)
        || matches!(c, '/' | '\\' | ':' | '<' | '>' | '|' | '?' | '*' | '"')
}

fn checked_dir(path: &Path) -> Result<Option<File>, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_dir()
                && metadata.uid() == current_euid()
                && metadata.mode() & 0o7777 == 0o700 =>
        {
            let dir = PrivateFs::open_dir_nofollow(path)?;
            let open = dir.metadata()?;
            if (open.dev(), open.ino()) != (metadata.dev(), metadata.ino()) {
                return Err(invalid_path(path));
            }
            Ok(Some(dir))
        }
        Ok(_) => Err(invalid_path(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) struct ObjectScanner {
    root: PathBuf,
    directories: Vec<(PathBuf, File, ReadDir)>,
}

impl ObjectScanner {
    pub(super) fn next_batch(&mut self, limit: usize) -> Result<Vec<BlobRecord>, StorageError> {
        let mut batch = Vec::new();
        while batch.len() < limit {
            let depth = self.directories.len();
            let Some((parent, parent_fd, entries)) = self.directories.last_mut() else {
                break;
            };
            let Some(entry) = entries.next().transpose()? else {
                self.directories.pop();
                continue;
            };
            let name = entry.file_name();
            let relative = parent.join(&name);
            let full = self.root.join(&relative);
            let name = name.to_str().ok_or_else(|| invalid_path(&relative))?;
            if depth == 1 || depth == 2 {
                if (depth == 1 && !valid_shard(name))
                    || (depth == 2 && BlobId::parse(name).is_err())
                {
                    return Err(invalid_path(&relative));
                }
                let child_fd =
                    PrivateFs::open_dir_at(parent_fd, name).map_err(|_| invalid_path(&relative))?;
                let child_meta = child_fd.metadata()?;
                let path_meta = fs::symlink_metadata(&full)?;
                if (child_meta.dev(), child_meta.ino()) != (path_meta.dev(), path_meta.ino()) {
                    return Err(invalid_path(&relative));
                }
                self.directories
                    .push((relative, child_fd, fs::read_dir(full)?));
            } else {
                let id = relative
                    .components()
                    .nth(2)
                    .and_then(|c| match c {
                        Component::Normal(id) => id.to_str(),
                        _ => None,
                    })
                    .and_then(|id| BlobId::parse(id).ok())
                    .ok_or_else(|| invalid_path(&relative))?;
                validate_path(&relative, &id)?;
                let file = PrivateFs::open_sealed_at(parent_fd, name)
                    .map_err(|_| invalid_path(&relative))?;
                let metadata = file.metadata()?;
                batch.push(BlobRecord {
                    blob_id: id,
                    disk_rel_path: relative,
                    size_bytes: metadata.len(),
                });
            }
        }
        Ok(batch)
    }
}
