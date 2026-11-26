use std::{
    fs::{self, File, Metadata, OpenOptions, ReadDir},
    io::{self, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use cooplet_platform::{AtomicPublish, PlatformError, PrivateFs, PublishOutcome, current_euid};
use cooplet_protocol::BlobId;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use super::{BlobInput, StoredBlob};
use crate::{BlobRecord, BlobStoreConfig, StorageError};

pub(super) const CHUNK_BYTES: usize = 64 * 1024;

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
}

impl Disk {
    pub(super) fn open(
        config: BlobStoreConfig,
        database_path: &Path,
    ) -> Result<Self, StorageError> {
        if config.max_blob_bytes == 0 {
            return Err(StorageError::InvalidConfig(
                "blob size limit must be positive",
            ));
        }
        let root = PrivateFs::ensure_private_dir(&config.root)?;
        bind_root(&root, database_path)?;
        let staging = PrivateFs::ensure_private_dir(&root.join("staging"))?;
        let objects = PrivateFs::ensure_private_dir(&root.join("objects"))?;
        Ok(Self {
            root,
            staging,
            objects,
            max_blob_bytes: config.max_blob_bytes,
            max_total_bytes: config.max_total_blob_bytes,
            used_bytes: AtomicU64::new(0),
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
        for item in fs::read_dir(&self.staging)? {
            let item = item?;
            let name = item.file_name();
            let ours = name
                .to_str()
                .and_then(|name| name.strip_suffix(".part"))
                .is_some_and(|stem| stem.len() == 32 && stem.bytes().all(lower_hex));
            if !ours || !item.file_type()?.is_file() {
                return Err(StorageError::InvalidBlobPath(
                    item.path().display().to_string(),
                ));
            }
            // A failed link-and-unlink publication may still share this
            // inode with the destination. Remove the staging name only.
            fs::remove_file(item.path())?;
        }
        dir.sync_all()?;
        Ok(())
    }

    pub(super) async fn stage(
        self: Arc<Self>,
        input: BlobInput,
        mut chunks: mpsc::Receiver<InputChunk>,
    ) -> Result<StagedBlob, StorageError> {
        let mut writer =
            tokio::task::spawn_blocking(move || StageWriter::new(self, input)).await??;
        loop {
            let Some(first) = chunks.recv().await else {
                tokio::task::spawn_blocking(move || drop(writer)).await?;
                return Err(StorageError::BlobAborted);
            };
            // Only hand ready input to the blocking pool. A reader can itself
            // need that pool (for example an async file with one blocking slot).
            let mut batch = Vec::with_capacity(super::STAGING_QUEUE);
            batch.push(first);
            while batch.len() < super::STAGING_QUEUE {
                match chunks.try_recv() {
                    Ok(chunk) => batch.push(chunk),
                    Err(_) => break,
                }
            }
            match tokio::task::spawn_blocking(move || writer.write_batch(batch)).await?? {
                StageProgress::Receiving(next) => writer = next,
                StageProgress::Finished(staged) => return Ok(staged),
            }
        }
    }

    pub(super) fn publish(&self, stage: &StagedBlob) -> Result<PublishOutcome, StorageError> {
        let path = validate_path(&stage.info.disk_rel_path, &stage.info.blob_id)?;
        let shard = PrivateFs::ensure_private_dir(&self.objects.join(path.shard))?;
        let destination = PrivateFs::ensure_private_dir(&shard.join(stage.info.blob_id.as_str()))?;
        let stage_dir = PrivateFs::open_dir_nofollow(&self.staging)?;
        let dest_dir = PrivateFs::open_dir_nofollow(&destination)?;
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
        remove_file_if_present(&self.staging.join(name))?;
        self.release(reserved)
    }

    /// Called after a matching committed tombstone or a proven unpublished
    /// registration. Debit bytes only if this call removed the file.
    pub(super) fn unlink(&self, record: &BlobRecord, account: bool) -> Result<(), StorageError> {
        let path = validate_path(&record.disk_rel_path, &record.blob_id)?;
        let shard = self.objects.join(path.shard);
        let Some(shard_fd) = checked_dir(&shard)? else {
            PrivateFs::open_dir_nofollow(&self.objects)?.sync_all()?;
            return Ok(());
        };
        let object = shard.join(record.blob_id.as_str());
        if let Some(object_fd) = checked_dir(&object)? {
            let full_path = object.join(path.name);
            if let Some(metadata) = checked_object(&full_path)? {
                if metadata.len() != record.size_bytes {
                    return Err(StorageError::BlobUnverified(format!(
                        "object size differs for {}",
                        record.blob_id
                    )));
                }
                if remove_file_if_present(&full_path)? && account {
                    self.release(metadata.len())?;
                }
            }
            object_fd.sync_all()?;
            match fs::remove_dir(&object) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                    ) => {}
                Err(error) => return Err(error.into()),
            }
        }
        shard_fd.sync_all()?;
        Ok(())
    }

    pub(super) fn open_live(&self, record: &BlobRecord) -> Result<File, StorageError> {
        let path = validate_path(&record.disk_rel_path, &record.blob_id)?;
        let shard = self.objects.join(path.shard);
        let object = shard.join(record.blob_id.as_str());
        let _shard = checked_dir(&shard)?.ok_or_else(|| object_missing(record))?;
        let _object = checked_dir(&object)?.ok_or_else(|| object_missing(record))?;
        let full_path = object.join(path.name);
        let file = PrivateFs::open_sealed_file(&full_path)?;
        if file.metadata()?.len() != record.size_bytes {
            return Err(StorageError::BlobUnverified(format!(
                "object changed for {}",
                record.blob_id
            )));
        }
        Ok(file)
    }

    pub(super) fn scan(&self) -> Result<ObjectScanner, StorageError> {
        Ok(ObjectScanner {
            root: self.root.clone(),
            directories: vec![(PathBuf::from("objects"), fs::read_dir(&self.objects)?)],
        })
    }

    pub(super) fn prove_separate(
        &self,
        candidate: &BlobRecord,
        registered: &BlobRecord,
    ) -> Result<(), StorageError> {
        let file = self.open_live(registered)?;
        let registered = file.metadata()?;
        let candidate = checked_object(&self.root.join(&candidate.disk_rel_path))?
            .ok_or_else(|| object_missing(candidate))?;
        if (registered.dev(), registered.ino()) == (candidate.dev(), candidate.ino()) {
            return Err(StorageError::BlobUnverified(
                "object paths resolve to the same inode".into(),
            ));
        }
        Ok(())
    }

    fn make_part(&self) -> Result<(File, String), StorageError> {
        for _ in 0..16 {
            let mut random = [0; 16];
            cooplet_platform::random_bytes(&mut random)?;
            let name = format!("{:032x}.part", u128::from_be_bytes(random));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(self.staging.join(&name))
            {
                Ok(file) => return Ok((file, name)),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(io::Error::new(io::ErrorKind::AlreadyExists, "blob staging name collisions").into())
    }
}

fn bind_root(root: &Path, database_path: &Path) -> Result<(), StorageError> {
    const OWNER: &str = "database.owner";
    let marker = root.join(OWNER);
    let owner = database_path.as_os_str().as_encoded_bytes();
    let read = || PrivateFs::read_private_file(&marker, owner.len().max(4096) as u64);
    let stored = match read() {
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
            PrivateFs::create_private_file_atomic(root, OWNER, owner)?;
            read()?
        }
        Err(error) => return Err(error.into()),
    };
    if stored != owner {
        return Err(StorageError::InvalidConfig(
            "blob root belongs to another database",
        ));
    }
    // Also cover a previous claim whose rename was visible but directory
    // sync did not complete before shutdown.
    PrivateFs::open_dir_nofollow(root)?.sync_all()?;
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
    Receiving(StageWriter),
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

    fn write_batch(mut self, batch: Vec<InputChunk>) -> Result<StageProgress, StorageError> {
        for chunk in batch {
            match chunk {
                InputChunk::Data(data) => {
                    let bytes = data.len() as u64;
                    let size = checked_blob_size(
                        self.reserved,
                        bytes,
                        self.disk.max_blob_bytes,
                        self.input.expected_size,
                    )?;
                    self.disk.reserve(bytes)?;
                    self.reserved = size;
                    self.digest.update(&data);
                    self.file.write_all(&data)?;
                }
                InputChunk::Finish => return self.finish().map(StageProgress::Finished),
            }
        }
        Ok(StageProgress::Receiving(self))
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

pub fn safe_disk_name(input: &str) -> String {
    let basename = input.rsplit(['/', '\\']).next().unwrap_or_default();
    let (stem, suffix) = basename
        .rsplit_once('.')
        .filter(|(stem, ext)| {
            !stem.is_empty()
                && (1..=16).contains(&ext.chars().count())
                && ext
                    .chars()
                    .all(|c| !c.is_whitespace() && !unsafe_filename_char(c))
        })
        .map_or((basename, ""), |(stem, _)| (stem, &basename[stem.len()..]));

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
    if safe.trim().is_empty() || (replaced && safe.chars().all(|c| c == '_' || c.is_whitespace())) {
        return "file.bin".into();
    }
    // A dot left at the end of a stem would join the extension as '..'.
    if !suffix.is_empty() && safe.ends_with('.') {
        safe.pop();
        safe.push('_');
    }
    safe.push_str(suffix);
    safe
}

fn unsafe_filename_char(c: char) -> bool {
    c.is_control() || matches!(c, '/' | '\\' | ':' | '<' | '>' | '|' | '?' | '*' | '"')
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

fn checked_object(path: &Path) -> Result<Option<Metadata>, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_file()
                && metadata.nlink() == 1
                && metadata.uid() == current_euid()
                && metadata.mode() & 0o7777 == 0o400 =>
        {
            Ok(Some(metadata))
        }
        Ok(_) => Err(invalid_path(path)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn remove_file_if_present(path: &Path) -> io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) struct ObjectScanner {
    root: PathBuf,
    directories: Vec<(PathBuf, ReadDir)>,
}

impl ObjectScanner {
    pub(super) fn next_batch(&mut self, limit: usize) -> Result<Vec<BlobRecord>, StorageError> {
        let mut batch = Vec::new();
        while batch.len() < limit {
            let depth = self.directories.len();
            let Some((parent, entries)) = self.directories.last_mut() else {
                break;
            };
            let Some(entry) = entries.next().transpose()? else {
                self.directories.pop();
                continue;
            };
            let name = entry.file_name();
            let relative = parent.join(&name);
            let full = self.root.join(&relative);
            if depth == 1 || depth == 2 {
                let name = name.to_str().ok_or_else(|| invalid_path(&relative))?;
                if (depth == 1 && !valid_shard(name))
                    || (depth == 2 && BlobId::parse(name).is_err())
                {
                    return Err(invalid_path(&relative));
                }
                checked_dir(&full)?.ok_or_else(|| invalid_path(&relative))?;
                self.directories.push((relative, fs::read_dir(full)?));
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
                let metadata = checked_object(&full)?.ok_or_else(|| invalid_path(&relative))?;
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
