use std::{path::PathBuf, time::Duration};

pub const DEFAULT_MAX_BLOB_BYTES: u64 = 50 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SynchronousMode {
    #[default]
    Full,
    Normal,
}

#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub db_path: PathBuf,
    pub read_pool_size: u32,
    pub write_queue_capacity: usize,
    pub broadcast_capacity: usize,
    pub max_page_count: u32,
    pub journal_size_limit_bytes: u64,
    pub wal_autocheckpoint_pages: u32,
    pub busy_timeout: Duration,
    pub synchronous: SynchronousMode,
    pub gc_interval: Duration,
}

impl StorageConfig {
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self {
            db_path: db_path.into(),
            read_pool_size: 4,
            write_queue_capacity: 256,
            broadcast_capacity: 1024,
            max_page_count: 1_048_576,
            journal_size_limit_bytes: 67_108_864,
            wal_autocheckpoint_pages: 1000,
            busy_timeout: Duration::from_secs(5),
            synchronous: SynchronousMode::Full,
            gc_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BlobStoreConfig {
    pub root: PathBuf,
    pub max_blob_bytes: u64,
    pub max_total_blob_bytes: u64,
}

impl BlobStoreConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_blob_bytes: DEFAULT_MAX_BLOB_BYTES,
            max_total_blob_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}
