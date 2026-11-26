use std::io;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("invalid storage configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("incompatible SQLite database: {0}")]
    IncompatibleDatabase(String),
    #[error("SQLite page limit {requested} cannot be set with {effective} pages")]
    PageLimit { requested: u32, effective: u32 },
    #[error("storage is full")]
    StorageFull,
    #[error("the database writer queue is full")]
    Overloaded,
    #[error("storage is closed")]
    Closed,
    #[error("the accepted writer operation has no confirmed result")]
    WriteOutcomeUnknown,
    #[error("SQLite could not confirm commit: {0}")]
    CommitFailed(rusqlite::Error),
    #[error("the database writer thread stopped unexpectedly")]
    WriterPanicked,
    #[error("blob exceeds its limit of {limit_bytes} bytes")]
    BlobTooLarge { limit_bytes: u64 },
    #[error("blob length mismatch: expected {expected} bytes, received {actual}")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("blob SHA-256 digest mismatch")]
    DigestMismatch,
    #[error("blob ID or destination already exists")]
    BlobCollision,
    #[error("blob path is invalid: {0}")]
    InvalidBlobPath(String),
    #[error("blob state could not be verified: {0}")]
    BlobUnverified(String),
    #[error("blob input ended without a successful EOF")]
    BlobAborted,
    #[error(transparent)]
    Validation(#[from] cooplet_protocol::ValidationError),
    #[error(transparent)]
    Platform(#[from] cooplet_platform::PlatformError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Sqlite(rusqlite::Error),
}

impl From<rusqlite::Error> for StorageError {
    fn from(error: rusqlite::Error) -> Self {
        if error.sqlite_error_code() == Some(rusqlite::ErrorCode::DiskFull) {
            Self::StorageFull
        } else {
            Self::Sqlite(error)
        }
    }
}

#[derive(Debug)]
pub enum WriteError<E> {
    Sqlite(rusqlite::Error),
    Domain(E),
}

impl<E> From<rusqlite::Error> for WriteError<E> {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sqlite(error)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WriteFailure<E> {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("application rejected the write")]
    App(E),
}

impl<E> From<WriteError<E>> for WriteFailure<E> {
    fn from(error: WriteError<E>) -> Self {
        match error {
            WriteError::Sqlite(error) => Self::Storage(error.into()),
            WriteError::Domain(error) => Self::App(error),
        }
    }
}
