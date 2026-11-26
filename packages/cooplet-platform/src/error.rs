use std::{io, path::PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishStage {
    RenameDirectorySync,
    DirectoryIdentity,
    DestinationSync,
    StagingUnlink,
    StagingSync,
}

#[derive(Debug, thiserror::Error)]
pub enum PlatformError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("invalid sidecar lock path: {0}")]
    InvalidLockPath(String),
    #[error("the sidecar lock is busy")]
    LockBusy,
    #[error("the path no longer refers to the opened object")]
    PathReplaced,
    #[error("insecure file permissions at {path}: {detail}")]
    InsecurePermissions { path: PathBuf, detail: String },
    #[error("file exceeds the limit of {limit_bytes} bytes")]
    FileTooLarge { limit_bytes: u64 },
    #[error("process ownership could not be established")]
    ProcessNotOwned,
    #[error("invalid process identity: {0}")]
    InvalidProcessIdentity(String),
    #[error("continuous monotonic clock unavailable")]
    ClockUnavailable,
    #[error("publication visible but durability unknown at {stage:?}: {source}")]
    PublishedDurabilityUnknown {
        stage: PublishStage,
        source: io::Error,
    },
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl PlatformError {
    pub(crate) fn insecure(path: &std::path::Path, detail: &str) -> Self {
        Self::InsecurePermissions {
            path: path.into(),
            detail: detail.into(),
        }
    }
}
