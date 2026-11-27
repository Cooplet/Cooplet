use cooplet_protocol::{ErrorCode, ProblemDetails};
use cooplet_storage::{StorageError, WriteError, WriteFailure};

#[derive(Debug, thiserror::Error)]
#[error("{code}: {detail}", code = .0.code, detail = .0.detail)]
pub struct NodeError(pub Box<ProblemDetails>);

pub type Result<T> = std::result::Result<T, NodeError>;

impl NodeError {
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self(Box::new(ProblemDetails::new(code, detail)))
    }

    pub fn code(&self) -> ErrorCode {
        self.0.code
    }

    pub fn with_extension(mut self, name: &str, value: impl Into<serde_json::Value>) -> Self {
        self.0.extensions.insert(name.into(), value.into());
        self
    }

    pub fn retryable(mut self, retryable: bool) -> Self {
        self.0.retryable = retryable;
        self
    }

    pub fn internal(error: impl std::fmt::Display) -> Self {
        tracing::warn!(%error, "Node operation failed");
        Self::new(
            ErrorCode::InternalError,
            "The Node could not complete the request",
        )
    }
}

impl From<StorageError> for NodeError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::StorageFull => Self::new(ErrorCode::StorageFull, error.to_string()),
            StorageError::Overloaded | StorageError::Closed => {
                Self::new(ErrorCode::Overloaded, error.to_string())
            }
            StorageError::WriteOutcomeUnknown => Self::new(
                ErrorCode::DeliveryUnknown,
                "The database write outcome is unknown",
            ),
            StorageError::CommitFailed(cause) => {
                tracing::warn!(error = %cause, "Database commit failed");
                Self::new(
                    ErrorCode::DeliveryUnknown,
                    "The database commit failed and the operation outcome is unknown",
                )
            }
            StorageError::BlobTooLarge { limit_bytes } => {
                Self::new(ErrorCode::BlobTooLarge, error.to_string())
                    .with_extension("limit_bytes", limit_bytes)
            }
            StorageError::LengthMismatch { .. } | StorageError::Validation(_) => {
                Self::new(ErrorCode::InvalidRequest, error.to_string())
            }
            StorageError::DigestMismatch => {
                Self::new(ErrorCode::BlobDigestMismatch, error.to_string())
            }
            other => Self::internal(other),
        }
    }
}

impl From<WriteFailure<NodeError>> for NodeError {
    fn from(error: WriteFailure<NodeError>) -> Self {
        match error {
            WriteFailure::Storage(error) => error.into(),
            WriteFailure::App(error) => error,
        }
    }
}

impl From<NodeError> for WriteError<NodeError> {
    fn from(error: NodeError) -> Self {
        Self::Domain(error)
    }
}

impl From<cooplet_protocol::IdError> for NodeError {
    fn from(error: cooplet_protocol::IdError) -> Self {
        Self::internal(error)
    }
}

impl From<std::io::Error> for NodeError {
    fn from(error: std::io::Error) -> Self {
        Self::internal(error)
    }
}

impl From<rusqlite::Error> for NodeError {
    fn from(error: rusqlite::Error) -> Self {
        Self::from(StorageError::from(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_commit_failed_is_sanitized_and_blob_too_large_keeps_limit() {
        let commit = NodeError::from(StorageError::CommitFailed(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(1),
            Some("sqlite detail: secret/path/node.sqlite3".into()),
        )));
        assert_eq!(commit.code(), ErrorCode::DeliveryUnknown);
        assert!(!commit.0.detail.contains("secret/path"));
        assert!(!commit.0.detail.contains("sqlite"));

        let oversized = NodeError::from(StorageError::BlobTooLarge { limit_bytes: 4096 });
        assert_eq!(oversized.code(), ErrorCode::BlobTooLarge);
        assert_eq!(oversized.0.extensions["limit_bytes"], 4096);
    }
}
