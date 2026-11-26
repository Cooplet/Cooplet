//! Shared Cooplet wire contracts. Applications own transport and persistence.

mod bridge;
mod domain;
mod event;
mod hash;
mod id;
mod problem;
mod question;

pub use bridge::*;
pub use domain::*;
pub use event::*;
pub use hash::*;
pub use id::*;
pub use problem::*;
pub use question::*;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("invalid wire JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[error("unsupported bridge version {0}")]
    BridgeVersion(u8),
    #[error("expected bridge kind {expected:?}, received {actual:?}")]
    BridgeKind {
        expected: FrameKind,
        actual: FrameKind,
    },
    #[error("bridge frame is {actual} bytes, limit is {MAX_WS_FRAME_BYTES}")]
    BridgeFrameTooLarge { actual: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid {0}")]
pub struct ValidationError(pub &'static str);

pub fn validate_idempotency_key(key: &str) -> Result<(), ValidationError> {
    if (8..=128).contains(&key.len()) && key.bytes().all(|byte| byte.is_ascii_graphic()) {
        Ok(())
    } else {
        Err(ValidationError("idempotency key"))
    }
}

pub fn validate_step_key(key: &str) -> Result<(), ValidationError> {
    if (1..=128).contains(&key.len())
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        Ok(())
    } else {
        Err(ValidationError("step key"))
    }
}
