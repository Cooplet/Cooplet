//! Shared Cooplet wire contracts. Applications own transport and persistence.

mod bridge;
mod domain;
mod event;
mod hash;
mod id;
mod problem;
mod question;
mod wire;

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

/// Validate a user-facing attachment filename (1..=255 UTF-8 bytes, non-blank, no ASCII controls).
pub fn validate_attachment_name(name: &str) -> Result<(), ValidationError> {
    if name.is_empty()
        || name.len() > 255
        || name.trim().is_empty()
        || name.bytes().any(|byte| byte < 0x20 || byte == 0x7f)
    {
        Err(ValidationError("attachment name"))
    } else {
        Ok(())
    }
}

/// Validate an RFC 9110 concrete media type (`type/subtype` with optional parameters).
pub fn validate_media_type(value: &str) -> Result<(), ValidationError> {
    if value.is_empty()
        || value.len() > 255
        || !value.is_ascii()
        || value
            .bytes()
            .any(|byte| (byte < 0x20 && byte != b'\t') || byte == 0x7f)
        || parse_media_type(value.as_bytes()).is_none()
    {
        Err(ValidationError("MIME type"))
    } else {
        Ok(())
    }
}

fn parse_media_type(value: &[u8]) -> Option<()> {
    let (top, rest) = http_token(skip_ows(value))?;
    let (subtype, rest) = http_token(rest.strip_prefix(b"/")?)?;
    if top == b"*" || subtype == b"*" {
        return None;
    }
    let mut rest = skip_ows(rest);
    while !rest.is_empty() {
        rest = skip_ows(rest.strip_prefix(b";")?);
        if rest.is_empty() || rest.starts_with(b";") {
            continue;
        }
        let (_, value) = http_token(rest)?;
        let value = value.strip_prefix(b"=")?;
        rest = skip_ows(if let Some(quoted) = value.strip_prefix(b"\"") {
            quoted_http_value(quoted)?
        } else {
            http_token(value)?.1
        });
    }
    Some(())
}

fn skip_ows(value: &[u8]) -> &[u8] {
    let start = value
        .iter()
        .position(|&byte| !matches!(byte, b' ' | b'\t'))
        .unwrap_or(value.len());
    &value[start..]
}

fn http_token(value: &[u8]) -> Option<(&[u8], &[u8])> {
    let end = value
        .iter()
        .position(|&byte| !byte.is_ascii_alphanumeric() && !b"!#$%&'*+-.^_\x60|~".contains(&byte))
        .unwrap_or(value.len());
    (end != 0).then(|| value.split_at(end))
}

fn quoted_http_value(mut value: &[u8]) -> Option<&[u8]> {
    while let Some((&byte, rest)) = value.split_first() {
        value = match byte {
            b'"' => return Some(rest),
            b'\\' => {
                let (&escaped, remaining) = rest.split_first()?;
                if !matches!(escaped, b'\t' | b' '..=b'~') {
                    return None;
                }
                remaining
            }
            b'\t' | b' '..=b'~' => rest,
            _ => return None,
        };
    }
    None
}
