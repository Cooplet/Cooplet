use std::{fmt, io, sync::Arc};

use cooplet_protocol::{ErrorCode, HubId, PROBLEM_JSON_MEDIA_TYPE, ProblemDetails};
use http::{StatusCode, header::CONTENT_TYPE};
use reqwest::Response;

const MAX_PROBLEM_BYTES: usize = 64 * 1024;

#[derive(Clone, thiserror::Error)]
pub enum ClientError {
    #[error("hub identity mismatch")]
    HubIdentityMismatch {
        expected: HubId,
        actual: Option<String>,
    },
    #[error("resync required (oldest={oldest_available_seq}, latest={latest_seq})")]
    ResyncRequired {
        oldest_available_seq: u64,
        latest_seq: u64,
        reason: String,
    },
    #[error("API problem ({}): {:?}", .0.status, .0.code)]
    Problem(Box<ProblemDetails>),
    #[error("blob digest mismatch: expected {expected}, actual {actual}")]
    BlobDigestMismatch { expected: String, actual: String },
    #[error("blob exceeds maximum size of {max_bytes} bytes")]
    BlobTooLarge { max_bytes: u64 },
    #[error("blob length mismatch: expected {expected} bytes, received {actual}")]
    BlobLengthMismatch { expected: u64, actual: u64 },
    #[error("invalid client configuration or request: {0}")]
    InvalidInput(String),
    #[error("unexpected HTTP status {status}")]
    UnexpectedStatus { status: u16 },
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("HTTP transport failed: {0}")]
    Transport(#[source] Arc<reqwest::Error>),
    #[error("I/O failed: {0}")]
    Io(#[source] Arc<io::Error>),
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Structured data remains available to callers. Formatting never
        // includes arbitrary response bodies, details, URLs or credentials.
        fmt::Display::fmt(self, formatter)
    }
}

impl From<reqwest::Error> for ClientError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(Arc::new(error.without_url()))
    }
}

impl From<io::Error> for ClientError {
    fn from(error: io::Error) -> Self {
        Self::Io(Arc::new(error))
    }
}

impl ClientError {
    pub(crate) fn invalid(message: &'static str) -> Self {
        Self::InvalidInput(message.into())
    }

    pub(crate) fn protocol(message: &'static str) -> Self {
        Self::Protocol(message.into())
    }

    pub(crate) fn unexpected(status: StatusCode) -> Self {
        // A proxy or peer can reflect a secret in its error body.
        Self::UnexpectedStatus {
            status: status.as_u16(),
        }
    }

    pub(crate) fn resync(oldest_available_seq: u64, latest_seq: u64, reason: String) -> Self {
        if oldest_available_seq == 0 || oldest_available_seq > latest_seq.saturating_add(1) {
            Self::protocol("inconsistent EventBus watermarks")
        } else {
            Self::ResyncRequired {
                oldest_available_seq,
                latest_seq,
                reason,
            }
        }
    }
}

pub(crate) fn media_type(response: &Response) -> Option<&str> {
    response
        .headers()
        .get(CONTENT_TYPE)?
        .to_str()
        .ok()
        .map(|value| value.split(';').next().unwrap_or_default().trim())
}

pub(crate) async fn read_bounded(
    mut response: Response,
    limit: usize,
) -> Result<Vec<u8>, ClientError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(ClientError::protocol("response body exceeds limit"));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > limit - body.len() {
            return Err(ClientError::protocol("response body exceeds limit"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

pub(crate) async fn parse_failure(
    response: Response,
    hub: Option<&HubId>,
    events: bool,
) -> ClientError {
    let status = response.status();
    let declared_problem = media_type(&response)
        .is_some_and(|value| value.eq_ignore_ascii_case(PROBLEM_JSON_MEDIA_TYPE));
    let body = match read_bounded(response, MAX_PROBLEM_BYTES).await {
        Ok(body) => body,
        Err(ClientError::Protocol(_)) if !declared_problem => {
            return ClientError::unexpected(status);
        }
        Err(error) => return error,
    };
    let problem: ProblemDetails = match serde_json::from_slice(&body) {
        Ok(problem) => problem,
        Err(_) if declared_problem => {
            return ClientError::protocol("invalid Cooplet problem response");
        }
        Err(_) => return ClientError::unexpected(status),
    };
    if problem.status != status.as_u16() {
        return ClientError::protocol("problem status does not match HTTP status");
    }
    if status == StatusCode::CONFLICT
        && problem.code == ErrorCode::HubIdentityMismatch
        && let Some(expected) = hub
    {
        let actual = problem
            .extensions
            .get("actual_hub_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| HubId::parse(value).ok())
            .map(|id| id.to_string());
        return ClientError::HubIdentityMismatch {
            expected: expected.clone(),
            actual,
        };
    }
    if status == StatusCode::GONE && events && problem.code == ErrorCode::ResyncRequired {
        let oldest = problem
            .extensions
            .get("oldest_available_seq")
            .and_then(serde_json::Value::as_u64);
        let latest = problem
            .extensions
            .get("latest_seq")
            .and_then(serde_json::Value::as_u64);
        return match (oldest, latest) {
            (Some(oldest), Some(latest)) => ClientError::resync(oldest, latest, problem.detail),
            _ => ClientError::protocol("resync response is missing EventBus watermarks"),
        };
    }
    ClientError::Problem(Box::new(problem))
}
