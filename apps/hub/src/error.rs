use std::fmt;

use axum::response::{IntoResponse, Response};
use cooplet_protocol::{
    ErrorCode, IdError, PROBLEM_JSON_MEDIA_TYPE, ProblemDetails, ProtocolError,
};
use cooplet_storage::{StorageError, WriteError, WriteFailure};
use http::{StatusCode, header};
use serde_json::Value;

const MAX_PROBLEM_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) struct HubError(Box<ProblemDetails>);

impl HubError {
    pub(crate) fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self(Box::new(ProblemDetails::new(code, detail)))
    }

    pub(crate) fn invalid(detail: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidRequest, detail)
    }

    pub(crate) fn internal(error: impl fmt::Display) -> Self {
        tracing::error!(%error, "Hub request failed");
        Self::new(
            ErrorCode::InternalError,
            "The Hub could not complete the request.",
        )
    }

    pub(crate) fn with_extension(
        mut self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Self {
        self.0.extensions.insert(key.into(), value.into());
        self
    }

    pub(crate) fn problem(&self) -> &ProblemDetails {
        &self.0
    }

    pub(crate) fn method_not_allowed() -> Self {
        let mut error = Self::invalid("The requested method is unavailable for this path.");
        error.0.status = 405;
        error.0.title = "Method not allowed".into();
        error
    }

    pub(crate) fn from_problem(problem: ProblemDetails) -> Self {
        Self(Box::new(problem))
    }
}

impl fmt::Display for HubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0.code, self.0.detail)
    }
}

impl std::error::Error for HubError {}

/// Apply the same response limit to persisted failure receipts and HTTP errors.
pub(crate) fn bounded_problem_json(problem: &ProblemDetails) -> serde_json::Result<Vec<u8>> {
    let bytes = serde_json::to_vec(problem)?;
    if bytes.len() <= MAX_PROBLEM_BYTES {
        return Ok(bytes);
    }

    let mut problem = problem.clone();
    problem.detail = "The request failed. See the problem code and extensions.".into();
    if problem.extensions.contains_key("action_hint") {
        problem.extensions.insert(
            "action_hint".into(),
            Value::from("Check the participant for a pending approval."),
        );
    }
    let bytes = serde_json::to_vec(&problem)?;
    if bytes.len() <= MAX_PROBLEM_BYTES {
        return Ok(bytes);
    }

    // Imported problem details can contain fields beyond the public Hub contract.
    problem.instance = None;
    problem.problem_type = problem.code.type_uri().into();
    problem.title = problem.code.title().into();
    problem.extensions.retain(|key, value| {
        matches!(
            key.as_str(),
            "actual_hub_id"
                | "existing_answer_id"
                | "oldest_available_seq"
                | "latest_seq"
                | "operation_id"
                | "message_id"
                | "approval_id"
                | "reason"
                | "action_hint"
        ) && serde_json::to_vec(value).is_ok_and(|bytes| bytes.len() <= 1024)
    });
    serde_json::to_vec(&problem)
}

impl IntoResponse for HubError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.0.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        match bounded_problem_json(&self.0) {
            Ok(bytes) => (
                status,
                [(header::CONTENT_TYPE, PROBLEM_JSON_MEDIA_TYPE)],
                bytes,
            )
                .into_response(),
            Err(error) => {
                tracing::error!(%error, "Could not serialize Hub problem");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(header::CONTENT_TYPE, PROBLEM_JSON_MEDIA_TYPE)],
                    r#"{"type":"https://cooplet.dev/problems/internal-error","title":"Internal error","status":500,"detail":"Could not encode the response.","code":"internal_error","retryable":true}"#,
                ).into_response()
            }
        }
    }
}

impl From<StorageError> for HubError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::StorageFull => {
                Self::new(ErrorCode::StorageFull, "The Hub storage limit was reached.")
            }
            StorageError::Overloaded | StorageError::Closed => {
                Self::new(ErrorCode::Overloaded, "Hub storage is unavailable.")
            }
            StorageError::BlobTooLarge { .. } => {
                Self::new(ErrorCode::BlobTooLarge, error.to_string())
            }
            StorageError::DigestMismatch => Self::new(
                ErrorCode::BlobDigestMismatch,
                "The uploaded bytes do not match the declared SHA-256.",
            ),
            StorageError::LengthMismatch { .. }
            | StorageError::Validation(_)
            | StorageError::BlobAborted => Self::invalid(error.to_string()),
            StorageError::WriteOutcomeUnknown | StorageError::CommitFailed(_) => Self::new(
                ErrorCode::DeliveryUnknown,
                "The Hub could not confirm the write outcome.",
            ),
            StorageError::Sqlite(rusqlite::Error::ToSqlConversionFailure(ref cause))
                if cause.is::<crate::storage::events::FrameTooLarge>() =>
            {
                Self::new(
                    ErrorCode::PayloadTooLarge,
                    "The event frame exceeds the maximum size.",
                )
            }
            error => Self::internal(error),
        }
    }
}

impl From<rusqlite::Error> for HubError {
    fn from(error: rusqlite::Error) -> Self {
        StorageError::from(error).into()
    }
}

impl From<WriteFailure<HubError>> for HubError {
    fn from(error: WriteFailure<HubError>) -> Self {
        match error {
            WriteFailure::Storage(error) => error.into(),
            WriteFailure::App(error) => error,
        }
    }
}

impl From<HubError> for WriteError<HubError> {
    fn from(error: HubError) -> Self {
        Self::Domain(error)
    }
}

impl From<IdError> for HubError {
    fn from(error: IdError) -> Self {
        match error {
            IdError::Invalid(_) => Self::invalid(error.to_string()),
            _ => Self::internal(error),
        }
    }
}

impl From<ProtocolError> for HubError {
    fn from(error: ProtocolError) -> Self {
        Self::invalid(error.to_string())
    }
}

impl From<serde_json::Error> for HubError {
    fn from(error: serde_json::Error) -> Self {
        Self::internal(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_problem_preserves_the_typed_extensions() -> Result<(), serde_json::Error> {
        let text = "á\n\"".repeat(30_000);
        let problem = ProblemDetails::new(ErrorCode::AwaitingApproval, &text)
            .with_extension("approval_id", "approval-1")
            .with_extension("action_hint", text.clone());
        let bytes = bounded_problem_json(&problem)?;
        assert!(bytes.len() <= MAX_PROBLEM_BYTES);
        let bounded: ProblemDetails = serde_json::from_slice(&bytes)?;
        assert_eq!(bounded.code, ErrorCode::AwaitingApproval);
        assert_eq!(bounded.extensions["approval_id"], "approval-1");
        assert_ne!(bounded.detail, text);
        assert_ne!(bounded.extensions["action_hint"], text);
        Ok(())
    }
}
