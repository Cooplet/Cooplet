use std::collections::BTreeMap;

use serde_json::Value;

use crate::{MessageId, OperationId, RequestId, ThreadId};

pub const PROBLEM_JSON_MEDIA_TYPE: &str = "application/problem+json";

macro_rules! error_codes {
    ($($variant:ident => $wire:tt: $status:literal, $retryable:literal, $slug:literal, $title:literal;)*) => {
        crate::wire::wire_string_enum! {
            pub enum ErrorCode {
                $($variant => $wire,)*
            }
        }

        impl ErrorCode {
            pub const ALL: &'static [Self] = &[$(Self::$variant,)*];

            pub const fn status(self) -> u16 {
                match self {
                    $(Self::$variant => $status,)*
                }
            }

            pub const fn default_retryable(self) -> bool {
                match self {
                    $(Self::$variant => $retryable,)*
                }
            }

            pub const fn type_uri(self) -> &'static str {
                match self {
                    $(Self::$variant => concat!("https://cooplet.dev/problems/", $slug),)*
                }
            }

            pub const fn title(self) -> &'static str {
                match self {
                    $(Self::$variant => $title,)*
                }
            }
        }
    };
}

error_codes! {
    InvalidRequest => "invalid_request": 400, false, "invalid-request", "Invalid request";
    Unauthorized => "unauthorized": 401, false, "unauthorized", "Unauthorized";
    NodeRevoked => "node_revoked": 401, false, "node-revoked", "Node revoked";
    Forbidden => "forbidden": 403, false, "forbidden", "Forbidden";
    AdminHumanRequired => "admin_human_required": 403, false, "admin-human-required", "Human administrator required";
    NotFound => "not_found": 404, false, "not-found", "Not found";
    OperationConflict => "operation_conflict": 409, false, "operation-conflict", "Operation conflict";
    OperationInProgress => "operation_in_progress": 409, true, "operation-in-progress", "Operation in progress";
    ParticipantBusy => "participant_busy": 409, true, "participant-busy", "Participant busy";
    AwaitingApproval => "awaiting_approval": 409, true, "awaiting-approval", "Awaiting approval";
    QuestionAlreadyAnswered => "question_already_answered": 409, false, "question-already-answered", "Question already answered";
    QuestionExpired => "question_expired": 409, false, "question-expired", "Question expired";
    PinConflict => "pin_conflict": 409, true, "pin-conflict", "Pin conflict";
    BlobDigestMismatch => "blob_digest_mismatch": 409, false, "blob-digest-mismatch", "Blob digest mismatch";
    HubIdentityMismatch => "hub_identity_mismatch": 409, false, "hub-identity-mismatch", "Hub identity mismatch";
    ResyncRequired => "resync_required": 410, false, "resync-required", "Resync required";
    BlobTooLarge => "blob_too_large": 413, false, "blob-too-large", "Blob too large";
    PayloadTooLarge => "payload_too_large": 413, false, "payload-too-large", "Payload too large";
    RateLimited => "rate_limited": 429, true, "rate-limited", "Rate limited";
    BridgeUnavailable => "bridge_unavailable": 502, true, "bridge-unavailable", "Bridge unavailable";
    DeliveryUnknown => "delivery_unknown": 503, false, "delivery-unknown", "Delivery outcome unknown";
    Overloaded => "overloaded": 503, true, "overloaded", "Overloaded";
    StorageFull => "storage_full": 507, false, "storage-full", "Storage full";
    InternalError => "internal_error": 500, true, "internal-error", "Internal error";
}

crate::wire::wire_object! {
    /// RFC 9457 problem document. Readers ignore unrecognized top-level members.
    pub struct ProblemDetails {
        #[serde(rename = "type")]
        pub problem_type: String,
        pub title: String,
        pub status: u16,
        pub detail: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub instance: Option<String>,
        pub code: ErrorCode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub request_id: Option<RequestId>,
        pub retryable: bool,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        pub extensions: BTreeMap<String, Value>,
    }
}

impl ProblemDetails {
    pub fn new(code: ErrorCode, detail: impl Into<String>) -> Self {
        Self {
            problem_type: code.type_uri().into(),
            title: code.title().into(),
            status: code.status(),
            detail: detail.into(),
            instance: None,
            code,
            request_id: None,
            retryable: code.default_retryable(),
            extensions: BTreeMap::new(),
        }
    }

    /// Typed identifiers, cursors, and reasons that a client can show when recovering from an error.
    pub fn recovery_fields(&self) -> impl Iterator<Item = (&'static str, &Value)> {
        [
            "operation_id",
            "message_id",
            "existing_answer_id",
            "thread_id",
            "floor_seq",
            "head_seq",
            "current_version",
            "reason",
        ]
        .into_iter()
        .filter_map(|key| {
            let value = self.extensions.get(key)?;
            let valid = match key {
                "operation_id" => value
                    .as_str()
                    .is_some_and(|id| OperationId::parse(id).is_ok()),
                "message_id" | "existing_answer_id" => value
                    .as_str()
                    .is_some_and(|id| MessageId::parse(id).is_ok()),
                "thread_id" => value.as_str().is_some_and(|id| ThreadId::parse(id).is_ok()),
                "current_version" => value.is_null() || value.is_u64(),
                "reason" => value.as_str().is_some_and(is_valid_error_reason),
                _ => value.is_u64(),
            };
            valid.then_some((key, value))
        })
    }

    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance = Some(instance.into());
        self
    }

    pub fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_extension(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.extensions.insert(key.into(), value.into());
        self
    }
}

/// Validates a machine-readable snake_case error reason extension.
pub fn is_valid_error_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 64
        && reason.contains('_')
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_fields_accepts_valid_reason_and_filters_invalid_values()
    -> Result<(), crate::IdError> {
        let op_id = OperationId::generate()?;
        let thread_id = ThreadId::generate()?;
        let problem = ProblemDetails::new(ErrorCode::ResyncRequired, "Cursor pruned")
            .with_extension("operation_id", op_id.as_str())
            .with_extension("thread_id", thread_id.as_str())
            .with_extension("floor_seq", 5_u64)
            .with_extension("head_seq", 10_u64)
            .with_extension("reason", "cursor_pruned");
        let fields: BTreeMap<&str, Value> = problem
            .recovery_fields()
            .map(|(k, v)| (k, v.clone()))
            .collect();
        assert_eq!(
            fields.get("operation_id"),
            Some(&Value::from(op_id.as_str()))
        );
        assert_eq!(
            fields.get("thread_id"),
            Some(&Value::from(thread_id.as_str()))
        );
        assert_eq!(fields.get("floor_seq"), Some(&Value::from(5_u64)));
        assert_eq!(fields.get("head_seq"), Some(&Value::from(10_u64)));
        assert_eq!(fields.get("reason"), Some(&Value::from("cursor_pruned")));

        for invalid in [
            "",
            "nounderscore",
            "Bad_Case",
            "has space",
            &"a_".repeat(33),
        ] {
            assert!(!is_valid_error_reason(invalid));
            let rejected = ProblemDetails::new(ErrorCode::InvalidRequest, "bad")
                .with_extension("reason", invalid);
            assert!(rejected.recovery_fields().next().is_none());
        }
        Ok(())
    }
}
