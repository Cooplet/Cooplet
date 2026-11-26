use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::RequestId;

pub const PROBLEM_JSON_MEDIA_TYPE: &str = "application/problem+json";

macro_rules! error_codes {
    ($($variant:ident: $status:literal, $retryable:literal, $slug:literal, $title:literal;)*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum ErrorCode {
            $($variant,)*
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
    InvalidRequest: 400, false, "invalid-request", "Invalid request";
    Unauthorized: 401, false, "unauthorized", "Unauthorized";
    NodeRevoked: 401, false, "node-revoked", "Node revoked";
    Forbidden: 403, false, "forbidden", "Forbidden";
    AdminHumanRequired: 403, false, "admin-human-required", "Human administrator required";
    NotFound: 404, false, "not-found", "Not found";
    OperationConflict: 409, false, "operation-conflict", "Operation conflict";
    OperationInProgress: 409, true, "operation-in-progress", "Operation in progress";
    ParticipantBusy: 409, true, "participant-busy", "Participant busy";
    AwaitingApproval: 409, true, "awaiting-approval", "Awaiting approval";
    QuestionAlreadyAnswered: 409, false, "question-already-answered", "Question already answered";
    QuestionExpired: 409, false, "question-expired", "Question expired";
    PinConflict: 409, true, "pin-conflict", "Pin conflict";
    BlobDigestMismatch: 409, false, "blob-digest-mismatch", "Blob digest mismatch";
    HubIdentityMismatch: 409, false, "hub-identity-mismatch", "Hub identity mismatch";
    ResyncRequired: 410, false, "resync-required", "Resync required";
    BlobTooLarge: 413, false, "blob-too-large", "Blob too large";
    PayloadTooLarge: 413, false, "payload-too-large", "Payload too large";
    RateLimited: 429, true, "rate-limited", "Rate limited";
    BridgeUnavailable: 502, true, "bridge-unavailable", "Bridge unavailable";
    DeliveryUnknown: 503, false, "delivery-unknown", "Delivery outcome unknown";
    Overloaded: 503, true, "overloaded", "Overloaded";
    StorageFull: 507, false, "storage-full", "Storage full";
    InternalError: 500, true, "internal-error", "Internal error";
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
