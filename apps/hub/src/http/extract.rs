use std::time::Duration;

use axum::{
    extract::{FromRequest, FromRequestParts, Path, Query, Request},
    http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts},
};
use bytes::Bytes;
use cooplet_client::IDEMPOTENCY_KEY;
use cooplet_protocol::{ErrorCode, validate_idempotency_key};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::sync::watch;
use utoipa::ToSchema;

use crate::{AppState, auth::AuthenticatedCaller, error::HubError};

use super::admission::{header_value, payload_too_large};

const JSON_READ_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
pub(super) struct ReadTimeout(pub Duration);

pub(super) struct Caller(pub AuthenticatedCaller);

impl FromRequestParts<AppState> for Caller {
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, _: &AppState) -> Result<Self, Self::Rejection> {
        parts.extensions.get().cloned().map(Self).ok_or_else(|| {
            HubError::internal("protected Hub route did not have an authenticated caller")
        })
    }
}

#[derive(Clone)]
pub(super) struct Revocations(pub watch::Receiver<u64>);

impl FromRequestParts<AppState> for Revocations {
    type Rejection = HubError;

    async fn from_request_parts(parts: &mut Parts, _: &AppState) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .remove()
            .ok_or_else(|| HubError::internal("event stream did not subscribe to revocation"))
    }
}

pub(super) struct StrictPath<T>(pub T);

impl<T: DeserializeOwned + Send> FromRequestParts<AppState> for StrictPath<T> {
    type Rejection = HubError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|_| HubError::invalid("Invalid path parameters."))
    }
}

pub(super) struct StrictQuery<T>(pub T);

impl<T: DeserializeOwned> FromRequestParts<AppState> for StrictQuery<T> {
    type Rejection = HubError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|_| HubError::invalid("Invalid query parameters."))
    }
}

pub(super) struct StrictJson<T>(pub T);

impl<T: DeserializeOwned> FromRequest<AppState> for StrictJson<T> {
    type Rejection = HubError;

    async fn from_request(request: Request, state: &AppState) -> Result<Self, Self::Rejection> {
        let (content_type, bytes) = read_body(request, state).await?;
        decode_json(content_type.as_ref(), &bytes).map(Self)
    }
}

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct EmptyJson {}

impl FromRequest<AppState> for EmptyJson {
    type Rejection = HubError;

    async fn from_request(request: Request, state: &AppState) -> Result<Self, Self::Rejection> {
        let (content_type, bytes) = read_body(request, state).await?;
        if bytes.is_empty() {
            return Ok(Self {});
        }
        decode_json(content_type.as_ref(), &bytes)
    }
}

async fn read_body(
    request: Request,
    state: &AppState,
) -> Result<(Option<HeaderValue>, Bytes), HubError> {
    let (content_type, duplicate_type) = header_value(request.headers(), &header::CONTENT_TYPE);
    let content_type = content_type.cloned();
    let timeout = request
        .extensions()
        .get::<ReadTimeout>()
        .map_or(JSON_READ_TIMEOUT, |limit| limit.0);
    let mut shutdown = state.shutdown.clone();
    let bytes = tokio::select! {
        biased;
        _ = shutdown.changed() => return Err(HubError::new(ErrorCode::Overloaded, "The Hub is shutting down.")),
        result = tokio::time::timeout(
            timeout,
            Bytes::from_request(request, state),
        ) => result,
    };
    let bytes = bytes
        .map_err(|_| HubError::invalid("The JSON request body read timed out."))?
        .map_err(|error| {
            if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                payload_too_large()
            } else {
                HubError::invalid("Could not read the JSON request body.")
            }
        })?;
    if duplicate_type {
        return Err(HubError::invalid("Duplicate Content-Type header."));
    }
    Ok((content_type, bytes))
}

fn decode_json<T: DeserializeOwned>(
    content_type: Option<&HeaderValue>,
    bytes: &[u8],
) -> Result<T, HubError> {
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(HubError::invalid("Expected a JSON object."));
    }
    if !is_json_content_type(content_type) {
        return Err(HubError::invalid("Expected Content-Type application/json."));
    }
    serde_json::from_slice(bytes)
        .map_err(|_| HubError::invalid("The JSON body does not match the endpoint schema."))
}

fn is_json_content_type(header: Option<&HeaderValue>) -> bool {
    let Some(value) = header.and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let essence = value.split(';').next().unwrap_or("").trim();
    let Some((ty, subtype)) = essence.split_once('/') else {
        return false;
    };
    if !ty.eq_ignore_ascii_case("application") || subtype.bytes().any(|b| !is_token_char(b)) {
        return false;
    }
    subtype.eq_ignore_ascii_case("json")
        || (subtype.len() > 5 && subtype[subtype.len() - 5..].eq_ignore_ascii_case("+json"))
}

fn is_token_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

pub(super) fn idempotency_key(headers: &HeaderMap) -> Result<&str, HubError> {
    let (first, duplicate) = header_value(headers, &IDEMPOTENCY_KEY);
    let key = first
        .filter(|_| !duplicate)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| HubError::invalid("Expected exactly one Idempotency-Key header."))?;
    validate_idempotency_key(key).map_err(|error| HubError::invalid(error.to_string()))?;
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodyless_commands_accept_only_an_empty_json_object_when_a_body_is_supplied() {
        let content_type = HeaderValue::from_static("application/json");
        assert!(decode_json::<EmptyJson>(Some(&content_type), b"{}").is_ok());
        for body in [b"[]".as_slice(), b"null", b"{\"unexpected\":1}", b"{}{}"] {
            let result = decode_json::<EmptyJson>(Some(&content_type), body);
            assert!(result.is_err_and(|error| error.problem().code == ErrorCode::InvalidRequest));
        }
    }

    #[test]
    fn json_content_type_matches_application_json_and_structured_suffix() {
        for valid in [
            "application/json",
            "APPLICATION/JSON; charset=utf-8",
            "application/problem+json",
            "application/vnd.cooplet+JSON; version=1",
        ] {
            let header = HeaderValue::from_static(valid);
            assert!(is_json_content_type(Some(&header)), "rejected {valid}");
        }
        for invalid in [
            "",
            "text/plain",
            "application/json+xml",
            "application/+json",
            "application/json extra",
        ] {
            let header = HeaderValue::from_static(invalid);
            assert!(!is_json_content_type(Some(&header)), "accepted {invalid}");
        }
        assert!(!is_json_content_type(None));
    }
}
