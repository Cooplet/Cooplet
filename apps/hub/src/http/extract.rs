use std::time::Duration;

use axum::{
    Json,
    body::Body,
    extract::{FromRequest, FromRequestParts, Path, Query, Request, rejection::JsonRejection},
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
        decode_json(content_type, bytes).await.map(Self)
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
        decode_json(content_type, bytes).await
    }
}

async fn read_body(
    request: Request,
    state: &AppState,
) -> Result<(Option<HeaderValue>, Bytes), HubError> {
    let (content_type, duplicate_type) = header_value(request.headers(), &header::CONTENT_TYPE);
    let content_type = content_type.cloned();
    let mut shutdown = state.shutdown.clone();
    let bytes = tokio::select! {
        biased;
        _ = shutdown.changed() => return Err(HubError::new(ErrorCode::Overloaded, "The Hub is shutting down.")),
        result = tokio::time::timeout(
            JSON_READ_TIMEOUT,
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

async fn decode_json<T: DeserializeOwned>(
    content_type: Option<HeaderValue>,
    bytes: Bytes,
) -> Result<T, HubError> {
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err(HubError::invalid("Expected a JSON object."));
    }

    // The timed, size-limited read already finished. Let Axum validate the media
    // type and decode the buffered JSON with its usual extractor.
    let mut request = Request::new(Body::from(bytes));
    if let Some(value) = content_type {
        request.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    Json::<T>::from_request(request, &())
        .await
        .map(|Json(value)| value)
        .map_err(|error| match error {
            JsonRejection::MissingJsonContentType(_) => {
                HubError::invalid("Expected Content-Type application/json.")
            }
            error if error.status() == StatusCode::PAYLOAD_TOO_LARGE => payload_too_large(),
            _ => HubError::invalid("The JSON body does not match the endpoint schema."),
        })
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

    #[tokio::test]
    async fn bodyless_commands_accept_only_an_empty_json_object_when_a_body_is_supplied() {
        let content_type = || Some(HeaderValue::from_static("application/json"));
        assert!(
            decode_json::<EmptyJson>(content_type(), Bytes::from_static(b"{}"))
                .await
                .is_ok()
        );
        for body in [b"[]".as_slice(), b"null", b"{\"unexpected\":1}", b"{}{}"] {
            let result = decode_json::<EmptyJson>(content_type(), Bytes::from_static(body)).await;
            assert!(result.is_err_and(|error| error.problem().code == ErrorCode::InvalidRequest));
        }
    }
}
