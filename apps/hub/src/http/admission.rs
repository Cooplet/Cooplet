use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use cooplet_client::X_COOPLET_HUB;
use cooplet_protocol::{ErrorCode, HubId};
use futures_util::{StreamExt, stream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{AppState, auth, error::HubError};

use super::{
    BEARER_CHALLENGE, ENROLLMENT_BODY_LIMIT, JSON_BODY_LIMIT,
    extract::{ReadTimeout, Revocations},
};

const ENROLLMENT_READ_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Access {
    Public,
    Bearer,
    Admin,
    Enrollment,
    EventStream,
    Blob,
}

#[derive(Clone)]
pub(super) struct Gate {
    state: AppState,
    access: Access,
    permits: Arc<Semaphore>,
}

impl Gate {
    pub(super) fn new(state: AppState, access: Access, permits: Arc<Semaphore>) -> Self {
        Self {
            state,
            access,
            permits,
        }
    }
}

pub(super) async fn check_request(
    State(gate): State<Gate>,
    request: Request,
    next: Next,
) -> Response {
    let requires_bearer = !matches!(gate.access, Access::Public | Access::Enrollment);
    let mut response = admit(gate, request, next).await.into_response();
    if requires_bearer && response.status() == StatusCode::UNAUTHORIZED {
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            HeaderValue::from_static(BEARER_CHALLENGE),
        );
    }
    response
}

async fn admit(gate: Gate, mut request: Request, next: Next) -> Result<Response, HubError> {
    let permit = gate.permits.try_acquire_owned().map_err(|_| {
        HubError::new(
            ErrorCode::Overloaded,
            "The Hub request concurrency limit is in use.",
        )
    })?;

    check_pin(request.headers(), &gate.state.meta.hub_id, gate.access)?;
    let authorization = single_authorization(request.headers())?;
    match gate.access {
        Access::Public => {}
        Access::Enrollment => {
            if authorization.is_some() {
                return Err(unauthorized());
            }
            request
                .extensions_mut()
                .insert(ReadTimeout(ENROLLMENT_READ_TIMEOUT));
        }
        access => {
            let revocations =
                (access == Access::EventStream).then(|| gate.state.revocations.subscribe());
            let token = bearer(authorization)?;
            let caller = auth::authenticate(&gate.state.reads, &gate.state.master, token).await?;
            if access == Access::Admin {
                caller.require_admin()?;
            }
            request.extensions_mut().insert(caller);
            if let Some(revocations) = revocations {
                request.extensions_mut().insert(Revocations(revocations));
            }
        }
    }

    let read_only = request.method() == Method::GET || request.method() == Method::HEAD;
    let bodyless = read_only || request.method() == Method::DELETE;
    check_body_headers(request.headers(), bodyless, gate.access)?;
    let mut response = next.run(request).await;
    if response.status().is_success()
        && (gate.access == Access::EventStream || gate.access == Access::Blob && read_only)
    {
        hold_until_body_ends(&mut response, permit);
    }
    Ok(response)
}

fn hold_until_body_ends(response: &mut Response, permit: OwnedSemaphorePermit) {
    // Pending reads keep the permit in the stream state. Dropping the HTTP body also
    // drops it on disconnect, including when the listener enforces its write deadline.
    let data = std::mem::take(response.body_mut()).into_data_stream();
    *response.body_mut() = Body::from_stream(stream::unfold(
        (data, permit),
        |(mut data, permit)| async move { data.next().await.map(|chunk| (chunk, (data, permit))) },
    ));
}

fn check_pin(headers: &HeaderMap, hub_id: &HubId, access: Access) -> Result<(), HubError> {
    let (first, duplicate) = header_value(headers, &X_COOPLET_HUB);
    if !duplicate
        && match first {
            Some(value) => value.as_bytes() == hub_id.as_str().as_bytes(),
            None => access == Access::Public,
        }
    {
        return Ok(());
    }
    Err(HubError::new(
        ErrorCode::HubIdentityMismatch,
        "The Hub identity pin is missing or does not match.",
    )
    .with_extension("actual_hub_id", hub_id.as_str()))
}

pub(crate) fn bridge_token<'a>(
    headers: &'a HeaderMap,
    hub_id: &HubId,
) -> Result<&'a str, HubError> {
    check_pin(headers, hub_id, Access::Bearer)?;
    check_body_headers(headers, true, Access::Bearer)?;
    bearer(single_authorization(headers)?)
}

fn single_authorization(headers: &HeaderMap) -> Result<Option<&axum::http::HeaderValue>, HubError> {
    let (authorization, duplicate) = header_value(headers, &header::AUTHORIZATION);
    if duplicate {
        return Err(unauthorized());
    }
    Ok(authorization)
}

fn bearer(authorization: Option<&axum::http::HeaderValue>) -> Result<&str, HubError> {
    let token = authorization
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("Bearer"))
        .map(|(_, token)| token.trim_start_matches(' '))
        .filter(|token| !token.is_empty() && token.bytes().all(|byte| byte.is_ascii_graphic()));
    token.ok_or_else(unauthorized)
}

fn unauthorized() -> HubError {
    HubError::new(
        ErrorCode::Unauthorized,
        "A valid Bearer credential is required here.",
    )
}

fn check_body_headers(
    headers: &HeaderMap,
    read_only: bool,
    access: Access,
) -> Result<(), HubError> {
    if read_only && headers.contains_key(header::TRANSFER_ENCODING) {
        return Err(HubError::invalid(
            "This route does not accept a request body.",
        ));
    }

    // Blob uploads validate their own framing and configured byte limits.
    if !read_only && access == Access::Blob {
        return Ok(());
    }

    let (length, duplicate) = header_value(headers, &header::CONTENT_LENGTH);
    if duplicate {
        return Err(HubError::invalid("Duplicate Content-Length header."));
    }
    let Some(length) = length else {
        return Ok(());
    };
    let length = length
        .to_str()
        .ok()
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| HubError::invalid("Invalid Content-Length header."))?;
    let max = if read_only {
        0
    } else if access == Access::Enrollment {
        ENROLLMENT_BODY_LIMIT
    } else {
        JSON_BODY_LIMIT
    };
    if length > max as u64 {
        return Err(payload_too_large());
    }
    Ok(())
}

pub(super) fn payload_too_large() -> HubError {
    HubError::new(
        ErrorCode::PayloadTooLarge,
        "The request body exceeds the route's byte limit.",
    )
}

pub(super) fn header_value<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
) -> (Option<&'a axum::http::HeaderValue>, bool) {
    let all = headers.get_all(name);
    let mut values = all.iter();
    (values.next(), values.next().is_some())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn streaming_response_releases_concurrency_slot_at_eof() -> Result<(), Box<dyn Error>> {
        let slots = Arc::new(Semaphore::new(1));
        let mut response = Response::new(Body::from("frame"));
        hold_until_body_ends(&mut response, slots.clone().try_acquire_owned()?);
        assert_eq!(slots.available_permits(), 0);

        let mut chunks = response.into_body().into_data_stream();
        assert_eq!(
            chunks.next().await.transpose()?,
            Some(Bytes::from_static(b"frame"))
        );
        assert_eq!(slots.available_permits(), 0);
        assert!(chunks.next().await.is_none());
        assert_eq!(slots.available_permits(), 1);
        Ok(())
    }

    #[test]
    fn dropping_streaming_response_releases_concurrency_slot() -> Result<(), Box<dyn Error>> {
        let slots = Arc::new(Semaphore::new(1));
        let mut response = Response::new(Body::from_stream(stream::pending::<
            Result<Bytes, io::Error>,
        >()));
        hold_until_body_ends(&mut response, slots.clone().try_acquire_owned()?);
        assert_eq!(slots.available_permits(), 0);
        drop(response);
        assert_eq!(slots.available_permits(), 1);
        Ok(())
    }
}
