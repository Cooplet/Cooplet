use std::time::Duration;

use axum::{
    body::{Bytes, HttpBody},
    extract::{FromRequest, FromRequestParts, Request},
    response::IntoResponse,
};
use cooplet_client::IDEMPOTENCY_KEY;
use cooplet_protocol::{ErrorCode, validate_idempotency_key};
use http::{
    HeaderMap, HeaderName, StatusCode,
    header::{AUTHORIZATION, CONTENT_TYPE},
    request::Parts,
};
use serde::de::DeserializeOwned;

use crate::{
    auth::Principal,
    error::{NodeError, Result},
    now_ms,
};

use super::{
    NodeState,
    problem::{invalid_request, payload_too_large},
};

pub struct Caller(pub Principal);

impl FromRequestParts<NodeState> for Caller {
    type Rejection = NodeError;

    async fn from_request_parts(parts: &mut Parts, state: &NodeState) -> Result<Self> {
        let unauthorized = || {
            NodeError::new(
                ErrorCode::Unauthorized,
                "A single Bearer credential is required",
            )
        };
        let value = one_header(&parts.headers, AUTHORIZATION)
            .map_err(|_| unauthorized())?
            .ok_or_else(unauthorized)?;
        let (scheme, token) = value.split_once(' ').ok_or_else(unauthorized)?;
        if !scheme.eq_ignore_ascii_case("Bearer") {
            return Err(unauthorized());
        }
        Ok(Self(
            state
                .auth
                .authenticate(&state.reads, token.to_string(), now_ms()?)
                .await?,
        ))
    }
}

pub struct Path<T>(pub T);
pub struct Query<T>(pub T);
pub struct Json<T>(pub T);
pub struct OptionalJson<T>(pub T);

impl<T: DeserializeOwned + Send> FromRequestParts<NodeState> for Path<T> {
    type Rejection = NodeError;

    async fn from_request_parts(parts: &mut Parts, state: &NodeState) -> Result<Self> {
        let axum::extract::Path(value) = axum::extract::Path::from_request_parts(parts, state)
            .await
            .map_err(|_| invalid_request("Invalid route parameter"))?;
        Ok(Self(value))
    }
}

impl<T: DeserializeOwned + Send> FromRequestParts<NodeState> for Query<T> {
    type Rejection = NodeError;

    async fn from_request_parts(parts: &mut Parts, state: &NodeState) -> Result<Self> {
        let axum::extract::Query(value) = axum::extract::Query::from_request_parts(parts, state)
            .await
            .map_err(|_| invalid_request("Invalid query parameters"))?;
        Ok(Self(value))
    }
}

impl<T: DeserializeOwned + Send> FromRequest<NodeState> for Json<T> {
    type Rejection = NodeError;

    async fn from_request(req: Request, state: &NodeState) -> Result<Self> {
        json(req, state)
            .await?
            .ok_or_else(|| invalid_request("Expected a JSON object"))
            .map(Self)
    }
}

impl<T: Default + DeserializeOwned + Send> FromRequest<NodeState> for OptionalJson<T> {
    type Rejection = NodeError;

    async fn from_request(req: Request, state: &NodeState) -> Result<Self> {
        Ok(Self(json(req, state).await?.unwrap_or_default()))
    }
}

async fn json<T: DeserializeOwned>(req: Request, state: &NodeState) -> Result<Option<T>> {
    let media = one_header(req.headers(), CONTENT_TYPE)?.is_some_and(|value| {
        let media = value
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        media == "application/json"
            || (media.starts_with("application/") && media.ends_with("+json"))
    });
    let bytes = buffer(req, state).await?;
    if bytes.is_empty() {
        return Ok(None);
    }
    if !media
        || bytes
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            != Some(b'{')
    {
        return Err(invalid_request("Expected an application/json object"));
    }
    let value = serde_json::from_slice(&bytes)
        .map_err(|_| invalid_request("Invalid JSON request fields"))?;
    Ok(Some(value))
}

const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub struct BodyLimit(pub usize);

pub async fn buffer(request: Request, state: &NodeState) -> Result<Bytes> {
    if let Some(limit) = request.extensions().get::<BodyLimit>()
        && request.body().size_hint().lower() > limit.0 as u64
    {
        return Err(payload_too_large());
    }

    let mut shutdown = state.shutdown.subscribe();
    tokio::select! {
        biased;
        _ = shutdown.wait_for(|shutting_down| *shutting_down) => {
            Err(NodeError::new(ErrorCode::Overloaded, "The Node is shutting down"))
        }
        body = tokio::time::timeout(BODY_READ_TIMEOUT, Bytes::from_request(request, state)) => {
            match body {
                Ok(Ok(bytes)) => Ok(bytes),
                Ok(Err(error)) => Err(if error.into_response().status() == StatusCode::PAYLOAD_TOO_LARGE {
                    payload_too_large()
                } else {
                    invalid_request("Could not read the request body")
                }),
                Err(_) => Err(invalid_request("Timed out reading the request body")
                    .with_extension("reason", "body_read_timeout")),
            }
        }
    }
}

pub fn idempotency_key(headers: &HeaderMap, required: bool) -> Result<Option<&str>> {
    let key = one_header(headers, IDEMPOTENCY_KEY)?;
    match key {
        Some(key) => {
            validate_idempotency_key(key)
                .map_err(|_| invalid_request("Invalid Idempotency-Key"))?;
            Ok(Some(key))
        }
        None if required => Err(invalid_request("Idempotency-Key is required")),
        None => Ok(None),
    }
}

pub fn required_key(headers: &HeaderMap) -> Result<String> {
    idempotency_key(headers, true)?
        .map(str::to_string)
        .ok_or_else(|| invalid_request("Idempotency-Key is required"))
}

pub fn one_header(headers: &HeaderMap, name: HeaderName) -> Result<Option<&str>> {
    let mut values = headers.get_all(name).iter();
    let value = values.next();
    if values.next().is_some() {
        return Err(invalid_request("Repeated HTTP header"));
    }
    value
        .map(|value| {
            value
                .to_str()
                .map_err(|_| invalid_request("Invalid HTTP header"))
        })
        .transpose()
}
