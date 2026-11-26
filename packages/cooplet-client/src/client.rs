use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;
use futures_core::Stream;
use futures_util::future::{BoxFuture, FutureExt, WeakShared};

use cooplet_protocol::{
    HealthResponse, HubId, InfoResponse, WhoamiResponse, validate_idempotency_key,
};
use http::{Method, header::AUTHORIZATION};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use reqwest::{RequestBuilder, Response};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;
use url::{Host, Url};

use crate::{
    BearerToken, ClientError, IDEMPOTENCY_KEY, X_COOPLET_HUB,
    error::{parse_failure, read_bounded},
};

pub(crate) type ResponseChunks = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

const DEFAULT_JSON_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_NDJSON_LINE_BYTES: usize = 1024 * 1024;
const DEFAULT_BLOB_BYTES: u64 = 50 * 1024 * 1024;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub enum Destination {
    LocalNode,
    Hub { expected_hub_id: HubId },
}

#[derive(Clone)]
pub struct ClientConfig {
    pub base_url: Url,
    pub destination: Destination,
    pub bearer_token: Option<BearerToken>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_json_response_bytes: usize,
    pub max_ndjson_line_bytes: usize,
    pub max_blob_bytes: u64,
}

impl ClientConfig {
    pub fn new(base_url: Url, destination: Destination, bearer_token: Option<BearerToken>) -> Self {
        Self {
            base_url,
            destination,
            bearer_token,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_json_response_bytes: DEFAULT_JSON_BYTES,
            max_ndjson_line_bytes: DEFAULT_NDJSON_LINE_BYTES,
            max_blob_bytes: DEFAULT_BLOB_BYTES,
        }
    }
}

// The callers own the in-flight GET. Cancelling one caller leaves other
// waiters on the same request, and cancelling them all drops that request.
enum HubPin {
    Unverified(Option<WeakShared<BoxFuture<'static, Result<(), ClientError>>>>),
    Verified,
}

#[derive(Clone)]
pub struct CoopletClient {
    http: reqwest::Client,
    pub(crate) config: Arc<ClientConfig>,
    hub_pin: Arc<Mutex<HubPin>>,
}

impl CoopletClient {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        validate_origin(
            &config.base_url,
            matches!(config.destination, Destination::LocalNode),
        )?;
        if config.connect_timeout.is_zero()
            || config.request_timeout.is_zero()
            || config.max_json_response_bytes == 0
            || config.max_ndjson_line_bytes == 0
            || config.max_ndjson_line_bytes == usize::MAX
            || config.max_blob_bytes == 0
        {
            return Err(ClientError::invalid("invalid timeout or size limit"));
        }
        let http = build_http(config.connect_timeout)?;
        Ok(Self {
            http,
            config: Arc::new(config),
            hub_pin: Arc::new(Mutex::new(HubPin::Unverified(None))),
        })
    }

    /// Query a prospective destination without credentials. Pinning the
    /// returned Hub ID is a separate caller decision.
    pub async fn discover_info(base_url: &Url) -> Result<InfoResponse, ClientError> {
        validate_origin(base_url, false)?;
        let http = build_http(DEFAULT_CONNECT_TIMEOUT)?;
        let url = make_url(base_url, "/v1/info", &[])?;
        timeout(DEFAULT_REQUEST_TIMEOUT, "public info", async {
            let response = http.get(url).send().await?;
            decode_json(response, DEFAULT_JSON_BYTES, None, false).await
        })
        .await
    }

    pub async fn verify_hub_identity(&self) -> Result<(), ClientError> {
        let Some(expected) = self.expected_hub() else {
            return Ok(());
        };
        let mut pin = self.hub_pin.lock().await;
        let HubPin::Unverified(pending) = &mut *pin else {
            return Ok(());
        };
        let attempt = if let Some(attempt) = pending.as_ref().and_then(WeakShared::upgrade) {
            attempt
        } else {
            let client = self.clone();
            let expected = expected.clone();
            let attempt = async move {
                let result = client.check_hub_identity(&expected).await;
                *client.hub_pin.lock().await = if result.is_ok() {
                    HubPin::Verified
                } else {
                    HubPin::Unverified(None)
                };
                result
            }
            .boxed()
            .shared();
            *pending = attempt.downgrade();
            attempt
        };
        drop(pin);
        attempt.await
    }

    async fn check_hub_identity(&self, expected: &HubId) -> Result<(), ClientError> {
        let actual = match self.info().await? {
            InfoResponse::Hub(info) if &info.hub_id == expected => return Ok(()),
            InfoResponse::Hub(info) => Some(info.hub_id.to_string()),
            InfoResponse::Node(_) => None,
        };
        Err(ClientError::HubIdentityMismatch {
            expected: expected.clone(),
            actual,
        })
    }

    pub async fn info(&self) -> Result<InfoResponse, ClientError> {
        let url = self.url("/v1/info", &[])?;
        self.execute_json(self.http.get(url), false, false).await
    }

    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.get_json("/v1/health", &[]).await
    }

    pub async fn whoami(&self) -> Result<WhoamiResponse, ClientError> {
        self.get_json("/v1/whoami", &[]).await
    }

    pub async fn get_json<Resp: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Resp, ClientError> {
        let url = self.url(path, query)?;
        let request = self.protected(Method::GET, url, None).await?;
        self.execute_json(request, true, false).await
    }

    pub async fn post_json<Req: Serialize + ?Sized, Resp: DeserializeOwned>(
        &self,
        path: &str,
        body: &Req,
        idempotency_key: Option<&str>,
    ) -> Result<Resp, ClientError> {
        self.mutate_json(Method::POST, path, body, idempotency_key)
            .await
    }

    pub async fn put_json<Req: Serialize + ?Sized, Resp: DeserializeOwned>(
        &self,
        path: &str,
        body: &Req,
        idempotency_key: Option<&str>,
    ) -> Result<Resp, ClientError> {
        self.mutate_json(Method::PUT, path, body, idempotency_key)
            .await
    }

    pub async fn delete_json<Resp: DeserializeOwned>(
        &self,
        path: &str,
        idempotency_key: Option<&str>,
    ) -> Result<Resp, ClientError> {
        let url = self.url(path, &[])?;
        let request = self.protected(Method::DELETE, url, idempotency_key).await?;
        self.execute_json(request, true, false).await
    }

    async fn mutate_json<Req: Serialize + ?Sized, Resp: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: &Req,
        idempotency_key: Option<&str>,
    ) -> Result<Resp, ClientError> {
        let url = self.url(path, &[])?;
        let request = self.protected(method, url, idempotency_key).await?;
        let body = serde_json::to_vec(body)
            .map_err(|_| ClientError::invalid("could not serialize JSON body"))?;
        self.execute_json(
            request
                .header(http::header::CONTENT_TYPE, "application/json")
                .body(body),
            true,
            false,
        )
        .await
    }

    pub(crate) fn url(&self, path: &str, query: &[(&str, String)]) -> Result<Url, ClientError> {
        make_url(&self.config.base_url, path, query)
    }

    pub(crate) fn expected_hub(&self) -> Option<&HubId> {
        match &self.config.destination {
            Destination::LocalNode => None,
            Destination::Hub { expected_hub_id } => Some(expected_hub_id),
        }
    }

    pub(crate) async fn protected(
        &self,
        method: Method,
        url: Url,
        idempotency_key: Option<&str>,
    ) -> Result<RequestBuilder, ClientError> {
        if let Some(key) = idempotency_key {
            validate_idempotency_key(key)
                .map_err(|_| ClientError::invalid("invalid idempotency key"))?;
        }
        self.verify_hub_identity().await?;
        let mut request = self.http.request(method, url);
        if let Some(hub_id) = self.expected_hub() {
            request = request.header(X_COOPLET_HUB, hub_id.as_str());
        }
        if let Some(token) = &self.config.bearer_token {
            request = request.header(AUTHORIZATION, token.header());
        }
        if let Some(key) = idempotency_key {
            request = request.header(IDEMPOTENCY_KEY, key);
        }
        Ok(request)
    }

    pub(crate) async fn execute_json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        protected: bool,
        events: bool,
    ) -> Result<T, ClientError> {
        let hub = if protected { self.expected_hub() } else { None };
        timeout(self.config.request_timeout, "JSON response", async {
            let response = request.send().await?;
            decode_json(response, self.config.max_json_response_bytes, hub, events).await
        })
        .await
    }

    pub(crate) async fn failure(&self, response: Response, events: bool) -> ClientError {
        match tokio::time::timeout(
            self.config.request_timeout,
            parse_failure(response, self.expected_hub(), events),
        )
        .await
        {
            Ok(error) => error,
            Err(_) => ClientError::Timeout("error response"),
        }
    }
}

async fn decode_json<T: DeserializeOwned>(
    response: Response,
    limit: usize,
    hub: Option<&HubId>,
    events: bool,
) -> Result<T, ClientError> {
    if !response.status().is_success() {
        return Err(parse_failure(response, hub, events).await);
    }
    let body = read_bounded(response, limit).await?;
    serde_json::from_slice(&body).map_err(|_| ClientError::protocol("invalid JSON response"))
}

fn build_http(connect_timeout: Duration) -> Result<reqwest::Client, ClientError> {
    Ok(reqwest::Client::builder()
        .use_rustls_tls()
        .connect_timeout(connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()?)
}

fn validate_origin(base: &Url, local_node: bool) -> Result<(), ClientError> {
    if base.host().is_none()
        || !matches!(base.path(), "" | "/")
        || base.query().is_some()
        || base.fragment().is_some()
        || !base.username().is_empty()
        || base.password().is_some()
    {
        return Err(ClientError::invalid("base URL must be a bare HTTP origin"));
    }
    let loopback = match base.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if (local_node && !loopback)
        || !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
    {
        return Err(ClientError::invalid(
            "Local Node requires loopback and remote Hub requires HTTPS",
        ));
    }
    Ok(())
}

/// Build an API route from individual path segments.
pub fn api_path(segments: &[&str]) -> Result<String, ClientError> {
    const PATH_ESCAPE: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    let mut path = String::from("/v1");
    for segment in segments {
        path.push('/');
        path.push_str(&utf8_percent_encode(segment, PATH_ESCAPE).to_string());
    }
    validate_path(&path)?;
    Ok(path)
}

fn make_url(base: &Url, path: &str, query: &[(&str, String)]) -> Result<Url, ClientError> {
    validate_path(path)?;
    let mut url = base
        .join(path)
        .map_err(|_| ClientError::invalid("invalid API route"))?;
    if url.origin() != base.origin() || url.path() != path {
        return Err(ClientError::invalid(
            "API route changes origin or is not normalized",
        ));
    }
    if !query.is_empty() {
        url.query_pairs_mut()
            .extend_pairs(query.iter().map(|(key, value)| (*key, value.as_str())));
    }
    Ok(url)
}

fn validate_path(path: &str) -> Result<(), ClientError> {
    let invalid = || ClientError::invalid("route must be a normalized path inside /v1/");
    let suffix = path.strip_prefix("/v1/").ok_or_else(invalid)?;
    for segment in suffix.split('/') {
        if segment.is_empty()
            || segment
                .bytes()
                .any(|b| !b.is_ascii_graphic() || b"?#\\".contains(&b))
        {
            return Err(invalid());
        }
        for (index, byte) in segment.bytes().enumerate() {
            if byte == b'%'
                && !segment
                    .as_bytes()
                    .get(index + 1..index + 3)
                    .is_some_and(|hex| hex.iter().all(u8::is_ascii_hexdigit))
            {
                return Err(invalid());
            }
        }
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| invalid())?;
        if matches!(decoded.as_ref(), "." | "..")
            || decoded.contains(['/', '\\', '%'])
            || decoded.chars().any(char::is_control)
        {
            return Err(invalid());
        }
    }
    Ok(())
}

pub(crate) async fn timeout<T>(
    duration: Duration,
    activity: &'static str,
    operation: impl Future<Output = Result<T, ClientError>>,
) -> Result<T, ClientError> {
    tokio::time::timeout(duration, operation)
        .await
        .map_err(|_| ClientError::Timeout(activity))?
}
