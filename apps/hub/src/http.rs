use std::sync::Arc;

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use cooplet_protocol::{ErrorCode, HubId};
use tokio::sync::Semaphore;

use crate::{AppState, error::HubError};

mod admission;
mod extract;
pub(crate) mod handlers;
pub(crate) use extract::EmptyJson;
use handlers::{admin_routes::*, command_routes::*, *};

const JSON_BODY_LIMIT: usize = 64 * 1024;
const ENROLLMENT_BODY_LIMIT: usize = 16 * 1024;
const DEFAULT_ENROLLMENT_CONCURRENCY: usize = 16;
pub(crate) const BEARER_CHALLENGE: &str = "Bearer realm=\"cooplet-hub\"";

#[derive(Clone, Copy)]
pub(crate) struct RequestLimits {
    requests: usize,
    event_streams: usize,
    blob_transfers: usize,
    enrollments: usize,
}

impl RequestLimits {
    pub(crate) fn new(
        max_concurrent_requests: usize,
        max_active_event_streams: usize,
        max_concurrent_blob_transfers: usize,
    ) -> Result<Self, HubError> {
        Self::with_enrollments(
            max_concurrent_requests,
            max_active_event_streams,
            max_concurrent_blob_transfers,
            DEFAULT_ENROLLMENT_CONCURRENCY,
        )
    }

    pub(crate) fn with_enrollments(
        max_concurrent_requests: usize,
        max_active_event_streams: usize,
        max_concurrent_blob_transfers: usize,
        max_concurrent_enrollments: usize,
    ) -> Result<Self, HubError> {
        if [
            max_concurrent_requests,
            max_active_event_streams,
            max_concurrent_blob_transfers,
            max_concurrent_enrollments,
        ]
        .into_iter()
        .any(|limit| !(1..=Semaphore::MAX_PERMITS).contains(&limit))
        {
            return Err(HubError::invalid(
                "HTTP concurrency limits must be positive and bounded.",
            ));
        }
        Ok(Self {
            requests: max_concurrent_requests,
            event_streams: max_active_event_streams,
            blob_transfers: max_concurrent_blob_transfers,
            enrollments: max_concurrent_enrollments,
        })
    }

    pub(crate) fn max_connections(self) -> usize {
        self.requests
            .saturating_add(self.event_streams)
            .saturating_add(self.blob_transfers)
            .saturating_add(self.enrollments)
            .saturating_mul(2)
            .clamp(64, 4096)
    }
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            requests: 256,
            event_streams: 64,
            blob_transfers: 32,
            enrollments: DEFAULT_ENROLLMENT_CONCURRENCY,
        }
    }
}

pub(crate) fn router(state: AppState, limits: RequestLimits) -> Router {
    use admission::Access;

    let requests = Arc::new(Semaphore::new(limits.requests));
    let protect = |router: Router<AppState>, access: Access| {
        apply_guard(router, &state, access, requests.clone())
    };

    let public = Router::new()
        .route("/v1/info", get(info))
        .route("/v1/openapi.json", get(openapi));
    let admin = Router::new()
        .route(
            "/v1/principals",
            get(list_principals).post(create_principal),
        )
        .route(
            "/v1/principals/{id}/tokens",
            get(list_tokens).post(issue_token),
        )
        .route(
            "/v1/principals/{id}/tokens/{token_id}/revoke",
            post(revoke_token),
        )
        .route("/v1/principals/{id}/disable", post(disable_principal))
        .route(
            "/v1/enrollment-tokens",
            get(list_enrollment).post(issue_enrollment),
        )
        .route(
            "/v1/enrollment-tokens/{token_id}/revoke",
            post(revoke_enrollment),
        )
        .route("/v1/nodes/{node_id}/revoke", post(revoke_node));
    let protected = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/whoami", get(whoami))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/participants", get(list_participants))
        .route("/v1/messages", post(send_message))
        .route("/v1/messages/{message_id}", get(get_message))
        .route("/v1/agents", post(create_agent))
        .route("/v1/agents/{participant_id}/stop", post(stop_agent))
        .route(
            "/v1/participants/{participant_id}/approval",
            post(respond_approval),
        )
        .route("/v1/workflows/runs", post(run_workflow))
        .route("/v1/workflows/runs/{run_id}/cancel", post(cancel_workflow))
        .route("/v1/events", get(events_page));
    let streams = apply_guard(
        Router::new().route("/v1/events/stream", get(events_stream)),
        &state,
        Access::EventStream,
        Arc::new(Semaphore::new(limits.event_streams)),
    );

    let json = protect(public, Access::Public)
        .merge(protect(admin, Access::Admin))
        .merge(protect(protected, Access::Bearer))
        .merge(streams)
        .layer(DefaultBodyLimit::max(JSON_BODY_LIMIT));

    // Unauthenticated enrollment has its own concurrency pool so slow clients
    // cannot starve authenticated admin or control requests.
    let enrollment = apply_guard(
        Router::new().route("/v1/enrollment/token", post(redeem_enrollment)),
        &state,
        Access::Enrollment,
        Arc::new(Semaphore::new(limits.enrollments)),
    )
    .layer(DefaultBodyLimit::max(ENROLLMENT_BODY_LIMIT));

    let blobs = apply_guard(
        Router::new()
            .route("/v1/blobs", post(upload_blob))
            .route("/v1/blobs/{blob_id}", get(download_blob)),
        &state,
        Access::Blob,
        Arc::new(Semaphore::new(limits.blob_transfers)),
    )
    .layer(DefaultBodyLimit::disable());

    json.merge(enrollment)
        .merge(blobs)
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.meta.hub_id.clone(),
            response_headers,
        ))
        .with_state(state)
}

fn apply_guard(
    router: Router<AppState>,
    state: &AppState,
    access: admission::Access,
    permits: Arc<Semaphore>,
) -> Router<AppState> {
    router.route_layer(middleware::from_fn_with_state(
        admission::Gate::new(state.clone(), access, permits),
        admission::check_request,
    ))
}

async fn not_found() -> HubError {
    HubError::new(ErrorCode::NotFound, "No Hub route matches this path.")
}

async fn method_not_allowed() -> HubError {
    HubError::method_not_allowed()
}

async fn response_headers(State(hub_id): State<HubId>, request: Request, next: Next) -> Response {
    use axum::http::{HeaderValue, header};
    use cooplet_client::X_COOPLET_HUB;

    let mut response = next.run(request).await;
    // Branded Hub IDs contain only ASCII characters allowed in HTTP headers.
    if let Ok(pin) = HeaderValue::from_str(hub_id.as_str()) {
        response.headers_mut().insert(X_COOPLET_HUB, pin);
    }
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, path::PathBuf, time::Duration};

    use cooplet_platform::PrivateFs;
    use cooplet_protocol::{EventId, ProblemDetails};
    use cooplet_storage::DEFAULT_MAX_BLOB_BYTES;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::watch,
        time,
    };

    use crate::{bridge::OfflineBridge, storage};

    use super::*;

    type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    async fn read_status_and_problem(
        mut stream: TcpStream,
    ) -> Result<(u16, Option<ProblemDetails>), Box<dyn Error + Send + Sync>> {
        let mut wire = Vec::new();
        time::timeout(Duration::from_secs(3), stream.read_to_end(&mut wire)).await??;
        let split = wire
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or("missing HTTP header terminator")?;
        let status: u16 = std::str::from_utf8(&wire[..split])?
            .split_whitespace()
            .nth(1)
            .ok_or("missing status code")?
            .parse()?;
        let body = &wire[split + 4..];
        let problem = serde_json::from_slice::<ProblemDetails>(body).ok();
        Ok((status, problem))
    }

    #[tokio::test]
    async fn saturated_enrollment_pool_returns_503_without_starving_authenticated_routes()
    -> TestResult {
        let root = std::env::temp_dir().join(EventId::generate()?.as_str());
        let _scratch = Scratch(PrivateFs::ensure_private_dir(&root)?);
        let init = storage::init(&root, "Isolation Hub").await?;
        let token =
            PrivateFs::read_private_file(&init.token_path.ok_or("missing admin token")?, 512)?;
        let bearer = std::str::from_utf8(&token)?.trim().to_owned();
        let hub = storage::open(&root, DEFAULT_MAX_BLOB_BYTES, 10 * 1024 * 1024, 16_384).await?;
        let (stop, stop_rx) = watch::channel(());
        let state = AppState::new(
            &hub.engine,
            hub.meta,
            hub.master,
            Arc::new(OfflineBridge),
            stop_rx,
            DEFAULT_MAX_BLOB_BYTES,
        )?;
        let hub_id = state.meta.hub_id.clone();
        let limits = RequestLimits::with_enrollments(4, 2, 2, 1)?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(axum::serve(listener, router(state, limits)).into_future());

        // Hold the sole enrollment slot open by sending headers without finishing the body.
        let mut slow_enroll = TcpStream::connect(addr).await?;
        let stalled_headers = format!(
            "POST /v1/enrollment/token HTTP/1.1\r\nHost: {addr}\r\nX-Cooplet-Hub: {hub_id}\r\nContent-Type: application/json\r\nContent-Length: 64\r\nConnection: close\r\n\r\n{{\"token\":\""
        );
        slow_enroll.write_all(stalled_headers.as_bytes()).await?;
        time::sleep(Duration::from_millis(50)).await;

        let mut second_enroll = TcpStream::connect(addr).await?;
        let enroll_body = r#"{"token":"invalid"}"#;
        let second_req = format!(
            "POST /v1/enrollment/token HTTP/1.1\r\nHost: {addr}\r\nX-Cooplet-Hub: {hub_id}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{enroll_body}",
            enroll_body.len()
        );
        second_enroll.write_all(second_req.as_bytes()).await?;
        let (status, problem) = read_status_and_problem(second_enroll).await?;
        assert_eq!(status, 503);
        assert_eq!(
            problem.ok_or("missing problem")?.code,
            ErrorCode::Overloaded
        );

        // Authenticated and public routes remain completely unblocked.
        let mut health = TcpStream::connect(addr).await?;
        let health_req = format!(
            "GET /v1/health HTTP/1.1\r\nHost: {addr}\r\nX-Cooplet-Hub: {hub_id}\r\nAuthorization: Bearer {bearer}\r\nConnection: close\r\n\r\n"
        );
        health.write_all(health_req.as_bytes()).await?;
        let (health_status, _) = read_status_and_problem(health).await?;
        assert_eq!(health_status, 200);

        drop(slow_enroll);
        server.abort();
        let _ = stop.send(());
        hub.engine.shutdown().await?;
        Ok(())
    }
}
