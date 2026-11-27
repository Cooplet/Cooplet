use std::{future::Future, sync::Arc, time::Duration};

use axum::{
    Extension,
    extract::{FromRequestParts, Request, State, ws::WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use cooplet_platform::AdmissionGuard;
use cooplet_protocol::{
    AggregateType, BRIDGE_ERROR_HEADER, BRIDGE_SUBPROTOCOL, ErrorCode, EventKind,
    MAX_WS_FRAME_BYTES,
};
use cooplet_storage::{WriteError, WriteOutcome};
use rusqlite::params;
use tokio::sync::Semaphore;

use crate::{
    AppState,
    auth::{self, AuthenticatedCaller},
    error::HubError,
    http::{BEARER_CHALLENGE, bridge_token},
    storage::events::{EventAudience, NewEvent, append, node_disconnected_data},
};

use super::{registry::SessionKey, session};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

#[utoipa::path(
    get, path = "/v1/bridge/connect", tag = "Bridge",
    description = "Upgrade an enrolled Node to the Cooplet WebSocket bridge with its node bearer credential.",
    params(
        ("Connection" = String, Header, description = "Must include the Upgrade token."),
        ("Upgrade" = String, Header, description = "websocket"),
        ("Sec-WebSocket-Version" = String, Header, description = "13"),
        ("Sec-WebSocket-Key" = String, Header, description = "Base64-encoded 16-byte WebSocket handshake nonce."),
        ("Sec-WebSocket-Protocol" = String, Header, description = "Must offer cooplet.v1."),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade accepted.",
            headers(
                ("Connection" = String, description = "Upgrade"),
                ("Upgrade" = String, description = "websocket"),
                ("Sec-WebSocket-Accept" = String, description = "RFC 6455 handshake proof."),
                ("Sec-WebSocket-Protocol" = String, description = "cooplet.v1"),
            ),
        ),
        (status = 400, description = "Invalid WebSocket handshake or subprotocol."),
        (status = 401, description = "Invalid or revoked node credential.",
            headers(("X-Cooplet-Bridge-Error" = String, description = "node_revoked when the node credential was revoked.")),
        ),
        (status = 409, description = "Missing or mismatched Hub identity pin.",
            headers(("X-Cooplet-Bridge-Error" = String, description = "hub_identity_mismatch")),
        ),
        (status = 503, description = "Bridge handshake or session capacity exceeded."),
    )
)]
pub(crate) async fn connect(
    State(state): State<AppState>,
    Extension(permits): Extension<Arc<Semaphore>>,
    request: Request,
) -> Response {
    match upgrade(state, permits, request).await {
        Ok(response) => response,
        Err(error) => {
            let code = error.problem().code;
            let mut response = error.into_response();
            if matches!(
                code,
                ErrorCode::NodeRevoked | ErrorCode::HubIdentityMismatch
            ) {
                response
                    .headers_mut()
                    .insert(BRIDGE_ERROR_HEADER, HeaderValue::from_static(code.as_str()));
            }
            if response.status() == StatusCode::UNAUTHORIZED {
                response.headers_mut().insert(
                    header::WWW_AUTHENTICATE,
                    HeaderValue::from_static(BEARER_CHALLENGE),
                );
            }
            response
        }
    }
}

async fn upgrade(
    state: AppState,
    permits: Arc<Semaphore>,
    request: Request,
) -> Result<Response, HubError> {
    let handshake = permits
        .try_acquire_owned()
        .map_err(|_| HubError::new(ErrorCode::Overloaded, "Bridge handshake limits are in use."))?;
    let bridge = state.bridge.live().cloned().ok_or_else(|| {
        HubError::new(
            ErrorCode::BridgeUnavailable,
            "The Hub bridge is unavailable.",
        )
    })?;
    let budget = ExpireOnDrop(AdmissionGuard::with_clock(
        HANDSHAKE_TIMEOUT,
        bridge.clock().clone(),
    ));
    let (mut parts, _) = request.into_parts();
    validate_http_upgrade(&parts.headers)?;
    let ws = WebSocketUpgrade::from_request_parts(&mut parts, &state)
        .await
        .map_err(|_| invalid_upgrade())?
        .protocols([BRIDGE_SUBPROTOCOL])
        .max_message_size(MAX_WS_FRAME_BYTES)
        .max_frame_size(MAX_WS_FRAME_BYTES)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_WS_FRAME_BYTES * 2);
    if ws.selected_protocol().is_none() {
        return Err(HubError::invalid(
            "The WebSocket subprotocol must offer cooplet.v1.",
        ));
    }
    let token = bridge_token(&parts.headers, &state.meta.hub_id)?;
    let caller = within(
        &budget.0,
        auth::authenticate_node(&state.reads, &state.master, token),
    )
    .await??;
    let (node_id, generation) = caller
        .node_bridge_identity()
        .ok_or_else(|| HubError::new(ErrorCode::Unauthorized, "A node credential is required."))?;
    let admission = bridge.admit(node_id.clone())?;

    // Own allocation through its commit. If a client disappears while receiving
    // 101, the old session still learns that its durable epoch has been fenced.
    let write_state = state.clone();
    let write_bridge = bridge.clone();
    let deadline = budget.0.clone();
    let key = SessionKey {
        node_id: node_id.clone(),
        generation,
        epoch: 0,
    };
    let allocation = state.background.spawn(async move {
        let _transition = write_bridge.transition(&key.node_id).await;
        if deadline.is_expired() {
            return Err(handshake_expired());
        }
        let epoch = match allocate_epoch(&write_state, deadline, caller, &key).await {
            Ok(epoch) => epoch,
            Err(error) => {
                if error.problem().code == ErrorCode::DeliveryUnknown {
                    write_state.bridge.disconnect_transient(&key.node_id);
                }
                return Err(error);
            }
        };
        admission.claim(epoch);
        Ok::<_, HubError>((SessionKey { epoch, ..key }, admission, handshake))
    })?;
    let (key, admission, handshake) = within(&budget.0, allocation)
        .await?
        .map_err(HubError::internal)??;

    Ok(ws
        .on_failed_upgrade(|error| tracing::debug!(%error, "Bridge upgrade did not complete"))
        .on_upgrade(move |socket| session::run(socket, state, bridge, key, admission, handshake)))
}

struct ExpireOnDrop(AdmissionGuard);

impl Drop for ExpireOnDrop {
    fn drop(&mut self) {
        self.0.expire();
    }
}

async fn within<T>(
    budget: &AdmissionGuard,
    future: impl Future<Output = T>,
) -> Result<T, HubError> {
    let remaining = budget.remaining_ms().ok_or_else(handshake_expired)?;
    let output = tokio::time::timeout(Duration::from_millis(remaining), future)
        .await
        .map_err(|_| handshake_expired())?;
    if budget.is_expired() {
        return Err(handshake_expired());
    }
    Ok(output)
}

fn handshake_expired() -> HubError {
    HubError::new(ErrorCode::Overloaded, "The bridge handshake expired.")
}

fn invalid_upgrade() -> HubError {
    HubError::invalid("A valid WebSocket upgrade is required.")
}

fn validate_http_upgrade(headers: &HeaderMap) -> Result<(), HubError> {
    let has_upgrade_token = headers.get_all(header::CONNECTION).iter().any(|value| {
        value
            .as_bytes()
            .split(|byte| *byte == b',')
            .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"upgrade"))
    });
    let mut keys = headers.get_all(header::SEC_WEBSOCKET_KEY).iter();
    let valid_key = keys.next().is_some_and(|value| {
        // A canonical Base64 encoding of exactly 16 bytes has 22 data
        // characters, two padding characters and two meaningful tail bits.
        matches!(value.as_bytes(), [data @ .., b'=', b'='] if data.len() == 22
            && data.iter().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
            && matches!(data.last(), Some(b'A' | b'Q' | b'g' | b'w')))
    }) && keys.next().is_none();
    if !has_upgrade_token || !valid_key {
        return Err(invalid_upgrade());
    }
    Ok(())
}

async fn allocate_epoch(
    state: &AppState,
    deadline: AdmissionGuard,
    caller: AuthenticatedCaller,
    key: &SessionKey,
) -> Result<u64, HubError> {
    let key = key.clone();
    let hub_id = state.meta.hub_id.clone();
    let shutdown = state.shutdown.clone();
    let wall_clock = state.wall_clock.clone();
    Ok(state
        .db
        .write(move |tx| {
            let now = wall_clock();
            auth::reverify_caller_tx(tx, &caller, now)?;
            let (previous_epoch, status): (u64, String) = tx.query_row(
                "SELECT session_epoch, status FROM nodes WHERE node_id = ?1",
                [key.node_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let epoch = previous_epoch
                .checked_add(1)
                .filter(|value| i64::try_from(*value).is_ok())
                .ok_or_else(|| HubError::internal("node session epoch overflow"))?;
            let count = tx.execute(
                "UPDATE nodes SET session_epoch = ?1, status = 'offline'
                  WHERE node_id = ?2 AND generation = ?3 AND revoked_at_ms IS NULL
                    AND status != 'revoked'",
                params![epoch, key.node_id.as_str(), key.generation],
            )?;
            if count != 1 {
                return Err(HubError::new(ErrorCode::NodeRevoked, "The node was revoked.").into());
            }
            let mut events = Vec::new();
            if status == "online" {
                events.push(append(
                    tx,
                    &hub_id,
                    NewEvent {
                        kind: EventKind::NodeDisconnected,
                        aggregate_type: AggregateType::Node,
                        aggregate_id: key.node_id.as_str(),
                        actor_id: None,
                        created_at_ms: now,
                        data: node_disconnected_data(&key.node_id, "offline", previous_epoch),
                        audience: EventAudience::Nodes {
                            node_ids: vec![key.node_id.clone()],
                        },
                    },
                )?);
            }
            if deadline.is_expired() || shutdown.has_changed().unwrap_or(true) {
                return Err(handshake_expired().into());
            }
            Ok::<_, WriteError<HubError>>(WriteOutcome::with_events(epoch, events))
        })
        .await?)
}
