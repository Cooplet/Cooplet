use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use cooplet_client::{
    BearerToken, ClientConfig, ClientError, CoopletClient, Destination, validate_hub_origin,
};
use cooplet_platform::{AdmissionGuard, PlatformError, PrivateFs};
use cooplet_protocol::{
    BRIDGE_PROTOCOL_VERSION, ErrorCode, HubId, InfoResponse, NodeId, validate_display_name,
};
use cooplet_storage::{DbActor, ReadPool, WriteOutcome};
use http::StatusCode;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use url::Url;
use utoipa::ToSchema;

use crate::{
    config::CREDS_DIR,
    error::{NodeError, Result},
    now_ms,
};

use super::{NodeBridge, offline, remaining};

pub(crate) const HUB_FILE: &str = "hub.json";
const MAX_BINDING_BYTES: usize = 64 * 1024;
const MAX_CA_BYTES: usize = 60 * 1024;
const ENROLL_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct EnrollRequest {
    pub hub_url: String,
    pub hub_id: HubId,
    #[schema(write_only)]
    pub token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_cert_pem: Option<String>,
}

#[derive(Clone, Copy, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnrollmentStatus {
    Enrolled,
    AlreadyEnrolled,
}

#[derive(Deserialize, Serialize, ToSchema)]
pub struct EnrollResponse {
    pub hub_id: HubId,
    pub node_id: NodeId,
    pub local_node_id: NodeId,
    pub generation: u64,
    pub status: EnrollmentStatus,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    local_node_id: NodeId,
    hub_id: HubId,
    node_id: NodeId,
    generation: u64,
    hub_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ca_cert_pem: Option<String>,
    node_token: String,
    enrolled_at_ms: i64,
}

#[derive(Serialize)]
struct RedeemRequest<'a> {
    token: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RedeemResponse {
    hub_id: HubId,
    node_id: NodeId,
    generation: u64,
    node_token: String,
}

pub struct HubConnection {
    credentials: Credentials,
    url: Url,
    trust: Vec<Vec<u8>>,
    client: CoopletClient,
}

impl HubConnection {
    fn new(credentials: Credentials, local_id: &NodeId) -> Result<Self> {
        if &credentials.local_node_id != local_id
            || credentials.generation == 0
            || credentials.enrolled_at_ms <= 0
            || token_node_id(&credentials.node_token, "cplt_node_").as_ref()
                != Some(&credentials.node_id)
        {
            return Err(binding_fault());
        }
        let url = origin(&credentials.hub_url)?;
        if url.as_str() != credentials.hub_url {
            return Err(binding_fault());
        }
        let trust = root_certificates(credentials.ca_cert_pem.as_deref())?;
        let token = BearerToken::new(&credentials.node_token).map_err(|_| binding_fault())?;
        let mut config = ClientConfig::new(
            url.clone(),
            Destination::Hub {
                expected_hub_id: credentials.hub_id.clone(),
            },
            Some(token),
        );
        config.root_certificates_pem.clone_from(&trust);
        // Relays run within the admission budget of their enclosing delivery.
        config.request_timeout = Duration::from_secs(80);
        let client = CoopletClient::new(config).map_err(|_| binding_fault())?;
        Ok(Self {
            credentials,
            url,
            trust,
            client,
        })
    }

    pub fn hub_id(&self) -> &HubId {
        &self.credentials.hub_id
    }

    pub fn node_id(&self) -> &NodeId {
        &self.credentials.node_id
    }

    pub fn client(&self) -> &CoopletClient {
        &self.client
    }

    pub(super) fn node_token(&self) -> &str {
        &self.credentials.node_token
    }

    pub(super) fn url(&self) -> &Url {
        &self.url
    }

    pub(super) fn trust(&self) -> &[Vec<u8>] {
        &self.trust
    }

    fn response(&self, status: EnrollmentStatus) -> EnrollResponse {
        EnrollResponse {
            hub_id: self.hub_id().clone(),
            node_id: self.node_id().clone(),
            local_node_id: self.credentials.local_node_id.clone(),
            generation: self.credentials.generation,
            status,
        }
    }
}

#[derive(Clone)]
pub(super) enum BindingState {
    Unbound,
    Fault,
    Bound(Arc<HubConnection>),
}

pub(super) struct BindingContext {
    pub(super) local_id: NodeId,
    state_dir: PathBuf,
    db: DbActor,
    reads: ReadPool,
}

impl BindingContext {
    pub(super) fn new(local_id: NodeId, state_dir: PathBuf, db: DbActor, reads: ReadPool) -> Self {
        Self {
            local_id,
            state_dir,
            db,
            reads,
        }
    }

    fn file(&self) -> PathBuf {
        self.state_dir.join(CREDS_DIR).join(HUB_FILE)
    }

    pub(super) async fn load(&self) -> BindingState {
        match self.read().await {
            Ok(Some(hub)) => {
                if self.project(&hub).await.is_err() {
                    tracing::warn!(
                        "Could not reconcile the saved Hub binding with the Node database"
                    );
                }
                BindingState::Bound(Arc::new(hub))
            }
            Ok(None) => BindingState::Unbound,
            Err(error) => {
                tracing::warn!(code = %error.code(), "The saved Hub binding is invalid");
                BindingState::Fault
            }
        }
    }

    async fn read(&self) -> Result<Option<HubConnection>> {
        let file = self.file();
        let data = tokio::task::spawn_blocking(move || read_credentials(&file))
            .await
            .map_err(|_| binding_fault())??;
        if let Some(credentials) = data {
            return HubConnection::new(credentials, &self.local_id).map(Some);
        }
        let has_projection = self
            .reads
            .read(|conn| {
                conn.query_row(
                    "SELECT hub_url IS NOT NULL OR hub_fingerprint IS NOT NULL OR
                            hub_enrolled_at_ms IS NOT NULL FROM node_meta WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .await?;
        if has_projection {
            return Err(binding_fault());
        }
        Ok(None)
    }

    pub(super) async fn event_head(&self) -> Result<u64> {
        Ok(self
            .reads
            .read(|conn| {
                conn.query_row(
                    "SELECT events_head_seq FROM node_meta WHERE id = 1",
                    [],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .await?)
    }

    async fn project(&self, hub: &HubConnection) -> Result<()> {
        let url = hub.url().to_string();
        let fingerprint = hub.hub_id().to_string();
        let enrolled_at = hub.credentials.enrolled_at_ms;
        self.db
            .write_sqlite(move |tx| {
                tx.execute(
                    "UPDATE node_meta SET hub_url = ?1, hub_fingerprint = ?2,
                         hub_enrolled_at_ms = ?3, updated_at_ms = MAX(updated_at_ms, ?3)
                     WHERE id = 1 AND (hub_url IS NOT ?1 OR hub_fingerprint IS NOT ?2 OR
                                      hub_enrolled_at_ms IS NOT ?3)",
                    params![url, fingerprint, enrolled_at],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        Ok(())
    }

    async fn persist(&self, hub: &HubConnection) -> Result<()> {
        let data = serde_json::to_vec(&hub.credentials).map_err(|_| binding_uncertain())?;
        if data.len() > MAX_BINDING_BYTES {
            return Err(binding_uncertain());
        }
        let dir = self.state_dir.join(CREDS_DIR);
        tokio::task::spawn_blocking(move || {
            PrivateFs::write_private_file_atomic(&dir, HUB_FILE, &data)
        })
        .await
        .map_err(|_| binding_uncertain())?
        .map_err(|_| binding_uncertain())
    }
}

impl NodeBridge {
    pub async fn enroll(
        self: &Arc<Self>,
        tasks: &Arc<crate::http::Tasks>,
        request: EnrollRequest,
    ) -> Result<(StatusCode, EnrollResponse)> {
        let bridge = self.clone();
        tasks
            .run(async move { bridge.enroll_once(request).await })
            .await
    }

    async fn enroll_once(&self, request: EnrollRequest) -> Result<(StatusCode, EnrollResponse)> {
        let _exclusive = self.enrollment.lock().await;
        if *self.shutdown.borrow() {
            return Err(offline());
        }
        let context = self.context.as_ref().ok_or_else(offline)?;
        let url = origin(&request.hub_url)?;

        let current = self.binding.borrow().clone();
        match current {
            BindingState::Bound(hub) => {
                if hub.hub_id() != &request.hub_id || hub.url() != &url {
                    return Err(NodeError::new(
                        ErrorCode::OperationConflict,
                        "This Node is already enrolled with a different Hub origin or identity",
                    ));
                }
                if context.project(&hub).await.is_err() {
                    tracing::warn!(
                        "Could not reconcile the saved Hub binding with the Node database"
                    );
                }
                return Ok((
                    StatusCode::OK,
                    hub.response(EnrollmentStatus::AlreadyEnrolled),
                ));
            }
            BindingState::Fault => return Err(binding_fault()),
            BindingState::Unbound => {}
        }
        if let Some(name) = &request.name {
            validate_display_name(name).map_err(|_| {
                NodeError::new(ErrorCode::InvalidRequest, "Invalid Node display name")
            })?;
        }
        let reserved_node = token_node_id(&request.token, "cplt_enr_").ok_or_else(|| {
            NodeError::new(ErrorCode::Unauthorized, "Invalid enrollment credential")
        })?;
        let trust = root_certificates(request.ca_cert_pem.as_deref())?;
        let enrolled_at_ms = now_ms()?;
        let deadline = AdmissionGuard::with_clock(ENROLL_TIMEOUT, self.clock.clone());
        let info =
            CoopletClient::discover_info_with_timeout(&url, &trust, enrollment_budget(&deadline)?)
                .await
                .map_err(enrollment_discovery_error)?;
        verify_info(info, &request.hub_id)?;
        let response: RedeemResponse = CoopletClient::enroll(
            &url,
            &request.hub_id,
            &trust,
            enrollment_budget(&deadline)?,
            &RedeemRequest {
                token: &request.token,
                name: request.name.as_deref(),
            },
        )
        .await
        .map_err(redeem_error)?;
        if response.hub_id != request.hub_id
            || response.node_id != reserved_node
            || response.generation == 0
        {
            return Err(binding_uncertain());
        }
        let credentials = Credentials {
            local_node_id: context.local_id.clone(),
            hub_id: response.hub_id,
            node_id: response.node_id,
            generation: response.generation,
            hub_url: url.to_string(),
            ca_cert_pem: request.ca_cert_pem,
            node_token: response.node_token,
            enrolled_at_ms,
        };
        let hub = Arc::new(
            HubConnection::new(credentials, &context.local_id).map_err(|_| binding_uncertain())?,
        );
        if let Err(error) = context.persist(&hub).await {
            // Publication can have succeeded before directory durability became
            // unknown. A fresh startup can check the durable file in that case.
            self.binding.send_replace(BindingState::Fault);
            return Err(error);
        }
        if context.project(&hub).await.is_err() {
            tracing::warn!("Could not project the enrollment to the Node database");
        }
        self.binding.send_replace(BindingState::Bound(hub.clone()));
        Ok((
            StatusCode::CREATED,
            hub.response(EnrollmentStatus::Enrolled),
        ))
    }
}

fn read_credentials(file: &Path) -> Result<Option<Credentials>> {
    match PrivateFs::read_private_file(file, MAX_BINDING_BYTES as u64) {
        Ok(data) => serde_json::from_slice(&data)
            .map(Some)
            .map_err(|_| binding_fault()),
        Err(PlatformError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(binding_fault()),
    }
}

fn origin(raw: &str) -> Result<Url> {
    let invalid = || NodeError::new(ErrorCode::InvalidRequest, "Invalid Hub origin");
    if raw.len() > 2048 {
        return Err(invalid());
    }
    let url = Url::parse(raw).map_err(|_| invalid())?;
    validate_hub_origin(&url).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "https" | "http") {
        return Err(invalid());
    }
    Ok(url)
}

fn root_certificates(pem: Option<&str>) -> Result<Vec<Vec<u8>>> {
    if pem.is_some_and(|pem| {
        pem.is_empty()
            || pem.len() > MAX_CA_BYTES
            || serde_json::to_vec(pem).map_or(true, |encoded| encoded.len() > MAX_CA_BYTES)
    }) {
        return Err(NodeError::new(
            ErrorCode::InvalidRequest,
            "Invalid Hub CA certificate bundle",
        ));
    }
    Ok(pem.into_iter().map(|pem| pem.as_bytes().to_vec()).collect())
}

fn token_node_id(token: &str, prefix: &str) -> Option<NodeId> {
    let (id, secret) = token.strip_prefix(prefix)?.split_once('.')?;
    if id.len() != 26
        || secret.len() != 64
        || !secret
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    NodeId::parse(&format!("node_{id}")).ok()
}

fn verify_info(info: InfoResponse, pin: &HubId) -> Result<()> {
    match info {
        InfoResponse::Hub(info) if &info.hub_id == pin => {
            if info.protocol_version == BRIDGE_PROTOCOL_VERSION {
                Ok(())
            } else {
                Err(NodeError::new(
                    ErrorCode::BridgeUnavailable,
                    "Unsupported Hub protocol version",
                ))
            }
        }
        _ => Err(NodeError::new(
            ErrorCode::HubIdentityMismatch,
            "The server does not match the expected Hub identity",
        )),
    }
}

fn enrollment_budget(deadline: &AdmissionGuard) -> Result<Duration> {
    remaining(deadline).ok_or_else(|| {
        NodeError::new(
            ErrorCode::BridgeUnavailable,
            "Hub enrollment timed out before redeeming the credential",
        )
    })
}

fn enrollment_discovery_error(error: ClientError) -> NodeError {
    match error {
        ClientError::InvalidInput(_) => NodeError::new(
            ErrorCode::InvalidRequest,
            "Invalid Hub origin or CA certificate bundle",
        ),
        _ => NodeError::new(
            ErrorCode::BridgeUnavailable,
            "Could not verify the Hub before enrollment",
        ),
    }
}

fn redeem_error(error: ClientError) -> NodeError {
    match error {
        ClientError::Problem(problem) if problem.status < 500 => {
            NodeError::new(problem.code, problem.code.title())
        }
        ClientError::HubIdentityMismatch { .. } => NodeError::new(
            ErrorCode::HubIdentityMismatch,
            "The server no longer matches the expected Hub identity",
        ),
        _ => binding_uncertain(),
    }
}

fn binding_uncertain() -> NodeError {
    NodeError::new(
        ErrorCode::DeliveryUnknown,
        "Hub enrollment may have consumed the credential without saving a usable local binding",
    )
    .with_extension("reason", "enrollment_outcome_unknown")
}

pub(super) fn binding_fault() -> NodeError {
    NodeError::new(
        ErrorCode::OperationConflict,
        "The saved Hub binding is invalid",
    )
    .with_extension("reason", "invalid_hub_binding")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_ids_and_hex_are_validated() -> std::result::Result<(), cooplet_protocol::IdError>
    {
        let expected = NodeId::generate()?;
        let ulid = expected.as_str().strip_prefix("node_").unwrap_or_default();
        let secret = "0123456789abcdef".repeat(4);
        let enrollment = format!("cplt_enr_{ulid}.{secret}");
        assert_eq!(
            token_node_id(&enrollment, "cplt_enr_").as_ref(),
            Some(&expected)
        );
        assert_eq!(token_node_id(&enrollment, "cplt_node_"), None);
        let upper_hex = format!("cplt_enr_{ulid}.{}", secret.to_ascii_uppercase());
        assert_eq!(token_node_id(&upper_hex, "cplt_enr_"), None);
        assert_eq!(
            token_node_id(&format!("cplt_enr_{ulid}.00"), "cplt_enr_"),
            None
        );
        Ok(())
    }

    #[test]
    fn origin_comparison_and_ca_size_precede_enrollment() -> Result<()> {
        assert_eq!(origin("http://127.0.0.1")?, origin("http://127.0.0.1/")?);
        assert!(origin("http://example.test").is_err());
        assert!(origin("https://example.test/path").is_err());
        assert!(origin("https://name@example.test/").is_err());
        assert!(root_certificates(Some(&"\n".repeat(MAX_CA_BYTES))).is_err());
        Ok(())
    }
}
