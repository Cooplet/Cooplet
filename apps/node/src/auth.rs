use std::{
    io,
    net::{Ipv4Addr, SocketAddrV4},
    path::Path,
};

use cooplet_client::{BearerToken, ClientConfig, CoopletClient, Destination};
use cooplet_platform::{PlatformError, PrivateFs, random_bytes};
use cooplet_protocol::{
    AuthorKind, ErrorCode, InfoResponse, NodeId, ParticipantId, ParticipantKind, PrincipalKind,
    ServiceScope, TokenId, WhoamiResponse,
};
use cooplet_storage::ReadPool;
use hmac::{Hmac, Mac};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{NodeError, Result};

pub const MASTER_FILE: &str = "node.master.key";
pub const OPERATOR_FILE: &str = "operator.json";
const MAX_CREDENTIAL_BYTES: u64 = 4096;
const DOMAIN: &[u8] = b"cooplet.node.v0";
const PRINCIPALS: [PrincipalKind; 4] = [
    PrincipalKind::Participant,
    PrincipalKind::Operator,
    PrincipalKind::Adapter,
    PrincipalKind::WorkflowRun,
];

#[derive(Clone)]
pub struct Auth {
    key: [u8; 32],
    node_id: NodeId,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub node_id: NodeId,
    pub participant_id: ParticipantId,
    endpoint: String,
    token: String,
}

impl Credential {
    pub fn new(
        node_id: NodeId,
        participant_id: ParticipantId,
        address: SocketAddrV4,
    ) -> io::Result<Self> {
        let mut secret = [0u8; 32];
        random_bytes(&mut secret).map_err(io::Error::other)?;
        Ok(Self {
            node_id,
            participant_id,
            endpoint: format!("http://{address}"),
            token: hex(&secret),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn address(&self) -> io::Result<SocketAddrV4> {
        let address: SocketAddrV4 = self
            .endpoint
            .strip_prefix("http://")
            .and_then(|address| address.parse().ok())
            .filter(|address: &SocketAddrV4| {
                *address.ip() == Ipv4Addr::LOCALHOST && address.port() != 0
            })
            .ok_or_else(invalid_credential)?;
        if self.endpoint != format!("http://{address}") {
            return Err(invalid_credential());
        }
        Ok(address)
    }

    pub fn read(path: &Path) -> io::Result<Self> {
        let bytes =
            PrivateFs::read_private_file(path, MAX_CREDENTIAL_BYTES).map_err(platform_io)?;
        if bytes
            .iter()
            .copied()
            .find(|byte| !byte.is_ascii_whitespace())
            != Some(b'{')
        {
            return Err(invalid_credential());
        }
        let credential: Self = serde_json::from_slice(&bytes).map_err(|_| invalid_credential())?;
        credential.address()?;
        if !valid_token(&credential.token) {
            return Err(invalid_credential());
        }
        Ok(credential)
    }

    pub fn publish(&self, dir: &Path, name: &str) -> io::Result<()> {
        let bytes = serde_json::to_vec(self).map_err(io::Error::other)?;
        PrivateFs::write_private_file_atomic(dir, name, &bytes).map_err(platform_io)
    }

    pub async fn connect(
        &self,
        expected: PrincipalKind,
    ) -> Result<(CoopletClient, WhoamiResponse)> {
        self.address().map_err(NodeError::internal)?;
        let origin = self.endpoint.parse().map_err(NodeError::internal)?;
        let info = CoopletClient::discover_info(&origin)
            .await
            .map_err(client_error)?;
        if !matches!(info, InfoResponse::Node(info) if info.node_id == self.node_id && info.protocol_version == 1)
        {
            return Err(NodeError::new(
                ErrorCode::Unauthorized,
                "The endpoint does not identify the expected Node",
            ));
        }
        let token = BearerToken::new(&self.token).map_err(client_error)?;
        let client = CoopletClient::new(ClientConfig::new(
            origin,
            Destination::LocalNode,
            Some(token),
        ))
        .map_err(client_error)?;
        let identity = client.whoami().await.map_err(client_error)?;
        if identity.node_id.as_ref() != Some(&self.node_id)
            || identity.participant_id.as_ref() != Some(&self.participant_id)
            || identity.principal_kind != expected
        {
            return Err(NodeError::new(
                ErrorCode::Unauthorized,
                "The credential does not identify the expected principal",
            ));
        }
        Ok((client, identity))
    }
}

fn client_error(error: cooplet_client::ClientError) -> NodeError {
    NodeError::new(
        ErrorCode::Unauthorized,
        format!("Could not verify the local Node credential: {error}"),
    )
}

pub fn read_master(dir: &Path) -> io::Result<[u8; 32]> {
    PrivateFs::read_private_file(&dir.join(MASTER_FILE), 32)
        .map_err(platform_io)?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid Node master key length"))
}

pub fn publish_master(dir: &Path) -> io::Result<[u8; 32]> {
    let mut key = [0; 32];
    random_bytes(&mut key).map_err(io::Error::other)?;
    PrivateFs::write_private_file_atomic(dir, MASTER_FILE, &key).map_err(platform_io)?;
    Ok(key)
}

pub fn platform_io(error: PlatformError) -> io::Error {
    match error {
        PlatformError::Io(error) => error,
        other => io::Error::other(other),
    }
}

fn invalid_credential() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid Node credential descriptor",
    )
}

fn valid_token(token: &str) -> bool {
    token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        result.push(char::from(DIGITS[usize::from(byte >> 4)]));
        result.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    result
}

fn unhex(input: &str) -> Option<[u8; 32]> {
    if !valid_token(input) {
        return None;
    }
    let mut bytes = [0; 32];
    for (output, pair) in bytes.iter_mut().zip(input.as_bytes().as_chunks::<2>().0) {
        let high = char::from(pair[0]).to_digit(16)? as u8;
        let low = char::from(pair[1]).to_digit(16)? as u8;
        *output = (high << 4) | low;
    }
    Some(bytes)
}

pub struct Principal {
    pub identity: WhoamiResponse,
    pub token_id: TokenId,
}

impl Principal {
    pub fn actor_id(&self) -> &str {
        &self.identity.actor_id
    }

    pub fn kind(&self) -> PrincipalKind {
        self.identity.principal_kind
    }

    pub fn author_kind(&self) -> AuthorKind {
        match self.kind() {
            PrincipalKind::Operator | PrincipalKind::HumanAdmin | PrincipalKind::HumanMember => {
                AuthorKind::Human
            }
            PrincipalKind::Participant => AuthorKind::Agent,
            PrincipalKind::Adapter
            | PrincipalKind::WorkflowRun
            | PrincipalKind::Service
            | PrincipalKind::Node => AuthorKind::Service,
        }
    }

    pub fn can_read_all(&self) -> bool {
        self.trusted_scope(ServiceScope::Read)
    }

    pub fn can_manage(&self) -> bool {
        self.trusted_scope(ServiceScope::Manage)
    }

    pub fn can_collaborate(&self) -> bool {
        self.trusted_scope(ServiceScope::Collaborate) || self.is_active_participant()
    }

    pub fn is_active_participant(&self) -> bool {
        self.kind() == PrincipalKind::Participant
            && self.identity.scopes.contains(&ServiceScope::Collaborate)
    }

    fn trusted_scope(&self, scope: ServiceScope) -> bool {
        matches!(
            self.kind(),
            PrincipalKind::Operator | PrincipalKind::Adapter
        ) && self.identity.scopes.iter().any(|allowed| *allowed >= scope)
    }
}

impl Auth {
    pub fn new(key: [u8; 32], node_id: NodeId) -> Self {
        Self { key, node_id }
    }

    fn mac(&self, kind: PrincipalKind, token: &str) -> io::Result<Hmac<Sha256>> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).map_err(io::Error::other)?;
        for segment in [DOMAIN, kind.as_str().as_bytes(), token.as_bytes()] {
            mac.update(&(segment.len() as u64).to_be_bytes());
            mac.update(segment);
        }
        Ok(mac)
    }

    pub fn insert_credential(
        &self,
        tx: &Connection,
        credential: &Credential,
        kind: PrincipalKind,
        now: i64,
    ) -> rusqlite::Result<()> {
        let scopes = match kind {
            PrincipalKind::Operator => r#"["read","collaborate","manage"]"#,
            PrincipalKind::Participant => r#"["collaborate"]"#,
            _ => {
                return Err(sql_conversion(io::Error::other(
                    "Unsupported local credential kind",
                )));
            }
        };
        let token_id = TokenId::generate().map_err(sql_conversion)?;
        let digest = hex(&self
            .mac(kind, &credential.token)
            .map_err(sql_conversion)?
            .finalize()
            .into_bytes());
        tx.execute(
            "INSERT INTO participant_credentials
             (token_id, participant_id, principal_kind, actor_id, scopes_json, verifier_hmac, created_at_ms)
             VALUES (?1, ?2, ?3, ?2, ?4, ?5, ?6)",
            params![token_id.as_str(), credential.participant_id.as_str(), kind.as_str(), scopes, digest, now],
        )?;
        Ok(())
    }

    pub fn operator_matches(
        &self,
        conn: &Connection,
        credential: &Credential,
        now: i64,
    ) -> rusqlite::Result<bool> {
        Ok(self
            .lookup(conn, &credential.token, now)?
            .is_some_and(|principal| {
                principal.kind() == PrincipalKind::Operator
                    && principal.identity.participant_id.as_ref()
                        == Some(&credential.participant_id)
                    && principal.actor_id() == credential.participant_id.as_str()
                    && principal.can_manage()
                    && principal.identity.name == "operator"
            }))
    }

    pub async fn authenticate(
        &self,
        reads: &ReadPool,
        token: String,
        now: i64,
    ) -> Result<Principal> {
        if !valid_token(&token) {
            return Err(unauthorized());
        }
        let auth = self.clone();
        reads
            .read(move |conn| auth.lookup(conn, &token, now).map_err(Into::into))
            .await?
            .ok_or_else(unauthorized)
    }

    pub async fn is_active(reads: &ReadPool, token_id: TokenId, now: i64) -> Result<bool> {
        Ok(reads
            .read(move |conn| {
                conn.query_row(
                    "SELECT EXISTS (SELECT 1 FROM participant_credentials WHERE token_id = ?1
                    AND revoked_at_ms IS NULL AND (expires_at_ms IS NULL OR expires_at_ms > ?2))",
                    params![token_id.as_str(), now],
                    |row| row.get(0),
                )
                .map_err(Into::into)
            })
            .await?)
    }

    fn lookup(
        &self,
        conn: &Connection,
        token: &str,
        now: i64,
    ) -> rusqlite::Result<Option<Principal>> {
        let digests = PRINCIPALS.map(|kind| {
            self.mac(kind, token)
                .map(|mac| hex(&mac.finalize().into_bytes()))
        });
        let [participant, operator, adapter, workflow] = digests;
        let mut rows = conn.prepare_cached(
            "SELECT c.token_id, c.participant_id, c.principal_kind, c.actor_id, c.scopes_json,
                    c.verifier_hmac, COALESCE(p.name, c.actor_id), p.kind
             FROM participant_credentials c LEFT JOIN participants p USING (participant_id)
             WHERE c.verifier_hmac IN (?1, ?2, ?3, ?4)
               AND c.revoked_at_ms IS NULL AND (c.expires_at_ms IS NULL OR c.expires_at_ms > ?5)",
        )?;
        let candidate = rows
            .query_row(
                params![
                    participant.map_err(sql_conversion)?,
                    operator.map_err(sql_conversion)?,
                    adapter.map_err(sql_conversion)?,
                    workflow.map_err(sql_conversion)?,
                    now,
                ],
                |row| {
                    let token_id: String = row.get(0)?;
                    let participant_id: Option<String> = row.get(1)?;
                    let kind: String = row.get(2)?;
                    let scopes: String = row.get(4)?;
                    let digest: String = row.get(5)?;
                    let participant_kind: Option<String> = row.get(7)?;
                    let identity = WhoamiResponse {
                        actor_id: row.get(3)?,
                        principal_kind: kind.parse().map_err(sql_conversion)?,
                        node_id: Some(self.node_id.clone()),
                        participant_id: participant_id
                            .as_deref()
                            .map(ParticipantId::parse)
                            .transpose()
                            .map_err(sql_conversion)?,
                        name: row.get(6)?,
                        scopes: serde_json::from_str(&scopes).map_err(sql_conversion)?,
                    };
                    let participant_kind = participant_kind
                        .as_deref()
                        .map(str::parse::<ParticipantKind>)
                        .transpose()
                        .map_err(sql_conversion)?;
                    Ok((
                        Principal {
                            identity,
                            token_id: token_id.parse().map_err(sql_conversion)?,
                        },
                        digest,
                        participant_kind,
                    ))
                },
            )
            .optional()?;
        let Some((principal, digest, participant_kind)) = candidate else {
            return Ok(None);
        };
        let participant_kind_matches = match principal.kind() {
            PrincipalKind::Operator => participant_kind == Some(ParticipantKind::Human),
            PrincipalKind::Participant => participant_kind == Some(ParticipantKind::Agent),
            PrincipalKind::Adapter | PrincipalKind::WorkflowRun => {
                principal.identity.participant_id.is_none() && participant_kind.is_none()
            }
            _ => false,
        };
        if !participant_kind_matches {
            return Ok(None);
        }
        let Some(expected) = unhex(&digest) else {
            return Ok(None);
        };
        let verified = self
            .mac(principal.kind(), token)
            .map_err(sql_conversion)?
            .verify_slice(&expected)
            .is_ok();
        Ok(verified.then_some(principal))
    }
}

fn unauthorized() -> NodeError {
    NodeError::new(
        ErrorCode::Unauthorized,
        "A valid Node bearer credential is required",
    )
}

fn sql_conversion(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

#[cfg(test)]
mod tests;
