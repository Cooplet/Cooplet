use std::{io, path::Path};

use cooplet_platform::{PlatformError, PrivateFs, random_bytes};
use cooplet_protocol::{NodeId, ServicePrincipalId, TokenId};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::HubError;

const SECRET_BYTES: usize = 32;
type Verifier = Hmac<Sha256>;

pub(crate) struct Master([u8; SECRET_BYTES]);

impl Master {
    pub(crate) fn generate() -> Result<Self, PlatformError> {
        random_secret().map(Self)
    }

    pub(crate) fn from_file(path: &Path) -> io::Result<Self> {
        let bytes =
            PrivateFs::read_private_file(path, SECRET_BYTES as u64).map_err(io::Error::other)?;
        let bytes = bytes.try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Hub master key must be 32 bytes",
            )
        })?;
        Ok(Self(bytes))
    }

    pub(crate) fn as_bytes(&self) -> &[u8; SECRET_BYTES] {
        &self.0
    }

    pub(crate) fn fingerprint(&self) -> String {
        format!("{:x}", Sha256::digest(self.0))
    }

    fn mac(&self, domain: &'static str, fields: &[&str]) -> Result<Verifier, HubError> {
        let mut mac = Verifier::new_from_slice(&self.0).map_err(HubError::internal)?;
        for value in ["cooplet.hub.v0", domain]
            .into_iter()
            .chain(fields.iter().copied())
        {
            mac.update(&(value.len() as u64).to_be_bytes());
            mac.update(value.as_bytes());
        }
        Ok(mac)
    }
}

pub(crate) struct SecretToken(String);

impl SecretToken {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}

pub(crate) struct MintedToken {
    pub(crate) verifier: String,
    pub(crate) secret: SecretToken,
}

#[derive(Clone, Copy)]
enum Domain {
    Bearer,
    Service,
    Enrollment,
    Node,
}

impl Domain {
    fn tag(self) -> &'static str {
        match self {
            Self::Bearer => "bearer-verifier",
            Self::Service => "service-verifier",
            Self::Enrollment => "enrollment-verifier",
            Self::Node => "node-verifier",
        }
    }

    fn prefix(self) -> &'static str {
        match self {
            Self::Bearer => "cplt_tok_",
            Self::Service => "cplt_sp_",
            Self::Enrollment => "cplt_enr_",
            Self::Node => "cplt_node_",
        }
    }

    fn id_prefix(self) -> &'static str {
        match self {
            Self::Bearer | Self::Enrollment => "tok_",
            Self::Service => "sp_",
            Self::Node => "node_",
        }
    }

    fn parse(self, wire: &str) -> Option<(String, &str)> {
        let (ulid, secret) = wire.strip_prefix(self.prefix())?.split_once('.')?;
        if ulid.len() != 26 || !is_hex_secret(secret) {
            return None;
        }
        Some((format!("{}{ulid}", self.id_prefix()), secret))
    }
}

pub(crate) enum ApiCredential<'a> {
    Bearer(TokenId, &'a str),
    Service(ServicePrincipalId, &'a str),
    Node(NodeId, &'a str),
}

impl<'a> ApiCredential<'a> {
    pub(crate) fn parse(wire: &'a str) -> Option<Self> {
        if let Some((id, secret)) = Domain::Bearer.parse(wire) {
            return Some(Self::Bearer(TokenId::parse(&id).ok()?, secret));
        }
        if let Some((id, secret)) = Domain::Service.parse(wire) {
            return Some(Self::Service(ServicePrincipalId::parse(&id).ok()?, secret));
        }
        let (id, secret) = Domain::Node.parse(wire)?;
        Some(Self::Node(NodeId::parse(&id).ok()?, secret))
    }

    fn mac(&self, master: &Master, generation: u64) -> Result<Verifier, HubError> {
        match self {
            Self::Bearer(id, raw) => master.mac(Domain::Bearer.tag(), &[id.as_str(), raw]),
            Self::Service(id, raw) => master.mac(Domain::Service.tag(), &[id.as_str(), raw]),
            Self::Node(id, raw) => master.mac(
                Domain::Node.tag(),
                &[id.as_str(), &generation.to_string(), raw],
            ),
        }
    }

    pub(crate) fn service_lookup(&self, master: &Master) -> Result<String, HubError> {
        Ok(format!(
            "{:x}",
            self.mac(master, 0)?.finalize().into_bytes()
        ))
    }

    pub(crate) fn verify(
        &self,
        master: &Master,
        stored: &str,
        generation: u64,
    ) -> Result<bool, HubError> {
        Ok(verify(self.mac(master, generation)?, stored))
    }
}

pub(crate) struct EnrollmentCredential<'a> {
    pub(crate) token_id: TokenId,
    raw: &'a str,
}

impl<'a> EnrollmentCredential<'a> {
    pub(crate) fn parse(wire: &'a str) -> Option<Self> {
        let (id, raw) = Domain::Enrollment.parse(wire)?;
        Some(Self {
            token_id: TokenId::parse(&id).ok()?,
            raw,
        })
    }

    pub(crate) fn verify(&self, master: &Master, stored: &str) -> Result<bool, HubError> {
        let mac = master.mac(
            Domain::Enrollment.tag(),
            &[self.token_id.as_str(), self.raw],
        )?;
        Ok(verify(mac, stored))
    }
}

pub(crate) fn enrollment_node_id(id: &TokenId) -> Result<NodeId, HubError> {
    NodeId::parse(&id.as_str().replacen("tok_", "node_", 1)).map_err(HubError::from)
}

pub(crate) fn mint_bearer(master: &Master, id: TokenId) -> Result<MintedToken, HubError> {
    mint(master, Domain::Bearer, id.as_str(), None)
}

pub(crate) fn mint_service(
    master: &Master,
    id: &ServicePrincipalId,
) -> Result<MintedToken, HubError> {
    mint(master, Domain::Service, id.as_str(), None)
}

pub(crate) fn mint_enrollment(master: &Master, id: TokenId) -> Result<MintedToken, HubError> {
    mint(master, Domain::Enrollment, id.as_str(), None)
}

pub(crate) fn mint_node(
    master: &Master,
    id: &NodeId,
    generation: u64,
) -> Result<MintedToken, HubError> {
    mint(master, Domain::Node, id.as_str(), Some(generation))
}

fn mint(
    master: &Master,
    domain: Domain,
    id: &str,
    generation: Option<u64>,
) -> Result<MintedToken, HubError> {
    let raw = encode_hex(&random_secret().map_err(HubError::internal)?);
    let mac = match generation {
        Some(generation) => master.mac(domain.tag(), &[id, &generation.to_string(), &raw])?,
        None => master.mac(domain.tag(), &[id, &raw])?,
    };
    let ulid = id
        .strip_prefix(domain.id_prefix())
        .ok_or_else(|| HubError::internal("invalid credential identity"))?;
    Ok(MintedToken {
        verifier: format!("{:x}", mac.finalize().into_bytes()),
        secret: SecretToken(format!("{}{ulid}.{raw}", domain.prefix())),
    })
}

fn random_secret() -> Result<[u8; SECRET_BYTES], PlatformError> {
    let mut bytes = [0; SECRET_BYTES];
    random_bytes(&mut bytes)?;
    Ok(bytes)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 0xf)]));
    }
    hex
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn is_hex_secret(hex: &str) -> bool {
    hex.len() == SECRET_BYTES * 2 && hex.bytes().all(|byte| hex_digit(byte).is_some())
}

fn verify(mac: Verifier, stored: &str) -> bool {
    if stored.len() != SECRET_BYTES * 2 {
        return false;
    }
    let mut bytes = [0; SECRET_BYTES];
    for (byte, pair) in bytes.iter_mut().zip(stored.as_bytes().chunks_exact(2)) {
        let (Some(high), Some(low)) = (hex_digit(pair[0]), hex_digit(pair[1])) else {
            return false;
        };
        *byte = (high << 4) | low;
    }
    mac.verify_slice(&bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrollment_and_node_domains_bind_identity_and_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let master = Master::generate()?;
        let token_id = TokenId::generate()?;
        let node_id = enrollment_node_id(&token_id)?;
        let enrollment = mint_enrollment(&master, token_id.clone())?;
        let node = mint_node(&master, &node_id, 1)?;
        let parsed = EnrollmentCredential::parse(enrollment.secret.as_str())
            .ok_or("invalid minted enrollment")?;
        assert_eq!(parsed.token_id, token_id);
        assert!(parsed.verify(&master, &enrollment.verifier)?);
        assert!(ApiCredential::parse(enrollment.secret.as_str()).is_none());
        let node_token =
            ApiCredential::parse(node.secret.as_str()).ok_or("invalid minted node token")?;
        assert!(node_token.verify(&master, &node.verifier, 1)?);
        assert!(!node_token.verify(&master, &node.verifier, 2)?);
        assert!(!node_token.verify(&master, &enrollment.verifier, 1)?);
        Ok(())
    }
}
