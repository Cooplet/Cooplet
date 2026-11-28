use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use url::{Host, Url};

use crate::ClientError;

/// Return whether the Hub uses a literal loopback address. A Hub outside
/// loopback must use HTTPS. A WebSocket URL should be derived from this origin.
pub fn validate_hub_origin(base: &Url) -> Result<bool, ClientError> {
    validate_origin(base, false)
}

pub(crate) fn validate_origin(base: &Url, local_node: bool) -> Result<bool, ClientError> {
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
    Ok(loopback)
}

/// Build the same trust configuration for HTTP and the WebSocket handshake.
pub fn build_rustls_client_config(
    root_certificates_pem: &[Vec<u8>],
) -> Result<Arc<ClientConfig>, ClientError> {
    let mut roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    for pem in root_certificates_pem {
        let mut seen_certificate = false;
        for certificate in CertificateDer::pem_slice_iter(pem) {
            let certificate =
                certificate.map_err(|_| ClientError::invalid("invalid root certificate PEM"))?;
            roots
                .add(certificate)
                .map_err(|_| ClientError::invalid("invalid root certificate DER"))?;
            seen_certificate = true;
        }
        if !seen_certificate {
            return Err(ClientError::invalid("invalid root certificate PEM"));
        }
    }
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| ClientError::invalid("invalid TLS protocol configuration"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}
