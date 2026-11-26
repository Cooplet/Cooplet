use std::fmt;

use http::header::{HeaderName, HeaderValue};

use crate::ClientError;

pub const X_COOPLET_HUB: HeaderName = HeaderName::from_static("x-cooplet-hub");
pub const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");
pub const X_COOPLET_FILENAME: HeaderName = HeaderName::from_static("x-cooplet-filename");
pub const X_COOPLET_SHA256: HeaderName = HeaderName::from_static("x-cooplet-sha256");

#[derive(Clone)]
pub struct BearerToken(HeaderValue);

impl BearerToken {
    pub fn new(raw: &str) -> Result<Self, ClientError> {
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_graphic()) {
            return Err(ClientError::invalid("invalid bearer token"));
        }
        let mut value = HeaderValue::from_str(&format!("Bearer {raw}"))
            .map_err(|_| ClientError::invalid("invalid bearer token"))?;
        value.set_sensitive(true);
        Ok(Self(value))
    }

    pub(crate) fn header(&self) -> HeaderValue {
        self.0.clone()
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BearerToken([REDACTED])")
    }
}
