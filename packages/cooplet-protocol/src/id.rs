use std::{
    fmt,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use utoipa::{
    PartialSchema, ToSchema,
    openapi::{
        ObjectBuilder, RefOr,
        schema::{Schema, Type},
    },
};

const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
const TIMESTAMP_LIMIT: u64 = 1 << 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("invalid {0} identifier")]
    Invalid(&'static str),
    #[error("clock is outside the ULID timestamp range")]
    ClockUnavailable,
    #[error("could not get random bytes: {0}")]
    EntropyUnavailable(getrandom::Error),
}

fn string_schema(pattern: &str) -> RefOr<Schema> {
    ObjectBuilder::new()
        .schema_type(Type::String)
        .pattern(Some(pattern))
        .into()
}

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub const PATTERN: &str = concat!("^", $prefix, "[0-7][0-9a-hjkmnpqrstvwxyz]{25}$");

            pub fn generate() -> Result<Self, IdError> {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| IdError::ClockUnavailable)?
                    .as_millis();
                let timestamp = u64::try_from(timestamp).map_err(|_| IdError::ClockUnavailable)?;
                let mut random = [0; 10];
                getrandom::fill(&mut random).map_err(IdError::EntropyUnavailable)?;
                Self::from_parts(timestamp, random)
            }

            pub fn from_parts(timestamp_ms: u64, random: [u8; 10]) -> Result<Self, IdError> {
                if timestamp_ms >= TIMESTAMP_LIMIT {
                    return Err(IdError::ClockUnavailable);
                }
                let mut bytes = [0; 16];
                bytes[..6].copy_from_slice(&timestamp_ms.to_be_bytes()[2..]);
                bytes[6..].copy_from_slice(&random);
                let value = u128::from_be_bytes(bytes);
                let mut id = String::with_capacity($prefix.len() + 26);
                id.push_str($prefix);
                for group in (0..26).rev() {
                    let digit = ((value >> (group * 5)) & 31) as usize;
                    id.push(char::from(ALPHABET[digit]));
                }
                Ok(Self(id))
            }

            pub fn parse(input: &str) -> Result<Self, IdError> {
                let suffix = input.strip_prefix($prefix).map(str::as_bytes);
                match suffix {
                    Some([b'0'..=b'7', rest @ ..])
                        if rest.len() == 25 && rest.iter().all(|byte| ALPHABET.contains(byte)) =>
                    {
                        Ok(Self(input.into()))
                    }
                    _ => Err(IdError::Invalid(stringify!($name))),
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(input: &str) -> Result<Self, Self::Err> {
                Self::parse(input)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                String::deserialize(deserializer)?
                    .parse()
                    .map_err(de::Error::custom)
            }
        }

        impl PartialSchema for $name {
            fn schema() -> RefOr<Schema> {
                string_schema(Self::PATTERN)
            }
        }

        impl ToSchema for $name {}
    };
}

define_id!(NodeId, "node_");
define_id!(ParticipantId, "part_");
define_id!(MessageId, "msg_");
define_id!(ThreadId, "thr_");
define_id!(BlobId, "blob_");
define_id!(RunId, "run_");
define_id!(EventId, "evt_");
define_id!(OperationId, "op_");
define_id!(UserId, "usr_");
define_id!(ServicePrincipalId, "sp_");
define_id!(TokenId, "tok_");
define_id!(HubId, "hub_");
define_id!(RequestId, "req_");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Node,
    Hub,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SourceId {
    Node(NodeId),
    Hub(HubId),
}

impl SourceId {
    pub const PATTERN: &str = "^(node|hub)_[0-7][0-9a-hjkmnpqrstvwxyz]{25}$";

    pub fn kind(&self) -> SourceKind {
        match self {
            Self::Node(_) => SourceKind::Node,
            Self::Hub(_) => SourceKind::Hub,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Node(id) => id.as_str(),
            Self::Hub(id) => id.as_str(),
        }
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SourceId {
    type Err = IdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.split_once('_') {
            Some(("node", _)) => input.parse().map(Self::Node),
            Some(("hub", _)) => input.parse().map(Self::Hub),
            _ => Err(IdError::Invalid("SourceId")),
        }
    }
}

impl Serialize for SourceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SourceId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

impl PartialSchema for SourceId {
    fn schema() -> RefOr<Schema> {
        string_schema(Self::PATTERN)
    }
}

impl ToSchema for SourceId {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AggregateType {
    Node,
    Participant,
    Message,
    Thread,
    Blob,
    WorkflowRun,
}
