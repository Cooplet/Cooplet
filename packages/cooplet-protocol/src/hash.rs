use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::ProtocolError;

/// Compact JSON with object keys sorted recursively. Array order is significant.
pub fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, ProtocolError> {
    let mut value = serde_json::to_value(value)?;
    sort_object_keys(&mut value);
    Ok(serde_json::to_vec(&value)?)
}

fn sort_object_keys(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.sort_keys();
            for child in object.values_mut() {
                sort_object_keys(child);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(sort_object_keys),
        _ => {}
    }
}

pub fn canonical_sha256<T: Serialize + ?Sized>(value: &T) -> Result<String, ProtocolError> {
    Ok(format!(
        "{:x}",
        Sha256::digest(canonical_json_bytes(value)?)
    ))
}
