use std::io;

use serde_json::Value;

pub fn adapter_report<'a>(diagnosis: &'a Value, kind: &str) -> io::Result<&'a Value> {
    diagnosis["adapters"]
        .as_array()
        .and_then(|reports| reports.iter().find(|report| report["client_kind"] == kind))
        .ok_or_else(|| io::Error::other(format!("Doctor did not report the {kind} adapter")))
}
