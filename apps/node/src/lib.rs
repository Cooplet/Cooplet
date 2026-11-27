//! Autonomous Cooplet Node.

#[cfg(test)]
extern crate self as cooplet_node;

pub mod auth;
pub mod bridge;
pub mod clients;
pub mod config;
pub mod daemon;
pub mod delivery;
pub mod error;
pub mod http;
pub mod mcp;
pub mod operations;
pub mod storage;
pub mod workflows;

use std::{
    io,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> io::Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    i64::try_from(elapsed.as_millis()).map_err(io::Error::other)
}
