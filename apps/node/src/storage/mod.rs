//! Node's application schema and transactional maintenance.

mod events;
mod gc;
mod recovery;

#[cfg(test)]
mod tests;

pub use events::{MAX_EVENT_FRAME_BYTES, append_event, event_frame_too_large, read_event};
pub(crate) use events::{check_next_event, json_encoded_len};
pub use gc::NodeGcHooks;
pub(crate) use gc::QUEUE_EXPIRED_DETAIL;
pub use recovery::recover;
pub(crate) use recovery::{
    reconcile_pending_message, retry_recovery_write, revoke_participant_credentials,
};

use cooplet_storage::Migration;

pub static MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "node",
    sql: include_str!("001-node.sql"),
}];

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

fn bad_data(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}
