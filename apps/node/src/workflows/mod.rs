//! A turn-completion entry point for the workflow runner.

use cooplet_protocol::ParticipantId;
use serde_json::Value;

use crate::clients::TurnFinishStatus;

pub struct TurnCompletion<'a> {
    pub participant_id: &'a ParticipantId,
    pub generation: u64,
    pub native_turn_id: Option<&'a str>,
    pub operation_key: Option<&'a str>,
    pub status: TurnFinishStatus,
    pub text: &'a str,
    pub structured: Option<&'a Value>,
}

pub async fn on_turn_completed(_event: TurnCompletion<'_>) {
    // The autonomous Node has no workflow runner to notify.
}
