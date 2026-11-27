use cooplet_protocol::ErrorCode;

use crate::error::{NodeError, Result};

/// A Hub projection can only be answered by its authoritative Hub.
pub(crate) fn require_local_authority(hub_message_id: Option<&str>) -> Result<()> {
    if hub_message_id.is_some() {
        return Err(NodeError::new(
            ErrorCode::BridgeUnavailable,
            "This conversation requires a connection to its Hub",
        ));
    }
    Ok(())
}
