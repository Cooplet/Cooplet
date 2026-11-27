use cooplet_protocol::{CommandReqPayload, CommandResPayload, NodeId};
use futures_util::future::BoxFuture;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReserveError {
    Offline,
    // The live WS registry will use this variant when its per-node slots fill up.
    #[allow(dead_code)]
    RateLimited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitError {
    /// The command frame is proven not to have left the Hub (safe to surface as 502).
    #[allow(dead_code)]
    Unavailable,
    /// The command may have reached the destination node; never replay automatically.
    Unknown,
}

/// A reservation owns one slot for a specific bridge session. Dropping it
/// releases the slot without sending a command.
pub(crate) trait CommandPermit: Send {
    fn submit(
        self: Box<Self>,
        request: CommandReqPayload,
    ) -> BoxFuture<'static, Result<CommandResPayload, SubmitError>>;
}

pub(crate) trait BridgeRegistry: Send + Sync {
    fn reserve(&self, node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError>;
    fn disconnect_revoked(&self, node_id: &NodeId);
    /// Abort pending in-flight bridge dispatches during graceful Hub shutdown.
    fn shutdown(&self) {}
}

pub(crate) struct OfflineBridge;

impl BridgeRegistry for OfflineBridge {
    fn reserve(&self, _node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        Err(ReserveError::Offline)
    }

    fn disconnect_revoked(&self, _node_id: &NodeId) {}
}
