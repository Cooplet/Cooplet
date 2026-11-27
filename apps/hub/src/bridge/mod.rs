use cooplet_protocol::{
    CommandReqPayload, CommandResPayload, NodeId, ThreadAckPayload, ThreadDeliverPayload,
};
use futures_util::future::BoxFuture;

mod projection;
mod registry;
mod roster;
mod session;
mod upgrade;

pub(crate) use projection::{ApprovalView, RosterCatalog, redact_approval_value};
pub(crate) use registry::LiveBridge;
pub(crate) use upgrade::{__path_connect, connect};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReserveError {
    Offline,
    RateLimited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubmitError {
    /// The frame is proven not to have reached the WebSocket writer.
    Unavailable,
    /// The frame may have reached the destination node.
    Unknown,
}

/// A reservation owns one slot for a specific session until dropped or completed.
pub(crate) trait CommandPermit: Send {
    fn session(&self) -> CommandSession;

    fn submit(
        self: Box<Self>,
        request: CommandReqPayload,
    ) -> BoxFuture<'static, Result<CommandResPayload, SubmitError>>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommandSession {
    pub(crate) generation: u64,
    pub(crate) epoch: u64,
}

pub(crate) trait ThreadPermit: Send {
    fn epoch(&self) -> u64;

    fn submit(
        self: Box<Self>,
        request: ThreadDeliverPayload,
    ) -> BoxFuture<'static, Result<ThreadAckPayload, SubmitError>>;
}

pub(crate) trait BridgeRegistry: Send + Sync {
    fn reserve(&self, node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError>;

    fn reserve_thread<'a>(
        &'a self,
        _node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Box<dyn ThreadPermit>, ReserveError>> {
        Box::pin(std::future::ready(Err(ReserveError::Offline)))
    }

    fn live(&self) -> Option<&LiveBridge> {
        None
    }

    fn is_online(&self, _node_id: &NodeId) -> bool {
        false
    }

    fn disconnect_revoked(&self, node_id: &NodeId);

    fn disconnect_transient(&self, _node_id: &NodeId) {}

    fn shutdown(&self) {}

    fn drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[cfg(test)]
pub(crate) struct OfflineBridge;

#[cfg(test)]
impl BridgeRegistry for OfflineBridge {
    fn reserve(&self, _node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        Err(ReserveError::Offline)
    }

    fn disconnect_revoked(&self, _node_id: &NodeId) {}
}
