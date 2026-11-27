//! Agy frames received while a client request is still being written.

use serde_json::Value;

use super::acp_wire::{Fault, Frame, ReceivedFrame, Reply, Wire};

// Server requests cannot be answered until the frame already on stdin
// finishes. Keep the backlog bounded even if the peer keeps writing them.
const MAX_DEFERRED_REQUESTS: usize = 4;

pub(super) struct InboundDuringWrite {
    id: u64,
    reply: Option<Reply>,
    unsupported: Vec<Value>,
}

impl InboundDuringWrite {
    pub(super) fn new(id: u64) -> Self {
        Self {
            id,
            reply: None,
            unsupported: Vec::new(),
        }
    }

    pub(super) fn consume(
        &mut self,
        incoming: ReceivedFrame,
        on_notification: impl FnOnce(&str, &Value, std::time::Instant) -> Result<(), Fault>,
    ) -> Result<(), Fault> {
        match incoming.frame {
            Frame::Request { method, .. } if method == "session/request_permission" => {
                return Err(Fault::PermissionRequest);
            }
            Frame::Request { id, .. } => {
                if self.unsupported.len() == MAX_DEFERRED_REQUESTS {
                    return Err(Fault::TooManyRequests);
                }
                self.unsupported.push(id);
            }
            Frame::Notification { method, params } if self.reply.is_none() => {
                on_notification(&method, &params, incoming.read_at)?;
            }
            Frame::Response { id, reply }
                if id.as_u64() == Some(self.id) && self.reply.is_none() =>
            {
                self.reply = Some(reply);
            }
            Frame::Notification { .. } | Frame::Response { .. } => {}
        }
        Ok(())
    }

    pub(super) async fn finish(self, wire: &Wire) -> Result<Option<Reply>, Fault> {
        for id in self.unsupported {
            wire.reject_request(id).await?;
        }
        Ok(self.reply)
    }
}
