use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use cooplet_platform::{MonoClock, MonoInstant};
use cooplet_protocol::{
    BridgeFrame, ErrorCode, NodeRequestCaller, NodeRequestPayload, NodeResponsePayload,
    NodeRpcOperation, OperationId, ProtocolError,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::error::{NodeError, Result};

use super::{NodeBridge, offline, session::Outbound, wait_deadline};

const MAX_REVERSE_IN_FLIGHT: usize = 16;
const WAITING: u8 = 0;
const SENT: u8 = 1;
const CANCELED: u8 = 2;

#[derive(Clone)]
pub(super) struct ReverseHandle {
    pub(super) epoch: u64,
    pub(super) outbox: mpsc::Sender<Outbound>,
    slots: Arc<Semaphore>,
}

impl ReverseHandle {
    pub(super) fn new(epoch: u64, outbox: mpsc::Sender<Outbound>) -> Self {
        Self {
            epoch,
            outbox,
            slots: Arc::new(Semaphore::new(MAX_REVERSE_IN_FLIGHT)),
        }
    }
}

struct Cancellation(Arc<AtomicU8>);

impl Cancellation {
    fn cancel(&self) -> bool {
        self.0.swap(CANCELED, Ordering::AcqRel) == SENT
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        self.cancel();
    }
}

pub(super) struct ReverseRequest {
    id: OperationId,
    frame: String,
    cut: u64,
    ticket: PendingReply,
}

impl ReverseRequest {
    pub(super) fn deadline(&self) -> MonoInstant {
        self.ticket.deadline
    }

    pub(super) fn take_frame(&mut self) -> String {
        std::mem::take(&mut self.frame)
    }

    fn can_send(&self, cursor: u64) -> bool {
        self.cut <= cursor
    }

    pub(super) fn mark_sent(&self, clock: &dyn MonoClock) -> bool {
        !clock.is_expired(self.ticket.deadline)
            && self
                .ticket
                .phase
                .compare_exchange(WAITING, SENT, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

struct PendingReply {
    deadline: MonoInstant,
    phase: Arc<AtomicU8>,
    reply: oneshot::Sender<NodeResponsePayload>,
    _slot: OwnedSemaphorePermit,
}

#[derive(Default)]
pub(super) struct ReverseRequests {
    waiting: VecDeque<ReverseRequest>,
    sent: HashMap<OperationId, PendingReply>,
}

impl ReverseRequests {
    pub(super) fn enqueue(&mut self, request: ReverseRequest) {
        self.waiting.push_back(request);
    }

    pub(super) fn has_ready(&self, cursor: Option<u64>) -> bool {
        cursor.is_some_and(|cursor| self.waiting.iter().any(|request| request.can_send(cursor)))
    }

    pub(super) fn take_ready(&mut self, cursor: Option<u64>) -> Option<ReverseRequest> {
        let cursor = cursor?;
        let index = self
            .waiting
            .iter()
            .position(|request| request.can_send(cursor))?;
        self.waiting.remove(index)
    }

    pub(super) fn sent(&mut self, request: ReverseRequest) {
        self.sent.insert(request.id, request.ticket);
    }

    pub(super) fn respond(&mut self, response: NodeResponsePayload, clock: &dyn MonoClock) {
        if let Some(request) = self.sent.remove(&response.request_id)
            && !clock.is_expired(request.deadline)
        {
            let _ = request.reply.send(response);
        }
    }

    pub(super) fn earliest_deadline(&self, clock: &dyn MonoClock) -> Option<MonoInstant> {
        self.waiting
            .iter()
            .map(|request| request.ticket.deadline)
            .chain(self.sent.values().map(|request| request.deadline))
            .min_by_key(|&deadline| clock.remaining_ms(deadline).unwrap_or(0))
    }

    pub(super) fn expire(&mut self, clock: &dyn MonoClock) {
        self.waiting.retain(|request| {
            request.ticket.phase.load(Ordering::Acquire) != CANCELED
                && !clock.is_expired(request.ticket.deadline)
        });
        self.sent
            .retain(|_, request| !clock.is_expired(request.deadline));
    }
}

impl NodeBridge {
    pub async fn request(
        &self,
        caller: NodeRequestCaller,
        operation: NodeRpcOperation,
    ) -> Result<NodeResponsePayload> {
        if *self.shutdown.borrow() {
            return Err(offline());
        }
        if let Some(code) = *self.terminal.borrow() {
            return Err(NodeError::new(code, code.title()));
        }
        let session = self.session.borrow().clone().ok_or_else(offline)?;
        let slot = session.slots.clone().try_acquire_owned().map_err(|_| {
            NodeError::new(
                ErrorCode::RateLimited,
                "Too many Hub bridge requests are in flight",
            )
        })?;
        let mut stop = self.shutdown.subscribe();
        let deadline = self.clock.deadline_after(operation.correlation_timeout());
        let mutating = operation.is_mutating();
        let cut = tokio::select! {
            result = self.event_head() => result?,
            () = wait_deadline(&*self.clock, Some(deadline)) => return Err(offline()),
            _ = stop.wait_for(|value| *value) => return Err(offline()),
        };
        let id = OperationId::generate()?;
        let bytes = BridgeFrame::encode_json(
            session.epoch,
            &NodeRequestPayload {
                request_id: id.clone(),
                caller,
                operation,
            },
        )
        .map_err(|error| match error {
            ProtocolError::BridgeFrameTooLarge { .. } => NodeError::new(
                ErrorCode::PayloadTooLarge,
                "The request exceeds the bridge frame limit",
            ),
            _ => NodeError::new(ErrorCode::InvalidRequest, "Invalid bridge request"),
        })?;
        let frame = String::from_utf8(bytes).map_err(NodeError::internal)?;
        let (reply, answered) = oneshot::channel();
        let phase = Arc::new(AtomicU8::new(WAITING));
        let cancellation = Cancellation(phase.clone());
        let request = ReverseRequest {
            id,
            frame,
            cut,
            ticket: PendingReply {
                deadline,
                phase,
                reply,
                _slot: slot,
            },
        };
        session
            .outbox
            .try_send(Outbound::Reverse(request))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => NodeError::new(
                    ErrorCode::RateLimited,
                    "The Hub bridge request queue is full",
                ),
                mpsc::error::TrySendError::Closed(_) => offline(),
            })?;
        let response = tokio::select! {
            biased;
            response = answered => response.ok(),
            () = wait_deadline(&*self.clock, Some(deadline)) => None,
            _ = stop.wait_for(|value| *value) => None,
        };
        response.ok_or_else(|| {
            if cancellation.cancel() && mutating {
                NodeError::new(
                    ErrorCode::DeliveryUnknown,
                    "The Hub may have applied the request before the bridge stopped waiting",
                )
            } else {
                offline()
            }
        })
    }

    async fn event_head(&self) -> Result<u64> {
        self.context
            .as_ref()
            .ok_or_else(offline)?
            .event_head()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;

    struct Clock(AtomicU64);

    impl MonoClock for Clock {
        fn now(&self) -> MonoInstant {
            MonoInstant::from_millis(self.0.load(Ordering::Relaxed))
        }
    }

    #[test]
    fn reverse_request_waits_for_its_event_cut_and_cannot_send_after_cancel()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let clock = Clock(AtomicU64::new(0));
        let slots = Arc::new(Semaphore::new(1));
        let (reply, _answer) = oneshot::channel();
        let phase = Arc::new(AtomicU8::new(WAITING));
        let cancellation = Cancellation(phase.clone());
        let mut requests = ReverseRequests::default();
        requests.enqueue(ReverseRequest {
            id: OperationId::generate()?,
            frame: "{}".into(),
            cut: 10,
            ticket: PendingReply {
                deadline: MonoInstant::from_millis(10),
                phase,
                reply,
                _slot: slots.clone().try_acquire_owned()?,
            },
        });
        assert!(!requests.has_ready(None));
        assert!(!requests.has_ready(Some(9)));
        assert!(requests.has_ready(Some(10)));
        assert!(!cancellation.cancel());
        let ready = requests
            .take_ready(Some(10))
            .ok_or_else(|| std::io::Error::other("the request was not present"))?;
        assert!(!ready.mark_sent(&clock));
        drop(ready);
        assert_eq!(slots.available_permits(), 1);
        Ok(())
    }
}
