use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use cooplet_platform::{MonoClock, MonoInstant};
use cooplet_protocol::{
    CommandReqPayload, CommandResPayload, ErrorCode, NodeId, ThreadAckPayload, ThreadDeliverPayload,
};
use futures_util::future::BoxFuture;
use tokio::sync::{
    OwnedRwLockReadGuard, OwnedRwLockWriteGuard, OwnedSemaphorePermit, RwLock, Semaphore, mpsc,
    oneshot, watch,
};

use crate::error::HubError;

use super::{
    BridgeRegistry, CommandPermit, CommandSession, ReserveError, SubmitError, ThreadPermit,
};

pub(super) const COMMAND_SLOTS: u32 = 32;
pub(super) const THREAD_SLOTS: u32 = 16;
pub(super) const SESSION_QUEUE: usize = 64;

#[derive(Clone)]
pub(crate) struct LiveBridge {
    inner: Arc<Inner>,
}

struct Inner {
    registry: Mutex<Registry>,
    tasks: watch::Sender<usize>,
    capacity: usize,
    clock: Arc<dyn MonoClock>,
}

struct Registry {
    open: bool,
    nodes: HashMap<NodeId, Entry>,
    active_nodes: usize,
}

#[derive(Default)]
struct Entry {
    // Retain the high-water mark for as long as a socket or upgrade still
    // references the node. After that only a fresh durable bump can enter.
    high_water_epoch: u64,
    allocated_epoch: u64,
    attempts: usize,
    transitions: usize,
    authority: Arc<RwLock<()>>,
    transient_revision: u64,
    revoked: bool,
    session: Option<ActiveSession>,
}

struct ActiveSession {
    identity: CommandSession,
    ready: bool,
    close: watch::Sender<Option<CloseReason>>,
    outbound: mpsc::Sender<Outbound>,
    cancelled_threads: watch::Sender<()>,
    commands: Arc<Semaphore>,
    threads: Arc<Semaphore>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CloseReason {
    Replaced,
    EpochMismatch,
    Revoked,
    Shutdown,
    Retry,
    InvalidFrame,
    InvalidDataType,
    TooLarge,
}

impl CloseReason {
    pub(super) const fn frame(self) -> (u16, &'static str) {
        match self {
            Self::Replaced => (4001, "superseded_by_new_epoch"),
            Self::EpochMismatch => (4002, "epoch_mismatch"),
            Self::Revoked => (4003, "node_revoked"),
            Self::Shutdown => (1001, "hub_shutdown"),
            Self::Retry => (1011, "bridge_restart"),
            Self::InvalidFrame => (1002, "invalid_bridge_frame"),
            Self::InvalidDataType => (1003, "expected_bridge_text"),
            Self::TooLarge => (1009, "bridge_frame_too_large"),
        }
    }
}

#[derive(Clone)]
pub(super) struct SessionKey {
    pub(super) node_id: NodeId,
    pub(super) generation: u64,
    pub(super) epoch: u64,
}

impl SessionKey {
    pub(super) fn identity(&self) -> CommandSession {
        CommandSession {
            generation: self.generation,
            epoch: self.epoch,
        }
    }
}

impl LiveBridge {
    pub(crate) fn new(capacity: usize, clock: Arc<dyn MonoClock>) -> Self {
        Self {
            inner: Arc::new(Inner {
                registry: Mutex::new(Registry {
                    open: true,
                    nodes: HashMap::new(),
                    active_nodes: 0,
                }),
                tasks: watch::channel(0).0,
                capacity,
                clock,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Registry> {
        self.inner
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn clock(&self) -> &Arc<dyn MonoClock> {
        &self.inner.clock
    }

    pub(super) fn admit(&self, node_id: NodeId) -> Result<Admission, HubError> {
        let mut registry = self.lock();
        let already_counted = registry
            .nodes
            .get(&node_id)
            .is_some_and(|entry| entry.attempts != 0);
        if !registry.open || (!already_counted && registry.active_nodes >= self.inner.capacity) {
            return Err(HubError::new(
                ErrorCode::Overloaded,
                "The Hub cannot admit another bridge.",
            ));
        }
        let entry = registry.nodes.entry(node_id.clone()).or_default();
        if entry.revoked {
            return Err(HubError::new(
                ErrorCode::NodeRevoked,
                "The node was revoked.",
            ));
        }
        entry.attempts += 1;
        let authority = entry.authority.clone();
        let transient_revision = entry.transient_revision;
        if !already_counted {
            registry.active_nodes += 1;
        }
        self.inner.tasks.send_modify(|count| *count += 1);
        Ok(Admission {
            bridge: self.clone(),
            node_id,
            authority,
            transient_revision,
            registered_epoch: None,
        })
    }

    /// Serialize durable changes to a node's bridge identity with application
    /// writes. Keep the guard through publication of the new registry state.
    pub(crate) async fn transition(&self, node_id: &NodeId) -> AuthorityTransition {
        let authority = {
            let mut registry = self.lock();
            let entry = registry.nodes.entry(node_id.clone()).or_default();
            entry.transitions += 1;
            entry.authority.clone()
        };
        let reference = TransitionRef {
            bridge: self.clone(),
            node_id: node_id.clone(),
        };
        let exclusive = authority.write_owned().await;
        AuthorityTransition {
            _exclusive: exclusive,
            _reference: reference,
        }
    }

    fn reserve_slot(
        &self,
        active: &ActiveSession,
        authority: Option<OwnedRwLockReadGuard<()>>,
    ) -> Result<Reserved, ReserveError> {
        let thread = authority.is_some();
        let slots = if thread {
            &active.threads
        } else {
            &active.commands
        };
        let permit = slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReserveError::RateLimited)?;
        Ok(Reserved {
            permit,
            identity: active.identity,
            outbound: active.outbound.clone(),
            cancel: thread.then(|| active.cancelled_threads.clone()),
            clock: self.inner.clock.clone(),
            authority,
        })
    }

    async fn reserve_thread_in_session(&self, node_id: &NodeId) -> Result<Reserved, ReserveError> {
        let (authority, identity, mut closed) = {
            let registry = self.lock();
            let (entry, active) = registry
                .registered_ready(node_id)
                .ok_or(ReserveError::Offline)?;
            (
                entry.authority.clone(),
                active.identity,
                active.close.subscribe(),
            )
        };
        // A rebinding or revoke denies immediate writes, but may roll back.
        // Keep the scheduler's original trigger until this authority is
        // available again. Exit when this particular session closes.
        let guard = tokio::select! {
            biased;
            _ = closed.wait_for(Option::is_some) => return Err(ReserveError::Offline),
            guard = authority.read_owned() => guard,
        };
        let registry = self.lock();
        let (_, active) = registry
            .registered_ready(node_id)
            .ok_or(ReserveError::Offline)?;
        if active.identity != identity {
            return Err(ReserveError::Offline);
        }
        self.reserve_slot(active, Some(guard))
    }

    pub(super) fn ready(&self, key: &SessionKey) -> bool {
        let mut registry = self.lock();
        if !registry.open {
            return false;
        }
        let Some(active) = registry
            .nodes
            .get_mut(&key.node_id)
            .filter(|entry| !entry.revoked)
            .and_then(|entry| entry.session.as_mut())
            .filter(|active| active.identity == key.identity())
        else {
            return false;
        };
        active.ready = true;
        true
    }

    pub(super) fn stop_dispatch(&self, key: &SessionKey) {
        if let Some(active) = self
            .lock()
            .nodes
            .get_mut(&key.node_id)
            .and_then(|entry| entry.session.as_mut())
            .filter(|active| active.identity == key.identity())
        {
            active.ready = false;
        }
    }

    fn disconnect(&self, node_id: &NodeId, reason: CloseReason) {
        if let Some(entry) = self.lock().nodes.get_mut(node_id) {
            if reason == CloseReason::Revoked {
                entry.revoked = true;
            }
            entry.transient_revision = entry.transient_revision.saturating_add(1);
            if let Some(active) = entry.session.take() {
                active.close.send_replace(Some(reason));
            }
        }
    }
}

impl BridgeRegistry for LiveBridge {
    fn live(&self) -> Option<&Self> {
        Some(self)
    }

    fn is_online(&self, node_id: &NodeId) -> bool {
        self.lock().ready_session(node_id).is_some()
    }

    fn reserve(&self, node_id: &NodeId) -> Result<Box<dyn CommandPermit>, ReserveError> {
        let registry = self.lock();
        let active = registry
            .ready_session(node_id)
            .ok_or(ReserveError::Offline)?;
        Ok(Box::new(self.reserve_slot(active, None)?))
    }

    fn reserve_thread<'a>(
        &'a self,
        node_id: &'a NodeId,
    ) -> BoxFuture<'a, Result<Box<dyn ThreadPermit>, ReserveError>> {
        Box::pin(async move {
            let reserved = self.reserve_thread_in_session(node_id).await?;
            Ok(Box::new(reserved) as Box<dyn ThreadPermit>)
        })
    }

    fn disconnect_revoked(&self, node_id: &NodeId) {
        self.disconnect(node_id, CloseReason::Revoked);
    }

    fn disconnect_transient(&self, node_id: &NodeId) {
        self.disconnect(node_id, CloseReason::Retry);
    }

    fn shutdown(&self) {
        let mut registry = self.lock();
        registry.open = false;
        for node in registry.nodes.values_mut() {
            if let Some(active) = node.session.take() {
                active.close.send_replace(Some(CloseReason::Shutdown));
            }
        }
    }

    fn drain(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {
            let _ = self
                .inner
                .tasks
                .subscribe()
                .wait_for(|count| *count == 0)
                .await;
        })
    }
}

impl Registry {
    fn ready_session(&self, node_id: &NodeId) -> Option<&ActiveSession> {
        let (entry, active) = self.registered_ready(node_id)?;
        let _authority = entry.authority.try_read().ok()?;
        Some(active)
    }

    fn registered_ready(&self, node_id: &NodeId) -> Option<(&Entry, &ActiveSession)> {
        if !self.open {
            return None;
        }
        let entry = self.nodes.get(node_id).filter(|entry| !entry.revoked)?;
        let active = entry.session.as_ref().filter(|active| active.ready)?;
        Some((entry, active))
    }
}

pub(super) struct Admission {
    bridge: LiveBridge,
    node_id: NodeId,
    authority: Arc<RwLock<()>>,
    transient_revision: u64,
    registered_epoch: Option<u64>,
}

impl Admission {
    pub(super) fn authority(&self) -> Arc<RwLock<()>> {
        self.authority.clone()
    }

    /// The database has fenced the old epoch even if the HTTP connection
    /// fails before completing its upgrade.
    pub(super) fn claim(&self, epoch: u64) {
        if let Some(entry) = self.bridge.lock().nodes.get_mut(&self.node_id)
            && epoch > entry.allocated_epoch
        {
            entry.allocated_epoch = epoch;
            if entry
                .session
                .as_ref()
                .is_some_and(|session| session.identity.epoch < epoch)
                && let Some(active) = entry.session.take()
            {
                active.close.send_replace(Some(CloseReason::Replaced));
            }
        }
    }

    pub(super) fn register(
        &mut self,
        identity: CommandSession,
        close: watch::Sender<Option<CloseReason>>,
        outbound: mpsc::Sender<Outbound>,
        cancelled_threads: watch::Sender<()>,
    ) -> Result<(), CloseReason> {
        let mut registry = self.bridge.lock();
        if !registry.open {
            return Err(CloseReason::Shutdown);
        }
        let entry = registry
            .nodes
            .get_mut(&self.node_id)
            .ok_or(CloseReason::Retry)?;
        if entry.revoked {
            return Err(CloseReason::Revoked);
        }
        if entry.transient_revision != self.transient_revision {
            return Err(CloseReason::Retry);
        }
        if identity.epoch < entry.allocated_epoch || identity.epoch <= entry.high_water_epoch {
            return Err(CloseReason::Replaced);
        }
        entry.high_water_epoch = identity.epoch;
        self.registered_epoch = Some(identity.epoch);
        if let Some(previous) = entry.session.replace(ActiveSession {
            identity,
            ready: false,
            close,
            outbound,
            cancelled_threads,
            commands: Arc::new(Semaphore::new(COMMAND_SLOTS as usize)),
            threads: Arc::new(Semaphore::new(THREAD_SLOTS as usize)),
        }) {
            previous.close.send_replace(Some(CloseReason::Replaced));
        }
        Ok(())
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        let mut registry = self.bridge.lock();
        if let Some(entry) = registry.nodes.get_mut(&self.node_id) {
            if entry
                .session
                .as_ref()
                .is_some_and(|session| self.registered_epoch == Some(session.identity.epoch))
            {
                entry.session = None;
            }
            entry.attempts -= 1;
            if entry.attempts == 0 {
                let remove = entry.transitions == 0;
                registry.active_nodes -= 1;
                if remove {
                    registry.nodes.remove(&self.node_id);
                }
            }
        }
        self.bridge.inner.tasks.send_modify(|count| *count -= 1);
    }
}

pub(crate) struct AuthorityTransition {
    _exclusive: OwnedRwLockWriteGuard<()>,
    _reference: TransitionRef,
}

struct TransitionRef {
    bridge: LiveBridge,
    node_id: NodeId,
}

impl Drop for TransitionRef {
    fn drop(&mut self) {
        let mut registry = self.bridge.lock();
        if let Some(entry) = registry.nodes.get_mut(&self.node_id) {
            entry.transitions -= 1;
            if entry.transitions == 0 && entry.attempts == 0 {
                registry.nodes.remove(&self.node_id);
            }
        }
    }
}

struct Reserved {
    permit: OwnedSemaphorePermit,
    identity: CommandSession,
    outbound: mpsc::Sender<Outbound>,
    cancel: Option<watch::Sender<()>>,
    clock: Arc<dyn MonoClock>,
    authority: Option<OwnedRwLockReadGuard<()>>,
}

impl CommandPermit for Reserved {
    fn session(&self) -> CommandSession {
        self.identity
    }

    fn submit(
        self: Box<Self>,
        request: CommandReqPayload,
    ) -> BoxFuture<'static, Result<CommandResPayload, SubmitError>> {
        let deadline = self
            .clock
            .deadline_after(Duration::from_millis(request.admission_budget_ms));
        Box::pin(send(self, request, deadline, |request| {
            Outbound::Command(Box::new(request))
        }))
    }
}

impl ThreadPermit for Reserved {
    fn epoch(&self) -> u64 {
        self.identity.epoch
    }

    fn submit(
        mut self: Box<Self>,
        request: ThreadDeliverPayload,
    ) -> BoxFuture<'static, Result<ThreadAckPayload, SubmitError>> {
        let Some(authority) = self.authority.take() else {
            return Box::pin(std::future::ready(Err(SubmitError::Unavailable)));
        };
        let deadline = self
            .clock
            .deadline_after(Duration::from_millis(request.admission_budget_ms));
        Box::pin(send(self, request, deadline, |request| {
            Outbound::Thread(Box::new(request), authority)
        }))
    }
}

async fn send<T, P>(
    reserved: Box<Reserved>,
    payload: P,
    deadline: MonoInstant,
    make: impl FnOnce(Request<T, P>) -> Outbound,
) -> Result<T, SubmitError> {
    let (tx, rx) = oneshot::channel();
    let mut cancel = reserved.cancel.map(|wake| CancelWaiter {
        flag: Arc::new(AtomicBool::new(false)),
        wake: Some(wake),
    });
    reserved
        .outbound
        .try_send(make(Request {
            payload,
            deadline,
            reply: Reply {
                sender: Some(tx),
                submitted: false,
                permit: Some(reserved.permit),
                cancelled: cancel.as_ref().map(|waiter| waiter.flag.clone()),
            },
        }))
        .map_err(|_| SubmitError::Unavailable)?;
    let result = rx.await.unwrap_or(Err(SubmitError::Unknown));
    if let Some(cancel) = &mut cancel {
        cancel.wake = None;
    }
    result
}

struct CancelWaiter {
    flag: Arc<AtomicBool>,
    wake: Option<watch::Sender<()>>,
}

impl Drop for CancelWaiter {
    fn drop(&mut self) {
        if let Some(wake) = self.wake.take() {
            self.flag.store(true, Ordering::Release);
            wake.send_replace(());
        }
    }
}

pub(super) enum Outbound {
    Command(Box<Request<CommandResPayload, CommandReqPayload>>),
    Thread(
        Box<Request<ThreadAckPayload, ThreadDeliverPayload>>,
        OwnedRwLockReadGuard<()>,
    ),
}

pub(super) struct Request<T, P> {
    pub(super) payload: P,
    pub(super) deadline: MonoInstant,
    pub(super) reply: Reply<T>,
}

pub(super) struct Reply<T> {
    sender: Option<oneshot::Sender<Result<T, SubmitError>>>,
    submitted: bool,
    permit: Option<OwnedSemaphorePermit>,
    cancelled: Option<Arc<AtomicBool>>,
}

impl<T> Reply<T> {
    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    pub(super) fn in_progress(&mut self) {
        self.submitted = true;
    }

    pub(super) fn complete(mut self, value: T) {
        drop(self.permit.take());
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Ok(value));
        }
    }
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(if self.submitted {
                SubmitError::Unknown
            } else {
                SubmitError::Unavailable
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use cooplet_platform::MonotonicClock;

    use super::*;

    fn session(epoch: u64) -> CommandSession {
        CommandSession {
            generation: 1,
            epoch,
        }
    }

    #[tokio::test]
    async fn delayed_upgrade_cannot_displace_higher_epoch_or_survive_revocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let bridge = LiveBridge::new(1, Arc::new(MonotonicClock));
        let node_id = NodeId::generate()?;
        let mut early = bridge.admit(node_id.clone())?;
        let mut late = bridge.admit(node_id.clone())?;
        early.claim(1);
        late.claim(2);
        let (close_late, closed_late) = watch::channel(None);
        let (outbound_late, _received_late) = mpsc::channel(SESSION_QUEUE);
        let (cancel, _) = watch::channel(());
        assert_eq!(
            late.register(session(2), close_late, outbound_late, cancel.clone()),
            Ok(())
        );
        assert!(!bridge.is_online(&node_id));
        assert!(bridge.ready(&SessionKey {
            node_id: node_id.clone(),
            generation: 1,
            epoch: 2
        }));

        let (close_early, _) = watch::channel(None);
        let (outbound_early, _) = mpsc::channel(SESSION_QUEUE);
        assert_eq!(
            early.register(session(1), close_early, outbound_early, cancel.clone()),
            Err(CloseReason::Replaced)
        );
        drop(early);
        assert_eq!(*closed_late.borrow(), None);
        assert!(bridge.is_online(&node_id));

        let mut racing = bridge.admit(node_id.clone())?;
        bridge.disconnect_revoked(&node_id);
        let (close_racing, _) = watch::channel(None);
        let (outbound_racing, _) = mpsc::channel(SESSION_QUEUE);
        assert_eq!(
            racing.register(session(3), close_racing, outbound_racing, cancel),
            Err(CloseReason::Revoked)
        );
        assert_eq!(*closed_late.borrow(), Some(CloseReason::Revoked));
        assert!(!bridge.is_online(&node_id));
        assert_eq!(
            bridge.admit(node_id).err().map(|err| err.problem().code),
            Some(ErrorCode::NodeRevoked)
        );
        drop(racing);
        drop(late);
        bridge.drain().await;
        Ok(())
    }

    #[tokio::test]
    async fn dispatch_slots_are_independent_and_released_with_the_reservation()
    -> Result<(), Box<dyn std::error::Error>> {
        let bridge = LiveBridge::new(1, Arc::new(MonotonicClock));
        let node_id = NodeId::generate()?;
        let mut admission = bridge.admit(node_id.clone())?;
        admission.claim(7);
        let (close, closed) = watch::channel(None);
        let (outbound, _incoming) = mpsc::channel(SESSION_QUEUE);
        let (cancel, _) = watch::channel(());
        assert_eq!(
            admission.register(session(7), close, outbound, cancel),
            Ok(())
        );
        assert!(matches!(
            bridge.reserve(&node_id),
            Err(ReserveError::Offline)
        ));
        assert!(bridge.ready(&SessionKey {
            node_id: node_id.clone(),
            generation: 1,
            epoch: 7
        }));

        let mut commands = Vec::new();
        for _ in 0..COMMAND_SLOTS {
            if let Ok(permit) = bridge.reserve(&node_id) {
                commands.push(permit);
            }
        }
        assert_eq!(commands.len(), COMMAND_SLOTS as usize);
        assert!(matches!(
            bridge.reserve(&node_id),
            Err(ReserveError::RateLimited)
        ));
        let mut threads = Vec::new();
        for _ in 0..THREAD_SLOTS {
            if let Ok(permit) = bridge.reserve_thread(&node_id).await {
                assert_eq!(permit.epoch(), 7);
                threads.push(permit);
            }
        }
        assert_eq!(threads.len(), THREAD_SLOTS as usize);
        assert!(matches!(
            bridge.reserve_thread(&node_id).await,
            Err(ReserveError::RateLimited)
        ));
        drop(commands.pop());
        assert!(bridge.reserve(&node_id).is_ok());
        bridge.shutdown();
        assert_eq!(*closed.borrow(), Some(CloseReason::Shutdown));
        assert!(matches!(
            bridge.reserve(&node_id),
            Err(ReserveError::Offline)
        ));
        assert!(matches!(
            bridge.reserve_thread(&node_id).await,
            Err(ReserveError::Offline)
        ));
        drop(admission);
        bridge.drain().await;
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_a_waiter_marks_its_wire_request_without_a_notification_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let bridge = LiveBridge::new(1, Arc::new(MonotonicClock));
        let node_id = NodeId::generate()?;
        let mut admission = bridge.admit(node_id.clone())?;
        let (close, _closed) = watch::channel(None);
        let (outbound, mut incoming) = mpsc::channel(SESSION_QUEUE);
        let (cancel, mut cancelled) = watch::channel(());
        admission
            .register(session(7), close, outbound, cancel)
            .map_err(|_| "could not register test bridge")?;
        assert!(bridge.ready(&SessionKey {
            node_id: node_id.clone(),
            generation: 1,
            epoch: 7,
        }));
        let permit = bridge
            .reserve_thread(&node_id)
            .await
            .map_err(|_| "could not reserve a thread slot")?;
        let mut submit = permit.submit(ThreadDeliverPayload {
            delivery_generation: 0,
            delivery_id: cooplet_protocol::OperationId::generate()?,
            thread_id: cooplet_protocol::ThreadId::generate()?,
            member_id: cooplet_protocol::ParticipantId::generate()?,
            from_seq: 1,
            to_seq: 1,
            posts: Vec::new(),
            truncated_earlier: false,
            delivery_mode: cooplet_protocol::DeliveryMode::QueueUntilIdle,
            admission_budget_ms: 5000,
        });
        std::future::poll_fn(|cx| {
            assert!(submit.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let Outbound::Thread(request, _authority) =
            incoming.recv().await.ok_or("no queued delivery")?
        else {
            return Err("unexpected queued command".into());
        };
        assert!(!request.reply.is_cancelled());
        drop(submit);
        cancelled.changed().await?;
        assert!(request.reply.is_cancelled());
        Ok(())
    }
}
