mod binding;
mod events;
mod execution;
mod reverse;
mod roster;
mod session;

use std::{
    path::PathBuf,
    sync::{Arc, Mutex as SyncMutex},
    time::Duration,
};

use cooplet_platform::{AdmissionGuard, MonoClock, MonoInstant, MonotonicClock, random_bytes};
use cooplet_protocol::{ErrorCode, NodeId};
use cooplet_storage::{DbActor, ReadPool};
use tokio::{
    sync::{Mutex, watch},
    task::{JoinHandle, JoinSet},
};

use crate::{
    error::{NodeError, Result},
    http::NodeState,
};

pub(crate) use binding::HUB_FILE;
use binding::{BindingContext, BindingState, binding_fault};
pub use binding::{EnrollRequest, EnrollResponse, EnrollmentStatus, HubConnection};
use reverse::ReverseHandle;

const RECONNECT_BACKOFF_MS: [u64; 6] = [200, 500, 1000, 2000, 5000, 15000];

pub struct NodeBridge {
    context: Option<BindingContext>,
    clock: Arc<dyn MonoClock>,
    binding: watch::Sender<BindingState>,
    enrollment: Mutex<()>,
    shutdown: watch::Sender<bool>,
    session: watch::Sender<Option<ReverseHandle>>,
    terminal: watch::Sender<Option<ErrorCode>>,
    task: SyncMutex<Option<JoinHandle<()>>>,
    workers: SyncMutex<JoinSet<()>>,
}

impl Default for NodeBridge {
    fn default() -> Self {
        let (binding, _) = watch::channel(BindingState::Unbound);
        let (shutdown, _) = watch::channel(false);
        let (session, _) = watch::channel(None);
        let (terminal, _) = watch::channel(None);
        Self {
            context: None,
            clock: Arc::new(MonotonicClock),
            binding,
            enrollment: Mutex::new(()),
            shutdown,
            session,
            terminal,
            task: SyncMutex::new(None),
            workers: SyncMutex::new(JoinSet::new()),
        }
    }
}

impl NodeBridge {
    pub(crate) async fn load(
        local_id: NodeId,
        state_dir: PathBuf,
        db: DbActor,
        reads: ReadPool,
    ) -> Arc<Self> {
        let context = BindingContext::new(local_id, state_dir, db, reads);
        let binding = context.load().await;
        let bridge = Arc::new(Self {
            context: Some(context),
            ..Self::default()
        });
        bridge.binding.send_replace(binding);
        bridge
    }

    pub fn connection(&self) -> Result<Arc<HubConnection>> {
        if *self.shutdown.borrow() {
            return Err(offline());
        }
        if let Some(code) = *self.terminal.borrow() {
            return Err(NodeError::new(code, code.title()));
        }
        match &*self.binding.borrow() {
            BindingState::Bound(hub) => Ok(hub.clone()),
            BindingState::Fault => Err(binding_fault()),
            BindingState::Unbound => Err(offline()),
        }
    }

    pub(crate) fn start(self: &Arc<Self>, state: NodeState, workspace_dir: PathBuf) {
        let mut task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if task.is_none() && !*self.shutdown.borrow() {
            let bridge = Arc::downgrade(self);
            *task = Some(tokio::spawn(async move {
                supervise(&state, workspace_dir, bridge).await;
            }));
        }
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.send_replace(true);
        self.session.send_replace(None);
    }

    pub(crate) async fn drain(&self) {
        let task = self
            .task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(task) = task
            && task.await.is_err()
        {
            tracing::warn!("Node bridge supervisor terminated unexpectedly");
        }
        let mut workers = std::mem::take(
            &mut *self
                .workers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        while let Some(result) = workers.join_next().await {
            if result.is_err() {
                tracing::warn!("Node bridge worker terminated unexpectedly");
            }
        }
    }

    fn spawn_worker(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut workers = self
            .workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some(result) = workers.try_join_next() {
            if result.is_err() {
                tracing::warn!("Node bridge worker terminated unexpectedly");
            }
        }
        workers.spawn(task);
    }
}

async fn supervise(state: &NodeState, workspace_dir: PathBuf, bridge: std::sync::Weak<NodeBridge>) {
    let Some(bridge) = bridge.upgrade() else {
        return;
    };
    let mut stop = bridge.shutdown.subscribe();
    let mut binding = bridge.binding.subscribe();
    let hub = loop {
        let snapshot = binding.borrow_and_update().clone();
        if let BindingState::Bound(hub) = snapshot {
            break hub;
        }
        tokio::select! {
            _ = stop.wait_for(|value| *value) => return,
            next = binding.changed() => if next.is_err() { return; },
        }
    };

    let limits = session::WorkerLimits::default();
    let mut attempts = 0;
    while !*stop.borrow() {
        let (ready, exit) =
            session::run(&bridge, state, &workspace_dir, &hub, &limits, &stop).await;
        bridge.session.send_replace(None);
        if let Some(code) = exit.terminal_code() {
            bridge.terminal.send_replace(Some(code));
            tracing::warn!(%code, "Node bridge reconnect halted");
            return;
        }
        if *stop.borrow() {
            return;
        }
        if ready {
            attempts = 0;
        }
        tracing::debug!(reason = exit.reason(), "Node bridge disconnected");
        let pause = jittered_backoff(attempts);
        attempts = (attempts + 1).min(RECONNECT_BACKOFF_MS.len() - 1);
        tokio::select! {
            _ = stop.wait_for(|value| *value) => return,
            () = tokio::time::sleep(pause) => {}
        }
    }
}

fn jittered_backoff(attempt: usize) -> Duration {
    let base = RECONNECT_BACKOFF_MS[attempt];
    let margin = base / 5;
    let lower = (base - margin).max(RECONNECT_BACKOFF_MS[0]);
    let upper = (base + margin).min(RECONNECT_BACKOFF_MS[RECONNECT_BACKOFF_MS.len() - 1]);
    let mut entropy = [0; 2];
    let milliseconds = if random_bytes(&mut entropy).is_ok() {
        lower + u64::from(u16::from_ne_bytes(entropy)) % (upper - lower + 1)
    } else {
        base
    };
    Duration::from_millis(milliseconds)
}

fn remaining(admission: &AdmissionGuard) -> Option<Duration> {
    admission.remaining_ms().map(Duration::from_millis)
}

async fn wait_stopped(stop: &watch::Receiver<bool>) {
    let _ = stop.clone().wait_for(|value| *value).await;
}

async fn wait_deadline(clock: &dyn MonoClock, deadline: Option<MonoInstant>) {
    if let Some(deadline) = deadline {
        loop {
            let Some(remaining) = clock.remaining_ms(deadline) else {
                return;
            };
            tokio::time::sleep(Duration::from_millis(remaining)).await;
        }
    }
    std::future::pending::<()>().await;
}

fn offline() -> NodeError {
    NodeError::new(
        ErrorCode::BridgeUnavailable,
        "The Node has no active Hub bridge",
    )
}

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
