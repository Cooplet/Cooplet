use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddrV4,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use cooplet_protocol::{ErrorCode, NodeId, ParticipantId};
use cooplet_storage::{DbActor, ReadPool};
use tokio::sync::watch;

use crate::{
    auth::Auth,
    error::{NodeError, Result},
    operations::OperationBook,
};

use super::AdapterRegistry;

mod approvals;
mod lifecycle;
mod participants;
mod processes;
mod sessions;
mod state;
mod stop;

pub use approvals::ApprovalResponse;
#[cfg(test)]
pub(crate) use lifecycle::ACTIVE_NAME_EXISTS_QUERY;
pub use lifecycle::{AgentResponse, CreateAgentRequest};
pub use participants::{ParticipantPage, ParticipantView};
pub use sessions::{LiveSession, SlotDenial, SubmitSlot};
pub use state::apply_submit_outcome;
pub use stop::{StopAgentRequest, StopAgentResponse, StopDisposition};

#[derive(Clone)]
pub struct AgentConfig {
    pub node_id: NodeId,
    pub endpoint: SocketAddrV4,
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
}

pub trait BootIdProvider: Send + Sync {
    fn current(&self) -> Option<String>;
}

struct SystemBootId;

impl BootIdProvider for SystemBootId {
    fn current(&self) -> Option<String> {
        cooplet_platform::current_linux_boot_id()
    }
}

type SessionKey = (ParticipantId, u64);
type QueueWakeup = Arc<dyn Fn(ParticipantId) + Send + Sync>;

#[derive(Default)]
struct Sessions {
    live: HashMap<ParticipantId, Arc<LiveSession>>,
    processes: HashMap<SessionKey, processes::ManagedProcess>,
    mutations: HashSet<ParticipantId>,
}

struct Service {
    db: DbActor,
    reads: ReadPool,
    auth: Auth,
    registry: AdapterRegistry,
    operations: OperationBook,
    config: AgentConfig,
    boot: Arc<dyn BootIdProvider>,
    sessions: Mutex<Sessions>,
    #[cfg(target_os = "linux")]
    exit_monitor: processes::ExitMonitor,
    stopping: watch::Sender<bool>,
    queue_wakeup: OnceLock<QueueWakeup>,
}

#[derive(Clone)]
pub struct AgentService(Arc<Service>);

impl AgentService {
    pub fn new(
        db: DbActor,
        reads: ReadPool,
        auth: Auth,
        registry: AdapterRegistry,
        operations: OperationBook,
        config: AgentConfig,
    ) -> Self {
        Self::with_boot_id(
            db,
            reads,
            auth,
            registry,
            operations,
            config,
            Arc::new(SystemBootId),
        )
    }

    pub fn with_boot_id(
        db: DbActor,
        reads: ReadPool,
        auth: Auth,
        registry: AdapterRegistry,
        operations: OperationBook,
        config: AgentConfig,
        boot: Arc<dyn BootIdProvider>,
    ) -> Self {
        Self(Arc::new(Service {
            db,
            reads,
            auth,
            registry,
            operations,
            config,
            boot,
            sessions: Mutex::new(Sessions::default()),
            #[cfg(target_os = "linux")]
            exit_monitor: processes::ExitMonitor::default(),
            stopping: watch::channel(false).0,
            queue_wakeup: OnceLock::new(),
        }))
    }

    pub fn set_queue_wakeup(&self, wakeup: QueueWakeup) -> Result<()> {
        self.0
            .queue_wakeup
            .set(wakeup)
            .map_err(|_| NodeError::internal("The queue driver is already installed"))
    }

    pub fn live_session(&self, id: &ParticipantId) -> Option<Arc<LiveSession>> {
        lock(&self.0.sessions).live.get(id).cloned()
    }

    /// Close native input and event drivers while retaining process ownership.
    pub fn quiesce(&self) {
        let sessions = lock(&self.0.sessions);
        self.0.stopping.send_replace(true);
        for live in sessions.live.values() {
            live.disconnect();
        }
        #[cfg(target_os = "linux")]
        self.0.exit_monitor.clear();
    }

    async fn cancel_on_shutdown<T>(&self, action: impl Future<Output = Result<T>>) -> Result<T> {
        let mut stopping = self.0.stopping.subscribe();
        tokio::select! {
            biased;
            _ = stopping.wait_for(|stopping| *stopping) => {
                Err(NodeError::new(ErrorCode::Overloaded, "Node is shutting down")
                    .with_extension("reason", "node_stopping"))
            }
            result = action => result,
        }
    }

    fn mutation(&self, id: &ParticipantId) -> Result<MutationLease> {
        let acquired = lock(&self.0.sessions).mutations.insert(id.clone());
        if !acquired {
            return Err(busy("operation_in_flight", true));
        }
        Ok(MutationLease {
            service: self.clone(),
            id: id.clone(),
        })
    }

    fn disconnect(&self, id: &ParticipantId, generation: u64) {
        let mut sessions = lock(&self.0.sessions);
        if sessions
            .live
            .get(id)
            .is_some_and(|live| live.generation() == generation)
            && let Some(live) = sessions.live.remove(id)
        {
            live.disconnect();
        }
        #[cfg(target_os = "linux")]
        self.0.exit_monitor.unwatch(&(id.clone(), generation));
    }
}

struct MutationLease {
    service: AgentService,
    id: ParticipantId,
}

impl Drop for MutationLease {
    fn drop(&mut self) {
        lock(&self.service.0.sessions).mutations.remove(&self.id);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn invalid(reason: &'static str, detail: impl Into<String>) -> NodeError {
    NodeError::new(ErrorCode::InvalidRequest, detail).with_extension("reason", reason)
}

fn busy(reason: &'static str, retryable: bool) -> NodeError {
    NodeError::new(
        ErrorCode::ParticipantBusy,
        "The participant cannot accept this operation",
    )
    .with_extension("reason", reason)
    .retryable(retryable)
}

fn not_found() -> NodeError {
    NodeError::new(ErrorCode::NotFound, "Participant not found")
}

fn unknown(reason: &'static str) -> NodeError {
    NodeError::new(
        ErrorCode::DeliveryUnknown,
        "The participant operation outcome is unknown",
    )
    .with_extension("reason", reason)
}

fn bad_data(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}
