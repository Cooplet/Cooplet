//! Native client contract. The Node selects a registered adapter by its opaque kind.

mod binding;
pub mod codex;
mod pi;
mod registry;
mod runtime;
mod templates;
mod turn_input;

pub use binding::*;
pub use cooplet_protocol::{ApprovalAction, ApprovalDecision, ApprovalWait, ApprovalWaitKind};
pub use pi::PiAdapter;
pub use registry::AdapterRegistry;
pub use runtime::*;
pub(crate) use templates::list_template_summaries;
pub use templates::{
    AgentOverrides, AgentTemplate, EffectiveAgentOptions, list_templates, load_template,
    load_template_options,
};
pub use turn_input::*;

use std::{
    any::Any,
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{ChildStderr, ChildStdin, ChildStdout, Command},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use cooplet_platform::{AdmissionGuard, ProcessIdentity};
use cooplet_protocol::{ErrorCode, MessageId, ParticipantId, ParticipantState};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use tokio::sync::mpsc;
use utoipa::ToSchema;

use crate::error::{NodeError, Result};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NativeSessionKey {
    pub client_kind: String,
    pub store_namespace: String,
    pub native_session_id: String,
}

impl NativeSessionKey {
    pub fn validate(&self, expected_client_kind: &str) -> Result<()> {
        if self.client_kind != expected_client_kind
            || self.client_kind.trim().is_empty()
            || self.store_namespace.trim().is_empty()
            || self.native_session_id.trim().is_empty()
        {
            return Err(NodeError::new(
                ErrorCode::InvalidRequest,
                "The adapter returned an invalid native session key",
            )
            .with_extension("reason", "invalid_native_session_key"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default, deny_unknown_fields)]
pub struct AdapterCapabilities {
    pub supports_create: bool,
    pub supports_resume: bool,
    pub supports_steer_while_working: bool,
    pub supports_structured_output: bool,
    pub supports_approval_observe: bool,
    pub supports_remote_approval: bool,
    pub supports_tui_attach: bool,
}

#[derive(Clone, Debug)]
pub struct AdapterConfig {
    pub state_dir: PathBuf,
    pub runtime_dir: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DiscoveryReport {
    pub client_kind: String,
    pub available: bool,
    #[serde(serialize_with = "serialize_display_path")]
    pub binary_path: Option<PathBuf>,
    pub version: Option<String>,
    pub capabilities: AdapterCapabilities,
    pub detail: Option<String>,
}

fn serialize_display_path<S: Serializer>(
    path: &Option<PathBuf>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    path.as_deref()
        .map(Path::to_string_lossy)
        .serialize(serializer)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessOwnership {
    #[default]
    Managed,
    Attached,
}

impl ProcessOwnership {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Managed => "managed",
            Self::Attached => "attached",
        }
    }
}

impl FromStr for ProcessOwnership {
    type Err = cooplet_protocol::ValidationError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "managed" => Ok(Self::Managed),
            "attached" => Ok(Self::Attached),
            _ => Err(cooplet_protocol::ValidationError("process ownership")),
        }
    }
}

pub struct SpawnedManagedProcess {
    pub identity: ProcessIdentity,
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnManagedError {
    #[error("The managed process was already spawned for this open")]
    AlreadySpawned,
    #[error("Admission expired before launching the managed process")]
    AdmissionExpired,
    #[error("Could not spawn the managed process: {0}")]
    SpawnFailed(String),
    #[error("The managed process could not be verified clean: {0}")]
    CleanupPending(String),
    #[error("Could not persist the managed process identity: {0}")]
    PersistenceFailed(String),
}

pub trait ManagedProcessSpawner: Send + Sync {
    fn spawn(
        &self,
        command: Command,
    ) -> BoxFuture<'_, std::result::Result<SpawnedManagedProcess, SpawnManagedError>>;
}

#[derive(Clone, Debug)]
pub enum SessionOpenMode {
    Create,
    Resume {
        native_key: NativeSessionKey,
        previous_binding: Value,
    },
}

/// Filesystem preparation may outlive a cancelled adapter future. The opener waits
/// for it before cleaning the generation and rolls back a new reservation only
/// after the process owner and database confirm that the open failed.
#[derive(Clone, Default)]
pub struct OpenPreparation {
    rollback: Arc<tokio::sync::Mutex<Option<OpenRollback>>>,
    before_managed_launch: Arc<AtomicBool>,
}

pub type OpenRollback = Box<dyn FnOnce() -> std::io::Result<()> + Send>;

impl OpenPreparation {
    pub async fn run<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Option<OpenRollback>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let mut rollback = self.rollback.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || job(&mut rollback))
            .await
            .map_err(NodeError::internal)?
    }

    /// The adapter awaits this preparation directly, before it can request a
    /// managed launch or cause any other native effect in this open attempt.
    pub(crate) async fn run_before_managed_launch<T: Send + 'static>(
        &self,
        job: impl FnOnce(&mut Option<OpenRollback>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        struct Waiting<'a>(&'a AtomicBool);

        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        self.before_managed_launch.store(true, Ordering::Release);
        let _waiting = Waiting(&self.before_managed_launch);
        self.run(job).await
    }

    fn awaiting_managed_preparation(&self) -> bool {
        self.before_managed_launch.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        drop(self.rollback.lock().await);
    }

    async fn undo(&self) {
        if let Some(cleanup) = self.rollback.lock().await.take() {
            match tokio::task::spawn_blocking(cleanup).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(%error, "Could not release a native session reservation")
                }
                Err(error) => tracing::warn!(%error, "Native session rollback task failed"),
            }
        }
    }
}

pub struct SessionOpenRequest {
    pub preparation: OpenPreparation,
    pub participant_id: ParticipantId,
    pub generation: u64,
    pub mode: SessionOpenMode,
    pub workspace_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub cred_file_path: PathBuf,
    pub template_id: Option<String>,
    pub effective_options: EffectiveAgentOptions,
    /// Core-composed collaboration rules plus optional template instructions, frozen at create.
    /// Adapters must append this to their CLI's native base system prompt and never replace it.
    pub instructions: Option<String>,
    pub spawner: Arc<dyn ManagedProcessSpawner>,
}

pub struct OpenedSession {
    pub native_key: NativeSessionKey,
    pub process_ownership: ProcessOwnership,
    pub handle: SessionHandle,
    pub capabilities: AdapterCapabilities,
    pub initial_state: ParticipantRuntimeState,
    pub adapter_binding: Value,
    pub events: mpsc::Receiver<AdapterEvent>,
}

#[derive(Clone, Debug)]
pub struct PersistedSessionRef {
    pub participant_id: ParticipantId,
    pub generation: u64,
    pub native_key: Option<NativeSessionKey>,
    pub process_ownership: ProcessOwnership,
    pub workspace_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub process_identity: Option<ProcessIdentity>,
    pub adapter_binding: Value,
}

/// Opaque live transport for one generation. The adapter controls the value
/// behind the type erasure, and the Node owns process lifetime separately.
#[derive(Clone)]
pub struct SessionHandle {
    transport: Arc<dyn Any + Send + Sync>,
    persisted: PersistedSessionRef,
}

impl SessionHandle {
    pub fn new<T: Any + Send + Sync>(transport: T, persisted: PersistedSessionRef) -> Self {
        Self {
            transport: Arc::new(transport),
            persisted,
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.transport.downcast_ref()
    }

    pub fn persisted(&self) -> &PersistedSessionRef {
        &self.persisted
    }

    pub(crate) fn with_persisted(mut self, persisted: PersistedSessionRef) -> Self {
        self.persisted = persisted;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInspection {
    pub state: ParticipantRuntimeState,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParticipantRuntimeState {
    Starting,
    Idle,
    Working,
    AwaitingApproval(ApprovalWait),
    Stopping,
    Stopped,
    Unavailable { reason: String },
    Unknown { reason: String },
}

impl ParticipantRuntimeState {
    pub const fn state(&self) -> ParticipantState {
        match self {
            Self::Starting => ParticipantState::Starting,
            Self::Idle => ParticipantState::Idle,
            Self::Working => ParticipantState::Working,
            Self::AwaitingApproval(_) => ParticipantState::AwaitingApproval,
            Self::Stopping => ParticipantState::Stopping,
            Self::Stopped => ParticipantState::Stopped,
            Self::Unavailable { .. } => ParticipantState::Unavailable,
            Self::Unknown { .. } => ParticipantState::Unknown,
        }
    }
}

pub struct SubmitTurnRequest {
    /// Node-wide turn correlation echoed in completion events. For a message this is its ID.
    pub operation_key: String,
    pub message_id: Option<MessageId>,
    pub input: TurnInput,
    pub output_schema: Option<Value>,
    pub steer: bool,
    pub admission: AdmissionGuard,
}

#[derive(Clone, Debug)]
pub enum SubmitOutcome {
    /// The native client acknowledged intake. Completion arrives on the event channel.
    Accepted {
        native_turn_id: Option<String>,
    },
    Busy,
    AwaitingApproval(ApprovalWait),
    /// The first native write was prevented by the admission deadline.
    Expired,
    /// The adapter can prove that the native client did not accept the input.
    Unavailable(String),
    /// A native write may have started, and the adapter could not verify its outcome.
    Unknown(String),
}

#[derive(Clone, Debug)]
pub enum ApprovalRespondOutcome {
    Accepted,
    Unsupported(String),
    Stale(String),
    Unavailable(String),
    Unknown(String),
}

#[derive(Clone, Debug)]
pub enum StopMode {
    /// Check admission immediately before the first possible interrupt write.
    CancelTurn {
        admission: AdmissionGuard,
    },
    StopSession,
}

#[derive(Clone, Debug)]
pub enum StopOutcome {
    Stopped,
    TurnCancelled,
    Unsupported(String),
    /// No interrupt was accepted because of a temporary condition.
    Unavailable(String),
    AlreadyGone,
    Unknown(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum TuiAttachPlan {
    Exec {
        #[schema(value_type = String)]
        program: PathBuf,
        args: Vec<String>,
        #[schema(value_type = Option<String>)]
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<PathBuf>,
        env: BTreeMap<String, String>,
    },
    Unsupported {
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct AdapterEvent {
    /// Strictly increases in each opened session, starting at one.
    pub event_seq: u64,
    pub kind: AdapterEventKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TurnFinishStatus {
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug)]
pub enum AdapterEventKind {
    StateChanged(ParticipantRuntimeState),
    ApprovalRequested(ApprovalWait),
    TurnCompleted {
        native_turn_id: Option<String>,
        operation_key: Option<String>,
        status: TurnFinishStatus,
        text: String,
        structured: Option<Value>,
    },
    ProcessExited {
        exit_code: Option<i32>,
    },
}

pub trait ClientAdapter: Send + Sync {
    fn kind(&self) -> &'static str;

    fn discover<'a>(&'a self, cfg: &'a AdapterConfig) -> BoxFuture<'a, Result<DiscoveryReport>>;

    fn capabilities(&self) -> AdapterCapabilities;

    fn create_or_resume(&self, request: SessionOpenRequest)
    -> BoxFuture<'_, Result<OpenedSession>>;

    fn inspect_state<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>>;

    /// Return as soon as intake is acknowledged. Check admission immediately
    /// before the first native write and do not retry a possible write.
    fn submit_turn<'a>(
        &'a self,
        handle: &'a SessionHandle,
        request: SubmitTurnRequest,
    ) -> BoxFuture<'a, SubmitOutcome>;

    /// Receives the native approval ID supplied by this adapter.
    fn respond_approval<'a>(
        &'a self,
        handle: &'a SessionHandle,
        decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome>;

    /// Admit a delegated approval immediately before the first possible native
    /// write. Return `None` only when expiry prevented that write. An operation
    /// that may have written must report its outcome even after expiry.
    /// Adapters that queue a response override this method and carry the guard
    /// to the task that owns the writer.
    fn respond_approval_with_admission<'a>(
        &'a self,
        handle: &'a SessionHandle,
        decision: ApprovalDecision,
        admission: &'a AdmissionGuard,
    ) -> BoxFuture<'a, Option<ApprovalRespondOutcome>> {
        Box::pin(async move {
            if admission.is_expired() {
                return None;
            }
            Some(self.respond_approval(handle, decision).await)
        })
    }

    fn cancel_or_stop<'a>(
        &'a self,
        handle: &'a SessionHandle,
        mode: StopMode,
    ) -> BoxFuture<'a, Result<StopOutcome>>;

    fn tui_attach_plan<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>>;
}
