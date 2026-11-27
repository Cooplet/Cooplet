//! Controls a vendor-independent transport at the boundary where native effects can occur.
#![allow(
    dead_code,
    reason = "Each integration-test binary uses a different subset of the fixture"
)]

use std::{
    collections::{BTreeMap, HashMap},
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use cooplet_node::{
    clients::{
        AdapterCapabilities, AdapterConfig, AdapterEvent, AdapterEventKind, ApprovalDecision,
        ApprovalRespondOutcome, ClientAdapter, DiscoveryReport, EffectiveAgentOptions,
        NativeSessionKey, OpenedSession, ParticipantRuntimeState, PersistedSessionRef,
        ProcessOwnership, SessionHandle, SessionInspection, SessionOpenMode, SessionOpenRequest,
        StopMode, StopOutcome, SubmitOutcome, SubmitTurnRequest, TuiAttachPlan,
    },
    error::{NodeError, Result},
};
use cooplet_protocol::ParticipantId;
use futures_util::future::BoxFuture;
use serde_json::json;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct MockAdapter {
    state: Arc<MockState>,
}

type DiscoveryBarrier = (oneshot::Sender<()>, oneshot::Receiver<()>);

struct MockState {
    sessions: Mutex<HashMap<(ParticipantId, u64), MockSession>>,
    stop_modes: Mutex<Vec<StopMode>>,
    discoveries: AtomicUsize,
    discovery_barrier: Mutex<Option<DiscoveryBarrier>>,
    fail_next_open: AtomicBool,
    open_calls: AtomicUsize,
    submit_calls: AtomicUsize,
    effects: Arc<AtomicUsize>,
    submits_sent: mpsc::UnboundedSender<MockSubmit>,
    submits: AsyncMutex<mpsc::UnboundedReceiver<MockSubmit>>,
    approvals_sent: mpsc::UnboundedSender<MockApproval>,
    approvals: AsyncMutex<mpsc::UnboundedReceiver<MockApproval>>,
}

#[derive(Clone)]
pub struct MockSession {
    pub target: PersistedSessionRef,
    pub template_id: Option<String>,
    pub options: EffectiveAgentOptions,
    pub instructions: Option<String>,
    pub credential: PathBuf,
    native: Arc<MockNative>,
}

struct MockNative {
    event_seq: AtomicU64,
    events: Mutex<Option<mpsc::Sender<AdapterEvent>>>,
}

pub struct MockSubmit {
    pub participant_id: ParticipantId,
    pub generation: u64,
    pub request: SubmitTurnRequest,
    effects: Arc<AtomicUsize>,
    result: oneshot::Sender<SubmitOutcome>,
}

pub struct MockApproval {
    pub participant_id: ParticipantId,
    pub generation: u64,
    pub decision: ApprovalDecision,
    result: oneshot::Sender<ApprovalRespondOutcome>,
}

impl Default for MockAdapter {
    fn default() -> Self {
        let (submits_sent, submits) = mpsc::unbounded_channel();
        let (approvals_sent, approvals) = mpsc::unbounded_channel();
        Self {
            state: Arc::new(MockState {
                sessions: Mutex::default(),
                stop_modes: Mutex::default(),
                discoveries: 0.into(),
                discovery_barrier: Mutex::default(),
                fail_next_open: AtomicBool::new(false),
                open_calls: 0.into(),
                submit_calls: 0.into(),
                effects: Arc::new(0.into()),
                submits_sent,
                submits: submits.into(),
                approvals_sent,
                approvals: approvals.into(),
            }),
        }
    }
}

impl MockAdapter {
    pub fn fail_next_open(&self) {
        self.state.fail_next_open.store(true, Ordering::Release);
    }

    pub fn session(&self, id: &ParticipantId, generation: u64) -> Option<MockSession> {
        lock(&self.state.sessions)
            .get(&(id.clone(), generation))
            .cloned()
    }

    pub fn submits(&self) -> usize {
        self.state.effects.load(Ordering::SeqCst)
    }

    pub fn submit_calls(&self) -> usize {
        self.state.submit_calls.load(Ordering::SeqCst)
    }

    pub fn open_calls(&self) -> usize {
        self.state.open_calls.load(Ordering::SeqCst)
    }

    pub fn hold_discovery(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, observed) = oneshot::channel();
        let (release, hold) = oneshot::channel();
        *lock(&self.state.discovery_barrier) = Some((entered, hold));
        (observed, release)
    }

    pub fn discoveries(&self) -> usize {
        self.state.discoveries.load(Ordering::SeqCst)
    }

    pub fn stop_modes(&self) -> Vec<StopMode> {
        lock(&self.state.stop_modes).clone()
    }

    pub async fn next_submit(&self) -> io::Result<MockSubmit> {
        receive(&self.state.submits).await
    }

    pub async fn next_approval(&self) -> io::Result<MockApproval> {
        receive(&self.state.approvals).await
    }
}

impl MockSession {
    pub async fn emit(&self, kind: AdapterEventKind) -> io::Result<()> {
        let event_seq = self.native.event_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.emit_at(event_seq, kind).await
    }

    pub async fn emit_at(&self, event_seq: u64, kind: AdapterEventKind) -> io::Result<()> {
        self.native.event_seq.fetch_max(event_seq, Ordering::SeqCst);
        let sender = lock(&self.native.events)
            .clone()
            .ok_or_else(transport_closed)?;
        sender
            .send(AdapterEvent { event_seq, kind })
            .await
            .map_err(|_| transport_closed())
    }

    pub fn close(&self) {
        lock(&self.native.events).take();
    }
}

impl MockSubmit {
    /// Count a native effect immediately and let the controller release its ACK later.
    pub fn admit(self) -> io::Result<oneshot::Sender<SubmitOutcome>> {
        if self.request.admission.is_expired() {
            return Err(io::Error::other(
                "Fixture admission expired before the native effect",
            ));
        }
        self.effects.fetch_add(1, Ordering::SeqCst);
        Ok(self.result)
    }

    pub fn finish(self, outcome: SubmitOutcome) -> io::Result<()> {
        let outcome = if matches!(
            outcome,
            SubmitOutcome::Accepted { .. } | SubmitOutcome::Unknown(_)
        ) {
            if self.request.admission.is_expired() {
                SubmitOutcome::Expired
            } else {
                self.effects.fetch_add(1, Ordering::SeqCst);
                outcome
            }
        } else {
            outcome
        };
        self.result.send(outcome).map_err(|_| transport_closed())
    }
}

impl MockApproval {
    pub fn finish(self, outcome: ApprovalRespondOutcome) -> io::Result<()> {
        self.result.send(outcome).map_err(|_| transport_closed())
    }
}

impl ClientAdapter for MockAdapter {
    fn kind(&self) -> &'static str {
        "mock"
    }

    fn discover<'a>(&'a self, _: &'a AdapterConfig) -> BoxFuture<'a, Result<DiscoveryReport>> {
        Box::pin(async move {
            self.state.discoveries.fetch_add(1, Ordering::SeqCst);
            let barrier = lock(&self.state.discovery_barrier).take();
            if let Some((entered, hold)) = barrier {
                let _ = entered.send(());
                let _ = hold.await;
            }
            Ok(DiscoveryReport {
                client_kind: self.kind().into(),
                available: true,
                binary_path: None,
                version: None,
                capabilities: self.capabilities(),
                detail: None,
            })
        })
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities {
            supports_create: true,
            supports_resume: true,
            supports_steer_while_working: true,
            supports_approval_observe: true,
            supports_remote_approval: true,
            supports_tui_attach: true,
            ..AdapterCapabilities::default()
        }
    }

    fn create_or_resume(
        &self,
        request: SessionOpenRequest,
    ) -> BoxFuture<'_, Result<OpenedSession>> {
        Box::pin(async move {
            self.state.open_calls.fetch_add(1, Ordering::SeqCst);
            if self.state.fail_next_open.swap(false, Ordering::AcqRel) {
                return Err(NodeError::internal(
                    "The fixture could not open a native session",
                ));
            }
            let (native_key, adapter_binding) = match request.mode {
                SessionOpenMode::Create => (
                    NativeSessionKey {
                        client_kind: self.kind().into(),
                        store_namespace: request.workspace_dir.to_string_lossy().into_owned(),
                        native_session_id: "shared-native-id".into(),
                    },
                    json!({"overrides":"adapter-owned"}),
                ),
                SessionOpenMode::Resume {
                    native_key,
                    previous_binding,
                } => (native_key, previous_binding),
            };
            let target = PersistedSessionRef {
                participant_id: request.participant_id,
                generation: request.generation,
                native_key: Some(native_key.clone()),
                process_ownership: ProcessOwnership::Attached,
                workspace_dir: request.workspace_dir,
                runtime_dir: request.runtime_dir,
                process_identity: None,
                adapter_binding: adapter_binding.clone(),
            };
            let (sender, events) = mpsc::channel(64);
            let session = MockSession {
                target: target.clone(),
                template_id: request.template_id,
                options: request.effective_options,
                instructions: request.instructions,
                credential: request.cred_file_path,
                native: Arc::new(MockNative {
                    event_seq: AtomicU64::default(),
                    events: Mutex::new(Some(sender)),
                }),
            };
            lock(&self.state.sessions).insert(
                (target.participant_id.clone(), target.generation),
                session.clone(),
            );
            Ok(OpenedSession {
                native_key,
                process_ownership: ProcessOwnership::Attached,
                handle: SessionHandle::new(session, target),
                capabilities: self.capabilities(),
                initial_state: ParticipantRuntimeState::Idle,
                adapter_binding,
                events,
            })
        })
    }

    fn inspect_state<'a>(
        &'a self,
        _: &'a PersistedSessionRef,
        live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>> {
        Box::pin(async move {
            Ok(SessionInspection {
                state: if live.is_some() {
                    ParticipantRuntimeState::Idle
                } else {
                    ParticipantRuntimeState::Stopped
                },
                detail: None,
            })
        })
    }

    fn submit_turn<'a>(
        &'a self,
        handle: &'a SessionHandle,
        request: SubmitTurnRequest,
    ) -> BoxFuture<'a, SubmitOutcome> {
        Box::pin(async move {
            self.state.submit_calls.fetch_add(1, Ordering::SeqCst);
            if request.admission.is_expired() {
                return SubmitOutcome::Expired;
            }
            let (result, received) = oneshot::channel();
            let sent = self.state.submits_sent.send(MockSubmit {
                participant_id: handle.persisted().participant_id.clone(),
                generation: handle.persisted().generation,
                request,
                effects: self.state.effects.clone(),
                result,
            });
            if sent.is_err() {
                return SubmitOutcome::Unavailable("The fixture controller closed".into());
            }
            received
                .await
                .unwrap_or_else(|_| SubmitOutcome::Unknown("The fixture ACK was dropped".into()))
        })
    }

    fn respond_approval<'a>(
        &'a self,
        handle: &'a SessionHandle,
        decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async move {
            let (result, received) = oneshot::channel();
            let sent = self.state.approvals_sent.send(MockApproval {
                participant_id: handle.persisted().participant_id.clone(),
                generation: handle.persisted().generation,
                decision,
                result,
            });
            if sent.is_err() {
                return ApprovalRespondOutcome::Unavailable("The fixture controller closed".into());
            }
            received.await.unwrap_or_else(|_| {
                ApprovalRespondOutcome::Unknown("The fixture ACK was dropped".into())
            })
        })
    }

    fn cancel_or_stop<'a>(
        &'a self,
        handle: &'a SessionHandle,
        mode: StopMode,
    ) -> BoxFuture<'a, Result<StopOutcome>> {
        Box::pin(async move {
            lock(&self.state.stop_modes).push(mode.clone());
            let session = handle
                .downcast_ref::<MockSession>()
                .ok_or_else(|| NodeError::internal("The fixture session is missing"))?;
            Ok(match mode {
                StopMode::CancelTurn { .. } => StopOutcome::TurnCancelled,
                StopMode::StopSession => {
                    session.close();
                    StopOutcome::Stopped
                }
            })
        })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        target: &'a PersistedSessionRef,
        _: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
        Box::pin(async move {
            Ok(match &target.native_key {
                Some(native_key) => TuiAttachPlan::Exec {
                    program: "mock-tui".into(),
                    args: vec!["--resume".into(), native_key.native_session_id.clone()],
                    cwd: Some(target.workspace_dir.clone()),
                    env: BTreeMap::new(),
                },
                None => TuiAttachPlan::Unsupported {
                    reason: "No saved session".into(),
                },
            })
        })
    }
}

async fn receive<T>(receiver: &AsyncMutex<mpsc::UnboundedReceiver<T>>) -> io::Result<T> {
    tokio::time::timeout(TEST_TIMEOUT, async { receiver.lock().await.recv().await })
        .await
        .map_err(io::Error::other)?
        .ok_or_else(transport_closed)
}

fn transport_closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "The fixture transport closed")
}

fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
