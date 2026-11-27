use std::{
    fs,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use cooplet_protocol::RequestId;
use futures_util::future::BoxFuture;
use tokio::sync::{Notify, oneshot};

use super::*;
use crate::{
    clients::{
        AdapterCapabilities, AdapterRegistry, ApprovalDecision, ApprovalRespondOutcome,
        PersistedSessionRef, SessionHandle, SessionInspection, StopMode, StopOutcome,
        SubmitOutcome, SubmitTurnRequest,
    },
    config::{NodeConfig, OpenNode},
    operations::OperationBook,
};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Default)]
struct ProbeAdapter {
    discovered: AtomicBool,
    hang: bool,
    probing: Notify,
    attaching: Notify,
    opening: Mutex<Option<TimedOpenProbe>>,
}

struct TimedOpenProbe {
    mode: OpenStall,
    entered: oneshot::Sender<()>,
    cancelled: oneshot::Sender<()>,
}

enum OpenStall {
    Preparation {
        reservation: PathBuf,
        release: oneshot::Receiver<()>,
    },
    ManagedProcess,
    AttachedEffect(PathBuf),
}

struct WatchOpenCancellation(Option<oneshot::Sender<()>>);

impl Drop for WatchOpenCancellation {
    fn drop(&mut self) {
        if let Some(cancelled) = self.0.take() {
            let _ = cancelled.send(());
        }
    }
}

impl ClientAdapter for ProbeAdapter {
    fn kind(&self) -> &'static str {
        "probe-test"
    }

    fn discover<'a>(
        &'a self,
        _config: &'a AdapterConfig,
    ) -> BoxFuture<'a, Result<DiscoveryReport>> {
        Box::pin(async move {
            if self.hang {
                self.probing.notify_one();
                return std::future::pending().await;
            }
            self.discovered.store(true, Ordering::Relaxed);
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
            supports_create: self.discovered.load(Ordering::Relaxed),
            ..AdapterCapabilities::default()
        }
    }

    fn create_or_resume(
        &self,
        request: SessionOpenRequest,
    ) -> BoxFuture<'_, Result<OpenedSession>> {
        let probe = self
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        Box::pin(async move {
            let Some(TimedOpenProbe {
                mode,
                entered,
                cancelled,
            }) = probe
            else {
                return Err(NodeError::internal("Preflight must not open a session"));
            };
            let _cancelled = WatchOpenCancellation(Some(cancelled));
            match mode {
                OpenStall::Preparation {
                    reservation,
                    release,
                } => {
                    request
                        .preparation
                        .run_before_managed_launch(move |rollback| {
                            fs::write(&reservation, b"reserved")?;
                            *rollback = Some(Box::new(move || fs::remove_file(reservation)));
                            let _ = entered.send(());
                            release.blocking_recv().map_err(NodeError::internal)?;
                            Ok(())
                        })
                        .await?;
                }
                OpenStall::ManagedProcess => {
                    let mut process = std::process::Command::new("sleep");
                    process.arg("120");
                    request
                        .spawner
                        .spawn(process)
                        .await
                        .map_err(NodeError::internal)?;
                    let _ = entered.send(());
                }
                OpenStall::AttachedEffect(marker) => {
                    fs::write(marker, b"native effect")?;
                    let _ = entered.send(());
                }
            }
            std::future::pending().await
        })
    }

    fn inspect_state<'a>(
        &'a self,
        _target: &'a PersistedSessionRef,
        _live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<SessionInspection>> {
        Box::pin(async { Err(NodeError::internal("Not an inspection test")) })
    }

    fn submit_turn<'a>(
        &'a self,
        _handle: &'a SessionHandle,
        _request: SubmitTurnRequest,
    ) -> BoxFuture<'a, SubmitOutcome> {
        Box::pin(async { SubmitOutcome::Unavailable("Not a submission test".into()) })
    }

    fn respond_approval<'a>(
        &'a self,
        _handle: &'a SessionHandle,
        _decision: ApprovalDecision,
    ) -> BoxFuture<'a, ApprovalRespondOutcome> {
        Box::pin(async { ApprovalRespondOutcome::Unsupported("Not an approval test".into()) })
    }

    fn cancel_or_stop<'a>(
        &'a self,
        _handle: &'a SessionHandle,
        _mode: StopMode,
    ) -> BoxFuture<'a, Result<StopOutcome>> {
        Box::pin(async { Ok(StopOutcome::Unsupported("No live session".into())) })
    }

    fn tui_attach_plan<'a>(
        &'a self,
        _target: &'a PersistedSessionRef,
        _live: Option<&'a SessionHandle>,
    ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
        Box::pin(async move {
            self.attaching.notify_one();
            std::future::pending().await
        })
    }
}

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn service(adapter: Arc<ProbeAdapter>) -> TestResult<(AgentService, OpenNode, Scratch)> {
    let scratch =
        Scratch(std::env::temp_dir().join(format!("cooplet-preflight-{}", RequestId::generate()?)));
    fs::create_dir(&scratch.0)?;
    let node = OpenNode::start(NodeConfig::new(scratch.0.join("state"), scratch.0.clone())).await?;
    let db = node.engine.db().clone();
    let reads = node.engine.reads().clone();
    let mut registry = AdapterRegistry::default();
    registry.register(adapter)?;
    let service = AgentService::new(
        db.clone(),
        reads.clone(),
        node.auth.clone(),
        registry,
        OperationBook::new(db, reads),
        crate::clients::AgentConfig {
            node_id: node.info.node_id.clone(),
            endpoint: node.operator.address()?,
            state_dir: node.state_dir.clone(),
            workspace_dir: node.workspace_dir.clone(),
        },
    );
    Ok((service, node, scratch))
}

#[tokio::test]
async fn launch_preflight_accepts_capabilities_made_available_by_discovery() -> TestResult {
    let adapter = Arc::new(ProbeAdapter::default());
    let (service, node, _scratch) = service(adapter.clone()).await?;
    assert!(!adapter.capabilities().supports_create);
    let plan = service
        .plan(OpenPlanInput {
            id: ParticipantId::generate()?,
            name: "probe".into(),
            workspace: node.workspace_dir.clone(),
            template_id: None,
            client_kind: Some(adapter.kind().into()),
            overrides: None,
            previous: None,
            ephemeral_for_run_id: None,
        })
        .await?;
    assert_eq!(plan.client_kind, adapter.kind());
    assert!(adapter.discovered.load(Ordering::Relaxed));
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn quiesce_cancels_discovery_and_attach_for_an_unresponsive_adapter() -> TestResult {
    let adapter = Arc::new(ProbeAdapter {
        hang: true,
        ..ProbeAdapter::default()
    });
    let (service, node, _scratch) = service(adapter.clone()).await?;
    let id = ParticipantId::generate()?;
    let participant = id.clone();
    let workspace = node.workspace_dir.to_string_lossy().into_owned();
    node.engine.db().write_sqlite(move |tx| {
        tx.execute(
            "INSERT INTO participants (participant_id,kind,name,client_kind,workspace_dir,state,created_at_ms,updated_at_ms)
             VALUES (?1,'agent','probe','probe-test',?2,'stopped',1,1)",
            params![participant.as_str(), workspace],
        )?;
        Ok(WriteOutcome::new(()))
    }).await?;
    let discovering = service.clone();
    let discovery = tokio::spawn(async move { discovering.discover_all().await });
    let attaching = service.clone();
    let attach = tokio::spawn(async move { attaching.attach_plan(id).await });
    let bound = Duration::from_secs(2);
    tokio::time::timeout(bound, async {
        tokio::join!(adapter.probing.notified(), adapter.attaching.notified());
    })
    .await?;
    service.quiesce();
    let (discovery, attach) =
        tokio::time::timeout(bound, async { tokio::join!(discovery, attach) }).await?;
    let discovery = discovery?
        .err()
        .ok_or("Quiesced discovery must return an error")?;
    let attach = attach?
        .err()
        .ok_or("Quiesced attach must return an error")?;
    for error in [discovery, attach] {
        assert_eq!(error.code(), ErrorCode::Overloaded);
        assert_eq!(error.0.extensions["reason"], "node_stopping");
    }
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn discover_retries_after_unavailable_and_sanitizes_internal_errors() -> TestResult {
    use std::sync::atomic::AtomicUsize;

    struct FlakyDiscoverAdapter {
        calls: AtomicUsize,
    }

    impl ClientAdapter for FlakyDiscoverAdapter {
        fn kind(&self) -> &'static str {
            "flaky-probe"
        }

        fn discover<'a>(
            &'a self,
            _config: &'a AdapterConfig,
        ) -> BoxFuture<'a, Result<DiscoveryReport>> {
            Box::pin(async move {
                let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    return Ok(DiscoveryReport {
                        client_kind: self.kind().into(),
                        available: false,
                        binary_path: None,
                        version: None,
                        capabilities: self.capabilities(),
                        detail: Some("cli not installed yet".into()),
                    });
                }
                if attempt == 1 {
                    return Err(NodeError::new(
                        ErrorCode::InternalError,
                        "secret/internal/path probe failure",
                    ));
                }
                Ok(DiscoveryReport {
                    client_kind: self.kind().into(),
                    available: true,
                    binary_path: None,
                    version: Some("1.0".into()),
                    capabilities: self.capabilities(),
                    detail: None,
                })
            })
        }

        fn capabilities(&self) -> AdapterCapabilities {
            AdapterCapabilities {
                supports_create: true,
                supports_resume: true,
                ..AdapterCapabilities::default()
            }
        }

        fn create_or_resume(
            &self,
            _request: SessionOpenRequest,
        ) -> BoxFuture<'_, Result<OpenedSession>> {
            Box::pin(async { Err(NodeError::internal("unused")) })
        }

        fn inspect_state<'a>(
            &'a self,
            _target: &'a PersistedSessionRef,
            _live: Option<&'a SessionHandle>,
        ) -> BoxFuture<'a, Result<SessionInspection>> {
            Box::pin(async { Err(NodeError::internal("unused")) })
        }

        fn submit_turn<'a>(
            &'a self,
            _handle: &'a SessionHandle,
            _request: SubmitTurnRequest,
        ) -> BoxFuture<'a, SubmitOutcome> {
            Box::pin(async { SubmitOutcome::Unavailable("unused".into()) })
        }

        fn respond_approval<'a>(
            &'a self,
            _handle: &'a SessionHandle,
            _decision: ApprovalDecision,
        ) -> BoxFuture<'a, ApprovalRespondOutcome> {
            Box::pin(async { ApprovalRespondOutcome::Unsupported("unused".into()) })
        }

        fn cancel_or_stop<'a>(
            &'a self,
            _handle: &'a SessionHandle,
            _mode: StopMode,
        ) -> BoxFuture<'a, Result<StopOutcome>> {
            Box::pin(async { Ok(StopOutcome::Unsupported("unused".into())) })
        }

        fn tui_attach_plan<'a>(
            &'a self,
            _target: &'a PersistedSessionRef,
            _live: Option<&'a SessionHandle>,
        ) -> BoxFuture<'a, Result<TuiAttachPlan>> {
            Box::pin(async {
                Ok(TuiAttachPlan::Unsupported {
                    reason: "unused".into(),
                })
            })
        }
    }

    let adapter = Arc::new(FlakyDiscoverAdapter {
        calls: AtomicUsize::new(0),
    });
    let mut registry = AdapterRegistry::default();
    registry.register(adapter.clone())?;
    let config = AdapterConfig {
        state_dir: PathBuf::new(),
        runtime_dir: PathBuf::new(),
    };

    // Attempt 0: unavailable -> must not be cached.
    let first = registry.discover("flaky-probe", &config).await?;
    assert!(!first.available);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 1);

    // Attempt 1: error -> must be sanitized and not cached.
    let failed = registry
        .discover("flaky-probe", &config)
        .await
        .err()
        .ok_or("Expected sanitized discovery failure")?;
    assert!(!failed.0.detail.contains("secret/internal/path"));
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);

    // Attempt 2: available -> cached for subsequent calls.
    let ok = registry.discover("flaky-probe", &config).await?;
    assert!(ok.available);
    let cached = registry.discover("flaky-probe", &config).await?;
    assert!(cached.available);
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn resume_rejects_ephemeral_workflow_participant() -> TestResult {
    let adapter = Arc::new(ProbeAdapter::default());
    let (service, node, _scratch) = service(adapter).await?;
    let id = ParticipantId::generate()?;
    let participant = id.clone();
    let workspace = node.workspace_dir.to_string_lossy().into_owned();
    node.engine
        .db()
        .write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO participants(participant_id, kind, name, client_kind, store_namespace,
                                          native_session_id, workspace_dir, ephemeral_for_run_id,
                                          state, created_at_ms, updated_at_ms, stopped_at_ms)
                 VALUES (?1, 'agent', 'wf-temp', 'probe-test', 'ns', 'native-1', ?2, 'run_01234567890123456789012345',
                         'stopped', 1, 1, 1)",
                params![participant.as_str(), workspace],
            )?;
            Ok(WriteOutcome::new(()))
        })
        .await?;
    let error = service
        .resume(
            node.operator.participant_id.as_str(),
            "resume-ephem-key",
            id,
        )
        .await
        .err()
        .ok_or("Ephemeral workflow participant should not be resumable")?;
    assert_eq!(error.code(), ErrorCode::InvalidRequest);
    assert_eq!(error.0.extensions["reason"], "ephemeral_participant");
    node.engine.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn open_timeout_uses_the_admission_state_and_native_progress_at_cancellation() -> TestResult {
    use std::sync::atomic::AtomicU64;

    use cooplet_platform::{MonoClock, MonoInstant};
    use cooplet_protocol::HubId;

    #[derive(Default)]
    struct Clock(AtomicU64);

    impl MonoClock for Clock {
        fn now(&self) -> MonoInstant {
            MonoInstant::from_millis(self.0.load(Ordering::SeqCst))
        }
    }

    #[derive(Clone, Copy)]
    enum Case {
        ExpiredPreparation,
        OpenAdmission,
        Standalone,
        ManagedProcess,
        AttachedEffect,
    }

    for case in [
        Case::ExpiredPreparation,
        Case::OpenAdmission,
        Case::Standalone,
        Case::ManagedProcess,
        Case::AttachedEffect,
    ] {
        let adapter = Arc::new(ProbeAdapter::default());
        let (service, node, scratch) = service(adapter.clone()).await?;
        let marker = scratch.0.join("open-marker");
        let (entered, reached) = oneshot::channel();
        let (cancelled, dropped) = oneshot::channel();
        let (release, mode) = match case {
            Case::ManagedProcess => (None, OpenStall::ManagedProcess),
            Case::AttachedEffect => (None, OpenStall::AttachedEffect(marker.clone())),
            _ => {
                let (release, wait) = oneshot::channel();
                (
                    Some(release),
                    OpenStall::Preparation {
                        reservation: marker.clone(),
                        release: wait,
                    },
                )
            }
        };
        *adapter
            .opening
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(TimedOpenProbe {
            mode,
            entered,
            cancelled,
        });
        let clock = Arc::new(Clock::default());
        let budget = AdmissionGuard::with_clock(Duration::from_secs(30), clock.clone());
        let operation_id = OperationId::generate()?;
        let key = operation_id.clone();
        let actor = node.operator.participant_id.to_string();
        let origin = format!("{}/{}", HubId::generate()?, node.info.node_id);
        let worker = tokio::spawn(async move {
            let request = CreateAgentRequest {
                client_kind: Some("probe-test".into()),
                name: Some("open-timeout".into()),
                ..Default::default()
            };
            if matches!(case, Case::Standalone) {
                service.create(&actor, key.as_str(), request, None).await
            } else {
                service
                    .create_delegated(
                        &DelegatedOperation {
                            id: key,
                            actor_id: actor,
                            origin,
                            admission: budget,
                        },
                        request,
                    )
                    .await
            }
        });

        let wait = Duration::from_secs(5);
        tokio::time::timeout(wait, reached).await??;
        let before_timeout: (bool, Option<i64>) = node.engine.reads().read(|db| {
            Ok(db.query_row(
                "SELECT state = 'starting', runtime_pid FROM participants WHERE name = 'open-timeout'",
                [], |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        }).await?;
        assert!(before_timeout.0);
        assert_eq!(
            before_timeout.1.is_some(),
            matches!(case, Case::ManagedProcess)
        );
        assert_eq!(marker.exists(), !matches!(case, Case::ManagedProcess));

        if !matches!(case, Case::OpenAdmission | Case::Standalone) {
            clock.0.store(30_001, Ordering::SeqCst);
        }
        tokio::time::pause();
        tokio::time::advance(OPEN_REQUEST_TIMEOUT + Duration::from_millis(1)).await;
        dropped.await?;
        tokio::time::resume();
        // Expiry during the cleanup wait must not rewrite the timer's verdict.
        clock.0.store(30_001, Ordering::SeqCst);
        if let Some(release) = release {
            release.send(()).map_err(|_| "preparation worker stopped")?;
        }

        let error = tokio::time::timeout(wait, worker)
            .await??
            .err()
            .ok_or("open must have failed")?;
        let expiry = matches!(case, Case::ExpiredPreparation);
        let (code, reason, status) = if expiry {
            (ErrorCode::BridgeUnavailable, "admission_expired", "failed")
        } else {
            (
                ErrorCode::DeliveryUnknown,
                "native_open_timed_out",
                "unknown",
            )
        };
        assert_eq!(error.code(), code);
        assert_eq!(error.0.extensions["reason"], reason);
        let (recorded, participant, stopped, no_process, revoked): (String, String, bool, bool, bool) =
            node.engine.reads().read(move |db| {
                Ok(db.query_row(
                    "SELECT o.state, p.participant_id, p.state = 'stopped', p.runtime_pid IS NULL,
                            NOT EXISTS (SELECT 1 FROM participant_credentials c
                                WHERE c.participant_id = p.participant_id AND c.revoked_at_ms IS NULL)
                     FROM operations o JOIN participants p ON p.participant_id = o.resource_id
                     WHERE o.operation_key = ?1",
                    [operation_id.as_str()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )?)
            }).await?;
        assert_eq!(recorded, status);
        assert!(stopped && no_process && revoked);
        assert!(
            !node
                .state_dir
                .join(CREDS_DIR)
                .join(credential_filename(&(participant.parse()?, 1)))
                .exists()
        );
        assert_eq!(marker.exists(), matches!(case, Case::AttachedEffect));
        node.engine.shutdown().await?;
    }
    Ok(())
}

#[tokio::test]
async fn completed_blocking_preparation_remains_pending_until_the_adapter_polls_it() -> TestResult {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    let preparation = OpenPreparation::default();
    let (release, wait) = oneshot::channel();
    let mut running = Box::pin(
        preparation
            .run_before_managed_launch(move |_| wait.blocking_recv().map_err(NodeError::internal)),
    );
    assert!(poll_fn(|cx| Poll::Ready(running.as_mut().poll(cx).is_pending())).await);
    release.send(()).map_err(|_| "preparation worker stopped")?;
    preparation.wait().await;
    assert!(preparation.awaiting_managed_preparation());
    drop(running);
    assert!(!preparation.awaiting_managed_preparation());
    Ok(())
}
