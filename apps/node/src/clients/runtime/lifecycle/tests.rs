use std::{
    fs,
    sync::atomic::{AtomicBool, Ordering},
};

use cooplet_protocol::RequestId;
use futures_util::future::BoxFuture;
use tokio::sync::Notify;

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
        _request: SessionOpenRequest,
    ) -> BoxFuture<'_, Result<OpenedSession>> {
        Box::pin(async { Err(NodeError::internal("Preflight must not open a session")) })
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
