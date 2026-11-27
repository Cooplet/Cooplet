#![allow(
    dead_code,
    reason = "Different integration-test binaries use different fixture capabilities"
)]

use std::{error::Error, ffi::OsStr, fs, path::PathBuf, sync::Arc, time::Duration};

use cooplet_client::CoopletClient;
use cooplet_node::{
    auth::Credential,
    clients::{AdapterRegistry, AgentService, ParticipantView},
    config::{NodeConfig, operator_path},
    daemon::Node,
};
use cooplet_protocol::{ParticipantId, PrincipalKind};
use cooplet_storage::{DbActor, ReadPool};
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};

use crate::mock_adapter::{MockAdapter, MockSession};

pub type TestError = Box<dyn Error + Send + Sync>;
pub type TestResult<T = ()> = Result<T, TestError>;

const WAIT: Duration = Duration::from_secs(5);

pub struct TestNode {
    pub root: PathBuf,
    pub state_dir: PathBuf,
    pub workspace_dir: PathBuf,
    pub client: CoopletClient,
    pub reads: ReadPool,
    pub db: DbActor,
    pub agents: Arc<AgentService>,
    shutdown: watch::Sender<bool>,
    server: Option<JoinHandle<TestResult>>,
}

impl TestNode {
    pub async fn start(mock: MockAdapter) -> TestResult<Self> {
        Self::start_with_state_name(mock, OsStr::new("state")).await
    }

    pub async fn start_with_state_name(mock: MockAdapter, name: &OsStr) -> TestResult<Self> {
        let root =
            std::env::temp_dir().join(format!("cooplet-adapter-{}", ParticipantId::generate()?));
        let state_dir = root.join(name);
        let workspace_dir = root.join("workspace");
        fs::create_dir_all(&workspace_dir)?;
        let mut registry = AdapterRegistry::default();
        registry.register(Arc::new(mock))?;
        let node = Node::open(
            NodeConfig::new(state_dir.clone(), workspace_dir.clone()),
            registry,
        )
        .await?;
        let shutdown = node.state.shutdown.clone();
        let reads = node.state.reads.clone();
        let db = node.state.db.clone();
        let agents = node.state.agents.clone();
        let server = tokio::spawn(node.serve());
        let (client, _) = Credential::read(&operator_path(&state_dir))?
            .connect(PrincipalKind::Operator)
            .await?;
        Ok(Self {
            root,
            state_dir,
            workspace_dir,
            client,
            reads,
            db,
            agents,
            shutdown,
            server: Some(server),
        })
    }

    pub async fn stop(&mut self) -> TestResult {
        self.shutdown.send_replace(true);
        if let Some(server) = self.server.take() {
            tokio::time::timeout(WAIT, server).await???;
        }
        Ok(())
    }

    pub async fn create(&self, key: &str, request: Value) -> TestResult<ParticipantId> {
        let created: Value = self
            .client
            .post_json("/v1/agents", &request, Some(key))
            .await?;
        Ok(serde_json::from_value(created["participant_id"].clone())?)
    }

    pub async fn agent_client(&self, session: &MockSession) -> TestResult<CoopletClient> {
        assert!(session.credential.starts_with(&self.state_dir));
        Ok(Credential::read(&session.credential)?
            .connect(PrincipalKind::Participant)
            .await?
            .0)
    }

    pub async fn participant_when(
        &self,
        id: &ParticipantId,
        ready: impl Fn(&ParticipantView) -> bool,
    ) -> TestResult<ParticipantView> {
        let path = format!("/v1/participants/{id}");
        tokio::time::timeout(WAIT, async {
            loop {
                let view: ParticipantView = self.client.get_json(&path, &[]).await?;
                if ready(&view) {
                    return Ok(view);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        if let Some(server) = &self.server {
            server.abort();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}
