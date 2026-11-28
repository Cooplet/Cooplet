use std::{io, path::PathBuf, sync::Arc};

use cooplet_storage::StorageEngine;
use tokio::{net::TcpListener, sync::watch};

use crate::{
    bridge::NodeBridge,
    clients::{AdapterRegistry, AgentConfig, AgentService},
    config::{NodeConfig, OpenNode},
    delivery::DeliveryService,
    http::{NodeState, listener::NodeListener, router},
    operations::OperationBook,
};

pub struct Node {
    engine: StorageEngine,
    listener: TcpListener,
    workspace_dir: PathBuf,
    pub state: NodeState,
}

impl Node {
    pub async fn open(
        config: NodeConfig,
        registry: AdapterRegistry,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let max_blob_bytes = config.max_blob_bytes;
        let open = OpenNode::start(config).await?;
        let db = open.engine.db().clone();
        let reads = open.engine.reads().clone();
        let blobs = open
            .engine
            .blobs()
            .ok_or_else(|| io::Error::other("Node blob storage was not initialized"))?
            .clone();
        let operations = OperationBook::new(db.clone(), reads.clone());
        let config = AgentConfig {
            node_id: open.info.node_id.clone(),
            endpoint: open.operator.address()?,
            workspace_dir: open.workspace_dir.clone(),
            state_dir: open.state_dir.clone(),
        };
        let agents = Arc::new(AgentService::new(
            db.clone(),
            reads.clone(),
            open.auth.clone(),
            registry,
            operations.clone(),
            config,
        ));
        let delivery = DeliveryService::new(
            db.clone(),
            reads.clone(),
            blobs.clone(),
            open.state_dir.join("blobs"),
            agents.clone(),
            operations.clone(),
        )?;
        let bridge = NodeBridge::load(
            open.info.node_id.clone(),
            open.state_dir,
            db.clone(),
            reads.clone(),
        )
        .await;
        let (shutdown, _) = watch::channel(false);
        let state = NodeState {
            info: open.info,
            db,
            reads,
            blobs,
            max_blob_bytes,
            auth: open.auth,
            operations,
            agents,
            delivery,
            bridge,
            shutdown,
            tasks: Arc::default(),
        };
        Ok(Self {
            engine: open.engine,
            listener: open.listener,
            workspace_dir: open.workspace_dir,
            state,
        })
    }

    pub fn address(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let bridge = self.state.bridge.clone();
        let _shutdown_bridge = ShutdownBridge(bridge.clone());
        bridge.start(self.state.clone(), self.workspace_dir);
        let mut stopping = self.state.shutdown.subscribe();
        let state = self.state.clone();
        let listener = NodeListener::new(self.listener, stopping.clone());
        let server =
            axum::serve(listener, router(self.state.clone())).with_graceful_shutdown(async move {
                let _ = stopping.wait_for(|value| *value).await;
                quiesce(&state);
            });
        let result = server.await;
        self.state.shutdown.send_replace(true);
        quiesce(&self.state);
        bridge.drain().await;
        self.state.tasks.wait().await;
        let sessions = self.state.agents.shutdown().await;
        self.state.delivery.shutdown().await;
        let closed = self.engine.shutdown().await;
        result?;
        closed?;
        sessions?;
        Ok(())
    }

    pub fn engine(&self) -> &StorageEngine {
        &self.engine
    }
}

struct ShutdownBridge(Arc<NodeBridge>);

impl Drop for ShutdownBridge {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}

fn quiesce(state: &NodeState) {
    state.bridge.shutdown();
    state.tasks.close();
    state.delivery.stop_admission();
    state.agents.quiesce();
}
