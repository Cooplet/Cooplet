use std::sync::Arc;

use cooplet_protocol::NodeInfoResponse;
use cooplet_storage::{BlobDiskStore, DbActor, ReadPool};
use tokio::sync::watch;

use crate::{
    auth::Auth, clients::AgentService, delivery::DeliveryService, operations::OperationBook,
};

pub(crate) mod blobs;
pub(crate) mod events;
mod extract;
pub(crate) mod listener;
mod openapi;
mod problem;
mod router;
mod routes;
mod tasks;

pub use extract::{Caller, Json, OptionalJson, Path, Query, idempotency_key};
pub use openapi::openapi_json;
pub use router::router;
pub use tasks::Tasks;

#[derive(Clone)]
pub struct NodeState {
    pub info: NodeInfoResponse,
    pub db: DbActor,
    pub reads: ReadPool,
    pub blobs: BlobDiskStore,
    pub max_blob_bytes: u64,
    pub auth: Auth,
    pub operations: OperationBook,
    pub agents: Arc<AgentService>,
    pub delivery: Arc<DeliveryService>,
    pub shutdown: watch::Sender<bool>,
    pub tasks: Arc<Tasks>,
}
