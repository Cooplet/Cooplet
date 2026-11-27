mod admin;
mod auth;
mod blobs;
mod bridge;
mod credentials;
mod delivery;
mod error;
mod event_bus;
mod http;
mod openapi;
mod runtime;
mod storage;

use std::{
    future::Future,
    process::ExitCode,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use cooplet_protocol::ErrorCode;
use cooplet_storage::{BlobDiskStore, DbActor, ReadPool, StorageEngine};
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    auth::RevocationWatch, bridge::BridgeRegistry, credentials::Master, error::HubError,
    event_bus::EventWake, storage::HubMeta,
};

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) db: DbActor,
    pub(crate) reads: ReadPool,
    pub(crate) blobs: BlobDiskStore,
    pub(crate) max_blob_bytes: u64,
    pub(crate) master: Arc<Master>,
    pub(crate) meta: HubMeta,
    pub(crate) revocations: RevocationWatch,
    pub(crate) bridge: Arc<dyn BridgeRegistry>,
    pub(crate) event_wake: EventWake,
    pub(crate) shutdown: watch::Receiver<()>,
    pub(crate) background: BackgroundWrites,
}

impl AppState {
    pub(crate) fn new(
        engine: &StorageEngine,
        meta: HubMeta,
        master: Master,
        bridge: Arc<dyn BridgeRegistry>,
        shutdown: watch::Receiver<()>,
        max_blob_bytes: u64,
    ) -> Result<Self, HubError> {
        Ok(Self {
            db: engine.db().clone(),
            reads: engine.reads().clone(),
            blobs: engine
                .blobs()
                .ok_or_else(|| HubError::internal("Hub blob store is missing"))?
                .clone(),
            max_blob_bytes,
            master: Arc::new(master),
            meta,
            revocations: RevocationWatch::default(),
            bridge,
            event_wake: EventWake::new(engine.db(), shutdown.clone()),
            shutdown,
            background: BackgroundWrites(
                watch::channel(WriteTasks {
                    open: true,
                    active: 0,
                })
                .0,
            ),
        })
    }
}

#[derive(Clone)]
pub(crate) struct BackgroundWrites(watch::Sender<WriteTasks>);

struct WriteTasks {
    open: bool,
    active: usize,
}

impl BackgroundWrites {
    /// Own writes whose commit or post-commit notification must outlive the HTTP caller.
    pub(crate) fn spawn<F>(&self, task: F) -> Result<JoinHandle<F::Output>, HubError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if !self.0.send_if_modified(|tasks| {
            if tasks.open {
                tasks.active += 1;
            }
            tasks.open
        }) {
            return Err(HubError::new(
                ErrorCode::Overloaded,
                "The Hub is shutting down.",
            ));
        }
        let active = ActiveWrite(self.0.clone());
        Ok(tokio::spawn(async move {
            let _active = active;
            task.await
        }))
    }

    pub(crate) fn close(&self) {
        self.0.send_modify(|tasks| tasks.open = false);
    }

    pub(crate) async fn drain(&self) {
        let _ = self.0.subscribe().wait_for(|tasks| tasks.active == 0).await;
    }
}

struct ActiveWrite(watch::Sender<WriteTasks>);

impl Drop for ActiveWrite {
    fn drop(&mut self) {
        self.0.send_modify(|tasks| tasks.active -= 1);
    }
}

pub(crate) fn now_ms() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        Err(error) => -i64::try_from(error.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

fn main() -> ExitCode {
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("Failed to install the Rustls ring crypto provider");
        return ExitCode::FAILURE;
    }
    runtime::main()
}
