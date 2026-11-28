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
mod threads;

use std::{
    future::Future,
    process::ExitCode,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use cooplet_protocol::ErrorCode;
use cooplet_storage::{BlobDiskStore, DbActor, ReadPool, StorageEngine};
use tokio::{
    sync::{Semaphore, watch},
    task::JoinHandle,
};

use crate::{
    auth::RevocationWatch, bridge::BridgeRegistry, credentials::Master, error::HubError,
    event_bus::EventWake, storage::HubMeta,
};

const DEFAULT_MAX_BACKGROUND_WRITES: usize = 512;

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
    pub(crate) thread_acl_watch: watch::Sender<u64>,
    pub(crate) thread_dispatch: threads::Dispatch,
    pub(crate) agents_can_create_threads: bool,
    pub(crate) wall_clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    pub(crate) shutdown: watch::Receiver<()>,
    pub(crate) background: BackgroundWrites,
}

impl AppState {
    pub(crate) fn new(
        engine: &StorageEngine,
        meta: HubMeta,
        master: Master,
        thread_acl_watch: watch::Sender<u64>,
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
            thread_acl_watch,
            thread_dispatch: threads::Dispatch::default(),
            agents_can_create_threads: true,
            wall_clock: Arc::new(now_ms),
            shutdown,
            background: BackgroundWrites::new(DEFAULT_MAX_BACKGROUND_WRITES),
        })
    }
}

#[derive(Clone)]
pub(crate) struct BackgroundWrites {
    tasks: watch::Sender<WriteTasks>,
    writes: Arc<Semaphore>,
}

struct WriteTasks {
    open: bool,
    active: usize,
}

impl BackgroundWrites {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            tasks: watch::channel(WriteTasks {
                open: true,
                active: 0,
            })
            .0,
            writes: Arc::new(Semaphore::new(limit.max(1))),
        }
    }

    /// Own writes whose commit or post-commit notification must outlive the HTTP caller.
    pub(crate) fn spawn<F>(&self, task: F) -> Result<JoinHandle<F::Output>, HubError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let permit = self.writes.clone().try_acquire_owned().map_err(|_| {
            HubError::new(
                ErrorCode::Overloaded,
                "Too many background writes are in progress.",
            )
        })?;
        self.spawn_tracked(async move {
            let _permit = permit;
            task.await
        })
    }

    /// Track work with its own admission bound, such as one thread dispatcher per online Node.
    pub(crate) fn spawn_tracked<F>(&self, task: F) -> Result<JoinHandle<F::Output>, HubError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let accepted = self.tasks.send_if_modified(|tasks| {
            if !tasks.open {
                return false;
            }
            tasks.active += 1;
            true
        });
        if !accepted {
            return Err(HubError::new(
                ErrorCode::Overloaded,
                "The Hub is shutting down.",
            ));
        }
        let active = ActiveWrite(self.tasks.clone());
        Ok(tokio::spawn(async move {
            let _active = active;
            task.await
        }))
    }

    pub(crate) fn close(&self) {
        self.tasks.send_modify(|tasks| tasks.open = false);
    }

    pub(crate) async fn drain(&self) {
        let _ = self
            .tasks
            .subscribe()
            .wait_for(|tasks| tasks.active == 0)
            .await;
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

pub fn run_cli() -> ExitCode {
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        eprintln!("Failed to install the Rustls ring crypto provider");
        return ExitCode::FAILURE;
    }
    runtime::main()
}
