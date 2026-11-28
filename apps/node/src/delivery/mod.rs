//! Durable local messaging and event-driven session intake.

mod envelope;
mod model;
mod outbound;
pub(crate) mod queries;
mod questions;
mod queue;
pub(crate) mod relay;
mod send;

#[cfg(test)]
mod tests;

pub use model::{MessagePage, MessageView, SendMessageRequest, SendMessageResponse};
pub(crate) use send::InboundHubMessage;

use std::{
    collections::HashMap,
    hash::Hash,
    io,
    path::PathBuf,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use cooplet_protocol::{AttachmentRef, BlobId, ErrorCode, MessageId, ParticipantId, ThreadId};
use cooplet_storage::{BlobDiskStore, DbActor, ReadPool, StorageError};
use rusqlite::params;
use tokio::sync::{RwLock, watch};

use crate::{
    auth::Principal,
    clients::AgentService,
    error::{NodeError, Result},
    operations::OperationBook,
};

type Clock = dyn Fn() -> io::Result<i64> + Send + Sync;

#[derive(Clone, Copy)]
struct SubmitFence {
    generation: u64,
    after_seq: u64,
}

pub struct DeliveryService {
    db: DbActor,
    reads: ReadPool,
    blobs: BlobDiskStore,
    blob_root: PathBuf,
    agents: Arc<AgentService>,
    operations: OperationBook,
    clock: Arc<Clock>,
    closing: watch::Sender<bool>,
    activity: RwLock<()>,
    workers: Mutex<HashMap<ParticipantId, bool>>,
    pub(crate) relay_locks: KeyedLocks<BlobId>,
    pub(crate) thread_locks: KeyedLocks<(ThreadId, ParticipantId)>,
}

impl DeliveryService {
    pub fn new(
        db: DbActor,
        reads: ReadPool,
        blobs: BlobDiskStore,
        blob_root: PathBuf,
        agents: Arc<AgentService>,
        operations: OperationBook,
    ) -> Result<Arc<Self>> {
        Self::new_with_clock(
            db,
            reads,
            blobs,
            blob_root,
            agents,
            operations,
            Arc::new(crate::now_ms),
        )
    }

    pub fn new_with_clock(
        db: DbActor,
        reads: ReadPool,
        blobs: BlobDiskStore,
        blob_root: PathBuf,
        agents: Arc<AgentService>,
        operations: OperationBook,
        clock: Arc<Clock>,
    ) -> Result<Arc<Self>> {
        let service = Arc::new(Self {
            db,
            reads,
            blobs,
            blob_root,
            agents,
            operations,
            clock,
            closing: watch::channel(false).0,
            activity: RwLock::new(()),
            workers: Mutex::default(),
            relay_locks: KeyedLocks::default(),
            thread_locks: KeyedLocks::default(),
        });
        let weak = Arc::downgrade(&service);
        service.agents.set_queue_wakeup(Arc::new(move |id| {
            if let Some(service) = weak.upgrade() {
                service.wake(id);
            }
        }))?;
        Ok(service)
    }

    pub fn stop_admission(&self) {
        self.closing.send_replace(true);
    }

    pub(crate) fn subscribe_closing(&self) -> watch::Receiver<bool> {
        self.closing.subscribe()
    }

    pub async fn shutdown(&self) {
        self.stop_admission();
        let _idle = self.activity.write().await;
    }

    fn ensure_accepting(&self) -> Result<()> {
        if *self.closing.borrow() {
            return Err(NodeError::new(
                ErrorCode::Overloaded,
                "Node delivery is shutting down",
            ));
        }
        Ok(())
    }

    pub async fn list(
        &self,
        caller: &Principal,
        after: Option<MessageId>,
        limit: Option<u16>,
    ) -> Result<MessagePage> {
        let can_read_all = authorize_read(caller)?;
        let actor = caller.actor_id().to_owned();
        let limit = queries::checked_page_limit(limit)?;
        let now = (self.clock)()?;
        self.reads
            .read(move |conn| {
                Ok(queries::read_page(
                    conn,
                    &actor,
                    can_read_all,
                    after.as_ref(),
                    limit,
                    now,
                ))
            })
            .await?
    }

    pub async fn show(&self, caller: &Principal, id: MessageId) -> Result<MessageView> {
        let can_read_all = authorize_read(caller)?;
        let actor = caller.actor_id().to_owned();
        let now = (self.clock)()?;
        self.reads
            .read(move |conn| Ok(queries::read_one(conn, &id, &actor, can_read_all, now)))
            .await?
    }

    async fn attachments_available(&self, attachments: &[AttachmentRef], now: i64) -> Result<bool> {
        let ids: Vec<_> = attachments
            .iter()
            .map(|attachment| attachment.blob_id.clone())
            .collect();
        if ids.is_empty() {
            return Ok(true);
        }
        let all_live = self
            .reads
            .read(move |conn| {
                let mut statement = conn.prepare_cached(
                    "SELECT EXISTS(SELECT 1 FROM blobs WHERE blob_id = ?1
                   AND deleted_at_ms IS NULL AND expires_at_ms > ?2)",
                )?;
                for id in ids {
                    if !statement
                        .query_row(params![id.as_str(), now], |row| row.get::<_, bool>(0))?
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
            .await?;
        if !all_live {
            return Ok(false);
        }
        for attachment in attachments {
            match self.blobs.open_blob(&attachment.blob_id).await {
                Ok(Some((_, _))) => {}
                Ok(None) => return Ok(false),
                Err(
                    error @ (StorageError::BlobUnverified(_)
                    | StorageError::InvalidBlobPath(_)
                    | StorageError::BlobMissing(_)),
                ) => {
                    tracing::warn!(blob_id = %attachment.blob_id, %error, "Attached blob failed integrity verification");
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(true)
    }

    fn lock_workers(&self) -> MutexGuard<'_, HashMap<ParticipantId, bool>> {
        self.workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct KeyedLocks<K>(Mutex<HashMap<K, Weak<tokio::sync::Mutex<()>>>>);

impl<K> Default for KeyedLocks<K> {
    fn default() -> Self {
        Self(Mutex::default())
    }
}

impl<K: Hash + Eq> KeyedLocks<K> {
    fn gate(&self, key: K) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        locks.retain(|_, gate| gate.strong_count() != 0);
        let entry = locks.entry(key).or_default();
        entry.upgrade().unwrap_or_else(|| {
            let gate = Arc::new(tokio::sync::Mutex::new(()));
            *entry = Arc::downgrade(&gate);
            gate
        })
    }

    pub(crate) async fn lock(&self, key: K) -> tokio::sync::OwnedMutexGuard<()> {
        self.gate(key).lock_owned().await
    }

    pub(crate) fn try_lock(&self, key: K) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.gate(key).try_lock_owned().ok()
    }
}

fn authorize_read(caller: &Principal) -> Result<bool> {
    if caller.can_read_all() {
        Ok(true)
    } else if caller.is_active_participant() {
        Ok(false)
    } else {
        Err(NodeError::new(
            ErrorCode::Forbidden,
            "Read scope is required",
        ))
    }
}
