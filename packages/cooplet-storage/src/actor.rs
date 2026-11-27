use std::{
    convert::Infallible,
    path::PathBuf,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use cooplet_platform::FileLock;
use cooplet_protocol::EventKind;
use rusqlite::{Connection, Transaction, TransactionBehavior};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    CheckpointReport, Migration, ReadPool, StorageConfig, StorageError, WriteError, WriteFailure,
    gc::checkpoint, pragma,
};

/// Lightweight wake-up emitted after a transaction commits an event row.
/// Subscribers read full envelopes from `ReadPool`, keeping the broadcast ring
/// fixed-size regardless of payload sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventNotice {
    pub seq: u64,
    pub kind: EventKind,
}

#[derive(Debug)]
pub struct WriteOutcome<T> {
    pub value: T,
    pub events: Vec<EventNotice>,
}

impl<T> WriteOutcome<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            events: Vec::new(),
        }
    }

    pub fn with_events(value: T, events: Vec<EventNotice>) -> Self {
        Self { value, events }
    }
}

type WriteJob = Box<dyn FnOnce(&mut Connection, &FileLock) + Send>;

enum Job {
    Write(WriteJob),
    Checkpoint(oneshot::Sender<Result<CheckpointReport, StorageError>>),
    Barrier(oneshot::Sender<()>),
}

struct WriterHandle {
    // Rust drops fields in declaration order, ensuring SQLite closes before
    // the sidecar lock is released on every exit path, including unwinds.
    conn: Connection,
    lock: Arc<FileLock>,
}

#[derive(Clone)]
pub struct DbActor {
    inbox: Arc<Mutex<Option<mpsc::Sender<Job>>>>,
    events: broadcast::Sender<EventNotice>,
}

impl DbActor {
    pub(crate) async fn start(
        mut config: StorageConfig,
        migrations: &[Migration],
        blob_root: Option<PathBuf>,
        existing_check: Option<pragma::ExistingDatabaseCheck>,
    ) -> Result<(Self, ReadPool, JoinHandle<()>), StorageError> {
        pragma::validate_config(&config)?;
        let migrations = migrations.to_vec();
        let (sender, mut receiver) = mpsc::channel(config.write_queue_capacity);
        let (started, startup) = oneshot::channel();
        let (events, _) = broadcast::channel(config.broadcast_capacity);
        let thread = thread::Builder::new()
            .name("cooplet-db-writer".into())
            .spawn(move || {
                let result = (|| {
                    let (conn, lock) = pragma::open_writer(
                        &mut config,
                        &migrations,
                        blob_root.as_deref(),
                        existing_check,
                    )?;
                    let handle = WriterHandle {
                        conn,
                        lock: Arc::new(lock),
                    };
                    let reads = ReadPool::open(config.clone(), handle.lock.clone())?;
                    Ok::<_, StorageError>((handle, reads))
                })();
                let (mut handle, reads) = match result {
                    Ok(opened) => opened,
                    Err(error) => {
                        let _ = started.send(Err(error));
                        return;
                    }
                };
                if started.send(Ok(reads)).is_err() {
                    return;
                }
                while let Some(job) = receiver.blocking_recv() {
                    match job {
                        Job::Write(write) => {
                            write(&mut handle.conn, &handle.lock);
                            if !handle.conn.is_autocommit() {
                                if let Err(error) = handle.conn.execute_batch("ROLLBACK") {
                                    tracing::error!(
                                        %error,
                                        "database writer could not roll back active transaction; stopping writer"
                                    );
                                    break;
                                }
                                if !handle.conn.is_autocommit() {
                                    tracing::error!(
                                        "database writer stayed outside autocommit after rollback; stopping writer"
                                    );
                                    break;
                                }
                            }
                        }
                        Job::Checkpoint(reply) => {
                            let result = handle
                                .lock
                                .verify_path()
                                .map_err(StorageError::from)
                                .and_then(|()| checkpoint(&handle.conn, &config));
                            let _ = reply.send(result);
                        }
                        Job::Barrier(reply) => {
                            let _ = reply.send(());
                        }
                    }
                }
                drop(handle);
            })?;
        let reads = startup.await.map_err(|_| StorageError::WriterPanicked)??;
        let actor = Self {
            inbox: Arc::new(Mutex::new(Some(sender))),
            events,
        };
        Ok((actor, reads, thread))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventNotice> {
        self.events.subscribe()
    }

    /// Once accepted, the writer owns the operation through commit or rollback.
    /// Cancelling this future does not cancel the operation.
    pub async fn write<T, E, F>(&self, write: F) -> Result<T, WriteFailure<E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(&Transaction<'_>) -> Result<WriteOutcome<T>, WriteError<E>> + Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        let events = self.events.clone();
        self.submit(Job::Write(Box::new(move |conn, lock| {
            let result = (|| {
                lock.verify_path().map_err(StorageError::from)?;
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(StorageError::from)?;
                let WriteOutcome {
                    value,
                    events: committed,
                } = match write(&tx) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if !tx.is_autocommit() && tx.rollback().is_err() && !conn.is_autocommit() {
                            return Err(StorageError::WriteOutcomeUnknown.into());
                        }
                        return Err(error.into());
                    }
                };
                if let Err(error) = tx.commit() {
                    if !conn.is_autocommit() {
                        let _ = conn.execute_batch("ROLLBACK");
                        if !conn.is_autocommit() {
                            return Err(StorageError::WriteOutcomeUnknown.into());
                        }
                    }
                    return Err(match StorageError::from(error) {
                        StorageError::Sqlite(error) => StorageError::CommitFailed(error).into(),
                        error => error.into(),
                    });
                }
                for event in committed {
                    let _ = events.send(event);
                }
                Ok(value)
            })();
            let _ = reply.send(result);
        })))?;
        result
            .await
            .map_err(|_| StorageError::WriteOutcomeUnknown)?
    }

    pub async fn write_sqlite<T, F>(&self, write: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&Transaction<'_>) -> rusqlite::Result<WriteOutcome<T>> + Send + 'static,
    {
        self.write::<_, Infallible, _>(move |tx| Ok(write(tx)?))
            .await
            .map_err(|error| match error {
                WriteFailure::Storage(error) => error,
                WriteFailure::App(impossible) => match impossible {},
            })
    }

    pub(crate) async fn checkpoint(&self) -> Result<CheckpointReport, StorageError> {
        let (reply, result) = oneshot::channel();
        self.submit(Job::Checkpoint(reply))?;
        result
            .await
            .map_err(|_| StorageError::WriteOutcomeUnknown)?
    }

    pub(crate) async fn barrier(&self) -> Result<(), StorageError> {
        let sender = self
            .inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or(StorageError::Closed)?;
        let (reply, result) = oneshot::channel();
        sender
            .send(Job::Barrier(reply))
            .await
            .map_err(|_| StorageError::Closed)?;
        result.await.map_err(|_| StorageError::WriteOutcomeUnknown)
    }

    pub(crate) fn close(&self) {
        self.inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn submit(&self, job: Job) -> Result<(), StorageError> {
        let inbox = self
            .inbox
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sender = inbox.as_ref().ok_or(StorageError::Closed)?;
        sender.try_send(job).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => StorageError::Overloaded,
            mpsc::error::TrySendError::Closed(_) => StorageError::Closed,
        })
    }
}
