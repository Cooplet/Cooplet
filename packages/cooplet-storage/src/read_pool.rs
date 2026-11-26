use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};

use cooplet_platform::FileLock;
use rusqlite::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{StorageConfig, StorageError, pragma};

#[derive(Clone)]
pub struct ReadPool(Arc<Pool>);

struct Pool {
    connections: Mutex<Vec<Connection>>,
    permits: Arc<Semaphore>,
    config: StorageConfig,
    lock: Mutex<Option<Arc<FileLock>>>,
    closed: AtomicBool,
}

impl ReadPool {
    pub(crate) fn database_and_sidecar(
        &self,
    ) -> Result<(&std::path::Path, Arc<FileLock>), StorageError> {
        let lock = self
            .0
            .lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let sidecar = lock.as_ref().ok_or(StorageError::Closed)?.clone();
        Ok((&self.0.config.db_path, sidecar))
    }

    pub(crate) fn open(config: StorageConfig, lock: Arc<FileLock>) -> Result<Self, StorageError> {
        let connections = (0..config.read_pool_size)
            .map(|_| pragma::open_readonly(&config.db_path, &config))
            .collect::<Result<_, _>>()?;
        Ok(Self(Arc::new(Pool {
            connections: Mutex::new(connections),
            permits: Arc::new(Semaphore::new(config.read_pool_size as usize)),
            config,
            lock: Mutex::new(Some(lock)),
            closed: AtomicBool::new(false),
        })))
    }

    pub(crate) fn mark_closed(&self) {
        self.0.closed.store(true, Ordering::Release);
    }

    /// Run one synchronous read on a blocking thread. Any transaction left
    /// active by the callback is rolled back before the connection is reused.
    pub async fn read<T, F>(&self, read: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StorageError> + Send + 'static,
    {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed);
        }
        let permit = self
            .0
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::Closed)?;
        if self.0.closed.load(Ordering::Acquire) {
            return Err(StorageError::Closed);
        }
        let pool = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let available = pool.connections().pop();
            let connection = match available {
                Some(connection) => connection,
                None => pragma::open_readonly(&pool.config.db_path, &pool.config)?,
            };
            let lease = ReadLease {
                pool,
                connection: Some(connection),
                _permit: permit,
            };
            let connection = lease.connection.as_ref().ok_or(StorageError::Closed)?;
            read(connection)
        })
        .await?
    }

    pub(crate) async fn close(&self) -> Result<(), StorageError> {
        self.mark_closed();
        let all = self
            .0
            .permits
            .acquire_many(self.0.config.read_pool_size)
            .await
            .map_err(|_| StorageError::Closed)?;
        self.0.permits.close();
        let pool = self.0.clone();
        tokio::task::spawn_blocking(move || {
            pool.connections().clear();
            pool.lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
        })
        .await?;
        drop(all);
        Ok(())
    }
}

impl Pool {
    fn connections(&self) -> MutexGuard<'_, Vec<Connection>> {
        // The mutex only guards infallible vec moves. A callback cannot
        // panic while holding it.
        self.connections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

struct ReadLease {
    pool: Arc<Pool>,
    connection: Option<Connection>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        if let Some(connection) = self.connection.take()
            && (connection.is_autocommit() || connection.execute_batch("ROLLBACK").is_ok())
        {
            self.pool.connections().push(connection);
        }
    }
}
