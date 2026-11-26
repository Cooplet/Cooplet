use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
};

use cooplet_platform::FileLock;
use rusqlite::Connection;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{StorageConfig, StorageError, pragma};

#[derive(Clone)]
pub struct ReadPool(Arc<Pool>);

struct Pool {
    connections: Mutex<VecDeque<Connection>>,
    permits: Arc<Semaphore>,
    config: StorageConfig,
    lock: Mutex<Option<Arc<FileLock>>>,
}

impl ReadPool {
    pub(crate) fn open(config: StorageConfig, lock: Arc<FileLock>) -> Result<Self, StorageError> {
        let connections = (0..config.read_pool_size)
            .map(|_| pragma::open_readonly(&config.db_path, &config))
            .collect::<Result<_, _>>()?;
        Ok(Self(Arc::new(Pool {
            connections: Mutex::new(connections),
            permits: Arc::new(Semaphore::new(config.read_pool_size as usize)),
            config,
            lock: Mutex::new(Some(lock)),
        })))
    }

    /// Run one synchronous read on a blocking thread. Any transaction left
    /// active by the callback is rolled back before the connection is reused.
    pub async fn read<T, F>(&self, read: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, StorageError> + Send + 'static,
    {
        let permit = self
            .0
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::Closed)?;
        let pool = self.0.clone();
        tokio::task::spawn_blocking(move || {
            let available = pool.connections().pop_front();
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
    fn connections(&self) -> MutexGuard<'_, VecDeque<Connection>> {
        // The mutex only guards infallible deque moves. A callback cannot
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
        if let Some(connection) = self.connection.take()
            && (connection.is_autocommit() || connection.execute_batch("ROLLBACK").is_ok())
            && connection.pragma_update(None, "query_only", true).is_ok()
        {
            self.pool.connections().push_back(connection);
        }
    }
}
