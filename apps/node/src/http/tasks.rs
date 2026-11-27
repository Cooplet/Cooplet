use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use cooplet_protocol::ErrorCode;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::error::{NodeError, Result};

const MAX_CONCURRENT_BLOB_UPLOADS: usize = 8;

pub struct Tasks {
    gate: Mutex<Gate>,
    idle: Notify,
    uploads: Arc<Semaphore>,
}

impl Default for Tasks {
    fn default() -> Self {
        Self {
            gate: Mutex::default(),
            idle: Notify::new(),
            uploads: Arc::new(Semaphore::new(MAX_CONCURRENT_BLOB_UPLOADS)),
        }
    }
}

#[derive(Default)]
struct Gate {
    closed: bool,
    pending: usize,
}

struct Active(Arc<Tasks>);

impl Drop for Active {
    fn drop(&mut self) {
        let mut gate = self
            .0
            .gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gate.pending -= 1;
        if gate.pending == 0 {
            self.0.idle.notify_one();
        }
    }
}

impl Tasks {
    pub fn try_acquire_upload_slot(&self) -> Result<OwnedSemaphorePermit> {
        self.uploads.clone().try_acquire_owned().map_err(|_| {
            NodeError::new(
                ErrorCode::Overloaded,
                "Too many concurrent blob uploads are in progress",
            )
        })
    }

    #[cfg(test)]
    pub(crate) fn available_upload_slots(&self) -> usize {
        self.uploads.available_permits()
    }
    pub async fn run<T, F>(self: &Arc<Self>, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        let active = {
            let mut gate = self
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if gate.closed {
                return Err(NodeError::new(
                    ErrorCode::Overloaded,
                    "The Node is shutting down",
                ));
            }
            gate.pending += 1;
            Active(self.clone())
        };
        tokio::spawn(async move {
            let _active = active;
            future.await
        })
        .await
        .map_err(NodeError::internal)?
    }

    pub fn is_closed(&self) -> bool {
        self.gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed
    }

    pub fn close(&self) {
        self.gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
    }

    pub async fn wait(&self) {
        loop {
            let wake = self.idle.notified();
            if self
                .gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pending
                == 0
            {
                return;
            }
            wake.await;
        }
    }
}
