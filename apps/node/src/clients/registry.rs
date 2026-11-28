use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
    time::Duration,
};

use cooplet_platform::AdmissionGuard;
use cooplet_protocol::ErrorCode;
use tokio::sync::Mutex;

use super::{AdapterConfig, ClientAdapter, DiscoveryReport};
use crate::error::{NodeError, Result};
use crate::operations::admission_expired;

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct AdapterRegistry {
    entries: BTreeMap<&'static str, Arc<RegisteredAdapter>>,
}

struct RegisteredAdapter {
    adapter: Arc<dyn ClientAdapter>,
    available_discovery: OnceLock<DiscoveryReport>,
    probe_lock: Mutex<()>,
}

impl AdapterRegistry {
    pub fn register(&mut self, adapter: Arc<dyn ClientAdapter>) -> Result<()> {
        let kind = adapter.kind();
        if kind.trim().is_empty() {
            return Err(NodeError::new(
                ErrorCode::InvalidRequest,
                "Adapter kind must not be empty",
            ));
        }
        if self.entries.contains_key(kind) {
            return Err(NodeError::new(
                ErrorCode::InvalidRequest,
                format!("An adapter for {kind} is already registered"),
            ));
        }
        self.entries.insert(
            kind,
            Arc::new(RegisteredAdapter {
                adapter,
                available_discovery: OnceLock::new(),
                probe_lock: Mutex::new(()),
            }),
        );
        Ok(())
    }

    pub fn get(&self, kind: &str) -> Result<Arc<dyn ClientAdapter>> {
        self.lookup(kind).map(|entry| entry.adapter.clone())
    }

    /// Cache only positive availability results so installing or fixing a native
    /// CLI takes effect without restarting the Node.
    pub async fn discover(&self, kind: &str, cfg: &AdapterConfig) -> Result<DiscoveryReport> {
        self.discover_for_open(kind, cfg, None).await
    }

    pub(crate) async fn discover_for_open(
        &self,
        kind: &str,
        cfg: &AdapterConfig,
        admission: Option<&AdmissionGuard>,
    ) -> Result<DiscoveryReport> {
        let entry = self.lookup(kind)?;
        let discovery = entry.discover(cfg, admission);
        let Some(guard) = admission else {
            return discovery.await;
        };
        let budget = guard.remaining_ms().ok_or_else(admission_expired)?;
        let result = tokio::time::timeout(Duration::from_millis(budget), discovery)
            .await
            .map_err(|_| admission_expired())?;
        if guard.is_expired() {
            return Err(admission_expired());
        }
        result
    }

    /// Explicit diagnostics probe each adapter afresh. Launches share a separate
    /// cached discovery so status reads never start native processes.
    pub async fn discover_all(&self, cfg: &AdapterConfig) -> Result<Vec<DiscoveryReport>> {
        let mut reports = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            reports.push(probe(entry.adapter.as_ref(), cfg).await?);
        }
        Ok(reports)
    }

    fn lookup(&self, kind: &str) -> Result<&RegisteredAdapter> {
        self.entries.get(kind).map(Arc::as_ref).ok_or_else(|| {
            NodeError::new(ErrorCode::InvalidRequest, "Unknown client kind")
                .with_extension("reason", "unknown_client_kind")
        })
    }
}

impl RegisteredAdapter {
    async fn discover(
        &self,
        cfg: &AdapterConfig,
        admission: Option<&AdmissionGuard>,
    ) -> Result<DiscoveryReport> {
        if let Some(report) = self.available_discovery.get() {
            return Ok(report.clone());
        }
        let _guard = self.probe_lock.lock().await;
        // The bridge epoch can close while a previous cold probe holds the lock.
        if admission.is_some_and(AdmissionGuard::is_expired) {
            return Err(admission_expired());
        }
        if let Some(report) = self.available_discovery.get() {
            return Ok(report.clone());
        }
        let report = probe(self.adapter.as_ref(), cfg).await?;
        if report.available {
            let _ = self.available_discovery.set(report.clone());
        }
        Ok(report)
    }
}

async fn probe(adapter: &dyn ClientAdapter, config: &AdapterConfig) -> Result<DiscoveryReport> {
    let report = tokio::time::timeout(DISCOVERY_TIMEOUT, adapter.discover(config))
        .await
        .map_err(|_| {
            NodeError::new(
                ErrorCode::Overloaded,
                "The adapter did not complete discovery",
            )
            .with_extension("reason", "adapter_discovery_timeout")
        })?
        .map_err(|error| {
            tracing::warn!(client_kind = adapter.kind(), %error, "Adapter discovery failed");
            NodeError::new(error.code(), "The adapter failed during discovery")
                .with_extension("reason", "adapter_discovery_failed")
        })?;
    if report.client_kind != adapter.kind() {
        return Err(NodeError::new(
            ErrorCode::InvalidRequest,
            "The discovery report identifies a different adapter",
        )
        .with_extension("reason", "invalid_discovery_report"));
    }
    Ok(report)
}
