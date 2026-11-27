use std::{collections::hash_map::Entry, process::Command, sync::Arc, time::Duration};

use cooplet_platform::{
    AdmissionGuard, PendingCleanup, PlatformError, ProcessHandle, ProcessIdentity, SpawnError,
    TerminateOutcome,
};
use cooplet_storage::WriteOutcome;
use futures_util::future::BoxFuture;
use rusqlite::params;
use tokio::sync::Mutex;

use crate::{
    clients::{ManagedProcessSpawner, SpawnManagedError, SpawnedManagedProcess},
    now_ms,
};

use super::{AgentService, SessionKey, lock};

const GRACE: Duration = Duration::from_millis(500);

pub(super) enum ManagedProcess {
    Running(ProcessHandle),
    Pending(Box<PendingCleanup>),
}

impl ManagedProcess {
    fn terminate(&mut self) -> bool {
        let result = match self {
            Self::Running(handle) => handle.terminate_and_reap(GRACE),
            Self::Pending(cleanup) => cleanup.terminate_and_reap(GRACE),
        };
        result.is_ok_and(|termination| termination.outcome != TerminateOutcome::CleanupUnverified)
    }

    fn reap_after_manual_cleanup(&mut self) -> Result<bool, PlatformError> {
        match self {
            Self::Running(handle) => handle.reap_after_manual_cleanup(),
            Self::Pending(cleanup) => cleanup.reap_after_manual_cleanup(),
        }
    }
}

pub(super) struct OpeningSpawner {
    service: AgentService,
    key: SessionKey,
    state: Arc<Mutex<OpeningState>>,
    admission: Option<AdmissionGuard>,
}

enum OpeningState {
    Available,
    Sealed,
    AdmissionExpired,
}

impl OpeningSpawner {
    pub(super) fn new(
        service: AgentService,
        key: SessionKey,
        admission: Option<AdmissionGuard>,
    ) -> Self {
        Self {
            service,
            key,
            state: Arc::new(Mutex::new(OpeningState::Available)),
            admission,
        }
    }

    /// Wait for the current launch attempt and prevent another. The result
    /// records expiry proven by the worker at the process creation boundary.
    pub(super) async fn seal(&self) -> bool {
        let mut state = self.state.lock().await;
        if matches!(*state, OpeningState::Available) {
            *state = OpeningState::Sealed;
        }
        matches!(*state, OpeningState::AdmissionExpired)
    }
}

impl ManagedProcessSpawner for OpeningSpawner {
    fn spawn(
        &self,
        command: Command,
    ) -> BoxFuture<'_, Result<SpawnedManagedProcess, SpawnManagedError>> {
        Box::pin(async move {
            let mut state = self.state.clone().lock_owned().await;
            if !matches!(*state, OpeningState::Available) {
                return Err(SpawnManagedError::AlreadySpawned);
            }
            *state = OpeningState::Sealed;
            let service = self.service.clone();
            let key = self.key.clone();
            let admission = self.admission.clone();
            tokio::spawn(async move {
                let result = spawn_owned(service, key, command, admission).await;
                if matches!(&result, Err(SpawnManagedError::AdmissionExpired)) {
                    *state = OpeningState::AdmissionExpired;
                }
                result
            })
            .await
            .map_err(|error| SpawnManagedError::PersistenceFailed(error.to_string()))?
        })
    }
}

async fn spawn_owned(
    service: AgentService,
    key: SessionKey,
    mut command: Command,
    admission: Option<AdmissionGuard>,
) -> Result<SpawnedManagedProcess, SpawnManagedError> {
    let result = tokio::task::spawn_blocking(move || {
        if admission.as_ref().is_some_and(AdmissionGuard::is_expired) {
            return Err(SpawnManagedError::AdmissionExpired);
        }
        Ok(ProcessHandle::spawn_isolated_group(&mut command))
    })
    .await
    .map_err(|error| SpawnManagedError::SpawnFailed(error.to_string()))??;
    let mut handle = match result {
        Ok(handle) => handle,
        Err(SpawnError::CleanupPending { source, cleanup }) => {
            lock(&service.0.sessions)
                .processes
                .insert(key, ManagedProcess::Pending(cleanup));
            return Err(SpawnManagedError::CleanupPending(source.to_string()));
        }
        Err(error) => return Err(SpawnManagedError::SpawnFailed(error.to_string())),
    };
    let identity = handle.identity().clone();
    let transport = SpawnedManagedProcess {
        identity: identity.clone(),
        stdin: handle.take_stdin(),
        stdout: handle.take_stdout(),
        stderr: handle.take_stderr(),
    };
    lock(&service.0.sessions)
        .processes
        .insert(key.clone(), ManagedProcess::Running(handle));
    service.0.db.write_sqlite(move |tx| {
        let updated = tx.execute(
            "UPDATE participants SET runtime_pid = ?3, runtime_pgid = ?4, runtime_start_token = ?5,
                    updated_at_ms = ?6 WHERE participant_id = ?1 AND generation = ?2 AND state = 'starting'",
            params![key.0.as_str(), key.1, identity.pid, identity.pgid, identity.start_token,
                now_ms().map_err(super::bad_data)?],
        )?;
        if updated != 1 {
            return Err(rusqlite::Error::QueryReturnedNoRows);
        }
        Ok(WriteOutcome::new(()))
    }).await.map_err(|error| SpawnManagedError::PersistenceFailed(error.to_string()))?;
    Ok(transport)
}

impl AgentService {
    pub(super) fn owned_process_identity(&self, key: &SessionKey) -> Option<ProcessIdentity> {
        match lock(&self.0.sessions).processes.get(key) {
            Some(ManagedProcess::Running(handle)) => Some(handle.identity().clone()),
            _ => None,
        }
    }

    pub(super) fn has_owned_process(&self, key: &SessionKey) -> bool {
        lock(&self.0.sessions).processes.contains_key(key)
    }

    #[cfg(target_os = "linux")]
    pub(super) fn try_open_owned_pidfd(&self, key: &SessionKey) -> Option<std::os::fd::OwnedFd> {
        let sessions = lock(&self.0.sessions);
        let ManagedProcess::Running(handle) = sessions.processes.get(key)? else {
            return None;
        };
        match handle.try_open_pidfd() {
            Ok(fd) => Some(fd),
            Err(error) => {
                tracing::warn!(%error, participant_id = %key.0, "Could not open pidfd for managed agent");
                None
            }
        }
    }

    pub(super) fn reap_after_manual_cleanup(&self, key: &SessionKey) {
        let mut sessions = lock(&self.0.sessions);
        if let Entry::Occupied(mut entry) = sessions.processes.entry(key.clone()) {
            match entry.get_mut().reap_after_manual_cleanup() {
                Ok(true) => {
                    entry.remove();
                }
                Ok(false) => {}
                Err(error) => tracing::warn!(%error, "Could not reap the manually cleaned process"),
            }
        }
    }

    pub(super) async fn clean_owned_process(
        &self,
        key: &SessionKey,
        fallback: Option<ProcessIdentity>,
    ) -> bool {
        let owned = lock(&self.0.sessions).processes.remove(key);
        let key = key.clone();
        let service = self.clone();
        let task = tokio::task::spawn_blocking(move || {
            if let Some(mut owned) = owned {
                let clean = owned.terminate();
                if !clean {
                    lock(&service.0.sessions).processes.insert(key, owned);
                }
                return clean;
            }
            fallback.is_some_and(|identity| {
                ProcessHandle::terminate_detached(&identity, GRACE)
                    .is_ok_and(|outcome| outcome != TerminateOutcome::CleanupUnverified)
            })
        });
        task.await.unwrap_or(false)
    }
}

/// Observe unexpected leader exit via Linux `pidfd` without reaping `Child`.
/// Uses a single lazily-started blocking monitor thread backed by
/// `cooplet_platform::poll_pidfds`, so idle nodes allocate zero extra threads
/// and active managed sessions never open unsafe `AsyncFd` registrations.
#[cfg(target_os = "linux")]
#[derive(Default)]
pub(super) struct ExitMonitor {
    state: Arc<std::sync::Mutex<MonitorState>>,
}

#[cfg(target_os = "linux")]
#[derive(Default)]
struct MonitorState {
    running: bool,
    wake_pending: bool,
    wake: Option<std::io::PipeWriter>,
    pending: Vec<MonitorCommand>,
}

#[cfg(target_os = "linux")]
struct WatchedProcess {
    key: SessionKey,
    service: std::sync::Weak<super::Service>,
    live: Arc<super::LiveSession>,
    pidfd: std::os::fd::OwnedFd,
}

#[cfg(target_os = "linux")]
enum MonitorCommand {
    Watch(WatchedProcess),
    Unwatch(SessionKey),
    Clear,
}

#[cfg(target_os = "linux")]
impl Drop for ExitMonitor {
    fn drop(&mut self) {
        self.clear();
    }
}

#[cfg(target_os = "linux")]
impl ExitMonitor {
    pub(super) fn watch(
        &self,
        service: std::sync::Weak<super::Service>,
        key: SessionKey,
        live: Arc<super::LiveSession>,
        pidfd: std::os::fd::OwnedFd,
    ) {
        let entry = WatchedProcess {
            key,
            service,
            live,
            pidfd,
        };
        let spawn = {
            let mut state = lock(&self.state);
            if entry.live.is_fenced() {
                return;
            }
            if state.running {
                state.pending.push(MonitorCommand::Watch(entry));
                wake_monitor(&mut state);
                None
            } else {
                match std::io::pipe() {
                    Ok((reader, writer)) => {
                        state.running = true;
                        state.wake_pending = false;
                        state.wake = Some(writer);
                        state.pending.push(MonitorCommand::Watch(entry));
                        Some(reader)
                    }
                    Err(error) => {
                        tracing::warn!(%error, "Could not create wake pipe for managed exit monitor");
                        None
                    }
                }
            }
        };
        if let Some(reader) = spawn {
            let shared = Arc::clone(&self.state);
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || run_exit_monitor(shared, reader, runtime));
        }
    }

    pub(super) fn unwatch(&self, key: &SessionKey) {
        let mut state = lock(&self.state);
        if !state.running {
            return;
        }
        state.pending.push(MonitorCommand::Unwatch(key.clone()));
        wake_monitor(&mut state);
    }

    pub(super) fn clear(&self) {
        let mut state = lock(&self.state);
        if !state.running {
            return;
        }
        state.pending.clear();
        state.pending.push(MonitorCommand::Clear);
        wake_monitor(&mut state);
    }
}

#[cfg(target_os = "linux")]
fn wake_monitor(state: &mut MonitorState) {
    use std::io::Write as _;

    if state.wake_pending {
        return;
    }
    if let Some(writer) = state.wake.as_mut()
        && writer.write_all(&[1u8]).is_ok()
    {
        state.wake_pending = true;
    }
}

#[cfg(target_os = "linux")]
fn run_exit_monitor(
    state: Arc<std::sync::Mutex<MonitorState>>,
    mut wake_reader: std::io::PipeReader,
    runtime: tokio::runtime::Handle,
) {
    use std::io::Read as _;

    let mut watched: Vec<WatchedProcess> = Vec::new();
    let mut woken = false;

    loop {
        if woken {
            let mut byte = [0u8; 1];
            let _ = wake_reader.read_exact(&mut byte);
        }
        {
            let mut guard = lock(&state);
            if woken {
                guard.wake_pending = false;
            }
            for command in guard.pending.drain(..) {
                match command {
                    MonitorCommand::Watch(entry) => {
                        watched.retain(|existing| existing.key != entry.key);
                        if !entry.live.is_fenced() {
                            watched.push(entry);
                        }
                    }
                    MonitorCommand::Unwatch(key) => {
                        watched.retain(|existing| existing.key != key);
                    }
                    MonitorCommand::Clear => watched.clear(),
                }
            }
            if watched.is_empty() {
                guard.running = false;
                guard.wake = None;
                guard.wake_pending = false;
                return;
            }
        }

        let pidfds: Vec<&std::os::fd::OwnedFd> = watched.iter().map(|entry| &entry.pidfd).collect();
        let poll = match cooplet_platform::poll_pidfds(&pidfds, &wake_reader) {
            Ok(poll) => poll,
            Err(error) => {
                tracing::warn!(%error, "Managed process exit monitor stopped unexpectedly");
                let mut guard = lock(&state);
                guard.running = false;
                guard.wake = None;
                guard.wake_pending = false;
                return;
            }
        };
        woken = poll.woken;

        for index in poll.exited.into_iter().rev() {
            let entry = watched.swap_remove(index);
            if let Some(service) = entry.service.upgrade()
                && !*service.stopping.borrow()
                && !entry.live.is_fenced()
            {
                runtime.spawn(async move {
                    super::sessions::mark_unknown(&service, &entry.live, "process_exited").await;
                });
            }
        }

        if poll.wake_closed {
            let mut guard = lock(&state);
            guard.running = false;
            guard.wake = None;
            guard.wake_pending = false;
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, process::Stdio};

    use cooplet_platform::ProcessProbe;
    use cooplet_protocol::{ParticipantId, ParticipantState, RequestId};
    use cooplet_storage::StorageEngine;

    use super::*;
    use crate::{
        clients::{AdapterRegistry, AgentConfig, StopAgentRequest, StopDisposition},
        config::{NodeConfig, OpenNode},
        operations::OperationBook,
    };

    struct TestDir(PathBuf);

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

    struct Fixture {
        service: AgentService,
        engine: StorageEngine,
        actor: ParticipantId,
        agent: ParticipantId,
        _dir: TestDir,
    }

    async fn fixture() -> TestResult<Fixture> {
        let temp = TestDir(
            std::env::temp_dir().join(format!("cooplet-process-{}", RequestId::generate()?)),
        );
        fs::create_dir(&temp.0)?;
        let node = OpenNode::start(NodeConfig::new(temp.0.join("state"), temp.0.clone())).await?;
        let db = node.engine.db().clone();
        let reads = node.engine.reads().clone();
        let service = AgentService::new(
            db.clone(),
            reads.clone(),
            node.auth,
            AdapterRegistry::default(),
            OperationBook::new(db.clone(), reads),
            AgentConfig {
                node_id: node.info.node_id,
                endpoint: node.operator.address()?,
                state_dir: node.state_dir,
                workspace_dir: node.workspace_dir,
            },
        );
        let id = ParticipantId::generate()?;
        let insert_id = id.clone();
        db.write_sqlite(move |tx| {
            tx.execute(
                "INSERT INTO participants(participant_id,kind,name,state,created_at_ms,updated_at_ms)
                 VALUES (?1,'agent','managed-worker','starting',1,1)",
                [insert_id.as_str()],
            )?;
            Ok(WriteOutcome::new(()))
        }).await?;
        Ok(Fixture {
            service,
            engine: node.engine,
            actor: node.operator.participant_id,
            agent: id,
            _dir: temp,
        })
    }

    #[test]
    fn blocking_pool_delay_cannot_admit_a_managed_process_after_the_deadline() -> TestResult {
        use std::{
            future::{Future, poll_fn},
            sync::atomic::{AtomicU64, Ordering},
            task::Poll,
        };

        use cooplet_platform::{MonoClock, MonoInstant};
        use tokio::{runtime::Builder, sync::oneshot, time::timeout};

        #[derive(Default)]
        struct Clock(AtomicU64);

        impl MonoClock for Clock {
            fn now(&self) -> MonoInstant {
                MonoInstant::from_millis(self.0.load(Ordering::SeqCst))
            }
        }

        Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()?
            .block_on(async {
                let fixture = fixture().await?;
                let clock = Arc::new(Clock::default());
                let admission = AdmissionGuard::with_clock(Duration::from_secs(30), clock.clone());
                let (entered, occupied) = oneshot::channel();
                let (release, blocked) = oneshot::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    let _ = entered.send(());
                    let _ = blocked.blocking_recv();
                });
                timeout(Duration::from_secs(5), occupied).await??;
                let path = fixture._dir.0.join("process-started");
                let mut command = Command::new("sh");
                command
                    .args(["-c", "printf started > \"$1\"; exec sleep 5", "sh"])
                    .arg(&path);
                let key = (fixture.agent.clone(), 1);
                let launch = spawn_owned(
                    fixture.service.clone(),
                    key.clone(),
                    command,
                    Some(admission),
                );
                tokio::pin!(launch);
                // The first poll submits spawn_blocking, behind the occupied worker.
                assert!(poll_fn(|cx| Poll::Ready(launch.as_mut().poll(cx).is_pending())).await);
                clock.0.store(30_001, Ordering::SeqCst);
                release.send(()).map_err(|_| "blocking worker closed")?;
                blocker.await?;
                assert!(matches!(
                    launch.await,
                    Err(SpawnManagedError::AdmissionExpired)
                ));
                assert!(!path.exists());
                assert!(!fixture.service.has_owned_process(&key));
                assert!(
                    fixture
                        .service
                        .participant_row(&fixture.agent)
                        .await?
                        .identity
                        .is_none()
                );
                fixture.engine.shutdown().await?;
                Ok(())
            })
    }

    #[tokio::test]
    async fn process_group_is_owned_only_once_and_identity_is_durable_before_return() -> TestResult
    {
        let fixture = fixture().await?;
        let service = &fixture.service;
        let id = &fixture.agent;
        let key = (id.clone(), 1);
        let spawner = OpeningSpawner::new(service.clone(), key.clone(), None);
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 60 & wait"]);
        let spawned = spawner.spawn(command).await?;
        assert_eq!(spawned.identity.probe()?, ProcessProbe::Running);
        assert_eq!(
            service.participant_row(id).await?.identity,
            Some(spawned.identity.clone())
        );
        assert!(matches!(
            spawner.spawn(Command::new("true")).await,
            Err(SpawnManagedError::AlreadySpawned)
        ));
        assert!(service.clean_owned_process(&key, None).await);
        assert!(matches!(
            spawned.identity.probe()?,
            ProcessProbe::Missing | ProcessProbe::Exited
        ));
        fixture.engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn manual_cleanup_releases_the_exited_managed_child() -> TestResult {
        let fixture = fixture().await?;
        let service = &fixture.service;
        let key = (fixture.agent.clone(), 1);
        let spawner = OpeningSpawner::new(service.clone(), key.clone(), None);
        let mut command = Command::new("sh");
        command.args(["-c", "read -r input"]).stdin(Stdio::piped());
        let mut spawned = spawner.spawn(command).await?;
        assert_eq!(spawned.identity.probe()?, ProcessProbe::Running);

        let id = fixture.agent.clone();
        fixture
            .engine
            .db()
            .write_sqlite(move |tx| {
                let binding = serde_json::json!({
                    "core": {"lifecycle_block": {"kind": "cleanup_unverified", "boot_id": null}},
                    "adapter": {},
                });
                tx.execute(
                    "UPDATE participants SET state = 'unknown', adapter_binding_json = ?2
                 WHERE participant_id = ?1",
                    params![id.as_str(), binding.to_string()],
                )?;
                Ok(WriteOutcome::new(()))
            })
            .await?;
        drop(spawned.stdin.take());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if spawned.identity.probe()? == ProcessProbe::Exited {
                    return Ok::<_, PlatformError>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        assert!(service.has_owned_process(&key));

        let response = service
            .stop(
                fixture.actor.as_str(),
                "manual-cleanup-regression",
                fixture.agent,
                StopAgentRequest {
                    cancel_turn_only: false,
                    acknowledge_manual_cleanup: true,
                },
            )
            .await?;
        assert_eq!(response.state, ParticipantState::Stopped);
        assert!(matches!(
            response.stop_outcome,
            StopDisposition::AcknowledgedManualCleanup
        ));
        assert!(!service.has_owned_process(&key));
        assert!(matches!(
            spawned.identity.probe()?,
            ProcessProbe::Missing | ProcessProbe::IdentityMismatch
        ));
        fixture.engine.shutdown().await?;
        Ok(())
    }
}
