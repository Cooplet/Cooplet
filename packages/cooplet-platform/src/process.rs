#[cfg(target_os = "linux")]
#[path = "process/linux.rs"]
mod sys;
#[cfg(target_os = "macos")]
#[path = "process/darwin.rs"]
mod sys;

use std::{
    io,
    mem::MaybeUninit,
    os::unix::process::CommandExt,
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus},
    thread,
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{MonoInstant, MonotonicClock, PlatformError, current_euid};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const KILL_CONFIRM_TIMEOUT: Duration = Duration::from_secs(1);
const SPAWN_CLEANUP_GRACE: Duration = Duration::from_millis(100);

/// Fingerprint of the group leader. Persist this only for a managed launch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub pgid: u32,
    pub start_token: String,
}

impl ProcessIdentity {
    pub fn capture(pid: u32, pgid: u32) -> Result<Self, PlatformError> {
        let pid_os = group_leader(pid, pgid)?;
        let (snapshot, token) = sys::capture(pid_os)?;
        if snapshot.pgid != pid_os || snapshot.uid != current_euid() {
            return Err(PlatformError::ProcessNotOwned);
        }
        Ok(Self {
            pid,
            pgid,
            start_token: token,
        })
    }

    pub fn probe(&self) -> Result<ProcessProbe, PlatformError> {
        let pid = self.validated_pid()?;
        match sys::capture(pid) {
            Ok((snapshot, token)) => {
                if !self.matches(&snapshot, &token) {
                    Ok(ProcessProbe::IdentityMismatch)
                } else if snapshot.exited {
                    Ok(ProcessProbe::Exited)
                } else {
                    Ok(ProcessProbe::Running)
                }
            }
            Err(PlatformError::Io(error)) if process_missing(pid, &error) => {
                Ok(ProcessProbe::Missing)
            }
            Err(error) => Err(error),
        }
    }

    fn validated_pid(&self) -> Result<libc::pid_t, PlatformError> {
        let pid = group_leader(self.pid, self.pgid)?;
        sys::validate_token(&self.start_token)?;
        Ok(pid)
    }

    fn matches(&self, snapshot: &Snapshot, token: &str) -> bool {
        u32::try_from(snapshot.pgid) == Ok(self.pgid)
            && snapshot.uid == current_euid()
            && token == self.start_token
    }
}

fn group_leader(pid: u32, pgid: u32) -> Result<libc::pid_t, PlatformError> {
    if pid == pgid
        && pid > 1
        && let Ok(pid) = libc::pid_t::try_from(pid)
    {
        return Ok(pid);
    }
    Err(PlatformError::InvalidProcessIdentity(
        "expected a positive group leader PID greater than one".into(),
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessProbe {
    Running,
    Exited,
    Missing,
    IdentityMismatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminateOutcome {
    AlreadyGone,
    TerminatedGracefully,
    KilledAfterGrace,
    CleanupUnverified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Termination {
    pub outcome: TerminateOutcome,
    pub leader_status: Option<ExitStatus>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("could not spawn an isolated process group: {0}")]
    Spawn(#[source] io::Error),
    #[error("could not capture the spawned process identity: {0}")]
    Capture(#[source] PlatformError),
    #[error("could not capture the spawned process identity and cleanup is pending: {source}")]
    CleanupPending {
        #[source]
        source: PlatformError,
        cleanup: Box<PendingCleanup>,
    },
}

#[derive(Debug)]
#[must_use = "retry cleanup to confirm group termination and reap the child"]
pub struct PendingCleanup(OwnedGroup);

impl PendingCleanup {
    pub fn terminate_and_reap(&mut self, grace: Duration) -> Result<Termination, PlatformError> {
        self.0.terminate(grace)
    }
}

/// Exclusive owner of a child started in its own process group. The caller
/// must keep the handle and use `terminate_and_reap` to release the group.
/// Host code must leave child reaping to this owner.
#[derive(Debug)]
#[must_use]
pub struct ProcessHandle {
    identity: ProcessIdentity,
    group: OwnedGroup,
}

impl ProcessHandle {
    pub fn spawn_isolated_group(command: &mut Command) -> Result<Self, SpawnError> {
        Self::spawn_with_capture(command, ProcessIdentity::capture)
    }

    fn spawn_with_capture(
        command: &mut Command,
        capture: impl FnOnce(u32, u32) -> Result<ProcessIdentity, PlatformError>,
    ) -> Result<Self, SpawnError> {
        let child = command
            .process_group(0)
            .spawn()
            .map_err(SpawnError::Spawn)?;
        let pid = child.id();
        let mut group = OwnedGroup {
            child,
            stop: StopState::default(),
            done: None,
        };
        match capture(pid, pid) {
            Ok(identity) => Ok(Self { identity, group }),
            Err(source) => {
                if group
                    .terminate(SPAWN_CLEANUP_GRACE)
                    .is_ok_and(|result| result.outcome != TerminateOutcome::CleanupUnverified)
                {
                    Err(SpawnError::Capture(source))
                } else {
                    Err(SpawnError::CleanupPending {
                        source,
                        cleanup: Box::new(PendingCleanup(group)),
                    })
                }
            }
        }
    }

    pub fn identity(&self) -> &ProcessIdentity {
        &self.identity
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.group.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.group.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.group.child.stderr.take()
    }

    /// Check for an exited leader while retaining its unreaped PID.
    pub fn observe_exit(&self) -> Result<bool, PlatformError> {
        if self.group.done.is_some() {
            return Ok(true);
        }
        Ok(observe_owned_exit(self.identity.validated_pid()?)?)
    }

    pub fn terminate_and_reap(&mut self, grace: Duration) -> Result<Termination, PlatformError> {
        self.group.terminate(grace)
    }

    /// Clean up a persisted managed launch via a stable OS address. Callers
    /// must withhold resume while the outcome remains unverified.
    pub fn terminate_detached(
        identity: &ProcessIdentity,
        grace: Duration,
    ) -> Result<TerminateOutcome, PlatformError> {
        let pid = identity.validated_pid()?;
        sys::terminate_detached(identity, pid, grace)
    }
}

#[derive(Debug)]
struct OwnedGroup {
    child: Child,
    stop: StopState,
    done: Option<Termination>,
}

impl OwnedGroup {
    fn terminate(&mut self, grace: Duration) -> Result<Termination, PlatformError> {
        if let Some(done) = self.done {
            return Ok(done);
        }
        let pid = group_leader(self.child.id(), self.child.id())?;
        let Ok(exited) = observe_owned_exit(pid) else {
            return Ok(unverified());
        };
        let mut leader_exited = exited;
        let outcome = self.stop.drive(
            grace,
            || {
                leader_exited = observe_owned_exit(pid)?;
                if leader_exited {
                    sys::group_gone(pid)
                } else {
                    Ok(false)
                }
            },
            |signal| {
                // A successful WNOWAIT observation proves the private Child still
                // anchors this PID. No numeric signal is sent once that is lost.
                observe_owned_exit(pid)?;
                send_signal(-pid, signal)
            },
        );
        if outcome == TerminateOutcome::CleanupUnverified || !leader_exited {
            return Ok(unverified());
        }
        let Ok(Some(status)) = self.child.try_wait() else {
            return Ok(unverified());
        };
        let done = Termination {
            outcome,
            leader_status: Some(status),
        };
        self.done = Some(done);
        Ok(done)
    }
}

fn unverified() -> Termination {
    Termination {
        outcome: TerminateOutcome::CleanupUnverified,
        leader_status: None,
    }
}

#[derive(Debug, Default)]
enum StopState {
    #[default]
    Ready,
    TermSent(MonoInstant),
    KillSent,
}

impl StopState {
    fn drive(
        &mut self,
        grace: Duration,
        mut gone: impl FnMut() -> io::Result<bool>,
        mut signal: impl FnMut(libc::c_int) -> io::Result<()>,
    ) -> TerminateOutcome {
        if gone().is_ok_and(|gone| gone) {
            return match self {
                Self::Ready => TerminateOutcome::AlreadyGone,
                Self::TermSent(_) => TerminateOutcome::TerminatedGracefully,
                Self::KillSent => TerminateOutcome::KilledAfterGrace,
            };
        }
        if matches!(self, Self::Ready) {
            if let Err(error) = signal(libc::SIGTERM) {
                return after_signal_error(error, &mut gone, TerminateOutcome::AlreadyGone);
            }
            *self = Self::TermSent(MonotonicClock::deadline_after(grace));
        }
        if let Self::TermSent(deadline) = self {
            // The group can finish during the last sleep of the grace period.
            if wait_for_group(*deadline, &mut gone) || gone().is_ok_and(|gone| gone) {
                return TerminateOutcome::TerminatedGracefully;
            }
            if let Err(error) = signal(libc::SIGKILL) {
                return after_signal_error(
                    error,
                    &mut gone,
                    TerminateOutcome::TerminatedGracefully,
                );
            }
            *self = Self::KillSent;
        }
        if wait_for_group(
            MonotonicClock::deadline_after(KILL_CONFIRM_TIMEOUT),
            &mut gone,
        ) {
            TerminateOutcome::KilledAfterGrace
        } else {
            TerminateOutcome::CleanupUnverified
        }
    }
}

fn after_signal_error(
    error: io::Error,
    gone: &mut impl FnMut() -> io::Result<bool>,
    confirmed: TerminateOutcome,
) -> TerminateOutcome {
    if error.raw_os_error() == Some(libc::ESRCH) && gone().is_ok_and(|gone| gone) {
        confirmed
    } else {
        TerminateOutcome::CleanupUnverified
    }
}

fn wait_for_group(deadline: MonoInstant, gone: &mut impl FnMut() -> io::Result<bool>) -> bool {
    while MonotonicClock::remaining_ms(deadline).is_some() {
        let observed_gone = gone().is_ok_and(|gone| gone);
        let Some(remaining) = MonotonicClock::remaining_ms(deadline) else {
            return false;
        };
        if observed_gone {
            return true;
        }
        thread::sleep(POLL_INTERVAL.min(Duration::from_millis(remaining)));
    }
    false
}

struct Snapshot {
    pgid: libc::pid_t,
    uid: u32,
    exited: bool,
}

fn observe_owned_exit(pid: libc::pid_t) -> io::Result<bool> {
    let mut info = MaybeUninit::<libc::siginfo_t>::zeroed();
    // SAFETY: the output buffer is valid and zero initialized. si_pid is
    // accessed only after a successful WEXITED request for this child.
    unsafe {
        if libc::waitid(
            libc::P_PID,
            pid as _,
            info.as_mut_ptr(),
            libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
        ) != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(info.assume_init().si_pid() == pid)
    }
}

fn process_missing(pid: libc::pid_t, metadata_error: &io::Error) -> bool {
    // A missing metadata file (such as Linux boot_id) or a filtered procfs
    // does not establish whether the process exists in our PID namespace.
    (metadata_error.kind() == io::ErrorKind::NotFound
        || metadata_error.raw_os_error() == Some(libc::ESRCH))
        && send_signal(pid, 0).is_err_and(|error| error.raw_os_error() == Some(libc::ESRCH))
}

fn send_signal(target: libc::pid_t, signal: libc::c_int) -> io::Result<()> {
    // SAFETY: callers pass a validated PID for signal zero, or a negated
    // validated PGID to address a group. kill borrows no memory.
    if unsafe { libc::kill(target, signal) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn group_absent(pgid: libc::pid_t) -> io::Result<bool> {
    match send_signal(-pgid, 0) {
        Ok(()) => Ok(false),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(true),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        io::{BufRead, BufReader},
        process::Stdio,
    };

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn wait_for_leader(handle: &ProcessHandle) -> Result<(), PlatformError> {
        let deadline = MonotonicClock::deadline_after(Duration::from_secs(2));
        while MonotonicClock::remaining_ms(deadline).is_some() {
            if handle.observe_exit()? {
                return Ok(());
            }
            thread::sleep(POLL_INTERVAL);
        }
        Err(io::Error::new(io::ErrorKind::TimedOut, "leader did not exit").into())
    }

    #[test]
    fn reaps_leader_after_graceful_signal_and_remembers_the_result() -> TestResult {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 20"]);
        let mut handle = ProcessHandle::spawn_isolated_group(&mut command)?;
        assert_eq!(handle.identity().probe()?, ProcessProbe::Running);
        let outcome = handle.terminate_and_reap(Duration::from_millis(400))?;
        assert_eq!(outcome.outcome, TerminateOutcome::TerminatedGracefully);
        assert!(outcome.leader_status.is_some());
        assert!(handle.observe_exit()?);
        assert_eq!(handle.terminate_and_reap(Duration::ZERO)?, outcome);
        Ok(())
    }

    #[test]
    fn holds_zombie_leader_until_term_resistant_group_member_is_gone() -> TestResult {
        let mut command = Command::new("sh");
        command.stdout(Stdio::piped()).args([
            "-c",
            r#"env -i /bin/sh -c 'cd /; trap "" TERM; printf "ready\n"; exec /bin/sleep 20' &
               exit 23"#,
        ]);
        let mut handle = ProcessHandle::spawn_isolated_group(&mut command)?;
        let mut stdout = BufReader::new(handle.take_stdout().ok_or("missing stdout")?);
        let mut readiness = String::new();
        stdout.read_line(&mut readiness)?;
        let observed = wait_for_leader(&handle);
        let probe = handle.identity().probe();
        let termination = handle.terminate_and_reap(Duration::from_millis(80))?;
        observed?;
        assert_eq!(readiness, "ready\n");
        assert_eq!(probe?, ProcessProbe::Exited);
        assert_eq!(termination.outcome, TerminateOutcome::KilledAfterGrace);
        assert_eq!(
            termination.leader_status.and_then(|status| status.code()),
            Some(23)
        );
        readiness.clear();
        assert_eq!(stdout.read_line(&mut readiness)?, 0);
        Ok(())
    }

    #[test]
    fn capture_failure_still_cleans_up_the_spawned_group() -> TestResult {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 20"]);
        let pid = std::cell::Cell::new(None);
        let result = ProcessHandle::spawn_with_capture(&mut command, |child_pid, _| {
            pid.set(Some(child_pid));
            Err(PlatformError::ProcessNotOwned)
        });
        let pid = pid.get().ok_or("missing child PID")?;
        if let Err(SpawnError::CleanupPending { mut cleanup, .. }) = result {
            cleanup.terminate_and_reap(Duration::from_millis(500))?;
            return Err("unexpected pending cleanup".into());
        }
        assert!(matches!(
            result,
            Err(SpawnError::Capture(PlatformError::ProcessNotOwned))
        ));
        assert!(group_absent(libc::pid_t::try_from(pid)?)?);
        Ok(())
    }

    #[test]
    fn checks_for_graceful_exit_at_the_end_of_grace() {
        let terminated = Cell::new(false);
        let result = StopState::default().drive(
            Duration::ZERO,
            || Ok(terminated.get()),
            |signal| {
                assert_eq!(signal, libc::SIGTERM);
                terminated.set(true);
                Ok(())
            },
        );
        assert_eq!(result, TerminateOutcome::TerminatedGracefully);
    }

    #[test]
    fn confirms_group_disappearance_when_a_signal_finds_no_recipient() {
        for (missing_on, expected) in [
            (libc::SIGTERM, TerminateOutcome::AlreadyGone),
            (libc::SIGKILL, TerminateOutcome::TerminatedGracefully),
        ] {
            let absent = Cell::new(false);
            let result = StopState::default().drive(
                Duration::ZERO,
                || Ok(absent.get()),
                |signal| {
                    if signal == missing_on {
                        absent.set(true);
                        return Err(io::Error::from_raw_os_error(libc::ESRCH));
                    }
                    Ok(())
                },
            );
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn a_confirmation_that_finishes_after_the_kill_deadline_is_inconclusive() {
        let deadline = MonotonicClock::deadline_after(Duration::from_millis(5));
        assert!(!wait_for_group(deadline, &mut || {
            thread::sleep(Duration::from_millis(10));
            Ok(true)
        }));
    }

    #[test]
    fn rejects_unsafe_identity_and_preserves_uncertain_cleanup_state() {
        for (pid, pgid) in [(0, 0), (1, 1), (11, 10), (u32::MAX, u32::MAX)] {
            assert!(ProcessIdentity::capture(pid, pgid).is_err());
        }
        let signals = RefCell::new(Vec::new());
        let mut state = StopState::default();
        let outcome = state.drive(
            Duration::ZERO,
            || Err(io::Error::other("group scan incomplete")),
            |signal| {
                signals.borrow_mut().push(signal);
                Ok(())
            },
        );
        assert_eq!(outcome, TerminateOutcome::CleanupUnverified);
        assert_eq!(*signals.borrow(), [libc::SIGTERM, libc::SIGKILL]);
        assert_eq!(
            state.drive(
                Duration::ZERO,
                || Ok(true),
                |_| { Err(io::Error::other("unexpected signal")) }
            ),
            TerminateOutcome::KilledAfterGrace
        );
        assert_eq!(*signals.borrow(), [libc::SIGTERM, libc::SIGKILL]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detached_only_signals_the_validated_address() -> TestResult {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 20"]);
        let mut handle = ProcessHandle::spawn_isolated_group(&mut command)?;
        let identity = handle.identity().clone();
        let boot_and_platform = identity
            .start_token
            .rsplit_once(':')
            .ok_or("start token")?
            .0;
        let stale = ProcessIdentity {
            start_token: format!("{boot_and_platform}:0"),
            ..identity.clone()
        };
        let last_boot = ProcessIdentity {
            start_token: "linux:00000000-0000-0000-0000-000000000000:0".into(),
            ..identity.clone()
        };
        // A live leader can have inaccessible metadata, for example on a
        // procfs mounted with subset=pid and no /proc/sys/kernel/random/boot_id.
        assert!(!process_missing(
            libc::pid_t::try_from(identity.pid)?,
            &io::Error::from(io::ErrorKind::NotFound),
        ));
        let invalid_start = ProcessHandle::terminate_detached(&stale, Duration::ZERO);
        let stale_boot = ProcessHandle::terminate_detached(&last_boot, Duration::ZERO);
        let still_live = !handle.observe_exit()?;
        let termination = ProcessHandle::terminate_detached(&identity, Duration::from_millis(300));
        let reaped = handle.terminate_and_reap(Duration::from_millis(300))?;
        assert_eq!(invalid_start?, TerminateOutcome::CleanupUnverified);
        assert_eq!(stale_boot?, TerminateOutcome::AlreadyGone);
        assert!(still_live);
        assert_eq!(termination?, TerminateOutcome::TerminatedGracefully);
        assert_eq!(reaped.outcome, TerminateOutcome::AlreadyGone);
        assert_eq!(identity.probe()?, ProcessProbe::Missing);
        assert_eq!(
            ProcessHandle::terminate_detached(&identity, Duration::ZERO)?,
            TerminateOutcome::AlreadyGone
        );
        Ok(())
    }
}

#[cfg(all(test, target_os = "macos"))]
#[test]
fn darwin_boot_time_change_does_not_confirm_detached_cleanup()
-> Result<(), Box<dyn std::error::Error>> {
    let mut command = Command::new("sh");
    command.args(["-c", "exec sleep 20"]);
    let mut handle = ProcessHandle::spawn_isolated_group(&mut command)?;
    let mut stale = handle.identity().clone();
    let (_, after_boot) = stale.start_token.rsplit_once(':').ok_or("start token")?;
    stale.start_token = format!("darwin:0:{after_boot}");
    let outcome = ProcessHandle::terminate_detached(&stale, Duration::ZERO);
    let alive = !handle.observe_exit()?;
    let reaped = handle.terminate_and_reap(Duration::from_millis(400))?;
    assert_eq!(outcome?, TerminateOutcome::CleanupUnverified);
    assert!(alive);
    assert_eq!(reaped.outcome, TerminateOutcome::TerminatedGracefully);
    Ok(())
}
