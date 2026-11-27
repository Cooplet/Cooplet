use std::{
    collections::BTreeSet,
    fs, io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    os::unix::fs::MetadataExt,
    path::Path,
    ptr,
    sync::OnceLock,
    time::Duration,
};

use super::{ProcessIdentity, Snapshot, StopState, TerminateOutcome, group_absent};
use crate::{PlatformError, current_euid};

static BOOT_ID: OnceLock<String> = OnceLock::new();

fn boot_id_str() -> io::Result<&'static str> {
    if let Some(cached) = BOOT_ID.get() {
        return Ok(cached.as_str());
    }
    let id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let id = id.trim();
    if valid_boot_id(id) {
        let _ = BOOT_ID.set(id.to_owned());
        BOOT_ID.get().map(String::as_str).ok_or_else(invalid_proc)
    } else {
        Err(invalid_proc())
    }
}

pub(super) fn boot_id() -> io::Result<String> {
    boot_id_str().map(str::to_owned)
}

fn valid_boot_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                matches!(byte, b'0'..=b'9' | b'a'..=b'f')
            }
        })
}

fn boot_from_token(token: &str) -> Option<&str> {
    let (boot, ticks) = token.strip_prefix("linux:")?.split_once(':')?;
    (valid_boot_id(boot) && ticks.parse::<u64>().is_ok()).then_some(boot)
}

pub(super) fn validate_token(token: &str) -> Result<(), PlatformError> {
    boot_from_token(token).map(|_| ()).ok_or_else(|| {
        PlatformError::InvalidProcessIdentity("expected a Linux boot ID and start ticks".into())
    })
}

pub(super) fn capture(pid: libc::pid_t) -> Result<(Snapshot, String), PlatformError> {
    let dir = format!("/proc/{pid}");
    let dir = Path::new(&dir);
    let stat = read_stat(dir)?;
    let uid = effective_uid(&fs::read(dir.join("status"))?)?;
    // A separately read stat confirms the PID and start time did not change
    // while reading the owner. Detached signaling also brackets this with a pidfd.
    let after = read_stat(dir)?;
    if stat.ticks != after.ticks || stat.pgid != after.pgid {
        return Err(invalid_proc().into());
    }
    let boot = boot_id_str()?;
    Ok((
        Snapshot {
            pgid: stat.pgid,
            uid,
            exited: after.exited,
        },
        format!("linux:{boot}:{}", stat.ticks),
    ))
}

fn effective_uid(status: &[u8]) -> io::Result<u32> {
    // Only the UID line is ASCII. The task name can contain arbitrary bytes.
    let uid = status
        .split(|&byte| byte == b'\n')
        .find_map(|line| line.strip_prefix(b"Uid:"))
        .and_then(|line| std::str::from_utf8(line).ok())
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|field| field.parse().ok());
    uid.ok_or_else(invalid_proc)
}

struct Stat {
    pgid: libc::pid_t,
    ticks: u64,
    exited: bool,
}

fn read_stat(dir: &Path) -> io::Result<Stat> {
    parse_stat(&fs::read(dir.join("stat"))?)
}

fn parse_stat(contents: &[u8]) -> io::Result<Stat> {
    // The executable name is arbitrary bytes and can contain a closing
    // parenthesis. The remaining fields are always ASCII.
    let end = contents
        .iter()
        .rposition(|&byte| byte == b')')
        .ok_or_else(invalid_proc)?;
    let tail = std::str::from_utf8(&contents[end + 1..]).map_err(|_| invalid_proc())?;
    let mut fields = tail.split_ascii_whitespace();
    let leader_exited = matches!(fields.next().ok_or_else(invalid_proc)?, "Z" | "X");
    let pgid = fields
        .nth(1)
        .and_then(|field| field.parse().ok())
        .ok_or_else(invalid_proc)?;
    // Field 20 counts the whole thread group, including a zombie main
    // thread. Its workers can continue after the main thread exits.
    let threads: u32 = fields
        .nth(14)
        .and_then(|field| field.parse().ok())
        .ok_or_else(invalid_proc)?;
    let ticks = fields
        .nth(1)
        .and_then(|field| field.parse().ok())
        .ok_or_else(invalid_proc)?;
    Ok(Stat {
        pgid,
        ticks,
        exited: leader_exited && threads == 1,
    })
}

fn invalid_proc() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "inconsistent process metadata")
}

pub(super) fn tokens_match_for_probe(stored: &str, live: &str) -> bool {
    stored == live
}

pub(super) fn prepare_group_visibility() -> io::Result<()> {
    verify_proc_visibility()
}

/// A conservative live-member check. Scans `/proc` without failing on unrelated
/// process exits and converges over newly spawned PIDs so background workspace
/// activity never causes false `CleanupUnverified` outcomes.
pub(super) fn group_gone(pgid: libc::pid_t, proc_visible: bool) -> io::Result<bool> {
    if group_absent(pgid)? {
        return Ok(true);
    }
    if !proc_visible {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "full process group visibility is unavailable",
        ));
    }
    const MAX_PASSES: usize = 8;
    let mut confirmed = BTreeSet::new();
    let mut path = String::with_capacity(32);
    let mut buffer = [0u8; 512];

    for _ in 0..MAX_PASSES {
        let current = list_pids()?;
        if current.is_empty() {
            return Err(invalid_proc());
        }
        // Evict PIDs observed absent between `/proc` directory snapshots.
        confirmed.retain(|pid| current.contains(pid));
        let pending: Vec<_> = current.difference(&confirmed).copied().collect();
        if pending.is_empty() {
            return Ok(true);
        }
        let own_uid = current_euid();
        for pid in pending {
            match read_stat_into(pid, &mut path, &mut buffer) {
                Ok(stat) => {
                    if stat.pgid == pgid && !stat.exited {
                        return Ok(false);
                    }
                    confirmed.insert(pid);
                }
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound
                        || error.raw_os_error() == Some(libc::ESRCH) =>
                {
                    // An unrelated process exited mid-scan. Leave it out of `confirmed`
                    // so any child it spawned or recycled PID is checked on the next pass.
                }
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    // Under `hidepid=1` (`noaccess`), foreign-UID entries remain listed in
                    // `/proc` while opening `/proc/<pid>/stat` returns EPERM or EACCES. Confirm
                    // foreign-UID processes via `/proc/<pid>` ownership; fail closed on own UID.
                    let proc_dir = path.strip_suffix("/stat").unwrap_or(path.as_str());
                    let proc_uid = fs::symlink_metadata(proc_dir).map(|meta| meta.uid());
                    if classify_stat_permission_denied(proc_uid, own_uid, error)? {
                        confirmed.insert(pid);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(false)
}

fn classify_stat_permission_denied(
    proc_uid: io::Result<u32>,
    own_uid: u32,
    original_error: io::Error,
) -> io::Result<bool> {
    match proc_uid {
        Ok(uid) if uid != own_uid => Ok(true),
        Err(error)
            if error.kind() == io::ErrorKind::NotFound
                || error.raw_os_error() == Some(libc::ESRCH) =>
        {
            Ok(false)
        }
        _ => Err(original_error),
    }
}

fn read_stat_into(pid: libc::pid_t, path: &mut String, buffer: &mut [u8; 512]) -> io::Result<Stat> {
    use std::fmt::Write as _;
    use std::io::Read as _;

    path.clear();
    let _ = write!(path, "/proc/{pid}/stat");
    let mut file = fs::File::open(path.as_str())?;
    let mut len = 0;
    loop {
        if len == buffer.len() {
            let mut rest = Vec::from(&buffer[..]);
            file.read_to_end(&mut rest)?;
            return parse_stat(&rest);
        }
        match file.read(&mut buffer[len..]) {
            Ok(0) => return parse_stat(&buffer[..len]),
            Ok(n) => len += n,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

fn list_pids() -> io::Result<BTreeSet<libc::pid_t>> {
    let mut pids = BTreeSet::new();
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        if let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        {
            pids.insert(pid);
        }
    }
    Ok(pids)
}

fn verify_proc_visibility() -> io::Result<()> {
    let own_pid = std::process::id().to_string();
    let current = fs::read_link("/proc/self")?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    if current == Path::new(&own_pid) && proc_mount_visible(&mountinfo) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "full process group visibility is unavailable",
        ))
    }
}

fn proc_mount_visible(mountinfo: &str) -> bool {
    // Use the topmost (last) mount on /proc so an overmount cannot be bypassed.
    // Within a single-UID workspace, own-UID dumpable children remain visible under
    // hidepid=0|off|1|noaccess|2|invisible; reject subset=pid, hidepid=4|ptraceable,
    // unknown hidepid values, non-root proc mounts, or non-proc mounts.
    mountinfo
        .lines()
        .filter_map(|line| line.split_once(" - "))
        .rfind(|(entry, _)| entry.split_ascii_whitespace().nth(4) == Some("/proc"))
        .is_some_and(|(entry, system)| {
            let root = entry.split_ascii_whitespace().nth(3);
            let mut system = system.split_ascii_whitespace();
            root == Some("/")
                && system.next() == Some("proc")
                && system.nth(1).is_some_and(|options| {
                    options.split(',').all(|option| {
                        if option == "subset=pid" {
                            return false;
                        }
                        match option.strip_prefix("hidepid=") {
                            None => true,
                            Some(mode) => {
                                matches!(mode, "0" | "off" | "1" | "noaccess" | "2" | "invisible")
                            }
                        }
                    })
                })
        })
}

pub(super) fn terminate_detached(
    identity: &ProcessIdentity,
    pid: libc::pid_t,
    grace: Duration,
) -> Result<TerminateOutcome, PlatformError> {
    if boot_id_str().is_ok_and(|boot| Some(boot) != boot_from_token(&identity.start_token)) {
        return Ok(TerminateOutcome::AlreadyGone);
    }
    if super::is_self_pid_or_pgid(pid) {
        return Ok(TerminateOutcome::CleanupUnverified);
    }
    if group_absent(pid).is_ok_and(|absent| absent) {
        return Ok(TerminateOutcome::AlreadyGone);
    }

    if verify_proc_visibility().is_err() {
        return Ok(TerminateOutcome::CleanupUnverified);
    }
    let Some(handle) = open_pidfd(pid)
        .ok()
        .and_then(|handle| verify_leader(identity, handle, pidfd_exited, || capture(pid)))
    else {
        return Ok(TerminateOutcome::CleanupUnverified);
    };
    Ok(stop_verified(
        &handle,
        grace,
        pidfd_signal,
        pidfd_exited,
        || group_gone(pid, true),
    ))
}

fn verify_leader<T>(
    identity: &ProcessIdentity,
    reference: T,
    exited: impl Fn(&T) -> io::Result<bool>,
    capture: impl FnOnce() -> Result<(Snapshot, String), PlatformError>,
) -> Option<T> {
    if exited(&reference).ok()? {
        return None;
    }
    let (snapshot, token) = capture().ok()?;
    if !identity.matches(&snapshot, &token) || exited(&reference).ok()? {
        return None;
    }
    Some(reference)
}

fn stop_verified<T>(
    reference: &T,
    grace: Duration,
    signal: impl Fn(&T, libc::c_int, libc::c_uint) -> io::Result<()>,
    exited: impl Fn(&T) -> io::Result<bool>,
    gone: impl Fn() -> io::Result<bool>,
) -> TerminateOutcome {
    let flags = match signal_scope(|flags| signal(reference, 0, flags)) {
        Ok(flags) => flags,
        Err(error)
            if error.raw_os_error() == Some(libc::ESRCH) && gone().is_ok_and(|gone| gone) =>
        {
            return TerminateOutcome::AlreadyGone;
        }
        Err(_) => return TerminateOutcome::CleanupUnverified,
    };
    StopState::default().drive(
        grace,
        || {
            if flags == libc::PIDFD_SIGNAL_PROCESS_GROUP
                && signal(reference, 0, flags)
                    .is_err_and(|error| error.raw_os_error() == Some(libc::ESRCH))
            {
                return Ok(true);
            }
            exited(reference).and_then(|exited| if exited { gone() } else { Ok(false) })
        },
        |which| signal(reference, which, flags),
    )
}

fn signal_scope(probe: impl FnOnce(libc::c_uint) -> io::Result<()>) -> io::Result<libc::c_uint> {
    match probe(libc::PIDFD_SIGNAL_PROCESS_GROUP) {
        Ok(()) => Ok(libc::PIDFD_SIGNAL_PROCESS_GROUP),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP)
            ) =>
        {
            Ok(0)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn open_pidfd(pid: libc::pid_t) -> io::Result<OwnedFd> {
    // SAFETY: pidfd_open takes a validated PID and zero flags, and on
    // success transfers a new fd exclusively to this caller.
    unsafe {
        let fd = libc::syscall(libc::SYS_pidfd_open, pid, 0u32);
        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(OwnedFd::from_raw_fd(fd as libc::c_int))
        }
    }
}

fn poll_fds(targets: &mut [libc::pollfd], timeout_ms: libc::c_int) -> io::Result<libc::c_int> {
    let nfds = libc::nfds_t::try_from(targets.len()).map_err(|_| invalid_proc())?;
    loop {
        for target in targets.iter_mut() {
            target.revents = 0;
        }
        // SAFETY: `targets` is a valid writable slice of `nfds` `pollfd` entries.
        let ready = unsafe { libc::poll(targets.as_mut_ptr(), nfds, timeout_ms) };
        if ready >= 0 {
            return Ok(ready);
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(super) fn pidfd_exited(fd: &OwnedFd) -> io::Result<bool> {
    let mut targets = [libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }];
    match poll_fds(&mut targets, 0)? {
        0 => Ok(false),
        1 if targets[0].revents & (libc::POLLIN | libc::POLLHUP) != 0 => Ok(true),
        _ => Err(invalid_proc()),
    }
}

pub(super) fn poll_pidfds(
    pidfds: &[&OwnedFd],
    wake: &io::PipeReader,
) -> io::Result<super::PidfdPoll> {
    let mut targets = Vec::with_capacity(pidfds.len() + 1);
    targets.push(libc::pollfd {
        fd: wake.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    });
    for pidfd in pidfds {
        targets.push(libc::pollfd {
            fd: pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        });
    }
    poll_fds(&mut targets, -1)?;
    let wake_revents = targets[0].revents;
    let woken = wake_revents & libc::POLLIN != 0;
    let wake_closed =
        !woken && (wake_revents & (libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0);
    let mut exited = Vec::new();
    for (index, target) in targets[1..].iter().enumerate() {
        if target.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0 {
            exited.push(index);
        }
    }
    Ok(super::PidfdPoll {
        woken,
        wake_closed,
        exited,
    })
}

fn pidfd_signal(fd: &OwnedFd, signal: libc::c_int, flags: libc::c_uint) -> io::Result<()> {
    // SAFETY: the fd is an owned pidfd and the null siginfo requests the
    // kernel-generated signal information for that stable reference.
    if unsafe {
        libc::syscall(
            libc::SYS_pidfd_send_signal,
            fd.as_raw_fd(),
            signal,
            ptr::null::<libc::siginfo_t>(),
            flags,
        )
    } == 0
    {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_stat_after_the_last_parenthesis() -> io::Result<()> {
        let stat = parse_stat(
            b"100 (arbitrary )\xff name) Z 1 100 100 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 5500",
        )?;
        assert!(stat.exited);
        assert_eq!(stat.pgid, 100);
        assert_eq!(stat.ticks, 5500);
        assert!(parse_stat(b"not enough fields").is_err());
        Ok(())
    }

    #[test]
    fn captures_a_live_worker_with_an_exited_main_thread_and_a_non_utf8_name() -> io::Result<()> {
        let stat = parse_stat(
            b"100 (arbitrary )\xff name) Z 1 100 100 0 0 0 0 0 0 0 0 0 0 0 0 0 2 0 5500",
        )?;
        assert!(!stat.exited);
        assert_eq!(stat.pgid, 100);
        assert_eq!(stat.ticks, 5500);
        assert_eq!(
            effective_uid(b"Name:\tworker-\xff\nUid:\t10 20 30 40\n")?,
            20
        );
        Ok(())
    }

    #[test]
    fn group_pidfd_capability_prefers_a_stable_group_and_fails_closed() -> io::Result<()> {
        assert_eq!(
            signal_scope(|flag| {
                assert_eq!(flag, libc::PIDFD_SIGNAL_PROCESS_GROUP);
                Ok(())
            })?,
            libc::PIDFD_SIGNAL_PROCESS_GROUP
        );
        assert_eq!(
            signal_scope(|_| Err(io::Error::from_raw_os_error(libc::EINVAL)))?,
            0
        );
        assert!(signal_scope(|_| Err(io::Error::from_raw_os_error(libc::EPERM))).is_err());
        assert!(signal_scope(|_| Err(io::Error::from_raw_os_error(libc::ESRCH))).is_err());
        Ok(())
    }

    #[test]
    fn proc_mount_visibility_uses_topmost_mount_and_allowlists_hidepid() {
        let clean = "24 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw\n";
        let subset =
            "24 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw,subset=pid\n";
        let overmounted_bad = format!("{clean}{subset}");
        let overmounted_good = format!("{subset}{clean}");
        let non_root = "24 1 0:21 /sub /proc rw - proc proc rw\n";
        let non_proc = "24 1 0:21 / /proc rw - tmpfs tmpfs rw\n";

        assert!(proc_mount_visible(clean));
        for allowed in ["0", "off", "1", "noaccess", "2", "invisible"] {
            let line = format!(
                "24 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw,hidepid={allowed}\n"
            );
            assert!(
                proc_mount_visible(&line),
                "expected hidepid={allowed} allowed"
            );
        }
        for rejected in ["4", "ptraceable", "future"] {
            let line = format!(
                "24 1 0:21 / /proc rw,nosuid,nodev,noexec,relatime - proc proc rw,hidepid={rejected}\n"
            );
            assert!(
                !proc_mount_visible(&line),
                "expected hidepid={rejected} rejected"
            );
        }
        assert!(!proc_mount_visible(subset));
        assert!(!proc_mount_visible(&overmounted_bad));
        assert!(proc_mount_visible(&overmounted_good));
        assert!(!proc_mount_visible(non_root));
        assert!(!proc_mount_visible(non_proc));
    }

    #[test]
    fn hidepid1_permission_denied_confirms_only_foreign_uid() -> io::Result<()> {
        let perm = || io::Error::new(io::ErrorKind::PermissionDenied, "denied");
        assert!(classify_stat_permission_denied(Ok(0), 1000, perm())?);
        assert!(!classify_stat_permission_denied(
            Err(io::Error::new(io::ErrorKind::NotFound, "exited")),
            1000,
            perm()
        )?);
        assert!(!classify_stat_permission_denied(
            Err(io::Error::from_raw_os_error(libc::ESRCH)),
            1000,
            perm()
        )?);
        assert!(classify_stat_permission_denied(Ok(1000), 1000, perm()).is_err());
        assert!(classify_stat_permission_denied(Err(perm()), 1000, perm()).is_err());
        Ok(())
    }
}

#[cfg(test)]
mod detached_tests {
    use std::cell::{Cell, RefCell};

    use super::*;
    use crate::current_euid;

    #[test]
    fn identity_capture_rejects_a_leader_lost_during_the_snapshot() {
        let identity = ProcessIdentity {
            pid: 40,
            pgid: 40,
            start_token: "linux:00000000-0000-0000-0000-000000000000:45".into(),
        };
        let observations = Cell::new(0);
        let result = verify_leader(
            &identity,
            "stable-reference",
            |address| {
                assert_eq!(address, &"stable-reference");
                let count = observations.get();
                observations.set(count + 1);
                Ok(count > 0)
            },
            || {
                Ok((
                    Snapshot {
                        pgid: 40,
                        uid: current_euid(),
                        exited: false,
                    },
                    identity.start_token.clone(),
                ))
            },
        );
        assert_eq!(result, None);
        assert_eq!(observations.get(), 2);
    }

    #[test]
    fn detached_group_sends_each_signal_to_the_same_stable_reference() {
        let sends = RefCell::new(Vec::new());
        let killed = Cell::new(false);
        let reference = String::from("held");
        let outcome = stop_verified(
            &reference,
            Duration::ZERO,
            |address, which, scope| {
                assert!(std::ptr::eq(address, &reference));
                assert_eq!(scope, libc::PIDFD_SIGNAL_PROCESS_GROUP);
                if which == 0 {
                    if killed.get() {
                        return Err(io::Error::from_raw_os_error(libc::ESRCH));
                    }
                } else {
                    sends.borrow_mut().push(which);
                    if which == libc::SIGKILL {
                        killed.set(true);
                    }
                }
                Ok(())
            },
            |_| Ok(false),
            || Ok(false),
        );
        assert_eq!(outcome, TerminateOutcome::KilledAfterGrace);
        assert_eq!(*sends.borrow(), [libc::SIGTERM, libc::SIGKILL]);
    }

    #[test]
    fn legacy_individual_pidfd_never_claims_a_residual_group_was_cleaned() {
        let sends = RefCell::new(Vec::new());
        let leader_gone = Cell::new(false);
        let result = stop_verified(
            &17,
            Duration::ZERO,
            |address, which, scope| {
                assert_eq!(*address, 17);
                if which == 0 {
                    assert_eq!(scope, libc::PIDFD_SIGNAL_PROCESS_GROUP);
                    return Err(io::Error::from_raw_os_error(libc::EINVAL));
                }
                assert_eq!(scope, 0);
                sends.borrow_mut().push(which);
                if which == libc::SIGTERM {
                    leader_gone.set(true);
                    Ok(())
                } else {
                    Err(io::Error::from_raw_os_error(libc::ESRCH))
                }
            },
            |_| Ok(leader_gone.get()),
            || Ok(false),
        );
        assert_eq!(result, TerminateOutcome::CleanupUnverified);
        assert_eq!(*sends.borrow(), [libc::SIGTERM, libc::SIGKILL]);
    }

    #[test]
    fn denied_group_address_sends_no_signal() {
        let calls = Cell::new(0);
        let result = stop_verified(
            &17,
            Duration::ZERO,
            |address, which, scope| {
                assert_eq!(*address, 17);
                assert_eq!(which, 0);
                assert_eq!(scope, libc::PIDFD_SIGNAL_PROCESS_GROUP);
                calls.set(calls.get() + 1);
                Err(io::Error::from_raw_os_error(libc::EPERM))
            },
            |_| Ok(false),
            || Ok(false),
        );
        assert_eq!(result, TerminateOutcome::CleanupUnverified);
        assert_eq!(calls.get(), 1);
    }
}
