use std::{
    collections::BTreeSet,
    fs, io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::Path,
    ptr,
    time::Duration,
};

use super::{ProcessIdentity, Snapshot, StopState, TerminateOutcome, group_absent};
use crate::PlatformError;

fn boot_id() -> io::Result<String> {
    let id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let id = id.trim();
    if valid_boot_id(id) {
        Ok(id.into())
    } else {
        Err(invalid_proc())
    }
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
    let uid = fs::read_to_string(dir.join("status"))?
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .and_then(|uid| uid.split_ascii_whitespace().nth(1))
        .and_then(|uid| uid.parse().ok())
        .ok_or_else(invalid_proc)?;
    // A separately read stat confirms the PID and start time did not change
    // while reading the owner. Detached signaling also brackets this with a pidfd.
    let after = read_stat(dir)?;
    if stat.ticks != after.ticks || stat.pgid != after.pgid {
        return Err(invalid_proc().into());
    }
    let boot = boot_id()?;
    Ok((
        Snapshot {
            pgid: stat.pgid,
            uid,
            exited: after.exited,
        },
        format!("linux:{boot}:{}", stat.ticks),
    ))
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
    let exited = matches!(fields.next().ok_or_else(invalid_proc)?, "Z" | "X");
    let pgid = fields
        .nth(1)
        .and_then(|field| field.parse().ok())
        .ok_or_else(invalid_proc)?;
    let ticks = fields
        .nth(16)
        .and_then(|field| field.parse().ok())
        .ok_or_else(invalid_proc)?;
    Ok(Stat {
        pgid,
        ticks,
        exited,
    })
}

fn invalid_proc() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "inconsistent process metadata")
}

/// A conservative live-member check. Confirm the view is the caller's full
/// PID namespace and reject observations when the process list changes
/// during inspection. An unreaped leader can remain as a zombie.
pub(super) fn group_gone(pgid: libc::pid_t) -> io::Result<bool> {
    if group_absent(pgid)? {
        return Ok(true);
    }
    verify_proc_visibility()?;
    let pids = list_pids()?;
    // An empty or filtered list cannot prove absence of live members.
    if pids.is_empty() {
        return Err(invalid_proc());
    }
    for pid in &pids {
        let dir = format!("/proc/{pid}");
        let dir = Path::new(&dir);
        let stat = read_stat(dir)?;
        if stat.pgid == pgid {
            if !stat.exited {
                return Ok(false);
            }
            // The thread-group leader can exit before another thread in
            // the process. Treat every visible extra task conservatively.
            let mut tasks = fs::read_dir(dir.join("task"))?;
            if tasks.next().transpose()?.is_none() || tasks.next().transpose()?.is_some() {
                return Ok(false);
            }
        }
    }
    Ok(pids == list_pids()?)
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
    let visible = mountinfo
        .lines()
        .filter_map(|line| line.split_once(" - "))
        .any(|(entry, system)| {
            let mut fields = entry.split_ascii_whitespace();
            if fields.nth(3) != Some("/") || fields.next() != Some("/proc") {
                return false;
            }
            let mut system = system.split_ascii_whitespace();
            system.next() == Some("proc")
                && system.nth(1).is_some_and(|options| {
                    options.split(',').all(|option| {
                        !option.starts_with("hidepid=")
                            || matches!(option, "hidepid=0" | "hidepid=off")
                    })
                })
        });
    if current == Path::new(&own_pid) && visible {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "full process group visibility is unavailable",
        ))
    }
}

pub(super) fn terminate_detached(
    identity: &ProcessIdentity,
    pid: libc::pid_t,
    grace: Duration,
) -> Result<TerminateOutcome, PlatformError> {
    if boot_id().is_ok_and(|boot| Some(boot.as_str()) != boot_from_token(&identity.start_token)) {
        return Ok(TerminateOutcome::AlreadyGone);
    }
    if group_absent(pid).is_ok_and(|absent| absent) {
        return Ok(TerminateOutcome::AlreadyGone);
    }

    let verified = (|| -> Option<OwnedFd> {
        verify_proc_visibility().ok()?;
        let handle = open_pidfd(pid).ok()?;
        if pidfd_exited(&handle).ok()? {
            return None;
        }
        let (snapshot, token) = capture(pid).ok()?;
        if snapshot.exited || !identity.matches(&snapshot, &token) || pidfd_exited(&handle).ok()? {
            return None;
        }
        Some(handle)
    })();
    let Some(handle) = verified else {
        return Ok(TerminateOutcome::CleanupUnverified);
    };
    let flags = match signal_scope(|flags| pidfd_signal(&handle, 0, flags)) {
        Ok(flags) => flags,
        Err(error)
            if error.raw_os_error() == Some(libc::ESRCH)
                && group_gone(pid).is_ok_and(|gone| gone) =>
        {
            return Ok(TerminateOutcome::AlreadyGone);
        }
        Err(_) => return Ok(TerminateOutcome::CleanupUnverified),
    };
    let outcome = StopState::default().drive(
        grace,
        || {
            if flags == libc::PIDFD_SIGNAL_PROCESS_GROUP
                && pidfd_signal(&handle, 0, flags)
                    .is_err_and(|error| error.raw_os_error() == Some(libc::ESRCH))
            {
                return Ok(true);
            }
            pidfd_exited(&handle)
                .and_then(|exited| if exited { group_gone(pid) } else { Ok(false) })
        },
        |signal| pidfd_signal(&handle, signal, flags),
    );
    Ok(outcome)
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

fn open_pidfd(pid: libc::pid_t) -> io::Result<OwnedFd> {
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

fn pidfd_exited(fd: &OwnedFd) -> io::Result<bool> {
    let mut target = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll receives one valid writable pollfd and a nonblocking timeout.
    match unsafe { libc::poll(&mut target, 1, 0) } {
        0 => Ok(false),
        1 if target.revents & (libc::POLLIN | libc::POLLHUP) != 0 => Ok(true),
        1 => Err(invalid_proc()),
        _ => Err(io::Error::last_os_error()),
    }
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
}
