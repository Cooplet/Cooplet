use std::{
    io,
    mem::{MaybeUninit, size_of, size_of_val},
    ptr,
    time::Duration,
};

use super::{ProcessIdentity, Snapshot, TerminateOutcome, group_absent};
use crate::PlatformError;

pub(super) fn validate_token(token: &str) -> Result<(), PlatformError> {
    let valid = token
        .strip_prefix("darwin:")
        .and_then(|token| token.split_once(':'))
        .and_then(|(boot, start)| Some((boot.parse::<u64>().ok()?, start.split_once('.')?)))
        .and_then(|(boot, (seconds, micros))| {
            Some((
                boot,
                seconds.parse::<u64>().ok()?,
                micros.parse::<u32>().ok()?,
            ))
        })
        .is_some_and(|(_, _, micros)| micros < 1_000_000);
    if valid {
        Ok(())
    } else {
        Err(PlatformError::InvalidProcessIdentity(
            "expected Darwin boot and process start times".into(),
        ))
    }
}

pub(super) fn capture(pid: libc::pid_t) -> Result<(Snapshot, String), PlatformError> {
    let info = process_info(pid)?;
    let boot = boot_time()?;
    let pgid = libc::pid_t::try_from(info.pbi_pgid).map_err(|_| invalid_proc())?;
    Ok((
        Snapshot {
            pgid,
            uid: info.pbi_uid,
            exited: info.pbi_status == libc::SZOMB,
        },
        format!(
            "darwin:{boot}:{}.{}",
            info.pbi_start_tvsec, info.pbi_start_tvusec
        ),
    ))
}

pub(super) fn tokens_match_for_probe(stored: &str, live: &str) -> bool {
    // Calendar KERN_BOOTTIME can shift across NTP adjustments or sleep/wake on Darwin.
    // Match the process start timestamp while keeping terminate_detached fail-closed.
    start_suffix(stored).is_some_and(|s| start_suffix(live) == Some(s))
}

fn start_suffix(token: &str) -> Option<&str> {
    token.strip_prefix("darwin:")?.split_once(':')?.1.into()
}

pub(super) fn prepare_group_visibility() -> io::Result<()> {
    Ok(())
}

fn process_info(pid: libc::pid_t) -> io::Result<libc::proc_bsdinfo> {
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    let size = size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the output points to zero-initialized, aligned storage and
    // proc_pidinfo documents that a full-size return initializes that buffer.
    unsafe {
        match libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            1, // PROC_PIDTBSDINFO also examines unreaped zombies with a nonzero argument.
            info.as_mut_ptr().cast(),
            size,
        ) {
            0 => Err(io::Error::last_os_error()),
            n if n == size => Ok(info.assume_init()),
            _ => Err(invalid_proc()),
        }
    }
}

fn boot_time() -> io::Result<u64> {
    let mut mib = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    let mut time = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut size = size_of_val(&time);
    // SAFETY: both the MIB and result buffers remain valid for this read-only
    // call, and size initially describes the writable result buffer.
    if unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            2,
            (&mut time as *mut libc::timeval).cast(),
            &mut size,
            ptr::null_mut(),
            0,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if size != size_of_val(&time) {
        return Err(invalid_proc());
    }
    u64::try_from(time.tv_sec).map_err(|_| invalid_proc())
}

pub(super) fn group_gone(pgid: libc::pid_t, _proc_visible: bool) -> io::Result<bool> {
    match group_absent(pgid) {
        Ok(true) => return Ok(true),
        Ok(false) => {}
        // Darwin's signal probe can return EPERM for a group composed only
        // of zombies. libproc must independently confirm membership.
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => {}
        Err(error) => return Err(error),
    }
    let pids = group_pids(pgid)?;
    for pid in &pids {
        let info = process_info(*pid)?;
        if info.pbi_pgid != pgid as u32 {
            return Err(invalid_proc());
        }
        if info.pbi_status != libc::SZOMB {
            return Ok(false);
        }
    }
    Ok(pids == group_pids(pgid)?)
}

fn group_pids(pgid: libc::pid_t) -> io::Result<Vec<libc::pid_t>> {
    for capacity in [64, 512, 4096, 32_768] {
        let mut pids = vec![0; capacity];
        let bytes = size_of_val(pids.as_slice()) as libc::c_int;
        // SAFETY: the vector has the advertised writable byte capacity.
        // __error returns this thread's errno, which distinguishes an empty
        // list from libproc's zero return on failure.
        let (count, errno) = unsafe {
            let errno = libc::__error();
            *errno = 0;
            let count = libc::proc_listpgrppids(pgid, pids.as_mut_ptr().cast(), bytes);
            (count, *errno)
        };
        if count < 0 || (count == 0 && errno != 0) {
            return Err(if errno != 0 {
                io::Error::from_raw_os_error(errno)
            } else {
                invalid_proc()
            });
        }
        let count = usize::try_from(count).map_err(|_| invalid_proc())?;
        if count < capacity {
            pids.truncate(count);
            pids.sort_unstable();
            return Ok(pids);
        }
    }
    Err(invalid_proc())
}

fn invalid_proc() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "inconsistent process metadata")
}

pub(super) fn terminate_detached(
    _identity: &ProcessIdentity,
    pid: libc::pid_t,
    _grace: Duration,
) -> Result<TerminateOutcome, PlatformError> {
    if super::is_self_pid_or_pgid(pid) {
        return Ok(TerminateOutcome::CleanupUnverified);
    }
    // Darwin has no stable group address for a persisted PID. A changed
    // KERN_BOOTTIME can also reflect a wall-clock adjustment.
    Ok(if group_absent(pid).is_ok_and(|absent| absent) {
        TerminateOutcome::AlreadyGone
    } else {
        TerminateOutcome::CleanupUnverified
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_token_matching_tolerates_boot_time_drift() {
        assert!(tokens_match_for_probe(
            "darwin:1700000000:1700001234.567890",
            "darwin:1700000005:1700001234.567890"
        ));
        assert!(!tokens_match_for_probe(
            "darwin:1700000000:1700001234.567890",
            "darwin:1700000000:1700001235.0"
        ));
        assert!(!tokens_match_for_probe("invalid", "darwin:1:2.3"));
    }
}
