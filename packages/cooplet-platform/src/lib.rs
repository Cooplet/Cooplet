//! Unix process, clock and filesystem primitives shared by Cooplet programs.

mod clock;
mod error;
mod fs;
mod lock;
mod process;
mod publish;

pub use clock::{AdmissionGuard, MonoInstant, MonotonicClock};
pub use error::{PlatformError, PublishStage};
pub use fs::PrivateFs;
pub use lock::{FileLock, LockProbe};
pub use process::{
    PendingCleanup, ProcessHandle, ProcessIdentity, ProcessProbe, SpawnError, TerminateOutcome,
    Termination, current_linux_boot_id,
};
#[cfg(target_os = "linux")]
pub use process::{PidfdPoll, poll_pidfds};
pub use publish::{AtomicPublish, PublishOutcome};

/// Check execute permission with the effective credentials used for a child process.
pub fn is_executable_file(path: &std::path::Path) -> bool {
    use std::{ffi::CString, os::unix::ffi::OsStrExt as _};

    if !path.is_file() {
        return false;
    }
    let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: path is a live NUL-terminated string and AT_FDCWD accepts any pathname.
    unsafe { libc::faccessat(libc::AT_FDCWD, path.as_ptr(), libc::X_OK, libc::AT_EACCESS) == 0 }
}

pub fn current_euid() -> u32 {
    // SAFETY: geteuid has no arguments or caller-side preconditions.
    unsafe { libc::geteuid() }
}

pub fn random_bytes(destination: &mut [u8]) -> Result<(), PlatformError> {
    getrandom::fill(destination).map_err(|error| std::io::Error::other(error.to_string()).into())
}

#[cfg(test)]
mod test_support;
