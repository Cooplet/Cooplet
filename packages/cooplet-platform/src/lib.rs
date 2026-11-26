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
    Termination,
};
pub use publish::{AtomicPublish, PublishOutcome};

pub fn current_euid() -> u32 {
    // SAFETY: geteuid has no arguments or caller-side preconditions.
    unsafe { libc::geteuid() }
}

pub fn random_bytes(destination: &mut [u8]) -> Result<(), PlatformError> {
    getrandom::fill(destination).map_err(|error| std::io::Error::other(error.to_string()).into())
}

#[cfg(test)]
mod test_support;
