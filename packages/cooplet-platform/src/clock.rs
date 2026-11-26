use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::PlatformError;

/// Continuous monotonic timestamp. Ordering intentionally omitted so callers
/// always evaluate deadlines through fail-closed `MonotonicClock` helpers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MonoInstant(Option<u128>);

impl MonoInstant {
    pub const INVALID: Self = Self(None);

    pub const fn is_valid(self) -> bool {
        self.0.is_some()
    }

    pub fn as_millis(self) -> Option<u64> {
        self.0
            .and_then(|nanos| u64::try_from(nanos / 1_000_000).ok())
    }

    pub(crate) fn after(self, duration: Duration) -> Self {
        Self(self.0.map(|now| now.saturating_add(duration.as_nanos())))
    }

    fn remaining_ms_at(self, now: Self) -> Option<u64> {
        let remaining = self.0?.checked_sub(now.0?)?;
        (remaining != 0).then(|| u64::try_from(remaining.div_ceil(1_000_000)).unwrap_or(u64::MAX))
    }
}

pub struct MonotonicClock;

impl MonotonicClock {
    pub fn try_now() -> Result<MonoInstant, PlatformError> {
        continuous_nanos().map(|value| MonoInstant(Some(value)))
    }

    pub fn now() -> MonoInstant {
        Self::try_now().unwrap_or(MonoInstant::INVALID)
    }

    pub fn deadline_after(ttl: Duration) -> MonoInstant {
        Self::now().after(ttl)
    }

    pub fn remaining_ms(deadline: MonoInstant) -> Option<u64> {
        deadline.remaining_ms_at(Self::now())
    }

    pub fn is_expired(deadline: MonoInstant) -> bool {
        Self::remaining_ms(deadline).is_none()
    }
}

#[cfg(target_os = "linux")]
fn continuous_nanos() -> Result<u128, PlatformError> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes into a valid timespec for the duration of the call.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) } != 0
        || time.tv_sec < 0
        || !(0..1_000_000_000).contains(&time.tv_nsec)
    {
        return Err(PlatformError::ClockUnavailable);
    }
    Ok(time.tv_sec as u128 * 1_000_000_000 + time.tv_nsec as u128)
}

#[cfg(target_os = "macos")]
fn continuous_nanos() -> Result<u128, PlatformError> {
    use std::sync::OnceLock;

    #[repr(C)]
    struct TimebaseInfo {
        numer: u32,
        denom: u32,
    }

    unsafe extern "C" {
        fn mach_timebase_info(info: *mut TimebaseInfo) -> libc::c_int;
        fn mach_continuous_time() -> u64;
    }

    static TIMEBASE: OnceLock<(u32, u32)> = OnceLock::new();
    let &(numer, denom) = match TIMEBASE.get() {
        Some(ratio) => ratio,
        None => {
            let mut timebase = TimebaseInfo { numer: 0, denom: 0 };
            // SAFETY: Mach writes the immutable hardware conversion ratio into valid storage.
            let status = unsafe { mach_timebase_info(&mut timebase) };
            if status != 0 || timebase.denom == 0 || timebase.numer == 0 {
                return Err(PlatformError::ClockUnavailable);
            }
            TIMEBASE.get_or_init(|| (timebase.numer, timebase.denom))
        }
    };
    // SAFETY: mach_continuous_time takes no arguments and has no preconditions.
    let ticks = unsafe { mach_continuous_time() };
    Ok(u128::from(ticks) * u128::from(numer) / u128::from(denom))
}

#[derive(Clone, Debug)]
pub struct AdmissionGuard {
    state: Arc<AdmissionState>,
}

#[derive(Debug)]
struct AdmissionState {
    deadline: MonoInstant,
    expired: AtomicBool,
}

impl AdmissionGuard {
    pub fn new(ttl: Duration) -> Self {
        Self::starting_at(MonotonicClock::now(), ttl)
    }

    fn starting_at(now: MonoInstant, ttl: Duration) -> Self {
        Self {
            state: Arc::new(AdmissionState {
                deadline: now.after(ttl),
                expired: AtomicBool::new(ttl.is_zero() || now == MonoInstant::INVALID),
            }),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.remaining_ms().is_none()
    }

    pub fn remaining_ms(&self) -> Option<u64> {
        if self.state.expired.load(Ordering::Acquire) {
            return None;
        }
        self.remaining_at(MonotonicClock::now())
    }

    fn remaining_at(&self, now: MonoInstant) -> Option<u64> {
        if self.state.expired.load(Ordering::Acquire) {
            return None;
        }
        let Some(remaining) = self.state.deadline.remaining_ms_at(now) else {
            self.state.expired.store(true, Ordering::Release);
            return None;
        };
        (!self.state.expired.load(Ordering::Acquire)).then_some(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn elapsed_sleep_and_clock_errors_latch_all_clones() {
        let start = MonoInstant(Some(100_000_000));
        let ttl = Duration::from_millis(10);
        let guard = AdmissionGuard::starting_at(start, ttl);
        let clone = guard.clone();
        assert_eq!(guard.remaining_at(start), Some(10));
        assert_eq!(guard.remaining_at(MonoInstant(Some(109_900_000))), Some(1));
        assert_eq!(clone.remaining_at(MonoInstant(Some(110_000_000))), None);
        assert_eq!(guard.remaining_at(start), None);

        let guard = AdmissionGuard::starting_at(start, ttl);
        assert_eq!(guard.clone().remaining_at(MonoInstant::INVALID), None);
        assert_eq!(guard.remaining_at(start), None);
        assert!(AdmissionGuard::new(Duration::ZERO).is_expired());
    }

    #[test]
    fn invalid_instant_always_fails_closed() {
        let deadline = MonoInstant(Some(100_000_000));
        assert!(!MonoInstant::INVALID.is_valid());
        assert!(deadline.is_valid());
        assert_eq!(MonotonicClock::remaining_ms(MonoInstant::INVALID), None);
        assert!(MonotonicClock::is_expired(MonoInstant::INVALID));
        assert_eq!(deadline.remaining_ms_at(MonoInstant::INVALID), None);
        assert_eq!(MonoInstant::INVALID.remaining_ms_at(deadline), None);
        assert_eq!(
            deadline.remaining_ms_at(MonoInstant(Some(50_000_000))),
            Some(50)
        );
    }
}
