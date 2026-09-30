//! The [`Clock`] seam — where record timestamps come from.
//!
//! Timestamps (e.g. `inserted_at` / `updated_at` attributes the domain stamps
//! automatically) are read from a `Clock`, so they can come from the OS wall
//! clock ([`SystemClock`]) or — behind the `hlc` feature — the `ash-time`
//! Hybrid Logical Clock (`AshTimeClock`) for causal ordering across nodes.
//!
//! Only [`SystemClock`] is unconditional: `ash-time` is an optional dependency,
//! so a default build has the seam and the wall clock and links no HLC.

#[cfg(feature = "hlc")]
use ash_time::HlcClock;

/// A source of the current time, in milliseconds since the Unix epoch.
pub trait Clock: Send + Sync {
    /// Milliseconds since the Unix epoch.
    fn now_millis(&self) -> i64;
}

/// A [`Clock`] backed by the OS wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// A [`Clock`] backed by the `ash-time` Hybrid Logical Clock, staying close to
/// wall time while remaining strictly monotone.
///
/// Requires the `hlc` feature.
#[cfg(feature = "hlc")]
#[derive(Default)]
pub struct AshTimeClock {
    inner: HlcClock,
}

#[cfg(feature = "hlc")]
impl AshTimeClock {
    /// Create a clock with a fresh HLC.
    pub fn new() -> Self {
        Self {
            inner: HlcClock::new(),
        }
    }
}

#[cfg(feature = "hlc")]
impl Clock for AshTimeClock {
    fn now_millis(&self) -> i64 {
        // HlcTimestamp.physical is nanoseconds since the Unix epoch.
        let ts = self.inner.now().unwrap_or_else(|_| self.inner.last());
        (ts.physical / 1_000_000) as i64
    }
}
