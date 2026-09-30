//! Execution-trace instrumentation for the executor.
//!
//! This module is the seam between the pipeline in [`domain`](crate::domain) and
//! the `tracing` ecosystem. It exists so the executor can be instrumented once,
//! at clean call sites, while emission stays **doubly opt-out**:
//!
//! 1. **Compile-time** — without the `trace` cargo feature, `tracing` is not a
//!    dependency and every macro here expands to a no-op that only touches the
//!    gate reference (so there are no unused-variable warnings). Nothing is
//!    emitted, nothing is linked.
//! 2. **Runtime** — with the feature on, emission is still gated by an
//!    [`AtomicBool`](std::sync::atomic::AtomicBool) the [`Domain`](crate::Domain)
//!    owns and flips through
//!    [`enable_tracing`](crate::Domain::enable_tracing) /
//!    [`disable_tracing`](crate::Domain::disable_tracing). The gate starts
//!    **off**, so an instrumented build stays silent until the consumer asks for
//!    the trace.
//!
//! The domain only ever *emits*; where the trace goes is the consumer's call,
//! made by installing a `tracing` subscriber. See
//! [`Domain::enable_tracing`](crate::Domain::enable_tracing) for the full model.
//!
//! ## Discipline: structure, never payloads
//!
//! Trace events record the **shape** of an action — resource, action name, the
//! authorization decision, counts — and never attribute *values*. A span that
//! captured a [`Record`](crate::Record) or a param bag by `Debug` would put
//! potentially sensitive, **un-redacted** data into the trace, turning a
//! diagnostics tool into a redaction bypass. Every call site passes only
//! non-sensitive fields, mirroring how the audit extension sanitizes its params.

use std::sync::atomic::AtomicBool;
#[cfg(feature = "trace")]
use std::sync::atomic::Ordering;

/// The shared on/off gate a [`Domain`](crate::Domain) owns. `Arc` so every clone
/// of the domain observes the same flag; `Relaxed` is sufficient — this gates
/// diagnostics, not a happens-before relationship anything depends on.
pub(crate) type TraceGate = std::sync::Arc<AtomicBool>;

/// Read the gate. Kept tiny and `#[inline]` so the disabled path is a single
/// atomic load the branch predictor learns immediately. Only the feature-on
/// macros call it; without the feature the instrumentation is compiled out.
#[cfg(feature = "trace")]
#[inline]
pub(crate) fn enabled(gate: &AtomicBool) -> bool {
    gate.load(Ordering::Relaxed)
}

/// Open an action **span** covering the whole pipeline run, if the gate is on.
///
/// Returns an entered-span guard (feature on) or `()` (feature off); either way
/// the caller binds it with `let _span = trace_span!(...)` and it drops at the
/// end of the pipeline. Fields must be non-sensitive (names, kinds, counts).
///
/// ```ignore
/// let _span = trace_span!(&self.trace_gate, "action", resource = R::NAME, action = name);
/// ```
#[cfg(feature = "trace")]
macro_rules! trace_span {
    ($gate:expr, $name:expr, $($field:tt)*) => {{
        if $crate::trace::enabled($gate) {
            let span = ::tracing::info_span!($name, $($field)*);
            Some(span.entered())
        } else {
            None
        }
    }};
}

/// A zero-sized stand-in for a span guard when the `trace` feature is off, so the
/// caller's `let _span = trace_span!(...)` binds a real (droppable) value rather
/// than a bare `()` — which keeps clippy quiet on the disabled path.
#[cfg(not(feature = "trace"))]
pub(crate) struct DisabledSpan;

/// Feature-off form: no `tracing`, no span — just touch the gate so it is never
/// an unused field, and yield a [`DisabledSpan`] the caller can bind and drop.
#[cfg(not(feature = "trace"))]
macro_rules! trace_span {
    ($gate:expr, $name:expr, $($field:tt)*) => {{
        let _ = $gate;
        $crate::trace::DisabledSpan
    }};
}

/// Emit a pipeline **event** — a single moment in the action (`staged`,
/// `authorized`, `denied`, `persisted`, …) — if the gate is on. Fields must be
/// non-sensitive.
///
/// ```ignore
/// trace_event!(&self.trace_gate, "authorized", decision = "allow");
/// trace_event!(&self.trace_gate, "denied", reason = %err);
/// ```
#[cfg(feature = "trace")]
macro_rules! trace_event {
    ($gate:expr, $name:expr $(, $($field:tt)*)?) => {{
        if $crate::trace::enabled($gate) {
            ::tracing::info!(step = $name $(, $($field)*)?);
        }
    }};
}

/// Feature-off form: no emission; touch the gate only.
#[cfg(not(feature = "trace"))]
macro_rules! trace_event {
    ($gate:expr, $name:expr $(, $($field:tt)*)?) => {{
        let _ = $gate;
    }};
}

pub(crate) use {trace_event, trace_span};
