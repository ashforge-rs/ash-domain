//! The [`Metrics`] seam — a minimal hook for observing action outcomes.
//!
//! The core ships **no metrics backend** and no timing machinery. It exposes one
//! thing: a place to be told *"an action finished, and here is its shape and
//! outcome."* Everything downstream — counters, histograms, labels, a Prometheus
//! / StatsD / OpenTelemetry exporter, log lines, nothing at all — is the
//! consumer's, implemented on their own type behind this trait.
//!
//! This is deliberately smaller than the `trace` seam. Trace
//! emits the *stages* of the pipeline into the `tracing` ecosystem (a fixed
//! dependency, gated by a cargo feature). Metrics emits a single **per-action
//! outcome** into a trait object the consumer supplies, so there is no feature
//! flag, no extra dependency, and — when no [`Metrics`] is registered — no work
//! at all (the domain holds `Option<Arc<dyn Metrics>>` and skips the call).
//!
//! ## Discipline: shape, never payloads
//!
//! Like trace, a [`MetricSample`] carries only the **non-sensitive shape** of an
//! action — the resource and action names, the [`ActionKind`], the
//! [`Outcome`], and a duration the consumer may record. It never carries a
//! [`Record`](crate::Record), a param bag, or an actor, so wiring up metrics can
//! never turn into a data-exfiltration path.
//!
//! ```
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use ash_domain::metrics::{Metrics, MetricSample, Outcome};
//!
//! /// A trivial backend: count committed vs denied actions.
//! #[derive(Default)]
//! struct Counters { committed: AtomicU64, denied: AtomicU64 }
//!
//! impl Metrics for Counters {
//!     fn record(&self, sample: MetricSample<'_>) {
//!         match sample.outcome {
//!             Outcome::Committed => { self.committed.fetch_add(1, Ordering::Relaxed); }
//!             Outcome::Denied => { self.denied.fetch_add(1, Ordering::Relaxed); }
//!             Outcome::Errored => {}
//!         }
//!     }
//! }
//! ```

use std::time::Duration;

use crate::action::ActionKind;

/// How an action finished — the one axis a metrics backend usually splits on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The action ran to completion (a write persisted, a read returned). Note a
    /// committed write may still have returned an `Err` to the caller from
    /// post-commit event delivery — this reflects the *pipeline* outcome.
    Committed,
    /// The authorization gate refused the action ([`Forbidden`](crate::Error::Forbidden)
    /// or [`PolicyError`](crate::Error::PolicyError)). Distinguished from
    /// [`Errored`](Outcome::Errored) so a backend can chart denials separately.
    Denied,
    /// The action failed for a non-authorization reason (invalid input, a data
    /// layer error, an unsupported operation).
    Errored,
}

/// One observation handed to a [`Metrics`] backend when an action finishes: the
/// action's shape, how it ended, and how long the pipeline took.
///
/// Non-sensitive by construction — names, a kind, an outcome, a duration. See the
/// [module docs](self) on the shape-not-payloads discipline.
#[derive(Clone, Copy, Debug)]
pub struct MetricSample<'a> {
    /// The resource the action ran on.
    pub resource: &'a str,
    /// The action name.
    pub action: &'a str,
    /// The action kind (write / read / generic).
    pub kind: ActionKind,
    /// How the action finished.
    pub outcome: Outcome,
    /// Wall-clock time the pipeline took, measured by the domain from the start of
    /// the action to this sample.
    pub duration: Duration,
}

/// A consumer-supplied sink for per-action [`MetricSample`]s.
///
/// Register one on a [`Domain`](crate::Domain) via
/// [`DomainConfig::metrics`](crate::DomainConfig::metrics); the domain then calls
/// [`record`](Metrics::record) once per action as it finishes. With no `Metrics`
/// registered the domain does nothing — there is no default backend.
///
/// The domain only ever *emits*; the implementation decides what a sample
/// becomes. It must be `Send + Sync` (the domain is shared across threads) and
/// should be cheap and non-blocking — `record` runs on the action's own path.
pub trait Metrics: Send + Sync {
    /// Observe a finished action. Called once per action; keep it fast and
    /// infallible (swallow backend errors — a metrics failure must never affect
    /// the action's result).
    fn record(&self, sample: MetricSample<'_>);
}
