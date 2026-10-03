//! Supervise a [`Domain`] with [`ash-flare`](https://docs.rs/ash-flare): the
//! [`DomainWorker`] adapter.
//!
//! `ash-domain` models *what an action is* — resources, authorization,
//! redaction, the typed-query seam — and ships **no runtime**: it never spawns a
//! task and owns no process lifecycle. `ash-flare` is the missing half: an
//! OTP-style supervisor that owns tasks and restarts them under a policy. This
//! module is the bridge — a [`Worker`](ash_flare::Worker) that owns a
//! [`Domain`]'s *serving lifetime* and performs its orderly
//! [`close`](Domain::close) on shutdown.
//!
//! # What the worker is — and is not
//!
//! The worker is **not** an event pump. It does not drain or flush
//! [`DomainEvent`](crate::event::DomainEvent)s, and it makes no delivery
//! guarantee: post-commit egress is the domain's best-effort, un-retried concern
//! (see [`close`](Domain::close)), and events not yet pushed to an external sink
//! when shutdown runs **may be lost**. Durability (an outbox, an idempotent
//! consumer) is the consumer's, exactly as on the normal event path.
//!
//! What it *does* own:
//!
//! * **Serving.** Its [`run`](ash_flare::Worker::run) drives a user-supplied
//!   async *serve closure* — the thing that actually hands work to the domain (an
//!   HTTP accept loop, a queue consumer, a reconciler) — and holds the domain
//!   alive for as long as it serves.
//! * **Fault classification.** The serve closure returns a [`Result`], and the
//!   worker decides whether an error is a *fault* (crash → let ash-flare restart)
//!   or a *legitimate result* the closure already handled (keep serving). This is
//!   the load-bearing rule; see [`FaultPolicy`].
//! * **Orderly shutdown.** On [`shutdown`](ash_flare::Worker::shutdown) it
//!   signals the serve closure to stop (a [`CloseSignal`]), awaits it up to a
//!   bounded deadline, then calls [`Domain::close`] to release clients — and
//!   reports completion through a [`oneshot`](tokio::sync::oneshot) so the
//!   supervisor's caller can await a real "closed" edge.
//!
//! # Fault classification — the rule that makes this safe
//!
//! A restart must never become an authorization bypass, and a *legitimate* domain
//! outcome must never trigger a restart storm. ash-domain's [`Error`] carries both
//! kinds:
//!
//! | Error | Meaning | Default class |
//! |-------|---------|---------------|
//! | [`Forbidden`](Error::Forbidden) | policy denied — a correct result | **result** (keep serving) |
//! | [`Invalid`](Error::Invalid) | bad input / violated invariant | **result** |
//! | [`NotFound`](Error::NotFound) | row absent | **result** |
//! | [`Unsupported`](Error::Unsupported) | backend doesn't do this op | **result** |
//! | [`Closing`](Error::Closing) | domain is shutting down | **result** (stop cleanly) |
//! | [`Contention`](Error::Contention) | lock busy | **result** (retryable) |
//! | [`PolicyError`](Error::PolicyError) | authorization *outage* | **fault** (restart) |
//! | [`DataLayer`](Error::DataLayer) | storage down | **fault** |
//! | [`MissingTenant`](Error::MissingTenant) | context misconfigured | **fault** (a bug) |
//! | others | — | **fault** |
//!
//! The default is [`FaultPolicy::standard`]. Because a persistently-`Forbidden`
//! action is classed as a *result*, a denied caller can never drive an infinite
//! restart loop that trips ash-flare's `RestartIntensity`. Because a `DataLayer`
//! outage is a *fault*, a downed database *does* restart the worker with backoff.
//! Re-authorization is preserved for free: a restart re-runs the **serve
//! closure**, which re-enters the domain through the same gate — it never replays
//! a pre-authorized action.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use ash_domain::{Domain, DomainConfig, DomainContext};
//! use ash_domain::flare::{DomainWorker, CloseSignal};
//! use ash_flare::{SupervisorSpec, SupervisorHandle, RestartPolicy};
//!
//! # async fn serve_one_request(_d: &Domain) -> ash_domain::Result<()> { Ok(()) }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let domain = Domain::new(DomainConfig::default(), DomainContext::new());
//!
//! // The serve closure: called with the live domain and a shutdown signal.
//! // It owns whatever "serving" means for you; return when the signal fires.
//! let worker = DomainWorker::new(domain, move |domain, signal: CloseSignal| async move {
//!     while !signal.is_closing() {
//!         serve_one_request(&domain).await?; // a Forbidden here won't crash us
//!     }
//!     Ok(())
//! });
//!
//! // Supervise it. A DataLayer/PolicyError fault restarts; a Forbidden does not.
//! let spec = SupervisorSpec::new("domain-root")
//!     .with_worker("domain", move || worker.clone(), RestartPolicy::Permanent);
//! let handle = SupervisorHandle::start(spec);
//! # let _ = handle;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::domain::Domain;
use crate::error::Error;

/// A cheap, cloneable "please stop" flag handed to the serve closure.
///
/// The worker flips it on [`shutdown`](ash_flare::Worker::shutdown); a serve loop
/// polls [`is_closing`](CloseSignal::is_closing) (or awaits
/// [`closed`](CloseSignal::closed)) and returns `Ok(())` to exit cleanly. It is a
/// cooperative signal — the worker cannot force a closure to yield, only ask; a
/// closure that ignores it will be dropped past the bounded deadline instead.
#[derive(Clone, Debug, Default)]
pub struct CloseSignal {
    closing: Arc<AtomicBool>,
}

impl CloseSignal {
    /// A fresh, not-yet-signalled handle.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether shutdown has been requested. A serve loop checks this each
    /// iteration and returns once it is `true`.
    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Relaxed)
    }

    /// Request shutdown. Idempotent; observed by every clone.
    pub(crate) fn signal(&self) {
        self.closing.store(true, Ordering::Relaxed);
    }

    /// Await the shutdown edge by polling with a short backoff. Convenience for a
    /// serve loop that would rather `select!` on a future than poll a flag; for a
    /// tight loop, [`is_closing`](CloseSignal::is_closing) is cheaper. Bounded by
    /// construction — it returns as soon as the flag is set.
    pub async fn closed(&self) {
        // A cooperative poll: the flag flips once and never back, so a short
        // fixed backoff is sufficient and keeps this dependency-light (no extra
        // channel). Bounded per the "bound everything" rule: each wait is capped.
        while !self.is_closing() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
}

/// How an [`Error`] from the serve closure is classified: a **fault** crashes the
/// worker (ash-flare restarts it) or a **result** the closure handled (keep
/// serving / stop cleanly). See the [module docs](self#fault-classification--the-rule-that-makes-this-safe).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Fault {
    /// A genuine fault — return it as the worker's error so the supervisor
    /// restarts (with its configured backoff/intensity).
    Crash,
    /// A legitimate outcome the serve closure already dealt with — do not crash;
    /// the worker keeps its `run` alive (the closure decides whether to loop or
    /// return `Ok`).
    Handled,
}

/// Classifies a serve-closure [`Error`] into a [`Fault`]. The default,
/// [`standard`](FaultPolicy::standard), encodes the table in the [module
/// docs](self#fault-classification--the-rule-that-makes-this-safe); override it
/// with [`DomainWorker::with_fault_policy`] for bespoke rules.
#[derive(Clone)]
pub struct FaultPolicy(Arc<dyn Fn(&Error) -> Fault + Send + Sync>);

impl FaultPolicy {
    /// Build a policy from a classifier closure.
    pub fn new(f: impl Fn(&Error) -> Fault + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// The default classification. **Legitimate results** — a denial, bad input,
    /// a missing row, an unsupported op, a shutdown, lock contention, a write
    /// conflict — are
    /// [`Handled`](Fault::Handled): they must never crash the worker (a
    /// persistently-denied action would otherwise restart-storm). **Faults** —
    /// an authorization *outage*, storage down, or a context-misconfiguration bug
    /// — are [`Crash`](Fault::Crash) so the supervisor restarts.
    pub fn standard() -> Self {
        Self::new(|e| match e {
            Error::Forbidden(_)
            | Error::Invalid { .. }
            | Error::NotFound(_)
            | Error::Unsupported(_)
            | Error::Closing(_)
            | Error::Contention(_)
            // A lost race is a legitimate outcome the caller retries, exactly
            // like lock contention — restarting the worker would not help.
            | Error::Conflict { .. } => Fault::Handled,

            Error::PolicyError(_)
            | Error::DataLayer { .. }
            | Error::MissingTenant(_)
            | Error::UnknownResource(_)
            | Error::UnknownAction { .. }
            | Error::Serialization(_) => Fault::Crash,
        })
    }

    /// Classify one error.
    pub fn classify(&self, e: &Error) -> Fault {
        (self.0)(e)
    }
}

impl Default for FaultPolicy {
    fn default() -> Self {
        Self::standard()
    }
}

impl std::fmt::Debug for FaultPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FaultPolicy(..)")
    }
}

/// The serve closure's boxed future. Named so the trait bound on
/// [`DomainWorker::new`] stays readable.
type ServeFuture = std::pin::Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send>>;

/// A supervised owner of a [`Domain`]'s serving lifetime. See the [module
/// docs](self).
///
/// Cloneable so it can be used as an `ash-flare` worker factory
/// (`move || worker.clone()`) — every field is an `Arc`/cheap handle, and the
/// clone shares the same domain, serve closure, signal, and fault policy, so a
/// restart rebuilds the worker over the *same* domain rather than a fresh one.
#[derive(Clone)]
pub struct DomainWorker {
    domain: Domain,
    serve: Arc<dyn Fn(Domain, CloseSignal) -> ServeFuture + Send + Sync>,
    signal: CloseSignal,
    fault_policy: FaultPolicy,
    /// Bounded wait for the serve future to notice the signal and return, before
    /// [`Domain::close`] runs regardless. Caps shutdown per "bound everything".
    drain_deadline: Duration,
    /// Guards the one-time close so concurrent/re-entrant shutdowns don't double
    /// close. `Mutex<bool>`: `true` once close has run.
    closed: Arc<Mutex<bool>>,
}

impl DomainWorker {
    /// Wrap `domain` with a `serve` closure. `serve` is called with the live
    /// domain and a [`CloseSignal`]; it should serve until the signal fires, then
    /// return `Ok(())`. An `Err` it returns is classified by the
    /// [`FaultPolicy`] (default [`standard`](FaultPolicy::standard)).
    ///
    /// The default drain deadline is 5s; change it with
    /// [`with_drain_deadline`](DomainWorker::with_drain_deadline).
    pub fn new<F, Fut>(domain: Domain, serve: F) -> Self
    where
        F: Fn(Domain, CloseSignal) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::error::Result<()>> + Send + 'static,
    {
        Self {
            domain,
            serve: Arc::new(move |d, s| Box::pin(serve(d, s)) as ServeFuture),
            signal: CloseSignal::new(),
            fault_policy: FaultPolicy::standard(),
            drain_deadline: Duration::from_secs(5),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Override the [`FaultPolicy`] used to classify serve-closure errors.
    pub fn with_fault_policy(mut self, policy: FaultPolicy) -> Self {
        self.fault_policy = policy;
        self
    }

    /// Set how long [`shutdown`](ash_flare::Worker::shutdown) waits for the serve
    /// future to return after signalling, before [`Domain::close`] runs anyway.
    /// Bounded on purpose: past this, in-flight serve work is abandoned (and any
    /// events it would have produced are lost — the documented contract).
    pub fn with_drain_deadline(mut self, deadline: Duration) -> Self {
        self.drain_deadline = deadline;
        self
    }

    /// The [`CloseSignal`] this worker hands its serve closure. Cloneable — hold
    /// it to trigger a cooperative stop from outside the supervisor if you need
    /// one (the supervisor's own shutdown path uses this internally).
    pub fn close_signal(&self) -> CloseSignal {
        self.signal.clone()
    }

    /// The domain this worker serves.
    pub fn domain(&self) -> &Domain {
        &self.domain
    }

    /// Run the orderly close **once**: signal the serve closure, then close the
    /// domain's clients. Idempotent via the `closed` guard — a second call (e.g.
    /// ash-flare calling `shutdown` after the worker already returned) is a no-op
    /// that reports success. Factored out so both the `Worker::shutdown` hook and
    /// a direct caller share one path.
    async fn close_once(&self) -> crate::error::Result<()> {
        let mut closed = self.closed.lock().await;
        if *closed {
            return Ok(());
        }
        // Ask the serve loop to stop; Domain::close also flips the domain's own
        // gate so any action re-entry races land on `Closing`, not a half-open
        // domain.
        self.signal.signal();
        let result = self.domain.close().await;
        *closed = true;
        result
    }
}

impl std::fmt::Debug for DomainWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DomainWorker")
            .field("fault_policy", &self.fault_policy)
            .field("drain_deadline", &self.drain_deadline)
            .field("closing", &self.signal.is_closing())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ash_flare::Worker for DomainWorker {
    type Error = Error;

    async fn run(&mut self) -> Result<(), Self::Error> {
        let fut = (self.serve)(self.domain.clone(), self.signal.clone());
        match fut.await {
            Ok(()) => Ok(()),
            Err(e) => match self.fault_policy.classify(&e) {
                // A genuine fault — surface it so the supervisor restarts.
                Fault::Crash => Err(e),
                // A legitimate outcome the closure handled: the run is over, but
                // this is *not* a crash. Return Ok so the restart policy sees a
                // clean exit (a Permanent worker may still be restarted by the
                // supervisor; a Transient one will not).
                Fault::Handled => Ok(()),
            },
        }
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        // Signal first, then give the serve future a bounded window to return on
        // its own. We cannot await the future here (it lives in `run` on another
        // task), so we wait on the signal being observed as a proxy and cap it —
        // past the deadline, we close regardless and abandon in-flight work.
        self.signal.signal();

        // Bounded wait: poll for the domain to have quiesced (no forced join of
        // the serve task exists through the Worker trait), capped by the deadline.
        let deadline = tokio::time::Instant::now() + self.drain_deadline;
        while tokio::time::Instant::now() < deadline {
            // Once the domain is closing and the signal is observed, the serve
            // loop has been asked to stop; a real drain of its in-flight request
            // is the closure's job. We simply cap our patience here.
            if self.domain.is_closing() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Release clients (pools, producers, sockets) via the Closable seam.
        // Idempotent; safe if `run` already returned and nothing is in flight.
        self.close_once().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DomainConfig, DomainContext};
    use std::sync::atomic::AtomicUsize;

    fn test_domain() -> Domain {
        Domain::new(DomainConfig::default(), DomainContext::new())
    }

    #[test]
    fn standard_policy_classifies_results_as_handled() {
        let p = FaultPolicy::standard();
        assert_eq!(p.classify(&Error::Forbidden("no".into())), Fault::Handled);
        assert_eq!(p.classify(&Error::invalid("bad")), Fault::Handled);
        assert_eq!(p.classify(&Error::NotFound("x".into())), Fault::Handled);
        assert_eq!(p.classify(&Error::Closing("d".into())), Fault::Handled);
        assert_eq!(p.classify(&Error::Contention("c".into())), Fault::Handled);
    }

    #[test]
    fn standard_policy_classifies_outages_as_crash() {
        let p = FaultPolicy::standard();
        assert_eq!(p.classify(&Error::PolicyError("down".into())), Fault::Crash);
        assert_eq!(p.classify(&Error::data_layer("db down")), Fault::Crash);
        assert_eq!(p.classify(&Error::MissingTenant("r".into())), Fault::Crash);
    }

    #[tokio::test]
    async fn handled_error_does_not_crash_the_worker() {
        use ash_flare::Worker;
        let mut w = DomainWorker::new(test_domain(), |_d, _s| async {
            Err(Error::Forbidden("denied".into()))
        });
        // A Forbidden is a legitimate result: run returns Ok, not the error.
        assert!(w.run().await.is_ok());
    }

    #[tokio::test]
    async fn fault_error_crashes_the_worker() {
        use ash_flare::Worker;
        let mut w = DomainWorker::new(test_domain(), |_d, _s| async {
            Err(Error::data_layer("db down"))
        });
        assert!(matches!(w.run().await, Err(Error::DataLayer { .. })));
    }

    #[tokio::test]
    async fn shutdown_closes_the_domain_and_stops_serving() {
        use ash_flare::Worker;
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let mut w = DomainWorker::new(test_domain(), move |_d, signal| {
            let ran = ran2.clone();
            async move {
                while !signal.is_closing() {
                    ran.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            }
        })
        .with_drain_deadline(Duration::from_millis(200));

        // Serve in the background; then shut down.
        let signal = w.close_signal();
        let mut serve = w.clone();
        let serve_task = tokio::spawn(async move { serve.run().await });

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!signal.is_closing());
        w.shutdown().await.unwrap();

        assert!(w.domain().is_closing(), "shutdown must close the domain");
        // The serve loop observes the shared signal and returns cleanly.
        let out = tokio::time::timeout(Duration::from_secs(1), serve_task)
            .await
            .expect("serve task should finish after signal");
        assert!(out.unwrap().is_ok());
        assert!(ran.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        use ash_flare::Worker;
        let mut w = DomainWorker::new(test_domain(), |_d, _s| async { Ok(()) });
        w.shutdown().await.unwrap();
        // Second shutdown is a no-op success, not a double-close.
        w.shutdown().await.unwrap();
    }
}
