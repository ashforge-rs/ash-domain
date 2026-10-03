//! Orderly shutdown of a [`Domain`](crate::Domain): the [`Closable`] seam and
//! the close gate.
//!
//! A [`Domain`](crate::Domain) is an `Arc`-based registry + executor with no
//! runtime of its own — it never spawns, never owns a task. So "stop the domain"
//! cannot mean "kill in-flight work"; the domain does not run any. What it *can*
//! own is two things this module provides:
//!
//! 1. **A close gate** — a shared [`AtomicBool`], the exact shape of the existing
//!    `trace` gate. Once
//!    [`begin_close`](crate::Domain::begin_close) flips it, every new call to
//!    [`handle_action`](crate::Domain::handle_action) fails fast with
//!    [`Error::Closing`](crate::Error::Closing) **before** it does anything
//!    observable. It is a cooperative "stop accepting new work" signal, not a
//!    forced halt: work another thread has already entered runs to completion
//!    (the gate is checked at the entry, not mid-pipeline).
//!
//! 2. **A place for clients to release real resources.** The domain's shared
//!    clients ([`DomainContext`](crate::DomainContext)) are the things that hold
//!    OS/network state — a connection pool, a broker producer, a socket. The core
//!    stores them as opaque `Send + Sync + 'static` values in a type-keyed bag,
//!    so it cannot know how to shut an arbitrary one down. [`Closable`] is how a
//!    client *opts in*: register it as closable and
//!    [`close`](crate::Domain::close) will drive its shutdown.
//!
//! ## What close does — and does not — guarantee
//!
//! [`close`](crate::Domain::close) flips the gate, then closes each registered
//! [`Closable`] **in reverse registration order** (last-registered shut down
//! first, the usual teardown order), and returns once they have all returned.
//!
//! It makes **no** guarantee about post-commit egress. A committed action's
//! [`DomainEvent`](crate::event::DomainEvent) is already delivered *best-effort,
//! in-process, un-retried* — that is the documented event contract. Events that a
//! handler had not yet pushed to an external broker when close ran are **not**
//! drained or flushed by close, and **may be lost**. Durable delivery is the
//! consumer's responsibility, exactly as it is on the normal event path — and
//! the way to get it is [`EventHandler::stage`](crate::event::EventHandler::stage),
//! which records the event inside the write's own transaction, before the commit
//! and so before close can be in the way at all. Close shuts clients down; it does not become a delivery
//! guarantee the rest of the core deliberately does not make.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;

/// The shared on/off gate a [`Domain`](crate::Domain) owns to signal shutdown.
/// `Arc` so every clone of the domain observes the same flag; `Relaxed` is
/// sufficient — flipping it only gates *entry* to new work, it is not a
/// happens-before edge any data depends on.
pub(crate) type CloseGate = Arc<AtomicBool>;

/// A domain-scoped client that can be shut down when the domain closes.
///
/// The [`DomainContext`](crate::DomainContext) stores clients as opaque
/// `Send + Sync + 'static` values keyed by type, so the core cannot know how to
/// release an arbitrary one. Implement `Closable` on a client and register it
/// with [`DomainContext::with_closable`](crate::DomainContext::with_closable) to
/// opt it into shutdown; [`Domain::close`](crate::Domain::close) then drives it.
///
/// # Contract
///
/// - `close` takes `&self`, not `&mut self`: the domain holds its context behind
///   an `Arc`, and a real client (a pool, a producer) shuts down through its own
///   interior-shared handle anyway. Model shutdown as an idempotent operation on
///   a shared handle.
/// - `close` should be **idempotent** and safe to call once. The domain calls it
///   at most once per close, but a client shared elsewhere may see other signals.
/// - `close` should be **bounded** — it must return. The core imposes no timeout
///   (a supervised [`DomainWorker`](crate::flare::DomainWorker) is where a
///   deadline belongs); a `close` that blocks forever hangs shutdown.
/// - A `close` error is *reported*, not fatal: [`Domain::close`](crate::Domain::close)
///   still closes the remaining clients and collects every error, so one client's
///   failed teardown does not strand the others.
///
/// ```
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use std::sync::Arc;
/// use ash_domain::lifecycle::Closable;
/// use ash_domain::Result;
///
/// /// A pretend connection pool that records that it was shut down.
/// struct Pool { closed: Arc<AtomicBool> }
///
/// #[async_trait::async_trait]
/// impl Closable for Pool {
///     async fn close(&self) -> Result<()> {
///         self.closed.store(true, Ordering::SeqCst); // release connections…
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Closable: Send + Sync + 'static {
    /// Release this client's resources. See the [trait contract](Closable):
    /// `&self`, idempotent, bounded; an `Err` is reported but does not stop the
    /// domain closing its other clients.
    async fn close(&self) -> crate::error::Result<()>;
}

/// A registered [`Closable`], kept in an ordered list on the
/// [`DomainContext`](crate::DomainContext) so [`Domain::close`](crate::Domain::close)
/// can shut them down deterministically (reverse registration order).
///
/// Held as `Arc<dyn Closable>` — object-safe, `Send + Sync`, cheap to clone —
/// mirroring how every other seam in the crate is stored.
pub(crate) type ClosableHandle = Arc<dyn Closable>;

/// Read the close gate. Kept tiny and `#[inline]` so the open path (the common
/// case) is a single relaxed atomic load the branch predictor learns.
#[inline]
pub(crate) fn is_closing(gate: &AtomicBool) -> bool {
    gate.load(Ordering::Relaxed)
}
