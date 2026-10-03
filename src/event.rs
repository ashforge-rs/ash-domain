//! Domain events: the facts the [`Domain`](crate::Domain) produces.
//!
//! A [`DomainEvent`] is the record that *something happened in the domain* — "a
//! `note` was `created`", "an `account` was `closed`". The domain builds one
//! after every committed action and hands it to each registered
//! [`EventHandler`]. It is a **fact**, produced whether or not anyone is
//! listening and independent of what a consumer does with it.
//!
//! This is the root seam for reacting to state changes. The core produces the
//! event and ships **no** sink of its own — no message broker, no audit store,
//! no job runner. A consumer implements [`EventHandler`] over their own
//! infrastructure and decides, per event, what it becomes:
//!
//! * turn it into a [`Notification`](crate::publish::Notification) and publish it
//!   (see [`DomainEvent::to_notification`]),
//! * append it to an audit trail,
//! * enqueue a [`Job`](crate::publish::Job) for deferred work,
//! * or ignore it.
//!
//! None of those projections belong to the domain. The domain's responsibility
//! ends at *producing the event and handing it over*; persistence, transport,
//! and fan-out are entirely the consumer's.
//!
//! Delivery is **post-commit and best-effort** by default: the write already
//! stands when a handler runs, so a crash between the two loses the event. A
//! handler that cannot afford that implements [`EventHandler::stage`] instead —
//! the transactional-outbox seam, where the event is written *inside the write's
//! own transaction* through the [`DataLayer`] handed to it, and a relay the
//! consumer owns publishes from that table afterwards. The core gains no outbox
//! table, no relay, and no delivery machinery: it only offers the transaction it
//! already had.
//!
//! Events usually come from the commit path, but the [`Emitter`] lets any code
//! holding the domain's context produce one **out of band** — the same handlers,
//! reached from a [`HandlerContext`](crate::HandlerContext) or the
//! [`Domain`](crate::Domain) — for domain signals that aren't a CRUD commit.
//! Those events carry no commit guarantee; see [`Emitter`] and [`DomainEvent`].

use std::sync::Arc;

use async_trait::async_trait;

use crate::action::ActionKind;
use crate::clock::Clock;
use crate::datalayer::DataLayer;
use crate::error::Result;
use crate::value::{Record, Value};

/// A state change in the domain — the fact the [`Domain`](crate::Domain)
/// produces, handed to every registered [`EventHandler`].
///
/// It carries enough to route, render, or persist the fact without the receiver
/// re-reading storage: the resource and action names, the [`ActionKind`], the
/// acting principal, the tenant, the affected record(s), and a timestamp.
///
/// **Two origins, one shape.** On the commit path the domain builds it after a
/// successful read / write / generic action, so it is a *fact the domain vouches
/// for* (an action really committed) with actor/tenant filled from the
/// changeset. Via the [`Emitter`] it can also be produced **out of band** from
/// any code holding the domain's context — in that case it is only "some code
/// asked to emit this": no committed action stands behind it, and the fields are
/// exactly what the caller supplied. A handler that must distinguish the two can
/// key on the `action` name it agrees with producers to use.
///
/// It is not itself a notification, an audit entry, or a message — those are
/// projections a consumer may derive from it. See
/// [`to_notification`](DomainEvent::to_notification).
#[derive(Clone, Debug)]
pub struct DomainEvent {
    /// The resource the action ran on.
    pub resource: String,
    /// The action name.
    pub action: String,
    /// The action kind.
    pub kind: ActionKind,
    /// The acting principal, if one was set on the context.
    pub actor: Option<Record>,
    /// The tenant the action was scoped to, if any.
    pub tenant: Option<Value>,
    /// The record(s) the action produced or affected: the created/updated row,
    /// the destroyed row's prior state, or empty for a generic action with no
    /// record result.
    pub records: Vec<Record>,
    /// When the action committed, in milliseconds since the Unix epoch, read
    /// from the domain's [`Clock`]. A domain fact carries when it
    /// happened, so a consumer can order or timestamp it without a clock of
    /// their own.
    pub at: i64,
}

impl DomainEvent {
    /// A domain event for a single-record action, with an explicit commit time.
    pub fn new(
        resource: impl Into<String>,
        action: impl Into<String>,
        kind: ActionKind,
        record: Record,
        at: i64,
    ) -> Self {
        Self {
            resource: resource.into(),
            action: action.into(),
            kind,
            actor: None,
            tenant: None,
            records: vec![record],
            at,
        }
    }

    /// Project this event into a publishable
    /// [`Notification`](crate::publish::Notification) — the routing shape a
    /// consumer broadcasts. This is one thing an event may *become*; the domain
    /// never does it, so it happens only when a consumer's [`EventHandler`]
    /// calls it.
    pub fn to_notification(&self) -> crate::publish::Notification {
        self.into()
    }
}

/// Receives a [`DomainEvent`] after an action commits.
///
/// Registered on a [`Domain`](crate::Domain); the domain calls
/// [`handle`](EventHandler::handle) for every registered handler once an action
/// has been persisted. Implement it on your own type to do whatever the event
/// should become — project it to a
/// [`Notification`](crate::publish::Notification) and publish, append it to an
/// audit trail, enqueue a [`Job`](crate::publish::Job), call a webhook, update a
/// cache — or persist the raw event for later replay.
///
/// # Delivery contract — best-effort, after the fact
///
/// A handler runs **after the action has already committed** to storage, and the
/// write **cannot be rolled back** (the core has no transaction seam). Handlers
/// are invoked in registration order; the **first** one to return `Err` stops
/// the scan — **later handlers do not run** — and its error propagates as the
/// action's result. So a `create`/`update`/`destroy`/`run` call can return `Err`
/// while the row is nonetheless committed and readable: the error signals "a
/// post-commit projection failed", *not* "the action was rolled back".
///
/// Delivery is **in-process and un-retried**: the domain does not persist an
/// outbox, retry, or reorder — that is a consumer concern (enqueue a
/// [`Job`](crate::publish::Job) from the handler if you need durability). A
/// handler that must not block the action, or must not prevent handlers
/// registered after it from running, must **swallow its own failures** and
/// return `Ok(())` (log/queue the failure inside the handler).
///
/// ```
/// use ash_domain::event::{DomainEvent, EventHandler};
/// use ash_domain::Result;
///
/// struct LogEvents;
///
/// #[async_trait::async_trait]
/// impl EventHandler for LogEvents {
///     async fn handle(&self, event: &DomainEvent) -> Result<()> {
///         // A real handler would persist, publish, or enqueue; this observes.
///         let _ = (&event.resource, &event.action, event.at);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Handle a committed-action domain event.
    ///
    /// Post-commit, in-process, un-retried, best-effort — the write already
    /// stands when this runs, and returning `Err` does not undo it. For delivery
    /// that cannot be lost, see [`stage`](EventHandler::stage).
    async fn handle(&self, event: &DomainEvent) -> Result<()>;

    /// Record `event` **inside the still-open write transaction**, before the
    /// commit — the transactional-outbox seam.
    ///
    /// This is the answer to the one atomicity gap the core otherwise leaves
    /// open: [`handle`](EventHandler::handle) runs after the commit, so a
    /// process that dies in between commits the row and loses the event. A
    /// handler that instead writes the event into an outbox *table* through the
    /// `txn` handed here commits the row and the event together — atomically,
    /// because it is literally the same transaction — and a relay process it
    /// owns publishes from that table afterwards.
    ///
    /// `txn` is the transaction's own [`DataLayer`] view, so an outbox row is
    /// written with the ordinary
    /// [`create`](DataLayer::create) the layer already implements — the core
    /// gains no outbox schema, no relay, and no delivery machinery. What the
    /// event becomes is entirely the consumer's, exactly as with `handle`.
    ///
    /// Returning `Err` **rolls the write back**: staging runs before the commit,
    /// so a failure here means the action did not happen, rather than a row
    /// standing with no event to announce it.
    ///
    /// The default does nothing. A handler that overrides it must also return
    /// `true` from [`stages`](EventHandler::stages) — that flag is what lets the
    /// domain refuse, loudly, to run against a store with no transaction to join
    /// rather than silently skipping the staging the deployment asked for.
    async fn stage(&self, event: &DomainEvent, txn: &dyn DataLayer) -> Result<()> {
        let _ = (event, txn);
        Ok(())
    }

    /// Whether this handler stages events into the write transaction (see
    /// [`stage`](EventHandler::stage)). Default `false`.
    ///
    /// A handler that returns `true` is declaring that its delivery guarantee
    /// depends on a transaction. If the [`Store`](crate::Store) in use offers
    /// none, the domain fails the write with
    /// [`Error::Unsupported`](crate::Error::Unsupported) instead of quietly
    /// giving the handler weaker guarantees than it asked for.
    fn stages(&self) -> bool {
        false
    }
}

/// A cheap, cloneable handle for emitting [`DomainEvent`]s **out of band** — from
/// anywhere holding the domain's shared context, not only from the commit path.
///
/// The [`Domain`](crate::Domain) builds one at construction over its registered
/// [`EventHandler`]s and its [`Clock`], and **registers it in the
/// [`DomainContext`](crate::DomainContext)** as a shared client. So any code that
/// can reach that context can emit:
///
/// - a [`HandlerContext`](crate::HandlerContext) (a generic action's handler):
///   `ctx.client::<Emitter>()`;
/// - service code holding the [`Domain`](crate::Domain): [`domain.emitter()`](crate::Domain::emitter)
///   or `domain.domain_context().client::<Emitter>()`.
///
/// Emitted events flow to the **same** registered handlers as commit-path events,
/// in registration order, with the same fail-closed-on-first-`Err` semantics (see
/// [`emit`](Emitter::emit)).
///
/// # These events are not commit facts
///
/// A commit-path [`DomainEvent`] is a *fact the domain vouches for*: it fired
/// because an action committed. An `Emitter` event is only "some code asked to
/// emit this" — **there is no tie to a committed action, and the fields carry
/// exactly what the caller supplied** (no actor/tenant is inferred). Reach for it
/// for domain signals that aren't a CRUD commit (a saga step, a scheduled tick, a
/// cross-cutting notification); don't use it to fake a persistence event.
///
/// `Emitter` is `Send + Sync + Clone` (it holds only `Arc`s), so it crosses
/// threads and `await` points freely.
#[derive(Clone)]
pub struct Emitter {
    handlers: Arc<Vec<Arc<dyn EventHandler>>>,
    clock: Arc<dyn Clock>,
}

impl Emitter {
    /// Build an emitter over a handler list and a clock. The
    /// [`Domain`](crate::Domain) calls this for you and registers the result in
    /// the [`DomainContext`](crate::DomainContext); construct one by hand only for
    /// a bespoke setup or a test.
    pub fn new(handlers: Arc<Vec<Arc<dyn EventHandler>>>, clock: Arc<dyn Clock>) -> Self {
        Self { handlers, clock }
    }

    /// Hand `event` to every registered [`EventHandler`], in registration order.
    ///
    /// Same delivery contract as the commit path: **in-process, un-retried**, and
    /// the **first** handler to return `Err` stops the scan (later handlers do not
    /// run) and that error is returned to the caller. Unlike the commit path there
    /// is no write to stand behind it — the caller decides what a failed emit
    /// means (retry, log, propagate). Returns `Ok(())` when no handler is
    /// registered (the event has no observer).
    pub async fn emit(&self, event: DomainEvent) -> Result<()> {
        for handler in self.handlers.iter() {
            handler.handle(&event).await?;
        }
        Ok(())
    }

    /// Build a [`DomainEvent`] from its parts — stamping `at` from the emitter's
    /// [`Clock`] — and [`emit`](Emitter::emit) it. A convenience for
    /// the common case where the caller doesn't hold a pre-built event.
    ///
    /// `actor` and `tenant` are left unset (an out-of-band event infers neither);
    /// set them on a hand-built [`DomainEvent`] passed to [`emit`](Emitter::emit)
    /// if you need them.
    pub async fn emit_now(
        &self,
        resource: impl Into<String>,
        action: impl Into<String>,
        kind: ActionKind,
        records: Vec<Record>,
    ) -> Result<()> {
        let event = DomainEvent {
            resource: resource.into(),
            action: action.into(),
            kind,
            actor: None,
            tenant: None,
            records,
            at: self.clock.now_millis(),
        };
        self.emit(event).await
    }

    /// How many [`EventHandler`]s this emitter fans out to.
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    /// Every registered handler, in registration order. The
    /// [`Domain`](crate::Domain) walks these to run the pre-commit
    /// [`stage`](EventHandler::stage) pass.
    pub fn handlers(&self) -> &[Arc<dyn EventHandler>] {
        &self.handlers
    }

    /// Whether any registered handler stages events into the write transaction
    /// (see [`EventHandler::stages`]).
    pub fn any_stages(&self) -> bool {
        self.handlers.iter().any(|h| h.stages())
    }
}

impl std::fmt::Debug for Emitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Emitter")
            .field("handlers", &self.handlers.len())
            .finish()
    }
}
