//! The [`Extension`] system — how `ash-*` crates attach to a resource.
//!
//! An extension observes and augments actions through lifecycle hooks the
//! [`Domain`](crate::Domain) invokes around every action. This is the seam that
//! turns the ecosystem's standalone primitives into resource features:
//!
//! | Extension | ash-* crate | Hook |
//! |-----------|-------------|------|
//! | state-machine guard ([`fsm`]) | `ash-fsm` | `before_action` validates the transition |
//! | idempotency | `ash-stamp` | `before_action` dedupes by key |
//! | paper trail / audit ([`audit`]) | `ash-log` | `after_action` records the change |
//! | single-writer | `ash-lock` | `before_action` takes a per-record lease |
//!
//! This crate ships two real extensions — the `ash-fsm` state-machine guard and
//! the `ash-log` audit trail — to prove the model; the rest are documented
//! attachment points.

use std::any::Any;

use async_trait::async_trait;

use crate::action::{ActionResult, Changeset};
use crate::error::Result;
use crate::query::Query;
use crate::value::Record;

// The shipped extensions are opt-in: each pulls an `ash-*` crate that a default
// build does not link. The `Extension` seam itself is always here — these are
// three implementations of it, not the mechanism.
#[cfg(feature = "audit")]
pub mod audit;
#[cfg(feature = "fsm")]
pub mod fsm;
#[cfg(feature = "lock")]
pub mod lock;

/// An opaque resource an [`Extension`] acquires in [`Extension::acquire`] and
/// that must stay alive for the whole action.
///
/// The [`Domain`](crate::Domain) holds it in scope from before persistence until
/// after [`Extension::after_action`], dropping it — releasing whatever it guards
/// — when the action ends, **including on every error path** (a failed
/// constraint check or persist). Acquisition happens only *after* the action is
/// authorized, so a forbidden caller can never take (or contend on) a lease.
/// This is what lets the single-writer [`lock`] extension hold a lease across
/// the write without a per-action context object.
#[derive(Default)]
pub struct ActionHold(
    // Held only for its `Drop` side effect (releasing the acquired resource);
    // never read back out, hence the allow.
    #[allow(dead_code)] Option<Box<dyn Any + Send>>,
);

impl ActionHold {
    /// Nothing to hold — the default for extensions that don't acquire anything.
    pub fn none() -> Self {
        Self(None)
    }

    /// Hold `value` for the remainder of the action; dropping the returned
    /// `ActionHold` drops `value`.
    pub fn new(value: impl Any + Send + 'static) -> Self {
        Self(Some(Box::new(value)))
    }
}

/// A hook that observes and augments a resource's actions.
///
/// Extensions are registered on a [`Domain`](crate::Domain) and invoked around
/// every **authorized** action — the policy gate runs first on every path, so a
/// denied request reaches none of the *effecting* hooks (no lock is acquired, no
/// changeset guarded, no result seen for a caller who was never allowed in).
/// Writes get `before_action` (after changes are staged and the action
/// authorized — a chance to guard or enrich the changeset) and `after_action`
/// (once the record is persisted — a chance to react: audit, notify, …). Reads
/// get the parallel `before_read` (after the action's preparations and
/// authorization — a chance to further scope the query) and `after_read` (a
/// chance to inspect or filter the results).
///
/// The one hook that fires on the **denied** path is
/// [`on_denied`](Extension::on_denied): a strictly read-only notification the
/// executor calls when the gate refuses an action, so an extension can *record* a
/// denial without being able to affect it (see its docs). All hooks default to
/// no-ops, so an extension implements only the ones it needs.
///
/// # Trust boundary — extensions run *inside* the authorization gate
///
/// Policies gate the **caller**; extensions are **deployment-installed,
/// domain-side code that runs within the gate**, and are trusted accordingly.
/// Two consequences follow from where the hooks sit in the pipeline, and both are
/// intentional:
///
/// * **`before_action` runs *after* the write is authorized and may still mutate
///   the [`Changeset`].** Operation policies judged the changeset as the caller
///   staged it; an extension can then change it (stamp a field, force a status).
///   Those later mutations are **not** re-authorized — an extension is trusted to
///   uphold the domain's rules, not police them.
/// * **`after_read` sees rows *before* attribute redaction.** Field-read policies
///   redact the returned rows only after every `after_read` hook has run, so an
///   extension observes the un-redacted values (an audit extension logging a read
///   sees the real row, not the caller's redacted view). Do not treat a value
///   reaching `after_read` as one the caller is allowed to see.
///
/// In short: extensions are part of the trusted computing base, on the domain's
/// side of the policy boundary — install only code you trust with unredacted
/// data and unchecked changeset edits.
#[async_trait]
pub trait Extension: Send + Sync {
    /// A short name for logging/introspection.
    fn name(&self) -> &str;

    /// Runs after changes are staged **and the action is authorized**, before
    /// constraints and persistence. Returning `Err` aborts the action.
    ///
    /// Mutations made here are **not re-authorized** — policies already judged the
    /// caller's staged changeset; an extension edits it as trusted domain-side
    /// code (see the [trust boundary](Extension#trust-boundary--extensions-run-inside-the-authorization-gate)).
    async fn before_action(&self, changeset: &mut Changeset) -> Result<()> {
        let _ = changeset;
        Ok(())
    }

    /// Runs after every extension's [`before_action`](Extension::before_action),
    /// still before persistence, to acquire a resource that must live for the
    /// whole action — a lock lease, say. The returned [`ActionHold`] is held by
    /// the [`Domain`](crate::Domain) across persistence and
    /// [`after_action`](Extension::after_action), then dropped when the action
    /// ends (including on error). Returning `Err` aborts the action. Defaults to
    /// holding nothing.
    async fn acquire(&self, changeset: &Changeset) -> Result<ActionHold> {
        let _ = changeset;
        Ok(ActionHold::none())
    }

    /// Runs after the action is persisted, on the committed `result`. May inspect
    /// or adjust it, and is where a domain-side side effect (request a shipment,
    /// enqueue a job) belongs — read the persisted truth from `result`, not the
    /// staged `changeset`.
    ///
    /// **Failure semantics depend on the store's transaction seam** (see
    /// [`Store::begin`](crate::Store::begin)). With a transaction open, this runs
    /// *before* the commit, so returning `Err` rolls the write back — nothing
    /// durable is left behind. With no transaction seam (the default), the write
    /// has *already* committed by the time this runs, so returning `Err` surfaces
    /// to the caller on a row that **stands committed** — the framework cannot
    /// undo it. Either way the returned error is the caller's, and this hook is
    /// **not retried**; at-least-once delivery of the side effect (an outbox) is a
    /// consumer concern. Events are emitted post-commit, best-effort, after every
    /// extension's `after_action` returns `Ok`.
    async fn after_action(&self, changeset: &Changeset, result: &mut ActionResult) -> Result<()> {
        let _ = (changeset, result);
        Ok(())
    }

    /// Runs after a read action's preparations **and authorization**, before
    /// the data layer is hit. May further scope the query. Returning `Err`
    /// aborts the read.
    async fn before_read(
        &self,
        resource: &str,
        action: &str,
        query: &mut Query,
        actor: Option<&Record>,
    ) -> Result<()> {
        let _ = (resource, action, query, actor);
        Ok(())
    }

    /// Runs after a read returns, before the records reach the caller. May
    /// inspect or filter them.
    ///
    /// **These rows are not yet redacted:** attribute-read policies null forbidden
    /// fields only *after* every `after_read` hook has run, so the values here are
    /// the raw stored ones, not the caller's redacted view (see the
    /// [trust boundary](Extension#trust-boundary--extensions-run-inside-the-authorization-gate)).
    async fn after_read(
        &self,
        resource: &str,
        action: &str,
        records: &mut Vec<Record>,
        actor: Option<&Record>,
    ) -> Result<()> {
        let _ = (resource, action, records, actor);
        Ok(())
    }

    /// Runs when the authorization gate **denies** an action, on **every** path
    /// (write, read, generic). This is the one hook that fires *before* the gate
    /// admits — it exists so an extension can record a denial the other hooks
    /// (which only ever see authorized actions) structurally cannot.
    ///
    /// It is **strictly read-only and cannot affect the outcome**: there is no
    /// changeset to mutate, no query to rescope, and the return type is `()` — a
    /// panic aside, nothing an implementation does here can turn a `Forbidden`
    /// into an `Allow` or leak a side effect onto the denied path. That is
    /// deliberate: the fail-closed invariant (authorization decides access, and a
    /// denied caller observes nothing but the denial) must hold regardless of what
    /// is installed. Treat this purely as a notification sink.
    ///
    /// It fires only for **authorization** denials — `error` is always
    /// [`Error::Forbidden`](crate::Error::Forbidden) (a policy vetoed) or
    /// [`Error::PolicyError`](crate::Error::PolicyError) (a policy backend failed
    /// closed). Ordinary input failures (`Invalid`, `NotFound`, `Unsupported`) do
    /// **not** reach here — they are not access denials.
    ///
    /// `actor` is the acting principal record, if the request carried one.
    async fn on_denied(
        &self,
        resource: &str,
        action: &str,
        actor: Option<&Record>,
        error: &crate::error::Error,
    ) {
        let _ = (resource, action, actor, error);
    }
}
