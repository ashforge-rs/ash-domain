//! The [`Domain`] — the registry and executor that ties everything together.
//!
//! A domain holds the registered resources, the extensions and policy applied
//! to every action, and the clock used to stamp timestamps. It does *not* hold
//! storage: persistence lives in the [`Context`]'s backend, so whether a domain
//! action can run at all is decided by the context you pass it.
//!
//! Every action runs through **one entry point**,
//! [`handle_action`](Domain::handle_action): the resource is resolved by type, the
//! action by name, and the *shape* of the [`ActionInput`](crate::ActionInput)
//! (params, an id, a query) selects create / read / update / destroy / generic. It
//! returns an [`ActionOutcome`](crate::ActionOutcome) of raw
//! [`Record`]s, which you project into the resource's typed
//! [`Data`](crate::Resource::Data) with
//! [`ActionOutcome::into_data`](crate::ActionOutcome::into_data). The
//! `#[derive(Resource)]` typed helpers (`Note::create`, `read`, `get`, `update`,
//! `destroy`) wrap this call for the default CRUD actions.
//!
//! It drives each action through the uniform pipeline. **Authorization
//! runs first** on every path — nothing observable (extension hooks, lock
//! acquisition, validation reporting, storage) happens for a request the policy
//! set denies; only pure staging (the action's declared changes, defaults,
//! stamps) precedes it, so policies judge the changeset **as the caller staged
//! it** (an extension's `before_action`, which runs after the gate, may still
//! edit it — see the trust boundary below):
//!
//! ```text
//! create/update:  stage params → changes → defaults, timestamps & tenant
//!                 → authorize → extensions.before_action → run validations
//!                 → persist → extensions.after_action → event_handlers
//! read:           tenant scope → preparations → authorize
//!                 → extensions.before_read → data layer read
//!                 → extensions.after_read → attribute redaction
//!                 → (read_loaded: load rels, aggregates, computed fields —
//!                    all over the already-redacted rows)
//! destroy:        fetch & tenant-check → authorize → extensions.before_action
//!                 → delete → extensions.after_action → event_handlers
//! generic:        authorize → extensions.before_action → handler → event_handlers
//! batch create:   [per row: the whole create pipeline up to validations]
//!                 → persist all → [per row: after_action] → commit
//!                 → [per row: event_handlers]
//! ```
//!
//! The batch line is the same pipeline, not a shortcut through it: every row is
//! staged, authorized, gated field-by-field and validated on its own, and the
//! two passes are separate so **no row reaches the layer until every row has
//! been authorized** — one denied row persists none of the batch.
//!
//! Two consequences of that order are part of the contract. First, an
//! unauthorized caller learns nothing beyond `Forbidden` (validation validity is
//! never reported before the authorization gate). Second, extensions run *inside*
//! the authorization gate as **trusted, deployment-installed** code: they run
//! only for authorized actions (a denied request can never acquire locks, write
//! audit staging, or drive a state machine), but because they run after the gate,
//! a write extension's `before_action` may edit the changeset without
//! re-authorization, and a read extension's `after_read` sees rows *before*
//! attribute redaction. Policies gate the caller; extensions are trusted to
//! uphold the domain's rules from within. See
//! [`Extension`](crate::extension::Extension) for the full trust boundary.
//!
//! The domain runs **no** built-in attribute validation. Presence, ranges,
//! formats, and cross-field rules are all the consumer's job, expressed as
//! [`Validation`](crate::action::Validation)s registered on the write action
//! (run after `before_action`, before persist).
//!
//! [`handle_action`](Domain::handle_action) is generic over the context backend
//! and requires it to be a [`Store`] — every action kind, including a generic one,
//! is driven with the context's persistence backend available.
//!
//! A domain is built from a plain [`DomainConfig`] struct, not a builder:
//!
//! ```
//! use ash_domain::{erase, DomainConfig, DomainContext, Domain, Record, Resource};
//! # use ash_domain::action::ActionDef;
//! # use ash_domain::attribute::{Attribute, AttrType};
//! struct Note;
//! impl Resource for Note {
//!     const NAME: &'static str = "note";
//!     type Data = Record;
//!     fn attributes() -> Vec<Attribute> { vec![Attribute::scalar::<String>("id")] }
//!     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
//! }
//!
//! let domain = Domain::new(DomainConfig {
//!     resources: vec![erase::<Note>()],
//!     ..DomainConfig::default()
//! }, DomainContext::new());
//! # let _ = domain;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use crate::action::{ActionDef, ActionInput, ActionKind, ActionOutcome};
use crate::clock::Clock;
use crate::context::{Context, DomainContext, Store};
use crate::error::{Error, Result};
use crate::event::Emitter;
use crate::extension::Extension;
use crate::id::IdGenerator;
use crate::policy::PolicySet;
use crate::query::Query;
use crate::resource::{ErasedResource, Resource};
use crate::trace::trace_span;
use crate::value::Record;

/// The registry + executor for a set of resources.
#[derive(Clone)]
pub struct Domain {
    resources: Arc<HashMap<String, Arc<dyn ErasedResource>>>,
    extensions: Arc<Vec<Arc<dyn Extension>>>,
    emitter: Emitter,
    policies: Arc<PolicySet>,
    clock: Arc<dyn Clock>,
    id_generator: Arc<dyn IdGenerator>,
    /// Optional per-action metrics sink. `None` (the default) means no observation
    /// happens at all — the domain skips the call entirely. See [`crate::metrics`].
    metrics: Option<Arc<dyn crate::metrics::Metrics>>,
    /// The row ceiling applied to a read that carries no
    /// [`Query::limit`](crate::Query::limit) of its own. `None` is the explicit
    /// unbounded opt-out. See [`DomainConfig::max_rows`].
    max_rows: Option<std::num::NonZeroU32>,
    /// The most rows one batch create may carry. See
    /// [`DomainConfig::max_batch`].
    max_batch: std::num::NonZeroU32,
    /// How deep a nested relationship load may go. See
    /// [`DomainConfig::max_load_depth`].
    max_load_depth: std::num::NonZeroU32,
    domain_context: Arc<DomainContext>,
    /// Runtime on/off gate for execution-trace emission (see
    /// [`enable_tracing`](Domain::enable_tracing)). Shared across clones and
    /// starts **off**. Independent of the `trace` cargo feature: without that
    /// feature the instrumentation is compiled out entirely and this flag is
    /// simply never read.
    trace_gate: crate::trace::TraceGate,
    /// Shutdown gate: once flipped by [`begin_close`](Domain::begin_close) /
    /// [`close`](Domain::close), every new [`handle_action`](Domain::handle_action)
    /// fails fast with [`Error::Closing`] before doing anything observable. Shared
    /// across clones (like `trace_gate`) and starts **off**. See
    /// [`crate::lifecycle`].
    close_gate: crate::lifecycle::CloseGate,
}

mod authorize;
mod config;
mod read;
mod registry;
mod tenant;
mod typed_query;
mod write;

pub use config::{
    Bound, DEFAULT_MAX_BATCH, DEFAULT_MAX_LOAD_DEPTH, DEFAULT_MAX_ROWS, DomainBuilder, DomainConfig,
};

use registry::{validate_policies, validate_resources};

impl Domain {
    /// Build a domain from a [`DomainConfig`] and a [`DomainContext`] of shared
    /// clients, **validating the whole registry first** — see
    /// [`try_new`](Domain::try_new) for what is checked.
    ///
    /// The `domain_context` holds the long-lived handles the domain and its
    /// handlers reuse across every operation (an HTTP client, a cache pool, an
    /// external policy-service client). Pass [`DomainContext::new()`] when the
    /// domain needs no shared clients. Handlers reach them through
    /// [`HandlerContext::client`](crate::HandlerContext::client).
    ///
    /// # Panics
    ///
    /// Panics if the config is inconsistent (a relationship to an unregistered
    /// resource, an aggregate over an unknown relationship, a policy scoped to a
    /// resource that doesn't exist, …). A domain is built once at startup from
    /// programmer-authored declarations, so an invalid config is a bug —
    /// fail-fast at construction, not at request time. Use
    /// [`try_new`](Domain::try_new) to handle the error instead.
    pub fn new(config: DomainConfig, domain_context: DomainContext) -> Self {
        Self::try_new(config, domain_context)
            .unwrap_or_else(|e| panic!("invalid DomainConfig: {e}"))
    }

    /// Fallible form of [`new`](Domain::new): build the domain, returning
    /// [`Error::Invalid`] if the config's declarations are inconsistent.
    ///
    /// The registry is validated **at construction**, so a mistake surfaces at
    /// startup instead of as a runtime failure on some future request. Checked:
    ///
    /// - resource names are unique; attribute and action names are unique per
    ///   resource; a generic action carries a handler;
    /// - every relationship points at a registered destination resource, its
    ///   `source_attribute` exists on the declaring resource and its
    ///   `destination_attribute` on the destination; a `through` join resource is
    ///   registered and carries both join attributes;
    /// - every aggregate rolls up a declared relationship, and aggregate /
    ///   computed names collide with neither each other nor the attributes;
    /// - a [`TenantStrategy::Attribute`] discriminator exists as an attribute;
    /// - every policy scope refers to a registered resource, and an
    ///   action/attribute scope to a declared action/attribute.
    pub fn try_new(config: DomainConfig, mut domain_context: DomainContext) -> Result<Self> {
        let mut resources: HashMap<String, Arc<dyn ErasedResource>> = HashMap::new();
        for resource in config.resources {
            let name = resource.name().to_string();
            if resources.insert(name.clone(), resource).is_some() {
                return Err(Error::invalid(format!(
                    "duplicate resource `{name}` in DomainConfig"
                )));
            }
        }
        validate_resources(&resources)?;
        validate_policies(&resources, &config.policies)?;

        // The out-of-band emitter fans out to the same registered handlers as the
        // commit path, over the same clock. Register it into the shared context so
        // any code reaching the `DomainContext` (a `HandlerContext`, or service
        // code via `domain_context()`) can emit — `ctx.client::<Emitter>()`.
        let event_handlers = Arc::new(config.event_handlers);
        let emitter = Emitter::new(event_handlers, config.clock.clone());
        domain_context.insert(emitter.clone());

        Ok(Domain {
            resources: Arc::new(resources),
            extensions: Arc::new(config.extensions),
            emitter,
            policies: Arc::new(config.policies),
            clock: config.clock,
            id_generator: config.id_generator,
            metrics: config.metrics,
            max_rows: config.max_rows,
            max_batch: config.max_batch,
            max_load_depth: config.max_load_depth,
            domain_context: Arc::new(domain_context),
            // Tracing starts off — an instrumented build stays silent until the
            // consumer calls `enable_tracing`.
            trace_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            // Open for business — the close gate starts off; `begin_close`/`close`
            // flips it.
            close_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Start building a domain with a fluent [`DomainBuilder`] instead of
    /// assembling a [`DomainConfig`] struct literal.
    ///
    /// The two forms are equivalent — the builder just wraps `DomainConfig` — but
    /// the builder reads better for the common case: `register::<R>()` hides the
    /// [`erase`](crate::erase) call, chained setters replace the
    /// `..DomainConfig::default()` spread, and there is one entry point instead of
    /// the `(config, context)` pair `new` takes.
    ///
    /// ```
    /// # use ash_domain::{Domain, Resource, Attribute, ActionDef, Record};
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> { vec![Attribute::scalar::<String>("id")] }
    /// #     fn actions() -> Vec<ActionDef> { vec![ActionDef::write("create")] }
    /// # }
    /// let domain = Domain::builder()
    ///     .register::<Todo>()
    ///     .permissive() // opt out of default-deny for a demo / trusted service
    ///     .build();
    /// ```
    pub fn builder() -> DomainBuilder {
        DomainBuilder::new()
    }

    /// Turn **execution-trace emission on** for this domain (and every clone of
    /// it — the gate is shared).
    ///
    /// The domain is instrumented with `tracing` spans and events — one span
    /// per action, an event per pipeline stage (`staged`, `authorized` /
    /// `denied`, `before_action`, `persisted`, `after_action`, and the read /
    /// generic analogues). This method flips the **runtime gate** that decides
    /// whether those fire; it starts off, so nothing is emitted until you call
    /// this.
    ///
    /// Two things it deliberately does **not** do:
    ///
    /// - It does **not** choose where the trace goes. The domain only *emits*;
    ///   you decide the sink by installing a `tracing` subscriber in your process
    ///   (`tracing_subscriber::fmt::init()` for stdout, an OpenTelemetry layer, a
    ///   file — anything). With the gate on but no subscriber installed, emission
    ///   is a near-free no-op.
    /// - It does **not** enable instrumentation that was compiled out. The
    ///   spans/events exist only when `ash-domain` is built with the `trace`
    ///   cargo feature. Without that feature this method still exists and flips
    ///   the flag, but there is nothing to emit — so on a default build it is
    ///   effectively inert. Enable the feature to get a trace, then call this to
    ///   turn it on.
    ///
    /// Off by default, gated at runtime, sink chosen by the caller: an
    /// opt-out-by-default execution trace.
    pub fn enable_tracing(&self) {
        self.trace_gate
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Turn execution-trace emission **off** for this domain (and its clones).
    /// The inverse of [`enable_tracing`](Domain::enable_tracing); safe to call at
    /// any time from any thread.
    pub fn disable_tracing(&self) {
        self.trace_gate
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether execution-trace emission is currently on. Note this reflects only
    /// the runtime gate — on a build without the `trace` feature the
    /// instrumentation is compiled out regardless of what this returns.
    pub fn tracing_enabled(&self) -> bool {
        self.trace_gate.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Flip the **close gate**: stop accepting new work, without touching clients.
    ///
    /// After this, every new [`handle_action`](Domain::handle_action) call fails
    /// fast with [`Error::Closing`] *before* it does anything observable (no
    /// staging, no authorization, no storage). Work another task has *already*
    /// entered is not interrupted — the gate is checked at the entry, so this is a
    /// cooperative "no new work" signal, not a forced halt. Shared across every
    /// clone of the domain, and **idempotent**.
    ///
    /// This is the cheap half of shutdown. Use it to quiesce a domain while other
    /// tasks drain, then call [`close`](Domain::close) to release clients. Calling
    /// `close` directly does this first, so you rarely need `begin_close` alone.
    /// There is deliberately no reopen: a domain that has begun closing is done —
    /// build a fresh one to serve again.
    pub fn begin_close(&self) {
        self.close_gate
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether the domain has begun closing (the [close gate](Domain::begin_close)
    /// is flipped). Reflected across every clone. Cheap — a single relaxed load.
    pub fn is_closing(&self) -> bool {
        crate::lifecycle::is_closing(&self.close_gate)
    }

    /// **Orderly shutdown.** Flip the close gate (as
    /// [`begin_close`](Domain::begin_close)), then shut down every registered
    /// [`Closable`](crate::lifecycle::Closable) client in **reverse** registration
    /// order (last registered, first closed — the usual teardown order), returning
    /// once they have all returned.
    ///
    /// # What it does and does not guarantee
    ///
    /// - **New work is refused** the instant this begins: no action started after
    ///   the gate flips runs. Work already in flight in another task is *not*
    ///   interrupted — quiesce those callers yourself (or supervise the domain
    ///   with a [`DomainWorker`](crate::flare::DomainWorker), which does the
    ///   bounded wait) before calling this.
    /// - **Every client is closed even if one fails.** A [`Closable::close`] that
    ///   returns `Err` does not stop the rest; all errors are collected and the
    ///   **first** is returned (with a count), so a failed teardown is visible but
    ///   never strands the other clients.
    /// - **Events are not flushed.** A committed action's
    ///   [`DomainEvent`](crate::event::DomainEvent) is delivered best-effort,
    ///   in-process, un-retried — close does not drain or flush events a handler
    ///   had not yet pushed to an external sink. **Those may be lost.** Durable
    ///   egress (an outbox, an idempotent consumer) is the consumer's job on the
    ///   normal event path, and close changes nothing about that. Account for it.
    ///
    /// Idempotent: a second `close` re-flips the (already-flipped) gate and
    /// re-drives the clients, which are contracted to tolerate it.
    pub async fn close(&self) -> Result<()> {
        self.begin_close();

        let closables = self.domain_context.closables();
        let mut first_err: Option<Error> = None;
        let mut error_count: usize = 0;
        // Reverse registration order: the last client registered is torn down
        // first, matching conventional resource teardown.
        for closable in closables.iter().rev() {
            if let Err(e) = closable.close().await {
                error_count += 1;
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        match first_err {
            None => Ok(()),
            Some(Error::DataLayer { message, source }) if error_count == 1 => {
                Err(Error::DataLayer { message, source })
            }
            Some(first) => {
                debug_assert!(error_count >= 1, "first_err set implies at least one error");
                // Preserve the first failure's meaning while noting the fan-out.
                Err(Error::data_layer(format!(
                    "{error_count} client(s) failed to close; first: {first}"
                )))
            }
        }
    }

    /// The domain's shared-client bag. Handlers usually reach clients via
    /// [`HandlerContext::client`](crate::HandlerContext::client) instead of this.
    pub fn domain_context(&self) -> &DomainContext {
        &self.domain_context
    }

    /// The domain's [`Emitter`] — the handle for emitting [`DomainEvent`]s **out
    /// of band** (from anywhere, not only the commit path). It fans out to the
    /// same registered [`EventHandler`]s.
    ///
    /// The same emitter is also registered in the
    /// [`DomainContext`](crate::DomainContext), so a generic action's
    /// [`HandlerContext`](crate::HandlerContext) reaches it with
    /// `ctx.client::<Emitter>()`. Use this accessor from service code that holds
    /// the [`Domain`]. Cloneable and cheap. See [`Emitter`] for the "these are not
    /// commit facts" caveat.
    pub fn emitter(&self) -> &Emitter {
        &self.emitter
    }

    /// Introspect a registered resource by name (its erased schema).
    pub fn resource(&self, name: &str) -> Option<&Arc<dyn ErasedResource>> {
        self.resources.get(name)
    }

    /// The names of every registered resource, sorted — the entry point for
    /// walking the validated registry (docs generation, an admin UI). Pair each
    /// with [`resource`](Domain::resource) for its erased schema and
    /// [`policy_tree`](Domain::policy_tree) for its policies.
    pub fn resource_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.resources.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    fn ensure_registered(&self, name: &str) -> Result<()> {
        if self.resources.contains_key(name) {
            Ok(())
        } else {
            Err(Error::UnknownResource(name.to_string()))
        }
    }

    /// Bind this domain to a [`Context`] and get a [`Bound`] handle you call the
    /// CRUD verbs on directly — `bound.create::<Todo>(todo)` instead of
    /// `Todo::create(&domain, &mut ctx, todo)`.
    ///
    /// The handle borrows the domain and mutably borrows the context for its
    /// lifetime, so the `(&domain, &mut ctx)` pair is threaded **once** here
    /// rather than at every call site. It is a thin, zero-cost convenience over
    /// [`handle_action`](Domain::handle_action): same pipeline, same
    /// authorization, same results — only the plumbing moves off the call.
    ///
    /// ```
    /// # use ash_domain::{Domain, Context, Resource, Attribute, ActionDef, Record};
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # use std::sync::Arc;
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> {
    /// #         vec![Attribute::scalar::<String>("id"), Attribute::scalar::<String>("message")]
    /// #     }
    /// #     fn actions() -> Vec<ActionDef> {
    /// #         vec![ActionDef::write("create"), ActionDef::read("read")]
    /// #     }
    /// # }
    /// # async fn run() -> ash_domain::Result<()> {
    /// let domain = Domain::builder().register::<Todo>().permissive().build();
    /// let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ///
    /// let mut todos = domain.bind(&mut ctx);
    /// let made = todos.create::<Todo>(Record::from_iter([("message", "buy milk")])).await?;
    /// let got = todos.get::<Todo>(made.get("id").cloned().unwrap()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind<'a, B: Store>(&'a self, ctx: &'a mut Context<B>) -> Bound<'a, B> {
        Bound { domain: self, ctx }
    }

    /// Run an action of `R` by name — the **single entry point** for every
    /// action kind.
    ///
    /// The action's declared [`ActionKind`], together with the *shape* of
    /// `input`, selects the operation. [`Write`](ActionKind::Write) covers both
    /// create and update, so the presence of an id in the input distinguishes
    /// them:
    ///
    /// | Input                                        | Action  |
    /// |----------------------------------------------|---------|
    /// | [`ActionInput::create`] (params, no id)      | create  |
    /// | [`ActionInput::update`] (params + id)        | update  |
    /// | [`ActionInput::destroy`] (id)                | destroy |
    /// | [`ActionInput::read`] (query)                | read    |
    /// | [`ActionInput::generic`] (params)            | generic |
    ///
    /// The result is an [`ActionOutcome`] carrying raw [`Record`]s (a write yields
    /// one, a read many, a generic a [`Value`], a destroy nothing). Project it into
    /// a typed [`Data`](crate::Resource::Data) with
    /// [`ActionOutcome::into_data`] / [`into_data_vec`](ActionOutcome::into_data_vec)
    /// when you want typing.
    ///
    /// **Authorization runs first on every path** — a denied caller sees
    /// `Forbidden` before any extension hook, lock, validation, or storage
    /// access, exactly as documented at the module level.
    ///
    /// A read through this path resolves no relationship loads or derived
    /// values: a query carrying `load`/`aggregates`/`computed` is **rejected**
    /// with [`Error::Invalid`] (never silently dropped) — build the input with
    /// [`Filter`](crate::Filter), which cannot carry them, and use
    /// [`Domain::read`] (or [`read_loaded`](Domain::read_loaded)) when you want
    /// them resolved. A generic handler always receives the context's
    /// [`Store`] as `Some`.
    pub async fn handle_action<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        input: ActionInput,
    ) -> Result<ActionOutcome> {
        // Shutdown gate, checked first — before staging, authorization, or any
        // storage access. A closing domain refuses new work with `Closing`, a
        // lifecycle signal (not a fault, not a denial), having observed nothing.
        if self.is_closing() {
            return Err(Error::Closing(R::NAME.to_string()));
        }
        self.ensure_registered(R::NAME)?;
        let actions = R::actions();
        let def = find_action_any(&actions, R::NAME, action)?;

        // Root span for the whole action; each pipeline stage below emits an
        // event that nests inside it. Fields are structure only (names + kind),
        // never attribute values — see `crate::trace`.
        let _span = trace_span!(
            &self.trace_gate,
            "action",
            resource = R::NAME,
            action = action,
            kind = ?def.kind
        );

        // Time the pipeline only when a metrics sink is registered; otherwise this
        // is `None` and no clock is read (the seam is truly zero-cost when unset).
        let started = self.metrics.as_ref().map(|_| std::time::Instant::now());
        let kind = def.kind;

        let result = self.dispatch_action::<R>(ctx, action, def, input).await;

        // One sample per action, classified by how it finished. Emitted after the
        // pipeline so a metrics failure can never affect the result.
        if let (Some(metrics), Some(started)) = (self.metrics.as_ref(), started) {
            let outcome = match &result {
                Ok(_) => crate::metrics::Outcome::Committed,
                Err(Error::Forbidden(_) | Error::PolicyError(_)) => crate::metrics::Outcome::Denied,
                Err(_) => crate::metrics::Outcome::Errored,
            };
            metrics.record(crate::metrics::MetricSample {
                resource: R::NAME,
                action,
                kind,
                outcome,
                duration: started.elapsed(),
            });
        }
        result
    }

    /// The pipeline dispatch for [`handle_action`](Domain::handle_action), split
    /// out so the entry point can wrap it with timing/metrics on every exit path.
    async fn dispatch_action<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        def: ActionDef,
        input: ActionInput,
    ) -> Result<ActionOutcome> {
        match (def.kind, input) {
            // ── create ──────────────────────────────────────────────────────
            (ActionKind::Write, ActionInput::Params { id: None, params }) => {
                let record = self.exec_create::<R>(ctx, &def, params).await?;
                Ok(ActionOutcome::Record(record))
            }
            // ── batch create ────────────────────────────────────────────────
            (ActionKind::Write, ActionInput::Batch { rows }) => {
                let records = self.exec_create_many::<R>(ctx, &def, rows).await?;
                Ok(ActionOutcome::Records(records))
            }
            // ── update ──────────────────────────────────────────────────────
            (
                ActionKind::Write,
                ActionInput::Params {
                    id: Some(id),
                    params,
                },
            ) => {
                let record = self.exec_update::<R>(ctx, &def, id, params).await?;
                Ok(ActionOutcome::Record(record))
            }
            // ── destroy ─────────────────────────────────────────────────────
            (ActionKind::Write, ActionInput::Empty { id: Some(id) }) => {
                self.exec_destroy::<R>(ctx, &def, id).await?;
                Ok(ActionOutcome::Unit)
            }
            (ActionKind::Write, ActionInput::Empty { id: None }) => Err(Error::invalid(format!(
                "write action `{action}` needs an id to update/destroy, or params to create"
            ))),
            (ActionKind::Write, ActionInput::Query(_)) => Err(Error::invalid(format!(
                "write action `{action}` cannot take a read query as input"
            ))),
            // ── read ────────────────────────────────────────────────────────
            (ActionKind::Read, ActionInput::Query(query)) => {
                // This path resolves no relationship loads or derived values;
                // dropping such a request silently would be exactly the silent
                // degradation the crate forbids — reject it loudly instead.
                reject_unresolved_read_requests(action, &query)?;
                let (records, _, _) = self.read_records::<R>(ctx, action, *query).await?;
                Ok(ActionOutcome::Records(records))
            }
            (ActionKind::Read, _) => Err(Error::invalid(format!(
                "read action `{action}` takes a query; use ActionInput::read"
            ))),
            // ── generic ─────────────────────────────────────────────────────
            (ActionKind::Generic, input) => {
                let params = match input {
                    ActionInput::Params { id: None, params } => params,
                    ActionInput::Empty { id: None } => Record::new(),
                    ActionInput::Params { id: Some(_), .. }
                    | ActionInput::Empty { id: Some(_) } => {
                        return Err(Error::invalid(format!(
                            "generic action `{action}` does not take an id"
                        )));
                    }
                    ActionInput::Query(_) => {
                        return Err(Error::invalid(format!(
                            "generic action `{action}` takes params, not a query"
                        )));
                    }
                    ActionInput::Batch { .. } => {
                        return Err(Error::invalid(format!(
                            "generic action `{action}` takes one param bag, not a batch"
                        )));
                    }
                };
                let value = self.exec_generic::<R>(ctx, action, params).await?;
                Ok(ActionOutcome::Value(value))
            }
        }
    }
}

/// Find an action by name and kind in a resource's declared actions.
fn find_action(
    actions: &[ActionDef],
    resource: &str,
    name: &str,
    kind: ActionKind,
) -> Result<ActionDef> {
    actions
        .iter()
        .find(|a| a.name == name && a.kind == kind)
        .cloned()
        .ok_or_else(|| Error::UnknownAction {
            resource: resource.to_string(),
            action: name.to_string(),
        })
}

/// Reject a read whose query carries requests this path cannot resolve —
/// `load`, `aggregates`, or `computed` are only honored by the paths built for
/// them ([`Domain::read`] / [`Domain::read_loaded`]). Erroring here keeps the
/// contract loud: an unresolvable request fails, it is never silently dropped.
fn reject_unresolved_read_requests(action: &str, query: &Query) -> Result<()> {
    if query.load.is_empty() && query.aggregate_names.is_empty() && query.computed.is_empty() {
        return Ok(());
    }
    Err(Error::invalid(format!(
        "read action `{action}`: this path does not resolve `load`/`aggregates`/`computed`; \
         use `Domain::read` (or `read_loaded`) for relationship loads and derived values"
    )))
}

/// Find an action by name regardless of kind — the caller then dispatches on the
/// returned [`ActionKind`]. Used by [`Domain::handle_action`], the single entry
/// point that serves every kind.
fn find_action_any(actions: &[ActionDef], resource: &str, name: &str) -> Result<ActionDef> {
    actions
        .iter()
        .find(|a| a.name == name)
        .cloned()
        .ok_or_else(|| Error::UnknownAction {
            resource: resource.to_string(),
            action: name.to_string(),
        })
}

#[cfg(test)]
mod tests;
