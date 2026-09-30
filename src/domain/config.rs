//! Building a domain and driving one: [`DomainConfig`], [`DomainBuilder`], and
//! the [`Bound`] convenience wrapper.

use std::num::NonZeroU32;
use std::sync::Arc;

use crate::action::{ActionInput, ActionOutcome};
use crate::clock::{Clock, SystemClock};
use crate::context::{Context, DomainContext, Store};
use crate::error::Result;
use crate::event::EventHandler;
use crate::extension::Extension;
use crate::id::{DefaultIdGenerator, IdGenerator};
use crate::policy::{Explanation, PolicySet};
use crate::query::Query;
use crate::resource::{ErasedResource, Resource};
use crate::value::{IntoRecord, Record, Value};

use super::Domain;

/// The default ceiling on rows a single read may return when the caller sets no
/// [`Query::limit`](crate::Query::limit): 10,000.
///
/// Not a tuned number — a bound. It is high enough that no ordinary page or
/// relationship load approaches it, and low enough that a read which *would*
/// have pulled a whole table is refused
/// ([`Error::Unsupported`](crate::Error::Unsupported)) instead of quietly
/// loading it into memory. Raise it with [`DomainBuilder::max_rows`], or remove
/// it with [`DomainBuilder::unbounded_rows`].
pub const DEFAULT_MAX_ROWS: NonZeroU32 = match NonZeroU32::new(10_000) {
    Some(n) => n,
    None => unreachable!(),
};

/// The default ceiling on rows in one batch create: 1,000.
///
/// A batch is caller-supplied and already in memory, so the bound here is on the
/// work the domain will do in one uninterruptible pass — staging, authorizing
/// and validating every row before any of them is persisted. Change it with
/// [`DomainBuilder::max_batch`]; there is deliberately no unbounded opt-out,
/// because an unbounded batch has no upper bound on how long a caller holds the
/// pipeline (and, under a transaction, the write lock).
pub const DEFAULT_MAX_BATCH: NonZeroU32 = match NonZeroU32::new(1_000) {
    Some(n) => n,
    None => unreachable!(),
};

/// The default ceiling on how deep a nested relationship load may go: 4.
///
/// A load path is caller-shaped (`"comments.author.posts"`), and each level is
/// another round of reads against the layer, fanning out over the rows the level
/// above returned. Without a ceiling a single request can walk the relationship
/// graph as far as the caller cares to type — including around a cycle — so this
/// is the same kind of bound as [`DEFAULT_MAX_ROWS`], on depth instead of width.
///
/// Four is deep enough for the shapes real APIs ask for (`order.customer.address`
/// is three) and shallow enough that a pathological path is refused rather than
/// served. Change it with [`DomainBuilder::max_load_depth`]; there is
/// deliberately no unbounded opt-out, because the cost of a deep load is
/// multiplicative in a way a row bound cannot cap.
pub const DEFAULT_MAX_LOAD_DEPTH: NonZeroU32 = match NonZeroU32::new(4) {
    Some(n) => n,
    None => unreachable!(),
};

/// The declarative inputs to [`Domain::new`], in place of a builder.
///
/// Set the fields you need and inherit the rest from [`Default`]. Note the
/// default [`PolicySet`](crate::PolicySet) is **empty and default-deny** — a
/// domain with no policies forbids every action. State the opposite explicitly
/// with `policies: PolicySet::permissive()` when the domain is gated elsewhere
/// (or in tests). The default clock is the [`SystemClock`]. Register resources
/// with [`erase`](crate::erase):
///
/// ```
/// use ash_domain::{erase, DomainConfig, DomainContext, Domain, Record, Resource};
/// # use ash_domain::action::ActionDef;
/// # use ash_domain::attribute::{Attribute, AttrType};
/// # struct Note;
/// # impl Resource for Note {
/// #     const NAME: &'static str = "note";
/// #     type Data = Record;
/// #     fn attributes() -> Vec<Attribute> { vec![Attribute::scalar::<String>("id")] }
/// #     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
/// # }
/// let config = DomainConfig {
///     resources: vec![erase::<Note>()],
///     ..DomainConfig::default()
/// };
/// let domain = Domain::new(config, DomainContext::new());
/// # let _ = domain;
/// ```
pub struct DomainConfig {
    /// The resources to register, erased with [`erase`](crate::erase).
    pub resources: Vec<Arc<dyn ErasedResource>>,
    /// Extensions applied to every action.
    pub extensions: Vec<Arc<dyn Extension>>,
    /// [`EventHandler`]s handed the [`DomainEvent`] produced after every
    /// committed action (default: none). This is the seam pub/sub, audit
    /// persistence, job-enqueue-on-write, and webhook fan-out build on: a
    /// handler decides what each event becomes.
    pub event_handlers: Vec<Arc<dyn EventHandler>>,
    /// The authorization policies, each tagged with the [`Scope`](crate::Scope)
    /// at which it applies. Default: empty and **default-deny** — every
    /// operation is forbidden until a policy allows it (or the set is built with
    /// [`PolicySet::permissive`]). See [`PolicySet`].
    pub policies: PolicySet,
    /// The clock used to stamp timestamps (default: [`SystemClock`]).
    pub clock: Arc<dyn Clock>,
    /// The generator that mints a primary key when a `create` omits one
    /// (default: [`DefaultIdGenerator`], a UUIDv4). Swap it to control key
    /// generation — e.g. a seeded/deterministic generator.
    pub id_generator: Arc<dyn IdGenerator>,
    /// The ceiling on how many rows a single read may return when the caller set
    /// no [`Query::limit`](crate::Query::limit) of its own — default
    /// [`DEFAULT_MAX_ROWS`].
    ///
    /// A read is bounded whether or not the caller thought about it: the domain
    /// hands the layer this bound, and a result set that exceeds it is
    /// **refused** with [`Error::Unsupported`](crate::Error::Unsupported), never
    /// silently truncated — the caller learns their read was unbounded instead of
    /// receiving a quietly partial answer. `None` opts out, which is the one way
    /// to let a read pull an unbounded result set into memory: explicit and
    /// visible, like every other degradation in this crate.
    pub max_rows: Option<NonZeroU32>,
    /// The most rows one [`ActionInput::Batch`](crate::ActionInput::Batch) may
    /// carry — default [`DEFAULT_MAX_BATCH`]. A larger batch is rejected with
    /// [`Error::Invalid`](crate::Error::Invalid) before any row is staged.
    pub max_batch: NonZeroU32,
    /// How deep a nested relationship load may go — default
    /// [`DEFAULT_MAX_LOAD_DEPTH`].
    ///
    /// Depth is counted in relationship hops: `"comments"` is 1,
    /// `"comments.author"` is 2. A request that asks for more is rejected with
    /// [`Error::Invalid`](crate::Error::Invalid) **before any read runs**, rather
    /// than served partially — a truncated object graph is a silently wrong
    /// answer, which this crate refuses everywhere else too.
    pub max_load_depth: NonZeroU32,
    /// An optional per-action metrics sink (default: `None` — no observation).
    /// The domain calls [`Metrics::record`](crate::metrics::Metrics::record) once
    /// per action with a [`MetricSample`](crate::metrics::MetricSample); the
    /// backend is entirely the consumer's. See [`crate::metrics`].
    pub metrics: Option<Arc<dyn crate::metrics::Metrics>>,
}

impl Default for DomainConfig {
    fn default() -> Self {
        Self {
            resources: Vec::new(),
            extensions: Vec::new(),
            event_handlers: Vec::new(),
            policies: PolicySet::new(),
            clock: Arc::new(SystemClock),
            id_generator: Arc::new(DefaultIdGenerator::default()),
            max_rows: Some(DEFAULT_MAX_ROWS),
            max_batch: DEFAULT_MAX_BATCH,
            max_load_depth: DEFAULT_MAX_LOAD_DEPTH,
            metrics: None,
        }
    }
}

/// A fluent builder for a [`Domain`], the ergonomic alternative to a
/// [`DomainConfig`] struct literal plus a [`DomainContext`].
///
/// Reach it with [`Domain::builder`]. Every setter takes and returns `self`, so
/// calls chain; [`build`](DomainBuilder::build) validates and constructs.
/// Everything not set keeps the same default a `DomainConfig` would have — an
/// empty, **default-deny** policy set included, so a domain you never call
/// [`permissive`](DomainBuilder::permissive) on stays fail-closed.
///
/// The builder wraps a `DomainConfig` and a `DomainContext`; nothing here is
/// reachable another way, and the struct-literal form remains available for code
/// that prefers it or needs to spread a base config.
#[derive(Default)]
pub struct DomainBuilder {
    config: DomainConfig,
    domain_context: DomainContext,
}

impl DomainBuilder {
    /// A fresh builder with every field at its `DomainConfig` default.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resource by type — the builder equivalent of pushing
    /// [`erase::<R>()`](crate::erase) into [`DomainConfig::resources`]. Call once
    /// per resource; order is irrelevant.
    pub fn register<R: Resource>(mut self) -> Self {
        self.config.resources.push(crate::erase::<R>());
        self
    }

    /// Replace the authorization policies. Without this the builder keeps the
    /// **default-deny** empty set — see [`permissive`](DomainBuilder::permissive)
    /// to run open deliberately.
    pub fn policies(mut self, policies: PolicySet) -> Self {
        self.config.policies = policies;
        self
    }

    /// Run the domain **open**: install [`PolicySet::permissive`], admitting every
    /// operation. This is an explicit opt-out of the fail-closed default, for a
    /// trusted service or a demo where authorization is out of scope — never the
    /// silent default.
    pub fn permissive(mut self) -> Self {
        self.config.policies = PolicySet::permissive();
        self
    }

    /// Add an [`Extension`] applied to every action. Repeatable; extensions run in
    /// registration order.
    pub fn extension(mut self, extension: Arc<dyn Extension>) -> Self {
        self.config.extensions.push(extension);
        self
    }

    /// Add an [`EventHandler`] handed the [`DomainEvent`] after every committed
    /// action. Repeatable.
    pub fn event_handler(mut self, handler: Arc<dyn EventHandler>) -> Self {
        self.config.event_handlers.push(handler);
        self
    }

    /// Set the [`Clock`] used to stamp timestamps (default: [`SystemClock`]).
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.config.clock = clock;
        self
    }

    /// Set the [`IdGenerator`] that mints a primary key when a `create` omits one
    /// (default: [`DefaultIdGenerator`]).
    pub fn id_generator(mut self, id_generator: Arc<dyn IdGenerator>) -> Self {
        self.config.id_generator = id_generator;
        self
    }

    /// Set the row ceiling applied to a read that carries no
    /// [`Query::limit`](crate::Query::limit) of its own. See
    /// [`DomainConfig::max_rows`].
    #[must_use]
    pub fn max_rows(mut self, max_rows: NonZeroU32) -> Self {
        self.config.max_rows = Some(max_rows);
        self
    }

    /// Let a read pull an **unbounded** result set — remove the row ceiling
    /// entirely.
    ///
    /// The opt-out exists because some reads legitimately walk a whole table (an
    /// export, a reindex). It is deliberately a named method rather than a bare
    /// `None`: an unbounded read should be something a deployment *chose*, not
    /// something it defaulted into.
    #[must_use]
    pub fn unbounded_rows(mut self) -> Self {
        self.config.max_rows = None;
        self
    }

    /// Set the most rows one batch create may carry. See
    /// [`DomainConfig::max_batch`].
    #[must_use]
    pub fn max_batch(mut self, max_batch: NonZeroU32) -> Self {
        self.config.max_batch = max_batch;
        self
    }

    /// Set how deep a nested relationship load may go. See
    /// [`DomainConfig::max_load_depth`].
    pub fn max_load_depth(mut self, max_load_depth: NonZeroU32) -> Self {
        self.config.max_load_depth = max_load_depth;
        self
    }

    /// Set the per-action [`Metrics`](crate::metrics::Metrics) sink (default:
    /// none).
    pub fn metrics(mut self, metrics: Arc<dyn crate::metrics::Metrics>) -> Self {
        self.config.metrics = Some(metrics);
        self
    }

    /// Register a shared client into the [`DomainContext`] — reachable later with
    /// [`HandlerContext::client`](crate::HandlerContext::client). Repeatable.
    pub fn client<T: Send + Sync + 'static>(mut self, client: T) -> Self {
        self.domain_context.insert(client);
        self
    }

    /// Consume the builder into the finished [`DomainConfig`] / [`DomainContext`]
    /// pair, for code that wants the raw config (to spread, inspect, or pass to
    /// [`Domain::new`]).
    pub fn into_parts(self) -> (DomainConfig, DomainContext) {
        (self.config, self.domain_context)
    }

    /// Validate and build the [`Domain`], **panicking** on an inconsistent
    /// configuration — the builder analogue of [`Domain::new`]. Use
    /// [`try_build`](DomainBuilder::try_build) to handle the error instead.
    pub fn build(self) -> Domain {
        Domain::new(self.config, self.domain_context)
    }

    /// Validate and build the [`Domain`], returning [`Error::Invalid`] on an
    /// inconsistent configuration — the builder analogue of [`Domain::try_new`].
    pub fn try_build(self) -> Result<Domain> {
        Domain::try_new(self.config, self.domain_context)
    }
}

/// A [`Domain`] bound to a [`Context`] — the ergonomic front for the CRUD verbs.
///
/// Get one with [`Domain::bind`]. It holds `&Domain` and `&mut Context<B>` for
/// its lifetime, so each verb takes only its own arguments: `bound.create::<R>(input)`,
/// `get::<R>(id)`, `read::<R>(query)`, `update::<R>(id, input)`,
/// `destroy::<R>(id)`. Every method is a thin wrapper over
/// [`handle_action`](Domain::handle_action) against the resource's **default CRUD
/// action** (`"create"` / `"read"` / `"update"` / `"destroy"`), projecting the
/// outcome into the resource's [`Data`](Resource::Data) — identical to what the
/// `#[derive(Resource)]` typed methods do, only with the domain/context threaded
/// once instead of per call. For a **custom** action use [`run`](Bound::run)
/// (or the derived typed method); [`explain_write`](Bound::explain_write) /
/// [`explain_read`](Bound::explain_read) dry-run authorization against the
/// same bound context.
pub struct Bound<'a, B: Store> {
    pub(super) domain: &'a Domain,
    pub(super) ctx: &'a mut Context<B>,
}

impl<B: Store> Bound<'_, B> {
    /// Create a record via the resource's `create` action, returning the typed row.
    pub async fn create<R: Resource>(&mut self, input: impl IntoRecord) -> Result<R::Data> {
        self.domain
            .handle_action::<R>(self.ctx, "create", ActionInput::create(input)?)
            .await?
            .into_data::<R::Data>()
    }

    /// Create several records of `R` in one batch through its `create` action.
    ///
    /// Each row is staged, authorized and validated on its own; the persist is
    /// the only batched part, and nothing is written until every row has passed.
    /// See [`ActionInput::create_many`](crate::ActionInput::create_many).
    pub async fn create_many<R: Resource, I, T>(&mut self, rows: I) -> Result<Vec<R::Data>>
    where
        I: IntoIterator<Item = T>,
        T: IntoRecord,
    {
        self.domain
            .handle_action::<R>(self.ctx, "create", ActionInput::create_many(rows)?)
            .await?
            .into_data_vec::<R::Data>()
    }

    /// Read records via the resource's `read` action, returning typed rows.
    pub async fn read<R: Resource>(&mut self, query: Query) -> Result<Vec<R::Data>> {
        self.domain
            .handle_action::<R>(self.ctx, "read", ActionInput::read(query))
            .await?
            .into_data_vec::<R::Data>()
    }

    /// Fetch one record by primary key, or `None`. A `read` under the hood — the
    /// scoped analogue of the derived `R::get`: authorized and redacted like any
    /// read, issued as the reserved key-set query on the primary key.
    pub async fn get<R: Resource>(&mut self, id: impl Into<Value>) -> Result<Option<R::Data>> {
        let query = Query::key_set(R::NAME, R::primary_key(), vec![id.into()]);
        Ok(self.read::<R>(query).await?.into_iter().next())
    }

    /// Update the record `id` via the resource's `update` action, returning the row.
    pub async fn update<R: Resource>(
        &mut self,
        id: impl Into<Value>,
        input: impl IntoRecord,
    ) -> Result<R::Data> {
        self.domain
            .handle_action::<R>(self.ctx, "update", ActionInput::update(id, input)?)
            .await?
            .into_data::<R::Data>()
    }

    /// Destroy the record `id` via the resource's `destroy` action.
    pub async fn destroy<R: Resource>(&mut self, id: impl Into<Value>) -> Result<()> {
        self.domain
            .handle_action::<R>(self.ctx, "destroy", ActionInput::destroy(id))
            .await?;
        Ok(())
    }

    /// Run **any** action of `R` by name — the [`Bound`] passthrough to
    /// [`handle_action`](Domain::handle_action), so a custom action is
    /// reachable without re-threading the domain/context pair the handle
    /// already holds. Same pipeline, same authorization; the result is the raw
    /// [`ActionOutcome`] (project with
    /// [`into_data`](crate::ActionOutcome::into_data) /
    /// [`into_data_vec`](crate::ActionOutcome::into_data_vec) when you want
    /// typing).
    pub async fn run<R: Resource>(
        &mut self,
        action: &str,
        input: ActionInput,
    ) -> Result<ActionOutcome> {
        self.domain
            .handle_action::<R>(self.ctx, action, input)
            .await
    }

    /// **Dry-run** the authorization of a write of `R` — the [`Bound`]
    /// analogue of [`Domain::explain_write`], evaluated against the bound
    /// context's actor and tenant.
    pub async fn explain_write<R: Resource>(
        &self,
        action: &str,
        params: Record,
    ) -> Result<Explanation> {
        self.domain
            .explain_write::<R>(&*self.ctx, action, params)
            .await
    }

    /// **Dry-run** the authorization of a read of `R` — the [`Bound`] analogue
    /// of [`Domain::explain_read`], evaluated against the bound context's
    /// actor and tenant.
    pub async fn explain_read<R: Resource>(
        &self,
        action: &str,
        query: Query,
    ) -> Result<Explanation> {
        self.domain
            .explain_read::<R>(&*self.ctx, action, query)
            .await
    }
}
