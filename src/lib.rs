//! # ash-domain
//!
//! **Model your domain, derive the rest.**
//!
//! `ash-domain` is a declarative, resource-oriented application core for Rust,
//! inspired by the Elixir [Ash Framework](https://ash-hq.org). Where the `ash-*`
//! ecosystem provides *satellite* primitives (`ash-fsm`, `ash-flow`, `ash-time`,
//! …), `ash-domain` is the **planet they orbit**: you declare a [`Resource`] once
//! — its attributes, relationships, and actions — and persistence, a uniform
//! action pipeline, and extension hooks are derived from that single definition.
//!
//! ## The model
//!
//! | Concept | What it is |
//! |---------|-----------|
//! | [`Resource`] | a domain entity: [`Attribute`]s, [`Relationship`]s, and [`ActionDef`]s |
//! | [`ActionKind`] | `Read` / `Create` / `Update` / `Destroy` / `Generic` |
//! | pipeline | composable [`Preparation`] · [`Change`] · [`Validation`] · [`GenericHandler`] |
//! | [`Changeset`] | the mutable state threaded through a write action |
//! | [`Domain`] | the registry + executor that runs actions |
//! | [`Context`]`<B>` | the mutable unit of work; its backend `B` decides its capabilities |
//! | [`Store`] | the backend's persistence capability; any `Arc<`[`DataLayer`]`>` is one |
//! | [`DataLayer`] | the pluggable storage trait, with a reference [in-memory](datalayer::memory) impl |
//! | [`Extension`] | how `ash-*` crates attach to a resource's actions |
//! | [`Policy`] / [`PolicySet`] | scoped authorization: gate operations and redact attribute reads |
//! | [`Clock`] | time source ([`SystemClock`] / [`AshTimeClock`], the `ash-time` HLC) |
//!
//! ## Two convictions
//!
//! **The context is a generic you build.** `Context<B>` carries a backend `B`
//! of your choosing, and `B`'s trait impls decide what the context can do:
//! persist (`B: Store`) or not. A store-less `Context<()>` has no CRUD methods —
//! the compiler removes them, rather than a runtime error rejecting them.
//! Richer persistence behaviour (transactions, batching, a unit-of-work) is not
//! built in: extend [`DataLayer`] and implement `Store` for a backend of your
//! own, giving it whatever lifecycle those semantics require.
//!
//! **The core validates nothing on your behalf.** It runs no built-in attribute
//! validation — no presence, range, length, or membership checks baked into
//! attribute metadata. Every rule, from a single required field to a cross-field
//! invariant, is the consumer's job, written the Rust way as an
//! `impl Validation for MyType` (or a [`validate_lambda`](ActionDef::validate_lambda))
//! that returns `Err`, registered on the write action — not configured through a
//! DSL.
//!
//! ## What is implemented vs. a seam
//!
//! Fully built: the resource/action model, the pipeline, the [`Domain`]
//! executor, the [`DataLayer`] trait with an **in-memory** backend (durable
//! backends are the consumer's to `impl DataLayer for`), and one real
//! [`Extension`] — the `ash-fsm` state-machine guard
//! ([`extension::fsm`]). Ergonomics come from the `derive` feature (on by
//! default): [`#[derive(Resource)]`](macro@Resource), from the workspace's
//! `ash-macros` crate, generates the [`Resource`] impl from annotated struct
//! fields. The validated registry is available as data —
//! [`Domain::schema`](Domain::schema), with a JSON Schema rendering — so an API
//! description, an admin UI or a client type can be *generated* from the same
//! declaration the executor runs. Left as documented seams: the API surfaces
//! themselves (GraphQL/JSON servers, admin UIs) and real policy engines. There
//! is **no distributed logic** in the core; `ash-lock` attaches as an optional
//! extension.
//!
//! The shipped extensions and the HLC clock are opt-in cargo features (`fsm`,
//! `audit`, `lock`, `hlc`; `extensions` for the three extensions at once), all
//! **off by default** — a default build links no `ash-*` code beyond the derive
//! macros.
//!
//! Reads are **bounded and ordered**: `Query::limit` caps every read (with the
//! domain's `max_rows` ceiling filling in), `Query::sort` gives it a
//! deterministic order, and `ReadRequest::page` turns the pair into keyset
//! pagination whose cursor neither skips nor repeats rows. Writes to a resource
//! that declares [`Resource::version_attribute`] are **conditional**: the update
//! lands only if the row still holds the version the caller read, and a lost race
//! surfaces as [`Error::Conflict`] instead of silently overwriting.
//!
//! Two more opt-in features: `sandbox` (a deterministic side-effect harness for
//! tests) and `trace` (execution-trace instrumentation — `tracing` spans and
//! per-stage events over every action, off unless the feature is built *and*
//! [`Domain::enable_tracing`] is called, with the trace's sink chosen by the
//! subscriber the consumer installs).
//!
//! ## Example
//!
//! ```
//! use ash_domain::{erase, Context, Domain, DomainConfig, DomainContext, Record, Value, Query, Resource};
//! use ash_domain::action::{ActionDef, ActionInput};
//! use ash_domain::attribute::{Attribute, AttrType};
//! use ash_domain::datalayer::memory::InMemoryDataLayer;
//! use std::sync::Arc;
//!
//! // 1. Declare a resource — a type, resolved at compile time. It keeps the
//! //    dynamic `Record` as its `Data`, so actions hand back a `Record`.
//! struct Note;
//! impl Resource for Note {
//!     const NAME: &'static str = "note";
//!     type Data = Record;
//!     fn attributes() -> Vec<Attribute> {
//!         vec![
//!             Attribute::scalar::<String>("id"),
//!             Attribute::scalar::<String>("title"),
//!             Attribute { default: Some(Value::Bool(false)), ..Attribute::scalar::<bool>("done") },
//!         ]
//!     }
//!     fn actions() -> Vec<ActionDef> {
//!         vec![ActionDef::write("create"), ActionDef::read("read")]
//!     }
//! }
//!
//! let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! rt.block_on(async {
//!     // 2. Build a domain from a plain config struct. Authorization is
//!     //    default-deny, so a domain with no policies must say so explicitly
//!     //    (`PolicySet::permissive()`); register real policies instead when
//!     //    the domain gates access itself.
//!     let domain = Domain::new(DomainConfig {
//!         resources: vec![erase::<Note>()],
//!         policies: ash_domain::PolicySet::permissive(),
//!         ..DomainConfig::default()
//!     }, DomainContext::new());
//!
//!     // 3. Create the context up front over a persistence backend, then thread
//!     //    it — `&mut` — through the actions. A bare `Arc<DataLayer>` is a `Store`.
//!     let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
//!     let created = domain
//!         .handle_action::<Note>(&mut ctx, "create", ActionInput::create_record(Record::from_iter([("title", "hello")])))
//!         .await
//!         .unwrap()
//!         .into_record()
//!         .unwrap();
//!     assert_eq!(created.get("title"), Some(&Value::from("hello")));
//!     assert_eq!(created.get("done"), Some(&Value::Bool(false))); // default applied
//!
//!     let all = domain
//!         .handle_action::<Note>(&mut ctx, "read", ActionInput::read(Query::new("note")))
//!         .await
//!         .unwrap()
//!         .into_records()
//!         .unwrap();
//!     assert_eq!(all.len(), 1);
//! });
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod action;
pub mod aggregate;
pub mod attribute;
pub mod clock;
pub mod context;
pub mod id;
#[cfg(feature = "sandbox")]
pub mod sandbox;
// Internal execution-trace instrumentation (macros used by the executor). Not
// public surface; emission is opt-out at compile time (the `trace` feature) and
// at runtime (`Domain::enable_tracing`).
pub mod datalayer;
pub mod domain;
pub mod error;
pub mod event;
pub mod extension;
pub mod lifecycle;
pub mod metrics;
pub mod policy;
pub mod publish;
mod trace;

// The ash-flare supervisor adapter — a `Worker` that owns a `Domain`'s serving
// lifetime and drives its orderly `close()` on shutdown. Behind the `flare`
// feature so the `ash-flare` dependency is opt-in.
#[cfg(feature = "flare")]
pub mod flare;
pub mod query;
pub mod query_type;
pub mod read;
pub mod resource;
pub mod schema;
pub mod value;

// Re-export `async_trait` so derived impls (e.g. `#[derive(TypedQuery)]`) and
// consumer trait impls can annotate with a version-matched `#[async_trait]`
// without adding their own skew-prone dependency.
pub use async_trait::async_trait;

// Re-export the ash-* crates that appear in the public API so callers can name
// their types without adding a skew-prone dependency of their own. Each rides
// its own feature — a default build links none of them.
#[cfg(feature = "fsm")]
pub use ash_fsm;
#[cfg(feature = "lock")]
pub use ash_lock;
#[cfg(feature = "audit")]
pub use ash_log;
#[cfg(feature = "hlc")]
pub use ash_time;

pub use action::{
    ActionDef, ActionInput, ActionKind, ActionOutcome, ActionResult, Change, Changeset,
    GenericHandler, Preparation, Validation,
};
pub use aggregate::{Aggregate, AggregateKind, Computed, Computer};
pub use attribute::{AttrType, Attribute};
#[cfg(feature = "hlc")]
pub use clock::AshTimeClock;
pub use clock::{Clock, SystemClock};
pub use context::{Context, ContextId, DomainContext, Extensions, FromRef, HandlerContext, Store};
pub use datalayer::{DataLayer, Transaction};
pub use domain::{
    Bound, DEFAULT_MAX_BATCH, DEFAULT_MAX_LOAD_DEPTH, DEFAULT_MAX_ROWS, Domain, DomainBuilder,
    DomainConfig,
};
pub use error::{Error, Result};
pub use event::{DomainEvent, Emitter, EventHandler};
pub use extension::Extension;
pub use id::{DefaultIdGenerator, IdGenerator};
pub use lifecycle::Closable;
pub use metrics::{MetricSample, Metrics, Outcome};
pub use policy::{
    Admit, AllowAll, AuthorizedOne, AuthorizedRead, ClientPolicy, Decision, Deny, Explanation,
    Policy, PolicyCheck, PolicyClient, PolicyLevel, PolicyNode, PolicyRequest, PolicySet,
    PolicyVote, ReadReport, Redaction, Scope, ScopedPolicy, Target, policy_lambda,
};
pub use publish::{Job, JobQueue, Notification, Publisher};
pub use query::{Cursor, Filter, Query, SortDirection, SortKey};
pub use query_type::TypedQuery;
pub use read::{OffsetPage, Page, ReadRequest};
pub use resource::{
    Cardinality, Embeddable, ErasedResource, Loaded, Relationship, Resource, ResourceHandle,
    TenantStrategy, Through, erase,
};
pub use schema::DomainSchema;
pub use value::{FromRecord, FromValue, IntoRecord, Record, Value};

/// Derive macro for the [`Resource`] trait (requires the `derive` feature, on by
/// default). Declare a resource by annotating a struct's fields instead of
/// hand-writing the trait impl.
///
/// - Container: `#[resource(name = "...", table = "...", tenant = "<attr>",
///   tenant_layer)]` (name defaults to the lower-cased type; `table` sets the
///   storage name; `tenant`/`tenant_layer` opt into multitenancy).
/// - Fields: `#[attribute(primary_key, default = <literal>, embed)]`; each
///   field's `AttrType` is a scalar carrying the field's own Rust type (inferred
///   automatically), or an embed. `primary_key` names the resource's key (no
///   attribute flag survives — it emits a `Resource::primary_key()` override).
///   The core runs no attribute validation — enforce presence/ranges/formats in
///   a [`Validation`] on the action. A default set of CRUD actions is generated.
/// - Relationships: `#[relationship(name = "...", has_many|belongs_to|has_one|
///   many_to_many, destination = "...", source = "...", destination_attr =
///   "...")]`, repeatable.
/// - Aggregates: `#[aggregate(count|sum|min|max|exists, name = "...",
///   relationship = "...", field = "...")]`, repeatable (`field` for
///   sum/min/max). Computed fields reference a `Computer` type, so they stay a
///   hand-written [`Resource::computed`](crate::Resource::computed) override.
/// - Typed interface: for the default CRUD actions the derive also generates
///   compile-checked associated fns — `Note::create(&domain, &mut ctx, params)`,
///   `read`, `get` (one row by primary key), `update`, `destroy` — so you name
///   the action as a method, not a
///   string. Each wraps the single [`Domain::handle_action`] entry point and
///   projects its outcome back into the typed row.
/// - Custom actions: `#[action(update, name = "completed")]` (repeatable) declares
///   an action beyond the CRUD set and generates a typed method for it —
///   `Note::completed(&domain, &mut ctx, id, params)`, reachable dynamically as
///   `domain.handle_action::<Note>(&mut ctx, "completed", ActionInput::update(id, params))`.
///   The attribute is declarative — it names the action and its shape (`create` /
///   `update` / `destroy` / `read`); the action's *behavior* (the
///   [`Change`](crate::action::Change) that sets `completed = true`) is attached as
///   explicit code. Generic actions, which
///   need a handler at declaration, stay on the hand-written `actions()` path and
///   run via `ActionInput::generic(input)`.
///
/// A derived resource is resolved by type and hands back **its own struct** —
/// `#[derive(Resource)]` also generates the [`FromRecord`]/[`IntoRecord`]
/// conversions and the typed action methods, so `Note::create` returns a `Note`,
/// not a `Record`:
///
/// ```
/// use std::sync::Arc;
/// use ash_domain::{erase, Context, Domain, DomainConfig, DomainContext, Record, Query, Resource};
/// use ash_domain::datalayer::memory::InMemoryDataLayer;
///
/// #[derive(Resource, Default)]
/// #[resource(name = "note")]
/// struct Note {
///     #[attribute(primary_key)]
///     id: String,
///     title: String,
///     #[attribute(default = false)]
///     done: bool,
/// }
///
/// let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// rt.block_on(async {
///     let domain = Domain::new(DomainConfig {
///         resources: vec![erase::<Note>()],
///         policies: ash_domain::PolicySet::permissive(),
///         ..DomainConfig::default()
///     }, DomainContext::new());
///
///     let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
///     // Typed code interface: `Note::create` is generated by the derive, so the
///     // resource and action name are both compile-checked (no `::<Note>("create")`).
///     let created: Note = Note::create(&domain, &mut ctx, Record::from_iter([("title", "hi")]))
///         .await
///         .unwrap();
///     assert_eq!(created.done, false);       // default applied, typed field
///     assert!(!created.id.is_empty());       // primary key auto-generated
///
///     // `read`/`update`/`destroy` are generated the same way.
///     let all: Vec<Note> = Note::read(&domain, &mut ctx, Query::new("note")).await.unwrap();
///     assert_eq!(all.len(), 1);
/// });
/// ```
///
/// The richer declarations — storage overrides, multitenancy, relationships, and
/// aggregates — are attributes too, so a `Post` scoped to an `org_id` tenant with
/// a `belongs_to` author and a mapped column reads:
///
/// ```
/// use ash_domain::{erase, Resource, TenantStrategy, Cardinality, AggregateKind};
///
/// #[derive(Resource, Default)]
/// #[resource(name = "post", table = "posts", tenant = "org_id")]
/// #[relationship(name = "author", belongs_to, destination = "author",
///                source = "author_id", destination_attr = "id")]
/// #[aggregate(count, name = "comment_count", relationship = "comments")]
/// #[relationship(name = "comments", has_many, destination = "comment",
///                source = "id", destination_attr = "post_id")]
/// struct Post {
///     #[attribute(primary_key)]
///     id: String,
///     author_id: String,
///     org_id: String,
///     title: String,
/// }
///
/// let h = erase::<Post>();
/// assert_eq!(h.storage_name(), "posts");                 // table override
/// assert_eq!(Post::tenant(), Some(TenantStrategy::Attribute("org_id".into())));
/// assert_eq!(Post::relationships().len(), 2);
/// assert_eq!(Post::aggregates()[0].kind, AggregateKind::Count);
/// // The primary-key name is emitted as a `primary_key()` override.
/// assert_eq!(Post::primary_key(), "id");
/// ```
///
#[cfg(feature = "derive")]
pub use ash_macros::Resource;

/// Derive macro for a **read-only projection** [`Resource`] (requires the
/// `derive` feature) — the custom shape a [`TypedQuery`] returns.
///
/// Identical to [`#[derive(Resource)]`](macro@Resource) but with a single `read`
/// action instead of the full CRUD set, and no typed `create`/`update`/`destroy`
/// helpers: a projection / aggregation is never written, only produced as the
/// result of a [`Domain::query`]. Its lone `read` action is what its policies gate
/// and redact under.
///
/// ```
/// use ash_domain::{Projection, Resource, ActionKind};
///
/// #[derive(Projection, Default)]
/// #[resource(name = "owner_stats")]
/// struct OwnerStats {
///     owner_id: String,
///     open_count: i64,
/// }
///
/// let actions = OwnerStats::actions();
/// assert_eq!(actions.len(), 1);
/// assert_eq!(actions[0].kind, ActionKind::Read);
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::Projection;

/// Derive macro for the [`TypedQuery`] trait (requires the `derive` feature) —
/// the `impl` boilerplate for a typed query run through [`Domain::query`].
///
/// A typed query is an ordinary read whose result is a **custom resource** (a
/// projection or aggregation), run against the context's concrete backend. The
/// hand-written impl is pure plumbing — the associated result type, the backend
/// capability bound, and a one-line body dispatching to a backend method — so this
/// derive writes it from a single `#[query(...)]` attribute:
///
/// ```
/// use std::sync::Arc;
/// use ash_domain::{
///     erase, Context, DataLayer, Domain, DomainConfig, DomainContext, Projection,
///     PolicySet, Record, Result, Store, TypedQuery, Value,
/// };
/// use ash_domain::datalayer::memory::InMemoryDataLayer;
///
/// #[derive(Projection, Default)]
/// #[resource(name = "owner_stats")]
/// struct OwnerStats {
///     owner_id: String,
///     open_count: i64,
/// }
///
/// // The backend capability the query needs. The `run =` method takes the
/// // `&Context<B>` the generated `run` hands it.
/// #[ash_domain::async_trait]
/// trait TodoStats: Sized {
///     async fn owner_stats(&self, ctx: &Context<Self>) -> Result<Vec<Record>>;
/// }
///
/// // The whole `impl TypedQuery` from one attribute: which result resource,
/// // which backend-capability bound, and which method to call.
/// #[derive(TypedQuery)]
/// #[query(resource = OwnerStats, backend = TodoStats, run = owner_stats)]
/// struct StatsByOwner;
///
/// // A concrete backend that is both a `Store` and carries the capability.
/// #[derive(Clone)]
/// struct Backend(Arc<InMemoryDataLayer>);
/// impl Store for Backend {
///     fn layer(&self) -> &dyn DataLayer { &*self.0 }
/// }
/// #[ash_domain::async_trait]
/// impl TodoStats for Backend {
///     async fn owner_stats(&self, _ctx: &Context<Self>) -> Result<Vec<Record>> {
///         Ok(vec![Record::from_iter([
///             ("owner_id", Value::from("u-1")),
///             ("open_count", Value::Int(3)),
///         ])])
///     }
/// }
///
/// # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// # rt.block_on(async {
/// let domain = Domain::new(DomainConfig {
///     resources: vec![erase::<OwnerStats>()],
///     policies: PolicySet::permissive(),
///     ..Default::default()
/// }, DomainContext::new());
/// let ctx = Context::new(Backend(Arc::new(InMemoryDataLayer::new())));
///
/// let rows: Vec<OwnerStats> = domain.query(&ctx, StatsByOwner).await.unwrap();
/// assert_eq!(rows[0].open_count, 3);
/// # });
/// ```
///
/// `#[query(...)]` options: `resource = <Type>` (required, the result resource),
/// `run = <method>` (required, the backend method called as
/// `ctx.backend().<method>(ctx)`), `backend = <Trait>` (optional, repeatable —
/// extra bounds on `B` beyond [`Store`]), `action = "<name>"` (optional —
/// authorize under a specific named read action of the result resource), and
/// `tenant_aware` (optional bare flag — see below).
///
/// # Tenant-scoped queries
///
/// When the result [`resource`] is **tenant-scoped**, the domain refuses to run
/// the query unless it declares [`tenant_aware`](TypedQuery::tenant_aware) — a
/// promise that `run` filters by [`ctx.tenant()`](Context::tenant) itself (the
/// domain can't fold a predicate into a hand-shaped read). Add the bare
/// `tenant_aware` flag to the attribute to make that promise; the `run` method
/// must actually honour it. Without it, a tenant-scoped query fails with
/// [`Error::MissingTenant`].
///
/// ```
/// use std::sync::Arc;
/// use ash_domain::{
///     erase, Context, DataLayer, Domain, DomainConfig, DomainContext, Projection,
///     PolicySet, Record, Result, Store, TenantStrategy, TypedQuery, Value,
/// };
/// use ash_domain::datalayer::memory::InMemoryDataLayer;
///
/// // A tenant-scoped projection: it carries an `org_id` discriminator.
/// #[derive(Projection, Default)]
/// #[resource(name = "org_stats", tenant = "org_id")]
/// struct OrgStats {
///     org_id: String,
///     open_count: i64,
/// }
///
/// #[ash_domain::async_trait]
/// trait OrgReports: Sized {
///     async fn org_stats(&self, ctx: &Context<Self>) -> Result<Vec<Record>>;
/// }
///
/// // `tenant_aware` asserts `run` scopes by the tenant, so the guard admits it.
/// #[derive(TypedQuery)]
/// #[query(resource = OrgStats, backend = OrgReports, run = org_stats, tenant_aware)]
/// struct StatsByOrg;
///
/// #[derive(Clone)]
/// struct Backend(Arc<InMemoryDataLayer>);
/// impl Store for Backend {
///     fn layer(&self) -> &dyn DataLayer { &*self.0 }
/// }
/// #[ash_domain::async_trait]
/// impl OrgReports for Backend {
///     async fn org_stats(&self, ctx: &Context<Self>) -> Result<Vec<Record>> {
///         // A real impl filters by this; here we just echo it back.
///         let org = ctx.tenant().and_then(Value::as_str).unwrap_or("").to_string();
///         Ok(vec![Record::from_iter([
///             ("org_id", Value::from(org)),
///             ("open_count", Value::Int(2)),
///         ])])
///     }
/// }
///
/// # let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// # rt.block_on(async {
/// let domain = Domain::new(DomainConfig {
///     resources: vec![erase::<OrgStats>()],
///     policies: PolicySet::permissive(),
///     ..Default::default()
/// }, DomainContext::new());
///
/// let mut ctx = Context::new(Backend(Arc::new(InMemoryDataLayer::new())));
/// ctx.set_tenant("acme");
/// let rows: Vec<OrgStats> = domain.query(&ctx, StatsByOrg).await.unwrap();
/// assert_eq!(rows[0].org_id, "acme");
/// # });
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::TypedQuery;

/// Derive macro for the [`Embeddable`] trait (requires the `derive` feature).
///
/// Declares a struct as an **embedded resource** — a structured value stored
/// inside another resource, with its own attributes but no name, actions, or
/// table. Uses the same `#[attribute(...)]` field options as
/// [`Resource`](macro@Resource); declare a field of the embeddable type on a
/// parent resource with `#[attribute(embed)]` (or `Vec<T>` for a repeated
/// embed):
///
/// ```
/// use ash_domain::{Embeddable, Resource};
///
/// #[derive(Embeddable, Default)]
/// struct Address {
///     city: String,
///     #[attribute(default = "US")]
///     country: String,
/// }
///
/// #[derive(Resource, Default)]
/// #[resource(name = "customer")]
/// struct Customer {
///     #[attribute(primary_key)]
///     id: String,
///     #[attribute(embed)]
///     address: Address,
/// }
///
/// // The embedded schema is carried inline on the parent attribute.
/// let attrs = Customer::attributes();
/// let address = attrs.into_iter().find(|a| a.name == "address").unwrap();
/// assert!(matches!(address.ty, ash_domain::attribute::AttrType::_Embed(_)));
/// ```
///
/// The parent round-trips as a typed struct: the embed reads back as a nested
/// `Address`, with its declared default applied by the write pipeline.
///
/// ```
/// # use std::sync::Arc;
/// # use ash_domain::{Embeddable, Resource, Context, Domain, DomainConfig, DomainContext, Record, Value, erase};
/// # use ash_domain::datalayer::memory::InMemoryDataLayer;
/// #[derive(Embeddable, Default, PartialEq, Debug)]
/// struct Address {
///     city: String,
///     #[attribute(default = "US")]
///     country: String,
/// }
///
/// #[derive(Resource, Default)]
/// #[resource(name = "customer")]
/// struct Customer {
///     #[attribute(primary_key)]
///     id: String,
///     #[attribute(embed)]
///     address: Address,
/// }
///
/// let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// rt.block_on(async {
///     let domain = Domain::new(DomainConfig {
///         resources: vec![erase::<Customer>()],
///         policies: ash_domain::PolicySet::permissive(),
///         ..Default::default()
///     }, DomainContext::new());
///     let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
///
///     // Build the embed as a nested map; omit `country` to prove the default.
///     let mut addr = Record::new();
///     addr.insert("city", "London");
///     let created: Customer = Customer::create(
///         &domain, &mut ctx,
///         Record::from_iter([("address", Value::from(addr))]),
///     ).await.unwrap();
///
///     assert_eq!(created.address, Address { city: "London".into(), country: "US".into() });
/// });
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::Embeddable;

/// Derive macro for the [`FromRecord`] trait (requires the `derive` feature).
///
/// [`#[derive(Resource)]`](macro@Resource) and
/// [`#[derive(Embeddable)]`](macro@Embeddable) already generate this conversion;
/// derive it on its own for a **plain data type** that isn't a resource — a read
/// projection or a generic action's typed output — so you can hydrate it from the
/// dynamic [`Record`] the pipeline flows. `Option<T>` fields are nullable,
/// integers are range-checked on the way in, and `#[attribute(embed)]` fields
/// round-trip through a nested type's `FromRecord`.
///
/// ```
/// use ash_domain::{FromRecord, Record, Value};
///
/// #[derive(FromRecord)]
/// struct AuthorRow {
///     id: i64,
///     name: String,
///     bio: Option<String>,
/// }
///
/// let rec = Record::from_iter([("id", Value::Int(7)), ("name", Value::from("Ada"))]);
/// let row = AuthorRow::from_record(&rec).unwrap();
/// assert_eq!((row.id, row.name.as_str(), row.bio), (7, "Ada", None));
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::FromRecord;

/// Derive macro for the [`IntoRecord`] trait (requires the `derive` feature).
///
/// [`#[derive(Resource)]`](macro@Resource) and
/// [`#[derive(Embeddable)]`](macro@Embeddable) already generate this conversion;
/// derive it on its own for a **typed action-params type**, so you can pass a
/// struct where the [`Domain`] write methods want `impl IntoRecord` instead of
/// hand-building a `Record::from_iter([...])`.
///
/// ```
/// use ash_domain::{IntoRecord, Value};
///
/// #[derive(IntoRecord)]
/// struct NewNote {
///     title: String,
///     done: bool,
/// }
///
/// // `into_record` is fallible (an out-of-`i64` integer field would error);
/// // for these string/bool fields it can only succeed.
/// let rec = NewNote { title: "hi".into(), done: false }.into_record().unwrap();
/// assert_eq!(rec.get("title"), Some(&Value::from("hi")));
/// assert_eq!(rec.get("done"), Some(&Value::Bool(false)));
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::IntoRecord;

/// Derive macro for a **string-backed enum field** (requires the `derive`
/// feature): generates [`FromValue`] and `From<Self> for Value` for a fieldless
/// enum, so a resource field can be a real Rust enum instead of a bare `String`.
///
/// Mark the field `#[attribute(enumerate)]` on the [`Resource`](macro@Resource)
/// and derive `ValueEnum` on its type. Each variant stores as its name
/// (lower-cased, or a `#[value(rename = "...")]` override); a string matching no
/// variant fails the read with [`Error::Serialization`], so an unknown value can
/// never enter a typed row.
///
/// ```
/// use ash_domain::{FromValue, Value, ValueEnum};
///
/// #[derive(ValueEnum, PartialEq, Debug)]
/// enum Status {
///     Open,
///     Done,
///     #[value(rename = "in_progress")]
///     InProgress,
/// }
///
/// assert_eq!(Value::from(Status::Open), Value::from("open"));
/// assert_eq!(Value::from(Status::InProgress), Value::from("in_progress"));
/// assert_eq!(Status::from_value(&Value::from("done")).unwrap(), Status::Done);
/// assert!(Status::from_value(&Value::from("nope")).is_err());
/// ```
#[cfg(feature = "derive")]
pub use ash_macros::ValueEnum;
