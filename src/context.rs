//! The [`Context`] — the mutable unit of work, generic over its backend.
//!
//! A `Context<B>` is the object you run work through. You create it *first*, at
//! the very beginning of a unit of work, and thread it — `&mut` — into every
//! action. It carries a stable [`ContextId`] (a correlation id for logging and
//! tracing), an optional acting principal, and a backend `B` of your choosing.
//!
//! The backend is the point of this generic: **`B` decides what the context can
//! do.** The one capability the core defines is persistence:
//!
//! * [`Store`] — persistence. The CRUD [`Domain`](crate::Domain) actions
//!   (`create` / `read` / `update` / `destroy`) require `B: Store`, which hands
//!   them a [`DataLayer`] to run against.
//!
//! Anything richer — transactions, batching, savepoints, a unit-of-work — is
//! **the consumer's to define**: implement a [`DataLayer`] with the extra
//! methods you need and a backend (a `Store`) that drives them, then add the
//! lifecycle methods to *your* type. The core deliberately ships no transaction
//! machinery, so it imposes no lifecycle you must fit.
//!
//! Alongside its backend, a context carries two open-ended bags any operation
//! threaded through it can write to:
//!
//! * [`Extensions`] — a type-indexed bag of *live* values ([`insert`](Context::insert)
//!   / [`get`](Context::get)): a request id, a recorded policy decision, a db or
//!   cache client, a logger. One value per Rust type, fully typed.
//! * `meta` — a string-keyed [`Record`] of dynamic, serializable values
//!   ([`set_meta`](Context::set_meta) / [`get_meta`](Context::get_meta)) for
//!   trace ids, flags, and arbitrary annotations.
//!
//! So the shape of your context is a compile-time choice. Persisting work? Any
//! `Arc<L>` over a [`DataLayer`] `L` is a `Store`, so it is a ready-made backend
//! — `Context::new(Arc::new(layer))` — and the CRUD actions light up. Just
//! processing data, with no storage? Use a backend that is not a `Store` —
//! `Context<()>` is the degenerate case — and the CRUD methods are simply not
//! there to call.
//!
//! ```
//! use std::sync::Arc;
//! use ash_domain::{Context, Record, Value, DataLayer, Store};
//! use ash_domain::datalayer::memory::InMemoryDataLayer;
//!
//! let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! rt.block_on(async {
//!     let layer = Arc::new(InMemoryDataLayer::new());
//!
//!     // A persistence-backed context: created up front, threaded `&mut`.
//!     // `Arc<InMemoryDataLayer>` is a `Store`, so it is a backend directly.
//!     let mut ctx = Context::new(layer.clone());
//!
//!     ctx.backend()
//!         .layer()
//!         .create("note", "id", Record::from_iter([("id", "1"), ("title", "a")]))
//!         .await
//!         .unwrap();
//!     assert!(layer.get("note", "id", &Value::from("1")).await.unwrap().is_some());
//!
//!     // A store-less context: the CRUD actions won't accept it.
//!     let _process_only = Context::new(());
//! });
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::datalayer::DataLayer;
use crate::error::Result;
use crate::value::{Record, Value};

/// A unique identifier for a [`Context`] — a correlation id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContextId(String);

impl ContextId {
    /// Generate a fresh, process-unique id.
    pub fn generate() -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        ContextId(format!("ctx-{nanos}-{seq}"))
    }

    /// Wrap an existing id string (e.g. a request id propagated from upstream).
    pub fn new(id: impl Into<String>) -> Self {
        ContextId(id.into())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContextId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A type-indexed bag of arbitrary values carried by a [`Context`].
///
/// This is how work threaded through a context accumulates *live* state that the
/// backend generic doesn't model: a request id set by an HTTP layer, a policy
/// decision recorded by an authorization check, a database or cache client to be
/// reused downstream, a logger or span. Each value is keyed by its own Rust
/// type, so there is **at most one value per type** and reads are fully typed —
/// no string keys, no downcasting at the call site. This is the same pattern as
/// `http::Extensions`.
///
/// Values must be `Send + Sync + 'static` so the context (and everything it
/// carries) can cross `await` points and move between threads.
///
/// ```
/// use ash_domain::Extensions;
///
/// #[derive(Debug, PartialEq)]
/// struct RequestId(String);
///
/// let mut ext = Extensions::new();
/// ext.insert(RequestId("abc-123".into()));
///
/// assert_eq!(ext.get::<RequestId>(), Some(&RequestId("abc-123".into())));
/// assert!(ext.get::<u64>().is_none());
/// ```
#[derive(Default)]
pub struct Extensions {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl Extensions {
    /// An empty bag.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a value, keyed by its type. Returns the previous value of the same
    /// type, if one was present.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(value))
            .and_then(downcast_owned)
    }

    /// Borrow the value of type `T`, if one is present.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref())
    }

    /// Mutably borrow the value of type `T`, if one is present.
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut())
    }

    /// Whether a value of type `T` is present.
    pub fn contains<T: Send + Sync + 'static>(&self) -> bool {
        self.map.contains_key(&TypeId::of::<T>())
    }

    /// Remove and return the value of type `T`, if one is present.
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.map.remove(&TypeId::of::<T>()).and_then(downcast_owned)
    }

    /// Move every value out of `other` into this bag, consuming it.
    ///
    /// A type present in both is **overwritten by `other`'s** value: the bag
    /// being merged in is the newer, more specific one (a handler's scratch
    /// merging back into the request context), so it wins.
    ///
    /// ```
    /// use ash_domain::Extensions;
    /// #[derive(Debug, PartialEq)] struct Timing(u64);
    /// #[derive(Debug, PartialEq)] struct Tag(&'static str);
    ///
    /// let mut parent = Extensions::new();
    /// parent.insert(Timing(1));
    /// parent.insert(Tag("parent"));
    ///
    /// let mut scratch = Extensions::new();
    /// scratch.insert(Timing(99));
    ///
    /// parent.merge(scratch);
    /// assert_eq!(parent.get::<Timing>(), Some(&Timing(99))); // scratch wins
    /// assert_eq!(parent.get::<Tag>(), Some(&Tag("parent"))); // untouched
    /// ```
    pub fn merge(&mut self, other: Extensions) {
        self.map.extend(other.map);
    }

    /// Whether the bag holds no values.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// How many values the bag holds.
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.map.len())
            .finish()
    }
}

fn downcast_owned<T: 'static>(boxed: Box<dyn Any + Send + Sync>) -> Option<T> {
    boxed.downcast().ok().map(|b| *b)
}

/// The **domain-scoped** bag of shared clients, owned by the
/// [`Domain`](crate::Domain) for its whole lifetime.
///
/// Where an [`Extensions`] bag on a [`Context`] is *request-scoped* (one unit of
/// work), a `DomainContext` holds the long-lived handles a domain and its
/// handlers reuse across every operation: an HTTP client, a database or cache
/// pool, a message-bus producer, an external policy-service client. It is passed
/// to [`Domain::new`](crate::Domain::new) and stored there.
///
/// Clients are keyed by type (the same primitive as [`Extensions`]), so there is
/// **at most one client per type** and handler code retrieves one with no string
/// keys or downcasts:
///
/// ```
/// use ash_domain::DomainContext;
///
/// #[derive(Debug, PartialEq)]
/// struct HttpClient(&'static str);
/// #[derive(Debug, PartialEq)]
/// struct CachePool(u32);
///
/// let dc = DomainContext::new()
///     .with(HttpClient("https://api"))
///     .with(CachePool(16));
///
/// assert_eq!(dc.client::<HttpClient>(), Some(&HttpClient("https://api")));
/// assert_eq!(dc.client::<CachePool>(), Some(&CachePool(16)));
/// assert!(dc.client::<u64>().is_none());
/// ```
///
/// A handler reaches these through its
/// [`HandlerContext::client`](HandlerContext::client), which projects them out
/// of the domain that owns this bag.
///
/// Each client must be `Send + Sync + 'static` (typically a cheap-to-clone
/// `Arc`-backed handle), so the domain — and everything it lends to handlers
/// across threads and `await` points — stays `Send + Sync`.
#[derive(Default)]
pub struct DomainContext {
    clients: Extensions,
    /// Clients that opted into shutdown via [`Closable`](crate::lifecycle::Closable),
    /// in registration order. [`Domain::close`](crate::Domain::close) drives these
    /// in **reverse** of this order. Kept separate from `clients` because closing
    /// is opt-in: a client is registered here *only* through
    /// [`with_closable`](DomainContext::with_closable) / [`insert_closable`](DomainContext::insert_closable).
    closables: Vec<crate::lifecycle::ClosableHandle>,
}

impl DomainContext {
    /// An empty bag — a domain with no shared clients.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `client`, keyed by its type (builder style). A later `with` of
    /// the same type replaces the earlier one.
    pub fn with<T: Send + Sync + 'static>(mut self, client: T) -> Self {
        self.clients.insert(client);
        self
    }

    /// Register `client` both as a type-keyed shared client (like
    /// [`with`](DomainContext::with)) **and** as a
    /// [`Closable`](crate::lifecycle::Closable) that
    /// [`Domain::close`](crate::Domain::close) will shut down.
    ///
    /// Use this for clients that hold real resources needing orderly release —
    /// a connection pool, a message-broker producer, a socket. On close they are
    /// shut down in **reverse** registration order (last registered, first
    /// closed). The client is retrievable by type exactly as with `with`.
    pub fn with_closable<T>(mut self, client: T) -> Self
    where
        T: crate::lifecycle::Closable + Clone,
    {
        self.insert_closable(client);
        self
    }

    /// In-place form of [`with_closable`](DomainContext::with_closable): register
    /// `client` as both a type-keyed client and a shutdown target. Returns the
    /// previous client of the same type, if one was present (its `Closable`
    /// registration, if any, is left in the shutdown list — closing a
    /// superseded handle is harmless given the idempotent contract).
    pub fn insert_closable<T>(&mut self, client: T) -> Option<T>
    where
        T: crate::lifecycle::Closable + Clone,
    {
        self.closables.push(Arc::new(client.clone()));
        self.clients.insert(client)
    }

    /// The registered [`Closable`](crate::lifecycle::Closable) clients, in
    /// registration order. [`Domain::close`](crate::Domain::close) is the normal
    /// caller; exposed so service code can inspect how many shutdown targets a
    /// domain carries.
    pub(crate) fn closables(&self) -> &[crate::lifecycle::ClosableHandle] {
        &self.closables
    }

    /// Register `client` in place, returning the previous client of the same
    /// type, if any.
    pub fn insert<T: Send + Sync + 'static>(&mut self, client: T) -> Option<T> {
        self.clients.insert(client)
    }

    /// Borrow the client of type `T`, if one was registered.
    pub fn client<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.clients.get::<T>()
    }

    /// Whether a client of type `T` is registered.
    pub fn has<T: Send + Sync + 'static>(&self) -> bool {
        self.clients.contains::<T>()
    }

    /// How many clients are registered.
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    /// Whether no clients are registered.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

impl std::fmt::Debug for DomainContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DomainContext")
            .field("clients", &self.clients.len())
            .field("closables", &self.closables.len())
            .finish()
    }
}

/// Project a value of type `Self` *out of* a composite backend `B`.
///
/// This is ash-domain's analogue of axum's `FromRef`. A context carries **one**
/// backend `B` ([`Context::backend`]), but a real application's backend is
/// usually a bundle — a data layer *and* a cache client *and* a metrics handle.
/// `FromRef` lets each of those be extracted from the one `B` by type, so code
/// asks for the piece it needs ([`ctx.extract::<Cache>()`](Context::extract))
/// without knowing the shape of the whole backend.
///
/// It is the composition seam that keeps `B` from having to be monolithic: your
/// backend is one `AppState` struct, you implement `FromRef<AppState>` for each
/// utility it holds (cheap `.clone()`s of `Arc`-shared handles), and both the
/// CRUD [`Store`] and your own code pull what they need out of the same value.
///
/// Every type is trivially projectable from itself (the reflexive blanket impl
/// below), so a single-utility backend needs no impl at all —
/// `ctx.extract::<B>()` clones the backend. Provide impls only to expose the
/// *parts* of a composite backend.
///
/// ```
/// use std::sync::Arc;
/// use ash_domain::{Context, FromRef};
///
/// #[derive(Clone)]
/// struct Cache(Arc<str>);
/// #[derive(Clone)]
/// struct Metrics(u64);
///
/// // One backend bundling several utilities.
/// #[derive(Clone)]
/// struct AppState { cache: Cache, metrics: Metrics }
///
/// impl FromRef<AppState> for Cache {
///     fn from_ref(state: &AppState) -> Self { state.cache.clone() }
/// }
/// impl FromRef<AppState> for Metrics {
///     fn from_ref(state: &AppState) -> Self { state.metrics.clone() }
/// }
///
/// let ctx = Context::new(AppState { cache: Cache("redis".into()), metrics: Metrics(0) });
/// let cache: Cache = ctx.extract::<Cache>();       // projected out of the bundle
/// assert_eq!(&*cache.0, "redis");
/// let _metrics: Metrics = ctx.extract();
/// ```
pub trait FromRef<B> {
    /// Extract a `Self` from a reference to the composite backend `B`.
    fn from_ref(backend: &B) -> Self;
}

/// Every backend is projectable from itself: `ctx.extract::<B>()` clones it.
/// This is what lets a single-utility backend skip `FromRef` impls entirely,
/// and it is why the CRUD [`Store`] path is unaffected by projection.
impl<B: Clone> FromRef<B> for B {
    fn from_ref(backend: &B) -> Self {
        backend.clone()
    }
}

/// The persistence capability of a context's backend.
///
/// A backend that can store records implements `Store`; the CRUD
/// [`Domain`](crate::Domain) actions take a `Context<impl Store>` and reach the
/// storage through [`layer`](Store::layer). A backend that does *not* implement
/// `Store` (for example `()`) cannot be passed to those actions at all — the
/// bound is not satisfied — which is how a store-less context statically opts
/// out of persistence.
///
/// Any `Arc<L>` over a [`DataLayer`] `L` is a `Store` (see the blanket impl
/// below), so a bare data layer is a usable backend with no wrapper. When you
/// want richer behaviour around persistence — transactions, batching, a
/// unit-of-work — define your own type, implement `Store` for it (returning the
/// [`DataLayer`] to run against), and give that type whatever lifecycle methods
/// the behaviour needs. The core takes no position on that lifecycle.
#[async_trait::async_trait]
pub trait Store: Send + Sync {
    /// The [`DataLayer`] the CRUD actions run against.
    fn layer(&self) -> &dyn DataLayer;

    /// **Optional atomicity capability:** open a [`Transaction`](crate::Transaction) the write
    /// pipeline runs inside, or `Ok(None)` (the default) to decline.
    ///
    /// When a store returns `Some(txn)`, the [`Domain`](crate::Domain) persists
    /// and runs `after_action` extensions **through the transaction**, then
    /// [`commit`](crate::datalayer::Transaction::commit)s — so a failure anywhere
    /// before the commit rolls the write back instead of leaving it durable. When
    /// it returns `None`, the pipeline uses the bare [`layer`](Store::layer) and
    /// keeps today's behaviour (persist commits immediately; a later failure can
    /// return `Err` on a committed row). Event delivery stays post-commit and
    /// best-effort in **both** paths — atomic event delivery needs an outbox,
    /// which is a consumer concern.
    ///
    /// The default declines, so every existing backend is unaffected. Override it
    /// to opt a store into transactional writes. See `docs/transaction-seam.md`.
    async fn begin(&self) -> Result<Option<Box<dyn crate::datalayer::Transaction>>> {
        Ok(None)
    }
}

/// A bare data layer, shared via `Arc`, is a ready-made [`Store`] backend: no
/// wrapper type needed for the common case of "persist straight through to this
/// layer". `Context::new(Arc::new(my_layer))` just works.
impl<L: DataLayer> Store for Arc<L> {
    fn layer(&self) -> &dyn DataLayer {
        &**self
    }
}

/// The mutable unit of work, generic over its backend `B`.
///
/// See the [module docs](self) for the capability model. Construct one with
/// [`new`](Context::new), set an actor if you have one, and thread it `&mut`
/// through the [`Domain`](crate::Domain) actions.
pub struct Context<B> {
    id: ContextId,
    actor: Option<Record>,
    tenant: Option<Value>,
    cross_tenant: bool,
    extensions: Extensions,
    meta: Record,
    backend: B,
}

impl<B> Context<B> {
    /// Create a context over `backend`, with a freshly-generated id.
    pub fn new(backend: B) -> Self {
        Self::with_id(backend, ContextId::generate())
    }

    /// Create a context over `backend` with an explicit id (e.g. a propagated
    /// correlation id).
    pub fn with_id(backend: B, id: ContextId) -> Self {
        Self {
            id,
            actor: None,
            tenant: None,
            cross_tenant: false,
            extensions: Extensions::new(),
            meta: Record::new(),
            backend,
        }
    }

    /// Set the acting principal, chaining — the construction-time form of
    /// [`set_actor`](Context::set_actor), for the per-request path where a
    /// context is built, populated, and used in one expression:
    ///
    /// ```
    /// # use ash_domain::{Context, Record};
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # use std::sync::Arc;
    /// let ctx = Context::new(Arc::new(InMemoryDataLayer::new()))
    ///     .with_actor(Record::from_iter([("id", "u1"), ("role", "admin")]))
    ///     .with_tenant("acme");
    /// # let _ = ctx;
    /// ```
    pub fn with_actor(mut self, actor: Record) -> Self {
        self.actor = Some(actor);
        self
    }

    /// Scope this unit of work to a tenant, chaining — the construction-time
    /// form of [`set_tenant`](Context::set_tenant). See
    /// [`with_actor`](Context::with_actor) for the intended shape.
    pub fn with_tenant(mut self, tenant: impl Into<Value>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// This context's unique id.
    pub fn id(&self) -> &ContextId {
        &self.id
    }

    /// The acting principal, if set.
    ///
    /// The actor is an opaque [`Record`] — **the core defines no identity type**
    /// (no `User`, session, token, or credential). It only carries whatever
    /// record you set here to the [`Policy`](crate::Policy) seam. That is the
    /// whole authentication contract: an auth library (an `ash-auth`) is a
    /// *consumer-side* concern that verifies credentials however it likes — its
    /// own tables, its own hashing, its own token format — and then calls
    /// [`set_actor`](Context::set_actor) with a record of whatever shape it
    /// chooses. Nothing about identity is locked to `ash-domain`; swap the auth
    /// library and the core is unaffected.
    pub fn actor(&self) -> Option<&Record> {
        self.actor.as_ref()
    }

    /// Set the acting principal — the record an auth layer produces after
    /// verifying a request. Its shape is entirely the caller's; the core treats
    /// it as opaque and only hands it to policies. See [`actor`](Context::actor).
    pub fn set_actor(&mut self, actor: Record) {
        self.actor = Some(actor);
    }

    /// The tenant this unit of work is scoped to, if any.
    ///
    /// A [`Domain`](crate::Domain) action on a resource that declares a tenant
    /// strategy ([`Resource::tenant`](crate::Resource::tenant)) reads this to
    /// scope storage: stamping it onto created records and filtering reads,
    /// updates, and destroys by it. A resource with no tenant strategy ignores
    /// it entirely.
    pub fn tenant(&self) -> Option<&Value> {
        self.tenant.as_ref()
    }

    /// Scope this unit of work to a tenant. Threaded into every subsequent action
    /// on a tenant-scoped resource until [`clear_tenant`](Context::clear_tenant).
    pub fn set_tenant(&mut self, tenant: impl Into<Value>) {
        self.tenant = Some(tenant.into());
    }

    /// Remove the tenant scope.
    pub fn clear_tenant(&mut self) {
        self.tenant = None;
    }

    /// Whether this unit of work may **read across tenants** — the sanctioned,
    /// visible escape from tenant scoping for a legitimate global/admin read.
    ///
    /// Defaults to `false`. See [`allow_cross_tenant`](Context::allow_cross_tenant)
    /// for the exact contract; the [`Domain`](crate::Domain) read paths consult
    /// this, writes never do.
    pub fn is_cross_tenant(&self) -> bool {
        self.cross_tenant
    }

    /// Opt this unit of work into reading **across all tenants**, dropping the
    /// tenant predicate the domain would otherwise fold into a
    /// [`read`](crate::Domain::handle_action) of a tenant-scoped resource.
    ///
    /// This is the deliberate, auditable seam for a global/admin read — the *only*
    /// sanctioned way past tenant scoping, so such a read is a visible call at the
    /// site rather than a raw query smuggled around the guard. Its contract is
    /// narrow on purpose:
    ///
    /// * **Reads only.** Create / update / destroy ignore it and still fail-closed:
    ///   a write needs a tenant ([`MissingTenant`](crate::Error::MissingTenant)),
    ///   is stamped into it, and a cross-tenant update/destroy is still `NotFound`.
    ///   A [`TypedQuery`](crate::query_type::TypedQuery) must additionally opt in
    ///   with `tenant_aware`, which under this flag means "scoped to *all* tenants".
    /// * **Authorization still runs.** The flag removes the tenant *predicate*, not
    ///   the policy gate — a caller with no admitting read policy is still
    ///   [`Forbidden`](crate::Error::Forbidden). Gate this behind an admin policy.
    /// * **Visible downstream.** The domain records the intent on the
    ///   [`Query`](crate::Query::across_tenants) it hands the layer, so a data
    ///   layer (and audit) can see a cross-tenant read for what it is.
    ///
    /// Stays set until [`deny_cross_tenant`](Context::deny_cross_tenant).
    pub fn allow_cross_tenant(&mut self) {
        self.cross_tenant = true;
    }

    /// Re-scope this unit of work to its tenant, undoing
    /// [`allow_cross_tenant`](Context::allow_cross_tenant).
    pub fn deny_cross_tenant(&mut self) {
        self.cross_tenant = false;
    }

    /// The typed extension bag: live values keyed by their Rust type — request
    /// ids, policy decisions, clients, loggers. See [`Extensions`].
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    /// Mutably borrow the [extension bag](Extensions).
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    /// Merge a finished [`HandlerContext`]'s scratch bag into this context's
    /// [extension bag](Extensions) — the merge-back the
    /// [`Domain`](crate::Domain) performs after a generic action's handler
    /// returns.
    ///
    /// This is what makes handler enrichment (a policy decision it observed, a
    /// timing, a downstream client it opened) reachable by the caller once the
    /// action is over. A type the handler recorded **overwrites** the same type
    /// already in the context: the handler ran later and saw more.
    ///
    /// The merge happens only on the handler's **success** path. A handler that
    /// returned `Err` had its action fail, and the core does not half-apply a
    /// failed action's observations to the request that outlived it.
    pub fn merge_scratch(&mut self, scratch: Extensions) {
        self.extensions.merge(scratch);
    }

    /// Stash a value in the [extension bag](Extensions), keyed by its type.
    /// Returns the previous value of the same type, if any. Shorthand for
    /// `ctx.extensions_mut().insert(value)`.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.extensions.insert(value)
    }

    /// Borrow a value of type `T` from the [extension bag](Extensions), if
    /// present. Shorthand for `ctx.extensions().get::<T>()`.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.extensions.get::<T>()
    }

    /// Read-only view of the string-keyed metadata bag: dynamic, serializable
    /// values (trace ids, feature flags, arbitrary annotations) carried
    /// alongside the typed [extensions](Extensions).
    pub fn meta(&self) -> &Record {
        &self.meta
    }

    /// Set a string-keyed metadata value.
    pub fn set_meta(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.meta.insert(key, value);
    }

    /// Look up a string-keyed metadata value.
    pub fn get_meta(&self, key: &str) -> Option<&Value> {
        self.meta.get(key)
    }

    /// Borrow the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Mutably borrow the backend.
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Project a utility of type `T` out of the backend by value.
    ///
    /// This is the axum-`State` move: the context holds one composite backend,
    /// and `extract` pulls a named piece out of it via [`FromRef`]. A data
    /// layer, a cache client, a metrics handle — each is `ctx.extract::<T>()`,
    /// regardless of how the backend bundles them. `T = B` works with no impl
    /// (the reflexive [`FromRef`] clones the whole backend); expose parts of a
    /// composite backend by implementing `FromRef<B>` for each.
    ///
    /// Extraction is by value — utilities are meant to be cheap-to-clone shared
    /// handles (`Arc`-backed pools, clients), the same contract axum places on
    /// its state.
    pub fn extract<T: FromRef<B>>(&self) -> T {
        T::from_ref(&self.backend)
    }

    /// Consume the context and return its backend.
    pub fn into_backend(self) -> B {
        self.backend
    }
}

/// A short-lived, **derived** context handed to a handler for the span of a
/// single operation.
///
/// Where a [`Context<B>`] is the caller's long-lived unit of work, a
/// `HandlerContext` is the *inner* view the [`Domain`](crate::Domain) builds
/// around one action while its handler runs, and drops when the action returns.
/// It exists to give handler code a richer, scoped surface than the bare
/// [`Changeset`](crate::action::Changeset) + [`Store`] pair without exposing (or
/// letting a handler mutate) the whole parent context.
///
/// It carries:
///
/// * a borrow of the [`Domain`](crate::Domain), so a handler can introspect
///   registered resources and the [`policy tree`](crate::Domain::policy_tree) —
///   and, later, re-enter the domain to run nested actions;
/// * **read-only** projections of the parent context's request state — the
///   [`actor`](HandlerContext::actor), [`tenant`](HandlerContext::tenant),
///   [`id`](HandlerContext::id), and [`meta`](HandlerContext::meta);
/// * the optional [`Store`] the handler reads/writes through;
/// * its **own** [`scratch`](HandlerContext::scratch) bag — a fresh
///   [`Extensions`] the handler writes enrichment into (policy results it
///   observed, logs, timings). It starts **empty** rather than inheriting the
///   parent's bag, so a handler cannot mutate request state mid-action; what it
///   records is merged back into the parent context by
///   [`Context::merge_scratch`](Context::merge_scratch) *after* the handler
///   returns, where the caller can read it.
///
/// Once the operation is done the [`Domain`](crate::Domain) calls
/// [`complete`](HandlerContext::complete), which **consumes** the context (and
/// hands back its scratch bag). Spent-context reuse is therefore a *compile
/// error*, not a runtime panic: the handler only ever borrows the context, so it
/// cannot keep it, and after `complete` there is no context left to misuse. The
/// lifetime `'a` additionally ties the borrows to the parent context and domain,
/// so a `HandlerContext` cannot outlive the operation it was derived for.
pub struct HandlerContext<'a> {
    domain: &'a crate::Domain,
    id: &'a ContextId,
    actor: Option<&'a Record>,
    tenant: Option<&'a Value>,
    meta: &'a Record,
    store: Option<&'a dyn Store>,
    scratch: Extensions,
}

impl<'a> HandlerContext<'a> {
    /// Derive a handler context from its parts. Called by the
    /// [`Domain`](crate::Domain) when it is about to run a handler; not usually
    /// constructed by consumer code.
    pub fn new(
        domain: &'a crate::Domain,
        id: &'a ContextId,
        actor: Option<&'a Record>,
        tenant: Option<&'a Value>,
        meta: &'a Record,
        store: Option<&'a dyn Store>,
    ) -> Self {
        Self {
            domain,
            id,
            actor,
            tenant,
            meta,
            store,
            scratch: Extensions::new(),
        }
    }

    /// The domain this operation runs within — for introspection now, re-entry
    /// later.
    pub fn domain(&self) -> &'a crate::Domain {
        self.domain
    }

    /// Borrow a shared client of type `T` from the domain's
    /// [`DomainContext`], if one was registered when the
    /// domain was built.
    ///
    /// This is how a handler reaches the long-lived, domain-scoped handles — an
    /// HTTP client, a cache pool, an external service client — rather than
    /// receiving them per call. Returns `None` when no client of that type is
    /// registered.
    ///
    /// ```
    /// # use ash_domain::HandlerContext;
    /// # struct HttpClient;
    /// # fn use_it(ctx: &HandlerContext<'_>) {
    /// if let Some(_http) = ctx.client::<HttpClient>() {
    ///     // call out through the shared client…
    /// }
    /// # }
    /// ```
    pub fn client<T: Send + Sync + 'static>(&self) -> Option<&'a T> {
        self.domain.domain_context().client::<T>()
    }

    /// The parent context's correlation id.
    pub fn id(&self) -> &ContextId {
        self.id
    }

    /// The acting principal, if the parent context had one (read-only).
    pub fn actor(&self) -> Option<&Record> {
        self.actor
    }

    /// The tenant this operation is scoped to, if any (read-only).
    pub fn tenant(&self) -> Option<&Value> {
        self.tenant
    }

    /// The parent context's string-keyed metadata (read-only).
    pub fn meta(&self) -> &Record {
        self.meta
    }

    /// Look up a parent metadata value by key.
    pub fn get_meta(&self, key: &str) -> Option<&Value> {
        self.meta.get(key)
    }

    /// The [`Store`] the handler runs against, if this operation was invoked on a
    /// persistence-backed context. Mirrors the `store` argument handlers already
    /// receive.
    pub fn store(&self) -> Option<&dyn Store> {
        self.store
    }

    /// The handler's own scratch bag — enrichment produced *during* this
    /// operation (policy results, logs, timings), keyed by type like
    /// [`Extensions`]. The domain merges it back into the request
    /// [`Context`] once the handler returns; see
    /// [`complete`](HandlerContext::complete).
    pub fn scratch(&self) -> &Extensions {
        &self.scratch
    }

    /// Mutably borrow the [`scratch`](HandlerContext::scratch) bag to record
    /// enrichment.
    pub fn scratch_mut(&mut self) -> &mut Extensions {
        &mut self.scratch
    }

    /// Stash a value in the [`scratch`](HandlerContext::scratch) bag, keyed by
    /// its type. Returns the previous value of the same type, if any.
    pub fn insert<T: Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.scratch.insert(value)
    }

    /// Borrow a value of type `T` from the [`scratch`](HandlerContext::scratch)
    /// bag, if present.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.scratch.get::<T>()
    }

    /// Mark the operation done by **consuming** the context, returning its
    /// scratch bag for the domain to merge into the request [`Context`] via
    /// [`Context::merge_scratch`](Context::merge_scratch). Called by the
    /// [`Domain`](crate::Domain) once the handler has produced its result.
    ///
    /// Because this takes `self`, a spent context cannot be reused — there is
    /// nothing left to call. Handlers only ever borrow the context (`&mut`), so
    /// they can neither complete it early nor hold it past their run; the
    /// exclusive right to finish it stays with the domain, enforced by the
    /// compiler rather than a runtime flag.
    pub fn complete(self) -> Extensions {
        self.scratch
    }

    /// Serialize the context's representable state to a JSON string — the form a
    /// handler returns *alongside* its result. The context is **not** fed back
    /// into the domain; this snapshot is the only thing that travels out.
    ///
    /// This is a **basic** implementation: it captures the fields that have a
    /// natural serial form — the [`id`](HandlerContext::id), `actor`, `tenant`,
    /// and `meta`. The typed [`scratch`](HandlerContext::scratch) bag holds
    /// arbitrary `dyn Any` values that cannot be serialized generically, so it is
    /// represented only by its entry **count** (`scratch_len`). A handler that
    /// wants specific scratch values in the output should serialize them itself,
    /// or a future revision can let entries opt into serialization.
    ///
    /// ```json
    /// {
    ///   "id": "ctx-… ",
    ///   "actor": { … } | null,
    ///   "tenant": <value> | null,
    ///   "meta": { … },
    ///   "scratch_len": 2
    /// }
    /// ```
    pub fn serialize(&self) -> String {
        // Build a serde_json::Value by hand from the parts, reusing the existing
        // Serialize impls on Record/Value. `serde_json::json!` keeps this basic
        // and dependency-free beyond serde_json (already a dependency).
        let snapshot = serde_json::json!({
            "id": self.id.as_str(),
            "actor": self.actor,
            "tenant": self.tenant,
            "meta": self.meta,
            "scratch_len": self.scratch.len(),
        });
        serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct RequestId(String);

    #[derive(Debug, PartialEq)]
    enum PolicyDecision {
        Allow,
        Deny,
    }

    #[test]
    fn extensions_insert_get_by_type() {
        let mut ext = Extensions::new();
        assert!(ext.is_empty());

        assert!(ext.insert(RequestId("r-1".into())).is_none());
        assert!(ext.insert(PolicyDecision::Allow).is_none());

        assert_eq!(ext.len(), 2);
        assert_eq!(ext.get::<RequestId>(), Some(&RequestId("r-1".into())));
        assert_eq!(ext.get::<PolicyDecision>(), Some(&PolicyDecision::Allow));
        assert!(ext.get::<u64>().is_none());
    }

    #[test]
    fn extensions_insert_replaces_and_returns_prior() {
        let mut ext = Extensions::new();
        ext.insert(RequestId("first".into()));
        let prev = ext.insert(RequestId("second".into()));
        assert_eq!(prev, Some(RequestId("first".into())));
        assert_eq!(ext.get::<RequestId>(), Some(&RequestId("second".into())));
        assert_eq!(ext.len(), 1);
    }

    #[test]
    fn extensions_get_mut_and_remove() {
        let mut ext = Extensions::new();
        ext.insert(PolicyDecision::Allow);
        *ext.get_mut::<PolicyDecision>().unwrap() = PolicyDecision::Deny;
        assert_eq!(ext.get::<PolicyDecision>(), Some(&PolicyDecision::Deny));

        assert_eq!(ext.remove::<PolicyDecision>(), Some(PolicyDecision::Deny));
        assert!(!ext.contains::<PolicyDecision>());
        assert!(ext.is_empty());
    }

    #[test]
    fn context_carries_extensions_and_meta() {
        let mut ctx = Context::new(());

        ctx.insert(RequestId("req-42".into()));
        ctx.set_meta("trace_id", "trace-abc");
        ctx.set_meta("attempt", 3i64);

        assert_eq!(ctx.get::<RequestId>(), Some(&RequestId("req-42".into())));
        assert_eq!(
            ctx.get_meta("trace_id").and_then(Value::as_str),
            Some("trace-abc")
        );
        assert_eq!(ctx.get_meta("attempt").and_then(Value::as_int), Some(3));
        assert!(ctx.get_meta("absent").is_none());
    }

    #[test]
    fn from_ref_projects_parts_of_a_composite_backend() {
        use crate::datalayer::memory::InMemoryDataLayer;

        #[derive(Clone)]
        struct Cache(&'static str);
        #[derive(Clone)]
        struct Metrics(u64);

        // A composite backend: a data layer *and* two unrelated utilities.
        #[derive(Clone)]
        struct App {
            db: Arc<InMemoryDataLayer>,
            cache: Cache,
            metrics: Metrics,
        }

        // It is a `Store` (CRUD reaches the db field)...
        impl Store for App {
            fn layer(&self) -> &dyn DataLayer {
                &*self.db
            }
        }
        // ...and its utilities project out by type.
        impl FromRef<App> for Cache {
            fn from_ref(a: &App) -> Self {
                a.cache.clone()
            }
        }
        impl FromRef<App> for Metrics {
            fn from_ref(a: &App) -> Self {
                a.metrics.clone()
            }
        }

        let ctx = Context::new(App {
            db: Arc::new(InMemoryDataLayer::new()),
            cache: Cache("redis"),
            metrics: Metrics(7),
        });

        // Both the CRUD store and the side utilities come out of one backend.
        assert_eq!(ctx.extract::<Cache>().0, "redis");
        assert_eq!(ctx.extract::<Metrics>().0, 7);
        // The whole backend is still a Store — CRUD reaches the db field.
        fn assert_store<B: Store>(_: &Context<B>) {}
        assert_store(&ctx);
    }

    #[test]
    fn extract_reflexive_clones_whole_backend() {
        #[derive(Clone, Debug, PartialEq)]
        struct Solo(u8);
        let ctx = Context::new(Solo(9));
        // No FromRef impl needed: the reflexive blanket clones the backend.
        assert_eq!(ctx.extract::<Solo>(), Solo(9));
    }

    #[test]
    fn context_and_extensions_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Extensions>();
        assert_send_sync::<Context<()>>();
    }
}
