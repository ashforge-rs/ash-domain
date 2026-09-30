//! Actions and the composable pipeline that runs them.
//!
//! Every operation on a resource is an [`ActionDef`] of one [`ActionKind`],
//! built from a declarative pipeline of [`Preparation`]s (for reads) and
//! [`Change`]s (for writes), plus an optional [`GenericHandler`] for custom
//! operations. The object threaded through a write is a [`Changeset`]; the
//! executor lives in [`crate::domain`].
//!
//! Note the boundary on validation: `ash-domain` runs **no** built-in attribute
//! validation. Every rule — presence, ranges, formats, cross-field invariants —
//! is the consumer's concern, expressed as a [`Validation`] registered on the
//! write action that returns `Err`, not a framework DSL. See the crate docs for
//! the rationale.

use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;

use crate::context::Store;
use crate::error::Result;
use crate::query::Query;
use crate::value::{FromRecord, IntoRecord, Record, Value};

/// The kind of an action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActionKind {
    /// Read records matching a query.
    Read,
    /// Write to storage: insert, update, or delete a record. Which storage
    /// operation runs is decided by the [`Domain`](crate::Domain) method the
    /// caller invokes, not by the action itself.
    Write,
    /// A custom, named operation not tied to storage.
    Generic,
}

/// A declared action on a resource.
#[derive(Clone)]
pub struct ActionDef {
    /// The action name (unique per resource).
    pub name: String,
    /// Its kind.
    pub kind: ActionKind,
    /// Preparations applied to the query (read actions).
    pub preparations: Vec<Arc<dyn Preparation>>,
    /// Changes applied to the changeset (write actions).
    pub changes: Vec<Arc<dyn Change>>,
    /// Validations run against the staged changeset (write actions). Each may
    /// reject the write by returning `Err`; none mutate it. The core ships no
    /// built-in attribute validation — presence, ranges, and formats all live
    /// here, as consumer-registered [`Validation`]s.
    pub validations: Vec<Arc<dyn Validation>>,
    /// The body of a [`ActionKind::Generic`] action.
    pub handler: Option<Arc<dyn GenericHandler>>,
}

impl ActionDef {
    /// A bare action of the given kind and name.
    pub fn new(name: impl Into<String>, kind: ActionKind) -> Self {
        Self {
            name: name.into(),
            kind,
            preparations: Vec::new(),
            changes: Vec::new(),
            validations: Vec::new(),
            handler: None,
        }
    }

    /// A `read` action.
    pub fn read(name: impl Into<String>) -> Self {
        Self::new(name, ActionKind::Read)
    }
    /// A `write` action. Whether it inserts, updates, or deletes is decided by
    /// the [`Domain`](crate::Domain) method the caller invokes against it.
    pub fn write(name: impl Into<String>) -> Self {
        Self::new(name, ActionKind::Write)
    }
    /// A `generic` action with a handler.
    pub fn generic(name: impl Into<String>, handler: Arc<dyn GenericHandler>) -> Self {
        let mut action = Self::new(name, ActionKind::Generic);
        action.handler = Some(handler);
        action
    }

    /// A `generic` action whose body is a **plain async closure** instead of a
    /// hand-written [`GenericHandler`] — the "very plain code" form.
    ///
    /// The closure takes the [`Changeset`] by value and the optional
    /// [`Store`] by reference, and returns the action's [`Value`].
    ///
    /// ```
    /// use ash_domain::action::ActionDef;
    /// use ash_domain::Value;
    ///
    /// let preview = ActionDef::generic_lambda("shout", |cs, _store| async move {
    ///     let msg = cs.params.get("msg").and_then(Value::as_str).unwrap_or("");
    ///     Ok(Value::from(msg.to_uppercase()))
    /// });
    /// # let _ = preview;
    /// ```
    ///
    /// **Store limitation.** Because the `store` is *borrowed*, a closure cannot
    /// hold it across an `.await` — so a generic that reads or writes storage
    /// (which awaits the [`DataLayer`](crate::DataLayer)) must be a hand-written
    /// [`GenericHandler`] impl instead. `generic_lambda` is for pure computation
    /// over the input; `change_lambda` and `prepare_lambda` have no such limit.
    pub fn generic_lambda<F, Fut>(name: impl Into<String>, lambda: F) -> Self
    where
        F: Fn(Changeset, Option<&dyn Store>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        Self::generic(name, Arc::new(GenericLambda(lambda)))
    }

    /// Attach a [`Change`] expressed as a **plain async closure** to this write
    /// action, returning the action for chaining. The closure takes the
    /// [`Changeset`] by value and returns it (mutated) — or `Err` to abort.
    ///
    /// ```
    /// use ash_domain::action::ActionDef;
    /// use ash_domain::Value;
    ///
    /// let create = ActionDef::write("create").change_lambda(|mut cs| async move {
    ///     if let Some(title) = cs.attribute("title").and_then(Value::as_str) {
    ///         cs.set_attribute("slug", title.to_lowercase().replace(' ', "-"));
    ///     }
    ///     Ok(cs)
    /// });
    /// # let _ = create;
    /// ```
    #[must_use]
    pub fn change_lambda<F, Fut>(mut self, lambda: F) -> Self
    where
        F: Fn(Changeset) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Changeset>> + Send + 'static,
    {
        self.changes.push(Arc::new(ChangeLambda(lambda)));
        self
    }

    /// Attach a [`Validation`] expressed as a **plain async closure** to this
    /// write action, returning the action for chaining. The closure receives the
    /// staged [`Changeset`] by value and returns `Ok(())` to admit or
    /// `Err(Error::invalid[_field])` to reject — it does **not** mutate the write
    /// (use [`change_lambda`](ActionDef::change_lambda) for that).
    ///
    /// ```
    /// use ash_domain::action::ActionDef;
    /// use ash_domain::{Error, Value};
    ///
    /// let create = ActionDef::write("create").validate_lambda(|cs| async move {
    ///     match cs.attribute("title").and_then(Value::as_str) {
    ///         Some(t) if !t.trim().is_empty() => Ok(()),
    ///         _ => Err(Error::invalid_field("title", "title must not be blank")),
    ///     }
    /// });
    /// # let _ = create;
    /// ```
    ///
    /// The closure takes `Changeset` **by value** (a per-write clone) so it can
    /// hold the input across an `.await` without lifetime gymnastics, mirroring
    /// [`change_lambda`](ActionDef::change_lambda). A validation that must avoid
    /// the clone implements [`Validation`] directly — its method borrows
    /// `&Changeset`.
    #[must_use]
    pub fn validate_lambda<F, Fut>(mut self, lambda: F) -> Self
    where
        F: Fn(Changeset) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.validations.push(Arc::new(ValidationLambda(lambda)));
        self
    }

    /// Attach a [`Preparation`] expressed as a **plain async closure** to this
    /// read action, returning the action for chaining. The closure takes the
    /// [`Query`] by value and returns it (scoped) — or `Err` to abort.
    ///
    /// ```
    /// use ash_domain::action::ActionDef;
    ///
    /// let read = ActionDef::read("read").prepare_lambda(|mut q| async move {
    ///     // Bind a param the data layer understands into the query bag.
    ///     q.params.insert("done", true);
    ///     Ok(q)
    /// });
    /// # let _ = read;
    /// ```
    #[must_use]
    pub fn prepare_lambda<F, Fut>(mut self, lambda: F) -> Self
    where
        F: Fn(Query) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Query>> + Send + 'static,
    {
        self.preparations.push(Arc::new(PreparationLambda(lambda)));
        self
    }

    /// Attach a hand-written [`Change`] (as a shared trait object) to this write
    /// action, returning it for chaining — the trait-object sibling of
    /// [`change_lambda`](ActionDef::change_lambda). Use this when the change needs
    /// its own state or must hold the store across an `.await` (which a closure
    /// cannot); reach for `change_lambda` for inline computation.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use async_trait::async_trait;
    /// # use ash_domain::action::{ActionDef, Change, Changeset};
    /// # use ash_domain::Result;
    /// struct Slugify;
    /// #[async_trait]
    /// impl Change for Slugify {
    ///     async fn change(&self, cs: &mut Changeset) -> Result<()> { let _ = cs; Ok(()) }
    /// }
    ///
    /// let create = ActionDef::write("create").change(Arc::new(Slugify));
    /// # let _ = create;
    /// ```
    #[must_use]
    pub fn change(mut self, change: Arc<dyn Change>) -> Self {
        self.changes.push(change);
        self
    }

    /// Attach a hand-written [`Validation`] (as a shared trait object) to this
    /// write action, returning it for chaining — the trait-object sibling of
    /// [`validate_lambda`](ActionDef::validate_lambda). Use this when the rule
    /// carries its own state (a compiled regex, a threshold, a client); reach for
    /// `validate_lambda` for an inline check.
    #[must_use]
    pub fn validate(mut self, validation: Arc<dyn Validation>) -> Self {
        self.validations.push(validation);
        self
    }

    /// Attach a hand-written [`Preparation`] (as a shared trait object) to this
    /// read action, returning it for chaining — the trait-object sibling of
    /// [`prepare_lambda`](ActionDef::prepare_lambda).
    #[must_use]
    pub fn prepare(mut self, preparation: Arc<dyn Preparation>) -> Self {
        self.preparations.push(preparation);
        self
    }
}

/// The mutable state threaded through a write action's pipeline.
///
/// `Clone` so a read-only [`Validation`] closure can take it by value (see
/// [`ActionDef::validate_lambda`]); every field is itself cheaply cloneable
/// (`String`, [`Record`], `Option<Value>`).
#[derive(Clone)]
pub struct Changeset {
    /// The resource being acted on.
    pub resource: String,
    /// The action name.
    pub action: String,
    /// The action kind.
    pub kind: ActionKind,
    /// The raw input parameters.
    pub params: Record,
    /// The accumulated attribute changes to persist.
    pub data: Record,
    /// The current persisted record (for update/delete writes).
    pub original: Option<Record>,
    /// The actor performing the action, for authorization.
    pub actor: Option<Record>,
    /// The tenant this action is scoped to, if the resource is tenant-scoped.
    pub tenant: Option<Value>,
}

impl Changeset {
    /// Create a changeset for an action.
    pub fn new(
        resource: impl Into<String>,
        action: impl Into<String>,
        kind: ActionKind,
        params: Record,
    ) -> Self {
        Self {
            resource: resource.into(),
            action: action.into(),
            kind,
            params,
            data: Record::new(),
            original: None,
            actor: None,
            tenant: None,
        }
    }

    /// A cheap empty changeset carrying only this one's `kind`, used as the
    /// momentary stand-in when a lambda adapter moves the real changeset out and
    /// back. Never observed by user code.
    fn placeholder(&self) -> Changeset {
        Changeset::new("", "", self.kind, Record::new())
    }

    /// The effective value of an attribute: a pending change, else the input
    /// param, else the original persisted value.
    pub fn attribute(&self, name: &str) -> Option<&Value> {
        self.data
            .get(name)
            .or_else(|| self.params.get(name))
            .or_else(|| self.original.as_ref().and_then(|o| o.get(name)))
    }

    /// Stage a change to an attribute.
    pub fn set_attribute(&mut self, name: impl Into<String>, value: impl Into<Value>) {
        self.data.insert(name, value);
    }
}

/// The result of a successfully executed action.
#[derive(Clone, Debug)]
pub enum ActionResult {
    /// A single record (insert/update write, read-one).
    Record(Record),
    /// Multiple records (read).
    Records(Vec<Record>),
    /// A generic action's return value.
    Value(Value),
    /// No result (delete write).
    None,
}

/// The input to [`Domain::handle_action`](crate::Domain::handle_action) — the
/// unified entry point for running any action by name.
///
/// A single call site can drive a create, read, update, destroy, or generic
/// action; the *shape* of the input, together with the action's declared
/// [`ActionKind`], selects which one runs. There is no separate `create` vs
/// `update` action kind ([`Write`](ActionKind::Write) covers both), so the
/// presence of an `id` is what distinguishes them:
///
/// | Action        | `ActionInput`                              |
/// |---------------|--------------------------------------------|
/// | create        | [`Params`](ActionInput::Params) with `id: None`     |
/// | update        | [`Params`](ActionInput::Params) with `id: Some(_)`  |
/// | destroy       | [`Empty`](ActionInput::Empty) with `id: Some(_)`    |
/// | read          | [`Query`](ActionInput::Query)                       |
/// | generic       | [`Params`](ActionInput::Params)/[`Empty`](ActionInput::Empty), `id: None` |
///
/// Use the constructors ([`create`](ActionInput::create),
/// [`update`](ActionInput::update), [`read`](ActionInput::read),
/// [`destroy`](ActionInput::destroy), [`generic`](ActionInput::generic)) rather
/// than the variants directly — they name the intent and fill `id` correctly.
#[derive(Clone, Debug)]
pub enum ActionInput {
    /// Parameters for a write (create/update) or a generic action, with an
    /// optional target `id`. `Some(id)` on a [`Write`](ActionKind::Write) action
    /// updates that record; `None` creates one.
    Params {
        /// The record identifying the target of an update, if any.
        id: Option<Value>,
        /// The staged input parameters.
        params: Record,
    },
    /// Parameters for a **batch create**: one param bag per row, no target id.
    ///
    /// Every row runs the *whole* per-row pipeline — staging, authorization,
    /// the field-level write gate, `before_action`, validations — and nothing is
    /// persisted until every row has passed. Only the persist itself is batched
    /// (see [`DataLayer::create_many`](crate::DataLayer::create_many)).
    Batch {
        /// The staged input parameters, one per row.
        rows: Vec<Record>,
    },
    /// A read [`Query`](crate::query::Query). Boxed because a `Query` is much
    /// larger than the other variants; the box keeps `ActionInput` small.
    Query(Box<Query>),
    /// No parameters, with an optional target `id`. `Some(id)` on a
    /// [`Write`](ActionKind::Write) action destroys that record; `None` runs a
    /// param-less generic action.
    Empty {
        /// The record to destroy, if this drives a destroy.
        id: Option<Value>,
    },
}

impl ActionInput {
    /// Input for a **create**: params, no target id.
    ///
    /// Fallible: [`IntoRecord`] now returns a [`Result`] so an out-of-`i64`-range
    /// integer field fails loudly rather than silently truncating (see
    /// [`IntoRecord`]). For a params source that cannot fail — a [`Record`] built
    /// by hand — the `Err` arm is unreachable.
    pub fn create(params: impl IntoRecord) -> Result<Self> {
        Ok(Self::Params {
            id: None,
            params: params.into_record()?,
        })
    }

    /// Input for a **create** from a plain [`Record`] — the infallible sibling
    /// of [`create`](ActionInput::create). A `Record` is already the neutral
    /// form, so there is no conversion to fail and no `?`/`unwrap()` at the
    /// call site.
    pub fn create_record(params: Record) -> Self {
        Self::Params { id: None, params }
    }

    /// Input for a **batch create**: one param bag per row.
    ///
    /// Fallible for the same reason as [`create`](ActionInput::create) — a row
    /// whose conversion fails takes the whole batch with it, before any row is
    /// staged, let alone persisted.
    ///
    /// The batch is bounded: a domain refuses more rows than
    /// [`DomainConfig::max_batch`](crate::DomainConfig::max_batch) allows.
    pub fn create_many<I, T>(rows: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: IntoRecord,
    {
        Ok(Self::Batch {
            rows: rows
                .into_iter()
                .map(IntoRecord::into_record)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    /// Input for a **batch create** from plain [`Record`]s — the infallible
    /// sibling of [`create_many`](ActionInput::create_many).
    pub fn create_many_records(rows: Vec<Record>) -> Self {
        Self::Batch { rows }
    }

    /// Input for an **update**: params applied to the record `id`. Fallible for
    /// the same reason as [`create`](ActionInput::create).
    pub fn update(id: impl Into<Value>, params: impl IntoRecord) -> Result<Self> {
        Ok(Self::Params {
            id: Some(id.into()),
            params: params.into_record()?,
        })
    }

    /// Input for an **update** from a plain [`Record`] — the infallible sibling
    /// of [`update`](ActionInput::update), for the same reason as
    /// [`create_record`](ActionInput::create_record).
    pub fn update_record(id: impl Into<Value>, params: Record) -> Self {
        Self::Params {
            id: Some(id.into()),
            params,
        }
    }

    /// Input for a **read**: the query to run. Prefer passing a
    /// [`Filter`](crate::Filter) — it names no resource (the call site's type
    /// parameter does) and cannot carry the `load`/`aggregates`/`computed`
    /// requests this path rejects; a full [`Query`] is also accepted.
    pub fn read(query: impl Into<Query>) -> Self {
        Self::Query(Box::new(query.into()))
    }

    /// Input for a **destroy**: the record `id` to remove.
    pub fn destroy(id: impl Into<Value>) -> Self {
        Self::Empty {
            id: Some(id.into()),
        }
    }

    /// Input for a **generic** action: its params (use [`Self::create`]'s empty
    /// form, or pass an empty record for a param-less handler). Fallible for the
    /// same reason as [`create`](ActionInput::create).
    pub fn generic(input: impl IntoRecord) -> Result<Self> {
        Ok(Self::Params {
            id: None,
            params: input.into_record()?,
        })
    }

    /// Input for a **generic** action from a plain [`Record`] — the infallible
    /// sibling of [`generic`](ActionInput::generic).
    pub fn generic_record(params: Record) -> Self {
        Self::Params { id: None, params }
    }
}

/// The result of [`Domain::handle_action`](crate::Domain::handle_action).
///
/// One enum spans every action shape: a write yields a [`Record`](ActionOutcome::Record),
/// a read yields [`Records`](ActionOutcome::Records), a generic action a
/// [`Value`](ActionOutcome::Value), and a destroy [`Unit`](ActionOutcome::Unit).
/// Because a single enum can't carry a per-call generic `R::Data`, the outcome
/// holds raw [`Record`]s; call [`into_data`](ActionOutcome::into_data) /
/// [`into_data_vec`](ActionOutcome::into_data_vec) to project them into a
/// resource's typed [`Data`](crate::Resource::Data) when you want typing.
#[derive(Clone, Debug)]
pub enum ActionOutcome {
    /// A single record — the created or updated row.
    Record(Record),
    /// Multiple records — a read's rows.
    Records(Vec<Record>),
    /// A generic action's return value.
    Value(Value),
    /// No result — a destroy.
    Unit,
}

impl ActionOutcome {
    /// The single [`Record`] of a create/update outcome, or an error if this
    /// outcome is not a [`Record`](ActionOutcome::Record).
    pub fn into_record(self) -> Result<Record> {
        match self {
            Self::Record(r) => Ok(r),
            other => Err(crate::error::Error::invalid(format!(
                "expected a single-record outcome, got {other:?}"
            ))),
        }
    }

    /// The [`Record`]s of a read outcome, or an error if this outcome is not
    /// [`Records`](ActionOutcome::Records).
    pub fn into_records(self) -> Result<Vec<Record>> {
        match self {
            Self::Records(rs) => Ok(rs),
            other => Err(crate::error::Error::invalid(format!(
                "expected a multi-record outcome, got {other:?}"
            ))),
        }
    }

    /// The [`Value`] of a generic outcome, or an error if this outcome is not
    /// a [`Value`](ActionOutcome::Value).
    pub fn into_value(self) -> Result<Value> {
        match self {
            Self::Value(v) => Ok(v),
            other => Err(crate::error::Error::invalid(format!(
                "expected a value outcome, got {other:?}"
            ))),
        }
    }

    /// Project a single-record outcome into a resource's typed
    /// [`Data`](crate::Resource::Data).
    pub fn into_data<D: FromRecord>(self) -> Result<D> {
        D::from_record(&self.into_record()?)
    }

    /// Project a multi-record outcome into a `Vec` of a resource's typed
    /// [`Data`](crate::Resource::Data).
    pub fn into_data_vec<D: FromRecord>(self) -> Result<Vec<D>> {
        self.into_records()?.iter().map(D::from_record).collect()
    }
}

/// Adjusts a read query before execution (e.g. scoping to the actor).
///
/// A consumer supplies one by implementing the trait on their own type:
///
/// ```
/// use ash_domain::action::Preparation;
/// use ash_domain::Query;
/// use ash_domain::Result;
///
/// struct OnlyDone;
///
/// #[async_trait::async_trait]
/// impl Preparation for OnlyDone {
///     async fn prepare(&self, query: &mut Query) -> Result<()> {
///         // Bind a param the data layer understands into the query bag.
///         query.params.insert("done", true);
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait Preparation: Send + Sync {
    /// Mutate the query in place.
    async fn prepare(&self, query: &mut Query) -> Result<()>;
}

/// Transforms a changeset: sets attributes, derives values, or rejects the
/// action by returning `Err`.
///
/// This is the single write-pipeline seam. Deriving a value and rejecting bad
/// input are the same shape — implement the trait on your own type:
///
/// ```
/// use ash_domain::action::{Change, Changeset};
/// use ash_domain::{Error, Result, Value};
///
/// struct Slugify;
///
/// #[async_trait::async_trait]
/// impl Change for Slugify {
///     async fn change(&self, cs: &mut Changeset) -> Result<()> {
///         match cs.attribute("title").and_then(Value::as_str) {
///             Some(title) if !title.trim().is_empty() => {
///                 cs.set_attribute("slug", title.trim().to_lowercase().replace(' ', "-"));
///                 Ok(())
///             }
///             // A change may also guard input by returning an error.
///             _ => Err(Error::invalid("title must not be blank")),
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Change: Send + Sync {
    /// Mutate the changeset in place; return `Err` to abort the action.
    async fn change(&self, changeset: &mut Changeset) -> Result<()>;
}

/// Validates a staged changeset **without mutating it**: return `Err` to reject
/// the write.
///
/// This is the user-owned replacement for the built-in attribute constraints the
/// core no longer ships. Presence checks, numeric/length ranges, formats, and
/// cross-field rules all live here — as a `Validation` registered on the write
/// action, not as framework-declared attribute metadata. The read-only
/// `&Changeset` (vs. [`Change`]'s `&mut`) is the type encoding the contract: a
/// validation judges the write, it does not shape it.
///
/// Validations run **after** every [`Change`], **after**
/// [`before_action`](crate::Extension::before_action), and **after
/// authorization** — the same pipeline slot the removed constraint check held.
/// So a denied caller never learns whether their input was valid (fail-closed:
/// they see `Forbidden`, never `Invalid`).
///
/// ```
/// use ash_domain::action::{Changeset, Validation};
/// use ash_domain::{Error, Result, Value};
///
/// struct TitleNotBlank;
///
/// #[async_trait::async_trait]
/// impl Validation for TitleNotBlank {
///     async fn validate(&self, cs: &Changeset) -> Result<()> {
///         match cs.attribute("title").and_then(Value::as_str) {
///             Some(t) if !t.trim().is_empty() => Ok(()),
///             _ => Err(Error::invalid_field("title", "title must not be blank")),
///         }
///     }
/// }
/// ```
#[async_trait]
pub trait Validation: Send + Sync {
    /// Inspect the staged changeset; return `Err` (typically
    /// [`Error::invalid_field`](crate::Error::invalid_field)) to abort the write.
    async fn validate(&self, changeset: &Changeset) -> Result<()>;
}

/// The body of a [`ActionKind::Generic`] action.
///
/// A generic action is any operation that isn't plain CRUD. The handler gets the
/// [`Changeset`] (its input params and the actor) and an optional [`Store`]. When
/// run through [`Domain::handle_action`](crate::Domain::handle_action) the store is
/// always `Some` — a generic action receives the context's persistence backend and
/// may read or write; the `Option` remains for hand-built handler contexts that
/// supply no store.
///
/// The store is reached through the abstract [`DataLayer`](crate::DataLayer), so
/// a handler reads and writes without caring whether that layer is in memory, a
/// local file, a database, or a socket to a remote store:
///
/// ```
/// use ash_domain::action::{Changeset, GenericHandler};
/// use ash_domain::{Query, Result, Store, Value};
///
/// /// Counts the records of a resource named by the `of` param.
/// struct CountAll;
///
/// #[async_trait::async_trait]
/// impl GenericHandler for CountAll {
///     async fn run(&self, cs: &mut Changeset, store: Option<&dyn Store>) -> Result<Value> {
///         let store = store.ok_or_else(|| ash_domain::Error::invalid("needs a store"))?;
///         let of = cs.params.get("of").and_then(Value::as_str).unwrap_or("");
///         let rows = store.layer().read(&Query::new(of)).await?;
///         Ok(Value::Int(rows.len() as i64))
///     }
/// }
/// ```
#[async_trait]
pub trait GenericHandler: Send + Sync {
    /// Run the custom operation, returning its result value. `store` is `Some`
    /// when invoked on a persistence-backed context, `None` otherwise.
    async fn run(&self, changeset: &mut Changeset, store: Option<&dyn Store>) -> Result<Value>;

    /// Run the custom operation with a derived [`HandlerContext`](crate::HandlerContext)
    /// — the richer, scoped view of the operation (domain handle, read-only
    /// actor/tenant/meta, and a scratch bag for enrichment).
    ///
    /// **Defaults to [`run`](GenericHandler::run)**, reaching the store through
    /// the context, so every existing handler keeps working unchanged. Override
    /// this instead of `run` when the handler needs the domain, the request
    /// metadata, or wants to record enrichment into
    /// [`scratch`](crate::HandlerContext::scratch_mut). The domain calls this
    /// method; `run` remains the delegate a plain handler implements.
    async fn run_ctx(
        &self,
        changeset: &mut Changeset,
        ctx: &mut crate::HandlerContext<'_>,
    ) -> Result<Value> {
        self.run(changeset, ctx.store()).await
    }
}

// ── lambda adapters ──────────────────────────────────────────────────────────
//
// Wrap a plain async closure as a pipeline step, so a one-off action needs no
// hand-written trait `impl`. The closures take their subject **by value** and
// (for `Change`/`Preparation`) return it — the owned form, because a borrowed
// `&mut` argument cannot have its lifetime tied to the returned future through a
// bare `Fn` bound. The adapter moves the subject out, runs the closure, and
// moves the result back, so from the caller's side it reads like an in-place
// edit.

/// Adapts a closure into a [`Preparation`]. Built by
/// [`ActionDef::prepare_lambda`].
struct PreparationLambda<F>(F);

#[async_trait]
impl<F, Fut> Preparation for PreparationLambda<F>
where
    F: Fn(Query) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Query>> + Send,
{
    async fn prepare(&self, query: &mut Query) -> Result<()> {
        let taken = std::mem::take(query);
        *query = (self.0)(taken).await?;
        Ok(())
    }
}

/// Adapts a closure into a [`Change`]. Built by [`ActionDef::change_lambda`].
struct ChangeLambda<F>(F);

#[async_trait]
impl<F, Fut> Change for ChangeLambda<F>
where
    F: Fn(Changeset) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Changeset>> + Send,
{
    async fn change(&self, changeset: &mut Changeset) -> Result<()> {
        let taken = std::mem::replace(changeset, changeset.placeholder());
        *changeset = (self.0)(taken).await?;
        Ok(())
    }
}

/// Adapts a closure into a [`Validation`]. Built by
/// [`ActionDef::validate_lambda`]. Validation is read-only, so — unlike
/// [`ChangeLambda`] — the adapter *clones* the changeset into the closure rather
/// than moving it out and back; the original is left untouched.
struct ValidationLambda<F>(F);

#[async_trait]
impl<F, Fut> Validation for ValidationLambda<F>
where
    F: Fn(Changeset) -> Fut + Send + Sync,
    Fut: Future<Output = Result<()>> + Send,
{
    async fn validate(&self, changeset: &Changeset) -> Result<()> {
        (self.0)(changeset.clone()).await
    }
}

/// Adapts a closure into a [`GenericHandler`]. Built by
/// [`ActionDef::generic_lambda`]. The store stays borrowed (it can't be owned —
/// it lives in the context); only the changeset is passed by value.
struct GenericLambda<F>(F);

#[async_trait]
impl<F, Fut> GenericHandler for GenericLambda<F>
where
    F: Fn(Changeset, Option<&dyn Store>) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Value>> + Send,
{
    async fn run(&self, changeset: &mut Changeset, store: Option<&dyn Store>) -> Result<Value> {
        let taken = std::mem::replace(changeset, changeset.placeholder());
        (self.0)(taken, store).await
    }
}
