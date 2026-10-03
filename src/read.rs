//! The typed read builder: [`Domain::read`] — **one door for every shape of
//! read**.
//!
//! A read varies along two independent axes:
//!
//! - **loads** — whether relationship loads / aggregates / computed fields are
//!   resolved alongside each row;
//! - **report** — whether the [`ReadReport`](crate::ReadReport) naming every
//!   policy-redacted attribute is returned with the rows.
//!
//! Neither axis touches the row bound: every shape runs under the domain's
//! [`max_rows`](crate::DomainConfig::max_rows) ceiling unless the request states
//! its own with [`limit`](ReadRequest::limit).
//!
//! [`ReadRequest`] tracks both axes in its *type*, so the requested combination
//! decides the output type — and an unresolvable request is unrepresentable
//! rather than silently dropped:
//!
//! | request                      | `.await` yields                          |
//! |------------------------------|------------------------------------------|
//! | plain                        | `Vec<R::Data>`                           |
//! | `.with_report()`             | `AuthorizedRead<R::Data>`                |
//! | `.load(…)`                   | `Vec<Loaded<R::Data>>`                   |
//! | `.load(…).with_report()`     | `AuthorizedRead<Loaded<R::Data>>`        |
//!
//! The one-row terminal [`get`](ReadRequest::get) exists on every shape; the
//! report-carrying shapes return an [`AuthorizedOne`] (the row, or `None`,
//! plus the report).
//!
//! Every combination runs the same authorized pipeline as the erased read path
//! (tenant scoping, preparations, **authorization first**, extensions,
//! redaction); the builder adds no new capability, it only makes each
//! combination reachable and honest. The resource is named once, by the type
//! parameter — there is no resource string to mismatch.

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;

use crate::context::{Context, Store};
use crate::domain::Domain;
use crate::error::{Error, Result};
use crate::policy::{AuthorizedOne, AuthorizedRead, Explanation};
use crate::query::{Query, in_param};
use crate::resource::{Loaded, Resource};
use crate::value::{FromRecord, IntoRecord, Record, Value};

/// Type-state: no relationship loads or derived values requested — rows come
/// back plain. Uninhabited; only ever a [`ReadRequest`] type parameter.
pub enum NoLoad {}
/// Type-state: relationship loads and/or derived values requested — rows come
/// back as [`Loaded`]. Uninhabited; only ever a [`ReadRequest`] type parameter.
pub enum WithLoad {}
/// Type-state: rows only, no redaction report. Uninhabited; only ever a
/// [`ReadRequest`] type parameter.
pub enum NoReport {}
/// Type-state: rows plus the [`ReadReport`](crate::ReadReport). Uninhabited;
/// only ever a [`ReadRequest`] type parameter.
pub enum WithReport {}

/// A typed read of `R`, under construction — see the [module docs](self) for
/// the output matrix. Build one with [`Domain::read`], refine it, then `.await`
/// it (it implements [`IntoFuture`]) or finish with [`get`](ReadRequest::get) /
/// [`explain`](ReadRequest::explain).
#[must_use = "a ReadRequest does nothing until awaited (or finished with `get`/`explain`)"]
pub struct ReadRequest<'a, R: Resource, B: Store, L = NoLoad, P = NoReport> {
    domain: &'a Domain,
    ctx: &'a Context<B>,
    action: Option<String>,
    params: Record,
    load: Vec<String>,
    limit: Option<u32>,
    sort: Vec<crate::query::SortKey>,
    after: Option<crate::query::Cursor>,
    offset: Option<u32>,
    aggregates: Vec<String>,
    computed: Vec<String>,
    /// A conversion failure from [`params`](ReadRequest::params), kept (first
    /// one wins) and surfaced when the request runs — builder chaining stays
    /// infallible, the error is never dropped.
    invalid: Option<Error>,
    _shape: PhantomData<Shape<R, L, P>>,
}

/// The variance/auto-trait carrier for [`ReadRequest`]'s phantom parameters: a
/// function pointer, so the request is `Send`/`Sync` regardless of `R`, `L`,
/// `P` (they are only ever *named*, never stored).
type Shape<R, L, P> = fn() -> (R, L, P);

impl Domain {
    /// Begin a **typed read** of `R` against `ctx` — the single, composable
    /// front door for reads.
    ///
    /// The request starts as "every row of `R`, under its conventional read
    /// action" and is refined by chaining; what you request decides what
    /// `.await` returns (see the [module docs](crate::read) for the matrix).
    /// The default action is the resource's first declared
    /// [`Read`](crate::ActionKind::Read) action (`"read"` if it declares none);
    /// pick another with [`action`](ReadRequest::action).
    ///
    /// Reads take `&Context`, so several requests can run concurrently over one
    /// context (e.g. under `futures::try_join!`, via
    /// [`into_future`](IntoFuture::into_future)).
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
    /// let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ///
    /// // Plain rows; params are the layer's convention, as on any read:
    /// let todos: Vec<Record> = domain.read::<Todo>(&ctx).filter("limit", 50).await?;
    ///
    /// // One row by primary key:
    /// let one: Option<Record> = domain.read::<Todo>(&ctx).get("some-id").await?;
    ///
    /// // Rows plus the redaction report:
    /// let auth = domain.read::<Todo>(&ctx).with_report().await?;
    /// let (rows, report) = (auth.rows, auth.report);
    /// # let _ = (todos, one, rows, report);
    /// # Ok(())
    /// # }
    /// ```
    pub fn read<'a, R: Resource>(
        &'a self,
        ctx: &'a Context<impl Store>,
    ) -> ReadRequest<'a, R, impl Store> {
        ReadRequest {
            domain: self,
            ctx,
            action: None,
            params: Record::new(),
            load: Vec::new(),
            aggregates: Vec::new(),
            computed: Vec::new(),
            limit: None,
            sort: Vec::new(),
            after: None,
            offset: None,
            invalid: None,
            _shape: PhantomData,
        }
    }
}

impl<'a, R: Resource, B: Store, L, P> ReadRequest<'a, R, B, L, P> {
    /// Run under the declared read action `name` instead of the resource's
    /// conventional (first-declared) read action. Policies and preparations are
    /// those of the named action; an unknown or non-read name fails when run.
    pub fn action(mut self, name: impl Into<String>) -> Self {
        self.action = Some(name.into());
        self
    }

    /// Set a param in the layer-interpreted bag — same contract as
    /// [`Query::param`]: what `key` means is the data layer's convention; the
    /// core attaches no semantics (a bound like `"limit"` is a layer convention
    /// too, not a core concept).
    pub fn filter(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(key, value);
        self
    }

    /// Merge a whole **typed param bag** into the layer-interpreted params —
    /// the compile-checked sibling of [`filter`](ReadRequest::filter). `params`
    /// is anything [`IntoRecord`] (typically a `#[derive(IntoRecord)]` struct
    /// you define), so your layer's param convention gets field names and types
    /// the compiler checks instead of strings. The core still attaches no
    /// semantics to any key — a `limit` field means whatever your data layer
    /// says it means, exactly as with [`filter`](ReadRequest::filter). A key
    /// set both ways keeps the later value.
    ///
    /// [`IntoRecord`] is fallible (an out-of-`i64`-range integer field has no
    /// lossless neutral form); a conversion failure is kept — first one wins —
    /// and returned when the request runs, so chaining stays uninterrupted and
    /// the error is never dropped.
    ///
    /// ```
    /// # use ash_domain::{Domain, Context, Resource, Attribute, ActionDef, Record, IntoRecord};
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # use std::sync::Arc;
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> {
    /// #         vec![Attribute::scalar::<String>("id"), Attribute::scalar::<bool>("done")]
    /// #     }
    /// #     fn actions() -> Vec<ActionDef> {
    /// #         vec![ActionDef::read("read")]
    /// #     }
    /// # }
    /// #[derive(IntoRecord)]
    /// struct OpenTodos {
    ///     done: bool,
    ///     limit: i64,
    /// }
    ///
    /// # async fn run() -> ash_domain::Result<()> {
    /// # let domain = Domain::builder().register::<Todo>().permissive().build();
    /// # let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    /// let open: Vec<Record> = domain
    ///     .read::<Todo>(&ctx)
    ///     .params(OpenTodos { done: false, limit: 10 })
    ///     .await?;
    /// # let _ = open;
    /// # Ok(())
    /// # }
    /// ```
    pub fn params(mut self, params: impl IntoRecord) -> Self {
        match params.into_record() {
            Ok(bag) => self.params.0.extend(bag.0),
            Err(err) => {
                self.invalid.get_or_insert(err);
            }
        }
        self
    }

    /// Bound this read to at most `limit` rows.
    ///
    /// A read that sets no bound of its own still runs under the domain's
    /// [`max_rows`](crate::DomainConfig::max_rows) ceiling — this is how a caller
    /// states a smaller, deliberate one (a page size), and the only way to read
    /// more rows than the ceiling allows without changing the domain.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use ash_domain::{Context, Domain, Record, Resource};
    /// # use ash_domain::action::ActionDef;
    /// # use ash_domain::attribute::Attribute;
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> { vec![Attribute::scalar::<String>("id")] }
    /// #     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
    /// # }
    /// # async fn run() -> ash_domain::Result<()> {
    /// # let domain = Domain::builder().register::<Todo>().permissive().build();
    /// # let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    /// let page: Vec<Record> = domain.read::<Todo>(&ctx).limit(25).await?;
    /// # let _ = page;
    /// # Ok(())
    /// # }
    /// ```
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sort this read by `attribute`, ascending — appending to any keys already
    /// set, so repeated calls build a compound order.
    ///
    /// A bounded read without a sort is an **arbitrary subset**, not a page: the
    /// layer may return any `limit` rows it likes, and two identical reads may
    /// disagree. Sorting is what makes [`limit`](ReadRequest::limit) mean
    /// "the first N", and what a [`page`](ReadRequest::page) cursor resumes
    /// against.
    pub fn sort_asc(mut self, attribute: impl Into<String>) -> Self {
        self.sort.push(crate::query::SortKey::asc(attribute));
        self
    }

    /// Sort this read by `attribute`, descending. See
    /// [`sort_asc`](ReadRequest::sort_asc).
    pub fn sort_desc(mut self, attribute: impl Into<String>) -> Self {
        self.sort.push(crate::query::SortKey::desc(attribute));
        self
    }

    /// Resume after `cursor`: the next page of the read that produced it.
    ///
    /// Pass the [`Cursor`](crate::query::Cursor) from a previous
    /// [`page`](ReadRequest::page) back verbatim, with the **same sort order**.
    /// A cursor on an unsorted read is refused — there is no order to resume.
    pub fn after(mut self, cursor: crate::query::Cursor) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Skip the first `offset` rows of the sorted result set — offset paging,
    /// for the caller who needs page *n* directly rather than the next page.
    ///
    /// Pairs with [`limit`](ReadRequest::limit) for the page size: page `n`
    /// (0-based) of a numbered pager is `.offset(n * size).limit(size)`, and
    /// [`offset_page`](ReadRequest::offset_page) runs it as one call. Requires
    /// a sort order, and is mutually exclusive with
    /// [`after`](ReadRequest::after); either mistake is refused when the read
    /// runs.
    ///
    /// Prefer a cursor when the caller only walks forward. An offset re-counts
    /// the result set on every page, so a row inserted or removed before the
    /// offset between two requests shifts every later row — the caller can see
    /// a row twice or miss it. That is the price of random access, and it is
    /// why this is not the default.
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Load the relationship path `path` (dotted for nesting, e.g.
    /// `"comments.author"`) alongside each row. Moves the request into the
    /// loaded shape: `.await` now yields [`Loaded`] rows. Repeatable. Loaded
    /// destinations are authorized and redacted like a direct read of them —
    /// see [`Domain::read_loaded`].
    pub fn load(mut self, path: impl Into<String>) -> ReadRequest<'a, R, B, WithLoad, P> {
        self.load.push(path.into());
        self.cast()
    }

    /// Resolve the declared aggregate `name` for each row (surfaced on
    /// [`Loaded::aggregates`]). Moves the request into the loaded shape.
    /// Repeatable. An unknown name fails when run.
    pub fn aggregate(mut self, name: impl Into<String>) -> ReadRequest<'a, R, B, WithLoad, P> {
        self.aggregates.push(name.into());
        self.cast()
    }

    /// Resolve the declared computed field `name` for each row (surfaced on
    /// [`Loaded::computed`]). Moves the request into the loaded shape.
    /// Repeatable. An unknown name fails when run.
    pub fn computed(mut self, name: impl Into<String>) -> ReadRequest<'a, R, B, WithLoad, P> {
        self.computed.push(name.into());
        self.cast()
    }

    /// Also return the [`ReadReport`](crate::ReadReport) naming every attribute
    /// a policy redacted (and why): `.await` now yields an [`AuthorizedRead`].
    /// Composes with [`load`](ReadRequest::load) — the report covers the
    /// top-level rows.
    pub fn with_report(self) -> ReadRequest<'a, R, B, L, WithReport> {
        self.cast()
    }

    /// **Dry-run** this read's authorization — "would it be allowed, and which
    /// policy decides?" — without touching the data layer. The terminal form of
    /// [`Domain::explain_read`].
    pub async fn explain(self) -> Result<Explanation> {
        let (domain, ctx, action, query) = self.parts()?;
        domain.explain_read::<R>(ctx, &action, query).await
    }

    /// Re-tag the type-state without touching the payload. The single place a
    /// shape transition happens; every field carries over unchanged.
    fn cast<L2, P2>(self) -> ReadRequest<'a, R, B, L2, P2> {
        ReadRequest {
            domain: self.domain,
            ctx: self.ctx,
            action: self.action,
            params: self.params,
            load: self.load,
            limit: self.limit,
            sort: self.sort,
            after: self.after,
            offset: self.offset,
            aggregates: self.aggregates,
            computed: self.computed,
            invalid: self.invalid,
            _shape: PhantomData,
        }
    }

    /// Resolve the request into what the pipeline takes: the action name (the
    /// caller's, or the resource's conventional read action) and the [`Query`].
    /// Surfaces a deferred [`params`](ReadRequest::params) conversion failure —
    /// every run path goes through here, so the error cannot be dropped.
    fn parts(self) -> Result<(&'a Domain, &'a Context<B>, String, Query)> {
        if let Some(err) = self.invalid {
            return Err(err);
        }
        let action = self
            .action
            .unwrap_or_else(|| self.domain.default_read_action(R::NAME));
        let mut query = Query::new(R::NAME);
        query.params = self.params;
        query.load = self.load;
        query.limit = self.limit;
        query.sort = self.sort;
        query.after = self.after;
        query.offset = self.offset;
        query.aggregate_names = self.aggregates;
        query.computed = self.computed;
        Ok((self.domain, self.ctx, action, query))
    }
}

// ── `get` terminals ──────────────────────────────────────────────────────────

impl<'a, R: Resource, B: Store> ReadRequest<'a, R, B, NoLoad, NoReport> {
    /// Fetch one row by primary key, or `None` — a read like any other
    /// (authorized, redacted), issued as the reserved key-set query on the
    /// primary key. Any params already set on the request still apply.
    pub async fn get(mut self, id: impl Into<Value>) -> Result<Option<R::Data>> {
        self.params.insert(
            crate::query::reserved::IN,
            in_param(R::primary_key(), vec![id.into()]),
        );
        let (domain, ctx, action, query) = self.parts()?;
        let (records, _, _) = domain.read_records::<R>(ctx, &action, query).await?;
        records.first().map(R::Data::from_record).transpose()
    }
}

impl<'a, R: Resource, B: Store> ReadRequest<'a, R, B, NoLoad, NoReport> {
    /// Run the read as one **page**: the rows, plus the
    /// [`Cursor`](crate::query::Cursor) that resumes after the last of them.
    ///
    /// Requires a sort order — a page of an unordered read is not a page — and
    /// pairs with [`limit`](ReadRequest::limit) for the page size.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use ash_domain::{Context, Domain, Record, Resource};
    /// # use ash_domain::action::ActionDef;
    /// # use ash_domain::attribute::Attribute;
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> {
    /// #         vec![Attribute::scalar::<String>("id")]
    /// #     }
    /// #     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
    /// # }
    /// # async fn run() -> ash_domain::Result<()> {
    /// # let domain = Domain::builder().register::<Todo>().permissive().build();
    /// # let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    /// let first = domain.read::<Todo>(&ctx).sort_asc("id").limit(25).page().await?;
    ///
    /// if let Some(cursor) = first.cursor() {
    ///     let next = domain
    ///         .read::<Todo>(&ctx)
    ///         .sort_asc("id")
    ///         .after(cursor.clone())
    ///         .limit(25)
    ///         .page()
    ///         .await?;
    ///     # let _ = next;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn page(self) -> Result<Page<R::Data>> {
        let sort = self.sort.clone();
        let (domain, ctx, action, query) = self.parts()?;
        let limit = query.limit;
        let (records, _, _) = domain.read_records::<R>(ctx, &action, query).await?;
        // The cursor is built from the *raw* rows, before conversion: a sort key
        // the caller may not read is still a valid position, and redaction must
        // not silently move where the next page starts.
        let cursor = Page::<R::Data>::cursor_for(&records, &sort, limit);
        let rows = records
            .iter()
            .map(R::Data::from_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page { rows, cursor })
    }

    /// Run the read as one **offset page**: the rows at
    /// [`offset`](ReadRequest::offset), plus whether a further page exists.
    ///
    /// The offset-paging counterpart of [`page`](ReadRequest::page). It requires
    /// both a sort order and a [`limit`](ReadRequest::limit) — an offset page
    /// with no page size is the whole tail of the result set, not a page — and
    /// it answers [`has_more`](OffsetPage::has_more) without a second round trip
    /// by asking the layer for **one row more** than the page and returning only
    /// the page.
    ///
    /// Prefer [`page`](ReadRequest::page) for a caller that only walks forward;
    /// see [`offset`](ReadRequest::offset) for what an offset gives up.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use ash_domain::{Context, Domain, Record, Resource};
    /// # use ash_domain::action::ActionDef;
    /// # use ash_domain::attribute::Attribute;
    /// # use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// # struct Todo;
    /// # impl Resource for Todo {
    /// #     const NAME: &'static str = "todo";
    /// #     type Data = Record;
    /// #     fn attributes() -> Vec<Attribute> {
    /// #         vec![Attribute::scalar::<String>("id")]
    /// #     }
    /// #     fn actions() -> Vec<ActionDef> { vec![ActionDef::read("read")] }
    /// # }
    /// # async fn run() -> ash_domain::Result<()> {
    /// # let domain = Domain::builder().register::<Todo>().permissive().build();
    /// # let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    /// // The third page of 25, straight to it.
    /// let third = domain
    ///     .read::<Todo>(&ctx)
    ///     .sort_asc("id")
    ///     .offset(50)
    ///     .limit(25)
    ///     .offset_page()
    ///     .await?;
    ///
    /// if third.has_more() {
    ///     # let _ =
    ///     domain.read::<Todo>(&ctx).sort_asc("id").offset(75).limit(25).offset_page().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn offset_page(mut self) -> Result<OffsetPage<R::Data>> {
        let offset = self.offset.unwrap_or(0);
        let Some(size) = self.limit else {
            return Err(Error::invalid(format!(
                "an offset page of `{}` needs a page size: set `limit` (an offset with \
                 no bound is the whole tail of the result set, not a page)",
                R::NAME
            )));
        };
        // Probe one row past the page so `has_more` costs no extra round trip.
        // `size` is a u32 and the probe is computed in u64, so a page size of
        // u32::MAX widens rather than wrapping; the layer's own bound is what
        // caps the read in that case.
        let probe = u64::from(size).saturating_add(1);
        self.limit = Some(u32::try_from(probe).unwrap_or(u32::MAX));

        let (domain, ctx, action, query) = self.parts()?;
        let (mut records, _, _) = domain.read_records::<R>(ctx, &action, query).await?;

        // A probe row came back ⇒ the result set continues past this page. Drop
        // it: the caller asked for `size` rows and must not receive `size + 1`.
        let has_more = records.len() as u64 > u64::from(size);
        records.truncate(size as usize);
        debug_assert!(
            records.len() as u64 <= u64::from(size),
            "an offset page never returns more than its page size"
        );

        let rows = records
            .iter()
            .map(R::Data::from_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(OffsetPage {
            rows,
            offset,
            has_more,
        })
    }
}

impl<'a, R: Resource, B: Store> ReadRequest<'a, R, B, WithLoad, NoReport> {
    /// Fetch one row by primary key with its requested loads resolved, or
    /// `None` — the loaded form of [`get`](ReadRequest::get).
    pub async fn get(mut self, id: impl Into<Value>) -> Result<Option<Loaded<R::Data>>> {
        self.params.insert(
            crate::query::reserved::IN,
            in_param(R::primary_key(), vec![id.into()]),
        );
        let (domain, ctx, action, query) = self.parts()?;
        let (rows, _) = domain
            .read_loaded_with_report::<R>(ctx, &action, query)
            .await?;
        Ok(rows.into_iter().next())
    }
}

impl<'a, R: Resource, B: Store> ReadRequest<'a, R, B, NoLoad, WithReport> {
    /// Fetch one row by primary key together with the redaction report — the
    /// report-carrying form of [`get`](ReadRequest::get), completing the
    /// output matrix (see the [module docs](self)).
    pub async fn get(mut self, id: impl Into<Value>) -> Result<AuthorizedOne<R::Data>> {
        self.params.insert(
            crate::query::reserved::IN,
            in_param(R::primary_key(), vec![id.into()]),
        );
        let (domain, ctx, action, query) = self.parts()?;
        let (records, _, report) = domain.read_records::<R>(ctx, &action, query).await?;
        let row = records.first().map(R::Data::from_record).transpose()?;
        Ok(AuthorizedOne { row, report })
    }
}

impl<'a, R: Resource, B: Store> ReadRequest<'a, R, B, WithLoad, WithReport> {
    /// Fetch one row by primary key with its requested loads resolved,
    /// together with the redaction report — the loaded, report-carrying form
    /// of [`get`](ReadRequest::get).
    pub async fn get(mut self, id: impl Into<Value>) -> Result<AuthorizedOne<Loaded<R::Data>>> {
        self.params.insert(
            crate::query::reserved::IN,
            in_param(R::primary_key(), vec![id.into()]),
        );
        let (domain, ctx, action, query) = self.parts()?;
        let (rows, report) = domain
            .read_loaded_with_report::<R>(ctx, &action, query)
            .await?;
        Ok(AuthorizedOne {
            row: rows.into_iter().next(),
            report,
        })
    }
}

/// One page of a sorted read: the rows, plus the position to resume after.
///
/// Produced by [`ReadRequest::page`]. The [`cursor`](Page::cursor) is `Some`
/// only when the page was **full** — a short page is the end of the result set,
/// so there is nothing to resume and no extra round trip to discover it.
#[derive(Clone, Debug)]
pub struct Page<T> {
    rows: Vec<T>,
    cursor: Option<crate::query::Cursor>,
}

impl<T> Page<T> {
    /// The rows of this page.
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// The cursor that resumes **after** the last row, or `None` when this page
    /// is the end of the result set.
    ///
    /// Pass it to [`ReadRequest::after`] with the same sort order to fetch the
    /// next page.
    pub fn cursor(&self) -> Option<&crate::query::Cursor> {
        self.cursor.as_ref()
    }

    /// Whether a further page may exist — exactly `cursor().is_some()`.
    pub fn has_more(&self) -> bool {
        self.cursor.is_some()
    }

    /// Consume the page, yielding its rows.
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }

    /// Consume the page, yielding its rows and its resume cursor.
    pub fn into_parts(self) -> (Vec<T>, Option<crate::query::Cursor>) {
        (self.rows, self.cursor)
    }

    /// Build the resume cursor for a finished page: the last row's value for
    /// each sort key, but only when the page came back **full**.
    ///
    /// A page shorter than its limit has exhausted the result set, so returning
    /// a cursor would cost the caller a round trip to learn what this already
    /// knows. An unsorted read has no position to describe and yields `None`.
    fn cursor_for(
        records: &[crate::value::Record],
        sort: &[crate::query::SortKey],
        limit: Option<u32>,
    ) -> Option<crate::query::Cursor> {
        if sort.is_empty() {
            return None;
        }
        let last = records.last()?;
        // Short page ⇒ end of the set. With no limit the whole set came back,
        // which is likewise the end.
        let full = limit.is_some_and(|l| records.len() as u64 >= u64::from(l));
        if !full {
            return None;
        }
        // Every sort key must be present on the row, or the cursor would point
        // at a position the layer cannot resume from.
        let keys: Option<Vec<crate::value::Value>> = sort
            .iter()
            .map(|k| last.get(&k.attribute).cloned())
            .collect();
        keys.map(crate::query::Cursor::from_keys)
    }
}

/// One page of an offset-paged read: the rows at a given offset, plus whether
/// the result set continues past them.
///
/// Produced by [`ReadRequest::offset_page`]. Where a [`Page`] hands back the
/// position to resume *after*, this hands back the position it was *read at* —
/// a numbered pager advances by arithmetic on [`offset`](OffsetPage::offset),
/// not by carrying a token. That is the whole trade: random access to page `n`,
/// at the cost of a page boundary that moves when rows before it are inserted
/// or removed. Prefer [`Page`] for a forward-only caller.
#[derive(Clone, Debug)]
pub struct OffsetPage<T> {
    rows: Vec<T>,
    offset: u32,
    has_more: bool,
}

impl<T> OffsetPage<T> {
    /// The rows of this page.
    pub fn rows(&self) -> &[T] {
        &self.rows
    }

    /// The offset this page was read at — the number of rows skipped before
    /// its first row.
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// Whether the result set continues past this page.
    ///
    /// Established by reading one row beyond the page, so it is exact for the
    /// moment the read ran — but, like every offset-paged answer, it describes
    /// the result set as it was *then*. It is not a count: this deliberately
    /// exposes no total, because the layer was never asked for one.
    pub fn has_more(&self) -> bool {
        self.has_more
    }

    /// The offset of the next page, or `None` when this is the last one.
    ///
    /// Pass it to [`ReadRequest::offset`] with the same sort order and limit.
    /// `None` also when the next offset would exceed `u32`, since there is no
    /// offset left to name.
    pub fn next_offset(&self) -> Option<u32> {
        if !self.has_more {
            return None;
        }
        u32::try_from(u64::from(self.offset) + self.rows.len() as u64).ok()
    }

    /// Consume the page, yielding its rows.
    pub fn into_rows(self) -> Vec<T> {
        self.rows
    }
}

// ── the four run shapes ──────────────────────────────────────────────────────

/// The boxed future a [`ReadRequest`] resolves to. `Send` on a default build;
/// with the `trace` feature the pipeline holds a span guard across `.await`s,
/// making **every** domain future `!Send` (a pre-existing property of that
/// feature, not specific to this builder), so the alias mirrors that.
#[cfg(not(feature = "trace"))]
type ReadFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;
#[cfg(feature = "trace")]
type ReadFut<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + 'a>>;

impl<'a, R, B> IntoFuture for ReadRequest<'a, R, B, NoLoad, NoReport>
where
    R: Resource,
    R::Data: Send,
    B: Store,
{
    type Output = Result<Vec<R::Data>>;
    type IntoFuture = ReadFut<'a, Vec<R::Data>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (domain, ctx, action, query) = self.parts()?;
            let (records, _, _) = domain.read_records::<R>(ctx, &action, query).await?;
            records.iter().map(R::Data::from_record).collect()
        })
    }
}

impl<'a, R, B> IntoFuture for ReadRequest<'a, R, B, NoLoad, WithReport>
where
    R: Resource,
    R::Data: Send,
    B: Store,
{
    type Output = Result<AuthorizedRead<R::Data>>;
    type IntoFuture = ReadFut<'a, AuthorizedRead<R::Data>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (domain, ctx, action, query) = self.parts()?;
            let (records, _, report) = domain.read_records::<R>(ctx, &action, query).await?;
            let rows = records
                .iter()
                .map(R::Data::from_record)
                .collect::<Result<Vec<_>>>()?;
            Ok(AuthorizedRead { rows, report })
        })
    }
}

impl<'a, R, B> IntoFuture for ReadRequest<'a, R, B, WithLoad, NoReport>
where
    R: Resource,
    R::Data: Send,
    B: Store,
{
    type Output = Result<Vec<Loaded<R::Data>>>;
    type IntoFuture = ReadFut<'a, Vec<Loaded<R::Data>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (domain, ctx, action, query) = self.parts()?;
            let (rows, _) = domain
                .read_loaded_with_report::<R>(ctx, &action, query)
                .await?;
            Ok(rows)
        })
    }
}

impl<'a, R, B> IntoFuture for ReadRequest<'a, R, B, WithLoad, WithReport>
where
    R: Resource,
    R::Data: Send,
    B: Store,
{
    type Output = Result<AuthorizedRead<Loaded<R::Data>>>;
    type IntoFuture = ReadFut<'a, AuthorizedRead<Loaded<R::Data>>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (domain, ctx, action, query) = self.parts()?;
            let (rows, report) = domain
                .read_loaded_with_report::<R>(ctx, &action, query)
                .await?;
            Ok(AuthorizedRead { rows, report })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datalayer::memory::InMemoryDataLayer;
    use crate::policy::{PolicySet, ScopedPolicy};
    use crate::resource::{Cardinality, Relationship};
    use crate::{
        ActionDef, ActionInput, Attribute, Context, Domain, DomainConfig, DomainContext, Error,
        Filter, Query, Record, Resource, Value, erase,
    };

    struct Author;
    impl Resource for Author {
        const NAME: &'static str = "author";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<String>("name"),
                Attribute::scalar::<String>("secret"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::write("create"), ActionDef::read("read")]
        }
        fn relationships() -> Vec<Relationship> {
            vec![Relationship {
                name: "posts".into(),
                destination: "post".into(),
                cardinality: Cardinality::HasMany,
                source_attribute: "id".into(),
                destination_attribute: "author_id".into(),
                through: None,
            }]
        }
        fn aggregates() -> Vec<crate::aggregate::Aggregate> {
            vec![crate::aggregate::Aggregate::count("post_count", "posts")]
        }
    }

    struct Post;
    impl Resource for Post {
        const NAME: &'static str = "post";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<String>("author_id"),
                Attribute::scalar::<String>("title"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::write("create"), ActionDef::read("read")]
        }
    }

    /// A resource whose only read action is *not* named `"read"`, to pin the
    /// builder's default-action convention.
    struct Widget;
    impl Resource for Widget {
        const NAME: &'static str = "widget";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![Attribute::scalar::<String>("id")]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::write("create"), ActionDef::read("list")]
        }
    }

    fn domain_with(policies: PolicySet) -> Domain {
        Domain::new(
            DomainConfig {
                resources: vec![erase::<Author>(), erase::<Post>(), erase::<Widget>()],
                policies,
                ..DomainConfig::default()
            },
            DomainContext::new(),
        )
    }

    /// Seed one author (returning its id) and `posts` posts pointing at it.
    async fn seed(d: &Domain, ctx: &mut Context<Arc<InMemoryDataLayer>>, posts: usize) -> Value {
        let author = d
            .handle_action::<Author>(
                ctx,
                "create",
                ActionInput::create(Record::from_iter([("name", "alice"), ("secret", "s3cr3t")]))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_record()
            .unwrap();
        let id = author.get("id").unwrap().clone();
        for i in 0..posts {
            d.handle_action::<Post>(
                ctx,
                "create",
                ActionInput::create(Record::from_iter([
                    ("author_id", id.clone()),
                    ("title", Value::from(format!("post-{i}"))),
                ]))
                .unwrap(),
            )
            .await
            .unwrap();
        }
        id
    }

    #[tokio::test]
    async fn plain_read_returns_rows() {
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        seed(&d, &mut ctx, 0).await;

        let rows: Vec<Record> = d.read::<Author>(&ctx).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name"), Some(&Value::from("alice")));
    }

    #[tokio::test]
    async fn get_fetches_by_primary_key() {
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        let id = seed(&d, &mut ctx, 0).await;

        let hit = d.read::<Author>(&ctx).get(id).await.unwrap();
        assert_eq!(hit.unwrap().get("name"), Some(&Value::from("alice")));

        let miss = d.read::<Author>(&ctx).get("nope").await.unwrap();
        assert!(miss.is_none());
    }

    #[tokio::test]
    async fn filter_params_pass_through_to_the_layer() {
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        seed(&d, &mut ctx, 3).await;

        // `"limit"` is the in-memory layer's own convention, not a core concept
        // — the builder hands the bag through untouched, like any read.
        let rows: Vec<Record> = d.read::<Post>(&ctx).filter("limit", 2).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn default_action_is_the_first_declared_read() {
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        d.handle_action::<Widget>(
            &mut ctx,
            "create",
            ActionInput::create(Record::new()).unwrap(),
        )
        .await
        .unwrap();

        // `Widget` declares no action named "read"; the builder must fall back
        // to its first declared read action ("list") rather than erroring.
        let rows: Vec<Record> = d.read::<Widget>(&ctx).await.unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn denied_by_default() {
        let d = domain_with(PolicySet::new());
        let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

        let err = d.read::<Author>(&ctx).await.unwrap_err();
        assert!(matches!(err, Error::Forbidden(_)));
    }

    /// Redact on read only — unlike `Deny`, which would also veto *writing*
    /// the attribute and block the seeding create.
    struct HideOnRead;
    #[async_trait::async_trait]
    impl crate::policy::Policy for HideOnRead {
        async fn authorize_attribute_read(
            &self,
            _resource: &str,
            _action: &str,
            _attribute: &str,
            _record: &Record,
            _actor: Option<&Record>,
        ) -> crate::policy::Decision {
            crate::policy::Decision::Forbid("hidden".into())
        }
    }

    #[tokio::test]
    async fn load_with_report_combines_relations_and_redactions() {
        // The combination the pre-builder API could not express: loaded
        // relations *and* the redaction report, in one read.
        let d = domain_with(PolicySet::permissive().with(ScopedPolicy::attribute(
            "author",
            "secret",
            "hide-secret",
            Arc::new(HideOnRead),
        )));
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        seed(&d, &mut ctx, 2).await;

        let auth = d
            .read::<Author>(&ctx)
            .load("posts")
            .aggregate("post_count")
            .with_report()
            .await
            .unwrap();

        assert_eq!(auth.rows.len(), 1);
        let author = &auth.rows[0];
        assert_eq!(author.get("posts").len(), 2);
        assert_eq!(author.aggregate("post_count"), Some(&Value::Int(2)));
        // The forbidden attribute is nulled on the row and named in the report.
        assert_eq!(author.row.get("secret"), Some(&Value::Null));
        assert_eq!(auth.report.redacted_attributes(), vec!["secret"]);
    }

    #[tokio::test]
    async fn erased_read_rejects_unresolvable_requests() {
        // `handle_action` resolves no loads/aggregates/computed: a query that
        // carries them must fail loudly, never be silently dropped.
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

        let query = Query::new("author").load(["posts"]);
        let err = d
            .handle_action::<Author>(&mut ctx, "read", ActionInput::read(query.clone()))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Invalid { .. }), "got: {err:?}");

        let err = d
            .read_authorized::<Author>(&ctx, "read", query)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Invalid { .. }), "got: {err:?}");
    }

    #[tokio::test]
    async fn filter_is_a_resourceless_read_input() {
        let d = domain_with(PolicySet::permissive());
        let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
        seed(&d, &mut ctx, 3).await;

        // A `Filter` names no resource (the type parameter does) and cannot
        // carry loads; the executor stamps the resource on dispatch.
        let outcome = d
            .handle_action::<Post>(
                &mut ctx,
                "read",
                ActionInput::read(Filter::new().param("limit", 1)),
            )
            .await
            .unwrap();
        assert_eq!(outcome.into_records().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn explain_is_a_dry_run() {
        let d = domain_with(PolicySet::new());
        let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

        let explanation = d.read::<Author>(&ctx).explain().await.unwrap();
        assert!(!explanation.is_allowed());

        let d = domain_with(PolicySet::permissive());
        let explanation = d.read::<Author>(&ctx).explain().await.unwrap();
        assert!(explanation.is_allowed());
    }
}
