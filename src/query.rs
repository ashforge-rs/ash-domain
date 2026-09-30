//! Backend-agnostic reads: a [`Query`] is a **bag of parameters**, not a
//! hand-built filter tree.
//!
//! A read names a `resource` and carries a `params` [`Record`] — an opaque bag
//! whose meaning is owned by the resource's read-action `Preparation`s and,
//! ultimately, by the [`DataLayer`](crate::datalayer::DataLayer) that receives
//! it. The core does **not** define a filter algebra: a layer is free to
//! interpret the bag as a SQL `WHERE`, an index scan, a REST call, or a custom
//! wire frame. How a well-formed bag becomes a native read is entirely the
//! layer's business.
//!
//! What the core *does* own are a few executor-driven concerns that ride
//! alongside the bag:
//!
//! - [`tenant`](Query::tenant) — a discriminator (not a filter) the domain sets
//!   from the [`Context`](crate::Context); a partitioning layer reads it directly.
//! - [`limit`](Query::limit) — the **row bound**: a hard ceiling on how many rows
//!   the layer may return. Unlike a param the layer is free to ignore, this one
//!   is part of the layer contract, and the domain checks it on the way back.
//!   The domain also sets it itself, from
//!   [`DomainConfig::max_rows`](crate::DomainConfig::max_rows), when a caller
//!   leaves it unset — so a read is bounded whether or not the caller thought
//!   about it.
//! - [`sort`](Query::sort) with [`after`](Query::after) or
//!   [`offset`](Query::offset) — the **ordering and paging** contract. A page
//!   needs an order, so a bound without a sort is an arbitrary subset; on top of
//!   an order a caller resumes either by keyset cursor (forward-only, stable
//!   under concurrent writes) or by offset (random access to page *n*, at the
//!   cost of a boundary that moves). Never both — the domain refuses a query
//!   carrying two positions.
//! - [`load`](Query::load) / [`aggregates`](Query::aggregate_names) /
//!   [`computed`](Query::computed) — relationship loads and derived values the
//!   *domain* resolves after the layer returns, over the abstract data layer.
//! - A small set of **reserved param keys** (see [`reserved`]) the executor
//!   writes into `params` for its own internal reads — a resolved key-set
//!   ([`reserved::IN`], used by relationship loads) and an unresolved
//!   relationship filter ([`reserved::RELATES`], which the domain rewrites into a
//!   key-set before the layer sees it). A layer must honour [`reserved::IN`] for
//!   relationship loading to work; everything else in `params` is the layer's own
//!   convention.
//!
//! There is no in-core query evaluation: a layer that cannot execute a bag reads
//! what it can and is responsible for keeping any degradation explicit. The
//! reference [`InMemoryDataLayer`](crate::datalayer::memory::InMemoryDataLayer)
//! defines its own private interpretation of the bag.

use crate::error::Result;
use crate::value::{IntoRecord, Record, Value};

/// Reserved param keys the [`Domain`](crate::Domain) writes into
/// [`Query::params`] for its own internal reads.
///
/// These are the one part of the bag the core defines rather than the layer.
/// Their names are prefixed `__ash_` so they cannot collide with a caller's or a
/// layer's own params. A layer that wants relationship loading to work **must**
/// interpret [`IN`](reserved::IN); the rest the domain resolves before the layer
/// is reached.
pub mod reserved {
    /// A resolved key-set: "return the rows of this resource whose `attr` is one
    /// of `keys`". Encoded as a [`Value::Map`](crate::Value::Map) under this key,
    /// with a [`Str`](crate::Value::Str) `"attr"` and a
    /// [`List`](crate::Value::List) `"keys"`.
    ///
    /// The executor writes this for every relationship-load hop (and as the
    /// *result* of resolving a [`RELATES`] filter). A data
    /// layer must honour it — see [`Query::key_set`](super::Query::key_set) to
    /// read it back.
    pub const IN: &str = "__ash_in";

    /// An **unresolved** relationship filter: "rows related through
    /// `relationship` to a row satisfying `predicate`". Encoded as a
    /// [`Value::Map`](crate::Value::Map) under this key. The
    /// [`Domain`](crate::Domain) resolves it into an [`IN`] key-set
    /// (reading the related keys through the abstract data layer) *before* the
    /// layer is asked to execute the read, so a layer never has to interpret it —
    /// though a SQL layer may prefer to and compile a `JOIN`. See
    /// [`Query::relates`](super::Query::relates) and
    /// [`Query::relates_filter`](super::Query::relates_filter).
    pub const RELATES: &str = "__ash_relates";

    /// Map field: the attribute name in an [`IN`] key-set.
    pub const IN_ATTR: &str = "attr";
    /// Map field: the list of admitted values in an [`IN`] key-set.
    pub const IN_KEYS: &str = "keys";
    /// Map field: the relationship name in a [`RELATES`] filter.
    pub const RELATES_REL: &str = "relationship";
    /// Map field: the inner predicate bag in a [`RELATES`]
    /// filter (a nested param [`Record`](crate::Record), encoded as a `Map`).
    pub const RELATES_PREDICATE: &str = "predicate";
}

/// A declarative read over a resource: its resource name plus an opaque bag of
/// parameters the data layer interprets.
///
/// Build one with [`Query::new`] and add params; the fields are public, so refine
/// with struct-update syntax:
///
/// ```
/// use ash_domain::query::Query;
///
/// let q = Query::new("note")
///     .param("done", false)
///     .param("limit", 10);
/// # let _ = q;
/// ```
///
/// What `done` and `limit` *mean* is up to the resource's read
/// [`Preparation`](crate::action::Preparation)s and the
/// [`DataLayer`](crate::datalayer::DataLayer); the core attaches no semantics to
/// them.
#[derive(Clone, Debug, Default)]
pub struct Query {
    /// The resource being read.
    pub resource: String,
    /// The opaque parameter bag the data layer interprets. May also carry the
    /// executor's [`reserved`] keys.
    pub params: Record,
    /// Relationships to load alongside each record. Resolved by the
    /// [`Domain`](crate::Domain) against the abstract data layer after the read;
    /// a layer may also honour them natively.
    pub load: Vec<String>,
    /// The tenant this read is scoped to, if any. The domain sets it from the
    /// [`Context`](crate::Context) for a tenant-scoped resource; a partitioning
    /// layer can read it directly. It is a discriminator, not a filter param.
    pub tenant: Option<Value>,
    /// A hard upper bound on the number of rows the layer may return, or `None`
    /// for "the caller set no bound" — in which case the domain sets one from
    /// [`DomainConfig::max_rows`](crate::DomainConfig::max_rows) before the layer
    /// ever sees the query.
    ///
    /// This is **not** an ordinary param a layer may ignore: it is a contract
    /// point like [`reserved::IN`]. A layer must return at most this many rows
    /// (a SQL `LIMIT`, a scan cut short, a page size on an API call); the domain
    /// verifies the count on the way back and fails the read with
    /// [`Error::DataLayer`](crate::Error::DataLayer) if the layer overran it,
    /// rather than truncating and hiding the breach.
    ///
    /// `Some(0)` is a legitimate request for an empty result, not "unbounded" —
    /// that is why this is a plain `u32` and not a `NonZeroU32`.
    pub limit: Option<u32>,
    /// Whether this is a sanctioned **cross-tenant** read — the domain dropped the
    /// tenant predicate because the caller opted in via
    /// [`Context::allow_cross_tenant`](crate::Context::allow_cross_tenant). A
    /// partitioning ([`Layer`](crate::TenantStrategy::Layer)) data layer must read
    /// across partitions when this is set; audit can see the read for what it is.
    /// Never set on a global (non-tenant) resource — there is nothing to cross.
    pub across_tenants: bool,
    /// Names of declared [`Aggregate`](crate::aggregate::Aggregate)s to compute
    /// alongside each row. Resolved by the domain; an unknown name is an error.
    pub aggregate_names: Vec<String>,
    /// Names of declared [`Computed`](crate::aggregate::Computed) fields to
    /// compute for each row. Resolved by the domain; an unknown name is an error.
    pub computed: Vec<String>,
    /// The **sort order**, outermost key first, or empty for "the layer's
    /// natural order".
    ///
    /// Like [`limit`](Query::limit) this is a contract point, not an ordinary
    /// param: a layer that cannot sort by these keys must say so
    /// ([`Error::Unsupported`](crate::Error::Unsupported)) rather than return
    /// rows in some other order, because a bounded read of unordered rows is an
    /// arbitrary subset rather than a page. The domain validates every key names
    /// a declared attribute before the layer sees it.
    pub sort: Vec<SortKey>,
    /// Resume a previous page: return only rows that fall **after** this cursor
    /// in the query's [`sort`](Query::sort) order.
    ///
    /// Produced by [`Page::cursor`] from a previous read and passed back
    /// verbatim. The layer decodes it with [`Cursor::keys`] and turns it into a
    /// keyset predicate. Meaningless without a `sort`, which is why the domain
    /// rejects the combination.
    pub after: Option<Cursor>,
    /// Skip this many rows of the sorted result set before returning any — the
    /// **offset** of offset/limit pagination.
    ///
    /// Like [`limit`](Query::limit) and [`sort`](Query::sort) this is a contract
    /// point, not an ordinary param: a layer must skip exactly this many rows of
    /// the order it was asked for (a SQL `OFFSET`, a scan advanced before
    /// collecting), or decline with
    /// [`Error::Unsupported`](crate::Error::Unsupported).
    ///
    /// Offset paging is the weaker of the two models the core offers, and
    /// deliberately so: rows inserted or removed before the offset between two
    /// pages shift every later row, so a caller walking pages can see a row
    /// twice or miss it entirely. It is here because it buys what a cursor
    /// cannot — random access to page *n* — which is what a numbered pager
    /// needs. Prefer [`after`](Query::after) whenever the caller only ever walks
    /// forward. The two are mutually exclusive: a query carrying both describes
    /// two different positions, and the domain rejects it.
    ///
    /// Meaningless without a `sort` for the same reason [`limit`](Query::limit)
    /// is — skipping rows of an arbitrary order yields an arbitrary subset — so
    /// the domain rejects that combination too.
    pub offset: Option<u32>,
}

/// The direction of one [`SortKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    /// Smallest first.
    Ascending,
    /// Largest first.
    Descending,
}

/// One key of a [`Query`]'s sort order: an attribute name and a direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortKey {
    /// The attribute to sort by. The domain checks it is declared on the
    /// resource before the layer sees the query.
    pub attribute: String,
    /// Which way to sort it.
    pub direction: SortDirection,
}

impl SortKey {
    /// Sort by `attribute`, smallest first.
    pub fn asc(attribute: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            direction: SortDirection::Ascending,
        }
    }

    /// Sort by `attribute`, largest first.
    pub fn desc(attribute: impl Into<String>) -> Self {
        Self {
            attribute: attribute.into(),
            direction: SortDirection::Descending,
        }
    }
}

/// An opaque position in a sorted result set — the "resume here" token of
/// keyset pagination.
///
/// A cursor carries the **sort-key values of the last row of a page**, nothing
/// else: no offset, no snapshot, no server state. A layer resumes by asking for
/// rows ordered after those values, which is why paging this way neither skips
/// nor repeats rows when the underlying data changes between pages — the defect
/// an `OFFSET` has and this does not.
///
/// It is deliberately **opaque to callers**: build one only with
/// [`Page::cursor`], and read it only from inside a data layer with
/// [`keys`](Cursor::keys). Callers pass it back verbatim.
#[derive(Clone, Debug, PartialEq)]
pub struct Cursor {
    /// The last row's value for each key of the query's sort order, in the same
    /// order as [`Query::sort`].
    keys: Vec<Value>,
}

impl Cursor {
    /// The sort-key values this cursor points at, in [`Query::sort`] order —
    /// what a [`DataLayer`](crate::datalayer::DataLayer) needs to build its
    /// keyset predicate.
    pub fn keys(&self) -> &[Value] {
        &self.keys
    }

    /// Build a cursor from raw sort-key values. For a data layer that
    /// round-trips a cursor through its own encoding; ordinary callers use
    /// [`Page::cursor`].
    pub fn from_keys(keys: Vec<Value>) -> Self {
        Self { keys }
    }
}

impl Query {
    /// Start a query against `resource` with an empty param bag.
    pub fn new(resource: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            ..Default::default()
        }
    }

    /// Set a param in the bag, consuming and returning `self` for chaining.
    pub fn param(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(key, value);
        self
    }

    /// Bound the read to at most `limit` rows.
    ///
    /// The layer must honour it (see [`Query::limit`]); the domain checks that it
    /// did. A caller-set bound is taken as deliberate and passed through
    /// untouched — the domain's own
    /// [`max_rows`](crate::DomainConfig::max_rows) ceiling only fills in for a
    /// read that carries no bound of its own.
    ///
    /// ```
    /// use ash_domain::Query;
    /// let q = Query::new("note").limit(50);
    /// assert_eq!(q.limit, Some(50));
    /// ```
    #[must_use]
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Sort the read by `attribute`, ascending — appending to any sort keys
    /// already set, so repeated calls build a compound order.
    ///
    /// ```
    /// use ash_domain::Query;
    /// let q = Query::new("note").sort_asc("created_at").sort_desc("id");
    /// assert_eq!(q.sort.len(), 2);
    /// ```
    #[must_use]
    pub fn sort_asc(mut self, attribute: impl Into<String>) -> Self {
        self.sort.push(SortKey::asc(attribute));
        self
    }

    /// Sort the read by `attribute`, descending. See
    /// [`sort_asc`](Query::sort_asc).
    #[must_use]
    pub fn sort_desc(mut self, attribute: impl Into<String>) -> Self {
        self.sort.push(SortKey::desc(attribute));
        self
    }

    /// Resume after `cursor` — the next page of the read that produced it.
    ///
    /// The sort order must match the one the cursor came from; the domain
    /// refuses a cursor on an unsorted read, since there is no order to resume.
    #[must_use]
    pub fn after(mut self, cursor: Cursor) -> Self {
        self.after = Some(cursor);
        self
    }

    /// Skip the first `offset` rows of the sorted result set.
    ///
    /// The offset-paging counterpart of [`after`](Query::after), and mutually
    /// exclusive with it: page `n` of a numbered pager is
    /// `.offset(n * page_size).limit(page_size)`. Requires a
    /// [`sort`](Query::sort) — the domain rejects an offset without one, since
    /// skipping rows of an unordered read skips arbitrary rows. See
    /// [`Query::offset`] for why a cursor is the better default.
    ///
    /// ```
    /// use ash_domain::Query;
    /// let q = Query::new("note").sort_asc("id").offset(50).limit(25);
    /// assert_eq!(q.offset, Some(50));
    /// ```
    #[must_use]
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Request `relationships` be loaded alongside each row.
    pub fn load<I, S>(mut self, relationships: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.load.extend(relationships.into_iter().map(Into::into));
        self
    }

    /// Request declared aggregates be computed for each row.
    pub fn aggregates<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.aggregate_names
            .extend(names.into_iter().map(Into::into));
        self
    }

    /// Request declared computed fields be computed for each row.
    pub fn computed<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.computed.extend(names.into_iter().map(Into::into));
        self
    }

    // ── reserved-param helpers ────────────────────────────────────────────────

    /// Build the reserved [`IN`](reserved::IN) key-set query: rows of `resource`
    /// whose `attr` is one of `keys`. The executor uses this for relationship
    /// loads; a layer reads it back with [`key_set`](Query::key_set).
    pub fn key_set(resource: impl Into<String>, attr: impl Into<String>, keys: Vec<Value>) -> Self {
        Query::new(resource).param(reserved::IN, in_param(attr.into(), keys))
    }

    /// Read this query's [`IN`](reserved::IN) key-set, if it carries one:
    /// `(attr, keys)`. A data layer calls this to honour an executor-issued
    /// relationship load.
    pub fn as_key_set(&self) -> Option<(&str, &[Value])> {
        let map = self.params.get(reserved::IN)?.as_map()?;
        let attr = map.get(reserved::IN_ATTR)?.as_str()?;
        let keys = map.get(reserved::IN_KEYS)?.as_list()?;
        Some((attr, keys))
    }

    /// Build an **unresolved** [`RELATES`](reserved::RELATES) filter: rows related
    /// through `relationship` to a row matching the `predicate` bag. The
    /// [`Domain`](crate::Domain) resolves this into a key-set before the layer
    /// runs; see [`relates_filter`](Query::relates_filter).
    pub fn relates(
        resource: impl Into<String>,
        relationship: impl Into<String>,
        predicate: Record,
    ) -> Self {
        let mut map = std::collections::BTreeMap::new();
        map.insert(
            reserved::RELATES_REL.to_string(),
            Value::Str(relationship.into()),
        );
        map.insert(
            reserved::RELATES_PREDICATE.to_string(),
            Value::Map(predicate.0),
        );
        Query::new(resource).param(reserved::RELATES, Value::Map(map))
    }

    /// Read this query's unresolved [`RELATES`](reserved::RELATES) filter, if it
    /// carries one: `(relationship, predicate)`. The domain uses this to decide
    /// whether relationship-filter resolution is needed before a read.
    pub fn relates_filter(&self) -> Option<(&str, Record)> {
        let map = self.params.get(reserved::RELATES)?.as_map()?;
        let relationship = map.get(reserved::RELATES_REL)?.as_str()?;
        let predicate = map.get(reserved::RELATES_PREDICATE)?.as_map()?;
        Some((relationship, Record(predicate.clone())))
    }
}

/// Encode a [`reserved::IN`] key-set value: rows whose `attr` is one of `keys`.
/// Shared by [`Query::key_set`] and the executor paths that fold a key-set into
/// an existing param bag.
pub(crate) fn in_param(attr: String, keys: Vec<Value>) -> Value {
    let mut map = std::collections::BTreeMap::new();
    map.insert(reserved::IN_ATTR.to_string(), Value::Str(attr));
    map.insert(reserved::IN_KEYS.to_string(), Value::List(keys));
    Value::Map(map)
}

/// A caller-side read input: the layer-interpreted param bag — and nothing
/// else.
///
/// This is the input for the erased read path,
/// [`ActionInput::read`](crate::ActionInput::read). Where a hand-built [`Query`]
/// can carry a resource name the typed call site contradicts (silently
/// overwritten) and `load`/`aggregates`/`computed` requests the erased path must
/// reject, a `Filter` can carry neither: the resource is always named by the
/// call site's type parameter, and relationship loads / derived values are only
/// expressible on the path that resolves them, [`Domain::read`](crate::Domain::read).
///
/// ```
/// use ash_domain::{ActionInput, Filter};
///
/// let input = ActionInput::read(
///     Filter::new().param("owner", "alice").param("limit", 50),
/// );
/// # let _ = input;
/// ```
#[derive(Clone, Debug, Default)]
pub struct Filter {
    params: Record,
}

impl Filter {
    /// An empty filter: an unconstrained read of the resource.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a param in the bag. What it means is the data layer's convention,
    /// exactly as [`Query::param`] — the core attaches no semantics.
    pub fn param(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(key, value);
        self
    }

    /// Merge a whole **typed param bag** into the filter — the compile-checked
    /// sibling of [`param`](Filter::param). `params` is anything [`IntoRecord`]
    /// (typically a `#[derive(IntoRecord)]` struct you define), so your layer's
    /// param convention gets field names and types the compiler checks instead
    /// of strings. The core still attaches no semantics to any key — the struct
    /// is a contract between you and your data layer, exactly as with
    /// [`param`](Filter::param). A key set both ways keeps the later value.
    ///
    /// Fallible because [`IntoRecord`] is: an out-of-`i64`-range integer field
    /// has no lossless neutral form and fails loudly here.
    ///
    /// ```
    /// # fn main() -> ash_domain::Result<()> {
    /// use ash_domain::{ActionInput, Filter, IntoRecord};
    ///
    /// #[derive(IntoRecord)]
    /// struct OpenTodos {
    ///     done: bool,
    ///     limit: i64,
    /// }
    ///
    /// let input = ActionInput::read(
    ///     Filter::new().params(OpenTodos { done: false, limit: 50 })?,
    /// );
    /// # let _ = input;
    /// # Ok(())
    /// # }
    /// ```
    pub fn params(mut self, params: impl IntoRecord) -> Result<Self> {
        self.params.0.extend(params.into_record()?.0);
        Ok(self)
    }
}

/// A [`Filter`] is a [`Query`] with no resource name; the executor stamps the
/// resource of the action it runs against (it does so for every read).
impl From<Filter> for Query {
    fn from(filter: Filter) -> Self {
        Query {
            params: filter.params,
            ..Query::default()
        }
    }
}
