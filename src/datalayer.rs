//! Persistence: the [`DataLayer`] seam.
//!
//! A resource is backed by a `DataLayer`, which knows how to create, read,
//! update, and destroy [`Record`]s. It is the primary extension point: the core
//! ships one implementation — the in-memory layer ([`memory`]) — and everything
//! else (a file, a database, a socket to a remote store) is the consumer's to
//! `impl DataLayer for`.
//!
//! Records are addressed by their primary-key value; the domain passes the
//! primary-key attribute name so a layer need not guess it. A read is issued as
//! a [`Query`] — an opaque **param bag** the layer interprets however it can (a
//! SQL `WHERE`, an index scan, a custom wire frame). The core defines no query
//! language and does no query evaluation of its own, so a layer that cannot
//! execute part of a bag is responsible for keeping any degradation explicit.
//! The one part of the bag the core defines is a small set of reserved params
//! (see [`reserved`](crate::query::reserved)); a layer **must** honour
//! [`reserved::IN`](crate::query::reserved::IN) for relationship loading to work.
//!
//! Several contract points ride outside the bag and are **not** optional:
//! [`reserved::IN`](crate::query::reserved::IN) above,
//! [`Query::limit`](crate::Query::limit) — the row bound — and the paging pair
//! [`Query::sort`](crate::Query::sort) with either
//! [`Query::after`](crate::Query::after) (keyset) or
//! [`Query::offset`](crate::Query::offset) (offset paging). A layer must return at
//! most that many rows; the domain checks the count on the way back and fails
//! the read ([`Error::DataLayer`]) if the layer overran
//! it, rather than truncating and hiding the breach. Run
//! [`conformance`] against your layer to check
//! both.
//!
//! The trait is deliberately minimal — just CRUD. Anything beyond it
//! (transactions, batching, savepoints, streaming) is **the consumer's to add**:
//! define your own trait that extends `DataLayer` with the methods you need,
//! implement it for your storage, and drive it through a backend of your own (a
//! [`Store`](crate::Store)). The core neither provides nor assumes a transaction
//! lifecycle.

use std::collections::HashMap;

use async_trait::async_trait;

use crate::aggregate::Aggregate;
use crate::error::{Error, Result};
use crate::query::Query;
use crate::value::{Record, Value};

pub mod conformance;
pub mod memory;

/// Pluggable storage for a domain's records: create, read, update, destroy.
///
/// This is the agnostic base every backend implements. It is intentionally just
/// CRUD; a consumer that needs more (transactions, batching, …) extends it with
/// their own trait and implementation.
///
/// Beyond CRUD, the trait exposes **optional push-down hooks** with defaults
/// that decline — [`aggregate`](DataLayer::aggregate) is the first. A layer that
/// can compute something in storage (a SQL `COUNT`) overrides the hook; a layer
/// that can't leaves the default, and the [`Domain`](crate::Domain) falls back to
/// its own reference computation. No layer is *required* to implement them, so
/// the seam adds capability without adding obligation.
#[async_trait]
pub trait DataLayer: Send + Sync {
    /// Insert a new record for `resource`. `pk` is the primary-key attribute
    /// name; the record is expected to carry a value for it.
    async fn create(&self, resource: &str, pk: &str, record: Record) -> Result<Record>;

    /// Read the records of `query.resource` matching the [`Query`]'s param bag.
    ///
    /// The bag is this layer's to interpret; the core attaches no query semantics
    /// to it. A layer **must** honour the reserved key-set param
    /// ([`reserved::IN`](crate::query::reserved::IN), read back with
    /// [`Query::as_key_set`]) so relationship loading works, **must** return at
    /// most [`Query::limit`] rows when that bound is set (the domain verifies the
    /// count and fails the read if it was overrun), **must** skip exactly
    /// [`Query::offset`] rows of the requested order when that is set (or decline
    /// with [`Error::Unsupported`] — the skip happens before the row bound
    /// applies, so a page is `offset` then `limit`), and should read
    /// [`Query::tenant`] if it partitions by tenant. Any param it cannot execute
    /// it should surface explicitly (e.g. [`Error::Unsupported`]) rather than
    /// degrade silently — the core no longer evaluates queries on the layer's
    /// behalf.
    async fn read(&self, query: &Query) -> Result<Vec<Record>>;

    /// Persist several records of `resource` in one call, returning them in the
    /// order they were given.
    ///
    /// The default implementation creates them one at a time, so **every layer
    /// already satisfies this** — override it only when your storage can do
    /// better (a multi-row `INSERT`, a batch API call). Overriding changes
    /// throughput, never semantics: the domain has already staged, authorized,
    /// and validated every row before calling this, and expects one returned
    /// record per input row, in order.
    ///
    /// It is **not** an atomicity boundary. A layer whose batch insert is atomic
    /// may make it so, but the domain does not assume it: when the store offers
    /// [`Store::begin`](crate::Store::begin) the batch already runs inside that
    /// transaction, which is where atomicity comes from. A default-implementation
    /// layer that fails halfway leaves the earlier rows written unless a
    /// transaction rolls them back — the same contract as any other write here.
    async fn create_many(
        &self,
        resource: &str,
        pk: &str,
        records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        let mut created = Vec::with_capacity(records.len());
        for record in records {
            created.push(self.create(resource, pk, record).await?);
        }
        Ok(created)
    }

    /// Fetch a single record by primary-key value.
    async fn get(&self, resource: &str, pk: &str, id: &Value) -> Result<Option<Record>>;

    /// Merge `changes` into the record identified by `id` and return the result.
    async fn update(
        &self,
        resource: &str,
        pk: &str,
        id: &Value,
        changes: &Record,
    ) -> Result<Record>;

    /// Apply `changes` to the record identified by `id` **only if** its current
    /// version attribute still equals `expected` — the conditional write behind
    /// optimistic concurrency.
    ///
    /// `version_attribute` names the column holding the row version, and
    /// `changes` already carries its **new** value (the domain bumps it before
    /// calling). The layer must perform the compare and the write as **one
    /// atomic step** — a `UPDATE … WHERE id = ? AND version = ?`, a conditional
    /// put, a compare-and-swap. Reading the version and then writing in two
    /// steps reintroduces the very race this exists to close.
    ///
    /// Return [`Error::Conflict`] when the row exists but
    /// its version does not match, and
    /// [`Error::NotFound`] when there is no such row.
    ///
    /// The default implementation **declines** with
    /// [`Error::Unsupported`], so a layer that cannot
    /// do a conditional write says so loudly instead of silently degrading to a
    /// last-writer-wins update. The domain only calls this for a resource that
    /// declares a [`version_attribute`](crate::Resource::version_attribute), so
    /// a layer that never serves such a resource need not implement it.
    async fn update_versioned(
        &self,
        resource: &str,
        pk: &str,
        id: &Value,
        changes: &Record,
        version_attribute: &str,
        expected: &Value,
    ) -> Result<Record> {
        let _ = (pk, id, changes, version_attribute, expected);
        Err(Error::Unsupported(format!(
            "this data layer does not implement update_versioned, but `{resource}` declares a \
             version attribute: implement the conditional write, or drop \
             Resource::version_attribute for that resource"
        )))
    }

    /// Remove the record identified by `id`. Idempotent.
    async fn destroy(&self, resource: &str, pk: &str, id: &Value) -> Result<()>;

    /// **Optional push-down:** compute `aggregate` over the records of
    /// `destination` grouped by `destination_attribute`, for the given parent
    /// `keys`, entirely in storage.
    ///
    /// This is the hook a SQL layer overrides to turn "count each author's
    /// posts" into one `SELECT author_id, COUNT(*) … WHERE author_id IN (…) GROUP
    /// BY author_id` instead of loading every related row. Return
    /// `Ok(Some(map))` keyed by [`value_key`](crate::value::value_key) of each
    /// parent key, with the rolled-up [`Value`] for each; return `Ok(None)` (the
    /// default) to decline, in which case the [`Domain`](crate::Domain) loads the
    /// related rows and applies [`Aggregate::compute`] itself.
    ///
    /// A key absent from the returned map is treated as the aggregate's empty
    /// value (e.g. a count of `0`).
    async fn aggregate(
        &self,
        destination: &str,
        destination_attribute: &str,
        keys: &[Value],
        aggregate: &Aggregate,
    ) -> Result<Option<HashMap<String, Value>>> {
        let _ = (destination, destination_attribute, keys, aggregate);
        Ok(None)
    }
}

/// An open **unit of work** over a [`DataLayer`] — the optional atomicity seam.
///
/// A `Transaction` *is* a [`DataLayer`]: inside the boundary the
/// [`Domain`](crate::Domain) issues the **same** create/read/update/destroy calls
/// it makes against a bare layer, only they are staged and become durable together
/// on [`commit`](Transaction::commit). Dropping the handle without committing —
/// or calling [`rollback`](Transaction::rollback) — discards them.
///
/// The core opens one only when a [`Store`](crate::Store) offers it via
/// [`Store::begin`](crate::Store::begin); a store that declines keeps the
/// non-transactional path (persist commits immediately, best-effort events). The
/// core ships **no** transaction runtime — a real implementation wraps whatever
/// its storage provides (a SQL `BEGIN`/`COMMIT`, a batch buffer). See
/// `docs/transaction-seam.md`.
///
/// `commit`/`rollback` take `self: Box<Self>` so a spent transaction cannot be
/// reused — the type system enforces it, the same way [`HandlerContext::complete`]
/// consumes a handler context.
///
/// [`HandlerContext::complete`]: crate::HandlerContext::complete
#[async_trait]
pub trait Transaction: DataLayer {
    /// Make every operation issued through this handle durable, then close it.
    /// Consumes the transaction. An `Err` means the commit itself failed and the
    /// unit of work did **not** take effect.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Discard every operation issued through this handle and close it. Consumes
    /// the transaction. The core calls this when a step *after* persist (e.g. an
    /// `after_action` extension) fails, so the write is undone rather than left
    /// committed.
    async fn rollback(self: Box<Self>) -> Result<()>;
}
