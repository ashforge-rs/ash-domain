//! Derived read values: [`Aggregate`]s over a relationship and per-record
//! [`Computed`] fields.
//!
//! Both are *declarative descriptors* a [`Resource`](crate::Resource) returns,
//! plus a compute seam the consumer implements — the same split as every other
//! part of the framework. The core defines what an aggregate or computed field
//! *is* and how it's requested; it ships no query planner or expression engine.
//!
//! * An [`Aggregate`] rolls up a related resource into a single scalar — the
//!   count of a `has_many`, the max of a column across matches. It's declared on
//!   the resource and requested by name.
//! * A [`Computed`] field derives a value from a record (and optionally its
//!   loaded relations) — a `full_name`, a `days_since`, a computed price. The
//!   value is produced by a [`Computer`] the consumer supplies.
//!
//! Neither is auto-applied by a plain read; a caller opts in by naming them,
//! mirroring [`Query::load`](crate::query::Query::load) for relationships. How a
//! data layer *executes* an aggregate (a real SQL `COUNT(...)` vs. the reference
//! fallback of counting loaded rows) is the layer's concern — the core carries
//! the declaration, not the SQL.

use async_trait::async_trait;

use crate::error::Result;
use crate::value::{Record, Value};

/// The roll-up function of an [`Aggregate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateKind {
    /// Number of related records.
    Count,
    /// Sum of the target field across related records.
    Sum,
    /// Minimum of the target field.
    Min,
    /// Maximum of the target field.
    Max,
    /// Whether any related record exists.
    Exists,
}

/// A declared roll-up of a relationship into a single scalar value.
///
/// Declared on a resource via
/// [`Resource::aggregates`](crate::Resource::aggregates) and requested by name
/// through [`Query::aggregates`](crate::query::Query::aggregates). It names the
/// [`relationship`](Aggregate::relationship) to roll up (matching a declared
/// [`Relationship`](crate::Relationship)), the [`kind`](Aggregate::kind) of
/// roll-up, and — for everything but [`Count`](AggregateKind::Count) /
/// [`Exists`](AggregateKind::Exists) — the [`field`](Aggregate::field) on the
/// related resource to roll up.
#[derive(Clone, Debug)]
pub struct Aggregate {
    /// The name this aggregate is requested and returned under.
    pub name: String,
    /// The relationship (by name) whose records are rolled up.
    pub relationship: String,
    /// The roll-up function.
    pub kind: AggregateKind,
    /// The related-resource field to roll up. Ignored for `Count` / `Exists`.
    pub field: Option<String>,
}

impl Aggregate {
    /// A `count` of `relationship`, returned under `name`.
    pub fn count(name: impl Into<String>, relationship: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            relationship: relationship.into(),
            kind: AggregateKind::Count,
            field: None,
        }
    }

    /// A roll-up of `field` over `relationship` with `kind`, returned under
    /// `name`.
    pub fn over(
        name: impl Into<String>,
        relationship: impl Into<String>,
        kind: AggregateKind,
        field: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            relationship: relationship.into(),
            kind,
            field: Some(field.into()),
        }
    }

    /// Compute this aggregate over a set of already-fetched related records.
    ///
    /// This is the **reference implementation** — the fallback a data layer that
    /// cannot push the roll-up down to storage can use after loading the related
    /// rows. A layer that *can* (a SQL `COUNT`/`SUM`) is free to ignore it and
    /// return the scalar directly. Numeric roll-ups operate on
    /// [`Value::Int`]; non-numeric or absent fields are
    /// skipped.
    pub fn compute(&self, related: &[Record]) -> Value {
        match self.kind {
            AggregateKind::Count => Value::Int(related.len() as i64),
            AggregateKind::Exists => Value::Bool(!related.is_empty()),
            AggregateKind::Sum | AggregateKind::Min | AggregateKind::Max => {
                let field = match &self.field {
                    Some(f) => f,
                    None => return Value::Null,
                };
                let nums = related
                    .iter()
                    .filter_map(|r| r.get(field).and_then(Value::as_int));
                let result = match self.kind {
                    AggregateKind::Sum => nums.fold(None, |acc, n| Some(acc.unwrap_or(0) + n)),
                    AggregateKind::Min => nums.min(),
                    AggregateKind::Max => nums.max(),
                    _ => unreachable!(),
                };
                result.map_or(Value::Null, Value::Int)
            }
        }
    }
}

/// A declared per-record derived value, produced by a [`Computer`].
///
/// Declared on a resource via
/// [`Resource::computed`](crate::Resource::computed) and requested by name
/// through [`Query::computed`](crate::query::Query::computed). The declaration is
/// just the name and the compute implementation; the core invokes the
/// [`Computer`] per row and hands the result back beside the record.
///
/// A computed field is neither a database view (a filtered row-set) nor a
/// projection (a column selection): it *adds* a derived value to each row.
#[derive(Clone)]
pub struct Computed {
    /// The name this computed field is requested and returned under.
    pub name: String,
    /// The implementation that computes the value from a record.
    pub computer: std::sync::Arc<dyn Computer>,
}

impl Computed {
    /// A computed field named `name`, produced by `computer`.
    pub fn new(name: impl Into<String>, computer: std::sync::Arc<dyn Computer>) -> Self {
        Self {
            name: name.into(),
            computer,
        }
    }
}

impl std::fmt::Debug for Computed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Computed")
            .field("name", &self.name)
            .finish()
    }
}

/// Computes a [`Computed`] field's value from a record.
///
/// The consumer's function behind a derived field. It receives the record as
/// read from storage and returns the derived [`Value`]; returning `Err` aborts
/// the read.
///
/// ```
/// use ash_domain::aggregate::Computer;
/// use ash_domain::{Record, Result, Value};
///
/// /// `full_name` = `first` + " " + `last`.
/// struct FullName;
///
/// #[async_trait::async_trait]
/// impl Computer for FullName {
///     async fn compute(&self, record: &Record) -> Result<Value> {
///         let first = record.get("first").and_then(Value::as_str).unwrap_or("");
///         let last = record.get("last").and_then(Value::as_str).unwrap_or("");
///         Ok(Value::from(format!("{first} {last}").trim().to_string()))
///     }
/// }
/// ```
#[async_trait]
pub trait Computer: Send + Sync {
    /// Compute the derived value for `record`.
    async fn compute(&self, record: &Record) -> Result<Value>;
}
