//! An in-memory [`DataLayer`] — the default reference backend.
//!
//! Records live in a shared, `Mutex`-guarded map (resource → primary-key →
//! record). It is a plain, non-transactional layer: a clean reference for
//! implementing the [`DataLayer`] trait. State does not survive process exit.
//!
//! Since [`Query`] is an opaque param bag, this layer defines **its own private
//! interpretation** of that bag — there is no core query language it inherits.
//! It honours:
//!
//! - [`reserved::IN`](crate::query::reserved::IN) — a key-set load
//!   (`attr` ∈ `keys`); required for relationship loading to work.
//! - [`Query::tenant`] — an equality match on the resource's tenant attribute
//!   is *not* applied here (the domain folds tenancy into params/tenant for a
//!   partitioning layer); this layer simply exposes every stored row and lets the
//!   domain's own tenant checks apply. Tenant filtering that must happen in the
//!   layer is expressed as an ordinary `eq` param below.
//! - `eq` (a [`Map`](crate::Value::Map) of attribute → value; all must match),
//!   `sort` (a `List` of `[attr, "asc"|"desc"]` pairs), `limit`, `offset`, and
//!   `cursor` (a `Map` of `{field, after, dir}`) — this layer's own convention
//!   for the common read shapes. A different layer is free to interpret the bag
//!   however it likes.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::datalayer::DataLayer;
use crate::error::{Error, Result};
use crate::query::{Query, SortDirection};
use crate::value::{Record, Value, value_key};

/// This layer's own reserved param keys, beyond the core
/// [`reserved`](crate::query::reserved) set. Public so tests and callers that
/// target the in-memory layer can build the same bag it reads.
pub mod params {
    /// Equality filter: a [`Map`](crate::Value::Map) of attribute → value; a row
    /// matches when every entry equals the row's value for that attribute.
    pub const EQ: &str = "eq";
    /// Sort keys: a [`List`](crate::Value::List) of two-element
    /// `[attr, "asc"|"desc"]` [`List`](crate::Value::List)s, applied in order.
    pub const SORT: &str = "sort";
    /// Maximum rows to return: an [`Int`](crate::Value::Int).
    pub const LIMIT: &str = "limit";
    /// Rows to skip before the limit: an [`Int`](crate::Value::Int).
    pub const OFFSET: &str = "offset";
    /// Keyset boundary: a [`Map`](crate::Value::Map) `{field, after, dir}` where
    /// `dir` is `"asc"` (return rows `> after`) or `"desc"` (`< after`).
    pub const CURSOR: &str = "cursor";
    /// Cursor field: the attribute the keyset boundary is keyed on.
    pub const CURSOR_FIELD: &str = "field";
    /// Cursor value: the last-seen value of `field` on the previous page.
    pub const CURSOR_AFTER: &str = "after";
    /// Cursor direction: `"asc"` or `"desc"`.
    pub const CURSOR_DIR: &str = "dir";
}

/// resource name → (primary-key string → record)
type Tables = HashMap<String, HashMap<String, Record>>;

/// An in-memory data layer.
#[derive(Default, Clone)]
pub struct InMemoryDataLayer {
    tables: Arc<Mutex<Tables>>,
}

impl InMemoryDataLayer {
    /// Create an empty data layer.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DataLayer for InMemoryDataLayer {
    async fn create(&self, resource: &str, pk: &str, record: Record) -> Result<Record> {
        let mut tables = self.tables.lock().expect("data layer mutex poisoned");
        create_in(&mut tables, resource, pk, record)
    }

    async fn read(&self, query: &Query) -> Result<Vec<Record>> {
        let tables = self.tables.lock().expect("data layer mutex poisoned");
        Ok(read_in(&tables, query))
    }

    async fn get(&self, resource: &str, _pk: &str, id: &Value) -> Result<Option<Record>> {
        let tables = self.tables.lock().expect("data layer mutex poisoned");
        Ok(get_in(&tables, resource, id))
    }

    async fn update(
        &self,
        resource: &str,
        _pk: &str,
        id: &Value,
        changes: &Record,
    ) -> Result<Record> {
        let mut tables = self.tables.lock().expect("data layer mutex poisoned");
        update_in(&mut tables, resource, id, changes)
    }

    async fn update_versioned(
        &self,
        resource: &str,
        _pk: &str,
        id: &Value,
        changes: &Record,
        version_attribute: &str,
        expected: &Value,
    ) -> Result<Record> {
        // The compare and the write happen under one mutex acquisition, so no
        // other writer can slip between them — the atomicity the seam requires.
        let mut tables = self.tables.lock().expect("data layer mutex poisoned");
        let key = value_key(id);
        let table = tables
            .get_mut(resource)
            .ok_or_else(|| Error::NotFound(format!("{resource}/{key}")))?;
        let record = table
            .get_mut(&key)
            .ok_or_else(|| Error::NotFound(format!("{resource}/{key}")))?;
        let current = record.get(version_attribute);
        if current != Some(expected) {
            return Err(Error::Conflict {
                resource: resource.to_string(),
                message: format!(
                    "row {key} was read at {version_attribute} {expected:?} but is now at {current:?}"
                ),
            });
        }
        for (field, value) in changes.iter() {
            record.insert(field.clone(), value.clone());
        }
        Ok(record.clone())
    }

    async fn destroy(&self, resource: &str, _pk: &str, id: &Value) -> Result<()> {
        let mut tables = self.tables.lock().expect("data layer mutex poisoned");
        destroy_in(&mut tables, resource, id);
        Ok(())
    }
}

// ── table operations ─────────────────────────────────────────────────────────

fn create_in(tables: &mut Tables, resource: &str, pk: &str, record: Record) -> Result<Record> {
    let key = record
        .get(pk)
        .map(value_key)
        .ok_or_else(|| Error::invalid(format!("record is missing primary key `{pk}`")))?;
    let table = tables.entry(resource.to_string()).or_default();
    if table.contains_key(&key) {
        return Err(Error::invalid(format!(
            "record `{key}` already exists in `{resource}`"
        )));
    }
    table.insert(key, record.clone());
    Ok(record)
}

fn read_in(tables: &Tables, query: &Query) -> Vec<Record> {
    let records: Vec<Record> = tables
        .get(&query.resource)
        .map(|t| t.values().cloned().collect())
        .unwrap_or_default();
    apply(query, records)
}

fn get_in(tables: &Tables, resource: &str, id: &Value) -> Option<Record> {
    tables
        .get(resource)
        .and_then(|t| t.get(&value_key(id)))
        .cloned()
}

fn update_in(tables: &mut Tables, resource: &str, id: &Value, changes: &Record) -> Result<Record> {
    let key = value_key(id);
    let table = tables
        .get_mut(resource)
        .ok_or_else(|| Error::NotFound(format!("{resource}/{key}")))?;
    let record = table
        .get_mut(&key)
        .ok_or_else(|| Error::NotFound(format!("{resource}/{key}")))?;
    for (field, value) in changes.iter() {
        record.insert(field.clone(), value.clone());
    }
    Ok(record.clone())
}

fn destroy_in(tables: &mut Tables, resource: &str, id: &Value) {
    if let Some(table) = tables.get_mut(resource) {
        table.remove(&value_key(id));
    }
}

// ── this layer's private interpretation of the param bag ──────────────────────

/// Evaluate the layer's understood params — the core `reserved::IN` key-set and
/// the core's `sort`/`after`/`offset`/`limit` contract points, plus this layer's
/// own `eq`/`sort`/`limit`/`offset`/`cursor` convention — over `records`.
fn apply(query: &Query, mut records: Vec<Record>) -> Vec<Record> {
    let p = &query.params;

    // Core reserved key-set: `attr` ∈ `keys`.
    if let Some((attr, keys)) = query.as_key_set() {
        records.retain(|r| r.get(attr).is_some_and(|v| keys.contains(v)));
    }

    // Equality filter: every entry must match.
    if let Some(eq) = p.get(params::EQ).and_then(Value::as_map) {
        records.retain(|r| eq.iter().all(|(field, want)| r.get(field) == Some(want)));
    }

    // Sort keys, applied last-to-first so the first key is primary.
    if let Some(sort) = p.get(params::SORT).and_then(Value::as_list) {
        for entry in sort.iter().rev() {
            let Some((field, dir)) = sort_entry(entry) else {
                continue;
            };
            records.sort_by(|a, b| {
                let ord = match (a.get(field), b.get(field)) {
                    (Some(x), Some(y)) => x.compare(y).unwrap_or(Ordering::Equal),
                    _ => Ordering::Equal,
                };
                if dir == Dir::Desc { ord.reverse() } else { ord }
            });
        }
    }

    // Keyset boundary applies after sorting, before offset/limit.
    if let Some(cursor) = p.get(params::CURSOR).and_then(Value::as_map) {
        if let (Some(field), Some(after), dir) = (
            cursor.get(params::CURSOR_FIELD).and_then(Value::as_str),
            cursor.get(params::CURSOR_AFTER),
            cursor
                .get(params::CURSOR_DIR)
                .and_then(Value::as_str)
                .map(Dir::from_str)
                .unwrap_or(Dir::Asc),
        ) {
            let wanted = match dir {
                Dir::Asc => Ordering::Greater,
                Dir::Desc => Ordering::Less,
            };
            records.retain(|r| {
                r.get(field)
                    .and_then(|v| v.compare(after))
                    .is_some_and(|ord| ord == wanted)
            });
        }
    }

    // The core's sort/cursor contract, applied after the filters (so it orders
    // only surviving rows) and after this layer's own `sort` param — these are
    // contract points the layer must honour, so they win over its conventions
    // rather than competing with them, exactly as `Query::limit` does below.
    records = apply_core_order(query, records);

    // The core's offset contract, applied to the core-ordered rows and before
    // any row bound: skipping must happen in the order the caller asked for,
    // and the limit bounds what is left *after* the skip.
    if let Some(offset) = query.offset {
        let offset = (offset as usize).min(records.len());
        records.drain(..offset);
    }

    if let Some(offset) = p.get(params::OFFSET).and_then(Value::as_int) {
        let offset = usize::try_from(offset).unwrap_or(0).min(records.len());
        records.drain(..offset);
    }
    if let Some(limit) = p.get(params::LIMIT).and_then(Value::as_int) {
        if let Ok(limit) = usize::try_from(limit) {
            records.truncate(limit);
        }
    }
    // The core's row bound, applied last: it is a ceiling on what may leave the
    // layer, so it wins over the layer's own `limit` param convention rather
    // than competing with it.
    if let Some(limit) = query.limit {
        records.truncate(limit as usize);
    }
    records
}

/// Apply the **core's** sort contract (`Query::sort`) and resume cursor
/// (`Query::after`). The core's `Query::offset` is applied by the caller, after
/// this — it counts rows of the order established here.
///
/// Separate from the layer's own `sort`/`cursor` param convention above and
/// applied before it: these are contract points every layer must honour, not
/// params this layer happens to understand. The domain has already checked that
/// each key names a declared attribute and that the cursor's width matches.
fn apply_core_order(query: &Query, mut records: Vec<Record>) -> Vec<Record> {
    if query.sort.is_empty() {
        return records;
    }
    // Compound order in one pass: compare key by key, first key primary, and
    // fall through to the next only on a tie.
    records.sort_by(|a, b| {
        for key in &query.sort {
            let ord = match (a.get(&key.attribute), b.get(&key.attribute)) {
                (Some(x), Some(y)) => x.compare(y).unwrap_or(Ordering::Equal),
                // An absent value sorts before a present one, consistently in
                // both directions so the order stays a total one.
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            let ord = match key.direction {
                SortDirection::Ascending => ord,
                SortDirection::Descending => ord.reverse(),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    // Keyset resume: keep only the rows that fall strictly after the cursor in
    // the order just applied — a lexicographic comparison across the same keys.
    if let Some(cursor) = &query.after {
        let bound = cursor.keys();
        debug_assert_eq!(
            bound.len(),
            query.sort.len(),
            "cursor width is validated against the sort order by the domain"
        );
        records.retain(|r| {
            for (key, after) in query.sort.iter().zip(bound) {
                let ord = match r.get(&key.attribute) {
                    Some(v) => v.compare(after).unwrap_or(Ordering::Equal),
                    None => Ordering::Less,
                };
                let ord = match key.direction {
                    SortDirection::Ascending => ord,
                    SortDirection::Descending => ord.reverse(),
                };
                match ord {
                    // Strictly past the cursor on this key: keep it.
                    Ordering::Greater => return true,
                    // Strictly before: it belongs to a previous page.
                    Ordering::Less => return false,
                    // Tied on this key — the next one decides.
                    Ordering::Equal => {}
                }
            }
            // Equal on every key: this *is* the cursor row, already returned.
            false
        });
    }
    records
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Dir {
    Asc,
    Desc,
}

impl Dir {
    fn from_str(s: &str) -> Self {
        if s.eq_ignore_ascii_case("desc") {
            Dir::Desc
        } else {
            Dir::Asc
        }
    }
}

/// Decode a `[attr, "asc"|"desc"]` sort entry.
fn sort_entry(entry: &Value) -> Option<(&str, Dir)> {
    let pair = entry.as_list()?;
    let field = pair.first()?.as_str()?;
    let dir = pair
        .get(1)
        .and_then(Value::as_str)
        .map(Dir::from_str)
        .unwrap_or(Dir::Asc);
    Some((field, dir))
}
