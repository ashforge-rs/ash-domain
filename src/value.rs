//! The framework's neutral data representation: [`Value`] and [`Record`].
//!
//! `ash-domain` moves domain data around as dynamic [`Record`]s (attribute name →
//! [`Value`]) rather than concrete user structs, so the core, the data layers,
//! and every extension can operate on any resource without knowing its Rust
//! type. A companion `ash-macros` crate (out of scope here) is intended to
//! derive conversions between a user struct and a `Record`.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A single attribute value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Absent / null.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 string (also used for `Uuid` attributes).
    Str(String),
    /// Opaque bytes.
    Bytes(Bytes),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    /// A nested, keyed structure — the storage form of an **embedded resource**
    /// ([`AttrType::_Embed`](crate::attribute::AttrType::_Embed)). Its keys are the
    /// embedded resource's attribute names; a data layer persists the whole thing
    /// in one column (typically as JSON).
    Map(BTreeMap<String, Value>),
    /// An ordered list of values — a repeated embedded resource
    /// ([`AttrType::_EmbedList`](crate::attribute::AttrType::_EmbedList)) is a
    /// `List` of [`Map`](Value::Map)s, and a scalar array is a `List` of
    /// primitives.
    List(Vec<Value>),
}

impl Value {
    /// Whether this value is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Borrow as a string slice, if this is a [`Value::Str`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => Some(s),
            _ => None,
        }
    }

    /// The integer, if this is a [`Value::Int`].
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Borrow the nested structure, if this is a [`Value::Map`] — the storage
    /// form of an embedded resource.
    pub fn as_map(&self) -> Option<&BTreeMap<String, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Borrow the list, if this is a [`Value::List`] — a repeated embedded
    /// resource or a scalar array.
    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(v) => Some(v),
            _ => None,
        }
    }

    /// Render this value as a self-contained ANSI-SQL literal, safe to inline
    /// into a statement.
    ///
    /// This is the escaping primitive a consumer's SQL [`DataLayer`](crate::DataLayer)
    /// uses to inline a value safely when building a statement string. It is
    /// injection-safe by construction:
    ///
    /// - `Str` is single-quoted with every embedded quote doubled (`''`), the
    ///   standard SQL escape, so no string can terminate its own literal.
    /// - `Bytes` uses the `x'…'` hex-blob form understood by SQLite, Postgres,
    ///   and MySQL.
    /// - `Int` / `Float` / `Timestamp` render as bare numerals, `Bool` as
    ///   `TRUE`/`FALSE`, and `Null` as `NULL`.
    ///
    /// A non-finite `Float` (`NaN`/`±∞`) has no portable SQL literal and renders
    /// as `NULL`.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::Int(n) | Value::Timestamp(n) => n.to_string(),
            Value::Float(f) if f.is_finite() => {
                // Force a decimal point so an integral float stays a float
                // literal (`2` → `2.0`) rather than reading as an integer.
                let s = f.to_string();
                if s.contains(['.', 'e', 'E']) {
                    s
                } else {
                    format!("{s}.0")
                }
            }
            Value::Float(_) => "NULL".to_string(),
            Value::Str(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(b) => {
                let mut hex = String::with_capacity(b.len() * 2 + 3);
                hex.push_str("x'");
                for byte in b.iter() {
                    hex.push_str(&format!("{byte:02x}"));
                }
                hex.push('\'');
                hex
            }
            // An embedded structure has no scalar SQL literal — it stores as its
            // JSON encoding, quote-escaped like any string. A layer that has a
            // native JSON column can special-case this variant instead.
            Value::Map(_) | Value::List(_) => {
                let json = serde_json::to_string(self).unwrap_or_else(|_| "null".to_string());
                format!("'{}'", json.replace('\'', "''"))
            }
        }
    }

    /// Total-ish ordering between two same-typed values, used by query
    /// filtering and sorting. Returns `None` for mismatched or incomparable
    /// variants.
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Str(a), Value::Str(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Str(v.to_owned())
    }
}
impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Str(v)
    }
}
impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int(v)
    }
}
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}
impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}
impl From<Bytes> for Value {
    fn from(v: Bytes) -> Self {
        Value::Bytes(v)
    }
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(Bytes::from(v))
    }
}
impl From<BTreeMap<String, Value>> for Value {
    fn from(v: BTreeMap<String, Value>) -> Self {
        Value::Map(v)
    }
}
/// A [`Record`] *is* a keyed structure, so it drops straight into a
/// [`Value::Map`] — how a nested embedded resource, built as a `Record`, is
/// staged onto its parent.
impl From<Record> for Value {
    fn from(v: Record) -> Self {
        Value::Map(v.0)
    }
}
impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::List(v)
    }
}

/// Extract a concrete Rust value from a [`Value`].
///
/// The inverse of the [`From<T> for Value`] impls: it is how a typed record
/// ([`FromRecord`]) reads each of its fields back out of the neutral
/// representation. Implement it for a type to make that type usable as a
/// resource field. Returns [`Error::Serialization`] on a variant mismatch.
pub trait FromValue: Sized {
    /// Convert `value` into `Self`, or fail if the variant does not match.
    fn from_value(value: &Value) -> Result<Self>;
}

fn mismatch(expected: &str, value: &Value) -> Error {
    Error::Serialization(format!("expected {expected}, got {value:?}"))
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bool(b) => Ok(*b),
            other => Err(mismatch("a boolean", other)),
        }
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            // A `Timestamp` is a tagged integer; accept it for integer fields.
            Value::Int(n) | Value::Timestamp(n) => Ok(*n),
            other => Err(mismatch("an integer", other)),
        }
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Float(f) => Ok(*f),
            other => Err(mismatch("a float", other)),
        }
    }
}

impl FromValue for String {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            // `Uuid` attributes are carried as strings, so `Str` covers both.
            Value::Str(s) => Ok(s.clone()),
            other => Err(mismatch("a string", other)),
        }
    }
}

impl FromValue for Bytes {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bytes(b) => Ok(b.clone()),
            other => Err(mismatch("bytes", other)),
        }
    }
}

impl FromValue for Vec<u8> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Bytes(b) => Ok(b.to_vec()),
            other => Err(mismatch("bytes", other)),
        }
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Null => Ok(None),
            present => Ok(Some(T::from_value(present)?)),
        }
    }
}

/// A record: an ordered map of attribute name to [`Value`].
///
/// This is the unit of data every [`DataLayer`](crate::DataLayer) stores and
/// returns, and what a [`Changeset`](crate::action::Changeset) accumulates.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Record(pub BTreeMap<String, Value>);

impl Record {
    /// An empty record.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Look up an attribute.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    /// Whether the record has a (non-null) value for `key`.
    pub fn has(&self, key: &str) -> bool {
        self.0.get(key).is_some_and(|v| !v.is_null())
    }

    /// Insert or overwrite an attribute.
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.0.insert(key.into(), value.into());
    }

    /// Iterate over the attributes.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.0.iter()
    }
}

impl<K: Into<String>, V: Into<Value>> FromIterator<(K, V)> for Record {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Record(
            iter.into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

/// Build a typed value from a [`Record`].
///
/// This is the seam that lets a [`Domain`](crate::Domain) action hand back the
/// consumer's own struct instead of a raw [`Record`]. `#[derive(Resource)]`
/// generates it field by field (via [`FromValue`]); the identity impl on
/// [`Record`] itself is what a resource keeps when it opts out of typing (its
/// `Resource::Data` is `Record`).
pub trait FromRecord: Sized {
    /// Read the fields of `record` into `Self`, or fail if a field is absent or
    /// of the wrong type.
    fn from_record(record: &Record) -> Result<Self>;
}

/// Turn a typed value into a [`Record`] — the inverse of [`FromRecord`].
///
/// **Fallible on purpose.** [`Value::Int`] is an `i64`, so a field whose Rust
/// type can exceed `i64` (a `u64`/`usize`/`isize` above [`i64::MAX`]) has no
/// lossless neutral form. Rather than silently truncate — the write path's old
/// behaviour — the derived impl returns [`Error::Serialization`] on such a value,
/// mirroring what [`FromRecord`] already does when *reading* an out-of-range
/// integer back. A conversion that cannot overflow (every other field type)
/// simply returns `Ok`.
pub trait IntoRecord {
    /// Flatten `self` into its neutral record form, or fail if a field cannot be
    /// represented losslessly (see the [trait docs](IntoRecord)).
    fn into_record(self) -> Result<Record>;
}

impl FromRecord for Record {
    fn from_record(record: &Record) -> Result<Self> {
        Ok(record.clone())
    }
}

impl IntoRecord for Record {
    fn into_record(self) -> Result<Record> {
        // A `Record` is already neutral — nothing to convert, nothing to lose.
        Ok(self)
    }
}

/// Build a [`Record`] from `key => value` pairs — the literal form of
/// [`Record::from_iter`], without its one-value-type-per-call limit.
///
/// `Record::from_iter` takes an iterator, so every pair must share a single
/// value type; mixing a `&str` and a `bool` means wrapping each value in
/// [`Value::from`] by hand. The macro inserts each pair as its own
/// [`Record::insert`] call, so any `impl Into<Value>` works per entry:
///
/// ```
/// use ash_domain::{record, Record, Value};
///
/// let rec = record! {
///     "title" => "Ship v1",
///     "done" => false,
///     "priority" => 3,
/// };
/// assert_eq!(rec.get("title"), Some(&Value::from("Ship v1")));
/// assert_eq!(rec.get("done"), Some(&Value::Bool(false)));
/// assert_eq!(rec.get("priority"), Some(&Value::Int(3)));
///
/// // Nests: a `Record` is itself `Into<Value>` (an embedded structure).
/// let customer = record! {
///     "name" => "Ada",
///     "address" => record! { "city" => "London" },
/// };
/// assert_eq!(
///     customer.get("address").and_then(Value::as_map).and_then(|m| m.get("city")),
///     Some(&Value::from("London")),
/// );
///
/// // The empty form is an empty record.
/// assert_eq!(record! {}, Record::new());
/// ```
///
/// Keys are expressions (`impl Into<String>`), so a computed key is fine; a
/// key given twice keeps the later value, as [`Record::insert`] does.
#[macro_export]
macro_rules! record {
    () => { $crate::Record::new() };
    ( $( $key:expr => $value:expr ),+ $(,)? ) => {{
        let mut record = $crate::Record::new();
        $( record.insert($key, $value); )+
        record
    }};
}

/// The stable string key a data layer uses to index a record by its primary key.
pub fn value_key(value: &Value) -> String {
    match value {
        Value::Str(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Timestamp(t) => t.to_string(),
        Value::Bytes(_) => "<bytes>".to_string(),
        Value::Null => "<null>".to_string(),
        // A structured value is never a primary key; a sentinel keeps this total.
        Value::Map(_) => "<map>".to_string(),
        Value::List(_) => "<list>".to_string(),
    }
}
