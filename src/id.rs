//! The [`IdGenerator`] seam — how a primary key is minted when an action's
//! input omits one.
//!
//! ID generation is injectable, exactly like the [`Clock`](crate::Clock) seam:
//! the [`Domain`](crate::Domain) calls its configured generator whenever a
//! `create` supplies no primary key. The default ([`DefaultIdGenerator`]) mints
//! a `"{resource}-{n}"` string — the core neither knows nor cares about UUIDs.
//! Swap in your own (a UUID/ULID generator, a seeded/deterministic one for
//! reproducible runs, snowflake ids, a database sequence, …) by setting
//! [`DomainConfig::id_generator`](crate::DomainConfig::id_generator).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::value::Value;

/// Mints primary-key values for new records whose input omitted the key.
///
/// Registered on a [`Domain`](crate::Domain) via
/// [`DomainConfig`](crate::DomainConfig) (default: [`DefaultIdGenerator`]).
/// Implement it to take full control of key generation — nothing about the core
/// is tied to any particular id shape.
pub trait IdGenerator: Send + Sync {
    /// Generate a primary-key value for a new `resource` record whose key
    /// attribute is named `pk`. Called only when the input did not supply the
    /// key, so the returned value should be unique for the resource.
    fn next_id(&self, resource: &str, pk: &str) -> Value;
}

/// The default generator: a `"{resource}-{n}"` string, where `n` is a
/// per-generator monotonic counter starting at `0`. Construct a fresh generator
/// per domain (the default) to keep sequences independent.
#[derive(Default)]
pub struct DefaultIdGenerator {
    seq: AtomicU64,
}

impl IdGenerator for DefaultIdGenerator {
    fn next_id(&self, resource: &str, _pk: &str) -> Value {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        Value::Str(format!("{resource}-{n}"))
    }
}
