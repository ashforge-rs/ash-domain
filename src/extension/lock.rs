//! The single-writer lock extension, built on [`ash_lock`].
//!
//! Attach a [`LockExtension`] to a domain to serialize concurrent writers of the
//! same record. Before an `update`, `destroy`, or generic action persists, the
//! extension acquires a per-record lease from an [`ash_lock::LockClient`] keyed
//! by `"{resource}/{primary_key}"`. The [`Domain`](crate::Domain) holds the
//! returned guard for the rest of the action and releases it when the action
//! ends — including on error (see [`ActionHold`]). `create` is deliberately not
//! locked: it mints a fresh, uncontended primary key, so there is nothing to
//! serialize against.
//!
//! The default [`ash_lock::InMemoryBackend`] gives single-process mutual
//! exclusion; point the client at an `EtcdBackend` (ash-lock's `etcd` feature)
//! for cross-process locking. Note that in-process the guard's background
//! release on drop makes this a genuine mutual-exclusion lease, not a fencing
//! scheme — cross-process fencing (rejecting stale writes at the data layer) is
//! a separate, future integration.
//!
//! ```no_run
//! # async fn ex() -> ash_domain::Result<()> {
//! use ash_domain::extension::lock::LockExtension;
//! use ash_domain::ash_lock::{LockClient, LockConfig};
//!
//! let client = LockClient::connect(LockConfig::default()).await.unwrap();
//! // Serialize writers of `account` records; lock the "id" attribute by default.
//! let ext = LockExtension::new(client).for_resource("account");
//! # let _ = ext;
//! # Ok(()) }
//! ```

use std::collections::HashSet;
use std::time::Duration;

use ash_lock::LockClient;
use async_trait::async_trait;

use crate::action::{ActionKind, Changeset};
use crate::error::Result;
use crate::extension::{ActionHold, Extension};
use crate::value::Value;

/// Serializes writers of the same record via an [`ash_lock`] lease.
///
/// See the [module docs](self) for the locking model.
pub struct LockExtension {
    client: LockClient,
    /// Resources to lock; empty means *all* resources.
    resources: HashSet<String>,
    /// The attribute whose value identifies the record (default `"id"`).
    key_attribute: String,
    /// Per-lock lease TTL; `None` uses the client's configured default.
    ttl: Option<Duration>,
}

impl LockExtension {
    /// Lock write actions on every resource, taking leases from `client`.
    pub fn new(client: LockClient) -> Self {
        Self {
            client,
            resources: HashSet::new(),
            key_attribute: "id".to_string(),
            ttl: None,
        }
    }

    /// Restrict locking to `resource`. Call repeatedly to allow several; with no
    /// call, every resource is locked.
    #[must_use]
    pub fn for_resource(mut self, resource: impl Into<String>) -> Self {
        self.resources.insert(resource.into());
        self
    }

    /// The attribute that identifies the record for the lock key (default
    /// `"id"`). Its effective value (pending change, else input, else the
    /// persisted original) becomes the key suffix.
    #[must_use]
    pub fn key_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.key_attribute = attribute.into();
        self
    }

    /// Override the lease TTL for each acquired lock (defaults to the client's
    /// configured TTL).
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Whether this extension locks `resource`.
    fn locks(&self, resource: &str) -> bool {
        self.resources.is_empty() || self.resources.contains(resource)
    }
}

#[async_trait]
impl Extension for LockExtension {
    fn name(&self) -> &str {
        "lock"
    }

    async fn acquire(&self, cs: &Changeset) -> Result<ActionHold> {
        // Only serialize writers of an *existing* record; reads never reach
        // `acquire`. An insert mints a fresh, uncontended primary key, so it
        // carries no value for the key attribute yet and falls out at the
        // key-presence check below.
        if !matches!(cs.kind, ActionKind::Write | ActionKind::Generic) {
            return Ok(ActionHold::none());
        }
        if !self.locks(&cs.resource) {
            return Ok(ActionHold::none());
        }
        // No usable key value ⇒ nothing to serialize against.
        let Some(key) = cs.attribute(&self.key_attribute).and_then(key_component) else {
            return Ok(ActionHold::none());
        };

        let resource_key = format!("{}/{}", cs.resource, key);
        let mut builder = self.client.lock(&resource_key).await;
        if let Some(ttl) = self.ttl {
            builder = builder.with_ttl(ttl);
        }
        // `ash_lock::Error` converts into `Error::Contention` via `From`.
        let guard = builder.acquire().await?;
        Ok(ActionHold::new(guard))
    }
}

/// Render a scalar key attribute as the lock-key suffix. Null or composite
/// values yield `None` — there is nothing sensible to key a record lock on.
fn key_component(v: &Value) -> Option<String> {
    match v {
        Value::Str(s) => Some(s.clone()),
        Value::Int(i) => Some(i.to_string()),
        Value::Timestamp(t) => Some(t.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null | Value::Float(_) | Value::Bytes(_) | Value::Map(_) | Value::List(_) => None,
    }
}
