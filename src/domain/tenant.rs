//! Tenant scoping: resolving the discriminator, folding it into a read, and
//! refusing a cross-tenant write.
//!
//! Every method here fails closed. A resource that declares a
//! [`TenantStrategy`] and reaches a path with no tenant set is
//! [`Error::MissingTenant`], never an unscoped query; a record whose
//! discriminator does not match the context's tenant is [`Error::NotFound`],
//! never someone else's row.

use crate::action::Changeset;
use crate::error::{Error, Result};
use crate::query::Query;
use crate::resource::TenantStrategy;
use crate::value::{Record, Value};

use super::Domain;
use super::read::fold_eq_param;

impl Domain {
    // ── multitenancy helpers ──────────────────────────────────────────────

    /// Resolve the tenant for an action on `resource`: `Ok(None)` for a global
    /// resource, `Ok(Some(t))` when the resource is tenant-scoped and a tenant is
    /// set, and [`Error::MissingTenant`] when it is scoped but none is set.
    pub(super) fn resolve_tenant(
        &self,
        strategy: Option<&TenantStrategy>,
        resource: &str,
        ctx_tenant: Option<&Value>,
    ) -> Result<Option<Value>> {
        match strategy {
            None => Ok(None),
            Some(_) => ctx_tenant
                .cloned()
                .map(Some)
                .ok_or_else(|| Error::MissingTenant(resource.to_string())),
        }
    }

    /// Scope a **read** by tenant, honoring the context's cross-tenant opt-in.
    ///
    /// For a global resource (`strategy` is `None`) this is a no-op. For a
    /// tenant-scoped resource:
    ///
    /// * `cross_tenant == false` (the default) → resolve the tenant fail-closed
    ///   ([`MissingTenant`](Error::MissingTenant) if none is set) and fold its
    ///   predicate in, exactly as a write would.
    /// * `cross_tenant == true` → drop the tenant predicate and mark the query
    ///   [`across_tenants`](Query::across_tenants), so a partitioning layer reads
    ///   every partition and audit sees the cross-tenant read. No tenant need be
    ///   set — reading *all* tenants is precisely the point.
    ///
    /// Writes never reach this; they use [`resolve_tenant`](Domain::resolve_tenant)
    /// and stay fail-closed regardless of the flag.
    pub(super) fn scope_read_by_tenant(
        &self,
        query: &mut Query,
        strategy: Option<&TenantStrategy>,
        resource: &str,
        ctx_tenant: Option<&Value>,
        cross_tenant: bool,
    ) -> Result<()> {
        if strategy.is_none() {
            return Ok(());
        }
        if cross_tenant {
            // Sanctioned global read: no predicate, marked for the layer/audit.
            query.across_tenants = true;
            return Ok(());
        }
        let tenant = self.resolve_tenant(strategy, resource, ctx_tenant)?;
        self.scope_query_by_tenant(query, strategy, tenant.as_ref());
        Ok(())
    }

    /// Fold a tenant discriminator predicate into a query's filter, for the
    /// [`Attribute`](TenantStrategy::Attribute) strategy. Also records the tenant
    /// on the query so a partitioning layer can read it.
    pub(super) fn scope_query_by_tenant(
        &self,
        query: &mut Query,
        strategy: Option<&TenantStrategy>,
        tenant: Option<&Value>,
    ) {
        let Some(tenant) = tenant else { return };
        query.tenant = Some(tenant.clone());
        // Under the `Attribute` strategy the discriminator must actually filter
        // rows. There is no core filter language, so the domain folds it into the
        // reference layer's `eq` convention (an attribute→value map). A layer that
        // partitions by tenant instead reads `query.tenant` directly and can
        // ignore this; a layer that honours `eq` gets tenant isolation for free.
        if let Some(attr) = strategy.and_then(TenantStrategy::attribute) {
            fold_eq_param(&mut query.params, attr, tenant.clone());
        }
    }

    /// Confirm a fetched record belongs to `tenant` under the
    /// [`Attribute`](TenantStrategy::Attribute) strategy; a mismatch is reported
    /// as [`Error::NotFound`] so cross-tenant access is indistinguishable from a
    /// missing record. A no-op for the [`Layer`](TenantStrategy::Layer) strategy
    /// (the layer already isolated it) and for global resources.
    pub(super) fn check_record_tenant(
        &self,
        record: &Record,
        resource: &str,
        strategy: Option<&TenantStrategy>,
        tenant: Option<&Value>,
        id: &Value,
    ) -> Result<()> {
        let (Some(attr), Some(tenant)) = (strategy.and_then(TenantStrategy::attribute), tenant)
        else {
            return Ok(());
        };
        if record.get(attr) == Some(tenant) {
            Ok(())
        } else {
            Err(Error::NotFound(format!("{resource}/{id:?}")))
        }
    }

    /// Stamp the tenant discriminator onto a create's staged data, for the
    /// [`Attribute`](TenantStrategy::Attribute) strategy. The stamp is
    /// authoritative — it overwrites any tenant value the caller supplied, so a
    /// record can never be created into another tenant.
    pub(super) fn stamp_tenant(
        &self,
        cs: &mut Changeset,
        strategy: Option<&TenantStrategy>,
        tenant: Option<&Value>,
    ) {
        if let (Some(attr), Some(tenant)) = (strategy.and_then(TenantStrategy::attribute), tenant) {
            cs.data.insert(attr.to_string(), tenant.clone());
        }
    }
}
