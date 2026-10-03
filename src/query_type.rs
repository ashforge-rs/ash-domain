//! Typed queries — a `read` reached through [`Domain::query`](crate::Domain::query).
//!
//! The ordinary read path ([`Domain::handle_action`](crate::Domain::handle_action)) returns rows of
//! the resource being read, shaped by a [`Query`](crate::Query) the core knows how
//! to translate. Sometimes you want a read whose **result is a custom shape** — a
//! projection, a join, a rolled-up aggregation that isn't any one stored resource.
//! That is what a [`TypedQuery`] is for.
//!
//! A typed query is **not a new execution path**. It is the ordinary
//! [`Read`](crate::ActionKind::Read) — same
//! [`authorize_read`](crate::Policy::authorize_read) operation gate, same
//! attribute redaction into a [`ReadReport`](crate::ReadReport) — entered through
//! a different method so it can return a **user-declared aggregation resource**
//! instead of the base resource's rows. You:
//!
//! 1. declare the aggregation as its own [`Resource`](crate::Resource) — its
//!    attributes are the projected / derived columns, and it carries a `Read`
//!    action whose policies gate and redact the result;
//! 2. implement [`TypedQuery`] for a small args type, naming that resource (and
//!    the read action to authorize under) and writing the hand-shaped read against
//!    the **concrete backend** `B`.
//!
//! Because [`run`](TypedQuery::run) is generic over the concrete backend, a typed
//! query can call whatever storage-specific method that backend exposes (a SQL
//! `JOIN`, a `GROUP BY`, an API call) — but for the same reason it **cannot** ride
//! the erased `Arc<dyn DataLayer>` the [`Domain`](crate::Domain) holds. It runs
//! against `Context<B>`'s concrete backend, alongside (not through) the erased
//! CRUD. See [`Domain::query`](crate::Domain::query).
//!
//! Two things stay the implementor's responsibility:
//!
//! - **Scoping the read.** Policies gate and redact the *result*; they do not
//!   inject a tenant / actor predicate into your hand-written read. A typed query
//!   over a tenant-scoped aggregation must filter by
//!   [`ctx.tenant()`](crate::Context::tenant) itself, exactly as a
//!   [`DataLayer::read`](crate::DataLayer::read) impl must — and must declare
//!   [`tenant_aware`](TypedQuery::tenant_aware) to assert it does. The domain
//!   **refuses to run** a query whose result resource is tenant-scoped unless that
//!   assertion is made and a tenant is set, so a typed query can never silently
//!   read across tenants (fail-closed, like every other path).
//! - **Producing well-formed rows.** Each returned [`Record`] is hydrated into the
//!   result resource's [`Data`](crate::Resource::Data), so its columns should
//!   match that resource's attributes.

use async_trait::async_trait;

use crate::context::Context;
use crate::error::Result;
use crate::value::Record;

/// A named, self-describing read whose result is a **custom resource** — declared
/// and implemented by the consumer, run through
/// [`Domain::query`](crate::Domain::query).
///
/// Implement this on a small args type (the query's inputs). The associated
/// [`Resource`](TypedQuery::Resource) is the shape it returns — a normal
/// [`Resource`](crate::Resource), typically a projection or aggregation — and is
/// what the read is **authorized and redacted as**. [`run`](TypedQuery::run)
/// performs the hand-shaped read against the context's concrete backend `B`.
///
/// ```
/// use ash_domain::{Context, Record, Result, Store, TypedQuery, Value};
///
/// # struct TodoStatsByOwner;
/// # impl ash_domain::Resource for TodoStatsByOwner {
/// #     const NAME: &'static str = "todo_stats_by_owner";
/// #     type Data = Record;
/// #     fn attributes() -> Vec<ash_domain::Attribute> { Vec::new() }
/// #     fn actions() -> Vec<ash_domain::ActionDef> { vec![ash_domain::ActionDef::read("read")] }
/// # }
/// /// A backend capability this typed query needs. A concrete layer implements it
/// /// with hand-written storage access (e.g. `SELECT owner_id, COUNT(*) … GROUP BY`).
/// #[async_trait::async_trait]
/// trait TodoStats {
///     async fn todo_stats_by_owner(&self, tenant: Option<&ash_domain::Value>) -> Result<Vec<Record>>;
/// }
///
/// /// The typed query: its inputs, the resource it returns, and how to run it.
/// struct StatsByOwner;
///
/// #[async_trait::async_trait]
/// impl<S> TypedQuery<S> for StatsByOwner
/// where
///     S: Store + TodoStats,
/// {
///     type Resource = TodoStatsByOwner;
///
///     async fn run(&self, ctx: &Context<S>) -> Result<Vec<Record>> {
///         // Scope the read yourself — policies gate/redact the result, they do
///         // not inject a tenant predicate into your hand-written query.
///         ctx.backend().todo_stats_by_owner(ctx.tenant()).await
///     }
/// }
/// ```
#[async_trait]
pub trait TypedQuery<B>: Send + Sync {
    /// The resource this query returns. Its [`Read`](crate::ActionKind::Read)
    /// policies authorize the query and redact its result; its
    /// [`Data`](crate::Resource::Data) is what
    /// [`Domain::query`](crate::Domain::query) hydrates each row into.
    type Resource: crate::Resource;

    /// The read action name to authorize the query under. Defaults to the result
    /// resource's first declared [`Read`](crate::ActionKind::Read) action (falling
    /// back to `"read"`), resolved by the domain — override only to gate a typed
    /// query under a *specific* named read action of the result resource.
    fn action(&self) -> Option<&str> {
        None
    }

    /// Whether this query scopes its own read by tenant.
    ///
    /// A typed query runs a **hand-shaped** read the domain cannot inspect, so it
    /// cannot fold a tenant predicate in the way the CRUD read path does. When the
    /// result [`Resource`](TypedQuery::Resource) declares a
    /// [`TenantStrategy`](crate::TenantStrategy), the domain therefore refuses to
    /// run the query unless it **asserts here** that its [`run`](TypedQuery::run)
    /// filters by [`ctx.tenant()`](crate::Context::tenant) — returning
    /// [`Error::MissingTenant`](crate::Error::MissingTenant) otherwise, so a
    /// tenant-scoped query can never silently read across tenants. The assertion
    /// is a promise the implementor keeps in `run`; the domain enforces that the
    /// promise was *made*, and that a tenant is actually set, but the predicate
    /// itself is still the query's to write.
    ///
    /// Defaults to `false` — fail-closed. A query over a **global** (non-tenant)
    /// result resource ignores this; the check only applies when the result
    /// resource is tenant-scoped.
    fn tenant_aware(&self) -> bool {
        false
    }

    /// Perform the hand-shaped read against the context's concrete backend `B`,
    /// returning the raw result rows. The domain authorizes **before** this runs
    /// and redacts the rows **after**, so this method only produces the data — it
    /// is responsible for scoping (tenant / actor) but not for policy. When the
    /// result resource is tenant-scoped, scope by
    /// [`ctx.tenant()`](crate::Context::tenant) and declare
    /// [`tenant_aware`](TypedQuery::tenant_aware) — otherwise the domain refuses
    /// to run the query.
    async fn run(&self, ctx: &Context<B>) -> Result<Vec<Record>>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::action::ActionDef;
    use crate::attribute::Attribute;
    use crate::context::{Context, Store};
    use crate::datalayer::{DataLayer, memory::InMemoryDataLayer};
    use crate::error::{Error, Result};
    use crate::policy::{Decision, Policy, PolicySet, ScopedPolicy};
    use crate::value::{Record, Value};
    use crate::{Domain, DomainConfig, DomainContext, Resource, erase};

    use super::TypedQuery;

    // A custom aggregation resource: the shape a typed query returns. It is a
    // normal Resource — its attributes are the projected columns, and its `read`
    // action's policies gate and redact it.
    struct OwnerStats;
    impl Resource for OwnerStats {
        const NAME: &'static str = "owner_stats";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("owner_id"),
                Attribute::scalar::<i64>("open_count"),
                // A field a policy will redact, to prove redaction runs.
                Attribute::scalar::<String>("secret"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
    }

    // A backend capability the typed query needs. A concrete layer implements it
    // with hand-shaped storage access (here, a canned aggregation).
    #[async_trait::async_trait]
    trait TodoStats {
        async fn owner_stats(&self) -> Result<Vec<Record>>;
    }

    // A concrete backend that is BOTH a Store (wrapping the in-memory layer) and
    // carries the custom `TodoStats` capability — exactly the "impl your own
    // thing" shape. It cannot ride `Arc<dyn DataLayer>`; it rides `Context<B>`.
    #[derive(Clone)]
    struct StatsBackend {
        layer: Arc<InMemoryDataLayer>,
    }
    impl Store for StatsBackend {
        fn layer(&self) -> &dyn DataLayer {
            &*self.layer
        }
    }
    #[async_trait::async_trait]
    impl TodoStats for StatsBackend {
        async fn owner_stats(&self) -> Result<Vec<Record>> {
            // A hand-shaped result that is not any one stored row.
            Ok(vec![Record::from_iter([
                ("owner_id", Value::from("u-1")),
                ("open_count", Value::Int(3)),
                ("secret", Value::from("top")),
            ])])
        }
    }

    // The typed query: its inputs (none here), the resource it returns, and how it
    // runs against the concrete backend.
    struct StatsByOwner;
    #[async_trait::async_trait]
    impl<B> TypedQuery<B> for StatsByOwner
    where
        B: Store + TodoStats,
    {
        type Resource = OwnerStats;
        async fn run(&self, ctx: &Context<B>) -> Result<Vec<Record>> {
            ctx.backend().owner_stats().await
        }
    }

    fn ctx() -> Context<StatsBackend> {
        Context::new(StatsBackend {
            layer: Arc::new(InMemoryDataLayer::new()),
        })
    }

    #[tokio::test]
    async fn typed_query_denied_by_default() {
        // No admitting policy → default-deny gate rejects the read.
        let domain = Domain::new(
            DomainConfig {
                resources: vec![erase::<OwnerStats>()],
                ..Default::default()
            },
            DomainContext::new(),
        );
        let err = domain.query(&ctx(), StatsByOwner).await.unwrap_err();
        assert!(
            matches!(err, Error::Forbidden(_)),
            "expected Forbidden, got {err:?}"
        );
    }

    #[tokio::test]
    async fn typed_query_runs_and_returns_custom_rows_when_admitted() {
        let domain = Domain::new(
            DomainConfig {
                resources: vec![erase::<OwnerStats>()],
                policies: PolicySet::permissive(),
                ..Default::default()
            },
            DomainContext::new(),
        );
        let rows = domain.query(&ctx(), StatsByOwner).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("owner_id").and_then(Value::as_str), Some("u-1"));
        assert_eq!(rows[0].get("open_count").and_then(Value::as_int), Some(3));
        // Not redacted here — no attribute policy hides `secret`.
        assert_eq!(rows[0].get("secret").and_then(Value::as_str), Some("top"));
    }

    // An attribute-read policy that always forbids `secret`. Every other Policy
    // method defaults to `NotApplicable`, so this only overrides the field read.
    struct HideSecret;
    #[async_trait::async_trait]
    impl Policy for HideSecret {
        async fn authorize_attribute_read(
            &self,
            _resource: &str,
            _action: &str,
            _attribute: &str,
            _record: &Record,
            _actor: Option<&Record>,
        ) -> Decision {
            Decision::Forbid("secret is hidden".into())
        }
    }

    #[tokio::test]
    async fn typed_query_redacts_result_attributes() {
        let policies = PolicySet::permissive().with(ScopedPolicy::attribute(
            "owner_stats",
            "secret",
            "hide",
            Arc::new(HideSecret),
        ));
        let domain = Domain::new(
            DomainConfig {
                resources: vec![erase::<OwnerStats>()],
                policies,
                ..Default::default()
            },
            DomainContext::new(),
        );

        // `query` nulls the redacted field...
        let rows = domain.query(&ctx(), StatsByOwner).await.unwrap();
        assert_eq!(rows[0].get("secret"), Some(&Value::Null));
        assert_eq!(rows[0].get("open_count").and_then(Value::as_int), Some(3));

        // ...and `query_authorized` reports which field was redacted.
        let authorized = domain.query_authorized(&ctx(), StatsByOwner).await.unwrap();
        assert_eq!(authorized.rows[0].get("secret"), Some(&Value::Null));
        assert_eq!(authorized.report.redactions.len(), 1);
        assert_eq!(authorized.report.redactions[0].attribute, "secret");
    }

    // ── tenant fail-closed matrix ─────────────────────────────────────────────
    //
    // A typed query runs a hand-shaped read the domain cannot fold a tenant
    // predicate into, so a query whose *result resource* is tenant-scoped must
    // assert (`tenant_aware`) that it scopes itself, and a tenant must be set —
    // or the domain refuses it with `MissingTenant`, never a silent cross-tenant
    // read.

    // A tenant-scoped aggregation resource (discriminator column `org_id`).
    struct TenantStats;
    impl Resource for TenantStats {
        const NAME: &'static str = "tenant_stats";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("org_id"),
                Attribute::scalar::<i64>("open_count"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
        fn tenant() -> Option<crate::TenantStrategy> {
            Some(crate::TenantStrategy::Attribute("org_id".into()))
        }
    }

    // A query over the tenant-scoped resource that DOES NOT declare tenant_aware.
    struct UnscopedStats;
    #[async_trait::async_trait]
    impl<B> TypedQuery<B> for UnscopedStats
    where
        B: Store + TodoStats,
    {
        type Resource = TenantStats;
        async fn run(&self, _ctx: &Context<B>) -> Result<Vec<Record>> {
            Ok(vec![Record::from_iter([
                ("org_id", Value::from("a")),
                ("open_count", Value::Int(1)),
            ])])
        }
    }

    // The same query, but it asserts it scopes by tenant. (The canned body here
    // ignores the tenant for brevity; the assertion is what the domain enforces.)
    struct ScopedStats;
    #[async_trait::async_trait]
    impl<B> TypedQuery<B> for ScopedStats
    where
        B: Store + TodoStats,
    {
        type Resource = TenantStats;
        fn tenant_aware(&self) -> bool {
            true
        }
        async fn run(&self, ctx: &Context<B>) -> Result<Vec<Record>> {
            // A real impl filters by this; here we just prove the tenant reaches it.
            let org = ctx
                .tenant()
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            Ok(vec![Record::from_iter([
                ("org_id", Value::from(org)),
                ("open_count", Value::Int(1)),
            ])])
        }
    }

    fn tenant_domain() -> Domain {
        Domain::new(
            DomainConfig {
                resources: vec![erase::<TenantStats>()],
                policies: PolicySet::permissive(),
                ..Default::default()
            },
            DomainContext::new(),
        )
    }

    #[tokio::test]
    async fn tenant_scoped_query_without_assertion_is_missing_tenant() {
        // Even with a tenant set, a query that never asserts it scopes itself is
        // refused — the domain cannot verify the hand-shaped read is scoped.
        let mut ctx = ctx();
        ctx.set_tenant("org-1");
        let err = tenant_domain()
            .query(&ctx, UnscopedStats)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::MissingTenant(_)),
            "expected MissingTenant, got {err:?}"
        );
    }

    #[tokio::test]
    async fn tenant_scoped_query_without_tenant_is_missing_tenant() {
        // Asserts tenant-awareness, but no tenant is set on the context → refused.
        let err = tenant_domain()
            .query(&ctx(), ScopedStats)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::MissingTenant(_)),
            "expected MissingTenant, got {err:?}"
        );
    }

    #[tokio::test]
    async fn tenant_scoped_query_runs_when_asserted_and_tenant_set() {
        // Both conditions met → the query runs and sees the tenant.
        let mut ctx = ctx();
        ctx.set_tenant("org-1");
        let rows = tenant_domain().query(&ctx, ScopedStats).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("org_id").and_then(Value::as_str), Some("org-1"));
    }

    #[tokio::test]
    async fn tenant_scoped_query_runs_cross_tenant_without_a_tenant() {
        // A sanctioned cross-tenant read: tenant_aware asserted, allow_cross_tenant
        // set, no single tenant needed — the query reads across all tenants.
        let mut ctx = ctx();
        ctx.allow_cross_tenant();
        let rows = tenant_domain().query(&ctx, ScopedStats).await.unwrap();
        assert_eq!(rows.len(), 1);
        // No tenant was set, so the query saw an empty discriminator.
        assert_eq!(rows[0].get("org_id").and_then(Value::as_str), Some(""));
    }

    #[tokio::test]
    async fn cross_tenant_does_not_excuse_a_query_from_asserting_tenant_aware() {
        // The opt-in relaxes the *tenant-set* requirement, not the assertion: a
        // query that never claims to scope itself is still refused.
        let mut ctx = ctx();
        ctx.allow_cross_tenant();
        let err = tenant_domain()
            .query(&ctx, UnscopedStats)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::MissingTenant(_)),
            "expected MissingTenant, got {err:?}"
        );
    }

    #[tokio::test]
    async fn global_query_ignores_tenant_guard() {
        // A query over a *global* result resource needs no assertion and no tenant
        // — the guard only applies to tenant-scoped result resources.
        let domain = Domain::new(
            DomainConfig {
                resources: vec![erase::<OwnerStats>()],
                policies: PolicySet::permissive(),
                ..Default::default()
            },
            DomainContext::new(),
        );
        let rows = domain.query(&ctx(), StatsByOwner).await.unwrap();
        assert_eq!(rows.len(), 1);
    }
}
