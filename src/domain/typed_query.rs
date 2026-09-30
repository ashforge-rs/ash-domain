//! The typed-query path: [`Domain::query`](super::Domain::query).
//!
//! Not a new execution path — the ordinary `Read`, with the same gate and the
//! same redaction, entered through a method generic over the concrete backend
//! so it can return a user-declared aggregation resource. See
//! [`crate::query_type`].

use crate::context::{Context, Store};
use crate::error::{Error, Result};
use crate::policy::{AuthorizedRead, ReadReport};
use crate::query::Query;
use crate::resource::Resource;
use crate::value::{FromRecord, Record};

use super::Domain;

impl Domain {
    /// Run a [`TypedQuery`](crate::query_type::TypedQuery) — a `read` whose result
    /// is a **custom resource** — against the context's concrete backend, returning
    /// its rows as the query's result-[`Resource`]'s [`Data`](crate::Resource::Data).
    ///
    /// This is the ordinary [`Read`](crate::ActionKind::Read) reached through a
    /// different door: it is **authorized and redacted exactly like a direct read**
    /// of [`TypedQuery::Resource`](crate::query_type::TypedQuery::Resource), only it
    /// can return a projection / aggregation the core doesn't otherwise model. The
    /// flow is *authorize → run the query → redact*:
    ///
    /// 1. the result resource's operation-level
    ///    [`authorize_read`](crate::Policy::authorize_read) must admit under
    ///    default-deny (a denial is [`Error::Forbidden`], never surfacing the
    ///    rows), authorized under [`TypedQuery::action`](crate::query_type::TypedQuery::action)
    ///    or, by default, the resource's first declared read action;
    /// 2. the typed query's [`run`](crate::query_type::TypedQuery::run) performs the hand-shaped read
    ///    against `ctx`'s **concrete** backend `B` (which is why this takes
    ///    `Context<B>` and not the erased store the CRUD methods take);
    /// 3. the result resource's attribute policies redact forbidden fields on the
    ///    returned rows (nulled), same as any read.
    ///
    /// Unlike the CRUD reads, a typed query runs **no preparations, no read
    /// extensions, and no tenant scoping** — it is off the action pipeline. Tenant
    /// / actor scoping of the read itself is the query's own responsibility (see
    /// [`TypedQuery`](crate::query_type::TypedQuery)); the domain contributes authorization and redaction, not the
    /// `WHERE` clause. Use [`query_authorized`](Domain::query_authorized) when you
    /// also need the [`ReadReport`] of what was redacted.
    pub async fn query<Q, B>(
        &self,
        ctx: &Context<B>,
        query: Q,
    ) -> Result<Vec<<Q::Resource as Resource>::Data>>
    where
        Q: crate::query_type::TypedQuery<B>,
        B: Store,
    {
        let (rows, _) = self.run_typed_query(ctx, query).await?;
        rows.iter()
            .map(<Q::Resource as Resource>::Data::from_record)
            .collect()
    }

    /// Like [`query`](Domain::query), but also returns the [`ReadReport`] naming
    /// every attribute an attribute-read policy redacted on the result rows, so the
    /// caller can hide, mask, or annotate the nulled fields itself — the typed-query
    /// analogue of [`read_authorized`](Domain::read_authorized).
    pub async fn query_authorized<Q, B>(
        &self,
        ctx: &Context<B>,
        query: Q,
    ) -> Result<AuthorizedRead<<Q::Resource as Resource>::Data>>
    where
        Q: crate::query_type::TypedQuery<B>,
        B: Store,
    {
        let (records, report) = self.run_typed_query(ctx, query).await?;
        let rows = records
            .iter()
            .map(<Q::Resource as Resource>::Data::from_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(AuthorizedRead { rows, report })
    }

    /// Shared body of [`query`](Domain::query) / [`query_authorized`]: authorize a
    /// read of the typed query's result resource, run it against the concrete
    /// backend, then redact the rows. Returns the raw (redacted) records and the
    /// redaction report.
    pub(super) async fn run_typed_query<Q, B>(
        &self,
        ctx: &Context<B>,
        query: Q,
    ) -> Result<(Vec<Record>, ReadReport)>
    where
        Q: crate::query_type::TypedQuery<B>,
        B: Store,
    {
        let resource = <Q::Resource as Resource>::NAME;
        self.ensure_registered(resource)?;
        let actor = ctx.actor().cloned();

        // Fail-closed tenant guard. A typed query runs a hand-shaped read the
        // domain cannot fold a tenant predicate into (unlike the CRUD read path),
        // so if the result resource is tenant-scoped the query must *assert* it
        // scopes itself (`tenant_aware`) — otherwise it could silently read across
        // tenants. `tenant_aware` under `allow_cross_tenant` means "scoped to all
        // tenants"; without it, a tenant must be set. Failures are `MissingTenant`;
        // this runs before authorization but reveals nothing observable (no data,
        // no side effect), exactly like `resolve_tenant` on the CRUD read path.
        if <Q::Resource as Resource>::tenant().is_some() {
            if !query.tenant_aware() {
                return Err(Error::MissingTenant(resource.to_string()));
            }
            // A tenant is required unless this is a sanctioned cross-tenant read,
            // where reading *all* tenants is the point and no single tenant applies.
            if ctx.tenant().is_none() && !ctx.is_cross_tenant() {
                return Err(Error::MissingTenant(resource.to_string()));
            }
        }

        // Authorize as a read of the result resource, under the query's chosen
        // action (or the resource's conventional first read action). A typed query
        // carries no structured `Query`, so policies see a bare read of the
        // resource — no filter shape to row-scope against.
        let action = match query.action() {
            Some(a) => a.to_string(),
            None => self.default_read_action(resource),
        };
        let authz_query = Query::new(resource);
        self.authorize_read(resource, &action, &authz_query, actor.as_ref())
            .await?;

        // Off-pipeline: no preparations, no read extensions, no tenant folding.
        // The query itself owns its scoping; the domain owns authorization and
        // redaction.
        let mut records = query.run(ctx).await?;

        let attrs = <Q::Resource as Resource>::attributes();
        let report = self
            .redact_records(resource, &action, &attrs, actor.as_ref(), &mut records)
            .await?;
        Ok((records, report))
    }
}
