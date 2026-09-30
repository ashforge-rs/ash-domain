//! The read pipeline: the bare read, the loaded read, and relationship
//! resolution.
//!
//! Authorization runs before the data layer is touched, redaction after the
//! layer and its extensions have run, and loads/aggregates/computed fields are
//! resolved over the **already-redacted** rows. Relationship loads are batched
//! one query per level (never one per row), and each loaded relation is
//! authorized under its own destination resource.

use std::collections::HashMap;

use crate::action::ActionKind;
use crate::context::{Context, Store};
use crate::error::{Error, Result};
use crate::policy::{AuthorizedRead, Decision, ReadReport};
use crate::query::Query;
use crate::resource::{Cardinality, Relationship, Resource};
use crate::trace::{trace_event, trace_span};
use crate::value::{FromRecord, Record, Value};

use super::{Domain, find_action, reject_unresolved_read_requests};

impl Domain {
    /// Like [`read`](Domain::handle_action), but also returns the [`ReadReport`] naming
    /// every attribute an [`authorize_attribute_read`](crate::Policy::authorize_attribute_read)
    /// policy redacted (and why), so the caller can hide, mask, or annotate the
    /// nulled fields on its own terms.
    ///
    /// Like `handle_action`, this path resolves no `load`/`aggregates`/`computed`
    /// and rejects a query carrying them; use [`Domain::read`] with
    /// [`with_report`](crate::ReadRequest::with_report) to combine loads with the
    /// report.
    pub async fn read_authorized<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        query: Query,
    ) -> Result<AuthorizedRead<R::Data>> {
        reject_unresolved_read_requests(action, &query)?;
        let (records, _, report) = self.read_records::<R>(ctx, action, query).await?;
        let rows = records
            .iter()
            .map(R::Data::from_record)
            .collect::<Result<Vec<_>>>()?;
        Ok(AuthorizedRead { rows, report })
    }

    /// Execute a `read` action of `R`, resolving each relationship named in
    /// [`Query::load`] and returning every row paired with its loaded relations.
    ///
    /// Loading walks the resource's declared [`relationships`](Resource::relationships):
    /// for each requested one it gathers the source-attribute values across the
    /// result set and issues **one** follow-up read of the destination resource
    /// filtered to those values (an in-clause), then groups the matches back onto
    /// each row — so a load costs one extra query per relationship level, not one
    /// per row. The follow-up reads go through the same abstract
    /// [`DataLayer`](crate::DataLayer), so the core carries no join logic and no
    /// SQL; a data layer that can do a real join is free to honour
    /// [`Query::load`] itself instead.
    ///
    /// **Nested loads** use dotted paths — `"comments.author"` loads each row's
    /// comments and then each comment's author, attached as [`Loaded`](crate::resource::Loaded) children
    /// on the child rows (one batched query per level). A
    /// [`many_to_many`](crate::Cardinality::ManyToMany) relationship with a
    /// [`through`](crate::Relationship::through) join resource resolves in two
    /// hops. An unknown relationship name (at any level) is an error.
    ///
    /// **Authorization applies to loaded relations too.** Each follow-up read is
    /// gated exactly like a direct read of the destination resource: its
    /// operation-level [`authorize_read`](crate::Policy::authorize_read) policies
    /// must allow it (a denial fails the whole load), and its attribute policies
    /// redact forbidden fields on the loaded rows. (For a `many_to_many`, the
    /// join-table hop is an internal detail and is not separately authorized; the
    /// visible destination rows are.) The per-row redaction *report* is not
    /// surfaced for nested rows, only for the top-level result via
    /// [`read_authorized`](Domain::read_authorized).
    ///
    /// A load has **no caller-supplied action name** — the caller named an action
    /// on the *source* resource, not the destination. So loads (and aggregates,
    /// below) authorize the destination under the conventional name: its **first
    /// declared [`Read`](crate::ActionKind::Read) action**, falling back to
    /// `"read"` if it declares none. Register a matching admit there — a
    /// domain/resource-scoped `Admit`, or an action-scoped one on that read action
    /// name — to permit loading a relationship under default-deny.
    ///
    /// **Derived values are gated too.** An aggregate is authorized like a read
    /// of its destination resource (under that same first-declared-read action)
    /// *before* the layer is asked to compute it (push-down included), so a
    /// `COUNT` cannot leak what a direct read would deny. Computed fields run over
    /// the **already-redacted** rows — a [`Computer`](crate::aggregate::Computer)
    /// never sees a value an attribute policy hid — and both aggregate and
    /// computed *outputs* are themselves subject to attribute-read policies under
    /// their declared names.
    pub async fn read_loaded<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        query: Query,
    ) -> Result<Vec<crate::resource::Loaded<R::Data>>> {
        let (rows, _) = self
            .read_loaded_with_report::<R>(ctx, action, query)
            .await?;
        Ok(rows)
    }

    /// [`read_loaded`](Domain::read_loaded) plus the top-level [`ReadReport`] —
    /// the loads-and-report combination the [`ReadRequest`](crate::ReadRequest)
    /// builder surfaces. The report covers the top-level rows only; nested rows
    /// are redacted but not reported, as documented on `read_loaded`.
    pub(crate) async fn read_loaded_with_report<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        query: Query,
    ) -> Result<(Vec<crate::resource::Loaded<R::Data>>, ReadReport)> {
        // The depth bound is checked on the *requested* paths before anything
        // runs — not even the top-level read. A request that cannot be served
        // whole is refused whole: serving its first level and truncating the
        // rest would hand back a partial object graph the caller cannot tell
        // from a complete one.
        self.check_load_depth(&query.load, R::NAME)?;
        let (records, query, report) = self.read_records::<R>(ctx, action, query).await?;
        let relationships = R::relationships();
        let declared_aggs = R::aggregates();
        let declared_computed = R::computed();
        let actor = ctx.actor().cloned();

        // Resolve the requested aggregates. Each is a read of its relationship's
        // destination resource, so it is **authorized like one first** — a
        // push-down (`SELECT COUNT … GROUP BY`) must not leak what a direct read
        // of the destination would deny. Only then is it offered to the layer;
        // if the layer declines (`None`), the fallback loads the relationship's
        // rows and computes in core. Independent aggregates resolve concurrently.
        struct AggPlan {
            agg: crate::aggregate::Aggregate,
            rel: Relationship,
            /// `Some` when the layer computed it: parent-key → value.
            pushed: Option<HashMap<String, Value>>,
        }
        let mut planned: Vec<(crate::aggregate::Aggregate, Relationship)> = Vec::new();
        for name in &query.aggregate_names {
            let agg = declared_aggs
                .iter()
                .find(|a| &a.name == name)
                .cloned()
                .ok_or_else(|| {
                    Error::invalid(format!("unknown aggregate `{name}` on `{}`", R::NAME))
                })?;
            let rel = relationships
                .iter()
                .find(|r| r.name == agg.relationship)
                .cloned()
                .ok_or_else(|| {
                    Error::invalid(format!(
                        "aggregate `{name}` references unknown relationship `{}` on `{}`",
                        agg.relationship,
                        R::NAME
                    ))
                })?;
            planned.push((agg, rel));
        }
        let agg_plans: Vec<AggPlan> =
            futures::future::try_join_all(planned.into_iter().map(|(agg, rel)| {
                let actor = actor.clone();
                let records = &records;
                async move {
                    let keys = distinct_source_keys(records, &rel.source_attribute);
                    let dest_action = self.default_read_action(&rel.destination);
                    let dest_query =
                        Query::key_set(&rel.destination, &rel.destination_attribute, keys.clone());
                    self.authorize_read(
                        &rel.destination,
                        &dest_action,
                        &dest_query,
                        actor.as_ref(),
                    )
                    .await?;
                    let pushed = ctx
                        .backend()
                        .layer()
                        .aggregate(&rel.destination, &rel.destination_attribute, &keys, &agg)
                        .await?;
                    Ok::<_, Error>(AggPlan { agg, rel, pushed })
                }
            }))
            .await?;

        // Aggregate fallbacks (where the layer declined push-down) need the
        // relationship's related rows grouped by parent key. Load each such
        // relationship once, directly (single hop, no nesting).
        let mut agg_fallback: HashMap<String, HashMap<String, Vec<Record>>> = HashMap::new();
        for plan in &agg_plans {
            if plan.pushed.is_none() && !agg_fallback.contains_key(&plan.agg.relationship) {
                let grouped = self.load_relationship(ctx, &plan.rel, &records).await?;
                agg_fallback.insert(plan.agg.relationship.clone(), grouped);
            }
        }

        // Validate computed-field names once, up front.
        let computed: Vec<crate::aggregate::Computed> = query
            .computed
            .iter()
            .map(|name| {
                declared_computed
                    .iter()
                    .find(|c| &c.name == name)
                    .cloned()
                    .ok_or_else(|| {
                        Error::invalid(format!("unknown computed field `{name}` on `{}`", R::NAME))
                    })
            })
            .collect::<Result<_>>()?;

        // Resolve the (possibly nested) load paths into per-parent `Loaded`
        // children. This recurses through the relationship graph, loading the
        // relationships of each level concurrently.
        let load_paths = parse_load_paths(&query.load);
        let related_by_index = self
            .resolve_loads(ctx, R::NAME, &records, &load_paths)
            .await?;

        // Computed fields derive from the record **as already redacted** by
        // attribute policies (see `read_records`): a `Computer` can never see a
        // value the actor may not read, so a derived field cannot smuggle a
        // forbidden attribute out. Rows compute concurrently.
        let mut computed_by_index: Vec<HashMap<String, Value>> =
            futures::future::try_join_all(records.iter().map(|record| {
                let computed = &computed;
                async move {
                    let mut values = HashMap::with_capacity(computed.len());
                    for c in computed {
                        values.insert(c.name.clone(), c.computer.compute(record).await?);
                    }
                    Ok::<_, Error>(values)
                }
            }))
            .await?;

        let gate_derived = self.policies.has_attribute_policies();
        let mut out = Vec::with_capacity(records.len());
        for (i, record) in records.iter().enumerate() {
            let related = related_by_index[i].clone();

            // Aggregates: the layer's pushed-down scalar if any, else roll up the
            // fallback-loaded related rows for this parent key.
            let mut agg_values: HashMap<String, Value> = HashMap::new();
            for plan in &agg_plans {
                let key = record
                    .get(&plan.rel.source_attribute)
                    .map(crate::value::value_key);
                let value = match &plan.pushed {
                    Some(by_key) => key
                        .and_then(|k| by_key.get(&k).cloned())
                        .unwrap_or_else(|| plan.agg.compute(&[])),
                    None => {
                        let rows = key
                            .and_then(|k| {
                                agg_fallback
                                    .get(&plan.agg.relationship)
                                    .and_then(|g| g.get(&k))
                            })
                            .map(Vec::as_slice)
                            .unwrap_or(&[]);
                        plan.agg.compute(rows)
                    }
                };
                agg_values.insert(plan.agg.name.clone(), value);
            }

            let mut computed_values = std::mem::take(&mut computed_by_index[i]);

            // Derived values are governed like attributes: an attribute-read
            // policy scoped to the aggregate's or computed field's *name* redacts
            // it per row, and a policy-backend error fails the read closed.
            if gate_derived {
                for (name, value) in agg_values.iter_mut().chain(computed_values.iter_mut()) {
                    match self
                        .policies
                        .authorize_attribute_read(R::NAME, action, name, record, actor.as_ref())
                        .await
                    {
                        // Visible-unless-forbidden, same as stored attributes.
                        Decision::Allow | Decision::NotApplicable => {}
                        Decision::Forbid(_) => *value = Value::Null,
                        Decision::Error(msg) => return Err(Error::PolicyError(msg)),
                    }
                }
            }

            out.push(crate::resource::Loaded {
                row: R::Data::from_record(record)?,
                related,
                aggregates: agg_values,
                computed: computed_values,
            });
        }
        Ok((out, report))
    }

    /// The shared read pipeline: resolve the action, scope by tenant, run
    /// preparations, **authorize**, then run read extensions, hit the layer, and
    /// run after-read extensions. Returns the raw records and the final
    /// [`Query`] (carrying the caller's `load` / `aggregates` / `computed`
    /// requests). Crate-visible so the [`ReadRequest`](crate::ReadRequest)
    /// builder can drive the same pipeline.
    pub(crate) async fn read_records<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        mut query: Query,
    ) -> Result<(Vec<Record>, Query, ReadReport)> {
        self.ensure_registered(R::NAME)?;
        let actions = R::actions();
        let def = find_action(&actions, R::NAME, action, ActionKind::Read)?;
        query.resource = R::NAME.to_string();
        let actor = ctx.actor().cloned();
        // Shape checks on the request itself, before anything observable: these
        // reject a malformed query no matter who asked, so they leak nothing a
        // denied caller could not already work out from the API surface.
        validate_sort(&query, &R::attributes(), R::NAME)?;

        // A read span of its own: `read_records` is reached both through
        // `handle_action` (nesting under its span) and directly via
        // `read`/`read_authorized`/`read_loaded`, so it must carry a span either
        // way. Structure only — the param bag is never captured.
        let _span = trace_span!(
            &self.trace_gate,
            "read",
            resource = R::NAME,
            action = action
        );

        let tenant_strategy = R::tenant();
        // A read may opt out of tenant scoping (a sanctioned cross-tenant read);
        // writes never can — hence a read-specific scoping path, not `resolve_tenant`.
        self.scope_read_by_tenant(
            &mut query,
            tenant_strategy.as_ref(),
            R::NAME,
            ctx.tenant(),
            ctx.is_cross_tenant(),
        )?;

        for prep in &def.preparations {
            prep.prepare(&mut query).await?;
        }
        trace_event!(&self.trace_gate, "prepared");
        // Authorize before anything observable runs: policies judge the query as
        // the caller (plus the action's own preparations) shaped it; extensions
        // never see a denied read.
        if let Err(e) = self
            .authorize_read(R::NAME, action, &query, actor.as_ref())
            .await
        {
            trace_event!(&self.trace_gate, "denied", reason = %e);
            return Err(e);
        }
        trace_event!(&self.trace_gate, "authorized");
        for ext in self.extensions.iter() {
            ext.before_read(R::NAME, action, &mut query, actor.as_ref())
                .await?;
        }

        // Resolve an unresolved relationship filter (`reserved::RELATES`) into a
        // plain key-set (`reserved::IN`) over this resource's join key, by reading
        // the related keys through the layer. A layer that prefers a native JOIN
        // could read the `RELATES` param itself, but resolving here keeps it
        // working everywhere.
        if let Some((relationship, predicate)) = query.relates_filter() {
            let relationship = relationship.to_string();
            let relationships = R::relationships();
            let (attr, keys) = self
                .resolve_relation_filter(ctx, R::NAME, &relationships, &relationship, predicate)
                .await?;
            query.params.0.remove(crate::query::reserved::RELATES);
            // Reuse the canonical key-set encoding, then lift its `IN` param.
            // Empty `keys` is significant: it makes the read return nothing.
            let encoded = Query::key_set(R::NAME, attr, keys);
            if let Some(v) = encoded.params.get(crate::query::reserved::IN).cloned() {
                query.params.insert(crate::query::reserved::IN, v);
            }
        }

        let mut records = self.execute_query(ctx, &query).await?;
        trace_event!(&self.trace_gate, "read", rows = records.len());
        for ext in self.extensions.iter() {
            ext.after_read(R::NAME, action, &mut records, actor.as_ref())
                .await?;
        }

        // Field-level authorization: null out any attribute a policy forbids the
        // actor from seeing, and record what was hidden. Runs after extensions
        // so it has the final row values.
        let attrs = R::attributes();
        let report = self
            .redact_records(R::NAME, action, &attrs, actor.as_ref(), &mut records)
            .await?;
        trace_event!(
            &self.trace_gate,
            "redacted",
            hidden = report.redactions.len()
        );
        Ok((records, query, report))
    }

    /// Run `query` against the context's data layer.
    ///
    /// The layer owns the interpretation of the [`Query`] param bag entirely; the
    /// core neither evaluates queries nor negotiates capabilities. A layer that
    /// cannot execute part of the bag surfaces that itself (see
    /// [`DataLayer::read`](crate::datalayer::DataLayer::read)).
    pub(super) async fn execute_query(
        &self,
        ctx: &Context<impl Store>,
        query: &Query,
    ) -> Result<Vec<Record>> {
        // A caller-set bound is deliberate: pass it down untouched and hold the
        // layer to it. An unbounded read gets the domain's ceiling instead — see
        // `read_within_ceiling`.
        let Some(limit) = query.limit else {
            return self.read_within_ceiling(ctx, query).await;
        };
        let rows = ctx.backend().layer().read(query).await?;
        check_row_bound(&rows, limit, &query.resource)?;
        Ok(rows)
    }

    /// Run a read that carries no bound of its own under
    /// [`max_rows`](DomainConfig::max_rows).
    ///
    /// The ceiling is applied by asking the layer for **one row more** than it
    /// allows: if that extra row comes back, the result set genuinely exceeded
    /// the ceiling and the read is refused. Truncating to the ceiling instead
    /// would hand the caller a silently partial answer — the one thing this
    /// crate never does with a degraded read.
    async fn read_within_ceiling(
        &self,
        ctx: &Context<impl Store>,
        query: &Query,
    ) -> Result<Vec<Record>> {
        // No ceiling (the explicit `unbounded_rows` opt-out), or a ceiling so
        // large the probe would overflow: nothing to bound with.
        let probe = self
            .max_rows
            .and_then(|ceiling| ceiling.get().checked_add(1).map(|probe| (ceiling, probe)));
        let Some((ceiling, probe)) = probe else {
            return ctx.backend().layer().read(query).await;
        };

        let mut bounded = query.clone();
        bounded.limit = Some(probe);
        let rows = ctx.backend().layer().read(&bounded).await?;
        check_row_bound(&rows, probe, &query.resource)?;
        if rows.len() > ceiling.get() as usize {
            return Err(Error::Unsupported(format!(
                "read of `{}` exceeds the domain's max_rows ceiling of {ceiling}: \
                 set an explicit Query::limit, page the read, or raise the ceiling \
                 (DomainBuilder::max_rows / unbounded_rows)",
                query.resource
            )));
        }
        Ok(rows)
    }

    /// Resolve an unresolved relationship filter (`reserved::RELATES`) into the
    /// source resource's key-set: read the destination resource under the inner
    /// `predicate` bag, collect the matching destination-attribute keys, and map
    /// them back to the source join attribute. Returns `(source_attribute, keys)`
    /// — an **empty** `keys` is significant (no destination row matched), so the
    /// caller still writes the resulting `IN` param and the read returns nothing.
    ///
    /// The predicate bag may itself carry a nested `reserved::RELATES` (a
    /// multi-hop filter such as `posts` filtered by `author.name`); it is resolved
    /// against the destination resource's relationships first, so the bounded
    /// recursion follows the relationship graph, not caller-shaped data.
    pub(super) fn resolve_relation_filter<'a>(
        &'a self,
        ctx: &'a Context<impl Store>,
        resource: &'a str,
        relationships: &'a [Relationship],
        relationship: &'a str,
        predicate: Record,
    ) -> RelationKeySetFut<'a> {
        Box::pin(async move {
            let rel = relationships
                .iter()
                .find(|r| r.name == relationship)
                .ok_or_else(|| {
                    Error::invalid(format!(
                        "unknown relationship `{relationship}` in filter on `{resource}`"
                    ))
                })?;

            // Build the destination read from the predicate bag, resolving a
            // nested relationship filter into a key-set first if present.
            let mut dest_query = Query {
                params: predicate,
                ..Query::new(&rel.destination)
            };
            if let Some((inner_rel, inner_pred)) = dest_query.relates_filter() {
                let inner_rel = inner_rel.to_string();
                let dest_relationships = self
                    .resource(&rel.destination)
                    .map(|r| r.relationships())
                    .unwrap_or_default();
                let (attr, keys) = self
                    .resolve_relation_filter(
                        ctx,
                        &rel.destination,
                        &dest_relationships,
                        &inner_rel,
                        inner_pred,
                    )
                    .await?;
                dest_query.params.0.remove(crate::query::reserved::RELATES);
                let encoded = Query::key_set(&rel.destination, attr, keys);
                if let Some(v) = encoded.params.get(crate::query::reserved::IN).cloned() {
                    dest_query.params.insert(crate::query::reserved::IN, v);
                }
            }

            let dest_rows = self.execute_query(ctx, &dest_query).await?;
            let mut keys = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for row in &dest_rows {
                if let Some(v) = row.get(&rel.destination_attribute) {
                    if seen.insert(crate::value::value_key(v)) {
                        keys.push(v.clone());
                    }
                }
            }
            Ok((rel.source_attribute.clone(), keys))
        })
    }

    /// Resolve one relationship for a set of parent records, grouped by the
    /// parent's source-attribute key. A direct relationship is one follow-up
    /// read; a [`ManyToMany`](Cardinality::ManyToMany) with a
    /// [`through`](Relationship::through) join resource is two hops.
    pub(super) async fn load_relationship(
        &self,
        ctx: &Context<impl Store>,
        rel: &Relationship,
        parents: &[Record],
    ) -> Result<HashMap<String, Vec<Record>>> {
        // The distinct source values to match against (skip nulls/absent).
        let values = distinct_source_keys(parents, &rel.source_attribute);
        if values.is_empty() {
            return Ok(HashMap::new());
        }

        if let Some(through) = &rel.through {
            return self.load_through(ctx, rel, through, values).await;
        }

        let query = Query::key_set(&rel.destination, &rel.destination_attribute, values);
        let mut related = self.execute_query(ctx, &query).await?;

        // A loaded relation is a read of `rel.destination`; gate it like one,
        // passing the same key-set query so a row-scoping policy sees it.
        self.authorize_loaded_rows(ctx, &rel.destination, &query, &mut related)
            .await?;

        let mut grouped: HashMap<String, Vec<Record>> = HashMap::new();
        for record in related {
            if let Some(key) = record
                .get(&rel.destination_attribute)
                .map(crate::value::value_key)
            {
                grouped.entry(key).or_default().push(record);
            }
        }
        // `belongs_to`/`has_one` keep at most one match; `has_many` keeps all.
        if matches!(
            rel.cardinality,
            Cardinality::BelongsTo | Cardinality::HasOne
        ) {
            for records in grouped.values_mut() {
                records.truncate(1);
            }
        }
        Ok(grouped)
    }

    /// Resolve a `many_to_many` relationship in two hops: read the join
    /// resource for the parent keys, then read the destination resource for the
    /// keys those join rows point at, and group destination rows back under each
    /// parent key by walking the join mapping.
    pub(super) async fn load_through(
        &self,
        ctx: &Context<impl Store>,
        rel: &Relationship,
        through: &crate::resource::Through,
        source_values: Vec<Value>,
    ) -> Result<HashMap<String, Vec<Record>>> {
        // Hop 1: join rows whose source key is one of the parents'.
        let join_query =
            Query::key_set(&through.resource, &through.source_attribute, source_values);
        let join_rows = self.execute_query(ctx, &join_query).await?;
        if join_rows.is_empty() {
            return Ok(HashMap::new());
        }

        // The destination keys to fetch, and the parent-key → destination-keys map.
        let dest_keys = distinct_source_keys(&join_rows, &through.destination_attribute);
        if dest_keys.is_empty() {
            return Ok(HashMap::new());
        }

        // Hop 2: the destination rows, indexed by their join key.
        let dest_query = Query::key_set(&rel.destination, &rel.destination_attribute, dest_keys);
        let mut dest_rows = self.execute_query(ctx, &dest_query).await?;

        // The destination rows are what the caller sees; gate them like a read
        // of `rel.destination`. (The join-table hop above stays an internal
        // detail and is not separately authorized.)
        self.authorize_loaded_rows(ctx, &rel.destination, &dest_query, &mut dest_rows)
            .await?;

        let mut dest_by_key: HashMap<String, Record> = HashMap::new();
        for row in dest_rows {
            if let Some(k) = row
                .get(&rel.destination_attribute)
                .map(crate::value::value_key)
            {
                dest_by_key.insert(k, row);
            }
        }

        // Walk the join rows, attaching each destination row to its parent key.
        let mut grouped: HashMap<String, Vec<Record>> = HashMap::new();
        for join in &join_rows {
            let parent_key = join
                .get(&through.source_attribute)
                .map(crate::value::value_key);
            let dest_key = join
                .get(&through.destination_attribute)
                .map(crate::value::value_key);
            if let (Some(pk), Some(dk)) = (parent_key, dest_key) {
                if let Some(dest) = dest_by_key.get(&dk) {
                    grouped.entry(pk).or_default().push(dest.clone());
                }
            }
        }
        Ok(grouped)
    }

    /// Refuse a load path deeper than
    /// [`DomainConfig::max_load_depth`](crate::DomainConfig::max_load_depth),
    /// before any nested read runs.
    ///
    /// Depth is relationship hops: `"comments"` is 1, `"comments.author"` is 2.
    /// Each extra hop is another round of reads fanning out over the rows the
    /// hop above returned, so the cost is multiplicative — a bound a row limit
    /// cannot express. Caller-supplied, so this is a `Result`, never an
    /// assertion.
    pub(super) fn check_load_depth(&self, paths: &[String], resource: &str) -> Result<()> {
        let ceiling = self.max_load_depth.get();
        for path in paths {
            // Hops = separators + 1. Counted on the raw path, so the check is
            // independent of how `parse_load_paths` groups them.
            let depth = path.split('.').count();
            let Ok(depth) = u32::try_from(depth) else {
                return Err(Error::invalid(format!(
                    "load path `{path}` on `{resource}` is absurdly deep"
                )));
            };
            if depth > ceiling {
                return Err(Error::invalid(format!(
                    "load path `{path}` on `{resource}` is {depth} levels deep, exceeding the \
                     domain's max_load_depth of {ceiling}: load fewer levels per request, or \
                     raise the ceiling (DomainBuilder::max_load_depth)"
                )));
            }
        }
        Ok(())
    }

    /// Recursively resolve load paths for a set of parent records, returning one
    /// map (relationship name → loaded children) per parent, in parent order.
    /// Each child is itself a [`Loaded`](crate::resource::Loaded) so its own
    /// nested loads are attached.
    ///
    /// The relationships at one level are independent reads, so they resolve
    /// **concurrently** — the context is only ever borrowed shared on the read
    /// path, which is what makes the join possible.
    pub(super) fn resolve_loads<'a>(
        &'a self,
        ctx: &'a Context<impl Store>,
        resource: &'a str,
        parents: &'a [Record],
        loads: &'a LoadTree,
    ) -> ResolveLoadsFut<'a> {
        Box::pin(async move {
            // Start each parent with an empty related-map.
            let mut result: Vec<RelatedMap> = parents.iter().map(|_| HashMap::new()).collect();
            if loads.is_empty() {
                return Ok(result);
            }
            // The recursion is bounded by `check_load_depth`, run once on the
            // caller's paths at the entry point: each level here consumes one
            // segment, so the remaining sub-paths shrink monotonically and the
            // depth already checked caps the whole tree. This asserts the
            // invariant rather than re-deriving the bound at every level.
            debug_assert!(
                loads.iter().all(|(_, sub)| sub.iter().all(|p| {
                    u32::try_from(p.split('.').count()).is_ok_and(|d| d < self.max_load_depth.get())
                })),
                "sub-paths must be shallower than the checked ceiling"
            );

            let relationships = self
                .resource(resource)
                .map(|r| r.relationships())
                .unwrap_or_default();

            // Fetch and recurse every requested relationship concurrently; each
            // yields its grouped children plus their own resolved sub-loads.
            let loaded = futures::future::try_join_all(loads.iter().map(|(name, sub_paths)| {
                let relationships = &relationships;
                async move {
                    let rel = relationships
                        .iter()
                        .find(|r| &r.name == name)
                        .cloned()
                        .ok_or_else(|| {
                            Error::invalid(format!("unknown relationship `{name}` on `{resource}`"))
                        })?;

                    // One grouped fetch for this relationship across all parents.
                    let grouped = self.load_relationship(ctx, &rel, parents).await?;

                    // Flatten the distinct child records to recurse their
                    // sub-loads in a single batch, then redistribute by identity.
                    let sub_tree = parse_load_paths(sub_paths);
                    let mut child_records: Vec<Record> = Vec::new();
                    for records in grouped.values() {
                        child_records.extend(records.iter().cloned());
                    }
                    let child_related = self
                        .resolve_loads(ctx, &rel.destination, &child_records, &sub_tree)
                        .await?;
                    Ok::<_, Error>((name, rel, grouped, child_related))
                }
            }))
            .await?;

            for (name, rel, grouped, child_related) in loaded {
                // Map each flattened child back to its loaded form by position.
                let mut child_iter = child_related.into_iter();
                let mut loaded_by_parent_key: RelatedMap = HashMap::new();
                for (pk, records) in &grouped {
                    let mut loaded_children = Vec::with_capacity(records.len());
                    for record in records {
                        let related = child_iter.next().unwrap_or_default();
                        loaded_children.push(crate::resource::Loaded {
                            row: record.clone(),
                            related,
                            aggregates: HashMap::new(),
                            computed: HashMap::new(),
                        });
                    }
                    loaded_by_parent_key.insert(pk.clone(), loaded_children);
                }

                // Attach to each parent by its source key.
                for (i, parent) in parents.iter().enumerate() {
                    let key = parent
                        .get(&rel.source_attribute)
                        .map(crate::value::value_key);
                    let children = key
                        .and_then(|k| loaded_by_parent_key.get(&k))
                        .cloned()
                        .unwrap_or_default();
                    result[i].insert(name.clone(), children);
                }
            }
            Ok(result)
        })
    }

    /// The **conventional read action** of `resource`: its first declared
    /// [`Read`](ActionKind::Read) action, or `"read"` if it declares none. Used
    /// wherever a read happens without a caller-supplied action name — a
    /// relationship load, a typed query's default, and the
    /// [`ReadRequest`](crate::ReadRequest) builder's default — so policies have
    /// one conventional name to match against.
    pub(crate) fn default_read_action(&self, resource: &str) -> String {
        self.resource(resource)
            .and_then(|r| {
                r.actions()
                    .into_iter()
                    .find(|a| a.kind == ActionKind::Read)
                    .map(|a| a.name)
            })
            .unwrap_or_else(|| "read".to_string())
    }
}

/// A parsed load tree: relationship name → the sub-paths to load beneath it.
/// `["comments.author", "comments.likes", "tags"]` becomes
/// `{ "comments": ["author", "likes"], "tags": [] }`.
pub(super) type LoadTree = Vec<(String, Vec<String>)>;

/// One parent record's loaded relations: relationship name → its loaded rows
/// (each itself a [`Loaded`](crate::resource::Loaded) carrying nested loads).
pub(super) type RelatedMap = HashMap<String, Vec<crate::resource::Loaded<Record>>>;

/// The pinned, boxed future [`Domain::resolve_loads`] returns — one
/// [`RelatedMap`] per parent record, in parent order.
pub(super) type ResolveLoadsFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<RelatedMap>>> + Send + 'a>>;

/// A resolved relationship-filter key-set: `(source_attribute, keys)`. The future
/// is boxed because [`Domain::resolve_relation_filter`] recurses for multi-hop
/// relationship filters.
pub(super) type RelationKeySetFut<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(String, Vec<Value>)>> + Send + 'a>>;

/// Group dotted load paths by their first segment, preserving order and keeping
/// each relationship's remaining sub-paths for the next level of recursion.
pub(super) fn parse_load_paths(paths: &[String]) -> LoadTree {
    let mut tree: LoadTree = Vec::new();
    for path in paths {
        let (head, rest) = match path.split_once('.') {
            Some((h, r)) => (h.to_string(), Some(r.to_string())),
            None => (path.clone(), None),
        };
        let entry = match tree.iter_mut().find(|(name, _)| name == &head) {
            Some(e) => e,
            None => {
                tree.push((head, Vec::new()));
                tree.last_mut().unwrap()
            }
        };
        if let Some(rest) = rest {
            entry.1.push(rest);
        }
    }
    tree
}

/// The distinct, non-null values of `attribute` across `records`, preserving
/// first-seen order. The join-key set for loading or aggregating a relationship.
pub(super) fn distinct_source_keys(records: &[Record], attribute: &str) -> Vec<Value> {
    let mut seen = std::collections::HashSet::new();
    let mut values = Vec::new();
    for record in records {
        if let Some(v) = record.get(attribute) {
            if !v.is_null() && seen.insert(crate::value::value_key(v)) {
                values.push(v.clone());
            }
        }
    }
    values
}

/// Fold an `attr == value` constraint into a query bag's `eq` param (the
/// reference layer's equality convention), merging into any existing `eq` map.
/// Used by tenant scoping under the `Attribute` strategy.
pub(super) fn fold_eq_param(params: &mut Record, attr: &str, value: Value) {
    use crate::datalayer::memory::params::EQ;
    let mut map = params
        .get(EQ)
        .and_then(Value::as_map)
        .cloned()
        .unwrap_or_default();
    map.insert(attr.to_string(), value);
    params.insert(EQ, Value::Map(map));
}

/// Hold a layer to the row bound it was handed.
///
/// The bound is a contract point, not a hint (see [`Query::limit`]): a layer
/// that returns more rows than it was allowed has broken it, and the domain says
/// so rather than truncating — a silent truncation here would turn a broken
/// layer into a wrong answer.
/// Check a read's sort order and paging position before the layer sees them.
///
/// Everything here is the caller's mistake rather than a storage failure —
/// hence [`Error::Invalid`], not `Unsupported`:
///
/// * every sort key names a **declared attribute** of the resource, so a typo
///   fails here instead of reaching the layer as an unknown column;
/// * a read carries **at most one** paging position — a keyset cursor
///   ([`Query::after`]) or an [`offset`](Query::offset), never both, since they
///   describe two different starting points and no layer can honour both;
/// * a paging position is accompanied by the sort order it is a position *in* —
///   an unordered read has no meaningful "after here" and no meaningful "skip
///   the first n"; and
/// * a resume [`Cursor`](crate::query::Cursor) carries **one value per sort
///   key** — a cursor whose width disagrees with the order describes a position
///   in some *other* query, and resuming from it would silently skip or repeat
///   rows.
fn validate_sort(
    query: &Query,
    attrs: &[crate::attribute::Attribute],
    resource: &str,
) -> Result<()> {
    for key in &query.sort {
        if !attrs.iter().any(|a| a.name == key.attribute) {
            return Err(Error::invalid(format!(
                "cannot sort `{resource}` by `{}`: no such attribute",
                key.attribute
            )));
        }
    }
    if query.after.is_some() && query.offset.is_some() {
        return Err(Error::invalid(format!(
            "this read of `{resource}` carries both a resume cursor and an offset: \
             they are two different positions in the result set — keep the cursor \
             for forward paging, or the offset for random access, not both"
        )));
    }
    if query.offset.is_some() && query.sort.is_empty() {
        return Err(Error::invalid(format!(
            "an offset was supplied for a read of `{resource}` with no sort order: \
             skipping rows of an unordered read skips arbitrary rows (add the sort \
             the offset counts against)"
        )));
    }
    let Some(cursor) = &query.after else {
        return Ok(());
    };
    if query.sort.is_empty() {
        return Err(Error::invalid(format!(
            "a cursor was supplied for a read of `{resource}` with no sort order: \
             there is no order to resume from (add the sort the cursor came from)"
        )));
    }
    if cursor.keys().len() != query.sort.len() {
        return Err(Error::invalid(format!(
            "the cursor for this read of `{resource}` carries {} key(s) but the sort \
             order has {}: the cursor belongs to a differently-sorted query",
            cursor.keys().len(),
            query.sort.len()
        )));
    }
    Ok(())
}

fn check_row_bound(rows: &[Record], limit: u32, resource: &str) -> Result<()> {
    if rows.len() as u64 > u64::from(limit) {
        return Err(Error::DataLayer {
            message: format!(
                "data layer returned {} rows for a read of `{resource}` bounded to {limit}",
                rows.len()
            ),
            source: None,
        });
    }
    Ok(())
}
