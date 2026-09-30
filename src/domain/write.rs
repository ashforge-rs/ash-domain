//! The write pipeline: create, update, destroy, and generic actions.
//!
//! Every method here runs **after** the authorization gate in
//! [`dispatch_action`](super::Domain::dispatch_action) has been reached — each
//! `exec_*` calls it explicitly before anything observable happens. The staging
//! helpers (defaults, primary key, tenant stamp) are pure: they shape the
//! [`Changeset`] the policies then judge.

use crate::action::{ActionDef, ActionKind, ActionResult, Changeset};
use crate::attribute::{AttrType, Attribute};
use crate::context::{Context, HandlerContext, Store};
use crate::error::{Error, Result};
use crate::event::DomainEvent;
use crate::extension::ActionHold;
use crate::resource::Resource;
use crate::trace::trace_event;
use crate::value::{Record, Value};

use super::{Domain, find_action};

impl Domain {
    /// The create pipeline: stage, authorize, run extensions, persist. Returns
    /// the persisted raw [`Record`]. Only [`handle_action`](Domain::handle_action)
    /// calls this.
    pub(super) async fn exec_create<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        def: &ActionDef,
        params: Record,
    ) -> Result<Record> {
        let pk = R::primary_key();
        let mut cs = self.stage_create::<R>(ctx, def, params).await?;
        // Authorize before anything observable, then run the trusted hooks and
        // the action's validations. Holds live until this action returns.
        let _holds = self.gate_write(&mut cs, def).await?;

        // Open a transaction if the store offers one; persist inside it so a
        // failure in the after-persist tail rolls the write back. A store that
        // declines (the default) keeps the immediate-commit path.
        let txn = self.begin_write(ctx.backend()).await?;
        let record = self
            .write_layer(ctx.backend(), txn.as_deref())
            .create(R::NAME, &pk, cs.data.clone())
            .await?;
        trace_event!(&self.trace_gate, "persisted");
        let mut result = ActionResult::Record(record.clone());
        self.finish_write(
            txn,
            std::slice::from_ref(&cs),
            std::slice::from_mut(&mut result),
        )
        .await?;
        trace_event!(&self.trace_gate, "after_action");
        Ok(record)
    }

    /// The batch create pipeline: stage and gate **every** row, then persist
    /// them in one call.
    ///
    /// The batching is confined to the persist. Each row still runs the whole
    /// per-row pipeline in the same order as a single create — stage, authorize,
    /// field-level write gate, `before_action`, validations — and the two passes
    /// are deliberately separate: no row reaches the data layer until every row
    /// has been authorized, so one denied row in a batch persists none of them.
    pub(super) async fn exec_create_many<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        def: &ActionDef,
        rows: Vec<Record>,
    ) -> Result<Vec<Record>> {
        self.check_batch_bound(R::NAME, rows.len())?;
        // An empty batch is a no-op, not an empty transaction: nothing to
        // authorize, persist, or emit.
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let pk = R::primary_key();

        // Pass one: stage and gate every row. `holds` keeps every lease alive
        // until the whole batch has finished, exactly as a single write holds
        // its own until it returns.
        let mut staged = Vec::with_capacity(rows.len());
        let mut holds = Vec::with_capacity(rows.len());
        for params in rows {
            let mut cs = self.stage_create::<R>(ctx, def, params).await?;
            holds.push(self.gate_write(&mut cs, def).await?);
            staged.push(cs);
        }
        trace_event!(&self.trace_gate, "staged_batch", rows = staged.len());

        // Pass two: persist. One layer call, inside the store's transaction when
        // it offers one — which is where a batch's atomicity comes from.
        let txn = self.begin_write(ctx.backend()).await?;
        let records = self
            .write_layer(ctx.backend(), txn.as_deref())
            .create_many(
                R::NAME,
                &pk,
                staged.iter().map(|cs| cs.data.clone()).collect(),
            )
            .await?;
        if records.len() != staged.len() {
            return Err(Error::DataLayer {
                message: format!(
                    "data layer returned {} rows for a batch create of {} `{}` rows",
                    records.len(),
                    staged.len(),
                    R::NAME
                ),
                source: None,
            });
        }
        trace_event!(&self.trace_gate, "persisted_batch", rows = records.len());
        let mut results: Vec<ActionResult> =
            records.iter().cloned().map(ActionResult::Record).collect();
        self.finish_write(txn, &staged, &mut results).await?;
        trace_event!(&self.trace_gate, "after_action");
        Ok(records)
    }

    /// Refuse a batch larger than [`DomainConfig::max_batch`](crate::DomainConfig::max_batch),
    /// before a single row is staged. Caller-supplied size, so this is a
    /// `Result`, never an assertion.
    fn check_batch_bound(&self, resource: &str, len: usize) -> Result<()> {
        if len > self.max_batch.get() as usize {
            return Err(Error::invalid(format!(
                "batch create of {len} `{resource}` rows exceeds the domain's max_batch of {}",
                self.max_batch
            )));
        }
        Ok(())
    }

    /// Stage a create: the pure part of the pipeline — the action's changes,
    /// declared defaults, the tenant stamp and the primary key — producing the
    /// [`Changeset`] the policies then judge. Observably inert: it touches no
    /// extension, no lock, and no layer.
    async fn stage_create<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        def: &ActionDef,
        params: Record,
    ) -> Result<Changeset> {
        let attrs = R::attributes();
        let pk = R::primary_key();
        let tenant_strategy = R::tenant();
        let tenant = self.resolve_tenant(tenant_strategy.as_ref(), R::NAME, ctx.tenant())?;

        let mut cs = Changeset::new(R::NAME, &def.name, ActionKind::Write, params);
        cs.actor = ctx.actor().cloned();
        cs.tenant = tenant.clone();
        self.stage_input(&attrs, &mut cs);
        for change in &def.changes {
            change.change(&mut cs).await?;
        }
        self.apply_defaults(&attrs, &mut cs);
        self.stamp_tenant(&mut cs, tenant_strategy.as_ref(), tenant.as_ref());
        self.ensure_primary_key(&pk, R::NAME, &mut cs);
        self.seed_version::<R>(&mut cs);
        Ok(cs)
    }

    /// Seed the initial row version on a create, for a resource that declares a
    /// [`version_attribute`](crate::Resource::version_attribute).
    ///
    /// Without this a created row carries no version, and its **first** update
    /// would fail — the update path requires the stored row to hold an integer
    /// version to compare against — leaving the row permanently unwritable.
    ///
    /// Stamped authoritatively over anything the caller supplied, for the same
    /// reason the update path stamps the next version: the version is the
    /// domain's to manage, never the caller's to choose. A resource whose
    /// attribute declares a `default` still starts here, so the sequence the
    /// update path increments always begins at a known value.
    fn seed_version<R: Resource>(&self, cs: &mut Changeset) {
        let Some(attribute) = R::version_attribute() else {
            return;
        };
        cs.data.insert(attribute, Value::Int(INITIAL_VERSION));
    }

    /// The gate every write passes: **authorize first**, then the trusted
    /// extension hooks, then the action's registered validations — so a denied
    /// caller never learns whether their input was valid, and no extension runs
    /// for a request the policy set refused. Returns the holds the action must
    /// keep alive until it ends.
    async fn gate_write(&self, cs: &mut Changeset, def: &ActionDef) -> Result<Vec<ActionHold>> {
        trace_event!(&self.trace_gate, "staged", changes = cs.data.0.len());
        if let Err(e) = self.authorize(cs).await {
            trace_event!(&self.trace_gate, "denied", reason = %e);
            return Err(e);
        }
        trace_event!(&self.trace_gate, "authorized");
        let holds = self.run_before(cs).await?;
        trace_event!(&self.trace_gate, "before_action");
        // The core validates no attributes itself; run the action's registered
        // Validations here — after changes, before_action, and authorization.
        for validation in &def.validations {
            validation.validate(cs).await?;
        }
        Ok(holds)
    }

    /// The update pipeline: fetch, tenant-check, stage, authorize, persist.
    /// Returns the merged raw [`Record`]. Only
    /// [`handle_action`](Domain::handle_action) calls this.
    pub(super) async fn exec_update<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        def: &ActionDef,
        id: Value,
        params: Record,
    ) -> Result<Record> {
        let attrs = R::attributes();
        let pk = R::primary_key();

        let tenant_strategy = R::tenant();
        let tenant = self.resolve_tenant(tenant_strategy.as_ref(), R::NAME, ctx.tenant())?;

        let original = ctx
            .backend()
            .layer()
            .get(R::NAME, &pk, &id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("{}/{id:?}", R::NAME)))?;
        self.check_record_tenant(
            &original,
            R::NAME,
            tenant_strategy.as_ref(),
            tenant.as_ref(),
            &id,
        )?;

        let mut cs = Changeset::new(R::NAME, &def.name, ActionKind::Write, params);
        cs.actor = ctx.actor().cloned();
        cs.tenant = tenant.clone();
        cs.original = Some(original);
        self.stage_input(&attrs, &mut cs);

        for change in &def.changes {
            change.change(&mut cs).await?;
        }
        // Re-stamp authoritatively so an update can never move a record's tenant.
        self.stamp_tenant(&mut cs, tenant_strategy.as_ref(), tenant.as_ref());
        trace_event!(&self.trace_gate, "staged", changes = cs.data.0.len());

        // Authorize before anything observable: extensions must not run — and
        // validation validity must not be reported — for a denied request.
        if let Err(e) = self.authorize(&cs).await {
            trace_event!(&self.trace_gate, "denied", reason = %e);
            return Err(e);
        }
        trace_event!(&self.trace_gate, "authorized");
        // Hold any acquired locks until the action ends (dropped on return/error).
        let _holds = self.run_before(&mut cs).await?;
        trace_event!(&self.trace_gate, "before_action");
        // The core validates no attributes itself; run the action's registered
        // Validations here — after changes, before_action, and authorization, so
        // a denied caller never learns whether their input was valid.
        for validation in &def.validations {
            validation.validate(&cs).await?;
        }

        // Optimistic concurrency, when the resource opts in: the write lands only
        // if the row still holds the version the caller read. Resolved *after*
        // staging so a `Change` cannot forge the expected version, and after the
        // gate so a denied caller learns nothing about the row's state.
        let version = self.resolve_expected_version::<R>(&cs, &id)?;
        if let Some(check) = &version {
            // Advance the row version as part of this write. Stamped
            // authoritatively over whatever the caller sent, exactly as the
            // tenant is: the caller states the version it *read*, never the one
            // it writes.
            let next = check.expected.as_int().unwrap_or(0).saturating_add(1);
            cs.data.insert(check.attribute.clone(), Value::Int(next));
        }

        let txn = self.begin_write(ctx.backend()).await?;
        let layer = self.write_layer(ctx.backend(), txn.as_deref());
        let persisted = match &version {
            Some(check) => {
                layer
                    .update_versioned(
                        R::NAME,
                        &pk,
                        &id,
                        &cs.data,
                        &check.attribute,
                        &check.expected,
                    )
                    .await
            }
            None => layer.update(R::NAME, &pk, &id, &cs.data).await,
        };
        // A conflict (or any failure) before the commit must not leave a
        // transaction open — roll it back, then surface the original cause.
        let record = match persisted {
            Ok(record) => record,
            Err(e) => {
                if let Some(txn) = txn {
                    let _ = txn.rollback().await;
                    trace_event!(&self.trace_gate, "rolled_back", reason = %e);
                }
                return Err(e);
            }
        };
        trace_event!(&self.trace_gate, "persisted");
        let mut result = ActionResult::Record(record.clone());
        self.finish_write(
            txn,
            std::slice::from_ref(&cs),
            std::slice::from_mut(&mut result),
        )
        .await?;
        trace_event!(&self.trace_gate, "after_action");
        Ok(record)
    }

    /// Resolve the optimistic-concurrency check for an update, and stamp the
    /// **next** version into the staged data.
    ///
    /// Returns `None` for a resource that declares no
    /// [`version_attribute`](crate::Resource::version_attribute) — the
    /// unversioned path, unchanged.
    ///
    /// The expected version is the one the **caller supplied** in its params,
    /// because that is the value it actually read; falling back to the stored
    /// row's own version would compare the row against itself and always
    /// succeed, which is no protection at all. A caller that omits it is telling
    /// us it did not read the row, and an update that does not know what it is
    /// overwriting is exactly what this feature exists to refuse — so that is an
    /// `Invalid`, not a silent unconditional write.
    fn resolve_expected_version<R: Resource>(
        &self,
        cs: &Changeset,
        id: &Value,
    ) -> Result<Option<VersionCheck>> {
        let Some(attribute) = R::version_attribute() else {
            return Ok(None);
        };
        // The row as it was fetched at the top of the update; `exec_update` sets
        // it before staging, so it is always present on this path.
        let original = cs
            .original
            .as_ref()
            .expect("Some: exec_update sets cs.original before staging");
        let expected = cs.params.get(&attribute).cloned().ok_or_else(|| {
            Error::invalid(format!(
                "`{}` requires the `{attribute}` it was read at on every update \
                 (optimistic concurrency); none was supplied for {id:?}",
                R::NAME
            ))
        })?;
        // The stored row must carry an integer version; a resource whose version
        // attribute is missing from storage cannot be safely updated, and
        // `try_new` already guaranteed the attribute is declared and integer.
        let current = original
            .get(&attribute)
            .and_then(Value::as_int)
            .ok_or_else(|| {
                Error::data_layer(format!(
                    "row {id:?} of `{}` has no integer `{attribute}`, but the resource declares it \
                 as its version attribute",
                    R::NAME
                ))
            })?;
        let Some(expected_int) = expected.as_int() else {
            return Err(Error::invalid(format!(
                "the `{attribute}` supplied for {id:?} of `{}` is not an integer: {expected:?}",
                R::NAME
            )));
        };
        // Fail fast on a stale version we can already see is stale, without a
        // round trip. The layer still re-checks atomically — this is an
        // optimization and a clearer error, never the enforcement point.
        if expected_int != current {
            return Err(Error::Conflict {
                resource: R::NAME.to_string(),
                message: format!(
                    "row {id:?} was read at {attribute} {expected_int} but is now at {current}: \
                     re-read the row, re-apply the change, and retry"
                ),
            });
        }
        Ok(Some(VersionCheck {
            attribute,
            expected,
        }))
    }

    /// The destroy pipeline: fetch, tenant-check, authorize, delete. Only
    /// [`handle_action`](Domain::handle_action) calls this.
    pub(super) async fn exec_destroy<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        def: &ActionDef,
        id: Value,
    ) -> Result<()> {
        let _ = def;
        let pk = R::primary_key();

        let tenant_strategy = R::tenant();
        let tenant = self.resolve_tenant(tenant_strategy.as_ref(), R::NAME, ctx.tenant())?;

        let original = ctx
            .backend()
            .layer()
            .get(R::NAME, &pk, &id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("{}/{id:?}", R::NAME)))?;
        self.check_record_tenant(
            &original,
            R::NAME,
            tenant_strategy.as_ref(),
            tenant.as_ref(),
            &id,
        )?;

        let mut cs = Changeset::new(R::NAME, &def.name, ActionKind::Write, Record::new());
        cs.actor = ctx.actor().cloned();
        cs.tenant = tenant.clone();
        cs.original = Some(original);

        // Authorize before anything observable (see the module docs).
        if let Err(e) = self.authorize(&cs).await {
            trace_event!(&self.trace_gate, "denied", reason = %e);
            return Err(e);
        }
        trace_event!(&self.trace_gate, "authorized");
        // Hold any acquired locks until the action ends (dropped on return/error).
        let _holds = self.run_before(&mut cs).await?;
        trace_event!(&self.trace_gate, "before_action");

        let txn = self.begin_write(ctx.backend()).await?;
        self.write_layer(ctx.backend(), txn.as_deref())
            .destroy(R::NAME, &pk, &id)
            .await?;
        trace_event!(&self.trace_gate, "persisted");
        let mut result = ActionResult::None;
        self.finish_write(
            txn,
            std::slice::from_ref(&cs),
            std::slice::from_mut(&mut result),
        )
        .await?;
        trace_event!(&self.trace_gate, "after_action");
        Ok(())
    }

    /// The generic pipeline: authorize, then run the action's handler with the
    /// context's [`Store`] available as `Some`. Returns the handler's [`Value`].
    /// Only [`handle_action`](Domain::handle_action) calls this.
    pub(super) async fn exec_generic<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        input: Record,
    ) -> Result<Value> {
        let actions = R::actions();
        let def = find_action(&actions, R::NAME, action, ActionKind::Generic)?;
        let handler = def
            .handler
            .clone()
            .ok_or_else(|| Error::invalid(format!("generic action `{action}` has no handler")))?;

        let mut cs = Changeset::new(R::NAME, action, ActionKind::Generic, input);
        cs.actor = ctx.actor().cloned();

        // Authorize before anything observable (see the module docs).
        if let Err(e) = self.authorize(&cs).await {
            trace_event!(&self.trace_gate, "denied", reason = %e);
            return Err(e);
        }
        trace_event!(&self.trace_gate, "authorized");
        // Hold any acquired locks until the action ends (dropped on return/error).
        let _holds = self.run_before(&mut cs).await?;
        trace_event!(&self.trace_gate, "before_action");

        // A generic handler always receives the context's store (as `Some`); all
        // borrows below are shared `&self` accessors on the context, so the store
        // handle and the request-state projections coexist.
        let store = ctx.backend() as &dyn Store;
        // Derive the scoped handler context for the span of this operation. It
        // borrows `self` (the domain) and the parent's read-only request state,
        // and gives the handler a scratch bag for enrichment.
        let mut hctx = HandlerContext::new(
            self,
            ctx.id(),
            ctx.actor(),
            ctx.tenant(),
            ctx.meta(),
            Some(store),
        );
        let value = handler.run_ctx(&mut cs, &mut hctx).await?;
        trace_event!(&self.trace_gate, "handler");
        // `complete` consumes the derived context, so it cannot be reused past
        // this run — the compiler enforces it. It also ends the shared borrows of
        // `ctx` the handler context held, which is what lets the merge below take
        // `&mut ctx`. Note the `?` above: a handler that failed never reaches
        // here, so a failed action contributes nothing to the request state.
        let scratch = hctx.complete();
        ctx.merge_scratch(scratch);

        let mut result = ActionResult::Value(value.clone());
        self.run_after(&cs, &mut result).await?;
        trace_event!(&self.trace_gate, "after_action");
        Ok(value)
    }

    // ── pipeline helpers ──────────────────────────────────────────────────

    /// Seed the changeset's staged data from public attribute params.
    pub(super) fn stage_input(&self, attrs: &[Attribute], cs: &mut Changeset) {
        for attr in attrs {
            if let Some(value) = cs.params.get(&attr.name) {
                cs.data.insert(attr.name.clone(), value.clone());
            }
        }
    }

    pub(super) fn apply_defaults(&self, attrs: &[Attribute], cs: &mut Changeset) {
        for attr in attrs {
            if !cs.data.has(&attr.name) {
                if let Some(default) = &attr.default {
                    cs.data.insert(attr.name.clone(), default.clone());
                }
            }
            // Fill defaults *inside* a staged embedded value too, so a nested
            // field the caller omitted still gets its declared default.
            match &attr.ty {
                AttrType::_Embed(fields) => {
                    if let Some(value) = cs.data.0.get_mut(&attr.name) {
                        apply_embedded_defaults(fields, value);
                    }
                }
                AttrType::_EmbedList(fields) => {
                    if let Some(Value::List(items)) = cs.data.0.get_mut(&attr.name) {
                        for item in items.iter_mut() {
                            apply_embedded_defaults(fields, item);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Generate the primary key when the input did not supply one, delegating to
    /// the domain's configured [`IdGenerator`] (default: [`DefaultIdGenerator`]).
    /// `pk` is the primary-key attribute name (from
    /// [`Resource::primary_key`](crate::Resource::primary_key)).
    ///
    /// "Did not supply one" means the pk is **absent, [`Null`](Value::Null), or
    /// the empty string** — not merely absent. The empty string matters because a
    /// derived resource with a `String` primary key defaults it to `""`, and
    /// `IntoRecord` then carries that `""` as a *present* value; without treating
    /// it as unset, a `create` from a `Default`-constructed struct would persist a
    /// row keyed on `""` instead of a fresh id. Treating `""` as unset lets the
    /// natural `id: String` field work like `id: Option<String>`.
    pub(super) fn ensure_primary_key(&self, pk: &str, resource: &str, cs: &mut Changeset) {
        let supplied = cs
            .data
            .get(pk)
            .is_some_and(|v| !v.is_null() && v.as_str() != Some(""));
        if supplied {
            return;
        }
        let generated = self.id_generator.next_id(resource, pk);
        cs.data.insert(pk.to_string(), generated);
    }

    /// Run every extension's `before_action` (staging/guarding), then every
    /// extension's `acquire` (taking any locks). The returned holds must be kept
    /// alive by the caller for the rest of the action: dropping them releases
    /// whatever was acquired, so binding them to a local that lives to the end of
    /// the action method guarantees release on every exit path, error included.
    pub(super) async fn run_before(&self, cs: &mut Changeset) -> Result<Vec<ActionHold>> {
        for ext in self.extensions.iter() {
            ext.before_action(cs).await?;
        }
        let mut holds = Vec::new();
        for ext in self.extensions.iter() {
            holds.push(ext.acquire(cs).await?);
        }
        Ok(holds)
    }

    pub(super) async fn run_after(&self, cs: &Changeset, result: &mut ActionResult) -> Result<()> {
        for ext in self.extensions.iter() {
            ext.after_action(cs, result).await?;
        }
        self.emit_event(cs, result).await?;
        Ok(())
    }

    /// The **after-persist tail** of a CRUD write, honoring an optional
    /// [`Transaction`](crate::datalayer::Transaction).
    ///
    /// The write op has already run — against `txn` when `Some`, else the bare
    /// layer. This then runs `after_action` extensions and, if a transaction is
    /// open, commits it (rolling it back on any extension failure so a committed
    /// row is never left behind), and finally emits the post-commit event.
    ///
    /// Ordering, transactional case:
    /// `after_action → commit → emit`. A failure in `after_action` rolls back and
    /// returns the error *without* emitting (nothing committed, no event). Events
    /// stay post-commit best-effort in both cases — atomic event delivery is a
    /// consumer concern (an outbox), by design.
    pub(super) async fn finish_write(
        &self,
        txn: Option<Box<dyn crate::datalayer::Transaction>>,
        staged: &[Changeset],
        results: &mut [ActionResult],
    ) -> Result<()> {
        debug_assert_eq!(
            staged.len(),
            results.len(),
            "one result per staged row: the callers build them in step"
        );
        // The rollback-able part: extensions that observe/enrich the committed
        // row, then the pre-commit staging pass that lets a handler write its
        // outbox row inside this very transaction.
        if let Err(e) = self.before_commit(txn.as_deref(), staged, results).await {
            if let Some(txn) = txn {
                // A rollback failure is itself surfaced, but the original error
                // is the cause the caller cares about — prefer it.
                let _ = txn.rollback().await;
            }
            trace_event!(&self.trace_gate, "rolled_back", reason = %e);
            return Err(e);
        }
        // The durable point (transactional case). After this the write stands —
        // one commit, whether this was one row or a batch.
        if let Some(txn) = txn {
            txn.commit().await?;
            trace_event!(&self.trace_gate, "committed");
        }
        // Post-commit, best-effort: an event-handler failure returns Err on a row
        // that is already committed — the documented, now-narrowed contract.
        for (cs, result) in staged.iter().zip(results.iter()) {
            self.emit_event(cs, result).await?;
        }
        Ok(())
    }

    /// Everything between the persist and the commit: `after_action` for every
    /// row, then the transactional-outbox staging pass. Any `Err` here rolls the
    /// write back — that is the point of doing both before the commit.
    async fn before_commit(
        &self,
        txn: Option<&dyn crate::datalayer::Transaction>,
        staged: &[Changeset],
        results: &mut [ActionResult],
    ) -> Result<()> {
        for (cs, result) in staged.iter().zip(results.iter_mut()) {
            for ext in self.extensions.iter() {
                ext.after_action(cs, result).await?;
            }
        }
        // Staging needs a transaction to join. `begin_write` already refused the
        // write if a staging handler is registered and the store offers none, so
        // reaching here without one means nothing asked to stage.
        let Some(txn) = txn else { return Ok(()) };
        self.stage_events(txn as &dyn crate::datalayer::DataLayer, staged, results)
            .await
    }

    /// The transactional-outbox pass: hand every registered handler each event
    /// **before** the commit, through the transaction's own [`DataLayer`] view,
    /// so an outbox row lands atomically with the write it announces.
    ///
    /// Skipped entirely when no handler stages — the common case pays one
    /// boolean, not an event construction.
    async fn stage_events(
        &self,
        layer: &dyn crate::datalayer::DataLayer,
        staged: &[Changeset],
        results: &[ActionResult],
    ) -> Result<()> {
        if !self.emitter.any_stages() {
            return Ok(());
        }
        for (cs, result) in staged.iter().zip(results.iter()) {
            let Some(event) = self.build_event(cs, result) else {
                continue;
            };
            for handler in self.emitter.handlers() {
                handler.stage(&event, layer).await?;
            }
        }
        trace_event!(&self.trace_gate, "staged_events", rows = staged.len());
        Ok(())
    }

    /// Open the write transaction, refusing the write outright when a registered
    /// handler stages events but this store offers no transaction to stage into.
    ///
    /// Silently skipping the staging would leave a deployment believing its
    /// outbox is atomic when it is not — the exact silent degradation this crate
    /// refuses elsewhere. Fail loudly, at the first write, instead.
    async fn begin_write(
        &self,
        store: &dyn Store,
    ) -> Result<Option<Box<dyn crate::datalayer::Transaction>>> {
        let txn = store.begin().await?;
        if txn.is_none() && self.emitter.any_stages() {
            return Err(Error::Unsupported(
                "an EventHandler stages events into the write transaction, but this Store \
                 offers none (Store::begin returned None): either use a transactional store \
                 or drop the staging handler"
                    .into(),
            ));
        }
        Ok(txn)
    }

    pub(super) fn write_layer<'a>(
        &self,
        store: &'a dyn Store,
        txn: Option<&'a dyn crate::datalayer::Transaction>,
    ) -> &'a dyn crate::datalayer::DataLayer {
        match txn {
            Some(t) => t as &dyn crate::datalayer::DataLayer,
            None => store.layer(),
        }
    }

    /// Build the [`DomainEvent`] for a committed action — the fact that it
    /// happened — and hand it to every registered [`EventHandler`]. What the
    /// event *becomes* (a notification, an audit entry, a job) is each handler's
    /// choice; the domain only produces and hands off.
    ///
    /// The affected records come from the action's result (the created/updated
    /// row, the destroyed row's prior state from the changeset, or a generic
    /// action's record output); a generic action that returned a scalar carries
    /// no records. The commit time is read from the domain's [`Clock`](crate::Clock).
    ///
    /// # Failure after commit
    ///
    /// This runs **after the write is already persisted**, and the write cannot
    /// be rolled back — the core has no transaction seam. Delivery is therefore
    /// **best-effort, in-process, and un-retried**: handlers are invoked in
    /// registration order, and the **first** one to return `Err` stops the scan
    /// (later handlers do *not* run) and propagates that error up as the action's
    /// result. So a caller can see the CRUD method return `Err` even though the
    /// row is committed and readable. A handler that must not surface its failure
    /// to the caller — or must not block the handlers registered after it — must
    /// swallow its own errors and return `Ok(())` (offload ret/queue/log inside
    /// the handler). See [`EventHandler`] for the full contract.
    pub(super) async fn emit_event(&self, cs: &Changeset, result: &ActionResult) -> Result<()> {
        // No registered handler means no observer; the fact still occurred, but
        // producing an event nobody receives is pure overhead, so skip building it.
        let Some(event) = self.build_event(cs, result) else {
            return Ok(());
        };
        // Fan out through the shared emitter so commit-path and out-of-band
        // events deliver identically: best-effort, in-process, un-retried, and
        // the first handler `Err` stops the rest and surfaces to the caller — it
        // does not undo the (already-committed) write. See this method's docs.
        self.emitter.emit(event).await
    }

    /// Build the [`DomainEvent`] for a finished action, or `None` when no
    /// handler is registered to receive it. Shared by the post-commit emit and
    /// the pre-commit staging pass so both describe the action identically.
    fn build_event(&self, cs: &Changeset, result: &ActionResult) -> Option<DomainEvent> {
        if self.emitter.handler_count() == 0 {
            return None;
        }
        let records = match (result, cs.kind) {
            (ActionResult::Record(r), _) => vec![r.clone()],
            (ActionResult::Records(rs), _) => rs.clone(),
            // A delete has no result record; report the row that was removed.
            (ActionResult::None, _) => cs.original.clone().into_iter().collect(),
            _ => Vec::new(),
        };
        Some(DomainEvent {
            resource: cs.resource.clone(),
            action: cs.action.clone(),
            kind: cs.kind,
            actor: cs.actor.clone(),
            tenant: cs.tenant.clone(),
            records,
            at: self.clock.now_millis(),
        })
    }
}

/// The version every created row starts at, for a resource that declares a
/// [`version_attribute`](crate::Resource::version_attribute). The update path
/// increments from here.
const INITIAL_VERSION: i64 = 1;

/// The optimistic-concurrency check for one update: which attribute carries the
/// row version, and the value the caller read it at.
///
/// Built by [`Domain::resolve_expected_version`] and consumed immediately by the
/// conditional write; it is never stored, so there is no window in which a stale
/// check could be reused.
struct VersionCheck {
    /// The version attribute's name.
    attribute: String,
    /// The version the caller read, which the stored row must still hold.
    expected: Value,
}

/// Apply declared defaults to a nested embedded [`Value::Map`], in place and
/// recursively, mirroring [`Domain::apply_defaults`] for the top level. A field
/// absent from the map gets its default; an embedded field recurses.
pub(super) fn apply_embedded_defaults(fields: &[Attribute], value: &mut Value) {
    let Value::Map(map) = value else { return };
    for field in fields {
        if !map.contains_key(&field.name) {
            if let Some(default) = &field.default {
                map.insert(field.name.clone(), default.clone());
            }
        }
        match &field.ty {
            AttrType::_Embed(nested) => {
                if let Some(inner) = map.get_mut(&field.name) {
                    apply_embedded_defaults(nested, inner);
                }
            }
            AttrType::_EmbedList(nested) => {
                if let Some(Value::List(items)) = map.get_mut(&field.name) {
                    for item in items.iter_mut() {
                        apply_embedded_defaults(nested, item);
                    }
                }
            }
            _ => {}
        }
    }
}
