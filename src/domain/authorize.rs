//! The authorization gate, attribute redaction, and the policy dry-run.
//!
//! Default-deny with admit-on-affirmative: an operation is forbidden unless a
//! matching policy affirmatively allows it, any matching policy may veto, and a
//! policy-backend failure is [`Error::PolicyError`] — distinct from a deliberate
//! [`Error::Forbidden`], and equally closed.

use crate::action::{ActionKind, Changeset};
use crate::attribute::Attribute;
use crate::context::{Context, Store};
use crate::error::{Error, Result};
use crate::policy::{Decision, Explanation, PolicyNode, PolicySet, ReadReport, Redaction};
use crate::query::Query;
use crate::resource::Resource;
use crate::value::{Record, Value};

use super::{Domain, find_action};

impl Domain {
    /// The domain's policy set (introspection). Use [`policy_tree`](Domain::policy_tree)
    /// for the grouped tree view.
    pub fn policies(&self) -> &PolicySet {
        &self.policies
    }

    /// A [`PolicyNode`] tree of every policy the domain enforces, grouped by the
    /// level it applies at — domain at the root, then resources, then their
    /// actions and attributes. Call [`render`](PolicyNode::render) for a
    /// human-readable outline.
    pub fn policy_tree(&self) -> PolicyNode {
        self.policies.tree()
    }

    /// **Dry-run** the authorization of a *write* of `R` — answer "would this be
    /// allowed, and which policy decides?" **without executing it**.
    ///
    /// Returns an [`Explanation`] (final [`Decision`], deciding policy, every
    /// matching policy's vote). It builds the same changeset the real write would
    /// authorize — staging the caller's params, applying attribute defaults, and
    /// stamping the tenant — then evaluates **only the operation gate**. It runs
    /// **no** registered `Change` hooks, **no** field-write pass, touches **no**
    /// data layer, and fires **no** extension or event. Authorization stays
    /// fail-closed: the reported decision matches what the live gate would return.
    ///
    /// A tenant-scoped resource still needs a tenant on the context (else
    /// [`Error::MissingTenant`]) — mirroring the real write's precondition — so the
    /// dry-run can't answer for an ill-formed request.
    pub async fn explain_write<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        params: Record,
    ) -> Result<Explanation> {
        self.ensure_registered(R::NAME)?;
        let actions = R::actions();
        let def = find_action(&actions, R::NAME, action, ActionKind::Write)?;
        let attrs = R::attributes();

        let tenant_strategy = R::tenant();
        let tenant = self.resolve_tenant(tenant_strategy.as_ref(), R::NAME, ctx.tenant())?;

        // Build the authorization input exactly as the write path does, minus the
        // consumer `Change` hooks (which may have side effects) — pure staging only.
        let mut cs = Changeset::new(R::NAME, &def.name, ActionKind::Write, params);
        cs.actor = ctx.actor().cloned();
        cs.tenant = tenant.clone();
        self.stage_input(&attrs, &mut cs);
        self.apply_defaults(&attrs, &mut cs);
        self.stamp_tenant(&mut cs, tenant_strategy.as_ref(), tenant.as_ref());

        Ok(self.policies.explain_write(&cs).await)
    }

    /// **Dry-run** the authorization of a *read* of `R` — the read counterpart of
    /// [`explain_write`](Domain::explain_write). Builds the tenant-scoped query the
    /// real read would authorize (honoring [`Context::allow_cross_tenant`]) and
    /// evaluates the read gate only, with no preparations, extensions, or layer
    /// access. Returns the [`Explanation`].
    pub async fn explain_read<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        mut query: Query,
    ) -> Result<Explanation> {
        self.ensure_registered(R::NAME)?;
        let actions = R::actions();
        let _def = find_action(&actions, R::NAME, action, ActionKind::Read)?;
        query.resource = R::NAME.to_string();

        let tenant_strategy = R::tenant();
        self.scope_read_by_tenant(
            &mut query,
            tenant_strategy.as_ref(),
            R::NAME,
            ctx.tenant(),
            ctx.is_cross_tenant(),
        )?;

        Ok(self
            .policies
            .explain_read(R::NAME, action, &query, ctx.actor())
            .await)
    }

    pub(super) async fn authorize(&self, cs: &Changeset) -> Result<()> {
        // The operation gate first: default-deny, fail-closed. A denied write must
        // never reach the field-write gate (which would leak, via a distinct
        // reason, whether a field was the blocker).
        let result = decision_to_result(self.policies.authorize_write(cs).await);
        if let Err(e) = &result {
            self.notify_denied(&cs.resource, &cs.action, cs.actor.as_ref(), e)
                .await;
            return result;
        }
        // Then the per-field write gate, within the now-authorized operation.
        self.authorize_attribute_writes(cs).await
    }

    /// After the operation gate admits a write, veto any individual field the
    /// caller is not permitted to set. Consulted once per attribute the **caller
    /// supplied** (present in `cs.params`), so domain-stamped fields (tenant,
    /// defaults, timestamps — which land in `cs.data`, not `params`) are never
    /// gated: the caller didn't set them. A [`Forbid`]/`Error` on any such field
    /// aborts the whole write fail-closed (a field being set can't be dropped the
    /// way a read is nulled), routed through the same `on_denied` chokepoint.
    pub(super) async fn authorize_attribute_writes(&self, cs: &Changeset) -> Result<()> {
        // Skip the pass entirely when no policy could gate a field write.
        if !self.policies.has_attribute_write_policies() {
            return Ok(());
        }
        let Some(resource) = self.resource(&cs.resource) else {
            return Ok(());
        };
        for attr in resource.attributes() {
            // Only fields the caller actually supplied are subject to the gate.
            if !cs.params.has(&attr.name) {
                continue;
            }
            let decision = self
                .policies
                .authorize_attribute_write(
                    &cs.resource,
                    &cs.action,
                    &attr.name,
                    cs,
                    cs.actor.as_ref(),
                )
                .await;
            if let Err(e) = decision_to_result(decision) {
                self.notify_denied(&cs.resource, &cs.action, cs.actor.as_ref(), &e)
                    .await;
                return Err(e);
            }
        }
        Ok(())
    }

    pub(super) async fn authorize_read(
        &self,
        resource: &str,
        action: &str,
        query: &Query,
        actor: Option<&Record>,
    ) -> Result<()> {
        let result = decision_to_result(
            self.policies
                .authorize_read(resource, action, query, actor)
                .await,
        );
        if let Err(e) = &result {
            self.notify_denied(resource, action, actor, e).await;
        }
        result
    }

    /// Notify every extension's [`on_denied`](Extension::on_denied) hook that the
    /// authorization gate refused an action — the one place a denial is
    /// observable, since the other extension hooks only ever run on the
    /// authorized path.
    ///
    /// Called from the two authorize chokepoints ([`authorize`](Domain::authorize)
    /// / [`authorize_read`](Domain::authorize_read)) so **every** denied path is
    /// covered without per-call-site plumbing. Fires **only** for authorization
    /// denials — [`Forbidden`](Error::Forbidden) (a policy vetoed) or
    /// [`PolicyError`](Error::PolicyError) (a policy backend failed closed) — never
    /// for ordinary input errors, which are not access denials. The hook is
    /// read-only and its result is discarded: a notification sink cannot alter the
    /// fail-closed outcome.
    pub(super) async fn notify_denied(
        &self,
        resource: &str,
        action: &str,
        actor: Option<&Record>,
        error: &Error,
    ) {
        // Guard: only authorization denials are audited as denials. This is also
        // why `authorize`/`authorize_read` can call this unconditionally on `Err`
        // — a non-authz error simply doesn't fan out.
        if !matches!(error, Error::Forbidden(_) | Error::PolicyError(_)) {
            return;
        }
        for ext in self.extensions.iter() {
            ext.on_denied(resource, action, actor, error).await;
        }
    }

    /// Walk `records` through the attribute-read policies, nulling any attribute
    /// a policy forbids and recording each redaction. A no-op (and no per-row
    /// work) when the set holds no field-capable policies. `attrs` is the
    /// resource's declared attributes — only declared attributes are checked.
    ///
    /// A [`Decision::Error`] from a policy (e.g. an external client failure)
    /// aborts the read fail-closed with [`Error::PolicyError`], rather than
    /// leaving the field un-redacted.
    pub(super) async fn redact_records(
        &self,
        resource: &str,
        action: &str,
        attrs: &[Attribute],
        actor: Option<&Record>,
        records: &mut [Record],
    ) -> Result<ReadReport> {
        let mut report = ReadReport::default();
        if !self.policies.has_attribute_policies() {
            return Ok(report);
        }
        for (row, record) in records.iter_mut().enumerate() {
            for attr in attrs {
                match self
                    .policies
                    .authorize_attribute_read(resource, action, &attr.name, record, actor)
                    .await
                {
                    // Visible-unless-forbidden: an affirmative Allow and an
                    // abstaining NotApplicable both leave the field untouched.
                    Decision::Allow | Decision::NotApplicable => {}
                    Decision::Forbid(reason) => {
                        record.insert(attr.name.clone(), Value::Null);
                        report.redactions.push(Redaction {
                            row,
                            attribute: attr.name.clone(),
                            reason,
                        });
                    }
                    Decision::Error(msg) => return Err(Error::PolicyError(msg)),
                }
            }
        }
        Ok(report)
    }

    /// Authorize destination `rows` produced while loading a relationship, so a
    /// loaded relation is gated exactly like a top-level read of that resource:
    /// the operation-level [`authorize_read`](PolicySet::authorize_read) must
    /// allow it (a denial fails the whole load), then attribute policies redact
    /// forbidden fields in place.
    ///
    /// `query` is the key-set read the loader ran (its `reserved::IN` param over
    /// the join key), passed to read policies so a row-scoping policy sees the
    /// same shape it would for a direct read. The per-row redaction report is
    /// dropped here — the [`Loaded`](crate::resource::Loaded) tree carries no
    /// report channel, so the security-relevant effect (nulling) is applied and
    /// the *reasons* are not surfaced for nested rows.
    pub(super) async fn authorize_loaded_rows(
        &self,
        ctx: &Context<impl Store>,
        resource: &str,
        query: &Query,
        rows: &mut [Record],
    ) -> Result<()> {
        let actor = ctx.actor().cloned();
        let action = self.default_read_action(resource);

        self.authorize_read(resource, &action, query, actor.as_ref())
            .await?;

        if self.policies.has_attribute_policies() {
            let attrs = self
                .resource(resource)
                .map(|r| r.attributes())
                .unwrap_or_default();
            self.redact_records(resource, &action, &attrs, actor.as_ref(), rows)
                .await?;
        }
        Ok(())
    }
}

/// Map a resolved operation [`Decision`] onto the domain's [`Result`]: `Allow`
/// succeeds, `Forbid` becomes [`Error::Forbidden`], and a client `Error` becomes
/// the distinct [`Error::PolicyError`] (fail-closed authorization outage).
///
/// [`PolicySet::authorize_write`]/[`authorize_read`](PolicySet::authorize_read)
/// always resolve a bare [`Decision::NotApplicable`] to the set default before
/// returning, so it should not reach here; treat it as fail-closed deny in case
/// a future caller passes an unresolved decision.
pub(super) fn decision_to_result(decision: Decision) -> Result<()> {
    match decision {
        Decision::Allow => Ok(()),
        Decision::Forbid(reason) => Err(Error::Forbidden(reason)),
        Decision::Error(msg) => Err(Error::PolicyError(msg)),
        Decision::NotApplicable => Err(Error::Forbidden(
            "no affirmative authorization decision".into(),
        )),
    }
}
