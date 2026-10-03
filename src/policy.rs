//! Authorization: the [`Policy`] seam and the [`PolicySet`] that scopes it.
//!
//! A policy runs before an action commits (and, for reads, before returned
//! fields are handed back) and decides whether the actor is allowed to proceed.
//! Authorization is first-class in Ash-style frameworks, so it is a core seam
//! here; real policy engines are extensions or consumer code.
//!
//! Operations are **default-deny**: an operation is forbidden unless some
//! matching policy affirmatively [`Allow`](Decision::Allow)s it, and any matching
//! policy may veto with [`Forbid`](Decision::Forbid). Merely matching a scope is
//! not consent — a policy with no opinion returns
//! [`NotApplicable`](Decision::NotApplicable). Opt out of default-deny explicitly
//! with [`PolicySet::permissive`]. See [`PolicySet`] for the precise combining
//! rule, including how attribute visibility differs.
//!
//! # The two questions a policy answers
//!
//! 1. **Operations** — may this actor run this write/read *action* at all?
//!    Writes are judged from their [`Changeset`] ([`Policy::authorize`]); reads
//!    from the prepared [`Query`] ([`Policy::authorize_read`]).
//! 2. **Attribute reads** — may this actor *see* this attribute on a returned
//!    row? ([`Policy::authorize_attribute_read`]) A forbidden attribute is
//!    redacted to [`Value::Null`](crate::Value) in the result and reported back to the caller
//!    (see [`AuthorizedRead`]); the read itself still succeeds.
//!
//! # Scoping — where a policy applies
//!
//! Policies live in a [`PolicySet`], each tagged with a [`Scope`] that says at
//! what *level* it applies: the whole domain, one resource, one action, or one
//! attribute. When the domain authorizes a target it gathers **every** policy
//! whose scope matches. For an operation the combined result is deny-overrides
//! over affirmative-allow: any [`Forbid`](Decision::Forbid) (or a
//! [`Decision::Error`] from a failed backend) wins; otherwise at least one
//! affirmative [`Allow`](Decision::Allow) is required, and policies that
//! [`abstain`](Decision::NotApplicable) neither admit nor deny. A policy
//! therefore constrains only what it opts into, and stacking a broad domain rule
//! with a narrow attribute rule composes safely.
//!
//! # Bring your own authorization backend
//!
//! A [`Policy`] can be hand-written, but the intended path for an **external
//! engine** (OPA, Cedar, Oso, a REST/gRPC "policy service") is the
//! [`PolicyClient`] seam: implement its one [`authorize`](PolicyClient::authorize)
//! method over a uniform [`PolicyRequest`], then wrap the client in
//! [`ClientPolicy`] and register it like any other policy. When the backend
//! can't answer — a network failure, a timeout — the client returns
//! [`Decision::Error`], and the domain aborts the action **fail-closed** with
//! the distinct [`Error::PolicyError`](crate::Error::PolicyError) (never a silent
//! allow, and distinguishable from a deliberate deny).
//!
//! The actor a policy inspects is an opaque [`Record`] set via
//! [`Context::set_actor`](crate::Context::set_actor). The core defines no
//! identity type, so **authentication is entirely decoupled**: an `ash-auth`
//! library verifies credentials on the consumer's terms and sets the actor
//! record; the core only routes it here. See
//! [`Context::actor`](crate::Context::actor) for the full contract.

use std::sync::Arc;

use async_trait::async_trait;

use crate::action::Changeset;
use crate::query::Query;
use crate::value::Record;

/// The outcome of an authorization check — a **four-valued** answer.
///
/// A policy is consulted only for targets its [`Scope`] matches, but matching is
/// not the same as consenting: a policy may *match* a target and still have no
/// opinion about it (e.g. a field-redaction policy scoped to a whole resource
/// has nothing to say about that resource's *write* operation). The four values
/// keep those apart:
///
/// - [`Allow`](Decision::Allow) — an **affirmative** vote: this policy permits
///   the operation. Under default-deny, an operation needs at least one of these
///   to proceed. Merely matching is *not* enough.
/// - [`NotApplicable`](Decision::NotApplicable) — the policy matched but has no
///   opinion. It neither admits nor forbids; the outcome is decided by the other
///   matching policies (or, absent any affirmative vote, by the set's default).
///   This is the default a `Policy` method returns when it isn't overridden.
/// - [`Forbid`](Decision::Forbid) — a **veto**, with a reason. Any single
///   `Forbid` denies the operation regardless of affirmative votes
///   (deny-overrides).
/// - [`Error`](Decision::Error) — the policy *could not decide*, typically
///   because a custom [`PolicyClient`] backend (OPA, Cedar, a REST/gRPC service)
///   failed or timed out. It is **fail-closed**: the domain turns it into
///   [`Error::PolicyError`](crate::Error::PolicyError) and aborts the action, so
///   a broken authorization backend never silently allows, and is
///   distinguishable from a deliberate [`Forbid`](Decision::Forbid).
///
/// The design rule this enforces: **adding a policy can never *widen* access.** A
/// policy written only to redact a field returns `NotApplicable` for operations,
/// so registering it does not accidentally admit writes on the resources it is
/// scoped to. Only an explicit `Allow` opens a gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// An affirmative vote: this policy permits the operation. Under default-deny
    /// at least one affirmative `Allow` is required to proceed.
    Allow,
    /// The policy matched the target but expresses no opinion — neither admits
    /// nor forbids. The default for an un-overridden [`Policy`] method.
    NotApplicable,
    /// A veto — the operation is denied, with a reason. Overrides any affirmative
    /// vote (deny-overrides).
    Forbid(String),
    /// The policy could not reach a decision (backend error/timeout). Treated as
    /// a hard failure, never as an implicit allow.
    Error(String),
}

impl Decision {
    /// `true` only for [`Decision::Allow`] — an affirmative vote. Note that
    /// [`NotApplicable`](Decision::NotApplicable), like
    /// [`Forbid`](Decision::Forbid) and [`Error`](Decision::Error), is **not**
    /// allowing: a policy with no opinion does not admit the operation.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow)
    }

    /// `true` for a [`Forbid`](Decision::Forbid) or [`Error`](Decision::Error) —
    /// the two verdicts that immediately stop the scan under deny-overrides.
    /// [`NotApplicable`](Decision::NotApplicable) is *not* a veto.
    pub fn is_veto(&self) -> bool {
        matches!(self, Decision::Forbid(_) | Decision::Error(_))
    }
}

/// The level at which a [`Policy`] applies within a [`PolicySet`].
///
/// Scopes are matched against a *target* (a resource + action, plus an
/// attribute when a field read is being judged). A broader scope matches every
/// target a narrower one does: [`Scope::Domain`] matches everything,
/// [`Scope::Resource`] matches every action and attribute of that resource, and
/// so on. See [`Scope::matches`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Applies to every action of every resource.
    Domain,
    /// Applies to every action (and attribute) of one resource.
    Resource(String),
    /// Applies to one named action of one resource.
    Action {
        /// The resource the action belongs to.
        resource: String,
        /// The action name.
        action: String,
    },
    /// Applies to reads of one attribute of one resource. Attribute scopes are
    /// consulted **only** for [`authorize_attribute_read`](Policy::authorize_attribute_read),
    /// never for operation checks. The name may also be a declared aggregate or
    /// computed field, whose derived output is gated the same way.
    Attribute {
        /// The resource the attribute belongs to.
        resource: String,
        /// The attribute (or aggregate / computed field) name.
        attribute: String,
    },
}

/// The thing being authorized: which resource/action, and — for a field read —
/// which attribute. Passed to [`Scope::matches`] to select applicable policies.
#[derive(Clone, Copy, Debug)]
pub struct Target<'a> {
    /// The resource name.
    pub resource: &'a str,
    /// The action name.
    pub action: &'a str,
    /// The attribute under consideration, when judging a field read.
    pub attribute: Option<&'a str>,
}

impl Scope {
    /// The resource this scope is anchored to, or `None` for [`Scope::Domain`].
    pub fn resource(&self) -> Option<&str> {
        match self {
            Scope::Domain => None,
            Scope::Resource(r) => Some(r),
            Scope::Action { resource, .. } => Some(resource),
            Scope::Attribute { resource, .. } => Some(resource),
        }
    }

    /// Does this scope apply to `target`?
    ///
    /// `is_attribute` distinguishes the two consultation modes: an operation
    /// check (`false`) never matches an [`Scope::Attribute`], and an attribute
    /// check (`true`) matches an [`Scope::Attribute`] only when the attribute
    /// name agrees. Broader scopes match in both modes.
    pub fn matches(&self, target: &Target, is_attribute: bool) -> bool {
        match self {
            Scope::Domain => true,
            Scope::Resource(r) => r == target.resource,
            Scope::Action { resource, action } => {
                resource == target.resource && action == target.action
            }
            Scope::Attribute {
                resource,
                attribute,
            } => {
                is_attribute
                    && resource == target.resource
                    && target.attribute == Some(attribute.as_str())
            }
        }
    }
}

/// Decides whether an action — or the visibility of one attribute — may proceed.
///
/// Every method defaults to [`Decision::NotApplicable`] — *no opinion* — so a
/// policy overrides only what it cares about, and overriding one method never
/// silently affects another. A read-only visibility policy implements just
/// [`authorize_attribute_read`](Policy::authorize_attribute_read) and leaves
/// operations at `NotApplicable`; an operation-gating policy implements
/// [`authorize`](Policy::authorize) and/or [`authorize_read`](Policy::authorize_read).
///
/// # `Allow` vs `NotApplicable`
///
/// For **operations**, the distinction is load-bearing under default-deny: to
/// admit an operation a policy must return an affirmative [`Decision::Allow`];
/// returning `NotApplicable` (or not overriding the method) leaves the operation
/// unadmitted. So a policy that means "these are allowed to write" must *say so*
/// with `Allow` — matching the scope is not consent. See [`PolicySet`] for the
/// full combining rule.
///
/// For **attribute reads** the two are equivalent: a field is visible unless a
/// policy `Forbid`s it, so both `Allow` and `NotApplicable` leave it visible.
/// (Default-denying un-policied fields would redact primary keys and break every
/// read — the operation gate is the fail-closed line; field policies narrow
/// within an already-authorized read.)
#[async_trait]
pub trait Policy: Send + Sync {
    /// Vote on the write action described by `changeset`: [`Decision::Allow`] to
    /// affirmatively permit it, [`Decision::Forbid`] to veto, or the default
    /// [`Decision::NotApplicable`] to abstain.
    async fn authorize(&self, changeset: &Changeset) -> Decision {
        let _ = changeset;
        Decision::NotApplicable
    }

    /// Vote on a read of `resource` via `action`, given the final `query` and the
    /// acting principal: [`Decision::Allow`] to permit, [`Decision::Forbid`] to
    /// veto, or the default [`Decision::NotApplicable`] to abstain.
    async fn authorize_read(
        &self,
        resource: &str,
        action: &str,
        query: &Query,
        actor: Option<&Record>,
    ) -> Decision {
        let _ = (resource, action, query, actor);
        Decision::NotApplicable
    }

    /// Whether this policy ever votes on **field visibility** — i.e. whether it
    /// overrides [`authorize_attribute_read`](Policy::authorize_attribute_read).
    ///
    /// The redaction pass consults every matching policy once **per row per
    /// attribute**, so at the default `max_rows` ceiling a single read can make
    /// tens of thousands of calls. A policy that only gates operations answers
    /// `NotApplicable` to every one of them; declaring `false` here lets the
    /// domain skip it entirely instead of awaiting that answer.
    ///
    /// **Defaults to `true`** — the fail-safe direction. An existing policy that
    /// does not know about this method keeps being consulted, so overriding it
    /// can only ever remove work a policy said it does not do. Return `false`
    /// only if this policy genuinely leaves `authorize_attribute_read` at its
    /// default; returning `false` while implementing it would silently disable
    /// your own redaction.
    ///
    /// This governs the **read** side only. Field *writes* are gated by
    /// [`authorize_attribute_write`](Policy::authorize_attribute_write), which is
    /// consulted regardless of what this returns — a forbidden write fails the
    /// whole action, so it is never skipped on a performance argument.
    fn redacts(&self) -> bool {
        true
    }

    /// Judge the acting principal *seeing* `attribute` on a returned `record` of
    /// `resource`. A [`Decision::Forbid`] redacts that attribute to
    /// [`Value::Null`](crate::Value) in the result rather than failing the read;
    /// [`Decision::Allow`] and the default [`Decision::NotApplicable`] both leave
    /// it visible (fields are visible unless forbidden).
    ///
    /// `record` is the full row as read from the layer, so a policy can decide
    /// per-row (e.g. hide `salary` unless the actor owns the row).
    async fn authorize_attribute_read(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        record: &Record,
        actor: Option<&Record>,
    ) -> Decision {
        let _ = (resource, action, attribute, record, actor);
        Decision::NotApplicable
    }

    /// Judge whether the acting principal may **set** `attribute` in the write
    /// described by `changeset`. Consulted once per attribute the *caller supplied*
    /// (the changeset's input params), after the operation-level
    /// [`authorize`](Policy::authorize) has admitted the write and before it
    /// persists.
    ///
    /// This is the write-side mirror of
    /// [`authorize_attribute_read`](Policy::authorize_attribute_read), but its
    /// veto is **stricter**: a read redacts a forbidden field to
    /// [`Value::Null`](crate::Value) and still returns the row, whereas a forbidden
    /// *write* cannot be silently dropped — a [`Decision::Forbid`] here **fails the
    /// whole write** with [`Error::Forbidden`](crate::Error::Forbidden). Like the
    /// read side, the field is **writable unless forbidden**: both
    /// [`Decision::Allow`] and the default [`Decision::NotApplicable`] permit it
    /// (a [`Decision::Error`] fails the write closed). Field-write policies narrow
    /// within an already-authorized write; they never widen it.
    async fn authorize_attribute_write(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        changeset: &Changeset,
        actor: Option<&Record>,
    ) -> Decision {
        let _ = (resource, action, attribute, changeset, actor);
        Decision::NotApplicable
    }
}

/// What a [`PolicyClient`] is being asked to authorize. A single, uniform shape
/// for all four checks so an external backend implements **one** method and
/// translates **one** request type into its own query (OPA input, Cedar entity
/// set, a REST body, …).
///
/// [`kind`](PolicyRequest::kind) says which check this is; [`attribute`](PolicyRequest::attribute)
/// is set only for [`PolicyCheck::AttributeRead`]; [`record`](PolicyRequest::record)
/// carries the changeset data on a write or the row under inspection on a field
/// read.
#[derive(Clone, Debug)]
pub struct PolicyRequest<'a> {
    /// Which of the four checks this is.
    pub kind: PolicyCheck,
    /// The resource being acted on.
    pub resource: &'a str,
    /// The action name.
    pub action: &'a str,
    /// The attribute under inspection, for [`PolicyCheck::AttributeRead`] only.
    pub attribute: Option<&'a str>,
    /// The acting principal (see [`Context::actor`](crate::Context::actor)).
    pub actor: Option<&'a Record>,
    /// The relevant record: a write's changeset data, or the row being read for
    /// a field check. `None` for an operation-level read.
    pub record: Option<&'a Record>,
    /// The prepared query, for [`PolicyCheck::Read`] only.
    pub query: Option<&'a Query>,
}

/// Which authorization question a [`PolicyRequest`] represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyCheck {
    /// A write action (create/update/destroy/generic).
    Write,
    /// A read action (operation level).
    Read,
    /// The visibility of a single attribute on a returned row.
    AttributeRead,
    /// The permission to *set* a single attribute in a write. Unlike
    /// [`AttributeRead`](PolicyCheck::AttributeRead) (which redacts on veto), a
    /// vetoed attribute write **aborts the whole write** — a field being set
    /// cannot be silently dropped.
    AttributeWrite,
}

/// The seam for an **external authorization backend** — the "bring your own
/// policy client" entry point.
///
/// Where [`Policy`] has four methods, a client answers every check through one
/// [`authorize`](PolicyClient::authorize) call over a [`PolicyRequest`], so an
/// OPA/Cedar/Oso/REST/gRPC integration only implements this and translates the
/// request into its own protocol. Wrap the client in [`ClientPolicy`] to use it
/// anywhere a [`Policy`] is expected (i.e. inside a [`ScopedPolicy`]).
///
/// The client is free to hold whatever async state it needs (an HTTP client, a
/// connection pool, a compiled Cedar policy set). On a backend failure it should
/// return [`Decision::Error`] — never a false [`Allow`](Decision::Allow) — so the
/// domain fails the action closed.
///
/// # Mapping a backend verdict to a [`Decision`]
///
/// An external engine that renders a verdict (OPA/Cedar returning "allow" or
/// "deny") should map:
///
/// - a positive verdict → [`Decision::Allow`] — an **affirmative** vote. Because
///   the domain is default-deny and admit-on-affirmative, a client that only ever
///   returned `NotApplicable` would never admit anything, so a real "allow" must
///   be `Allow`.
/// - a negative verdict → [`Decision::Forbid`] (with the engine's reason);
/// - an engine that can genuinely *abstain* (e.g. "no policy applies to this
///   input") → [`Decision::NotApplicable`], letting other registered policies (or
///   the set default) decide;
/// - unreachable / timeout / malformed response → [`Decision::Error`], which the
///   domain turns into [`Error::PolicyError`](crate::Error::PolicyError),
///   fail-closed.
///
/// Most engines are the authority for the whole scope they're wrapped in, so the
/// common mapping is simply allow → `Allow`, deny → `Forbid`, failure → `Error`.
#[async_trait]
pub trait PolicyClient: Send + Sync {
    /// Decide `request`. Return [`Decision::Error`] if the backend could not be
    /// reached or produced no verdict; a positive verdict must be an affirmative
    /// [`Decision::Allow`] (see the trait docs on mapping verdicts).
    async fn authorize(&self, request: PolicyRequest<'_>) -> Decision;
}

/// Adapts any [`PolicyClient`] into a [`Policy`], routing each of the four
/// [`Policy`] methods into a single [`PolicyClient::authorize`] call with the
/// matching [`PolicyRequest`]. This is what lets a user drop an external
/// authorization backend straight into a [`ScopedPolicy`]:
///
/// ```
/// # use std::sync::Arc;
/// # use async_trait::async_trait;
/// # use ash_domain::{ClientPolicy, Decision, PolicyClient, PolicyRequest, PolicySet, ScopedPolicy};
/// struct MyOpaClient; // holds an HTTP client, base URL, etc.
/// #[async_trait]
/// impl PolicyClient for MyOpaClient {
///     async fn authorize(&self, req: PolicyRequest<'_>) -> Decision {
///         // ... call out to OPA; on network failure:
///         // return Decision::Error("opa unreachable".into());
///         let _ = req;
///         Decision::Allow
///     }
/// }
///
/// let policies = PolicySet::new().with(ScopedPolicy::resource(
///     "invoice",
///     "opa",
///     Arc::new(ClientPolicy::new(Arc::new(MyOpaClient))),
/// ));
/// # let _ = policies;
/// ```
pub struct ClientPolicy {
    client: Arc<dyn PolicyClient>,
}

impl ClientPolicy {
    /// Wrap a client so it can be used as a [`Policy`].
    pub fn new(client: Arc<dyn PolicyClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Policy for ClientPolicy {
    async fn authorize(&self, changeset: &Changeset) -> Decision {
        self.client
            .authorize(PolicyRequest {
                kind: PolicyCheck::Write,
                resource: &changeset.resource,
                action: &changeset.action,
                attribute: None,
                actor: changeset.actor.as_ref(),
                record: Some(&changeset.data),
                query: None,
            })
            .await
    }

    async fn authorize_read(
        &self,
        resource: &str,
        action: &str,
        query: &Query,
        actor: Option<&Record>,
    ) -> Decision {
        self.client
            .authorize(PolicyRequest {
                kind: PolicyCheck::Read,
                resource,
                action,
                attribute: None,
                actor,
                record: None,
                query: Some(query),
            })
            .await
    }

    async fn authorize_attribute_read(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        record: &Record,
        actor: Option<&Record>,
    ) -> Decision {
        self.client
            .authorize(PolicyRequest {
                kind: PolicyCheck::AttributeRead,
                resource,
                action,
                attribute: Some(attribute),
                actor,
                record: Some(record),
                query: None,
            })
            .await
    }

    async fn authorize_attribute_write(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        changeset: &Changeset,
        actor: Option<&Record>,
    ) -> Decision {
        self.client
            .authorize(PolicyRequest {
                kind: PolicyCheck::AttributeWrite,
                resource,
                action,
                attribute: Some(attribute),
                actor,
                // The changeset's staged data — what the write intends to persist.
                record: Some(&changeset.data),
                query: None,
            })
            .await
    }
}

/// Build a [`Policy`] from a **plain closure** over the uniform
/// [`PolicyRequest`] — the policy analogue of the pipeline's lambda adapters
/// ([`change_lambda`](crate::ActionDef::change_lambda),
/// [`validate_lambda`](crate::ActionDef::validate_lambda), …), for rules that
/// don't warrant a hand-written [`Policy`] type.
///
/// The closure receives every check — [`kind`](PolicyRequest::kind) says which
/// of the four it is — and returns a [`Decision`]. Under default-deny the
/// closure must **affirmatively** return [`Decision::Allow`] to admit an
/// operation; return [`Decision::NotApplicable`] to abstain, and for every
/// check the rule has no opinion on (attribute checks in particular, which are
/// visible-unless-forbidden). Routing reuses [`ClientPolicy`]'s translation,
/// so each check carries exactly what the corresponding [`Policy`] method
/// would see.
///
/// The closure is synchronous — a rule that needs IO implements [`Policy`] or
/// [`PolicyClient`] instead.
///
/// ```
/// use ash_domain::{policy_lambda, Decision, PolicyCheck, PolicySet, ScopedPolicy};
///
/// // Owner-only: admit a write iff the actor owns the record.
/// let owner_only = policy_lambda(|req| match req.kind {
///     PolicyCheck::Write => {
///         let actor_id = req.actor.and_then(|a| a.get("id"));
///         let owner_id = req.record.and_then(|r| r.get("owner_id"));
///         match (actor_id, owner_id) {
///             (Some(a), Some(o)) if a == o => Decision::Allow,
///             _ => Decision::Forbid("not the owner".into()),
///         }
///     }
///     // No opinion on reads or attribute checks.
///     _ => Decision::NotApplicable,
/// });
///
/// let policies =
///     PolicySet::new().with(ScopedPolicy::resource("todo", "owner-only", owner_only));
/// # let _ = policies;
/// ```
pub fn policy_lambda<F>(f: F) -> Arc<dyn Policy>
where
    F: Fn(&PolicyRequest<'_>) -> Decision + Send + Sync + 'static,
{
    /// The closure as a [`PolicyClient`], so [`ClientPolicy`] does the
    /// method-to-request translation once, not a second time here.
    struct LambdaClient<F>(F);

    #[async_trait]
    impl<F> PolicyClient for LambdaClient<F>
    where
        F: Fn(&PolicyRequest<'_>) -> Decision + Send + Sync,
    {
        async fn authorize(&self, request: PolicyRequest<'_>) -> Decision {
            (self.0)(&request)
        }
    }

    Arc::new(ClientPolicy::new(Arc::new(LambdaClient(f))))
}

/// A [`Policy`] tagged with the [`Scope`] at which it applies. Stored in a
/// [`PolicySet`]; built with the [`Scope`] constructors on [`ScopedPolicy`].
#[derive(Clone)]
pub struct ScopedPolicy {
    /// Where this policy applies.
    pub scope: Scope,
    /// A short human name, surfaced in the [`policy_tree`](PolicySet::tree).
    pub label: String,
    /// The policy itself.
    pub policy: Arc<dyn Policy>,
}

impl ScopedPolicy {
    /// A policy applying to the whole domain.
    pub fn domain(label: impl Into<String>, policy: Arc<dyn Policy>) -> Self {
        Self {
            scope: Scope::Domain,
            label: label.into(),
            policy,
        }
    }

    /// A policy applying to every action/attribute of one resource.
    pub fn resource(
        resource: impl Into<String>,
        label: impl Into<String>,
        policy: Arc<dyn Policy>,
    ) -> Self {
        Self {
            scope: Scope::Resource(resource.into()),
            label: label.into(),
            policy,
        }
    }

    /// A policy applying to one action of one resource.
    pub fn action(
        resource: impl Into<String>,
        action: impl Into<String>,
        label: impl Into<String>,
        policy: Arc<dyn Policy>,
    ) -> Self {
        Self {
            scope: Scope::Action {
                resource: resource.into(),
                action: action.into(),
            },
            label: label.into(),
            policy,
        }
    }

    /// A policy gating reads of one attribute of one resource.
    pub fn attribute(
        resource: impl Into<String>,
        attribute: impl Into<String>,
        label: impl Into<String>,
        policy: Arc<dyn Policy>,
    ) -> Self {
        Self {
            scope: Scope::Attribute {
                resource: resource.into(),
                attribute: attribute.into(),
            },
            label: label.into(),
            policy,
        }
    }
}

/// The ordered collection of [`ScopedPolicy`]s a [`Domain`](crate::Domain)
/// enforces.
///
/// The domain consults it three ways — [`authorize_write`](PolicySet::authorize_write),
/// [`authorize_read`](PolicySet::authorize_read), and
/// [`authorize_attribute_read`](PolicySet::authorize_attribute_read) — each
/// gathering the policies whose [`Scope`] matches the target.
///
/// # Operations: default-deny, admit-on-affirmative
///
/// An operation is **denied unless some matching policy affirmatively allows it,
/// and any matching policy may veto it.** Concretely, over the matching policies:
///
/// - any [`Forbid`](Decision::Forbid) or [`Error`](Decision::Error) → denied /
///   fail-closed (deny-overrides);
/// - else at least one affirmative [`Allow`](Decision::Allow) → allowed;
/// - else (only [`NotApplicable`](Decision::NotApplicable) votes, or nothing
///   matched) → the set's default: **deny**, unless the set is
///   [`permissive`](PolicySet::permissive).
///
/// The consequence to internalize: **matching a scope is not consent.** A policy
/// registered only to redact a field abstains (`NotApplicable`) on operations,
/// so it never widens the operation gate for the resource it is scoped to —
/// adding a policy can only *narrow* access, never open it. To *admit* an
/// operation, register a policy that returns an affirmative `Allow` (see
/// [`Admit`] and [`AllowAll`], or write one). An empty set denies everything;
/// [`PolicySet::permissive`] is the explicit escape for tests, prototypes, or a
/// domain gated outside `ash-domain`.
///
/// # Attribute visibility: visible-unless-forbidden
///
/// Field reads follow a **different** rule, stated here so it is never implicit:
/// once an operation is authorized, every attribute of the returned rows is
/// *visible unless a matching policy forbids it*. `Allow` and `NotApplicable`
/// both leave a field visible; only [`Forbid`](Decision::Forbid) redacts it (to
/// [`Value::Null`](crate::Value)), and a [`Decision::Error`] during a field check
/// still fails the whole read closed. Field redaction is opt-in per field
/// (register a [`Scope::Attribute`] policy — or a broader domain/resource policy
/// overriding [`authorize_attribute_read`](Policy::authorize_attribute_read)).
/// Default-denying every un-policied field would redact primary keys and break
/// every read, so the operation gate is the fail-closed line and field policies
/// narrow within it.
#[derive(Clone, Default)]
pub struct PolicySet {
    policies: Vec<ScopedPolicy>,
    /// When `true`, an operation target matched by no policy is allowed instead
    /// of denied. Set only by [`PolicySet::permissive`] /
    /// [`PolicySet::allow_unmatched`].
    permissive: bool,
}

impl PolicySet {
    /// An empty, **default-deny** set: every operation is forbidden until a
    /// matching policy allows it. See the [type docs](PolicySet) for the exact
    /// rule (and for how attribute visibility differs).
    pub fn new() -> Self {
        Self::default()
    }

    /// A set that **allows operations no policy matches** — the explicit
    /// "stated otherwise" escape from default-deny, for tests, prototypes, or a
    /// domain gated outside `ash-domain`. Registered policies still apply
    /// deny-overrides on the targets they match.
    pub fn permissive() -> Self {
        Self {
            policies: Vec::new(),
            permissive: true,
        }
    }

    /// Switch this set to allow unmatched operation targets (builder form of
    /// [`permissive`](PolicySet::permissive)).
    #[must_use]
    pub fn allow_unmatched(mut self) -> Self {
        self.permissive = true;
        self
    }

    /// Whether unmatched operation targets are allowed (`true`) or denied (the
    /// default, `false`).
    pub fn is_permissive(&self) -> bool {
        self.permissive
    }

    /// Add a scoped policy (builder style).
    pub fn with(mut self, policy: ScopedPolicy) -> Self {
        self.policies.push(policy);
        self
    }

    /// Add a scoped policy in place.
    pub fn push(&mut self, policy: ScopedPolicy) {
        self.policies.push(policy);
    }

    /// Every scoped policy in registration order.
    pub fn policies(&self) -> &[ScopedPolicy] {
        &self.policies
    }

    /// Combine the operation-level decisions of every policy matching `target`
    /// (attribute scopes excluded). The rule:
    ///
    /// 1. Any [`Forbid`](Decision::Forbid) or [`Error`](Decision::Error) wins
    ///    immediately — deny-overrides, fail-closed. It stops the scan and
    ///    propagates unchanged (never downgraded to allow).
    /// 2. Otherwise, if at least one matching policy voted an affirmative
    ///    [`Allow`](Decision::Allow), the operation is allowed.
    /// 3. Otherwise — only [`NotApplicable`](Decision::NotApplicable) votes, or
    ///    no policy matched at all — the set's default decides: `Forbid` under
    ///    **default-deny**, `Allow` under [`permissive`](PolicySet::permissive).
    ///
    /// The key change from a plain match-and-no-veto scheme: *matching is not
    /// consent*. A policy scoped broadly for field redaction that abstains on
    /// operations (`NotApplicable`) no longer admits writes on that resource;
    /// only an affirmative `Allow` opens the gate.
    async fn authorize_operation(
        &self,
        target: Target<'_>,
        changeset: Option<&Changeset>,
        query: Option<&Query>,
        actor: Option<&Record>,
    ) -> Decision {
        let mut matched = false;
        let mut affirmed = false;
        for scoped in &self.policies {
            if !scoped.scope.matches(&target, false) {
                continue;
            }
            matched = true;
            let decision = match (changeset, query) {
                (Some(cs), _) => scoped.policy.authorize(cs).await,
                (None, Some(q)) => {
                    scoped
                        .policy
                        .authorize_read(target.resource, target.action, q, actor)
                        .await
                }
                (None, None) => Decision::NotApplicable,
            };
            match decision {
                // Deny-overrides, fail-closed: propagate the veto unchanged.
                Decision::Forbid(_) | Decision::Error(_) => return decision,
                // An affirmative vote; keep scanning in case a later policy vetoes.
                Decision::Allow => affirmed = true,
                // No opinion — does not admit on its own.
                Decision::NotApplicable => {}
            }
        }
        if affirmed || self.permissive {
            Decision::Allow
        } else if matched {
            // Policies matched the target but none affirmatively allowed it.
            Decision::Forbid(format!(
                "no policy affirmatively allows action `{}` on `{}` (default-deny; matching \
                 policies abstained — add an affirmative Allow, e.g. an Admit policy)",
                target.action, target.resource
            ))
        } else {
            Decision::Forbid(format!(
                "no policy authorizes action `{}` on `{}` (default-deny; register a policy \
                 or use PolicySet::permissive)",
                target.action, target.resource
            ))
        }
    }

    /// **Dry-run** an operation decision: evaluate the policy set exactly as
    /// [`authorize_operation`](PolicySet::authorize_operation) would, but return an
    /// [`Explanation`] — the final [`Decision`], the label of the policy that
    /// *decided* it, and every matching policy's individual vote — **without**
    /// executing anything. Pure evaluation over the policies: no data layer, no
    /// extensions, no events. See [`explain_write`](PolicySet::explain_write) /
    /// [`explain_read`](PolicySet::explain_read).
    async fn explain_operation(
        &self,
        target: Target<'_>,
        changeset: Option<&Changeset>,
        query: Option<&Query>,
        actor: Option<&Record>,
    ) -> Explanation {
        let mut votes = Vec::new();
        let mut affirmed: Option<String> = None;
        for scoped in &self.policies {
            if !scoped.scope.matches(&target, false) {
                continue;
            }
            let decision = match (changeset, query) {
                (Some(cs), _) => scoped.policy.authorize(cs).await,
                (None, Some(q)) => {
                    scoped
                        .policy
                        .authorize_read(target.resource, target.action, q, actor)
                        .await
                }
                (None, None) => Decision::NotApplicable,
            };
            votes.push(PolicyVote {
                label: scoped.label.clone(),
                decision: decision.clone(),
            });
            match decision {
                // Deny-overrides: the first veto decides, and is the decider.
                Decision::Forbid(_) | Decision::Error(_) => {
                    return Explanation {
                        decision,
                        deciding_policy: Some(scoped.label.clone()),
                        votes,
                    };
                }
                // Remember the first affirmative vote as the would-be decider.
                Decision::Allow => {
                    if affirmed.is_none() {
                        affirmed = Some(scoped.label.clone());
                    }
                }
                Decision::NotApplicable => {}
            }
        }
        if let Some(label) = affirmed {
            Explanation {
                decision: Decision::Allow,
                deciding_policy: Some(label),
                votes,
            }
        } else if self.permissive {
            // The set default admitted it; no single policy decided.
            Explanation {
                decision: Decision::Allow,
                deciding_policy: None,
                votes,
            }
        } else if !votes.is_empty() {
            Explanation {
                decision: Decision::Forbid(format!(
                    "no policy affirmatively allows action `{}` on `{}` (default-deny; matching \
                     policies abstained — add an affirmative Allow, e.g. an Admit policy)",
                    target.action, target.resource
                )),
                deciding_policy: None,
                votes,
            }
        } else {
            Explanation {
                decision: Decision::Forbid(format!(
                    "no policy authorizes action `{}` on `{}` (default-deny; register a policy \
                     or use PolicySet::permissive)",
                    target.action, target.resource
                )),
                deciding_policy: None,
                votes,
            }
        }
    }

    /// **Dry-run** a *write* decision from its `changeset`: the diagnostic
    /// counterpart of [`authorize_write`](PolicySet::authorize_write). Returns the
    /// [`Explanation`] (final decision, deciding policy, every vote) and runs the
    /// operation gate **only** — no field-write pass, no persistence, no
    /// extensions.
    pub async fn explain_write(&self, changeset: &Changeset) -> Explanation {
        let target = Target {
            resource: &changeset.resource,
            action: &changeset.action,
            attribute: None,
        };
        self.explain_operation(target, Some(changeset), None, changeset.actor.as_ref())
            .await
    }

    /// **Dry-run** a *read* decision from its prepared `query`: the diagnostic
    /// counterpart of [`authorize_read`](PolicySet::authorize_read).
    pub async fn explain_read(
        &self,
        resource: &str,
        action: &str,
        query: &Query,
        actor: Option<&Record>,
    ) -> Explanation {
        let target = Target {
            resource,
            action,
            attribute: None,
        };
        self.explain_operation(target, None, Some(query), actor)
            .await
    }

    /// Authorize a write action from its `changeset`, combining all matching
    /// domain/resource/action policies by the operation rule (deny-overrides,
    /// then admit-on-affirmative, else the set default). See [`PolicySet`].
    pub async fn authorize_write(&self, changeset: &Changeset) -> Decision {
        let target = Target {
            resource: &changeset.resource,
            action: &changeset.action,
            attribute: None,
        };
        self.authorize_operation(target, Some(changeset), None, changeset.actor.as_ref())
            .await
    }

    /// Authorize a read action from its prepared `query`, by the same operation
    /// rule as [`authorize_write`](PolicySet::authorize_write).
    pub async fn authorize_read(
        &self,
        resource: &str,
        action: &str,
        query: &Query,
        actor: Option<&Record>,
    ) -> Decision {
        let target = Target {
            resource,
            action,
            attribute: None,
        };
        self.authorize_operation(target, None, Some(query), actor)
            .await
    }

    /// Judge the visibility of a single `attribute` on `record`. Consults every
    /// domain/resource/attribute policy that matches; action scopes are **not**
    /// consulted for attribute reads (they gate operations).
    ///
    /// **Visible-unless-forbidden** — the field rule, distinct from the operation
    /// rule above: a field is shown unless a matching policy vetoes it. A
    /// [`Forbid`](Decision::Forbid) redacts the field (and stops the scan); a
    /// [`Decision::Error`] fails the whole read closed (propagated by the
    /// caller). Both [`Allow`](Decision::Allow) and
    /// [`NotApplicable`](Decision::NotApplicable) leave the field visible, so an
    /// un-policied field is *not* redacted (default-denying every field would
    /// null primary keys). The returned `Allow` here means "not forbidden",
    /// unifying the affirmative and abstaining cases for the caller.
    pub async fn authorize_attribute_read(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        record: &Record,
        actor: Option<&Record>,
    ) -> Decision {
        let target = Target {
            resource,
            action,
            attribute: Some(attribute),
        };
        for scoped in &self.policies {
            // Action-scoped policies gate the operation, not field visibility.
            if matches!(scoped.scope, Scope::Action { .. }) {
                continue;
            }
            // A policy that declares it never votes on field visibility is
            // skipped without awaiting it — this runs once per row per
            // attribute, so an operation-only policy in the set would otherwise
            // cost an await per field for nothing.
            if !scoped.policy.redacts() {
                continue;
            }
            if !scoped.scope.matches(&target, true) {
                continue;
            }
            let decision = scoped
                .policy
                .authorize_attribute_read(resource, action, attribute, record, actor)
                .await;
            // Only a veto (Forbid) or a backend Error acts; an abstaining
            // NotApplicable (and an affirmative Allow) leave the field visible,
            // so keep scanning for a later veto rather than short-circuiting.
            if decision.is_veto() {
                return decision;
            }
        }
        Decision::Allow
    }

    /// Judge whether a single `attribute` may be **set** in the write described by
    /// `changeset`. Consults every domain/resource/attribute policy that matches;
    /// action scopes are **not** consulted (they gate the operation).
    ///
    /// **Writable-unless-forbidden** — the same shape as
    /// [`authorize_attribute_read`](PolicySet::authorize_attribute_read), and for
    /// the same reason: an operation is already default-denied at the gate, so a
    /// field is settable unless a matching policy vetoes it. Requiring an
    /// affirmative field-write `Allow` for every column would make every write
    /// demand a policy per attribute and break the moment a new attribute is added.
    /// A [`Forbid`](Decision::Forbid) here is a hard veto (the caller aborts the
    /// whole write — a field being set cannot be silently dropped the way a read is
    /// nulled); a [`Decision::Error`] fails the write closed; both
    /// [`Allow`](Decision::Allow) and [`NotApplicable`](Decision::NotApplicable)
    /// leave the field writable.
    pub async fn authorize_attribute_write(
        &self,
        resource: &str,
        action: &str,
        attribute: &str,
        changeset: &Changeset,
        actor: Option<&Record>,
    ) -> Decision {
        let target = Target {
            resource,
            action,
            attribute: Some(attribute),
        };
        for scoped in &self.policies {
            // Action-scoped policies gate the operation, not field writes.
            if matches!(scoped.scope, Scope::Action { .. }) {
                continue;
            }
            if !scoped.scope.matches(&target, true) {
                continue;
            }
            let decision = scoped
                .policy
                .authorize_attribute_write(resource, action, attribute, changeset, actor)
                .await;
            // Only a veto (Forbid) or backend Error acts; keep scanning otherwise.
            if decision.is_veto() {
                return decision;
            }
        }
        Decision::Allow
    }

    /// `true` if any policy could veto a field write, i.e. the domain must run the
    /// caller's supplied fields through
    /// [`authorize_attribute_write`](PolicySet::authorize_attribute_write). Same
    /// conservative rule as [`has_attribute_policies`](PolicySet::has_attribute_policies):
    /// only pure [`Scope::Action`] policies never gate a field write, so a
    /// non-empty set holding anything else means fields are checked. Lets the
    /// domain skip the per-field write pass for the common operation-only case.
    pub fn has_attribute_write_policies(&self) -> bool {
        self.policies
            .iter()
            .any(|p| !matches!(p.scope, Scope::Action { .. }))
    }

    /// `true` if any policy could redact a field, i.e. the domain must walk
    /// returned rows through [`authorize_attribute_read`](PolicySet::authorize_attribute_read).
    ///
    /// Two things exclude a policy: an [`Scope::Action`] scope (it gates the
    /// operation, never field visibility), and a policy that declares
    /// [`redacts`](Policy::redacts) `false` (it does not implement
    /// `authorize_attribute_read` at all). Everything else is assumed to redact,
    /// because the default of `redacts` is `true` — a policy that says nothing is
    /// still consulted.
    ///
    /// This is what lets the domain skip the per-row pass entirely for the common
    /// operation-only case, and the per-row pass is expensive: one call per row
    /// per attribute.
    pub fn has_attribute_policies(&self) -> bool {
        self.policies
            .iter()
            .any(|p| !matches!(p.scope, Scope::Action { .. }) && p.policy.redacts())
    }

    /// Build a [`PolicyNode`] tree describing every registered policy grouped by
    /// where it applies — domain at the root, resources beneath, then actions
    /// and attributes. Pure introspection for docs, admin UIs, and debugging.
    pub fn tree(&self) -> PolicyNode {
        let mut root = PolicyNode::new(PolicyLevel::Domain, "domain");

        // Domain-scoped policies attach to the root.
        for p in self.policies.iter().filter(|p| p.scope == Scope::Domain) {
            root.policies.push(p.label.clone());
        }

        // Group everything else by resource.
        let mut resources: Vec<String> = self
            .policies
            .iter()
            .filter_map(|p| p.scope.resource().map(str::to_string))
            .collect();
        resources.sort();
        resources.dedup();

        for resource in resources {
            let mut rnode = PolicyNode::new(PolicyLevel::Resource, &resource);
            for p in &self.policies {
                match &p.scope {
                    Scope::Resource(r) if *r == resource => {
                        rnode.policies.push(p.label.clone());
                    }
                    Scope::Action {
                        resource: r,
                        action,
                    } if *r == resource => {
                        rnode
                            .child_mut(PolicyLevel::Action, action)
                            .policies
                            .push(p.label.clone());
                    }
                    Scope::Attribute {
                        resource: r,
                        attribute,
                    } if *r == resource => {
                        rnode
                            .child_mut(PolicyLevel::Attribute, attribute)
                            .policies
                            .push(p.label.clone());
                    }
                    _ => {}
                }
            }
            root.children.push(rnode);
        }
        root
    }
}

/// The level of a node in a [`PolicyNode`] tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyLevel {
    /// The domain root.
    Domain,
    /// A resource.
    Resource,
    /// An action of a resource.
    Action,
    /// An attribute of a resource.
    Attribute,
}

impl PolicyLevel {
    fn tag(self) -> &'static str {
        match self {
            PolicyLevel::Domain => "domain",
            PolicyLevel::Resource => "resource",
            PolicyLevel::Action => "action",
            PolicyLevel::Attribute => "attribute",
        }
    }
}

/// One node of the policy tree produced by [`PolicySet::tree`]: a level, the
/// name at that level, the labels of policies applied directly there, and any
/// child nodes.
#[derive(Clone, Debug)]
pub struct PolicyNode {
    /// This node's level.
    pub level: PolicyLevel,
    /// The name at this level (`"domain"`, a resource, action, or attribute).
    pub name: String,
    /// Labels of the policies attached directly at this node.
    pub policies: Vec<String>,
    /// Nested nodes (a resource's actions/attributes).
    pub children: Vec<PolicyNode>,
}

impl PolicyNode {
    fn new(level: PolicyLevel, name: impl Into<String>) -> Self {
        Self {
            level,
            name: name.into(),
            policies: Vec::new(),
            children: Vec::new(),
        }
    }

    /// Get-or-create the child of `level`/`name` under this node.
    fn child_mut(&mut self, level: PolicyLevel, name: &str) -> &mut PolicyNode {
        if let Some(idx) = self
            .children
            .iter()
            .position(|c| c.level == level && c.name == name)
        {
            &mut self.children[idx]
        } else {
            self.children.push(PolicyNode::new(level, name));
            self.children.last_mut().unwrap()
        }
    }

    /// Render the subtree as an indented, human-readable outline.
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.render_into(&mut out, 0);
        out
    }

    fn render_into(&self, out: &mut String, depth: usize) {
        let indent = "  ".repeat(depth);
        let policies = if self.policies.is_empty() {
            String::new()
        } else {
            format!(" [{}]", self.policies.join(", "))
        };
        out.push_str(&format!(
            "{indent}{}: {}{}\n",
            self.level.tag(),
            self.name,
            policies
        ));
        for child in &self.children {
            child.render_into(out, depth + 1);
        }
    }
}

/// The report a read returns alongside its rows: which attributes were redacted
/// to [`Value::Null`](crate::Value) by an [`authorize_attribute_read`](Policy::authorize_attribute_read)
/// denial, and why. The rows themselves already have the offending fields
/// nulled; this tells the caller *what* was hidden so it can present it
/// differently (drop the field, mask it, surface a notice) per its own rules.
#[derive(Clone, Debug, Default)]
pub struct ReadReport {
    /// One entry per (row index, attribute) that was redacted.
    pub redactions: Vec<Redaction>,
}

impl ReadReport {
    /// `true` if nothing was redacted.
    pub fn is_clean(&self) -> bool {
        self.redactions.is_empty()
    }

    /// The distinct attribute names that were redacted anywhere in the result.
    pub fn redacted_attributes(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self
            .redactions
            .iter()
            .map(|r| r.attribute.as_str())
            .collect();
        names.sort_unstable();
        names.dedup();
        names
    }
}

/// The result of a **dry-run** authorization: what the policy set *would* decide,
/// which policy decided it, and how each matching policy voted — produced by
/// [`PolicySet::explain_write`] / [`PolicySet::explain_read`] (and the
/// [`Domain`](crate::Domain) `explain_*` wrappers) without running the action.
///
/// It is purely diagnostic: no data layer, extension, or event is touched
/// producing it, so it is safe to call for a "would this be allowed / why?"
/// answer ahead of (or instead of) the real request. The `decision` is
/// fail-closed identical to what the live gate would return.
#[derive(Clone, Debug)]
pub struct Explanation {
    /// The decision the live gate would reach for this target.
    pub decision: Decision,
    /// The label of the policy that *decided* the outcome — the first vetoing
    /// policy under deny-overrides, or the first affirmative `Allow`. `None` when
    /// no single policy decided: a permissive-default allow, or a default-deny
    /// with no affirmative vote.
    pub deciding_policy: Option<String>,
    /// Every matching policy's individual vote, in evaluation order. On a veto the
    /// scan stops, so the last entry is the deciding veto.
    pub votes: Vec<PolicyVote>,
}

impl Explanation {
    /// Whether the dry-run decision is an affirmative allow.
    pub fn is_allowed(&self) -> bool {
        self.decision.is_allowed()
    }
}

/// One matching policy's vote in an [`Explanation`]: its label and the
/// [`Decision`] it returned.
#[derive(Clone, Debug)]
pub struct PolicyVote {
    /// The policy's label (as registered in the [`ScopedPolicy`]).
    pub label: String,
    /// The decision it cast for this target.
    pub decision: Decision,
}

/// A single attribute redacted from a single returned row.
#[derive(Clone, Debug)]
pub struct Redaction {
    /// Index of the row in the returned slice.
    pub row: usize,
    /// The attribute that was nulled.
    pub attribute: String,
    /// The policy's reason.
    pub reason: String,
}

/// Rows plus the [`ReadReport`] describing any field redactions applied to them.
/// Returned by the domain's authorized-read entry points.
#[derive(Clone, Debug)]
pub struct AuthorizedRead<T> {
    /// The returned rows, with forbidden attributes already nulled.
    pub rows: Vec<T>,
    /// What was redacted and why.
    pub report: ReadReport,
}

/// One row (or none) plus the [`ReadReport`] — the single-row analogue of
/// [`AuthorizedRead`], returned by the report-carrying `get` terminals of the
/// [`ReadRequest`](crate::read::ReadRequest) builder.
#[derive(Clone, Debug)]
pub struct AuthorizedOne<T> {
    /// The row, if the primary key matched — forbidden attributes already
    /// nulled.
    pub row: Option<T>,
    /// What was redacted and why.
    pub report: ReadReport,
}

/// A convenience [`Policy`] that forbids one thing with a fixed reason,
/// depending on how it is scoped. Useful for quick rules and tests; real
/// deployments implement [`Policy`] with their own logic.
///
/// It denies operations (`authorize`/`authorize_read`), attribute reads, **and**
/// attribute writes unconditionally, so scope it narrowly (e.g. an
/// [`Scope::Attribute`] to hide *and* freeze a field, or an [`Scope::Action`] to
/// disable an operation). Note an attribute-scoped `Deny` therefore both redacts
/// the field on read **and** forbids setting it on write; if you want only one,
/// write a policy that overrides just that method.
pub struct Deny(pub String);

#[async_trait]
impl Policy for Deny {
    async fn authorize(&self, _changeset: &Changeset) -> Decision {
        Decision::Forbid(self.0.clone())
    }
    async fn authorize_read(
        &self,
        _resource: &str,
        _action: &str,
        _query: &Query,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Forbid(self.0.clone())
    }
    async fn authorize_attribute_read(
        &self,
        _resource: &str,
        _action: &str,
        _attribute: &str,
        _record: &Record,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Forbid(self.0.clone())
    }
    async fn authorize_attribute_write(
        &self,
        _resource: &str,
        _action: &str,
        _attribute: &str,
        _changeset: &Changeset,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Forbid(self.0.clone())
    }
}

/// A policy that **affirmatively permits** every operation it is scoped to — the
/// building block for the recommended pattern of "one admit policy + narrowing
/// forbid policies".
///
/// Under default-deny/admit-on-affirmative this is *not* equivalent to
/// registering nothing: a domain-scoped `AllowAll` casts an affirmative
/// [`Allow`](Decision::Allow) on every operation (opening the gate everywhere,
/// like [`PolicySet::permissive`] but visible in the [policy tree](PolicySet::tree)),
/// and a narrower [`Scope`] opens just that resource or action. It stays out of
/// the way of field visibility — `authorize_attribute_read` abstains, so it
/// redacts nothing.
///
/// [`Admit`] is the same thing with a clearer name at the call site; they are
/// interchangeable.
pub struct AllowAll;

#[async_trait]
impl Policy for AllowAll {
    // Operations only — it abstains on field visibility, so the domain can skip
    // it in the per-row redaction pass entirely.
    fn redacts(&self) -> bool {
        false
    }
    async fn authorize(&self, _changeset: &Changeset) -> Decision {
        Decision::Allow
    }
    async fn authorize_read(
        &self,
        _resource: &str,
        _action: &str,
        _query: &Query,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Allow
    }
    // authorize_attribute_read stays NotApplicable (default): admitting an
    // operation is not the same as un-redacting a field, so this never
    // overrides another policy's field veto.
}

/// Affirmatively admits every operation it is scoped to — an alias for
/// [`AllowAll`] that reads better where the intent is "these are allowed".
///
/// The recommended shape under default-deny is one narrow `Admit` (or several)
/// plus [`Deny`]/custom policies that narrow within it:
///
/// ```
/// # use std::sync::Arc;
/// # use ash_domain::{Admit, Deny, PolicySet, ScopedPolicy};
/// let policies = PolicySet::new()
///     // Admit reads and writes of `todo` in the first place…
///     .with(ScopedPolicy::resource("todo", "admit-todo", Arc::new(Admit)))
///     // …then hide one field within that.
///     .with(ScopedPolicy::attribute("todo", "secret", "hide-secret", Arc::new(Deny("hidden".into()))));
/// # let _ = policies;
/// ```
pub struct Admit;

#[async_trait]
impl Policy for Admit {
    // Operations only, like `AllowAll` — nothing to consult per field.
    fn redacts(&self) -> bool {
        false
    }
    async fn authorize(&self, _changeset: &Changeset) -> Decision {
        Decision::Allow
    }
    async fn authorize_read(
        &self,
        _resource: &str,
        _action: &str,
        _query: &Query,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Allow
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::action::ActionKind;
    use crate::value::Value;

    /// A policy that votes a fixed [`Decision`] on every question asked of it.
    /// Every combination of votes below is built from these, so the tests
    /// exercise the *combination rule* rather than any one policy's judgement.
    struct Vote(Decision);
    impl Vote {
        fn arc(decision: &Decision) -> Arc<dyn Policy> {
            Arc::new(Vote(decision.clone()))
        }
    }
    #[async_trait]
    impl Policy for Vote {
        async fn authorize(&self, _cs: &Changeset) -> Decision {
            self.0.clone()
        }
        async fn authorize_read(
            &self,
            _resource: &str,
            _action: &str,
            _query: &Query,
            _actor: Option<&Record>,
        ) -> Decision {
            self.0.clone()
        }
        async fn authorize_attribute_read(
            &self,
            _resource: &str,
            _action: &str,
            _attribute: &str,
            _record: &Record,
            _actor: Option<&Record>,
        ) -> Decision {
            self.0.clone()
        }
        async fn authorize_attribute_write(
            &self,
            _resource: &str,
            _action: &str,
            _attribute: &str,
            _cs: &Changeset,
            _actor: Option<&Record>,
        ) -> Decision {
            self.0.clone()
        }
    }

    /// The whole vote space, once. Every test below enumerates over this rather
    /// than sampling it — with four decisions and up to three policies the space
    /// is 84 cases, so exhausting it is cheaper than a random search and proves
    /// more.
    fn votes() -> Vec<Decision> {
        vec![
            Decision::Allow,
            Decision::Forbid("no".into()),
            Decision::Error("backend down".into()),
            Decision::NotApplicable,
        ]
    }

    /// Every combination of up to `max` votes, including the empty set.
    fn combinations(max: usize) -> Vec<Vec<Decision>> {
        let mut out = vec![Vec::new()];
        let mut frontier = vec![Vec::new()];
        for _ in 0..max {
            let mut next = Vec::new();
            for prefix in &frontier {
                for vote in votes() {
                    let mut combo = prefix.clone();
                    combo.push(vote);
                    next.push(combo);
                }
            }
            out.extend(next.clone());
            frontier = next;
        }
        out
    }

    fn set(votes: &[Decision], permissive: bool) -> PolicySet {
        let mut policies = if permissive {
            PolicySet::permissive()
        } else {
            PolicySet::new()
        };
        for (i, vote) in votes.iter().enumerate() {
            policies.push(ScopedPolicy::resource(
                "note",
                format!("vote-{i}"),
                Vote::arc(vote),
            ));
        }
        policies
    }

    fn changeset() -> Changeset {
        Changeset::new("note", "create", ActionKind::Write, Record::new())
    }

    fn is_allow(d: &Decision) -> bool {
        matches!(d, Decision::Allow)
    }

    #[tokio::test]
    async fn an_operation_is_allowed_only_by_an_affirmative_allow() {
        // The load-bearing invariant, over the whole vote space: under
        // default-deny, an operation is permitted if and only if some policy
        // said Allow and none vetoed. No combination of abstentions admits.
        for combo in combinations(3) {
            let decision = set(&combo, false).authorize_write(&changeset()).await;
            let vetoed = combo.iter().any(Decision::is_veto);
            let affirmed = combo.iter().any(is_allow);
            assert_eq!(
                is_allow(&decision),
                affirmed && !vetoed,
                "votes {combo:?} resolved to {decision:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_veto_beats_any_number_of_allows() {
        for combo in combinations(3) {
            if !combo.iter().any(Decision::is_veto) {
                continue;
            }
            for permissive in [false, true] {
                let decision = set(&combo, permissive).authorize_write(&changeset()).await;
                assert!(
                    !is_allow(&decision),
                    "votes {combo:?} (permissive={permissive}) resolved to {decision:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_backend_error_stays_an_error_and_is_never_downgraded() {
        // A policy backend that failed must not be reported as a deliberate
        // Forbid: the domain maps the two to distinct errors, and conflating
        // them would hide an outage behind an authorization decision.
        for combo in combinations(3) {
            let first_veto = combo.iter().find(|d| d.is_veto());
            let Some(Decision::Error(_)) = first_veto else {
                continue;
            };
            let decision = set(&combo, false).authorize_write(&changeset()).await;
            assert!(
                matches!(decision, Decision::Error(_)),
                "votes {combo:?} resolved to {decision:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_first_veto_in_registration_order_wins() {
        let combo = vec![
            Decision::Allow,
            Decision::Forbid("first".into()),
            Decision::Error("second".into()),
        ];
        let decision = set(&combo, false).authorize_write(&changeset()).await;
        let Decision::Forbid(reason) = decision else {
            panic!("expected the earlier Forbid to win, got {decision:?}");
        };
        assert_eq!(reason, "first");
    }

    #[tokio::test]
    async fn an_empty_set_denies_and_a_permissive_set_allows() {
        assert!(!is_allow(
            &PolicySet::new().authorize_write(&changeset()).await
        ));
        assert!(is_allow(
            &PolicySet::permissive().authorize_write(&changeset()).await
        ));
    }

    #[tokio::test]
    async fn abstention_alone_never_admits_even_when_policies_matched() {
        // "Matching is not consent": a broadly-scoped policy that exists for
        // field redaction must not admit operations by being present.
        let combo = vec![Decision::NotApplicable, Decision::NotApplicable];
        let decision = set(&combo, false).authorize_write(&changeset()).await;
        let Decision::Forbid(reason) = decision else {
            panic!("expected default-deny, got {decision:?}");
        };
        assert!(reason.contains("abstained"), "{reason}");
    }

    #[tokio::test]
    async fn reads_resolve_by_the_same_rule_as_writes() {
        for combo in combinations(2) {
            let policies = set(&combo, false);
            let write = policies.authorize_write(&changeset()).await;
            let read = policies
                .authorize_read("note", "create", &Query::new("note"), None)
                .await;
            assert_eq!(
                is_allow(&write),
                is_allow(&read),
                "votes {combo:?}: write {write:?} vs read {read:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_attribute_is_visible_unless_a_policy_vetoes_it() {
        // The field rule is the inverse of the operation rule, on purpose:
        // default-denying every field would null primary keys. Only a veto acts.
        let record = Record::from_iter([("title", Value::from("hi"))]);
        for combo in combinations(3) {
            let decision = set(&combo, false)
                .authorize_attribute_read("note", "read", "title", &record, None)
                .await;
            let vetoed = combo.iter().any(Decision::is_veto);
            assert_eq!(
                is_allow(&decision),
                !vetoed,
                "votes {combo:?} resolved to {decision:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_attribute_write_is_permitted_unless_a_policy_vetoes_it() {
        let cs = changeset();
        for combo in combinations(3) {
            let decision = set(&combo, false)
                .authorize_attribute_write("note", "create", "title", &cs, None)
                .await;
            let vetoed = combo.iter().any(Decision::is_veto);
            assert_eq!(
                is_allow(&decision),
                !vetoed,
                "votes {combo:?} resolved to {decision:?}"
            );
        }
    }

    #[tokio::test]
    async fn an_action_scope_gates_operations_but_never_fields() {
        // An action-scoped policy is consulted for the operation and skipped for
        // attribute visibility — otherwise "you may not destroy" would silently
        // become "you may not see".
        let policies = PolicySet::permissive().with(ScopedPolicy::action(
            "note",
            "read",
            "action-veto",
            Vote::arc(&Decision::Forbid("no".into())),
        ));
        let record = Record::from_iter([("title", Value::from("hi"))]);

        let operation = policies
            .authorize_read("note", "read", &Query::new("note"), None)
            .await;
        assert!(!is_allow(&operation), "the operation is gated");

        let field = policies
            .authorize_attribute_read("note", "read", "title", &record, None)
            .await;
        assert!(is_allow(&field), "the field is not");
    }

    #[tokio::test]
    async fn a_scope_only_matches_its_own_target() {
        let policies = PolicySet::new().with(ScopedPolicy::resource(
            "other",
            "admit-other",
            Arc::new(Admit),
        ));
        // An Admit scoped to another resource must not open this one.
        let decision = policies.authorize_write(&changeset()).await;
        assert!(!is_allow(&decision), "got {decision:?}");
    }
}
