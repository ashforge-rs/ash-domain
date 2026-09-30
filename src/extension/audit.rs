//! The audit-trail extension, built on [`ash_log`].
//!
//! Attach an [`AuditExtension`] to a domain to record every write action to a
//! security audit log. After an action is persisted, the extension builds an
//! [`ash_log::AuditEvent`] — carrying the resource, action, acting principal, and
//! result — and hands it to a pluggable [`ash_log::AuditBackend`]. This is a
//! genuine reuse of `ash-log`'s event/backend model (append-only, tamper-evident,
//! compliance-ready) rather than a bespoke logger.
//!
//! Reads are audited too, through `after_read`, so a paper trail can cover access
//! as well as change. **Denied** actions are audited as well, through
//! [`on_denied`](Extension::on_denied): the executor calls that hook at the
//! authorization gate — the one place a denial is observable, since the other
//! hooks only ever run once an action is *authorized*. So the trail records both
//! what happened and what was refused (`AuditResult::Denied`), closing the blind
//! spot an `after_*`-only audit would leave.
//!
//! ```
//! use std::sync::Arc;
//! use ash_domain::extension::audit::AuditExtension;
//! use ash_domain::ash_log::NoopAuditBackend;
//!
//! // Audit every resource, writing to any `AuditBackend` (here a no-op; use
//! // `StdoutAuditBackend`, a file backend, or a `MultiAuditBackend` in anger).
//! let ext = AuditExtension::new(Arc::new(NoopAuditBackend));
//!
//! // Scope it to specific resources, and name the actor attribute that holds
//! // the principal id (defaults to `"id"`).
//! let scoped = AuditExtension::new(Arc::new(NoopAuditBackend))
//!     .for_resource("account")
//!     .principal_attribute("email");
//! let _ = (Arc::new(ext), Arc::new(scoped));
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use ash_log::{AuditBackend, AuditEvent, AuditEventType, AuditResult};
use async_trait::async_trait;

use crate::action::{ActionResult, Changeset};
use crate::error::Result;
use crate::extension::Extension;
use crate::value::{Record, Value};

/// Records a resource's actions to an [`ash_log`] audit backend.
pub struct AuditExtension {
    backend: Arc<dyn AuditBackend>,
    /// Resources to audit; empty means *all* resources.
    resources: HashSet<String>,
    /// The actor attribute whose value is the principal id.
    principal_attribute: String,
}

impl AuditExtension {
    /// Audit actions on every resource, writing events to `backend`.
    pub fn new(backend: Arc<dyn AuditBackend>) -> Self {
        Self {
            backend,
            resources: HashSet::new(),
            principal_attribute: "id".to_string(),
        }
    }

    /// Restrict auditing to `resource`. Call repeatedly to allow several; with no
    /// call, every resource is audited.
    #[must_use]
    pub fn for_resource(mut self, resource: impl Into<String>) -> Self {
        self.resources.insert(resource.into());
        self
    }

    /// The actor attribute that carries the principal id (default `"id"`).
    #[must_use]
    pub fn principal_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.principal_attribute = attribute.into();
        self
    }

    /// Whether this extension audits `resource`.
    fn audits(&self, resource: &str) -> bool {
        self.resources.is_empty() || self.resources.contains(resource)
    }

    /// The principal id pulled from the acting record, if any.
    fn principal(&self, actor: Option<&Record>) -> Option<String> {
        actor
            .and_then(|a| a.get(&self.principal_attribute))
            .and_then(Value::as_str)
            .map(str::to_owned)
    }

    /// Emit an event to the backend for a persisted write.
    fn record_write(&self, changeset: &Changeset, result: AuditResult) {
        let mut builder = AuditEvent::builder()
            // Create/update/destroy/generic are all state-changing method
            // invocations; the `action` metadata distinguishes them.
            .event_type(AuditEventType::MethodInvocation)
            .method(format!("{}.{}", changeset.resource, changeset.action))
            .result(result)
            .metadata("resource", changeset.resource.clone())
            .metadata("action", changeset.action.clone());

        if let Some(principal) = self.principal(changeset.actor.as_ref()) {
            builder = builder.principal(principal);
        }
        if let Some(params) = record_to_json(&changeset.data) {
            builder = builder.params(params);
        }

        self.backend.log_audit(&builder.build());
    }

    /// Emit an event to the backend for an action the authorization gate
    /// **denied**. Distinct from [`record_write`](Self::record_write): the
    /// event is an [`AuthorizationCheck`](AuditEventType::AuthorizationCheck) with
    /// a [`Denied`](AuditResult::Denied) result at
    /// [`Critical`](ash_log::AuditSeverity) severity, carrying the denial reason —
    /// exactly the access-denied record `ash-log` is built for.
    ///
    /// No params are attached: a denied action never staged an observable
    /// changeset, and the point of the record is the *attempt*, not its payload.
    fn record_denied(&self, resource: &str, action: &str, actor: Option<&Record>, reason: &str) {
        let mut builder = AuditEvent::builder()
            .event_type(AuditEventType::AuthorizationCheck)
            .method(format!("{resource}.{action}"))
            .result(AuditResult::Denied)
            .severity(ash_log::AuditSeverity::Critical)
            .metadata("resource", resource.to_owned())
            .metadata("action", action.to_owned())
            .error(reason.to_owned());

        if let Some(principal) = self.principal(actor) {
            builder = builder.principal(principal);
        }

        self.backend.log_audit(&builder.build());
    }
}

/// Best-effort conversion of a [`Record`] to a `serde_json::Value` object for the
/// event's `params`. Returns `None` if the record is empty.
fn record_to_json(record: &Record) -> Option<serde_json::Value> {
    let map: serde_json::Map<String, serde_json::Value> = record
        .iter()
        .map(|(k, v)| (k.clone(), value_to_json(v)))
        .collect();
    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(map))
    }
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(n) | Value::Timestamp(n) => serde_json::Value::from(*n),
        Value::Float(f) => serde_json::json!(f),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        // Opaque bytes are not audited verbatim; note their length instead.
        Value::Bytes(b) => serde_json::json!({ "bytes": b.len() }),
        // Embedded structures audit recursively, preserving their shape.
        Value::Map(m) => serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
    }
}

#[async_trait]
impl Extension for AuditExtension {
    fn name(&self) -> &str {
        "audit"
    }

    async fn after_action(&self, changeset: &Changeset, result: &mut ActionResult) -> Result<()> {
        // `after_action` runs only once the action is persisted, so the outcome
        // is a success from the audit trail's point of view; a failed action
        // returns `Err` before reaching here.
        let _ = result;
        if self.audits(&changeset.resource) {
            self.record_write(changeset, AuditResult::Success);
        }
        Ok(())
    }

    async fn after_read(
        &self,
        resource: &str,
        action: &str,
        records: &mut Vec<Record>,
        actor: Option<&Record>,
    ) -> Result<()> {
        if !self.audits(resource) {
            return Ok(());
        }

        let mut builder = AuditEvent::builder()
            .event_type(AuditEventType::MethodInvocation)
            .method(format!("{resource}.{action}"))
            .result(AuditResult::Success)
            .metadata("resource", resource.to_owned())
            .metadata("action", action.to_owned())
            .metadata("count", records.len());

        if let Some(principal) = self.principal(actor) {
            builder = builder.principal(principal);
        }

        self.backend.log_audit(&builder.build());
        Ok(())
    }

    async fn on_denied(
        &self,
        resource: &str,
        action: &str,
        actor: Option<&Record>,
        error: &crate::error::Error,
    ) {
        // Closes the audit trail's blind spot: the other hooks only ever run on
        // the authorized path, so without this a denied action left no trace. The
        // executor calls this only for authorization denials, so `error` is a
        // `Forbidden`/`PolicyError` — its `Display` is the recorded reason.
        if self.audits(resource) {
            self.record_denied(resource, action, actor, &error.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use ash_log::AuditResult;

    use super::*;
    use crate::action::ActionKind;

    /// A backend that keeps every event it is handed, for assertions.
    #[derive(Default)]
    struct CapturingBackend {
        events: Mutex<Vec<AuditEvent>>,
    }

    impl AuditBackend for CapturingBackend {
        fn log_audit(&self, event: &AuditEvent) {
            self.events.lock().unwrap().push(event.clone());
        }
    }

    fn actor(email: &str) -> Record {
        Record::from_iter([("email", email)])
    }

    #[tokio::test]
    async fn write_action_is_audited_with_principal_and_params() {
        let backend = Arc::new(CapturingBackend::default());
        let ext = AuditExtension::new(backend.clone()).principal_attribute("email");

        let mut cs = Changeset::new(
            "account",
            "close",
            ActionKind::Write,
            Record::from_iter([("id", "1")]),
        );
        cs.actor = Some(actor("alice@example.com"));
        cs.data.insert("status", "closed");

        let mut result = ActionResult::None;
        ext.after_action(&cs, &mut result).await.unwrap();

        let events = backend.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.method.as_deref(), Some("account.close"));
        assert_eq!(e.principal.as_deref(), Some("alice@example.com"));
        assert_eq!(e.result, AuditResult::Success);
        assert_eq!(e.metadata.get("action").unwrap(), "close");
        // Staged data is carried as sanitized params.
        assert_eq!(e.params.as_ref().unwrap()["status"], "closed");
    }

    #[tokio::test]
    async fn resource_filter_skips_unlisted_resources() {
        let backend = Arc::new(CapturingBackend::default());
        let ext = AuditExtension::new(backend.clone()).for_resource("account");

        let mut result = ActionResult::None;
        let cs = Changeset::new("note", "create", ActionKind::Write, Record::new());
        ext.after_action(&cs, &mut result).await.unwrap();

        assert!(backend.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn denied_action_is_audited_as_denied() {
        let backend = Arc::new(CapturingBackend::default());
        let ext = AuditExtension::new(backend.clone()).principal_attribute("email");

        let err = crate::error::Error::Forbidden("owner-only".into());
        ext.on_denied(
            "account",
            "close",
            Some(&actor("mallory@example.com")),
            &err,
        )
        .await;

        let events = backend.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.event_type, AuditEventType::AuthorizationCheck);
        assert_eq!(e.result, AuditResult::Denied);
        assert_eq!(e.severity, ash_log::AuditSeverity::Critical);
        assert_eq!(e.method.as_deref(), Some("account.close"));
        assert_eq!(e.principal.as_deref(), Some("mallory@example.com"));
        // The denial reason rides in `error`; no params on a denied attempt.
        assert!(e.error.as_deref().unwrap().contains("owner-only"));
        assert!(e.params.is_none());
    }

    #[tokio::test]
    async fn denied_respects_the_resource_filter() {
        let backend = Arc::new(CapturingBackend::default());
        let ext = AuditExtension::new(backend.clone()).for_resource("account");

        let err = crate::error::Error::Forbidden("nope".into());
        // A denial on an un-audited resource produces nothing.
        ext.on_denied("note", "create", None, &err).await;

        assert!(backend.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_action_is_audited_with_count() {
        let backend = Arc::new(CapturingBackend::default());
        let ext = AuditExtension::new(backend.clone());

        let mut records = vec![Record::new(), Record::new(), Record::new()];
        ext.after_read("note", "list", &mut records, None)
            .await
            .unwrap();

        let events = backend.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].method.as_deref(), Some("note.list"));
        assert_eq!(events[0].metadata.get("count").unwrap(), 3);
    }
}
