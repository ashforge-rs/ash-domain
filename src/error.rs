//! The framework error type.

/// The result type used throughout `ash-domain`.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong resolving, authorizing, or executing an action.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// No resource is registered under this name in the domain.
    #[error("unknown resource: {0}")]
    UnknownResource(String),

    /// The resource has no action with this name and the requested kind.
    #[error("unknown action `{action}` on resource `{resource}`")]
    UnknownAction {
        /// The resource queried.
        resource: String,
        /// The action name that was not found.
        action: String,
    },

    /// A record was expected but not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// A [`Policy`](crate::Policy) forbade the action.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// A policy could not reach a decision — typically an external
    /// [`PolicyClient`](crate::PolicyClient) backend failed or timed out
    /// ([`Decision::Error`](crate::Decision::Error)). Distinct from
    /// [`Forbidden`](Error::Forbidden): the action is aborted **fail-closed**,
    /// but this signals an authorization *outage*, not a deliberate denial, so a
    /// caller can retry or alert rather than treat it as "access denied".
    #[error("policy error: {0}")]
    PolicyError(String),

    /// A tenant-scoped resource was accessed without a tenant set on the
    /// [`Context`](crate::Context). Call
    /// [`set_tenant`](crate::Context::set_tenant) before the action.
    #[error("resource `{0}` is tenant-scoped but no tenant is set on the context")]
    MissingTenant(String),

    /// The data layer failed (I/O, database, etc.). `source`, when present, is
    /// the underlying cause and participates in the [`std::error::Error`] chain
    /// (`source()` returns it), so a consumer can downcast to their storage error
    /// rather than parse a flattened string. Build it with
    /// [`Error::data_layer`](Error::data_layer) /
    /// [`Error::data_layer_from`](Error::data_layer_from).
    #[error("data layer error: {message}")]
    DataLayer {
        /// Description of the failure.
        message: String,
        /// The underlying cause, if the layer supplied one.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// (De)serialization of a record failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Invalid input or a violated domain invariant (also raised by extensions
    /// such as the state-machine guard). `field` names the offending
    /// attribute/path when the failure is attributable to one (e.g.
    /// `"address.city"`), else `None`, so a caller can branch on *what* was
    /// invalid instead of parsing the message. Build it with
    /// [`Error::invalid`](Error::invalid) /
    /// [`Error::invalid_field`](Error::invalid_field).
    #[error("{message}")]
    Invalid {
        /// The attribute/path at fault, when the failure is attributable to one.
        field: Option<String>,
        /// Human-readable description of what was invalid.
        message: String,
    },

    /// The operation is not supported by this backend — for a consumer data
    /// layer to signal an operation it does not implement (e.g. a transaction or
    /// batch method it chose not to support).
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// A lock could not be acquired for a contended record — raised by the
    /// single-writer [`lock`](crate::extension::lock) extension when `ash-lock`
    /// fails to grant the lease.
    #[error("lock contention: {0}")]
    Contention(String),

    /// An **optimistic-concurrency conflict**: the row was modified by someone
    /// else between the read and the write, so this update was refused rather
    /// than allowed to overwrite a change it never saw.
    ///
    /// Raised only for a resource that declares a
    /// [`version_attribute`](crate::Resource::version_attribute). It is a
    /// legitimate control-flow outcome, not a fault — the correct response is to
    /// re-read the row, re-apply the change to the new state, and retry. Distinct
    /// from [`Contention`](Error::Contention), which means a *lock* could not be
    /// taken (the write never attempted); a `Conflict` means the write was
    /// attempted and lost the race.
    #[error("update conflict on {resource}: {message}")]
    Conflict {
        /// The resource whose row was contended.
        resource: String,
        /// What was expected versus what was found.
        message: String,
    },

    /// The domain is shutting down and is no longer accepting new work — raised
    /// once [`Domain::begin_close`](crate::Domain::begin_close) /
    /// [`close`](crate::Domain::close) has flipped the close gate. It is a
    /// **lifecycle** signal, not a fault: the action never ran and nothing was
    /// observed, so a caller draining its own work should stop, not retry. Like
    /// [`Forbidden`](Error::Forbidden) it is a legitimate control-flow outcome —
    /// distinct from [`DataLayer`](Error::DataLayer) / [`PolicyError`](Error::PolicyError),
    /// which signal a genuine fault.
    #[error("domain is closing: {0}")]
    Closing(String),
}

impl Error {
    /// An [`Invalid`](Error::Invalid) error with no attributable field.
    pub fn invalid(message: impl Into<String>) -> Self {
        Error::Invalid {
            field: None,
            message: message.into(),
        }
    }

    /// An [`Invalid`](Error::Invalid) error attributed to `field` (an attribute
    /// name or dotted path).
    pub fn invalid_field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Invalid {
            field: Some(field.into()),
            message: message.into(),
        }
    }

    /// A [`DataLayer`](Error::DataLayer) error with no captured cause.
    pub fn data_layer(message: impl Into<String>) -> Self {
        Error::DataLayer {
            message: message.into(),
            source: None,
        }
    }

    /// A [`DataLayer`](Error::DataLayer) error wrapping its underlying `source`,
    /// which stays reachable through [`std::error::Error::source`].
    pub fn data_layer_from(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Error::DataLayer {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }
}

#[cfg(feature = "lock")]
impl From<ash_lock::Error> for Error {
    fn from(e: ash_lock::Error) -> Self {
        Error::Contention(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn data_layer_from_preserves_the_source_chain() {
        let cause = std::io::Error::other("boom");
        let err = Error::data_layer_from("read failed", cause);

        // Display carries the message; the cause stays reachable via source().
        assert_eq!(err.to_string(), "data layer error: read failed");
        let source = err.source().expect("source should be present");
        assert!(source.to_string().contains("boom"));
    }

    #[test]
    fn data_layer_without_a_cause_has_no_source() {
        let err = Error::data_layer("read failed");
        assert!(err.source().is_none());
    }

    #[test]
    fn invalid_field_is_matchable_and_keeps_the_message() {
        let err = Error::invalid_field("address.city", "`address.city`: is required");
        match err {
            Error::Invalid { field, message } => {
                assert_eq!(field.as_deref(), Some("address.city"));
                assert!(message.contains("address.city"));
            }
            other => panic!("expected Invalid, got {other:?}"),
        }
    }
}
