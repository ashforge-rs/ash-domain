//! The state-machine guard extension, built on [`ash_fsm`].
//!
//! Attach a [`StateMachineExtension`] to a domain to make a string-valued state
//! attribute obey a declared [`ash_fsm`] state machine: an action that would
//! move the record to an illegal next state is rejected in `before_action`, and
//! a legal action stamps the new state onto the changeset.
//!
//! The legal transitions are expressed with `ash-fsm`'s own
//! [`Transition`](ash_fsm::Transition) /
//! [`StateMachineConfig`] types, and checked with
//! [`StateMachineConfig::is_transition_allowed`], so this is a genuine reuse of
//! the state-machine crate rather than a re-implementation.
//!
//! ```
//! use std::sync::Arc;
//! use ash_domain::extension::fsm::{StateMachineExtension, StateValue};
//! use ash_domain::ash_fsm::{StateMachineConfig, Transition};
//!
//! // Pending --submit--> Active --close--> Closed
//! let config = StateMachineConfig::new(StateValue::from("pending")).transitions(vec![
//!     Transition::new("submit").from(vec![StateValue::from("pending")]).to(vec![StateValue::from("active")]),
//!     Transition::new("close").from(vec![StateValue::from("active")]).to(vec![StateValue::from("closed")]),
//! ]);
//!
//! let ext = StateMachineExtension::new("ticket", "state", config)
//!     .on_action("submit", "active")
//!     .on_action("close", "closed");
//! let _ = Arc::new(ext);
//! ```

use std::collections::HashMap;

use ash_fsm::{State, StateMachineConfig};
use async_trait::async_trait;

use crate::action::Changeset;
use crate::error::{Error, Result};
use crate::extension::Extension;
use crate::value::Value;

/// A resource state represented as a string, usable as an [`ash_fsm`] state.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StateValue(pub String);

impl State for StateValue {}

impl From<&str> for StateValue {
    fn from(s: &str) -> Self {
        StateValue(s.to_string())
    }
}
impl From<String> for StateValue {
    fn from(s: String) -> Self {
        StateValue(s)
    }
}

/// Guards transitions of a string state attribute against an `ash-fsm` machine.
pub struct StateMachineExtension {
    resource: String,
    attribute: String,
    config: StateMachineConfig<StateValue>,
    /// Which target state each guarded action moves the record to.
    action_targets: HashMap<String, StateValue>,
}

impl StateMachineExtension {
    /// Guard the `attribute` of `resource` with `config`.
    pub fn new(
        resource: impl Into<String>,
        attribute: impl Into<String>,
        config: StateMachineConfig<StateValue>,
    ) -> Self {
        Self {
            resource: resource.into(),
            attribute: attribute.into(),
            config,
            action_targets: HashMap::new(),
        }
    }

    /// Declare that `action` transitions the record to `target_state`.
    pub fn on_action(mut self, action: impl Into<String>, target_state: impl Into<String>) -> Self {
        self.action_targets
            .insert(action.into(), StateValue(target_state.into()));
        self
    }

    /// The state a record is currently in: its persisted value, or the config's
    /// default initial state for a create.
    fn current_state(&self, changeset: &Changeset) -> StateValue {
        changeset
            .original
            .as_ref()
            .and_then(|o| o.get(&self.attribute))
            .and_then(|v| v.as_str())
            .map(|s| StateValue(s.to_string()))
            .unwrap_or_else(|| self.config.default_initial_state.clone())
    }
}

#[async_trait]
impl Extension for StateMachineExtension {
    fn name(&self) -> &str {
        "state_machine"
    }

    async fn before_action(&self, changeset: &mut Changeset) -> Result<()> {
        if changeset.resource != self.resource {
            return Ok(());
        }
        let Some(target) = self.action_targets.get(&changeset.action).cloned() else {
            // Not a guarded action on this resource.
            return Ok(());
        };

        let from = self.current_state(changeset);
        if !self
            .config
            .is_transition_allowed(&changeset.action, &from, &target)
        {
            return Err(Error::invalid(format!(
                "illegal transition `{}` on `{}`: {} -> {}",
                changeset.action, self.resource, from.0, target.0
            )));
        }

        // Legal: stamp the new state onto the changeset.
        changeset
            .data
            .insert(self.attribute.clone(), Value::Str(target.0));
        Ok(())
    }
}
