//! The sandbox's diagnostic collectors: the audit trail and (with the `trace`
//! feature) the execution trace both land in the effect log, assertable like
//! any other effect.
#![cfg(feature = "sandbox")]
// The audit collector needs the `audit` feature as well as `sandbox`; the trace
// test below carries its own `trace` gate.
#![cfg(feature = "audit")]

use std::sync::Arc;

use ash_domain::attribute::Attribute;
use ash_domain::extension::audit::AuditExtension;
use ash_domain::sandbox::Sandbox;
use ash_domain::{
    ActionDef, ActionInput, Domain, DomainConfig, DomainContext, Record, Resource, Result, erase,
    expect_one,
};

/// A minimal resource: an id and a body, with a single `create` write.
struct Note;

impl Resource for Note {
    const NAME: &'static str = "note";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("body"),
        ]
    }

    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create")]
    }
}

#[tokio::test]
async fn audit_trail_lands_in_the_effect_log() -> Result<()> {
    let sb = Sandbox::new();
    let config = DomainConfig {
        resources: vec![erase::<Note>()],
        extensions: vec![Arc::new(AuditExtension::new(sb.audit_backend()))],
        ..sb.config()
    };
    let domain = Domain::new(config, DomainContext::new());
    let mut ctx = sb.context();

    domain
        .handle_action::<Note>(&mut ctx, "create", ActionInput::create(Record::new())?)
        .await?;

    let audited = expect_one!(sb.effects, "audit", method: "note.create")?;
    assert_eq!(audited.get_str("result"), Some("Success"));
    assert_eq!(audited.get_str("resource"), Some("note"));
    assert_eq!(audited.get_str("action"), Some("create"));
    Ok(())
}

#[cfg(feature = "trace")]
mod trace {
    use super::*;
    use ash_domain::{Error, PolicySet, expect_none};

    #[tokio::test]
    async fn pipeline_trace_lands_in_order() -> Result<()> {
        let sb = Sandbox::new();
        let _guard = sb.collect_tracing();
        let domain = Domain::new(
            DomainConfig {
                resources: vec![erase::<Note>()],
                ..sb.config()
            },
            DomainContext::new(),
        );
        domain.enable_tracing();
        let mut ctx = sb.context();

        domain
            .handle_action::<Note>(&mut ctx, "create", ActionInput::create(Record::new())?)
            .await?;

        // The action span, with its structure-only fields.
        expect_one!(sb.effects, "trace.span", name: "action", resource: "note", action: "create")?;

        // The pipeline events, in pipeline order, each labelled with its span.
        let steps: Vec<String> = sb
            .effects
            .all()
            .iter()
            .filter(|e| e.kind == "trace")
            .map(|e| {
                assert_eq!(e.get_str("span"), Some("action"));
                e.get_str("step")
                    .expect("pipeline events carry a step")
                    .to_owned()
            })
            .collect();
        assert_eq!(
            steps,
            [
                "staged",
                "authorized",
                "before_action",
                "persisted",
                "after_action"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn denied_action_traces_denied_and_nothing_further() -> Result<()> {
        let sb = Sandbox::new();
        let _guard = sb.collect_tracing();
        let mut config = DomainConfig {
            resources: vec![erase::<Note>()],
            ..sb.config()
        };
        // Default-deny: the scenario asserts the denial itself.
        config.policies = PolicySet::new();
        let domain = Domain::new(config, DomainContext::new());
        domain.enable_tracing();
        let mut ctx = sb.context();

        let err = domain
            .handle_action::<Note>(&mut ctx, "create", ActionInput::create(Record::new())?)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Forbidden(_)), "got {err:?}");

        expect_one!(sb.effects, "trace", step: "denied")?;
        expect_none!(sb.effects, "trace", step: "persisted")?;
        expect_none!(sb.effects, "write")?;
        Ok(())
    }
}
