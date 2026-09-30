//! The sandbox's transactional store: a scenario can now drive the domain's
//! transactional path — commit, rollback, and the pre-commit outbox staging
//! pass — and assert over its recorded lifecycle.
#![cfg(feature = "sandbox")]

use std::sync::Arc;

use ash_domain::attribute::Attribute;
use ash_domain::datalayer::DataLayer;
use ash_domain::event::{DomainEvent, EventHandler};
use ash_domain::sandbox::Sandbox;
use ash_domain::{
    ActionDef, ActionInput, Domain, DomainConfig, DomainContext, Error, PolicySet, Record,
    Resource, Result, Value, erase, expect_none, expect_one,
};

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

fn note_domain(config: DomainConfig) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive(),
            ..config
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn a_write_runs_inside_a_recorded_transaction() -> Result<()> {
    let sb = Sandbox::new();
    let domain = note_domain(sb.config());
    let mut ctx = sb.transactional_context();

    domain
        .handle_action::<Note>(
            &mut ctx,
            "create",
            ActionInput::create(Record::from_iter([("id", "n1"), ("body", "hello")]))?,
        )
        .await?;

    // The lifecycle is visible and ordered: begin → the write → commit.
    expect_one!(sb.effects, "txn", op: "begin")?;
    expect_one!(sb.effects, "txn", op: "commit")?;
    expect_none!(sb.effects, "txn", op: "rollback")?;

    let ops: Vec<String> = sb
        .effects
        .all()
        .iter()
        .filter(|e| e.kind == "txn" || e.kind == "write")
        .filter_map(|e| e.get_str("op").map(str::to_string))
        .collect();
    assert_eq!(ops, ["begin", "create", "commit"], "{ops:?}");

    // The write was made through the transaction, not around it.
    let write = expect_one!(sb.effects, "write", op: "create")?;
    assert_eq!(write.get("in_txn"), Some(&Value::Bool(true)));
    Ok(())
}

#[tokio::test]
async fn a_failed_commit_leaves_nothing_durable() -> Result<()> {
    let sb = Sandbox::new();
    let domain = note_domain(sb.config());
    let layer = sb.layer();
    let store =
        ash_domain::sandbox::TransactionalSpyStore::over_spy(layer.clone(), sb.effects.clone())
            .with_faults(sb.faults.clone());
    let mut ctx = ash_domain::Context::new(store);

    // Script the commit to fail — the scenario this store exists to make possible.
    sb.faults.set(|call| {
        (call.kind == "txn" && call.get_str("op") == Some("commit"))
            .then(|| Error::data_layer("injected: commit failed"))
    });

    let err = domain
        .handle_action::<Note>(
            &mut ctx,
            "create",
            ActionInput::create(Record::from_iter([("id", "n1"), ("body", "hello")]))?,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DataLayer { .. }), "{err:?}");

    // Recorded as a fault...
    let commit = expect_one!(sb.effects, "txn", op: "commit")?;
    assert_eq!(commit.get_str("outcome"), Some("fault"));

    // ...and the row never reached the layer: a failed commit discards the work.
    let stored = layer.get("note", "id", &Value::from("n1")).await?;
    assert!(
        stored.is_none(),
        "a failed commit must leave nothing durable"
    );
    Ok(())
}

/// An outbox handler: stages each event into the write's own transaction.
/// Before `transactional_context` existed this could not be driven at all — the
/// domain refuses to run a staging handler against a store offering no
/// transaction.
struct OutboxHandler;

#[async_trait::async_trait]
impl EventHandler for OutboxHandler {
    fn stages(&self) -> bool {
        true
    }

    async fn stage(&self, event: &DomainEvent, txn: &dyn DataLayer) -> Result<()> {
        txn.create(
            "outbox",
            "id",
            Record::from_iter([
                ("id", Value::from(format!("evt-{}", event.action))),
                ("resource", Value::from(event.resource.clone())),
            ]),
        )
        .await
        .map(|_| ())
    }

    async fn handle(&self, _event: &DomainEvent) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn an_outbox_row_is_staged_inside_the_write_transaction() -> Result<()> {
    let sb = Sandbox::new();
    let domain = note_domain(DomainConfig {
        event_handlers: vec![Arc::new(OutboxHandler)],
        ..sb.config()
    });
    let mut ctx = sb.transactional_context();

    domain
        .handle_action::<Note>(
            &mut ctx,
            "create",
            ActionInput::create(Record::from_iter([("id", "n1"), ("body", "hello")]))?,
        )
        .await?;

    // Both writes are inside the same transaction, and the outbox row is staged
    // *before* the commit — the atomicity the seam promises.
    let ops: Vec<String> = sb
        .effects
        .all()
        .iter()
        .filter(|e| e.kind == "txn" || e.kind == "write")
        .filter_map(|e| e.get_str("op").map(str::to_string))
        .collect();
    assert_eq!(ops, ["begin", "create", "create", "commit"], "{ops:?}");

    let outbox = sb.effects.expect_one("outbox row", |e| {
        e.kind == "write" && e.get_str("resource") == Some("outbox")
    })?;
    assert_eq!(outbox.get("in_txn"), Some(&Value::Bool(true)));
    Ok(())
}

#[tokio::test]
async fn a_staging_failure_rolls_the_write_back() -> Result<()> {
    /// Stages, then fails — the write must not survive.
    struct FailingOutbox;
    #[async_trait::async_trait]
    impl EventHandler for FailingOutbox {
        fn stages(&self) -> bool {
            true
        }
        async fn stage(&self, _e: &DomainEvent, _txn: &dyn DataLayer) -> Result<()> {
            Err(Error::data_layer("injected: outbox unavailable"))
        }
        async fn handle(&self, _event: &DomainEvent) -> Result<()> {
            Ok(())
        }
    }

    let sb = Sandbox::new();
    let domain = note_domain(DomainConfig {
        event_handlers: vec![Arc::new(FailingOutbox)],
        ..sb.config()
    });
    let layer = sb.layer();
    let store =
        ash_domain::sandbox::TransactionalSpyStore::over_spy(layer.clone(), sb.effects.clone());
    let mut ctx = ash_domain::Context::new(store);

    let err = domain
        .handle_action::<Note>(
            &mut ctx,
            "create",
            ActionInput::create(Record::from_iter([("id", "n1"), ("body", "hello")]))?,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::DataLayer { .. }), "{err:?}");

    // Rolled back, not committed...
    expect_one!(sb.effects, "txn", op: "rollback")?;
    expect_none!(sb.effects, "txn", op: "commit")?;
    // ...and nothing is durable.
    assert!(layer.get("note", "id", &Value::from("n1")).await?.is_none());
    Ok(())
}

/// A versioned resource, to prove the conditional write is visible in the log.
struct Acct;

impl Resource for Acct {
    const NAME: &'static str = "acct";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<i64>("balance"),
            Attribute::scalar::<i64>("version"),
        ]
    }

    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::write("update")]
    }

    fn version_attribute() -> Option<String> {
        Some("version".into())
    }
}

#[tokio::test]
async fn a_batch_create_is_recorded_as_one_call() -> Result<()> {
    // The point of a batch is that it is *one* round trip; the log must show
    // that, not N separate creates.
    let sb = Sandbox::new();
    let domain = note_domain(sb.config());
    let mut ctx = sb.context();

    domain
        .handle_action::<Note>(
            &mut ctx,
            "create",
            ActionInput::create_many_records(vec![
                Record::from_iter([("id", "n1"), ("body", "a")]),
                Record::from_iter([("id", "n2"), ("body", "b")]),
                Record::from_iter([("id", "n3"), ("body", "c")]),
            ]),
        )
        .await?;

    let batch = expect_one!(sb.effects, "write", op: "create_many")?;
    assert_eq!(batch.get("rows"), Some(&Value::Int(3)));
    // ...and not as three individual creates.
    expect_none!(sb.effects, "write", op: "create")?;
    Ok(())
}

#[tokio::test]
async fn a_versioned_update_records_the_conditional_write() -> Result<()> {
    // Taking the unconditional path for a versioned resource is the lost-update
    // bug itself, so a scenario must be able to assert which path ran.
    let sb = Sandbox::new();
    let domain = Domain::new(
        DomainConfig {
            resources: vec![erase::<Acct>()],
            policies: PolicySet::permissive(),
            ..sb.config()
        },
        DomainContext::new(),
    );
    let mut ctx = sb.context();

    domain
        .handle_action::<Acct>(
            &mut ctx,
            "create",
            ActionInput::create(Record::from_iter([
                ("id", Value::from("a1")),
                ("balance", Value::Int(100)),
            ]))?,
        )
        .await?;

    domain
        .handle_action::<Acct>(
            &mut ctx,
            "update",
            ActionInput::update(
                "a1",
                Record::from_iter([("balance", Value::Int(150)), ("version", Value::Int(1))]),
            )?,
        )
        .await?;

    let versioned = expect_one!(sb.effects, "write", op: "update_versioned")?;
    assert_eq!(versioned.get("expected_version"), Some(&Value::Int(1)));
    assert_eq!(versioned.get_str("version_attribute"), Some("version"));
    // The unconditional path was never taken.
    expect_none!(sb.effects, "write", op: "update")?;
    Ok(())
}

#[tokio::test]
async fn the_sandbox_store_passes_the_transaction_conformance_check() -> Result<()> {
    // The fake must uphold the same contract a real transactional store does —
    // otherwise scenarios green against it would be green against nothing.
    use ash_domain::datalayer::conformance::Conformance;

    let sb = Sandbox::new();
    let store =
        ash_domain::sandbox::TransactionalSpyStore::over_spy(sb.layer(), sb.effects.clone());
    let report = Conformance::new("scratch", "id").check_store(&store).await;
    assert!(report.is_conformant(), "{report}");
    Ok(())
}
