//! Close the atomicity gap the other sandbox example ends on — and prove it
//! closed, four ways, with the sandbox's *transactional* store.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example sandbox_outbox --features sandbox
//! ```
//!
//! [`sandbox_side_effect`](sandbox_side_effect) ends at the honest limit of a
//! store with no transaction seam: the write *is* the commit, so a post-commit
//! side effect that fails leaves a durable row nobody was told about, and
//! "at-least-once delivery is yours (an outbox)". This example builds that
//! outbox and holds it to the standard.
//!
//! The seam is [`EventHandler::stage`]: it runs **inside the still-open write
//! transaction**, so the outbox row and the row it announces commit together or
//! not at all. [`Sandbox::transactional_context`] is what makes that path
//! reachable — a plain [`Sandbox::context`] declines transactions, and the
//! domain then *refuses* a staging handler outright rather than silently
//! downgrading it. Four phases:
//!
//! - **REFUSED** — a staging handler against a non-transactional store. The
//!   domain fails the write with `Unsupported` instead of quietly giving the
//!   handler weaker guarantees than it asked for. No degradation, loud.
//! - **ATOMIC** — the happy path. Assert the *ordering* out of the effect log:
//!   both writes carry `in_txn: true` and both precede the single `txn commit`.
//!   That is the atomicity claim, stated as an assertion rather than a hope.
//! - **FAULT(commit)** — the commit itself fails. Neither the order nor its
//!   outbox row survives; the log's `txn commit outcome="fault"` is the proof,
//!   and reading both back from the store confirms it.
//! - **RELAY** — what the outbox is *for*. Drop the post-commit handler on the
//!   floor (the process died between commit and publish, the classic loss), then
//!   run a relay over the outbox table and recover the event anyway.
//!
//! Beyond the transaction seam, this example also exercises sandbox surface the
//! side-effect one does not: [`ManualClock`] (the domain stamps `at` from it, so
//! `DomainEvent::at` is asserted to the millisecond), [`Sandbox::seeded`]
//! (reproducible ids), [`EffectLog::all`] for ordering assertions the `expect_*`
//! macros deliberately do not cover, and [`EffectLog::clear`] to separate phases
//! of one scenario.

use std::sync::Arc;

use ash_domain::attribute::Attribute;
use ash_domain::sandbox::{Effect, EffectLog, Sandbox, TransactionalSpyStore};
use ash_domain::{
    ActionDef, ActionInput, Context, DataLayer, Domain, DomainConfig, DomainContext, DomainEvent,
    Error, EventHandler, Query, Record, Resource, Result, Value, erase, expect_none, expect_one,
};
use async_trait::async_trait;

/// The millisecond the scenario's clock is parked at, so the event timestamp is
/// an exact assertion rather than "something nonzero".
const COMMIT_MILLIS: i64 = 1_700_000_000_000;

/// The order being placed — the row whose durability the outbox row must match.
struct Order;

impl Resource for Order {
    const NAME: &'static str = "order";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("sku"),
        ]
    }

    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create")]
    }
}

/// The outbox table. An ordinary resource — that is the point: the core gains no
/// outbox schema, no relay, no delivery machinery. It is a row like any other,
/// written through the layer the transaction already hands out.
struct Outbox;

impl Resource for Outbox {
    const NAME: &'static str = "outbox";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("resource"),
            Attribute::scalar::<String>("action"),
            Attribute::scalar::<String>("payload"),
            // When the action committed, from the domain's clock — a relay can
            // order or age its backlog without a clock of its own.
            Attribute::scalar::<i64>("at"),
        ]
    }

    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create")]
    }
}

/// The transactional-outbox handler: it writes the event into the outbox table
/// **through the write's own transaction**, so the two commit as one.
///
/// `handle` is the other half — the post-commit publish a relay makes redundant.
/// Here it records the attempt so a phase can show it firing (ATOMIC) or being
/// skipped entirely (RELAY, where the process "dies" before it runs).
struct OutboxHandler {
    /// Where the fake publisher records its attempts.
    log: EffectLog,
    /// Whether the post-commit publish runs at all — RELAY sets this false to
    /// stage the classic loss: committed row, event never published.
    publish: bool,
}

#[async_trait]
impl EventHandler for OutboxHandler {
    /// Post-commit, best-effort — exactly as before. The outbox exists precisely
    /// because this can be lost.
    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        if !self.publish {
            // The process died between commit and publish. Nothing recorded —
            // the log's silence is what the relay phase asserts against.
            return Ok(());
        }
        self.log.record(
            Effect::new("published")
                .with("resource", event.resource.clone())
                .with("action", event.action.clone())
                .with("outcome", "ok"),
        );
        Ok(())
    }

    /// The seam that closes the gap: called *before* the commit, with the
    /// transaction's own [`DataLayer`] view. An `Err` here rolls the write back,
    /// so there is never a row standing with no event to announce it.
    async fn stage(&self, event: &DomainEvent, txn: &dyn DataLayer) -> Result<()> {
        // The affected row is the payload. A create always produces one; anything
        // else is a real error, surfaced — never a silently-empty outbox row.
        let Some(record) = event.records.first() else {
            return Err(Error::invalid("staged event carries no record"));
        };
        let id = record
            .get("id")
            .cloned()
            .ok_or_else(|| Error::invalid("staged record has no id"))?;

        let mut row = Record::new();
        // A relay reads rows back by id; reusing the order's id keys the outbox
        // row to the write it announces, and makes the pairing assertable.
        row.insert("id", id.clone());
        row.insert("resource", event.resource.clone());
        row.insert("action", event.action.clone());
        // The payload a relay republishes. A real outbox would serialize the
        // whole record; one field keeps the example's log readable.
        row.insert("payload", record.get("sku").cloned().unwrap_or(Value::Null));
        row.insert("at", event.at);
        txn.create("outbox", "id", row).await?;
        Ok(())
    }

    /// Declaring the dependency. This flag is what lets the domain refuse,
    /// loudly, to run against a store with no transaction to join.
    fn stages(&self) -> bool {
        true
    }
}

/// A domain over both resources, wired with the outbox handler. `publish`
/// controls whether the post-commit half runs at all.
fn outbox_domain(sb: &Sandbox, publish: bool) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Order>(), erase::<Outbox>()],
            // Replaces the sandbox's default RecordingHandler: this scenario is
            // about *this* handler's two halves, not the generic event record.
            event_handlers: vec![Arc::new(OutboxHandler {
                log: sb.effects.clone(),
                publish,
            })],
            ..sb.config()
        },
        DomainContext::new(),
    )
}

/// Place one order, returning its id.
async fn place_order(
    domain: &Domain,
    ctx: &mut Context<impl ash_domain::Store>,
    sku: &str,
) -> Result<Value> {
    let mut params = Record::new();
    params.insert("sku", sku);
    let order = domain
        .handle_action::<Order>(ctx, "create", ActionInput::create_record(params))
        .await?
        .into_record()?;
    order
        .get("id")
        .cloned()
        .ok_or_else(|| Error::invalid("created order has no id"))
}

/// A sandbox with the clock parked at [`COMMIT_MILLIS`], so every event's `at`
/// is exactly assertable, and ids reproducible from a fixed seed.
fn scenario() -> Sandbox {
    let sb = Sandbox::seeded(42);
    sb.clock.set(COMMIT_MILLIS);
    sb
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // ── REFUSED: a staging handler against a store that offers no transaction ──
    // `sb.context()` declines transactions, so the outbox handler cannot get the
    // atomicity it declared. The domain fails the write rather than downgrade it.
    let sb = scenario();
    let mut ctx = sb.context();
    let err = place_order(&outbox_domain(&sb, true), &mut ctx, "sku-1")
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );
    // Refused *before* anything observable: no order row, no outbox row.
    expect_none!(sb.effects, "write")?;
    println!("REFUSED       \u{2713} {err}\n");

    // ── ATOMIC: the transactional path, with the ordering asserted ──
    let sb = scenario();
    let mut ctx = sb.transactional_context();
    place_order(&outbox_domain(&sb, true), &mut ctx, "sku-2").await?;

    // Both writes ran inside the transaction…
    expect_one!(sb.effects, "write", op: "create", resource: "order", in_txn: true)?;
    expect_one!(sb.effects, "write", op: "create", resource: "outbox", in_txn: true)?;
    // …and there was exactly one commit, carrying both of them.
    expect_one!(sb.effects, "txn", op: "commit", outcome: "ok", applied: 2i64)?;
    // The atomicity claim itself, stated as an ordering assertion over the raw
    // log — the `expect_*` macros answer "was it called", not "in what order".
    assert_ordering(&sb.effects)?;
    // The post-commit publish still ran, after the commit — belt and braces.
    expect_one!(sb.effects, "published", resource: "order")?;
    println!("ATOMIC        \u{2713} order + outbox row in one transaction, one commit");
    sb.effects.dump();

    // ── FAULT(commit): the commit fails — neither row may survive ──
    let sb = scenario();
    sb.faults.set(|call| {
        (call.kind == "txn" && call.get_str("op") == Some("commit"))
            .then(|| Error::data_layer("injected: commit failed"))
    });
    // Hold the layer so both tables can be read back straight from the store,
    // behind the domain, after the failure.
    let layer = sb.layer();
    let store = TransactionalSpyStore::over_spy(layer.clone(), sb.effects.clone())
        .with_faults(sb.faults.clone());
    let mut ctx = Context::new(store);
    let err = place_order(&outbox_domain(&sb, true), &mut ctx, "sku-3")
        .await
        .unwrap_err();

    // The commit was attempted and faulted — recorded, not silently dropped…
    expect_one!(sb.effects, "txn", op: "commit", outcome: "fault")?;
    // …and nothing was published for a write that never landed.
    expect_none!(sb.effects, "published")?;
    // The rows really are absent: both tables are empty, read straight back.
    assert_empty(&*layer, "order").await?;
    assert_empty(&*layer, "outbox").await?;
    println!("FAULT(commit) \u{2713} \"{err}\": neither the order nor its outbox row survived");

    // ── RELAY: the publish is lost, the outbox recovers it ──
    // `publish: false` is the classic failure the outbox exists for: the
    // transaction commits, then the process dies before anything is published.
    let sb = scenario();
    let layer = sb.layer();
    let store = TransactionalSpyStore::over_spy(layer.clone(), sb.effects.clone());
    let mut ctx = Context::new(store);
    let id = place_order(&outbox_domain(&sb, false), &mut ctx, "sku-4").await?;

    // The write committed…
    expect_one!(sb.effects, "txn", op: "commit", outcome: "ok")?;
    // …and nothing was published. Under a bare post-commit handler this event is
    // simply gone; the row stands and no one was ever told.
    expect_none!(sb.effects, "published")?;

    // But the fact was committed *with* the row, so a relay can still find it.
    // This is an ordinary read against the layer — a relay is a separate process
    // that never goes through the domain, which is why it reads the layer here.
    sb.effects.clear();
    let recovered = relay(&*layer, &sb.effects).await?;
    assert_eq!(recovered, 1, "the one committed event is recoverable");
    let published = expect_one!(sb.effects, "published", source: "outbox")?;
    // The event carries the commit time the domain stamped from the manual clock
    // — a relay can order its backlog without a clock of its own.
    assert_eq!(published.get("at"), Some(&Value::Int(COMMIT_MILLIS)));
    // …and it is the row that was actually written, keyed to it by id.
    assert_eq!(published.get("order"), Some(&id));
    println!(
        "RELAY         \u{2713} publish lost, outbox row recovered it (at={COMMIT_MILLIS}ms) \u{2014}\n              \
         the row and the fact that it happened committed together, so nothing can be lost."
    );

    Ok(())
}

/// The atomicity claim as an assertion: both writes precede the single commit,
/// and the publish follows it. Reads the raw ordered log, because *order* is the
/// property here and the `expect_*` macros deliberately answer only membership.
fn assert_ordering(log: &EffectLog) -> Result<()> {
    let all = log.all();
    let position = |pred: &dyn Fn(&Effect) -> bool| -> Result<usize> {
        all.iter()
            .position(pred)
            .ok_or_else(|| Error::invalid("expected effect is missing from the log"))
    };
    let order =
        position(&|e: &Effect| e.kind == "write" && e.get_str("resource") == Some("order"))?;
    let outbox =
        position(&|e: &Effect| e.kind == "write" && e.get_str("resource") == Some("outbox"))?;
    let commit = position(&|e: &Effect| e.kind == "txn" && e.get_str("op") == Some("commit"))?;
    let publish = position(&|e: &Effect| e.kind == "published")?;

    if !(order < commit && outbox < commit && commit < publish) {
        return Err(Error::invalid(format!(
            "expected order({order}) and outbox({outbox}) writes before commit({commit}), \
             and publish({publish}) after it"
        )));
    }
    Ok(())
}

/// Assert `resource` holds no rows — read straight from the layer, behind the
/// domain, so the check is about durability and nothing else.
async fn assert_empty(layer: &dyn DataLayer, resource: &str) -> Result<()> {
    let rows = layer.read(&Query::new(resource)).await?;
    if !rows.is_empty() {
        return Err(Error::invalid(format!(
            "expected `{resource}` to be empty, found {} row(s)",
            rows.len()
        )));
    }
    Ok(())
}

/// The relay: drain the outbox table and publish each row. In production this is
/// a separate process that marks or deletes rows as it goes; here it records what
/// it published so the scenario can assert the event was recovered.
///
/// Bounded by construction — it publishes the rows one read returned, and never
/// loops for more.
async fn relay(layer: &dyn DataLayer, log: &EffectLog) -> Result<usize> {
    let rows = layer.read(&Query::new("outbox").limit(128)).await?;
    for row in &rows {
        log.record(
            Effect::new("published")
                .with("source", "outbox")
                .with("order", row.get("id").cloned().unwrap_or(Value::Null))
                .with(
                    "resource",
                    row.get("resource").cloned().unwrap_or(Value::Null),
                )
                .with("at", row.get("at").cloned().unwrap_or(Value::Null))
                .with("outcome", "ok"),
        );
    }
    Ok(rows.len())
}
