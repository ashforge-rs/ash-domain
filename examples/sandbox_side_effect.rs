//! Red→green, then two faults — make a domain side effect bulletproof with the
//! sandbox: prove it can't fire *falsely*, and see exactly what happens when it
//! fails to fire.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example sandbox_side_effect --features sandbox
//! ```
//!
//! The workflow: code the side effect (or the fault) in the sandbox *first* and
//! assert on it — it's **red**, because the domain doesn't handle it yet — then
//! introduce the solution code until it goes **green**. The sandbox records
//! every call (values and outcome, faulted or not); the assertions are yours.
//!
//! The side effect is "completing an order requests a shipment", the solution
//! code is an [`Extension`], and the two faults are the two hazards that decide
//! whether it's actually bulletproof:
//!
//! - **FAULT(db)** — the db dies on the completion write. Proves the solution
//!   only ships *committed* completions: no false shipment for a write that
//!   never landed.
//! - **FAULT(ship)** — the write commits, then the shipment client is down. The
//!   dual hazard: with no transaction seam the write *is* the commit, so the
//!   caller gets `Err` on a row that **stands committed**. Nothing is silently
//!   dropped — but at-least-once delivery is on you (an outbox). This is the
//!   contract you reason from, so the example makes it observable.

use std::sync::Arc;

use ash_domain::action::{ActionResult, Changeset};
use ash_domain::attribute::Attribute;
use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::extension::Extension;
use ash_domain::sandbox::{Effect, EffectLog, FaultPlan, Sandbox, SpyLayer};
use ash_domain::{
    ActionDef, ActionInput, Context, DataLayer, Domain, DomainConfig, DomainContext, Error, Record,
    Resource, Result, Store, Value, erase, expect_none, expect_one,
};
use async_trait::async_trait;

/// The recording, fault-injectable in-memory store the sandbox hands out — named
/// here so a scenario can hold onto one and read a committed row straight back.
type SpyStore = Arc<SpyLayer<InMemoryDataLayer>>;

/// A tiny order with a `status`, and a `complete` action that sets it.
struct Order;

impl Resource for Order {
    const NAME: &'static str = "order";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute {
                default: Some(Value::from("open")),
                ..Attribute::scalar::<String>("status")
            },
        ]
    }

    fn actions() -> Vec<ActionDef> {
        vec![
            ActionDef::write("create"),
            ActionDef::write("complete").change_lambda(|mut cs| async move {
                cs.set_attribute("status", "complete");
                Ok(cs)
            }),
        ]
    }
}

/// The solution code: after a completion is *persisted*, request a shipment.
/// In production this would call your shipment client; in the sandbox the fake
/// client is a few lines — consult the scripted fault, then record the call.
struct ShipOnComplete {
    /// Where the fake client records its attempts.
    shipper: EffectLog,
    /// The scripted fault source, so the client can be made to fail on demand —
    /// exactly as [`SpyLayer`] and the event recorder honour the same plan.
    faults: FaultPlan,
}

#[async_trait]
impl Extension for ShipOnComplete {
    fn name(&self) -> &str {
        "ship-on-complete"
    }

    async fn after_action(&self, cs: &Changeset, result: &mut ActionResult) -> Result<()> {
        if !(cs.resource == "order" && cs.action == "complete") {
            return Ok(());
        }
        // Read the id from the *persisted* result — the committed truth — not the
        // staged changeset. A completion always returns the stored record;
        // anything else is a real error, surfaced, never a silently-null shipment.
        let ActionResult::Record(persisted) = result else {
            return Err(Error::invalid(
                "complete did not return the persisted order",
            ));
        };
        let id = persisted
            .get("id")
            .cloned()
            .ok_or_else(|| Error::invalid("persisted order has no id"))?;

        // The fake shipment client: build the call, consult the scripted fault,
        // and record it either way — a faulted call is still a recorded call, so
        // the log always answers "was a shipment attempted?".
        let call = Effect::new("shipment.requested").with("order", id);
        if let Some(err) = self.faults.decide(&call) {
            self.shipper
                .record(call.with("outcome", "fault").with("error", err.to_string()));
            return Err(err);
        }
        self.shipper.record(call.with("outcome", "ok"));
        Ok(())
    }
}

/// A domain with no side-effect code — the RED baseline.
fn plain_domain(sb: &Sandbox) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Order>()],
            ..sb.config() // deterministic clock + ids, recording notifier
        },
        DomainContext::new(),
    )
}

/// A domain wired with the shipment extension — the solution under test.
fn ship_domain(sb: &Sandbox) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Order>()],
            extensions: vec![Arc::new(ShipOnComplete {
                shipper: sb.effects.clone(),
                faults: sb.faults.clone(),
            })],
            ..sb.config()
        },
        DomainContext::new(),
    )
}

/// The success path: create an order and complete it, returning its id. When a
/// fault is armed on either write this returns `Err` (the caller asserts on it);
/// the partial-failure phases step outside this helper on purpose.
async fn complete_an_order(domain: &Domain, ctx: &mut Context<impl Store>) -> Result<Value> {
    let order = domain
        .handle_action::<Order>(ctx, "create", ActionInput::create(Record::new())?)
        .await?
        .into_record()?;
    let id = order
        .get("id")
        .cloned()
        .ok_or_else(|| Error::invalid("created order has no id"))?;
    domain
        .handle_action::<Order>(
            ctx,
            "complete",
            ActionInput::update(id.clone(), Record::new())?,
        )
        .await?;
    Ok(id)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // ── RED: the domain has no shipment code yet, so the call never happens ──
    let sb = Sandbox::new();
    let mut ctx = sb.context();
    complete_an_order(&plain_domain(&sb), &mut ctx).await?;
    match expect_one!(sb.effects, "shipment.requested") {
        Err(e) => println!("RED         \u{2717} {e}\n"),
        Ok(_) => println!("(unexpectedly green)"),
    }

    // ── GREEN: introduce the solution code (the extension), re-run, assert ──
    let sb = Sandbox::new();
    let mut ctx = sb.context();
    complete_an_order(&ship_domain(&sb), &mut ctx).await?;
    let shipped = expect_one!(sb.effects, "shipment.requested")?;
    let order_id = shipped
        .get_str("order")
        .expect("Some: the fake client attaches `order` to every shipment.requested");
    println!("GREEN       \u{2713} shipment requested for order {order_id:?}");
    // The whole ordered log — `dump` colorizes it when stdout is a terminal
    // (kind by taxonomy, outcome by result) and stays plain when piped.
    sb.effects.dump();

    // ── FAULT(db): the db dies on the completion write, before it commits ──
    // You script the failure; the solution must not ship an uncommitted order.
    let sb = Sandbox::new();
    sb.faults.set(|call| {
        (call.kind == "write" && call.get_str("op") == Some("update"))
            .then(|| Error::data_layer("injected: db down"))
    });
    let mut ctx = sb.context();
    let err = complete_an_order(&ship_domain(&sb), &mut ctx)
        .await
        .unwrap_err();

    // The faulted call is still recorded — "this was called" — and no shipment
    // leaks for a completion that never landed.
    expect_one!(sb.effects, "write", op: "update", outcome: "fault")?;
    expect_none!(sb.effects, "shipment.requested")?;
    println!("FAULT(db)   \u{2713} \"{err}\": write faulted, no shipment leaked");

    // ── FAULT(ship): the write commits, then the shipment client is down ──
    // The hard half of "bulletproof". The completion is durable *before* the
    // side effect runs, and this store has no transaction seam — so the domain
    // cannot roll it back. We drive create/complete by hand (not the helper),
    // because the whole point is the partial failure the success path can't show.
    let sb = Sandbox::new();
    sb.faults.set(|call| {
        (call.kind == "shipment.requested").then(|| Error::invalid("injected: shipping API down"))
    });
    let store: SpyStore = Arc::new(
        SpyLayer::new(InMemoryDataLayer::new(), sb.effects.clone()).with_faults(sb.faults.clone()),
    );
    let mut ctx = Context::new(store.clone());
    let domain = ship_domain(&sb);

    let order = domain
        .handle_action::<Order>(&mut ctx, "create", ActionInput::create(Record::new())?)
        .await?
        .into_record()?;
    let id = order
        .get("id")
        .cloned()
        .ok_or_else(|| Error::invalid("created order has no id"))?;
    let err = domain
        .handle_action::<Order>(
            &mut ctx,
            "complete",
            ActionInput::update(id.clone(), Record::new())?,
        )
        .await
        .unwrap_err();

    // The shipment was attempted and faulted — recorded, not silently dropped…
    expect_one!(sb.effects, "shipment.requested", outcome: "fault")?;
    // …the completion write itself succeeded — with no transaction seam the write
    // *is* the commit, so there is nothing to roll back…
    expect_one!(sb.effects, "write", op: "update", outcome: "ok")?;
    // …the post-commit event was never emitted (`after_action` failed first), so
    // event subscribers never hear of the committed completion either — the
    // second consumer the outbox has to serve…
    expect_none!(sb.effects, "event", action: "complete")?;
    // …and the row really is durable: read it straight back from the store.
    let stored = store
        .get("order", "id", &id)
        .await?
        .ok_or_else(|| Error::invalid("completed order is not in the store"))?;
    assert_eq!(
        stored.get("status").and_then(Value::as_str),
        Some("complete")
    );
    println!(
        "FAULT(ship) \u{2713} \"{err}\": order committed (status=complete) but shipment faulted \u{2014}\n            \
         the caller sees Err on a durable row, so at-least-once delivery is yours (an outbox)."
    );

    Ok(())
}
