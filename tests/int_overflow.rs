//! Proves the **write-path integer-overflow fix**: a resource field whose Rust
//! type can exceed `i64` (a `u64`) no longer *silently truncates* when converted
//! to the neutral [`Value::Int`] (an `i64`). Instead the derived
//! [`IntoRecord`](ash_domain::IntoRecord) — and therefore
//! [`ActionInput::create`](ash_domain::ActionInput::create) and the typed
//! `create` helper — returns [`Error::Serialization`], mirroring the checked
//! *read* path.
//!
//! Lives in an external test crate because the `#[derive(Resource)]` expansion
//! emits `::ash_domain::…` paths that only resolve outside the crate itself.

use std::sync::Arc;

use ash_domain::action::ActionInput;
use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{
    Context, Domain, DomainConfig, DomainContext, Error, FromRecord, IntoRecord, PolicySet,
    Resource, erase,
};

#[derive(Resource, Default, Debug)]
#[resource(name = "counter")]
struct Counter {
    #[attribute(primary_key)]
    id: String,
    /// A `u64` — its high half exceeds `i64::MAX`, so it is the type the fix
    /// guards.
    ticks: u64,
}

fn counter_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Counter>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[test]
fn u64_within_i64_range_round_trips() {
    // A value that fits in i64 converts losslessly and reads back intact.
    let within = i64::MAX as u64; // the largest still-representable u64
    let rec = Counter {
        id: "c1".into(),
        ticks: within,
    }
    .into_record()
    .expect("in-range u64 must convert");
    assert_eq!(
        rec.get("ticks").and_then(ash_domain::Value::as_int),
        Some(i64::MAX)
    );

    // And it reads back through FromRecord to the same u64.
    let back = Counter::from_record(&rec).expect("round-trip read");
    assert_eq!(back.ticks, within);
}

#[test]
fn u64_above_i64_max_errors_instead_of_truncating() {
    // The bug this fix closes: before, `ticks as i64` wrapped this to a negative
    // number silently. Now it is a loud `Serialization` error.
    let overflow = (i64::MAX as u64) + 1; // 9_223_372_036_854_775_808
    let err = Counter {
        id: "c2".into(),
        ticks: overflow,
    }
    .into_record()
    .expect_err("out-of-range u64 must fail, not wrap");

    match err {
        Error::Serialization(msg) => {
            assert!(
                msg.contains("ticks"),
                "error names the offending field: {msg}"
            );
            assert!(msg.contains("i64"), "error explains the cause: {msg}");
        }
        other => panic!("expected Serialization, got {other:?}"),
    }
}

#[tokio::test]
async fn create_with_overflowing_u64_fails_closed() {
    // The failure propagates through the real write entry point: the action
    // never runs, nothing is persisted.
    let domain = counter_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let overflow = (i64::MAX as u64) + 1;
    let input = ActionInput::create(Counter {
        id: "c3".into(),
        ticks: overflow,
    });
    assert!(
        matches!(input, Err(Error::Serialization(_))),
        "ActionInput::create must surface the overflow before the action runs"
    );

    // Nothing was written — the store has no row for c3.
    let rows = Counter::read(&domain, &mut ctx, ash_domain::Query::new("counter"))
        .await
        .unwrap();
    assert!(rows.is_empty(), "no row should have been persisted");
}
