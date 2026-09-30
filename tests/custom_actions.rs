//! End-to-end coverage for **custom actions declared on the `Resource` derive**.
//!
//! A `#[action(update, name = "completed")]` attribute makes the derive emit both
//! an `ActionDef` named `completed` and a typed `Note::completed(…)` method. This
//! test proves the two doors to that action agree:
//!
//! * `Note::completed(&domain, &mut ctx, id, params)` — the typed method, and
//! * `domain.handle_action::<Note>(&mut ctx, "completed", ActionInput::update(id, params))`
//!   — the dynamic entry point,
//!
//! both run the same authorized `completed` write pipeline. Behavior (what the
//! action changes) is supplied by the caller's params here; the derive only
//! *declares* the action, per its declarative contract.
//!
//! These live in an external test crate on purpose: the derive expands to
//! `::ash_domain::…` paths that only resolve outside `ash-domain` itself.

use std::sync::Arc;

use ash_domain::action::ActionInput;
use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{
    Context, Domain, DomainConfig, DomainContext, PolicySet, Record, Resource, Value,
};
use ash_domain::{Store, erase};

#[derive(Resource, Default)]
#[resource(name = "note")]
// A custom update-shaped action beyond the default CRUD set.
#[action(update, name = "completed")]
// A custom read-shaped action, to exercise the read signature too.
#[action(read, name = "recent")]
struct Note {
    #[attribute(primary_key)]
    id: String,
    title: String,
    #[attribute(default = false)]
    done: bool,
}

fn note_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

async fn seed(domain: &Domain, ctx: &mut Context<impl Store>) -> Value {
    let created = Note::create(domain, ctx, Record::from_iter([("title", "buy milk")]))
        .await
        .unwrap();
    Value::from(created.id)
}

#[tokio::test]
async fn typed_custom_method_runs_the_named_action() {
    let domain = note_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = seed(&domain, &mut ctx).await;

    // The generated typed method drives the `completed` action.
    let updated = Note::completed(
        &domain,
        &mut ctx,
        id.clone(),
        Record::from_iter([("done", true)]),
    )
    .await
    .unwrap();

    assert!(updated.done);
    assert_eq!(updated.title, "buy milk"); // untouched fields survive the merge
}

#[tokio::test]
async fn handle_action_runs_the_same_named_action() {
    let domain = note_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = seed(&domain, &mut ctx).await;

    // The dynamic entry point reaches the identical `completed` action by name.
    let outcome = domain
        .handle_action::<Note>(
            &mut ctx,
            "completed",
            ActionInput::update(id, Record::from_iter([("done", true)])).unwrap(),
        )
        .await
        .unwrap();

    let row = outcome.into_data::<Note>().unwrap();
    assert!(row.done);
}

#[tokio::test]
async fn custom_read_action_is_declared_and_callable() {
    let domain = note_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed(&domain, &mut ctx).await;

    // The `recent` custom read action returns rows, via the typed method…
    let rows = Note::recent(&domain, &mut ctx, ash_domain::Query::new("note"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);

    // …and via handle_action by name.
    let outcome = domain
        .handle_action::<Note>(
            &mut ctx,
            "recent",
            ActionInput::read(ash_domain::Query::new("note")),
        )
        .await
        .unwrap();
    assert_eq!(outcome.into_records().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_action_name_is_rejected() {
    let domain = note_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = seed(&domain, &mut ctx).await;

    // An action the resource never declared is an error, not a silent no-op.
    let err = domain
        .handle_action::<Note>(
            &mut ctx,
            "nope",
            ActionInput::update(id, Record::new()).unwrap(),
        )
        .await;
    assert!(err.is_err());
}
