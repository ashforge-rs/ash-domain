//! Coverage for the **generated typed `create_many`** on `#[derive(Resource)]`,
//! and for the batch contract it rides on.
//!
//! A batch create is not a shortcut past the pipeline: every row is staged,
//! authorized and validated on its own, and **nothing** is persisted until all
//! of them pass. Only the persist is batched. These tests pin both halves — the
//! typed ergonomics and the fail-closed guarantee.
//!
//! External test crate on purpose: the derive expands to `::ash_domain::…` paths
//! that only resolve outside `ash-domain` itself.

use std::num::NonZeroU32;
use std::sync::Arc;

use ash_domain::action::Changeset;
use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::policy::{Decision, Policy, ScopedPolicy};
use ash_domain::{
    Context, Domain, DomainConfig, DomainContext, Error, PolicySet, Query, Record, Resource, Store,
    Value, async_trait, erase,
};

#[derive(Resource, Default, Debug)]
#[resource(name = "note")]
struct Note {
    #[attribute(primary_key)]
    id: Option<String>,
    title: String,
}

fn note_domain(policies: PolicySet) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies,
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

fn titles(n: usize) -> Vec<Record> {
    (0..n)
        .map(|i| Record::from_iter([("title", format!("note {i}"))]))
        .collect()
}

async fn stored(domain: &Domain, ctx: &mut Context<impl Store>) -> usize {
    Note::read(domain, ctx, Query::new("note"))
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn create_many_returns_the_typed_rows_in_order() {
    let domain = note_domain(PolicySet::permissive());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let created = Note::create_many(&domain, &mut ctx, titles(3))
        .await
        .unwrap();

    let got: Vec<&str> = created.iter().map(|n| n.title.as_str()).collect();
    assert_eq!(got, vec!["note 0", "note 1", "note 2"]);
    // Each row went through the ordinary create pipeline, so each got its
    // generated primary key.
    assert!(created.iter().all(|n| n.id.is_some()));
    assert_eq!(stored(&domain, &mut ctx).await, 3);
}

/// Forbids a note titled "bad" — a per-row decision, so it can deny one row of
/// an otherwise fine batch.
struct NoBadNotes;
#[async_trait]
impl Policy for NoBadNotes {
    async fn authorize(&self, cs: &Changeset) -> Decision {
        match cs.data.get("title").and_then(Value::as_str) {
            Some("bad") => Decision::Forbid("no bad notes".into()),
            _ => Decision::NotApplicable,
        }
    }
}

#[tokio::test]
async fn one_denied_row_persists_none_of_the_batch() {
    let domain = note_domain(PolicySet::permissive().with(ScopedPolicy::resource(
        "note",
        "no-bad",
        Arc::new(NoBadNotes),
    )));
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let rows = vec![
        Record::from_iter([("title", "fine")]),
        Record::from_iter([("title", "bad")]),
        Record::from_iter([("title", "also fine")]),
    ];
    let err = Note::create_many(&domain, &mut ctx, rows).await;

    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
    assert_eq!(
        stored(&domain, &mut ctx).await,
        0,
        "authorization runs for every row before any row is persisted"
    );
}

#[tokio::test]
async fn a_batch_under_default_deny_is_forbidden() {
    // The batch path is not a way around the gate: an empty policy set denies it
    // exactly as it denies a single create.
    let domain = note_domain(PolicySet::new());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let err = Note::create_many(&domain, &mut ctx, titles(2)).await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
}

#[tokio::test]
async fn a_batch_over_the_bound_is_refused() {
    let domain = Domain::builder()
        .register::<Note>()
        .permissive()
        .max_batch(NonZeroU32::new(2).expect("non-zero"))
        .build();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let err = Note::create_many(&domain, &mut ctx, titles(3)).await;
    let Err(Error::Invalid { message, .. }) = err else {
        panic!("expected Invalid, got {err:?}");
    };
    assert!(message.contains("max_batch"), "{message}");
    assert_eq!(stored(&domain, &mut ctx).await, 0);
}
