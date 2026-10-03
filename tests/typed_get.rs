//! Coverage for the **generated typed `get`** on `#[derive(Resource)]`.
//!
//! `Resource::get(&domain, &mut ctx, id)` fetches a single row by primary key and
//! returns it as the typed struct. It is a *read* under the hood — it issues the
//! reserved key-set query and runs the `read` action — so it must behave like any
//! read: return the row when present, `None` when absent, and **fail closed** for
//! a caller no policy admits (never leak the row as a fallback).
//!
//! External test crate on purpose: the derive expands to `::ash_domain::…` paths
//! that only resolve outside `ash-domain` itself.

use std::sync::Arc;

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{
    Context, Domain, DomainConfig, DomainContext, Error, PolicySet, Record, Resource, Store, Value,
    erase,
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

async fn seed(domain: &Domain, ctx: &mut Context<impl Store>) -> Value {
    let created = Note::create(domain, ctx, Record::from_iter([("title", "buy milk")]))
        .await
        .unwrap();
    Value::from(created.id.expect("create stamps the primary key"))
}

#[tokio::test]
async fn get_returns_the_row_by_primary_key() {
    let domain = note_domain(PolicySet::permissive());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = seed(&domain, &mut ctx).await;

    let got = Note::get(&domain, &mut ctx, id).await.unwrap();
    let note = got.expect("the seeded note is fetched by id");
    assert_eq!(note.title, "buy milk");
}

#[tokio::test]
async fn get_returns_none_for_a_missing_id() {
    let domain = note_domain(PolicySet::permissive());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed(&domain, &mut ctx).await;

    // A primary key no row carries yields `None`, not an error.
    let got = Note::get(&domain, &mut ctx, Value::from("no-such-id"))
        .await
        .unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn get_fails_closed_under_default_deny() {
    // Seed under a permissive domain so a row exists…
    let seed_domain = note_domain(PolicySet::permissive());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = seed(&seed_domain, &mut ctx).await;

    // …then `get` through a default-deny domain over the *same* store. No policy
    // admits the read, so it must be `Forbidden` — never the row as a fallback.
    let denied_domain = note_domain(PolicySet::new());
    let err = Note::get(&denied_domain, &mut ctx, id).await.unwrap_err();
    assert!(
        matches!(err, Error::Forbidden(_)),
        "expected Forbidden, got {err:?}"
    );
}
