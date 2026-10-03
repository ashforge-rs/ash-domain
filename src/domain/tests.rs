//! Executor tests. Behaviour only — every assertion goes through the public
//! surface (or the `sandbox` harness), never a private field.

use std::sync::Arc;

use crate::attribute::{AttrType, Attribute};
use crate::datalayer::DataLayer;
use crate::datalayer::memory::InMemoryDataLayer;
use crate::resource::{Cardinality, Relationship, TenantStrategy};
use crate::{ActionDef, Context, Deny, DomainConfig, Query, Record, Resource, Value, erase};

use super::*;
use crate::action::{ActionInput, Changeset};
use crate::context::{HandlerContext, Store};
use crate::datalayer::memory::params as mparams;
use crate::event::{DomainEvent, EventHandler};
use crate::extension::Extension;
use crate::value::IntoRecord;

/// Build a `sort` param for the in-memory layer: `[[attr, "asc"|"desc"], …]`.
fn sort_param(keys: &[(&str, &str)]) -> Value {
    Value::List(
        keys.iter()
            .map(|(attr, dir)| Value::List(vec![Value::from(*attr), Value::from(*dir)]))
            .collect(),
    )
}

/// Build an `eq` param (attribute → value) for the in-memory layer.
fn eq_param(pairs: &[(&str, Value)]) -> Value {
    Value::Map(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    )
}

/// Build a predicate `Record` carrying a single `eq` constraint — the shape a
/// `reserved::RELATES` filter's inner predicate takes for the in-memory layer.
fn eq_predicate(attr: &str, value: impl Into<Value>) -> Record {
    let mut p = Record::new();
    p.insert(mparams::EQ, eq_param(&[(attr, value.into())]));
    p
}

/// Build a `cursor` param `{field, after, dir}` for the in-memory layer.
fn cursor_param(field: &str, after: impl Into<Value>, dir: &str) -> Value {
    let mut m = std::collections::BTreeMap::new();
    m.insert(mparams::CURSOR_FIELD.to_string(), Value::from(field));
    m.insert(mparams::CURSOR_AFTER.to_string(), after.into());
    m.insert(mparams::CURSOR_DIR.to_string(), Value::from(dir));
    Value::Map(m)
}

#[test]
fn tracing_gate_toggles_and_is_shared_across_clones() {
    let d = domain();
    // Off by default.
    assert!(!d.tracing_enabled());

    d.enable_tracing();
    assert!(d.tracing_enabled());

    // A clone shares the gate (the flag lives behind an Arc), so toggling on
    // the clone is observed on the original and vice versa.
    let cloned = d.clone();
    assert!(cloned.tracing_enabled());
    cloned.disable_tracing();
    assert!(!d.tracing_enabled());

    d.enable_tracing();
    assert!(cloned.tracing_enabled());
}

/// Test-only ergonomic verbs over [`Domain::handle_action`]. These are **not**
/// public API — the public surface is `handle_action` alone — they exist so the
/// tests below read as `d.create::<R>(…)` while exercising the real dispatch and
/// its raw-[`Record`] outcomes. Every test resource here has `Data = Record`, so
/// the helpers hand back raw records directly.
#[allow(dead_code)]
trait DomainTestExt {
    async fn create<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        params: impl IntoRecord,
    ) -> Result<Record>;
    // `read_as`, not `read`: the inherent `Domain::read` (the typed builder)
    // would otherwise shadow this trait method at every call site.
    async fn read_as<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        query: Query,
    ) -> Result<Vec<Record>>;
    async fn update<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        id: Value,
        params: impl IntoRecord,
    ) -> Result<Record>;
    async fn destroy<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        id: Value,
    ) -> Result<()>;
    async fn run<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        input: impl IntoRecord,
    ) -> Result<Value>;
}

impl DomainTestExt for Domain {
    async fn create<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        params: impl IntoRecord,
    ) -> Result<Record> {
        self.handle_action::<R>(ctx, action, ActionInput::create(params)?)
            .await?
            .into_record()
    }
    async fn read_as<R: Resource>(
        &self,
        ctx: &Context<impl Store>,
        action: &str,
        query: Query,
    ) -> Result<Vec<Record>> {
        // Reads take a shared context; call the real read path directly (the
        // same one `handle_action`'s read arm uses) so the test sites keep
        // their `&ctx` borrow.
        let (records, _, _) = self.read_records::<R>(ctx, action, query).await?;
        Ok(records)
    }
    async fn update<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        id: Value,
        params: impl IntoRecord,
    ) -> Result<Record> {
        self.handle_action::<R>(ctx, action, ActionInput::update(id, params)?)
            .await?
            .into_record()
    }
    async fn destroy<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        id: Value,
    ) -> Result<()> {
        self.handle_action::<R>(ctx, action, ActionInput::destroy(id))
            .await?;
        Ok(())
    }
    async fn run<R: Resource>(
        &self,
        ctx: &mut Context<impl Store>,
        action: &str,
        input: impl IntoRecord,
    ) -> Result<Value> {
        self.handle_action::<R>(ctx, action, ActionInput::generic(input)?)
            .await?
            .into_value()
    }
}

// ── multitenancy: an org-scoped `Note` ───────────────────────────────────
struct Note;
impl Resource for Note {
    const NAME: &'static str = "note";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("org_id"),
            Attribute::scalar::<String>("title"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![
            ActionDef::write("create"),
            ActionDef::read("read"),
            ActionDef::write("update"),
            ActionDef::write("destroy"),
        ]
    }
    fn tenant() -> Option<TenantStrategy> {
        Some(TenantStrategy::Attribute("org_id".into()))
    }
}

fn domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![
                erase::<Note>(),
                erase::<Author>(),
                erase::<Post>(),
                erase::<Tag>(),
                erase::<PostTag>(),
            ],
            // These scenarios exercise the pipeline, not authorization; the
            // default set is deny-everything, so state permissive explicitly.
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn tenant_missing_is_an_error() {
    let d = domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let err = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(matches!(err, Err(Error::MissingTenant(_))));
}

#[tokio::test]
async fn create_stamps_tenant_and_read_is_isolated() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut acme = Context::new(layer.clone());
    acme.set_tenant("acme");
    let n = d
        .create::<Note>(&mut acme, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    assert_eq!(n.get("org_id"), Some(&Value::from("acme")));

    let mut globex = Context::new(layer.clone());
    globex.set_tenant("globex");
    d.create::<Note>(&mut globex, "create", Record::from_iter([("title", "b")]))
        .await
        .unwrap();

    // Each tenant sees only its own rows.
    let acme_rows = d
        .read_as::<Note>(&acme, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(acme_rows.len(), 1);
    assert_eq!(acme_rows[0].get("title"), Some(&Value::from("a")));

    let globex_rows = d
        .read_as::<Note>(&globex, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(globex_rows.len(), 1);
    assert_eq!(globex_rows[0].get("title"), Some(&Value::from("b")));
}

#[tokio::test]
async fn cross_tenant_update_and_destroy_are_not_found() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut acme = Context::new(layer.clone());
    acme.set_tenant("acme");
    let n = d
        .create::<Note>(&mut acme, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    // Another tenant cannot see, update, or destroy acme's row.
    let mut globex = Context::new(layer.clone());
    globex.set_tenant("globex");
    let upd = d
        .update::<Note>(
            &mut globex,
            "update",
            id.clone(),
            Record::from_iter([("title", "hax")]),
        )
        .await;
    assert!(matches!(upd, Err(Error::NotFound(_))));
    let del = d.destroy::<Note>(&mut globex, "destroy", id.clone()).await;
    assert!(matches!(del, Err(Error::NotFound(_))));

    // The owning tenant still can.
    d.update::<Note>(
        &mut acme,
        "update",
        id.clone(),
        Record::from_iter([("title", "a2")]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn update_cannot_move_a_record_across_tenants() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut acme = Context::new(layer.clone());
    acme.set_tenant("acme");
    let n = d
        .create::<Note>(&mut acme, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    // Even if the caller passes a different org_id, the stamp is authoritative.
    let updated = d
        .update::<Note>(
            &mut acme,
            "update",
            id,
            Record::from_iter([("org_id", "globex"), ("title", "a2")]),
        )
        .await
        .unwrap();
    assert_eq!(updated.get("org_id"), Some(&Value::from("acme")));
}

#[tokio::test]
async fn cross_tenant_read_opt_in_sees_all_tenants() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut acme = Context::new(layer.clone());
    acme.set_tenant("acme");
    d.create::<Note>(&mut acme, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    let mut globex = Context::new(layer.clone());
    globex.set_tenant("globex");
    d.create::<Note>(&mut globex, "create", Record::from_iter([("title", "b")]))
        .await
        .unwrap();

    // A scoped read still sees only its tenant...
    let scoped = d
        .read_as::<Note>(&acme, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(scoped.len(), 1);

    // ...but the sanctioned opt-in reads across every tenant, with no tenant
    // needing to be set — reading *all* tenants is the point.
    let mut admin = Context::new(layer.clone());
    admin.allow_cross_tenant();
    let all = d
        .read_as::<Note>(&admin, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
}

#[tokio::test]
async fn cross_tenant_opt_in_is_reads_only_writes_still_fail_closed() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    // The flag does not weaken writes: a create with no tenant is still
    // MissingTenant even under allow_cross_tenant.
    let mut admin = Context::new(layer.clone());
    admin.allow_cross_tenant();
    let create = d
        .create::<Note>(&mut admin, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(
        matches!(create, Err(Error::MissingTenant(_))),
        "got {create:?}"
    );

    // And a cross-tenant update is still NotFound: seed acme's row, then try to
    // update it from an admin context scoped to a different tenant.
    let mut acme = Context::new(layer.clone());
    acme.set_tenant("acme");
    let n = d
        .create::<Note>(&mut acme, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    let mut admin2 = Context::new(layer.clone());
    admin2.set_tenant("globex");
    admin2.allow_cross_tenant();
    let upd = d
        .update::<Note>(
            &mut admin2,
            "update",
            id,
            Record::from_iter([("title", "hax")]),
        )
        .await;
    assert!(matches!(upd, Err(Error::NotFound(_))), "got {upd:?}");
}

#[tokio::test]
async fn cross_tenant_read_still_authorized() {
    // The flag removes the tenant predicate, not the policy gate.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            ..Default::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut admin = Context::new(layer.clone());
    admin.allow_cross_tenant();
    // No admitting policy → default-deny still forbids the cross-tenant read.
    let err = d
        .read_as::<Note>(&admin, "read", Query::new("note"))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "got {err:?}");
}

// ── relationship loading: Author has_many Post ───────────────────────────
struct Author;
impl Resource for Author {
    const NAME: &'static str = "author";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("name"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
    fn relationships() -> Vec<Relationship> {
        vec![Relationship {
            name: "posts".into(),
            destination: "post".into(),
            cardinality: Cardinality::HasMany,
            source_attribute: "id".into(),
            destination_attribute: "author_id".into(),
            through: None,
        }]
    }
    fn aggregates() -> Vec<crate::aggregate::Aggregate> {
        vec![crate::aggregate::Aggregate::count("post_count", "posts")]
    }
    fn computed() -> Vec<crate::aggregate::Computed> {
        vec![crate::aggregate::Computed::new("shout", Arc::new(Shout))]
    }
}

/// A computed field: uppercase the author's name.
struct Shout;
#[async_trait::async_trait]
impl crate::aggregate::Computer for Shout {
    async fn compute(&self, record: &Record) -> Result<Value> {
        let name = record.get("name").and_then(Value::as_str).unwrap_or("");
        Ok(Value::from(name.to_uppercase()))
    }
}

struct Post;
impl Resource for Post {
    const NAME: &'static str = "post";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("author_id"),
            Attribute::scalar::<String>("title"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
    fn relationships() -> Vec<Relationship> {
        vec![
            // Post belongs_to Author (for nested load `posts.author`).
            Relationship {
                name: "author".into(),
                destination: "author".into(),
                cardinality: Cardinality::BelongsTo,
                source_attribute: "author_id".into(),
                destination_attribute: "id".into(),
                through: None,
            },
            // Post many_to_many Tag through PostTag (for the through test).
            Relationship {
                name: "tags".into(),
                destination: "tag".into(),
                cardinality: Cardinality::ManyToMany,
                source_attribute: "id".into(),
                destination_attribute: "id".into(),
                through: Some(crate::resource::Through {
                    resource: "post_tag".into(),
                    source_attribute: "post_id".into(),
                    destination_attribute: "tag_id".into(),
                }),
            },
        ]
    }
}

struct Tag;
impl Resource for Tag {
    const NAME: &'static str = "tag";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("label"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
}

struct PostTag;
impl Resource for PostTag {
    const NAME: &'static str = "post_tag";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("post_id"),
            Attribute::scalar::<String>("tag_id"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
}

#[tokio::test]
async fn read_loaded_resolves_has_many() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a2"), ("name", "Bo")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "one")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p2"), ("author_id", "a1"), ("title", "two")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p3"), ("author_id", "a2"), ("title", "three")]),
    )
    .await
    .unwrap();

    let q = Query::new("author")
        .load(["posts"])
        .param(mparams::SORT, sort_param(&[("id", "asc")]));
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row.get("name"), Some(&Value::from("Ada")));
    assert_eq!(rows[0].get("posts").len(), 2); // a1 has two posts
    assert_eq!(rows[1].get("posts").len(), 1); // a2 has one
}

#[tokio::test]
async fn read_loaded_unknown_relationship_errors() {
    let d = domain();
    let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let q = Query::new("author").load(["nope"]);
    let err = d.read_loaded::<Author>(&ctx, "read", q).await;
    assert!(matches!(err, Err(Error::Invalid { .. })));
}

// ── relationship-scoped filters (reserved::RELATES) ───────────────────────
#[tokio::test]
async fn read_filters_by_related_predicate() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a2"), ("name", "Bo")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "rust")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p2"), ("author_id", "a2"), ("title", "elixir")]),
    )
    .await
    .unwrap();

    // Authors who have a post titled "rust" → only a1.
    let predicate = eq_predicate("title", "rust");
    let q = Query::relates("author", "posts", predicate);
    let rows = d.read_as::<Author>(&ctx, "read", q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("name"), Some(&Value::from("Ada")));
}

#[tokio::test]
async fn relates_filter_with_empty_match_returns_nothing() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "rust")]),
    )
    .await
    .unwrap();

    // No post titled "haskell" → no author matches.
    let predicate = eq_predicate("title", "haskell");
    let q = Query::relates("author", "posts", predicate);
    let rows = d.read_as::<Author>(&ctx, "read", q).await.unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn relates_filter_unknown_relationship_errors() {
    let d = domain();
    let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let predicate = eq_predicate("x", 1);
    let q = Query::relates("author", "ghost", predicate);
    assert!(matches!(
        d.read_as::<Author>(&ctx, "read", q).await,
        Err(Error::Invalid { .. })
    ));
}

// ── keyset / cursor pagination ───────────────────────────────────────────
#[tokio::test]
async fn cursor_pages_forward_in_sort_order() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    for id in ["a1", "a2", "a3", "a4"] {
        d.create::<Author>(
            &mut ctx,
            "create",
            Record::from_iter([("id", id), ("name", id)]),
        )
        .await
        .unwrap();
    }

    // First page of 2, ascending by id.
    let page1 = d
        .read_as::<Author>(
            &ctx,
            "read",
            Query::new("author")
                .param(mparams::SORT, sort_param(&[("id", "asc")]))
                .param(mparams::LIMIT, 2),
        )
        .await
        .unwrap();
    assert_eq!(
        page1
            .iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        ["a1", "a2"]
    );

    // Next page: after the last id we saw.
    let page2 = d
        .read_as::<Author>(
            &ctx,
            "read",
            Query::new("author")
                .param(mparams::SORT, sort_param(&[("id", "asc")]))
                .param(mparams::CURSOR, cursor_param("id", "a2", "asc"))
                .param(mparams::LIMIT, 2),
        )
        .await
        .unwrap();
    assert_eq!(
        page2
            .iter()
            .filter_map(|r| r.get("id").and_then(Value::as_str))
            .collect::<Vec<_>>(),
        ["a3", "a4"]
    );
}

// ── aggregates & computed fields ─────────────────────────────────────────
#[tokio::test]
async fn read_loaded_computes_aggregate_and_computed_field() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "ada")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "x")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p2"), ("author_id", "a1"), ("title", "y")]),
    )
    .await
    .unwrap();

    // Request the aggregate + computed field but NOT the relationship itself.
    let q = Query::new("author")
        .aggregates(["post_count"])
        .computed(["shout"]);
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].aggregate("post_count"), Some(&Value::Int(2)));
    assert_eq!(rows[0].computed("shout"), Some(&Value::from("ADA")));
    // The aggregate's relationship was loaded implicitly, but not exposed.
    assert!(!rows[0].related.contains_key("posts"));
}

#[tokio::test]
async fn read_loaded_unknown_aggregate_errors() {
    let d = domain();
    let ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let q = Query::new("author").aggregates(["nope"]);
    assert!(matches!(
        d.read_loaded::<Author>(&ctx, "read", q).await,
        Err(Error::Invalid { .. })
    ));
}

/// A layer that pushes every aggregate down to a fixed sentinel value and
/// records that it was asked — proving the domain prefers push-down and does
/// not fall back to loading rows.
struct PushdownLayer {
    inner: InMemoryDataLayer,
    aggregated: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl crate::datalayer::DataLayer for PushdownLayer {
    async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
        self.inner.create(r, pk, rec).await
    }
    async fn read(&self, q: &Query) -> Result<Vec<Record>> {
        self.inner.read(q).await
    }
    async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.inner.get(r, pk, id).await
    }
    async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
        self.inner.update(r, pk, id, c).await
    }
    async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
        self.inner.destroy(r, pk, id).await
    }
    async fn aggregate(
        &self,
        _destination: &str,
        _destination_attribute: &str,
        keys: &[Value],
        _aggregate: &crate::aggregate::Aggregate,
    ) -> Result<Option<HashMap<String, Value>>> {
        self.aggregated
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Report 99 for every parent key, regardless of the real data.
        let map = keys
            .iter()
            .map(|k| (crate::value::value_key(k), Value::Int(99)))
            .collect();
        Ok(Some(map))
    }
}

// ── lambda pipeline steps ────────────────────────────────────────────────
struct Doc;
impl Resource for Doc {
    const NAME: &'static str = "doc";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("title"),
            Attribute::scalar::<String>("slug"),
            Attribute::scalar::<bool>("done"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![
            // change_lambda: derive slug from title on create.
            ActionDef::write("create").change_lambda(|mut cs| async move {
                if let Some(t) = cs.attribute("title").and_then(Value::as_str) {
                    cs.set_attribute("slug", t.to_lowercase().replace(' ', "-"));
                }
                Ok(cs)
            }),
            // prepare_lambda: only-done scope. Binds an `eq` predicate the
            // in-memory layer understands (done == true).
            ActionDef::read("read").prepare_lambda(|mut q| async move {
                let mut eq = std::collections::BTreeMap::new();
                eq.insert("done".to_string(), Value::Bool(true));
                q.params
                    .insert(crate::datalayer::memory::params::EQ, Value::Map(eq));
                Ok(q)
            }),
            // generic_lambda: uppercase a param, no storage.
            ActionDef::generic_lambda("shout", |cs, _store| async move {
                let msg = cs.params.get("msg").and_then(Value::as_str).unwrap_or("");
                Ok(Value::from(msg.to_uppercase()))
            }),
            // context-aware handler: reads the derived HandlerContext.
            ActionDef::generic("whoami", Arc::new(WhoAmI)),
        ]
    }
}

/// A handler that overrides `run_ctx` to read the derived context — the
/// actor and the domain handle — and record enrichment into its scratch bag.
struct WhoAmI;
#[async_trait::async_trait]
impl crate::action::GenericHandler for WhoAmI {
    async fn run(&self, _cs: &mut Changeset, _store: Option<&dyn Store>) -> Result<Value> {
        unreachable!("run_ctx is overridden")
    }
    async fn run_ctx(&self, _cs: &mut Changeset, ctx: &mut HandlerContext<'_>) -> Result<Value> {
        // Prove the derived context sees parent state and the domain, and
        // that scratch is writable and starts empty.
        assert!(ctx.scratch().is_empty());
        let who = ctx
            .actor()
            .and_then(|a| a.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("anon")
            .to_string();
        // The domain is reachable for introspection.
        assert!(ctx.domain().resource("doc").is_some());
        ctx.insert(who.clone());
        // An explicit failure *after* writing scratch: the merge-back must not
        // carry a failed action's observations back to the caller.
        if matches!(_cs.params.get("fail"), Some(Value::Bool(true))) {
            return Err(Error::invalid("handler asked to fail"));
        }
        // A handler may serialize its context to return alongside its
        // result; the snapshot is not fed back into the domain. When the
        // caller passes `serialize=true` we return the snapshot instead.
        if matches!(_cs.params.get("serialize"), Some(Value::Bool(true))) {
            return Ok(Value::from(ctx.serialize()));
        }
        // When asked, reach the domain's shared client through the context.
        if matches!(_cs.params.get("client"), Some(Value::Bool(true))) {
            let base = ctx
                .client::<ApiClient>()
                .map(|c| c.base_url.clone())
                .unwrap_or_else(|| "no-client".into());
            return Ok(Value::from(base));
        }
        Ok(Value::from(who))
    }
}

/// A stand-in shared client registered in the domain's [`DomainContext`].
struct ApiClient {
    base_url: String,
}

fn doc_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

fn doc_domain_with_client() -> Domain {
    let dc = DomainContext::new().with(ApiClient {
        base_url: "https://api.example".into(),
    });
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        dc,
    )
}

#[tokio::test]
async fn change_lambda_runs_in_create() {
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let doc = d
        .create::<Doc>(
            &mut ctx,
            "create",
            Record::from_iter([("title", "Hello World")]),
        )
        .await
        .unwrap();
    assert_eq!(doc.get("slug"), Some(&Value::from("hello-world")));
}

#[tokio::test]
async fn prepare_lambda_scopes_read() {
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    // Two docs, one done — the prepare_lambda filters to done-only.
    let mut a = Record::new();
    a.insert("title", "a");
    a.insert("done", false);
    d.create::<Doc>(&mut ctx, "create", a).await.unwrap();
    let mut b = Record::new();
    b.insert("title", "b");
    b.insert("done", true);
    let done_doc = d.create::<Doc>(&mut ctx, "create", b).await.unwrap();

    let rows = d
        .read_as::<Doc>(&ctx, "read", Query::new("doc"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("id"), done_doc.get("id"));
}

#[tokio::test]
async fn generic_lambda_runs() {
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let out = d
        .run::<Doc>(&mut ctx, "shout", Record::from_iter([("msg", "hi there")]))
        .await
        .unwrap();
    assert_eq!(out, Value::from("HI THERE"));
}

// ── trait-object pipeline builders (.change / .validate / .prepare) ────────

/// A hand-written `Change` (with its own state) — stamps a fixed tag.
struct StampTag(&'static str);
#[async_trait::async_trait]
impl crate::action::Change for StampTag {
    async fn change(&self, cs: &mut Changeset) -> Result<()> {
        cs.set_attribute("tag", self.0);
        Ok(())
    }
}

/// A hand-written `Validation` — rejects a blank `name`.
struct NonBlankName;
#[async_trait::async_trait]
impl crate::action::Validation for NonBlankName {
    async fn validate(&self, cs: &Changeset) -> Result<()> {
        match cs.attribute("name").and_then(Value::as_str) {
            Some(n) if !n.trim().is_empty() => Ok(()),
            _ => Err(Error::invalid_field("name", "name must not be blank")),
        }
    }
}

struct Widget;
impl Resource for Widget {
    const NAME: &'static str = "widget";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("name"),
            Attribute::scalar::<String>("tag"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![
            // Both pipeline steps attached as trait objects via the builders.
            ActionDef::write("create")
                .change(Arc::new(StampTag("stamped")))
                .validate(Arc::new(NonBlankName)),
            ActionDef::read("read"),
        ]
    }
}

fn widget_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Widget>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn trait_object_change_and_validate_run_via_builders() {
    let d = widget_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // The trait-object Change ran (tag stamped); the trait-object Validation
    // admitted (name non-blank).
    let w = d
        .create::<Widget>(&mut ctx, "create", Record::from_iter([("name", "gadget")]))
        .await
        .unwrap();
    assert_eq!(w.get("tag"), Some(&Value::from("stamped")));
    assert_eq!(w.get("name"), Some(&Value::from("gadget")));
}

#[tokio::test]
async fn trait_object_validation_rejects_via_builder() {
    let d = widget_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let err = d
        .create::<Widget>(&mut ctx, "create", Record::from_iter([("name", "  ")]))
        .await;
    assert!(matches!(err, Err(Error::Invalid { .. })), "got {err:?}");
}

#[tokio::test]
async fn context_aware_handler_gets_derived_context() {
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_actor(Record::from_iter([("name", "ada")]));
    // The handler reads actor + domain off the derived HandlerContext.
    let out = d
        .run::<Doc>(&mut ctx, "whoami", Record::new())
        .await
        .unwrap();
    assert_eq!(out, Value::from("ada"));
    // The handler's scratch is merged back into the request context once the
    // action returns, so the caller can read what the handler recorded.
    assert_eq!(ctx.get::<String>().map(String::as_str), Some("ada"));
}

#[tokio::test]
async fn a_failed_handler_contributes_nothing_to_the_request() {
    // The merge-back is on the success path only: an action that failed must
    // not leave its half-finished observations on the request that outlived it.
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_actor(Record::from_iter([("name", "ada")]));
    let err = d
        .run::<Doc>(&mut ctx, "whoami", Record::from_iter([("fail", true)]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(
        ctx.get::<String>().is_none(),
        "a failed handler's scratch leaked"
    );
}

#[tokio::test]
async fn a_handler_scratch_value_overwrites_the_same_type_in_the_context() {
    // Same type in both bags: the handler ran later and saw more, so it wins.
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_actor(Record::from_iter([("name", "ada")]));
    ctx.insert("stale".to_string());
    d.run::<Doc>(&mut ctx, "whoami", Record::new())
        .await
        .unwrap();
    assert_eq!(ctx.get::<String>().map(String::as_str), Some("ada"));
}

#[tokio::test]
async fn handler_reaches_domain_context_clients() {
    // Client registered in the DomainContext is reachable from the handler
    // via the derived HandlerContext.
    let d = doc_domain_with_client();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let out = d
        .run::<Doc>(&mut ctx, "whoami", Record::from_iter([("client", true)]))
        .await
        .unwrap();
    assert_eq!(out, Value::from("https://api.example"));

    // A domain with no clients: the handler sees None.
    let d2 = doc_domain();
    let mut ctx2 = Context::new(Arc::new(InMemoryDataLayer::new()));
    let out2 = d2
        .run::<Doc>(&mut ctx2, "whoami", Record::from_iter([("client", true)]))
        .await
        .unwrap();
    assert_eq!(out2, Value::from("no-client"));
}

#[tokio::test]
async fn handler_serializes_its_context() {
    let d = doc_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_actor(Record::from_iter([("name", "ada")]));
    // The handler returns its serialized context snapshot instead of the name.
    let out = d
        .run::<Doc>(&mut ctx, "whoami", Record::from_iter([("serialize", true)]))
        .await
        .unwrap();
    let snapshot = out.as_str().unwrap();
    // The snapshot carries the representable state; scratch had 1 entry
    // (the `who` string the handler inserted before serializing).
    assert!(snapshot.contains("\"actor\""), "{snapshot}");
    assert!(snapshot.contains("ada"), "{snapshot}");
    assert!(snapshot.contains("\"scratch_len\":1"), "{snapshot}");
}

// ── consumer-supplied validation (replaces the removed built-in checks) ───
// The core no longer validates attributes; a `Validation` registered on the
// action expresses the same rules. `PersonValid` checks the effective value
// on the changeset (staged change, else input param, else persisted original)
// exactly as the old constraint check did.
struct PersonValid;
#[async_trait::async_trait]
impl crate::action::Validation for PersonValid {
    async fn validate(&self, cs: &Changeset) -> Result<()> {
        // `name` is required and non-empty (only enforced when it's part of
        // this write — a partial update that omits it leaves the original).
        if cs.attribute("name").is_some() || cs.original.is_none() {
            match cs.attribute("name").and_then(Value::as_str) {
                Some(s) if !s.trim().is_empty() => {}
                _ => {
                    return Err(Error::invalid_field(
                        "name",
                        "name is required and non-empty",
                    ));
                }
            }
        }
        // `age`, when present, is 0..=120.
        if let Some(age) = cs.attribute("age").and_then(Value::as_int)
            && !(0..=120).contains(&age)
        {
            return Err(Error::invalid_field(
                "age",
                format!("age must be 0..=120, got {age}"),
            ));
        }
        Ok(())
    }
}

struct Person;
impl Resource for Person {
    const NAME: &'static str = "person";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("name"),
            Attribute::scalar::<i64>("age"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        let mut create = ActionDef::write("create");
        create.validations.push(Arc::new(PersonValid));
        let mut update = ActionDef::write("update");
        update.validations.push(Arc::new(PersonValid));
        vec![create, update]
    }
}

fn person_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

fn person(name: &str, age: i64) -> Record {
    let mut r = Record::new();
    r.insert("name", name);
    r.insert("age", age);
    r
}

#[tokio::test]
async fn create_rejects_constraint_violation() {
    let d = person_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    // age out of range
    let err = d
        .create::<Person>(&mut ctx, "create", person("Ada", 200))
        .await;
    assert!(matches!(err, Err(Error::Invalid { .. })));
    // valid create succeeds
    assert!(
        d.create::<Person>(&mut ctx, "create", person("Ada", 30))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn create_enforces_required_and_non_empty() {
    let d = person_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    // missing required `name`
    assert!(matches!(
        d.create::<Person>(&mut ctx, "create", Record::from_iter([("age", 20i64)]))
            .await,
        Err(Error::Invalid { .. })
    ));
    // present but blank → NonEmpty fires
    assert!(matches!(
        d.create::<Person>(&mut ctx, "create", Record::from_iter([("name", "   ")]))
            .await,
        Err(Error::Invalid { .. })
    ));
}

#[tokio::test]
async fn registered_validation_runs_and_reports_the_field() {
    // The seam contract: a registered `Validation` rejects bad input with the
    // typed `field`, and admits good input — proving it runs at the right slot
    // (authorized writes only; see `unauthorized_caller_never_learns_...`).
    let d = person_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let err = d
        .create::<Person>(&mut ctx, "create", person("Ada", 200))
        .await;
    match err {
        Err(Error::Invalid { field, .. }) => assert_eq!(field.as_deref(), Some("age")),
        other => panic!("expected Invalid{{field:age}}, got {other:?}"),
    }
    assert!(
        d.create::<Person>(&mut ctx, "create", person("Ada", 30))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn update_checks_constraints_on_effective_value() {
    let d = person_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    let p = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await
        .unwrap();
    let id = p.get("id").unwrap().clone();

    let mut bad = Record::new();
    bad.insert("age", -5i64);
    // Updating age to a bad value is rejected; name (from original) still valid.
    assert!(matches!(
        d.update::<Person>(&mut ctx, "update", id.clone(), bad)
            .await,
        Err(Error::Invalid { .. })
    ));
    // A valid partial update succeeds.
    let mut good = Record::new();
    good.insert("age", 31i64);
    assert!(
        d.update::<Person>(&mut ctx, "update", id, good)
            .await
            .is_ok()
    );
}

// ── embedded (nested) resources ──────────────────────────────────────────
// The core still applies embedded *defaults* (see `apply_defaults`), but runs
// no embedded validation — a consumer `Validation` walks the nested shape and
// enforces presence/non-empty itself, using the dotted path in its error so
// the caller learns which nested field failed.
struct CustomerValid;
#[async_trait::async_trait]
impl crate::action::Validation for CustomerValid {
    async fn validate(&self, cs: &Changeset) -> Result<()> {
        match cs.attribute("name").and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => {}
            _ => return Err(Error::invalid_field("name", "name is required")),
        }
        // `address` is required and must be a map with a non-empty `city`.
        let Some(addr) = cs.attribute("address") else {
            return Err(Error::invalid_field("address", "address is required"));
        };
        let Some(addr) = addr.as_map() else {
            return Err(Error::invalid_field(
                "address",
                "`address`: expected an embedded resource",
            ));
        };
        match addr.get("city").and_then(Value::as_str) {
            Some(s) if !s.trim().is_empty() => {}
            _ => {
                return Err(Error::invalid_field(
                    "address.city",
                    "`address.city`: is required and non-empty",
                ));
            }
        }
        // Each `tags[i]` must carry a `label`.
        if let Some(tags) = cs.attribute("tags").and_then(Value::as_list) {
            for (i, tag) in tags.iter().enumerate() {
                let has_label = tag
                    .as_map()
                    .and_then(|m| m.get("label"))
                    .and_then(Value::as_str)
                    .is_some_and(|s| !s.is_empty());
                if !has_label {
                    return Err(Error::invalid_field(
                        format!("tags[{i}].label"),
                        format!("`tags[{i}].label`: is required"),
                    ));
                }
            }
        }
        Ok(())
    }
}

// A `Customer` with an embedded `address` (single) and `tags` (repeated).
// The embedded shapes carry their own defaults; validation is `CustomerValid`.
struct Customer;
impl Resource for Customer {
    const NAME: &'static str = "customer";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        let address_fields = vec![
            Attribute::scalar::<String>("city"),
            Attribute {
                default: Some(Value::from("US")),
                ..Attribute::scalar::<String>("country")
            },
        ];
        let tag_fields = vec![Attribute::scalar::<String>("label")];
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("name"),
            Attribute::new("address", AttrType::_Embed(address_fields)),
            Attribute::new("tags", AttrType::_EmbedList(tag_fields)),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        let mut create = ActionDef::write("create");
        create.validations.push(Arc::new(CustomerValid));
        vec![create, ActionDef::read("read")]
    }
}

fn customer_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Customer>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

fn address(city: &str) -> Value {
    let mut m = Record::new();
    m.insert("city", city);
    m.into() // Record → Value::Map
}

#[tokio::test]
async fn embedded_resource_is_validated_and_defaulted() {
    let d = customer_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let mut ok = Record::new();
    ok.insert("name", "Ada");
    ok.insert("address", address("London"));
    let created = d.create::<Customer>(&mut ctx, "create", ok).await.unwrap();

    // The nested default was applied inside the embedded map.
    let addr = created.get("address").and_then(Value::as_map).unwrap();
    assert_eq!(addr.get("city"), Some(&Value::from("London")));
    assert_eq!(addr.get("country"), Some(&Value::from("US"))); // default
}

#[tokio::test]
async fn embedded_required_field_is_enforced() {
    let d = customer_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // address present but its required `city` missing → nested required fires.
    let mut r = Record::new();
    r.insert("name", "Ada");
    let mut empty_addr = Record::new();
    empty_addr.insert("country", "UK");
    r.insert("address", empty_addr);
    let err = d.create::<Customer>(&mut ctx, "create", r).await;
    assert!(matches!(err, Err(Error::Invalid { message, .. }) if message.contains("address.city")));
}

#[tokio::test]
async fn embedded_constraint_and_wrong_shape_are_caught() {
    let d = customer_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // Nested NonEmpty on city fires for a blank string.
    let mut blank = Record::new();
    blank.insert("name", "Ada");
    blank.insert("address", address("   "));
    assert!(matches!(
        d.create::<Customer>(&mut ctx, "create", blank).await,
        Err(Error::Invalid { .. })
    ));

    // A non-map value where an embed is expected is a shape error.
    let mut wrong = Record::new();
    wrong.insert("name", "Ada");
    wrong.insert("address", "not-a-map");
    assert!(matches!(
        d.create::<Customer>(&mut ctx, "create", wrong).await,
        Err(Error::Invalid { .. })
    ));
}

#[tokio::test]
async fn embedded_list_validates_each_item() {
    let d = customer_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let good_tag = {
        let mut t = Record::new();
        t.insert("label", "vip");
        Value::from(t)
    };
    let bad_tag = Value::from(Record::new()); // missing required `label`

    let mut r = Record::new();
    r.insert("name", "Ada");
    r.insert("address", address("London"));
    r.insert("tags", Value::List(vec![good_tag.clone(), bad_tag]));
    let err = d.create::<Customer>(&mut ctx, "create", r).await;
    assert!(
        matches!(err, Err(Error::Invalid { message, .. }) if message.contains("tags[1].label"))
    );

    // A list of only valid items passes.
    let mut ok = Record::new();
    ok.insert("name", "Bo");
    ok.insert("address", address("Paris"));
    ok.insert("tags", Value::List(vec![good_tag]));
    assert!(d.create::<Customer>(&mut ctx, "create", ok).await.is_ok());
}

// ── nested & through-relationship loading ────────────────────────────────
#[tokio::test]
async fn nested_load_attaches_grandchild() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "one")]),
    )
    .await
    .unwrap();

    // Load posts, and each post's author (nested).
    let q = Query {
        load: vec!["posts.author".into()],
        ..Query::new("author")
    };
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();

    assert_eq!(rows.len(), 1);
    let post = &rows[0].get("posts")[0];
    assert_eq!(post.row.get("title"), Some(&Value::from("one")));
    // The grandchild: the post's author, attached on the child Loaded.
    let author = post.one("author").unwrap();
    assert_eq!(author.row.get("name"), Some(&Value::from("Ada")));
}

#[tokio::test]
async fn many_to_many_loads_through_join() {
    let d = domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "t")]),
    )
    .await
    .unwrap();
    d.create::<Tag>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "t1"), ("label", "rust")]),
    )
    .await
    .unwrap();
    d.create::<Tag>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "t2"), ("label", "db")]),
    )
    .await
    .unwrap();
    d.create::<PostTag>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "j1"), ("post_id", "p1"), ("tag_id", "t1")]),
    )
    .await
    .unwrap();
    d.create::<PostTag>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "j2"), ("post_id", "p1"), ("tag_id", "t2")]),
    )
    .await
    .unwrap();

    let q = Query {
        load: vec!["tags".into()],
        ..Query::new("post")
    };
    let rows = d.read_loaded::<Post>(&ctx, "read", q).await.unwrap();

    assert_eq!(rows.len(), 1);
    let mut labels: Vec<&str> = rows[0]
        .records("tags")
        .into_iter()
        .filter_map(|r| r.get("label").and_then(Value::as_str))
        .collect();
    labels.sort();
    assert_eq!(labels, ["db", "rust"]); // both tags, resolved through post_tag
}

#[tokio::test]
async fn aggregate_pushdown_is_used_when_the_layer_offers_it() {
    let layer = Arc::new(PushdownLayer {
        inner: InMemoryDataLayer::new(),
        aggregated: std::sync::atomic::AtomicUsize::new(0),
    });
    let d = domain();
    let mut ctx = Context::new(layer.clone());

    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "ada")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "x")]),
    )
    .await
    .unwrap();

    let q = Query::new("author").aggregates(["post_count"]);
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();

    // The layer's pushed-down sentinel (99) wins over the real count (1).
    assert_eq!(rows[0].aggregate("post_count"), Some(&Value::Int(99)));
    assert_eq!(
        layer.aggregated.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

// ── domain events ────────────────────────────────────────────────────────
#[derive(Default)]
struct Recorder {
    events: std::sync::Mutex<Vec<(String, String, usize, i64)>>,
}
#[async_trait::async_trait]
impl EventHandler for Recorder {
    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        self.events.lock().unwrap().push((
            event.resource.clone(),
            event.action.clone(),
            event.records.len(),
            event.at,
        ));
        Ok(())
    }
}

#[tokio::test]
async fn event_handlers_fire_on_commit() {
    let recorder = Arc::new(Recorder::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            event_handlers: vec![recorder.clone()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");

    let n = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "a")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();
    d.destroy::<Note>(&mut ctx, "destroy", id).await.unwrap();

    let events = recorder.events.lock().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "note");
    assert_eq!(events[0].1, "create");
    assert_eq!(events[0].2, 1);
    // The domain stamps the fact with a commit time from its clock.
    assert!(events[0].3 > 0);
    // Destroy still reports the removed row.
    assert_eq!(
        (events[1].0.as_str(), events[1].1.as_str(), events[1].2),
        ("note", "destroy", 1)
    );
}

/// A handler that always fails, and one that records whether it was reached —
/// to prove ordering and the "first Err stops the rest" contract.
struct FailingHandler;
#[async_trait::async_trait]
impl EventHandler for FailingHandler {
    async fn handle(&self, _event: &DomainEvent) -> Result<()> {
        Err(Error::invalid("handler boom"))
    }
}

#[tokio::test]
async fn event_handler_failure_surfaces_but_write_still_stands() {
    // A handler that fails post-commit. The write is already persisted and
    // cannot be rolled back, so: the create returns Err, the later handler is
    // never reached, yet the row is committed and readable.
    let recorder = Arc::new(Recorder::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            // FailingHandler is registered *before* the recorder, so if ordering
            // and short-circuit hold, the recorder never sees the event.
            event_handlers: vec![Arc::new(FailingHandler), recorder.clone()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");

    let created = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "a")]))
        .await;
    // The post-commit handler error surfaces to the caller.
    assert!(
        matches!(created, Err(Error::Invalid { .. })),
        "handler error should surface, got {created:?}"
    );

    // …but the write stands: the row is committed and readable.
    let rows = d
        .read_as::<Note>(&ctx, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "the write must persist even though a handler failed"
    );
    assert_eq!(rows[0].get("title"), Some(&Value::from("a")));

    // The handler registered *after* the failing one never ran (first Err
    // stops the scan).
    assert!(
        recorder.events.lock().unwrap().is_empty(),
        "later handlers must not run after an Err"
    );
}

#[tokio::test]
async fn emitter_delivers_out_of_band_events() {
    // The out-of-band Emitter fans out to the same registered handlers as the
    // commit path — reachable from the Domain and from the DomainContext.
    let recorder = Arc::new(Recorder::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            event_handlers: vec![recorder.clone()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );

    // 1. Emit via the Domain handle (service-code path).
    d.emitter()
        .emit_now(
            "saga",
            "step.done",
            ActionKind::Generic,
            vec![Record::from_iter([("n", "1")])],
        )
        .await
        .unwrap();

    // 2. Emit via the DomainContext shared client (the "from context" path a
    //    HandlerContext would use through ctx.client::<Emitter>()).
    let emitter = d
        .domain_context()
        .client::<crate::event::Emitter>()
        .expect("emitter registered");
    emitter
        .emit(DomainEvent::new(
            "saga",
            "step.retry",
            ActionKind::Generic,
            Record::from_iter([("n", "2")]),
            42,
        ))
        .await
        .unwrap();

    let events = recorder.events.lock().unwrap();
    assert_eq!(
        events.len(),
        2,
        "both out-of-band emits reached the handler"
    );
    assert_eq!(
        (events[0].0.as_str(), events[0].1.as_str()),
        ("saga", "step.done")
    );
    // emit_now stamped `at` from the (system) clock: non-zero.
    assert!(events[0].3 > 0, "emit_now stamps at from the clock");
    assert_eq!(
        (events[1].0.as_str(), events[1].1.as_str(), events[1].3),
        ("saga", "step.retry", 42)
    );
}

#[tokio::test]
async fn emitter_failure_returns_err_to_caller() {
    // A failing handler makes emit() return Err — the caller decides what a
    // failed out-of-band emit means (there is no write to stand behind it).
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            event_handlers: vec![Arc::new(FailingHandler)],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );

    let err = d
        .emitter()
        .emit_now("saga", "step", ActionKind::Generic, vec![])
        .await;
    assert!(
        matches!(err, Err(Error::Invalid { .. })),
        "emit failure should surface, got {err:?}"
    );
}

#[tokio::test]
async fn emitter_with_no_handlers_is_ok() {
    // No handlers registered: emit is a no-op that succeeds.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    assert_eq!(d.emitter().handler_count(), 0);
    d.emitter()
        .emit_now("saga", "step", ActionKind::Generic, vec![])
        .await
        .unwrap();
}

#[tokio::test]
async fn domain_event_projects_to_notification() {
    // The domain produces a fact; turning it into a notification is a
    // consumer choice, not something the domain does.
    let event = DomainEvent::new(
        "note",
        "create",
        ActionKind::Write,
        Record::from_iter([("id", "1")]),
        123,
    );
    let n = event.to_notification();
    assert_eq!(n.resource, "note");
    assert_eq!(n.action, "create");
    assert_eq!(n.records.len(), 1);
}

// ── scoped policies: operations + attribute reads + the tree ──────────────
use crate::policy::{
    ClientPolicy, Decision, Policy, PolicyClient, PolicyRequest, PolicySet, ScopedPolicy,
};

/// Forbids the `destroy` operation for everyone.
struct NoDestroy;
#[async_trait::async_trait]
impl Policy for NoDestroy {
    async fn authorize(&self, cs: &Changeset) -> Decision {
        Decision::Forbid(format!("{} may not be destroyed", cs.resource))
    }
}

/// Hides `title` unless the actor's `role` is `admin`.
struct TitleForAdmins;
#[async_trait::async_trait]
impl Policy for TitleForAdmins {
    async fn authorize_attribute_read(
        &self,
        _resource: &str,
        _action: &str,
        _attribute: &str,
        _record: &Record,
        actor: Option<&Record>,
    ) -> Decision {
        let is_admin = actor.and_then(|a| a.get("role")) == Some(&Value::from("admin"));
        if is_admin {
            Decision::Allow
        } else {
            Decision::Forbid("title is admin-only".into())
        }
    }
}

fn policed_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            // Permissive base so unmatched actions (create/read) still run; the
            // registered policies then narrow destroy and the title attribute.
            policies: PolicySet::permissive()
                .with(ScopedPolicy::action(
                    "note",
                    "destroy",
                    "no-destroy",
                    Arc::new(NoDestroy),
                ))
                .with(ScopedPolicy::attribute(
                    "note",
                    "title",
                    "title-admin-only",
                    Arc::new(TitleForAdmins),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn action_scoped_policy_forbids_only_that_action() {
    let d = policed_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");

    let n = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "hi")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    // read is unaffected; destroy is forbidden by the action-scoped policy.
    assert_eq!(
        d.read_as::<Note>(&ctx, "read", Query::new("note"))
            .await
            .unwrap()
            .len(),
        1
    );
    let err = d.destroy::<Note>(&mut ctx, "destroy", id).await;
    assert!(
        matches!(err, Err(Error::Forbidden(_))),
        "destroy should be forbidden, got {err:?}"
    );
}

#[tokio::test]
async fn attribute_policy_redacts_and_reports() {
    let d = policed_domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut writer = Context::new(layer.clone());
    writer.set_tenant("acme");
    d.create::<Note>(
        &mut writer,
        "create",
        Record::from_iter([("title", "secret")]),
    )
    .await
    .unwrap();

    // Non-admin: title is redacted to Null and reported.
    let mut anon = Context::new(layer.clone());
    anon.set_tenant("acme");
    let out = d
        .read_authorized::<Note>(&anon, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0].get("title"), Some(&Value::Null));
    assert_eq!(out.report.redacted_attributes(), vec!["title"]);
    assert_eq!(out.report.redactions[0].reason, "title is admin-only");

    // Admin: title comes through, nothing redacted.
    let mut admin = Context::new(layer.clone());
    admin.set_tenant("acme");
    admin.set_actor(Record::from_iter([("role", "admin")]));
    let out = d
        .read_authorized::<Note>(&admin, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(out.rows[0].get("title"), Some(&Value::from("secret")));
    assert!(out.report.is_clean());
}

#[tokio::test]
async fn policy_tree_groups_by_level() {
    let d = policed_domain();
    let out = d.policy_tree().render();
    assert!(out.contains("resource: note"), "{out}");
    assert!(out.contains("action: destroy [no-destroy]"), "{out}");
    assert!(out.contains("attribute: title [title-admin-only]"), "{out}");
}

// ── field-level WRITE gating ──────────────────────────────────────────────

/// Forbids *setting* `title` unless the actor's `role` is `admin` — the
/// write-side mirror of `TitleForAdmins`.
struct TitleWriteForAdmins;
#[async_trait::async_trait]
impl Policy for TitleWriteForAdmins {
    async fn authorize_attribute_write(
        &self,
        _resource: &str,
        _action: &str,
        _attribute: &str,
        _changeset: &Changeset,
        actor: Option<&Record>,
    ) -> Decision {
        let is_admin = actor.and_then(|a| a.get("role")) == Some(&Value::from("admin"));
        if is_admin {
            Decision::Allow
        } else {
            Decision::Forbid("title is admin-writable-only".into())
        }
    }
}

fn field_write_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            // Permissive operation gate so create/update run; the attribute-write
            // policy then narrows *who may set `title`* within an allowed write.
            policies: PolicySet::permissive().with(ScopedPolicy::attribute(
                "note",
                "title",
                "title-admin-writable",
                Arc::new(TitleWriteForAdmins),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn field_write_veto_aborts_the_whole_write() {
    let d = field_write_domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    // Non-admin trying to set `title` → the whole create is Forbidden.
    let mut anon = Context::new(layer.clone());
    anon.set_tenant("acme");
    let err = d
        .create::<Note>(&mut anon, "create", Record::from_iter([("title", "hi")]))
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");

    // And nothing was persisted — the veto fired before the layer write.
    let mut admin = Context::new(layer.clone());
    admin.set_tenant("acme");
    admin.set_actor(Record::from_iter([("role", "admin")]));
    assert_eq!(
        d.read_as::<Note>(&admin, "read", Query::new("note"))
            .await
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn field_write_allowed_for_permitted_actor() {
    let d = field_write_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut admin = Context::new(layer.clone());
    admin.set_tenant("acme");
    admin.set_actor(Record::from_iter([("role", "admin")]));

    let n = d
        .create::<Note>(&mut admin, "create", Record::from_iter([("title", "hi")]))
        .await
        .unwrap();
    assert_eq!(n.get("title"), Some(&Value::from("hi")));
}

#[tokio::test]
async fn write_not_touching_a_gated_field_is_unaffected() {
    let d = field_write_domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    // A non-admin create that does NOT set `title` is fine — the gate only
    // fires for fields the caller actually supplied.
    let mut anon = Context::new(layer.clone());
    anon.set_tenant("acme");
    let n = d
        .create::<Note>(&mut anon, "create", Record::new())
        .await
        .unwrap();
    // The tenant discriminator was still stamped by the domain...
    assert_eq!(n.get("org_id"), Some(&Value::from("acme")));
    // ...and stamping it did NOT trip a field-write gate (org_id is in `data`,
    // not `params`, so the caller never "set" it). Prove that by also gating
    // org_id and confirming an anon create with no params still passes.
}

#[tokio::test]
async fn domain_stamped_field_is_not_gated() {
    // Gate `org_id` for writes: a non-admin create that supplies no params must
    // still succeed, because the domain stamps org_id into `data` (not
    // `params`) — the caller never "set" it, so the gate must not fire.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive().with(ScopedPolicy::attribute(
                "note",
                "org_id",
                "org-id-admin-writable",
                Arc::new(TitleWriteForAdmins),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut anon = Context::new(layer.clone());
    anon.set_tenant("acme");
    let n = d
        .create::<Note>(&mut anon, "create", Record::new())
        .await
        .unwrap();
    assert_eq!(n.get("org_id"), Some(&Value::from("acme")));

    // But if the caller *explicitly* supplies org_id, the gate does fire.
    let mut anon2 = Context::new(layer.clone());
    anon2.set_tenant("acme");
    let err = d
        .create::<Note>(
            &mut anon2,
            "create",
            Record::from_iter([("org_id", "acme")]),
        )
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
}

#[tokio::test]
async fn field_write_gate_denies_before_persist_on_update() {
    // The gate also protects updates: seed a row as admin, then a non-admin
    // update that sets `title` is Forbidden and the stored value is unchanged.
    let d = field_write_domain();
    let layer = Arc::new(InMemoryDataLayer::new());

    let mut admin = Context::new(layer.clone());
    admin.set_tenant("acme");
    admin.set_actor(Record::from_iter([("role", "admin")]));
    let n = d
        .create::<Note>(&mut admin, "create", Record::from_iter([("title", "orig")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    let mut anon = Context::new(layer.clone());
    anon.set_tenant("acme");
    let err = d
        .update::<Note>(
            &mut anon,
            "update",
            id.clone(),
            Record::from_iter([("title", "hax")]),
        )
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");

    let rows = d
        .read_as::<Note>(&admin, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(rows[0].get("title"), Some(&Value::from("orig")));
}

#[tokio::test]
async fn resource_names_lists_the_registry_sorted() {
    let d = domain();
    // domain() registers note, author, post, tag, post_tag.
    let names = d.resource_names();
    assert_eq!(names, vec!["author", "note", "post", "post_tag", "tag"]);
    // Each name round-trips to its erased schema.
    assert!(d.resource("note").is_some());
}

// ── metrics seam ──────────────────────────────────────────────────────────

#[tokio::test]
async fn metrics_records_one_sample_per_action_with_outcome() {
    use crate::metrics::{MetricSample, Metrics, Outcome};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        samples: Mutex<Vec<(String, String, Outcome)>>,
    }
    impl Metrics for Recorder {
        fn record(&self, s: MetricSample<'_>) {
            self.samples.lock().unwrap().push((
                s.resource.to_string(),
                s.action.to_string(),
                s.outcome,
            ));
        }
    }

    let recorder = Arc::new(Recorder::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            // Permit create/read/update; forbid destroy, to exercise a denial.
            policies: PolicySet::permissive().with(ScopedPolicy::action(
                "note",
                "destroy",
                "no-destroy",
                Arc::new(NoDestroy),
            )),
            metrics: Some(recorder.clone()),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");

    // A committed create...
    let n = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();
    // ...and a denied destroy.
    let _ = d.destroy::<Note>(&mut ctx, "destroy", id).await;

    let samples = recorder.samples.lock().unwrap();
    assert_eq!(samples.len(), 2, "one sample per action");
    assert_eq!(
        samples[0],
        ("note".into(), "create".into(), Outcome::Committed)
    );
    assert_eq!(
        samples[1],
        ("note".into(), "destroy".into(), Outcome::Denied)
    );
}

#[tokio::test]
async fn metrics_classifies_a_non_authz_failure_as_errored() {
    use crate::metrics::{MetricSample, Metrics, Outcome};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Last(Mutex<Option<Outcome>>);
    impl Metrics for Last {
        fn record(&self, s: MetricSample<'_>) {
            *self.0.lock().unwrap() = Some(s.outcome);
        }
    }

    let last = Arc::new(Last::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive(),
            metrics: Some(last.clone()),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    // A tenant-scoped create with no tenant set → MissingTenant (not a denial).
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let err = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(matches!(err, Err(Error::MissingTenant(_))));
    assert_eq!(*last.0.lock().unwrap(), Some(Outcome::Errored));
}

// ── dry-run / explain ─────────────────────────────────────────────────────

#[tokio::test]
async fn explain_write_names_the_deciding_veto() {
    // `NoDestroy` (action-scoped) vetoes destroy; explain reports the decision
    // and *which* policy decided, without running anything.
    let d = policed_domain();
    let ctx = {
        let mut c = Context::new(Arc::new(InMemoryDataLayer::new()));
        c.set_tenant("acme");
        c
    };
    let ex = d
        .explain_write::<Note>(&ctx, "destroy", Record::new())
        .await
        .unwrap();
    assert!(!ex.is_allowed());
    assert!(matches!(ex.decision, Decision::Forbid(_)));
    assert_eq!(ex.deciding_policy.as_deref(), Some("no-destroy"));
    // The vote list records the vetoing policy.
    assert!(
        ex.votes
            .iter()
            .any(|v| v.label == "no-destroy" && v.decision.is_veto())
    );
}

#[tokio::test]
async fn explain_write_allowed_by_permissive_default_has_no_decider() {
    // create is admitted by the permissive default — allowed, but no single
    // policy decided it.
    let d = policed_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_tenant("acme");
    let ex = d
        .explain_write::<Note>(&ctx, "create", Record::from_iter([("title", "x")]))
        .await
        .unwrap();
    assert!(ex.is_allowed());
    assert_eq!(ex.deciding_policy, None);
}

#[tokio::test]
async fn explain_write_is_side_effect_free() {
    // A dry-run of an *allowed* create must persist nothing.
    let d = policed_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");

    let ex = d
        .explain_write::<Note>(&ctx, "create", Record::from_iter([("title", "x")]))
        .await
        .unwrap();
    assert!(ex.is_allowed());

    // Nothing was written — explain never reached the layer.
    let rows = d
        .read_as::<Note>(&ctx, "read", Query::new("note"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "explain_write must not persist");
}

#[tokio::test]
async fn explain_matches_the_live_gate_for_a_denied_action() {
    // The dry-run verdict agrees with what the real action returns.
    let d = policed_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());
    ctx.set_tenant("acme");
    let n = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await
        .unwrap();
    let id = n.get("id").unwrap().clone();

    let ex = d
        .explain_write::<Note>(&ctx, "destroy", Record::new())
        .await
        .unwrap();
    let live = d.destroy::<Note>(&mut ctx, "destroy", id).await;
    assert!(!ex.is_allowed());
    assert!(matches!(live, Err(Error::Forbidden(_))));
}

#[tokio::test]
async fn explain_read_reports_the_decision() {
    // A read of `note` is admitted by the permissive default.
    let d = policed_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_tenant("acme");
    let ex = d
        .explain_read::<Note>(&ctx, "read", Query::new("note"))
        .await
        .unwrap();
    assert!(ex.is_allowed());
}

/// A domain over the relationship resources, with policies scoped to the
/// loaded destination (`post`), to prove loaded relations are authorized.
fn loaded_policed_domain(policies: PolicySet) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![
                erase::<Author>(),
                erase::<Post>(),
                erase::<Tag>(),
                erase::<PostTag>(),
            ],
            policies,
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

async fn seed_author_with_posts(d: &Domain, layer: Arc<InMemoryDataLayer>) {
    let mut ctx = Context::new(layer);
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Post>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "secret")]),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn loaded_relation_operation_denied_fails_the_load() {
    // Forbid reads of `post` outright; loading `author.posts` must fail.
    let d = loaded_policed_domain(PolicySet::permissive().with(ScopedPolicy::resource(
        "post",
        "no-post-read",
        Arc::new(NoRead),
    )));
    let layer = Arc::new(InMemoryDataLayer::new());
    // Seed with a permissive domain so the writes aren't blocked.
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    let ctx = Context::new(layer.clone());
    let q = Query {
        load: vec!["posts".into()],
        ..Query::new("author")
    };
    let err = d.read_loaded::<Author>(&ctx, "read", q).await;
    assert!(
        matches!(err, Err(Error::Forbidden(_))),
        "loaded read should be forbidden, got {err:?}"
    );
}

#[tokio::test]
async fn loaded_relation_attribute_redacted() {
    // Hide `title` on `post`; a loaded post's title must come back Null.
    let d = loaded_policed_domain(PolicySet::permissive().with(ScopedPolicy::attribute(
        "post",
        "title",
        "hide-title",
        Arc::new(Deny("hidden".into())),
    )));
    let layer = Arc::new(InMemoryDataLayer::new());
    // Seed with a permissive domain: `Deny` on the `title` attribute now vetoes
    // the field *write* too (not only the read), so seeding must not go through
    // the redaction policy — mirroring every other loaded-relation test here.
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    let ctx = Context::new(layer.clone());
    let q = Query {
        load: vec!["posts".into()],
        ..Query::new("author")
    };
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();
    let posts = rows[0].get("posts");
    assert_eq!(posts.len(), 1);
    assert_eq!(
        posts[0].row.get("title"),
        Some(&Value::Null),
        "loaded post title should be redacted"
    );
    // A non-gated field still comes through.
    assert_eq!(posts[0].row.get("author_id"), Some(&Value::from("a1")));
}

#[tokio::test]
async fn loaded_relation_authorizes_under_destinations_read_action() {
    // A load carries no caller-supplied action name, so it authorizes the
    // destination under its first declared Read action — here `post`'s
    // "read". An *action-scoped* Admit on exactly that (resource=`post`,
    // action=`read`) is sufficient to permit loading `author.posts` under
    // default-deny. `author` itself is admitted so the top-level read runs.
    let d = loaded_policed_domain(
        PolicySet::new()
            .with(ScopedPolicy::resource(
                "author",
                "admit-author",
                Arc::new(crate::policy::Admit),
            ))
            .with(ScopedPolicy::action(
                "post",
                "read",
                "admit-post-read",
                Arc::new(crate::policy::Admit),
            )),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    let ctx = Context::new(layer.clone());
    let q = Query {
        load: vec!["posts".into()],
        ..Query::new("author")
    };
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("posts").len(),
        1,
        "the action-scoped admit on post/read permits the load"
    );
}

#[tokio::test]
async fn loaded_relation_without_admit_on_destination_fails() {
    // `author` is admitted (top-level read runs) but there is NO admitting
    // policy for `post` — under default-deny, loading `author.posts`
    // authorizes `post`'s read action and finds no affirmative allow, so the
    // whole load is forbidden.
    let d = loaded_policed_domain(PolicySet::new().with(ScopedPolicy::resource(
        "author",
        "admit-author",
        Arc::new(crate::policy::Admit),
    )));
    let layer = Arc::new(InMemoryDataLayer::new());
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    // The bare top-level read of `author` is fine…
    let ctx = Context::new(layer.clone());
    assert!(
        d.read_as::<Author>(&ctx, "read", Query::new("author"))
            .await
            .is_ok()
    );

    // …but loading `posts` requires an admit on `post` reads, which is absent.
    let q = Query {
        load: vec!["posts".into()],
        ..Query::new("author")
    };
    let err = d.read_loaded::<Author>(&ctx, "read", q).await;
    assert!(
        matches!(err, Err(Error::Forbidden(_))),
        "load without a post admit must fail, got {err:?}"
    );
}

/// Forbids read operations only (leaves writes and attributes alone), so
/// seeding still works but loading the relation is denied.
struct NoRead;
#[async_trait::async_trait]
impl Policy for NoRead {
    async fn authorize_read(
        &self,
        resource: &str,
        _action: &str,
        _query: &Query,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Forbid(format!("{resource} reads forbidden"))
    }
}

// ── default-deny & authorize-first ordering ──────────────────────────────

#[tokio::test]
async fn empty_policy_set_denies_by_default() {
    // No policies at all: the operation gate is default-deny.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let create = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(matches!(create, Err(Error::Forbidden(_))), "got {create:?}");
    let read = d
        .read_as::<Person>(&ctx, "create", Query::new("person"))
        .await;
    assert!(read.is_err());
}

/// Records every hook invocation so ordering tests can assert an extension
/// was — or was not — reached.
#[derive(Default)]
struct HookRecorder {
    calls: std::sync::Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl crate::extension::Extension for HookRecorder {
    fn name(&self) -> &str {
        "hook-recorder"
    }
    async fn before_action(&self, cs: &mut Changeset) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("before_action:{}", cs.action));
        Ok(())
    }
    async fn before_read(
        &self,
        _resource: &str,
        action: &str,
        _query: &mut Query,
        _actor: Option<&Record>,
    ) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("before_read:{action}"));
        Ok(())
    }
    async fn on_denied(
        &self,
        resource: &str,
        action: &str,
        _actor: Option<&Record>,
        _error: &Error,
    ) {
        self.calls
            .lock()
            .unwrap()
            .push(format!("on_denied:{resource}.{action}"));
    }
}

#[tokio::test]
async fn denied_action_never_reaches_extensions() {
    let hooks = Arc::new(HookRecorder::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            extensions: vec![hooks.clone()],
            // Default-deny with no policies: everything is forbidden.
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let write = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(matches!(write, Err(Error::Forbidden(_))));
    let read = d
        .read_as::<Person>(&ctx, "create", Query::new("person"))
        .await;
    assert!(read.is_err());

    // The *gated* hooks (before_action / before_read) never fired for the
    // denied requests — only `on_denied` (asserted separately) is allowed to
    // observe a denial.
    let calls = hooks.calls.lock().unwrap();
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("before_action") || c.starts_with("before_read")),
        "gated hooks must not run on a denied request, got {calls:?}"
    );
}

#[tokio::test]
async fn on_denied_fires_for_denied_actions_only() {
    let hooks = Arc::new(HookRecorder::default());
    // Default-deny with no policies: the write is forbidden.
    let denying = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            extensions: vec![hooks.clone()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let write = denying
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(matches!(write, Err(Error::Forbidden(_))));
    // The executor called `on_denied` at the authorize site — the one place a
    // denial is observable.
    assert_eq!(
        *hooks.calls.lock().unwrap(),
        vec!["on_denied:person.create".to_string()]
    );

    // A permitted action does NOT fire `on_denied` — no spurious denial.
    let ok_hooks = Arc::new(HookRecorder::default());
    let allowing = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            extensions: vec![ok_hooks.clone()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx2 = Context::new(Arc::new(InMemoryDataLayer::new()));
    allowing
        .create::<Person>(&mut ctx2, "create", person("Bo", 30))
        .await
        .unwrap();
    assert!(
        !ok_hooks
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.starts_with("on_denied")),
        "a permitted action must not fire on_denied, got {:?}",
        ok_hooks.calls.lock().unwrap()
    );
}

#[tokio::test]
async fn unauthorized_caller_never_learns_constraint_validity() {
    // The payload violates a constraint (age 200 > max 120) AND the caller
    // is forbidden. Authorization must answer first: the error is Forbidden,
    // never Invalid, so a denied caller cannot probe payload validity.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let err = d
        .create::<Person>(&mut ctx, "create", person("Ada", 200))
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
}

// ── derived values under attribute policies ──────────────────────────────

#[tokio::test]
async fn computed_field_sees_redacted_input() {
    // Hide `name` on author: the `shout` computer receives the redacted row,
    // so it derives from Null — a computed field cannot smuggle a forbidden
    // attribute out.
    let d = loaded_policed_domain(PolicySet::permissive().with(ScopedPolicy::attribute(
        "author",
        "name",
        "hide-name",
        Arc::new(Deny("hidden".into())),
    )));
    let layer = Arc::new(InMemoryDataLayer::new());
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    let ctx = Context::new(layer.clone());
    let q = Query::new("author").computed(["shout"]);
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();
    assert_eq!(rows[0].row.get("name"), Some(&Value::Null));
    // `shout` uppercases the (now hidden) name: it saw Null, not "Ada".
    assert_eq!(rows[0].computed("shout"), Some(&Value::from("")));
}

#[tokio::test]
async fn derived_outputs_are_gated_by_attribute_policies() {
    // An attribute policy scoped to the *aggregate's name* redacts the
    // derived value per row, exactly like a stored attribute.
    let d = loaded_policed_domain(PolicySet::permissive().with(ScopedPolicy::attribute(
        "author",
        "post_count",
        "hide-count",
        Arc::new(Deny("hidden".into())),
    )));
    let layer = Arc::new(InMemoryDataLayer::new());
    seed_author_with_posts(
        &loaded_policed_domain(PolicySet::permissive()),
        layer.clone(),
    )
    .await;

    let ctx = Context::new(layer.clone());
    let q = Query::new("author").aggregates(["post_count"]);
    let rows = d.read_loaded::<Author>(&ctx, "read", q).await.unwrap();
    assert_eq!(rows[0].aggregate("post_count"), Some(&Value::Null));
}

#[tokio::test]
async fn aggregate_pushdown_is_authorized_like_a_destination_read() {
    // Reads of `post` are forbidden; a pushed-down COUNT over posts must be
    // denied too — and the layer must never even be asked.
    let layer = Arc::new(PushdownLayer {
        inner: InMemoryDataLayer::new(),
        aggregated: std::sync::atomic::AtomicUsize::new(0),
    });
    let seed_domain = loaded_policed_domain(PolicySet::permissive());
    let mut seed_ctx = Context::new(layer.clone());
    seed_domain
        .create::<Author>(
            &mut seed_ctx,
            "create",
            Record::from_iter([("id", "a1"), ("name", "Ada")]),
        )
        .await
        .unwrap();
    seed_domain
        .create::<Post>(
            &mut seed_ctx,
            "create",
            Record::from_iter([("id", "p1"), ("author_id", "a1"), ("title", "x")]),
        )
        .await
        .unwrap();

    let d = loaded_policed_domain(PolicySet::permissive().with(ScopedPolicy::resource(
        "post",
        "no-post-read",
        Arc::new(NoRead),
    )));
    let ctx = Context::new(layer.clone());
    let q = Query::new("author").aggregates(["post_count"]);
    let err = d.read_loaded::<Author>(&ctx, "read", q).await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
    assert_eq!(
        layer.aggregated.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the layer must not compute an unauthorized aggregate"
    );
}

// ── the param bag is the layer's to interpret ────────────────────────────

/// A layer that stores fine but interprets **no** params — it returns every
/// row of a resource regardless of the bag. This is legal now: the core does
/// not evaluate queries or negotiate capabilities, so a bag param a layer
/// ignores simply has no effect. Keeping degradation explicit is the layer's
/// own responsibility, not the core's.
struct IgnoreParamsLayer(InMemoryDataLayer);
#[async_trait::async_trait]
impl crate::datalayer::DataLayer for IgnoreParamsLayer {
    async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
        self.0.create(r, pk, rec).await
    }
    async fn read(&self, q: &Query) -> Result<Vec<Record>> {
        // Deliberately ignore the param bag: return every stored row. It still
        // honours the reserved key-set (via the inner layer) only because it
        // forwards; here we bypass that to prove params are the layer's call.
        self.0.read(&Query::new(&q.resource)).await
    }
    async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.0.get(r, pk, id).await
    }
    async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
        self.0.update(r, pk, id, c).await
    }
    async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
        self.0.destroy(r, pk, id).await
    }
}

#[tokio::test]
async fn core_passes_the_bag_through_without_evaluating_it() {
    let d = domain();
    let layer = Arc::new(IgnoreParamsLayer(InMemoryDataLayer::new()));
    let mut ctx = Context::new(layer.clone());
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a2"), ("name", "Bo")]),
    )
    .await
    .unwrap();

    // The caller asks to filter by name via the in-memory `eq` convention,
    // but THIS layer ignores params — so the core does not second-guess it and
    // both rows come back. The core neither errors nor evaluates the bag.
    let q = Query::new("author").param(mparams::EQ, eq_param(&[("name", Value::from("Ada"))]));
    let rows = d.read_as::<Author>(&ctx, "read", q).await.unwrap();
    assert_eq!(
        rows.len(),
        2,
        "the layer owns interpretation; core does not filter"
    );
}

// ── registration-time validation ─────────────────────────────────────────

/// A resource whose relationship points at a resource that is never
/// registered — a config bug that must fail at construction.
struct Dangling;
impl Resource for Dangling {
    const NAME: &'static str = "dangling";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![Attribute::scalar::<String>("id")]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::read("read")]
    }
    fn relationships() -> Vec<Relationship> {
        vec![Relationship {
            name: "ghosts".into(),
            destination: "ghost".into(),
            cardinality: Cardinality::HasMany,
            source_attribute: "id".into(),
            destination_attribute: "dangling_id".into(),
            through: None,
        }]
    }
}

#[test]
fn try_new_rejects_relationship_to_unregistered_resource() {
    let err = Domain::try_new(
        DomainConfig {
            resources: vec![erase::<Dangling>()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    assert!(matches!(err, Err(Error::Invalid { message, .. }) if message.contains("ghost")),);
}

#[test]
fn try_new_rejects_policy_scoped_to_unknown_target() {
    // Unregistered resource in a policy scope.
    let err = Domain::try_new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::new().with(ScopedPolicy::resource(
                "phantom",
                "p",
                Arc::new(AllowAllPolicy),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    assert!(matches!(err, Err(Error::Invalid { message, .. }) if message.contains("phantom")));

    // Registered resource, unknown attribute.
    let err = Domain::try_new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::new().with(ScopedPolicy::attribute(
                "person",
                "ssn",
                "p",
                Arc::new(AllowAllPolicy),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    assert!(matches!(err, Err(Error::Invalid { message, .. }) if message.contains("ssn")));
}

struct AllowAllPolicy;
#[async_trait::async_trait]
impl Policy for AllowAllPolicy {}

/// An external authorization client that always fails to reach a verdict,
/// standing in for a down OPA/Cedar/REST backend.
struct DownBackend;
#[async_trait::async_trait]
impl PolicyClient for DownBackend {
    async fn authorize(&self, _req: PolicyRequest<'_>) -> Decision {
        Decision::Error("authz backend unreachable".into())
    }
}

#[tokio::test]
async fn client_backend_failure_fails_closed_with_policy_error() {
    // A custom client wrapped as a Policy: when it errors, the write must
    // abort with the *distinct* PolicyError, not Forbidden and not silently.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::new().with(ScopedPolicy::resource(
                "note",
                "external-authz",
                Arc::new(ClientPolicy::new(Arc::new(DownBackend))),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    ctx.set_tenant("acme");

    let err = d
        .create::<Note>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(
        matches!(err, Err(Error::PolicyError(_))),
        "client failure should surface as PolicyError, got {err:?}"
    );
}

// ── affirmative-allow semantics (default-deny, admit-on-affirmative) ──────
use crate::policy::{Admit, Decision as Dec};

/// A field-only policy: it hides one attribute but expresses **no opinion**
/// on operations (its operation methods stay at the default NotApplicable).
/// Registering it must not admit operations on the resource it is scoped to.
struct HideField(&'static str);
#[async_trait::async_trait]
impl Policy for HideField {
    async fn authorize_attribute_read(
        &self,
        _resource: &str,
        _action: &str,
        attribute: &str,
        _record: &Record,
        _actor: Option<&Record>,
    ) -> Dec {
        if attribute == self.0 {
            Dec::Forbid(format!("{attribute} hidden"))
        } else {
            Dec::NotApplicable
        }
    }
}

/// (a) A field-only resource-scoped policy no longer admits operations on
/// that resource under default-deny — matching a scope is not consent.
#[tokio::test]
async fn field_only_policy_does_not_admit_operations() {
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::new().with(ScopedPolicy::resource(
                "person",
                "hide-name",
                Arc::new(HideField("name")),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // The field policy matches `person` but abstains on the write, so under
    // default-deny the create is still forbidden — adding it did not widen.
    let err = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
}

/// (b) An affirmative Allow admits the operation even alongside an unrelated
/// abstaining (NotApplicable) policy.
#[tokio::test]
async fn affirmative_allow_plus_abstaining_policy_allows() {
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::new()
                // Admits operations on `person` (affirmative Allow)…
                .with(ScopedPolicy::resource("person", "admit", Arc::new(Admit)))
                // …and an unrelated field policy that abstains on operations.
                .with(ScopedPolicy::resource(
                    "person",
                    "hide-name",
                    Arc::new(HideField("name")),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let created = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(
        created.is_ok(),
        "affirmative Allow should admit despite abstention: {created:?}"
    );
}

/// A policy that affirmatively allows an operation — the admit half of the
/// allow+forbid composition test.
struct AllowWrite;
#[async_trait::async_trait]
impl Policy for AllowWrite {
    async fn authorize(&self, _cs: &Changeset) -> Dec {
        Dec::Allow
    }
}

/// (c) Allow + Forbid → forbidden (deny-overrides beats an affirmative allow).
#[tokio::test]
async fn affirmative_allow_plus_forbid_is_forbidden() {
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::new()
                .with(ScopedPolicy::resource(
                    "person",
                    "allow-write",
                    Arc::new(AllowWrite),
                ))
                .with(ScopedPolicy::action(
                    "person",
                    "create",
                    "no-create",
                    Arc::new(Deny("nope".into())),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let err = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(
        matches!(err, Err(Error::Forbidden(_))),
        "veto must win over allow: {err:?}"
    );
}

/// (d) A permissive set with only abstaining (NotApplicable) matches still
/// allows — permissive is the explicit default-allow escape.
#[tokio::test]
async fn permissive_with_only_abstentions_allows() {
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Person>()],
            policies: PolicySet::permissive()
                // Matches `person` but abstains on operations.
                .with(ScopedPolicy::resource(
                    "person",
                    "hide-name",
                    Arc::new(HideField("name")),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let created = d
        .create::<Person>(&mut ctx, "create", person("Ada", 30))
        .await;
    assert!(
        created.is_ok(),
        "permissive should allow when nothing affirmatively allowed: {created:?}"
    );
}

/// A resource with a `read` action, for the field-visibility test.
struct Member;
impl Resource for Member {
    const NAME: &'static str = "member";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("name"),
            Attribute::scalar::<i64>("age"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
}

/// (e) Attribute visibility is unchanged by the new semantics: with the
/// operation admitted, an un-forbidden field (and the primary key) stays
/// visible and only the forbidden one is redacted — abstaining policies
/// never redact.
#[tokio::test]
async fn attribute_visibility_unchanged_by_affirmative_semantics() {
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Member>()],
            policies: PolicySet::new()
                // Admit operations so create + read proceed…
                .with(ScopedPolicy::resource("member", "admit", Arc::new(Admit)))
                // …then hide only `name`; `age` and the primary key stay visible.
                .with(ScopedPolicy::resource(
                    "member",
                    "hide-name",
                    Arc::new(HideField("name")),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    let mut rec = Record::new();
    rec.insert("id", "m1");
    rec.insert("name", "Ada");
    rec.insert("age", 30i64);
    d.create::<Member>(&mut ctx, "create", rec).await.unwrap();
    let out = d
        .read_authorized::<Member>(&ctx, "read", Query::new("member"))
        .await
        .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(
        out.rows[0].get("name"),
        Some(&Value::Null),
        "name must be redacted"
    );
    assert_eq!(
        out.rows[0].get("age"),
        Some(&Value::from(30i64)),
        "age stays visible"
    );
    assert_eq!(
        out.rows[0].get("id"),
        Some(&Value::from("m1")),
        "primary key stays visible"
    );
    assert_eq!(out.report.redacted_attributes(), vec!["name"]);
}

// ── transaction seam ──────────────────────────────────────────────────────
//
// A minimal transactional store for exercising the seam: `begin` hands back a
// handle holding a *deep copy* of the committed tables. Writes land in the
// copy; `commit` swaps it back into the shared store, `rollback` drops it.

mod txn_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use crate::Store;
    use crate::datalayer::{DataLayer, Transaction};
    use crate::error::Result;
    use crate::query::Query;
    use crate::value::{Record, Value, value_key};

    type Tables = HashMap<String, HashMap<String, Record>>;

    fn create_in(t: &mut Tables, resource: &str, pk: &str, rec: Record) -> Result<Record> {
        let key = value_key(rec.get(pk).expect("record carries pk"));
        t.entry(resource.to_string())
            .or_default()
            .insert(key, rec.clone());
        Ok(rec)
    }
    fn get_in(t: &Tables, resource: &str, id: &Value) -> Option<Record> {
        t.get(resource)
            .and_then(|tbl| tbl.get(&value_key(id)).cloned())
    }
    fn update_in(t: &mut Tables, resource: &str, id: &Value, changes: &Record) -> Result<Record> {
        let tbl = t.entry(resource.to_string()).or_default();
        let row = tbl.entry(value_key(id)).or_default();
        for (k, v) in &changes.0 {
            row.insert(k.clone(), v.clone());
        }
        Ok(row.clone())
    }

    /// The committed store. `begin` snapshots its tables into a transaction.
    #[derive(Clone, Default)]
    pub struct TxStore {
        tables: Arc<Mutex<Tables>>,
    }

    impl TxStore {
        pub fn new() -> Self {
            Self::default()
        }
        /// How many rows are committed for `resource` (test assertion helper).
        pub fn committed_count(&self, resource: &str) -> usize {
            self.tables
                .lock()
                .unwrap()
                .get(resource)
                .map_or(0, HashMap::len)
        }
    }

    // The bare (non-transactional) DataLayer view — used when the domain reads,
    // and the fallback when no txn is open. Writes go straight to committed state.
    #[async_trait]
    impl DataLayer for TxStore {
        async fn create(&self, resource: &str, pk: &str, rec: Record) -> Result<Record> {
            create_in(&mut self.tables.lock().unwrap(), resource, pk, rec)
        }
        async fn read(&self, query: &Query) -> Result<Vec<Record>> {
            let t = self.tables.lock().unwrap();
            Ok(t.get(&query.resource)
                .map(|tbl| tbl.values().cloned().collect())
                .unwrap_or_default())
        }
        async fn get(&self, resource: &str, _pk: &str, id: &Value) -> Result<Option<Record>> {
            Ok(get_in(&self.tables.lock().unwrap(), resource, id))
        }
        async fn update(
            &self,
            resource: &str,
            _pk: &str,
            id: &Value,
            changes: &Record,
        ) -> Result<Record> {
            update_in(&mut self.tables.lock().unwrap(), resource, id, changes)
        }
        async fn destroy(&self, resource: &str, _pk: &str, id: &Value) -> Result<()> {
            if let Some(tbl) = self.tables.lock().unwrap().get_mut(resource) {
                tbl.remove(&value_key(id));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Store for TxStore {
        fn layer(&self) -> &dyn DataLayer {
            self
        }
        async fn begin(&self) -> Result<Option<Box<dyn Transaction>>> {
            // Snapshot committed state; the txn mutates the snapshot only.
            let snapshot = self.tables.lock().unwrap().clone();
            Ok(Some(Box::new(TxHandle {
                shared: self.tables.clone(),
                staged: Mutex::new(snapshot),
            })))
        }
    }

    /// An open unit of work over a [`TxStore`].
    pub struct TxHandle {
        shared: Arc<Mutex<Tables>>,
        staged: Mutex<Tables>,
    }

    #[async_trait]
    impl DataLayer for TxHandle {
        async fn create(&self, resource: &str, pk: &str, rec: Record) -> Result<Record> {
            create_in(&mut self.staged.lock().unwrap(), resource, pk, rec)
        }
        async fn read(&self, query: &Query) -> Result<Vec<Record>> {
            let t = self.staged.lock().unwrap();
            Ok(t.get(&query.resource)
                .map(|tbl| tbl.values().cloned().collect())
                .unwrap_or_default())
        }
        async fn get(&self, resource: &str, _pk: &str, id: &Value) -> Result<Option<Record>> {
            Ok(get_in(&self.staged.lock().unwrap(), resource, id))
        }
        async fn update(
            &self,
            resource: &str,
            _pk: &str,
            id: &Value,
            changes: &Record,
        ) -> Result<Record> {
            update_in(&mut self.staged.lock().unwrap(), resource, id, changes)
        }
        async fn destroy(&self, resource: &str, _pk: &str, id: &Value) -> Result<()> {
            if let Some(tbl) = self.staged.lock().unwrap().get_mut(resource) {
                tbl.remove(&value_key(id));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Transaction for TxHandle {
        async fn commit(self: Box<Self>) -> Result<()> {
            // Publish the staged snapshot as the new committed state.
            *self.shared.lock().unwrap() = self.staged.into_inner().unwrap();
            Ok(())
        }
        async fn rollback(self: Box<Self>) -> Result<()> {
            // Drop the staged snapshot — committed state is untouched.
            Ok(())
        }
    }
}

use txn_support::TxStore;

/// An extension whose `after_action` always fails — to prove rollback.
struct FailAfter;
#[async_trait::async_trait]
impl Extension for FailAfter {
    fn name(&self) -> &str {
        "fail-after"
    }
    async fn after_action(
        &self,
        _cs: &Changeset,
        _result: &mut crate::action::ActionResult,
    ) -> Result<()> {
        Err(Error::invalid("after_action boom"))
    }
}

fn txn_domain(extensions: Vec<Arc<dyn Extension>>) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            extensions,
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn transactional_after_action_failure_rolls_back() {
    // The headline guarantee: an after_action failure undoes the committed row.
    let store = TxStore::new();
    let d = txn_domain(vec![Arc::new(FailAfter)]);
    let mut ctx = Context::new(store.clone());

    let err = d
        .create::<Doc>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(matches!(err, Err(Error::Invalid { .. })), "got {err:?}");
    // Nothing committed — the write was rolled back.
    assert_eq!(store.committed_count("doc"), 0, "row must be rolled back");
}

#[tokio::test]
async fn transactional_success_commits() {
    // With no failing extension, the write commits through the transaction.
    let store = TxStore::new();
    let d = txn_domain(vec![]);
    let mut ctx = Context::new(store.clone());

    d.create::<Doc>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await
        .unwrap();
    assert_eq!(store.committed_count("doc"), 1, "row must be committed");
}

#[tokio::test]
async fn transactional_event_failure_still_commits() {
    // The narrowed contract: events fire *after* commit, so a handler failure
    // returns Err to the caller but the row IS committed — even with the txn
    // seam (rolling back a durable write for a webhook failure would be worse).
    let store = TxStore::new();
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            event_handlers: vec![Arc::new(FailingHandler)],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(store.clone());

    let created = d
        .create::<Doc>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(
        matches!(created, Err(Error::Invalid { .. })),
        "handler error surfaces, got {created:?}"
    );
    // Committed despite the post-commit handler error — the transaction had
    // already committed before events ran.
    assert_eq!(
        store.committed_count("doc"),
        1,
        "row commits before events run"
    );
}

#[tokio::test]
async fn transactional_denial_opens_no_transaction() {
    // A denied write never reaches begin(): fail-closed, nothing staged.
    let store = TxStore::new();
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            // Deny the create action outright.
            policies: PolicySet::new(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(store.clone());

    let err = d
        .create::<Doc>(&mut ctx, "create", Record::from_iter([("title", "x")]))
        .await;
    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
    assert_eq!(store.committed_count("doc"), 0);
}

#[tokio::test]
async fn non_transactional_store_is_unaffected() {
    // The default in-memory store declines begin() and keeps the old path:
    // an after_action failure returns Err on a row that IS committed.
    // (Widget has a plain, unfiltered read — unlike Doc's done-only scope.)
    let layer = Arc::new(InMemoryDataLayer::new());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Widget>()],
            policies: PolicySet::permissive(),
            extensions: vec![Arc::new(FailAfter)],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(layer.clone());

    let err = d
        .create::<Widget>(&mut ctx, "create", Record::from_iter([("name", "w")]))
        .await;
    assert!(matches!(err, Err(Error::Invalid { .. })), "got {err:?}");
    // No transaction seam → the row stands despite the after_action failure.
    let rows = d
        .read_as::<Widget>(&ctx, "read", Query::new("widget"))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "non-transactional path leaves the committed row"
    );
}

// ── primary-key stamping: absent / Null / "" all count as "not supplied" ──

/// A minimal non-tenant resource for the pk-stamping and `bind` tests, with a
/// plain `read` (no scope predicate) so a get-by-id returns the row.
struct Memo;
impl Resource for Memo {
    const NAME: &'static str = "memo";
    type Data = Record;
    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("body"),
        ]
    }
    fn actions() -> Vec<ActionDef> {
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
}

fn memo_domain() -> Domain {
    Domain::builder().register::<Memo>().permissive().build()
}

#[tokio::test]
async fn empty_string_primary_key_is_stamped_like_an_absent_one() {
    let d = memo_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // An *empty-string* id is what a `Default`-constructed derived struct with
    // a `String` primary key sends. It must be treated as "not supplied" and
    // replaced with a generated id — not persisted as a row keyed on "".
    let created = d
        .create::<Memo>(
            &mut ctx,
            "create",
            Record::from_iter([("id", ""), ("body", "hi")]),
        )
        .await
        .unwrap();
    let id = created.get("id").and_then(Value::as_str).unwrap();
    assert!(
        !id.is_empty(),
        "empty-string pk should have been stamped, got {id:?}"
    );
}

#[tokio::test]
async fn a_supplied_primary_key_is_kept() {
    let d = memo_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // A non-empty id the caller supplies is honoured verbatim.
    let created = d
        .create::<Memo>(
            &mut ctx,
            "create",
            Record::from_iter([("id", "chosen"), ("body", "hi")]),
        )
        .await
        .unwrap();
    assert_eq!(created.get("id").and_then(Value::as_str), Some("chosen"));
}

// ── the `bind` handle: same pipeline, plumbing threaded once ──────────────

#[tokio::test]
async fn bound_handle_round_trips_a_record() {
    let d = memo_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let mut memos = d.bind(&mut ctx);

    let created = memos
        .create::<Memo>(Record::from_iter([("body", "hi")]))
        .await
        .unwrap();
    let id = created.get("id").cloned().unwrap();

    let got = memos.get::<Memo>(id).await.unwrap().expect("row present");
    assert_eq!(got.get("body").and_then(Value::as_str), Some("hi"));
}

#[tokio::test]
async fn bound_get_fails_closed_under_default_deny() {
    // Seed under a permissive domain…
    let permissive = memo_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let id = permissive
        .bind(&mut ctx)
        .create::<Memo>(Record::from_iter([("body", "hi")]))
        .await
        .unwrap()
        .get("id")
        .cloned()
        .unwrap();

    // …then read through a default-deny domain over the same store: Forbidden,
    // never the row. The bound handle rides the same authorized read path.
    let denied = Domain::builder().register::<Memo>().build(); // no permissive()
    let err = denied.bind(&mut ctx).get::<Memo>(id).await.unwrap_err();
    assert!(
        matches!(err, Error::Forbidden(_)),
        "expected Forbidden, got {err:?}"
    );
}

// ── the row bound: every read is bounded, and never silently truncated ───

/// A layer that records the `limit` of every read it is handed, so a test can
/// assert what the executor actually pushed down.
struct LimitSpyLayer {
    inner: InMemoryDataLayer,
    seen: std::sync::Mutex<Vec<Option<u32>>>,
}
impl LimitSpyLayer {
    fn new() -> Self {
        Self {
            inner: InMemoryDataLayer::new(),
            seen: std::sync::Mutex::new(Vec::new()),
        }
    }
    fn limits(&self) -> Vec<Option<u32>> {
        self.seen.lock().expect("spy mutex poisoned").clone()
    }
}
#[async_trait::async_trait]
impl crate::datalayer::DataLayer for LimitSpyLayer {
    async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
        self.inner.create(r, pk, rec).await
    }
    async fn read(&self, q: &Query) -> Result<Vec<Record>> {
        self.seen.lock().expect("spy mutex poisoned").push(q.limit);
        self.inner.read(q).await
    }
    async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.inner.get(r, pk, id).await
    }
    async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
        self.inner.update(r, pk, id, c).await
    }
    async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
        self.inner.destroy(r, pk, id).await
    }
}

/// A layer that returns one row more than it was allowed — a broken layer,
/// which the domain must catch rather than trust.
struct OverrunLayer(InMemoryDataLayer);
#[async_trait::async_trait]
impl crate::datalayer::DataLayer for OverrunLayer {
    async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
        self.0.create(r, pk, rec).await
    }
    async fn read(&self, q: &Query) -> Result<Vec<Record>> {
        // Ignore the bound entirely: hand back every stored row.
        self.0.read(&Query::new(&q.resource)).await
    }
    async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.0.get(r, pk, id).await
    }
    async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
        self.0.update(r, pk, id, c).await
    }
    async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
        self.0.destroy(r, pk, id).await
    }
}

fn bounded_domain(max_rows: Option<u32>) -> Domain {
    let mut config = DomainConfig {
        resources: vec![
            erase::<Note>(),
            erase::<Author>(),
            erase::<Post>(),
            erase::<Tag>(),
            erase::<PostTag>(),
        ],
        policies: PolicySet::permissive(),
        ..DomainConfig::default()
    };
    config.max_rows = max_rows.map(|n| std::num::NonZeroU32::new(n).expect("non-zero in tests"));
    Domain::new(config, DomainContext::new())
}

async fn seed_authors(
    d: &Domain,
    ctx: &mut Context<Arc<impl crate::datalayer::DataLayer + 'static>>,
    n: usize,
) {
    for i in 0..n {
        d.create::<Author>(
            ctx,
            "create",
            Record::from_iter([
                ("id", Value::from(format!("a{i}"))),
                ("name", Value::from(format!("author {i}"))),
            ]),
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn an_unbounded_read_carries_the_domain_ceiling_plus_one() {
    let d = bounded_domain(Some(50));
    let layer = Arc::new(LimitSpyLayer::new());
    let mut ctx = Context::new(layer.clone());
    seed_authors(&d, &mut ctx, 2).await;

    d.read_as::<Author>(&ctx, "read", Query::new("author"))
        .await
        .unwrap();

    // One row *more* than the ceiling: that extra row is how the domain can tell
    // "exactly at the ceiling" from "there was more" without truncating.
    assert_eq!(layer.limits(), vec![Some(51)]);
}

#[tokio::test]
async fn a_caller_limit_is_passed_down_untouched() {
    let d = bounded_domain(Some(50));
    let layer = Arc::new(LimitSpyLayer::new());
    let mut ctx = Context::new(layer.clone());
    seed_authors(&d, &mut ctx, 5).await;

    let rows = d
        .read_as::<Author>(&ctx, "read", Query::new("author").limit(2))
        .await
        .unwrap();

    assert_eq!(
        layer.limits(),
        vec![Some(2)],
        "the caller's bound is deliberate"
    );
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn a_read_past_the_ceiling_is_refused_not_truncated() {
    let d = bounded_domain(Some(2));
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed_authors(&d, &mut ctx, 3).await;

    let err = d
        .read_as::<Author>(&ctx, "read", Query::new("author"))
        .await;
    let Err(Error::Unsupported(message)) = err else {
        panic!("expected Unsupported, got {err:?}");
    };
    // The message has to be actionable — it names the ceiling and the ways out.
    assert!(message.contains("max_rows"), "{message}");
    assert!(message.contains("Query::limit"), "{message}");
}

#[tokio::test]
async fn an_unbounded_offset_read_is_still_held_to_the_ceiling() {
    // An offset bounds where a read *starts*, never how much it returns, so it
    // is no way past `max_rows`: the tail beyond the offset is measured against
    // the ceiling exactly as a bare read is.
    let d = bounded_domain(Some(2));
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed_authors(&d, &mut ctx, 5).await;

    let err = d
        .read_as::<Author>(&ctx, "read", Query::new("author").sort_asc("id").offset(1))
        .await;
    let Err(Error::Unsupported(message)) = err else {
        panic!("expected Unsupported, got {err:?}");
    };
    assert!(message.contains("max_rows"), "{message}");

    // Offset far enough that the remaining tail fits, and the read succeeds —
    // the ceiling measures what comes back, not the whole table.
    let rows = d
        .read_as::<Author>(&ctx, "read", Query::new("author").sort_asc("id").offset(3))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn a_read_at_the_ceiling_still_succeeds() {
    let d = bounded_domain(Some(3));
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed_authors(&d, &mut ctx, 3).await;

    let rows = d
        .read_as::<Author>(&ctx, "read", Query::new("author"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 3, "the ceiling is inclusive");
}

#[tokio::test]
async fn a_caller_limit_reads_past_the_ceiling() {
    let d = bounded_domain(Some(2));
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    seed_authors(&d, &mut ctx, 5).await;

    // The ceiling only fills in for a read with no bound of its own; a caller
    // who states one has already thought about it.
    let rows = d
        .read_as::<Author>(&ctx, "read", Query::new("author").limit(4))
        .await
        .unwrap();
    assert_eq!(rows.len(), 4);
}

#[tokio::test]
async fn the_unbounded_opt_out_reads_everything() {
    let d = bounded_domain(None);
    let layer = Arc::new(LimitSpyLayer::new());
    let mut ctx = Context::new(layer.clone());
    seed_authors(&d, &mut ctx, 5).await;

    let rows = d
        .read_as::<Author>(&ctx, "read", Query::new("author"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        layer.limits(),
        vec![None],
        "opted out means no bound is pushed down"
    );
}

#[tokio::test]
async fn a_layer_that_overruns_the_bound_fails_the_read() {
    let d = bounded_domain(Some(2));
    let layer = Arc::new(OverrunLayer(InMemoryDataLayer::new()));
    let mut ctx = Context::new(layer.clone());
    seed_authors(&d, &mut ctx, 3).await;

    // Caller-set bound: the layer ignores it and hands back 3 rows for a bound
    // of 1. The domain reports the broken contract instead of truncating.
    let err = d
        .read_as::<Author>(&ctx, "read", Query::new("author").limit(1))
        .await;
    let Err(Error::DataLayer { message, .. }) = err else {
        panic!("expected DataLayer, got {err:?}");
    };
    assert!(message.contains("bounded to 1"), "{message}");
}

#[tokio::test]
async fn a_relationship_load_is_bounded_too() {
    let d = bounded_domain(Some(2));
    let layer = Arc::new(LimitSpyLayer::new());
    let mut ctx = Context::new(layer.clone());
    d.create::<Author>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "a1"), ("name", "Ada")]),
    )
    .await
    .unwrap();
    for i in 0..3 {
        d.create::<Post>(
            &mut ctx,
            "create",
            Record::from_iter([
                ("id", Value::from(format!("p{i}"))),
                ("title", Value::from("t")),
                ("author_id", Value::from("a1")),
            ]),
        )
        .await
        .unwrap();
    }

    // The fan-out of a has_many load is exactly the unbounded resource use the
    // ceiling exists for: three children under a ceiling of two is refused.
    let err = d
        .read_loaded::<Author>(&ctx, "read", Query::new("author").load(["posts"]))
        .await;
    assert!(matches!(err, Err(Error::Unsupported(_))), "got {err:?}");
    assert!(
        layer.limits().iter().all(|l| *l == Some(3)),
        "every read, load included, carries the bound: {:?}",
        layer.limits()
    );
}

#[test]
fn the_default_ceiling_is_ten_thousand() {
    assert_eq!(DEFAULT_MAX_ROWS.get(), 10_000);
    assert_eq!(DomainConfig::default().max_rows, Some(DEFAULT_MAX_ROWS));
}

// ── batch create: per-row pipeline, batched persist ──────────────────────

/// A layer that implements the batch persist natively and counts how it was
/// reached, so a test can tell a real `create_many` from the default loop.
struct BatchSpyLayer {
    inner: InMemoryDataLayer,
    batches: std::sync::atomic::AtomicUsize,
    singles: std::sync::atomic::AtomicUsize,
}
impl BatchSpyLayer {
    fn new() -> Self {
        Self {
            inner: InMemoryDataLayer::new(),
            batches: std::sync::atomic::AtomicUsize::new(0),
            singles: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    fn counts(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.batches.load(Relaxed), self.singles.load(Relaxed))
    }
}
#[async_trait::async_trait]
impl crate::datalayer::DataLayer for BatchSpyLayer {
    async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
        self.singles
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.create(r, pk, rec).await
    }
    async fn create_many(&self, r: &str, pk: &str, rows: Vec<Record>) -> Result<Vec<Record>> {
        self.batches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(self.inner.create(r, pk, row).await?);
        }
        Ok(out)
    }
    async fn read(&self, q: &Query) -> Result<Vec<Record>> {
        self.inner.read(q).await
    }
    async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.inner.get(r, pk, id).await
    }
    async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
        self.inner.update(r, pk, id, c).await
    }
    async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
        self.inner.destroy(r, pk, id).await
    }
}

fn memo_rows(bodies: &[&str]) -> Vec<Record> {
    bodies
        .iter()
        .enumerate()
        .map(|(i, body)| {
            Record::from_iter([("id", format!("m{i}")), ("body", (*body).to_string())])
        })
        .collect()
}

async fn create_many<R: Resource>(
    d: &Domain,
    ctx: &mut Context<impl Store>,
    rows: Vec<Record>,
) -> Result<Vec<Record>> {
    d.handle_action::<R>(ctx, "create", ActionInput::create_many_records(rows))
        .await?
        .into_records()
}

#[tokio::test]
async fn batch_create_persists_every_row_in_order() {
    let d = memo_domain();
    let layer = Arc::new(BatchSpyLayer::new());
    let mut ctx = Context::new(layer.clone());

    let created = create_many::<Memo>(&d, &mut ctx, memo_rows(&["a", "b", "c"]))
        .await
        .unwrap();

    assert_eq!(created.len(), 3);
    let bodies: Vec<_> = created
        .iter()
        .filter_map(|r| r.get("body"))
        .cloned()
        .collect();
    assert_eq!(
        bodies,
        vec![Value::from("a"), Value::from("b"), Value::from("c")]
    );
    // One batch call, no per-row creates: the persist really was batched.
    assert_eq!(layer.counts(), (1, 0));
}

#[tokio::test]
async fn a_layer_without_a_batch_persist_still_works() {
    // The default `create_many` loops over `create`, so every existing layer
    // satisfies the batch path without changing a line.
    let d = memo_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let created = create_many::<Memo>(&d, &mut ctx, memo_rows(&["a", "b"]))
        .await
        .unwrap();
    assert_eq!(created.len(), 2);

    let rows = d
        .read_as::<Memo>(&ctx, "read", Query::new("memo"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn one_denied_row_persists_none_of_the_batch() {
    // Fail-closed, batch-wide: authorization runs for every row before any row
    // is persisted, so a single denial leaves the store untouched.
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Memo>()],
            policies: PolicySet::permissive().with(ScopedPolicy::resource(
                "memo",
                "no-secrets",
                Arc::new(NoSecrets),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(BatchSpyLayer::new());
    let mut ctx = Context::new(layer.clone());

    let err = create_many::<Memo>(&d, &mut ctx, memo_rows(&["fine", "secret", "fine"])).await;

    assert!(matches!(err, Err(Error::Forbidden(_))), "got {err:?}");
    assert_eq!(layer.counts(), (0, 0), "nothing reached the layer");
    let rows = d
        .read_as::<Memo>(&ctx, "read", Query::new("memo"))
        .await
        .unwrap();
    assert!(rows.is_empty(), "no row of a denied batch is persisted");
}

/// Forbids any memo whose body is "secret" — a per-row decision, so it can deny
/// one row of a batch.
struct NoSecrets;
#[async_trait::async_trait]
impl crate::policy::Policy for NoSecrets {
    async fn authorize(&self, cs: &Changeset) -> Decision {
        match cs.data.get("body").and_then(Value::as_str) {
            Some("secret") => Decision::Forbid("no secrets".into()),
            _ => Decision::NotApplicable,
        }
    }
}

#[tokio::test]
async fn a_batch_over_the_bound_is_refused_before_staging() {
    let d = Domain::builder()
        .register::<Memo>()
        .permissive()
        .max_batch(std::num::NonZeroU32::new(2).unwrap())
        .build();
    let layer = Arc::new(BatchSpyLayer::new());
    let mut ctx = Context::new(layer.clone());

    let err = create_many::<Memo>(&d, &mut ctx, memo_rows(&["a", "b", "c"])).await;

    let Err(Error::Invalid { message, .. }) = err else {
        panic!("expected Invalid, got {err:?}");
    };
    assert!(message.contains("max_batch"), "{message}");
    assert_eq!(
        layer.counts(),
        (0, 0),
        "the bound is checked before any work"
    );
}

#[tokio::test]
async fn an_empty_batch_is_a_no_op() {
    let d = memo_domain();
    let layer = Arc::new(BatchSpyLayer::new());
    let mut ctx = Context::new(layer.clone());

    let created = create_many::<Memo>(&d, &mut ctx, Vec::new()).await.unwrap();

    assert!(created.is_empty());
    assert_eq!(layer.counts(), (0, 0), "an empty batch opens nothing");
}

#[tokio::test]
async fn a_batch_emits_one_event_per_row() {
    let events = Arc::new(RecordingHandler::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Memo>()],
            policies: PolicySet::permissive(),
            event_handlers: vec![events.clone()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    create_many::<Memo>(&d, &mut ctx, memo_rows(&["a", "b", "c"]))
        .await
        .unwrap();

    let seen = events.seen();
    assert_eq!(
        seen.len(),
        3,
        "one event per committed row, not one per batch"
    );
    assert!(
        seen.iter()
            .all(|e| e.resource == "memo" && e.action == "create")
    );
}

/// Collects the domain events it is handed.
#[derive(Default)]
struct RecordingHandler {
    events: std::sync::Mutex<Vec<DomainEvent>>,
}
impl RecordingHandler {
    fn seen(&self) -> Vec<DomainEvent> {
        self.events.lock().expect("handler mutex poisoned").clone()
    }
}
#[async_trait::async_trait]
impl EventHandler for RecordingHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        self.events
            .lock()
            .expect("handler mutex poisoned")
            .push(event.clone());
        Ok(())
    }
}

#[tokio::test]
async fn a_batch_rolls_back_as_one_transaction() {
    // The whole batch commits or none of it does: an after_action failure on
    // any row undoes every row.
    let store = TxStore::new();
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            extensions: vec![Arc::new(FailAfter)],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(store.clone());

    let rows = vec![
        Record::from_iter([("id", "d1"), ("title", "one")]),
        Record::from_iter([("id", "d2"), ("title", "two")]),
    ];
    let err = create_many::<Doc>(&d, &mut ctx, rows).await;

    assert!(err.is_err());
    assert_eq!(
        store.committed_count("doc"),
        0,
        "no row of the batch survives"
    );
}

// ── the transactional outbox: events staged inside the write's transaction ──

/// An outbox writer: on `stage` it inserts the event as a row of the `outbox`
/// resource **through the transaction it is handed**, so the outbox row and the
/// write it announces commit together or not at all. Its post-commit `handle`
/// does nothing — publishing is the relay's job, not the domain's.
struct OutboxWriter {
    staged: std::sync::atomic::AtomicUsize,
    handled: std::sync::atomic::AtomicUsize,
    fail: bool,
}
impl OutboxWriter {
    fn new(fail: bool) -> Self {
        Self {
            staged: std::sync::atomic::AtomicUsize::new(0),
            handled: std::sync::atomic::AtomicUsize::new(0),
            fail,
        }
    }
    fn counts(&self) -> (usize, usize) {
        use std::sync::atomic::Ordering::Relaxed;
        (self.staged.load(Relaxed), self.handled.load(Relaxed))
    }
}
#[async_trait::async_trait]
impl EventHandler for OutboxWriter {
    async fn handle(&self, _event: &DomainEvent) -> Result<()> {
        self.handled
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
    async fn stage(
        &self,
        event: &DomainEvent,
        txn: &dyn crate::datalayer::DataLayer,
    ) -> Result<()> {
        let n = self
            .staged
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.fail {
            return Err(Error::invalid("outbox write failed"));
        }
        let row = Record::from_iter([
            ("id", Value::from(format!("evt{n}"))),
            ("resource", Value::from(event.resource.as_str())),
            ("action", Value::from(event.action.as_str())),
        ]);
        txn.create("outbox", "id", row).await?;
        Ok(())
    }
    fn stages(&self) -> bool {
        true
    }
}

fn outbox_domain(handler: Arc<OutboxWriter>) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            event_handlers: vec![handler],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

#[tokio::test]
async fn a_staged_event_commits_with_the_row() {
    let store = TxStore::new();
    let outbox = Arc::new(OutboxWriter::new(false));
    let d = outbox_domain(outbox.clone());
    let mut ctx = Context::new(store.clone());

    d.create::<Doc>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "d1"), ("title", "one")]),
    )
    .await
    .unwrap();

    assert_eq!(
        outbox.counts(),
        (1, 1),
        "staged before the commit, handled after it"
    );
    assert_eq!(store.committed_count("doc"), 1);
    assert_eq!(
        store.committed_count("outbox"),
        1,
        "the outbox row committed too"
    );
}

#[tokio::test]
async fn a_failed_staging_rolls_the_write_back() {
    // Staging runs *before* the commit, so a handler that cannot record the
    // event undoes the action rather than leaving a row nobody announced.
    let store = TxStore::new();
    let outbox = Arc::new(OutboxWriter::new(true));
    let d = outbox_domain(outbox.clone());
    let mut ctx = Context::new(store.clone());

    let err = d
        .create::<Doc>(
            &mut ctx,
            "create",
            Record::from_iter([("id", "d1"), ("title", "one")]),
        )
        .await;

    assert!(err.is_err(), "a staging failure fails the write");
    assert_eq!(store.committed_count("doc"), 0, "the row is rolled back");
    assert_eq!(outbox.counts().1, 0, "post-commit delivery never runs");
}

#[tokio::test]
async fn a_batch_stages_one_event_per_row_in_one_transaction() {
    let store = TxStore::new();
    let outbox = Arc::new(OutboxWriter::new(false));
    let d = outbox_domain(outbox.clone());
    let mut ctx = Context::new(store.clone());

    let rows = vec![
        Record::from_iter([("id", "d1"), ("title", "one")]),
        Record::from_iter([("id", "d2"), ("title", "two")]),
    ];
    create_many::<Doc>(&d, &mut ctx, rows).await.unwrap();

    assert_eq!(outbox.counts(), (2, 2));
    assert_eq!(store.committed_count("outbox"), 2);
    assert_eq!(store.committed_count("doc"), 2);
}

#[tokio::test]
async fn staging_without_a_transaction_is_refused_not_skipped() {
    // The in-memory store offers no transaction. Rather than quietly giving the
    // handler weaker guarantees than it declared, the write is refused.
    let outbox = Arc::new(OutboxWriter::new(false));
    let d = outbox_domain(outbox.clone());
    let layer = Arc::new(InMemoryDataLayer::new());
    let mut ctx = Context::new(layer.clone());

    let err = d
        .create::<Doc>(
            &mut ctx,
            "create",
            Record::from_iter([("id", "d1"), ("title", "one")]),
        )
        .await;

    let Err(Error::Unsupported(message)) = err else {
        panic!("expected Unsupported, got {err:?}");
    };
    assert!(message.contains("Store::begin"), "{message}");
    assert_eq!(outbox.counts(), (0, 0));
    // Refused before the persist, so nothing was written either.
    let rows = d
        .read_as::<Doc>(&ctx, "read", Query::new("doc"))
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn a_handler_that_does_not_stage_is_unaffected() {
    // The seam is opt-in: an ordinary handler keeps the post-commit contract and
    // runs happily against a non-transactional store.
    let events = Arc::new(RecordingHandler::default());
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::permissive(),
            event_handlers: vec![events.clone()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    d.create::<Doc>(
        &mut ctx,
        "create",
        Record::from_iter([("id", "d1"), ("title", "one")]),
    )
    .await
    .unwrap();

    assert_eq!(events.seen().len(), 1);
}

#[tokio::test]
async fn a_transactional_store_passes_the_conformance_check() {
    // The transaction checks are only meaningful against a store that offers
    // one; run them against the test store the seam's own tests use, so the kit
    // is known to be satisfiable and not just satisfiable-by-declining.
    use crate::datalayer::conformance::Conformance;

    let store = TxStore::new();
    let report = Conformance::new("scratch", "id").check_store(&store).await;

    assert!(report.is_conformant(), "{report}");
    assert_eq!(
        report.passed(),
        [
            "rollback-undoes-the-write",
            "commit-makes-the-write-durable"
        ]
    );
}

#[tokio::test]
async fn the_in_memory_layer_passes_the_full_conformance_check() {
    use crate::datalayer::conformance::Conformance;

    let layer = InMemoryDataLayer::new();
    let report = Conformance::new("scratch", "id").check(&layer).await;

    assert!(report.is_conformant(), "{report}");
    // Every contract point the kit knows about, including the two added with the
    // row bound and the batch persist.
    assert!(report.passed().contains(&"honours-row-limit"), "{report}");
    assert!(
        report.passed().contains(&"batch-create-round-trip"),
        "{report}"
    );
    assert!(report.passed().contains(&"absent-get-is-none"), "{report}");
}

// ── sort keys and cursor pagination ──────────────────────────────────────

/// Seed `n` docs with ids `doc-00`, `doc-01`, … so lexicographic order on `id`
/// is also their numeric order — a stable, total order to page through.
async fn seeded_docs(n: usize) -> (Domain, Context<Arc<InMemoryDataLayer>>) {
    let d = doc_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    for i in 0..n {
        layer
            .create(
                "doc",
                "id",
                Record::from_iter([
                    ("id", Value::from(format!("doc-{i:02}"))),
                    ("title", Value::from(format!("t{i}"))),
                    // `Doc::read` scopes to done == true; these rows are the
                    // population under test, not a test of that filter.
                    ("done", Value::Bool(true)),
                ]),
            )
            .await
            .unwrap();
    }
    (d, Context::new(layer))
}

fn ids(rows: &[Record]) -> Vec<String> {
    rows.iter()
        .map(|r| {
            r.get("id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn a_sorted_read_comes_back_in_order() {
    let (d, ctx) = seeded_docs(5).await;
    let asc: Vec<Record> = d.read::<Doc>(&ctx).sort_asc("id").await.unwrap();
    assert_eq!(
        ids(&asc),
        ["doc-00", "doc-01", "doc-02", "doc-03", "doc-04"]
    );

    let desc: Vec<Record> = d.read::<Doc>(&ctx).sort_desc("id").await.unwrap();
    assert_eq!(
        ids(&desc),
        ["doc-04", "doc-03", "doc-02", "doc-01", "doc-00"]
    );
}

#[tokio::test]
async fn paging_a_sorted_read_visits_every_row_exactly_once() {
    // The property that matters: walking the pages reconstructs the full set,
    // in order, with nothing skipped and nothing repeated.
    let (d, ctx) = seeded_docs(10).await;
    let mut seen: Vec<String> = Vec::new();
    let mut cursor = None;
    // Bounded: 10 rows at 3 per page is 4 pages; cap well above that so a
    // non-terminating cursor fails the test instead of hanging it.
    for _ in 0..10 {
        let mut req = d.read::<Doc>(&ctx).sort_asc("id").limit(3);
        if let Some(c) = cursor.take() {
            req = req.after(c);
        }
        let page = req.page().await.unwrap();
        seen.extend(ids(page.rows()));
        match page.cursor() {
            Some(c) => cursor = Some(c.clone()),
            None => break,
        }
    }
    let expected: Vec<String> = (0..10).map(|i| format!("doc-{i:02}")).collect();
    assert_eq!(seen, expected, "paging must not skip or repeat a row");
}

#[tokio::test]
async fn a_short_page_reports_no_cursor() {
    // Fewer rows than the limit means the set is exhausted: no cursor, so the
    // caller does not pay a round trip to discover the end.
    let (d, ctx) = seeded_docs(2).await;
    let page = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .limit(10)
        .page()
        .await
        .unwrap();
    assert_eq!(page.rows().len(), 2);
    assert!(page.cursor().is_none());
    assert!(!page.has_more());
}

#[tokio::test]
async fn a_full_final_page_yields_a_cursor_that_returns_nothing() {
    // A page that exactly fills the limit cannot know it is last, so it does
    // hand back a cursor — which must then resolve to an empty page.
    let (d, ctx) = seeded_docs(4).await;
    let page = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .limit(4)
        .page()
        .await
        .unwrap();
    assert_eq!(page.rows().len(), 4);
    let cursor = page.cursor().cloned().expect("a full page yields a cursor");
    let next = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .after(cursor)
        .limit(4)
        .page()
        .await
        .unwrap();
    assert!(next.rows().is_empty());
    assert!(next.cursor().is_none());
}

#[tokio::test]
async fn paging_a_descending_sort_walks_backwards() {
    let (d, ctx) = seeded_docs(5).await;
    let first = d
        .read::<Doc>(&ctx)
        .sort_desc("id")
        .limit(2)
        .page()
        .await
        .unwrap();
    assert_eq!(ids(first.rows()), ["doc-04", "doc-03"]);
    let next = d
        .read::<Doc>(&ctx)
        .sort_desc("id")
        .after(first.cursor().cloned().unwrap())
        .limit(2)
        .page()
        .await
        .unwrap();
    assert_eq!(ids(next.rows()), ["doc-02", "doc-01"]);
}

#[tokio::test]
async fn a_compound_sort_breaks_ties_by_the_next_key() {
    let d = doc_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    // Repeated `title` for pairs, distinct ids: title then id is a total order.
    // (`done` is true throughout — `Doc::read` scopes to it.)
    for (id, title) in [("a", "x"), ("b", "y"), ("c", "x"), ("e", "y")] {
        layer
            .create(
                "doc",
                "id",
                Record::from_iter([
                    ("id", Value::from(id)),
                    ("title", Value::from(title)),
                    ("done", Value::Bool(true)),
                ]),
            )
            .await
            .unwrap();
    }
    let ctx = Context::new(layer);
    let rows: Vec<Record> = d
        .read::<Doc>(&ctx)
        .sort_asc("title")
        .sort_desc("id")
        .await
        .unwrap();
    // "x" group first (desc by id within it), then the "y" group.
    assert_eq!(ids(&rows), ["c", "a", "e", "b"]);
}

#[tokio::test]
async fn sorting_by_an_undeclared_attribute_is_rejected() {
    let (d, ctx) = seeded_docs(1).await;
    let err = d.read::<Doc>(&ctx).sort_asc("nope").await.unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("no such attribute"), "{err}");
}

#[tokio::test]
async fn a_cursor_without_a_sort_order_is_rejected() {
    // There is no order to resume from — fail loudly rather than return an
    // arbitrary subset that looks like a page.
    let (d, ctx) = seeded_docs(3).await;
    let err = d
        .read::<Doc>(&ctx)
        .after(crate::query::Cursor::from_keys(vec![Value::from("doc-00")]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("no sort order"), "{err}");
}

#[tokio::test]
async fn a_cursor_whose_width_disagrees_with_the_sort_is_rejected() {
    // A two-key cursor against a one-key sort belongs to a different query;
    // resuming from it would silently skip or repeat rows.
    let (d, ctx) = seeded_docs(3).await;
    let err = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .after(crate::query::Cursor::from_keys(vec![
            Value::from("doc-00"),
            Value::from("extra"),
        ]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("differently-sorted"), "{err}");
}

#[tokio::test]
async fn an_unsorted_page_has_no_cursor() {
    // `page()` without a sort still returns rows, but cannot describe a
    // position, so it reports no cursor rather than inventing one.
    let (d, ctx) = seeded_docs(5).await;
    let page = d.read::<Doc>(&ctx).limit(2).page().await.unwrap();
    assert_eq!(page.rows().len(), 2);
    assert!(page.cursor().is_none());
}

// ── offset pagination ────────────────────────────────────────────────────

#[tokio::test]
async fn an_offset_skips_that_many_rows_of_the_sort_order() {
    let (d, ctx) = seeded_docs(5).await;
    let rows: Vec<Record> = d.read::<Doc>(&ctx).sort_asc("id").offset(2).await.unwrap();
    assert_eq!(ids(&rows), ["doc-02", "doc-03", "doc-04"]);
}

#[tokio::test]
async fn an_offset_is_applied_before_the_row_bound() {
    // offset 2, limit 2 is the third and fourth rows — not the first two of a
    // truncated set. Getting this backwards would silently return page one for
    // every page.
    let (d, ctx) = seeded_docs(5).await;
    let rows: Vec<Record> = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(2)
        .limit(2)
        .await
        .unwrap();
    assert_eq!(ids(&rows), ["doc-02", "doc-03"]);
}

#[tokio::test]
async fn offset_paging_visits_every_row_exactly_once() {
    // The same property the cursor walk guarantees, reached by arithmetic:
    // over an unchanging set, walking offsets reconstructs the full order.
    let (d, ctx) = seeded_docs(10).await;
    let mut seen: Vec<String> = Vec::new();
    let mut offset = 0u32;
    // 10 rows at 3 per page is 4 pages; cap above that so a page that never
    // reports the end fails the test instead of hanging it.
    for _ in 0..10 {
        let page = d
            .read::<Doc>(&ctx)
            .sort_asc("id")
            .offset(offset)
            .limit(3)
            .offset_page()
            .await
            .unwrap();
        assert!(page.rows().len() <= 3, "a page never exceeds its size");
        seen.extend(ids(page.rows()));
        match page.next_offset() {
            Some(next) => offset = next,
            None => break,
        }
    }
    let expected: Vec<String> = (0..10).map(|i| format!("doc-{i:02}")).collect();
    assert_eq!(
        seen, expected,
        "offset paging must not skip or repeat a row"
    );
}

#[tokio::test]
async fn an_offset_page_reports_more_without_returning_the_probe_row() {
    // `has_more` is answered by reading one row past the page; that row must
    // never reach the caller, who asked for a page of 2.
    let (d, ctx) = seeded_docs(5).await;
    let page = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(0)
        .limit(2)
        .offset_page()
        .await
        .unwrap();
    assert_eq!(ids(page.rows()), ["doc-00", "doc-01"]);
    assert!(page.has_more());
    assert_eq!(page.next_offset(), Some(2));
    assert_eq!(page.offset(), 0);
}

#[tokio::test]
async fn a_final_offset_page_reports_no_more() {
    // Exactly-full last page: the probe finds nothing beyond it, so the caller
    // learns the set is exhausted without a further round trip.
    let (d, ctx) = seeded_docs(4).await;
    let page = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(2)
        .limit(2)
        .offset_page()
        .await
        .unwrap();
    assert_eq!(ids(page.rows()), ["doc-02", "doc-03"]);
    assert!(!page.has_more());
    assert!(page.next_offset().is_none());
}

#[tokio::test]
async fn an_offset_past_the_end_is_an_empty_page() {
    let (d, ctx) = seeded_docs(3).await;
    let page = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(10)
        .limit(5)
        .offset_page()
        .await
        .unwrap();
    assert!(page.rows().is_empty());
    assert!(!page.has_more());
}

#[tokio::test]
async fn an_offset_without_a_sort_order_is_rejected() {
    // Skipping rows of an unordered read skips arbitrary rows — the same
    // reasoning that refuses a cursor without a sort.
    let (d, ctx) = seeded_docs(3).await;
    let err = d.read::<Doc>(&ctx).offset(1).await.unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("no sort order"), "{err}");
}

#[tokio::test]
async fn an_offset_and_a_cursor_together_are_rejected() {
    // Two different positions in one read: no layer can honour both, so this
    // is refused rather than silently resolved one way.
    let (d, ctx) = seeded_docs(3).await;
    let err = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(1)
        .after(crate::query::Cursor::from_keys(vec![Value::from("doc-00")]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(
        err.to_string()
            .contains("both a resume cursor and an offset"),
        "{err}"
    );
}

#[tokio::test]
async fn an_offset_page_without_a_page_size_is_rejected() {
    // An offset with no bound is the whole tail of the set, not a page — and
    // `has_more` would have nothing to compare against.
    let (d, ctx) = seeded_docs(3).await;
    let err = d
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(1)
        .offset_page()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("needs a page size"), "{err}");
}

#[tokio::test]
async fn an_offset_page_is_authorized_and_redacted_like_any_read() {
    // Offset paging is a shape of read, not a bypass: it runs the same
    // pipeline, so a denied caller sees Forbidden and never a row.
    let (_, ctx) = seeded_docs(5).await;
    // A default (empty) policy set denies: authorization runs first, so the
    // layer is never reached.
    let denying = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let err = denying
        .read::<Doc>(&ctx)
        .sort_asc("id")
        .offset(0)
        .limit(2)
        .offset_page()
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");
}

// ── optimistic concurrency ───────────────────────────────────────────────

/// A resource that opts into lost-update protection by naming its version
/// attribute — the seam `Resource::version_attribute` exposes.
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
        vec![
            ActionDef::write("create"),
            ActionDef::write("update"),
            ActionDef::read("read"),
        ]
    }
    fn version_attribute() -> Option<String> {
        Some("version".into())
    }
}

fn acct_domain() -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Acct>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
}

async fn seeded_acct() -> (Domain, Context<Arc<InMemoryDataLayer>>) {
    let d = acct_domain();
    let layer = Arc::new(InMemoryDataLayer::new());
    layer
        .create(
            "acct",
            "id",
            Record::from_iter([
                ("id", Value::from("a1")),
                ("balance", Value::Int(100)),
                ("version", Value::Int(1)),
            ]),
        )
        .await
        .unwrap();
    (d, Context::new(layer))
}

#[tokio::test]
async fn a_versioned_update_bumps_the_version() {
    let (d, mut ctx) = seeded_acct().await;
    let row = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(150)), ("version", Value::Int(1))]),
        )
        .await
        .unwrap();
    assert_eq!(row.get("balance"), Some(&Value::Int(150)));
    // The domain stamps the next version; the caller never supplies it.
    assert_eq!(row.get("version"), Some(&Value::Int(2)));
}

#[tokio::test]
async fn a_stale_update_is_refused_instead_of_clobbering() {
    // The whole point: two writers read version 1, both write. The second must
    // fail rather than silently overwrite the first's change.
    let (d, mut ctx) = seeded_acct().await;

    // Writer A commits, moving the row to version 2.
    d.update::<Acct>(
        &mut ctx,
        "update",
        Value::from("a1"),
        Record::from_iter([("balance", Value::Int(150)), ("version", Value::Int(1))]),
    )
    .await
    .unwrap();

    // Writer B still holds version 1 — the update it never saw.
    let err = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(999)), ("version", Value::Int(1))]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict { .. }), "{err:?}");

    // A's write stands; B's was refused, not applied.
    let rows: Vec<Record> = d.read::<Acct>(&ctx).await.unwrap();
    assert_eq!(rows[0].get("balance"), Some(&Value::Int(150)));
    assert_eq!(rows[0].get("version"), Some(&Value::Int(2)));
}

#[tokio::test]
async fn a_versioned_update_without_a_version_is_refused() {
    // An update that does not say what it read cannot be checked, and an
    // unchecked write is the very thing this resource opted out of.
    let (d, mut ctx) = seeded_acct().await;
    let err = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(150))]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("optimistic concurrency"), "{err}");
}

#[tokio::test]
async fn a_non_integer_version_is_refused() {
    let (d, mut ctx) = seeded_acct().await;
    let err = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(1)), ("version", Value::from("one"))]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
}

#[tokio::test]
async fn the_caller_cannot_forge_the_next_version() {
    // The version the caller sends is the one it *read*; the one written is the
    // domain's to stamp. A caller trying to set it directly must not win.
    let (d, mut ctx) = seeded_acct().await;
    let row = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(5)), ("version", Value::Int(1))]),
        )
        .await
        .unwrap();
    // Bumped to 2 — not to anything the caller chose.
    assert_eq!(row.get("version"), Some(&Value::Int(2)));
}

#[tokio::test]
async fn an_unversioned_resource_updates_unconditionally() {
    // The default path is untouched: a resource that declares no version
    // attribute keeps last-writer-wins, with no version param required.
    //
    // `Plain` is `Acct` minus the `version_attribute` override, so this isolates
    // exactly that one difference.
    struct Plain;
    impl Resource for Plain {
        const NAME: &'static str = "plain";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<i64>("balance"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::write("update"), ActionDef::read("read")]
        }
    }

    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Plain>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    layer
        .create(
            "plain",
            "id",
            Record::from_iter([("id", Value::from("p1")), ("balance", Value::Int(1))]),
        )
        .await
        .unwrap();
    let mut ctx = Context::new(layer);
    // No version param supplied, and the update simply lands.
    let row = d
        .update::<Plain>(
            &mut ctx,
            "update",
            Value::from("p1"),
            Record::from_iter([("balance", Value::Int(2))]),
        )
        .await
        .unwrap();
    assert_eq!(row.get("balance"), Some(&Value::Int(2)));
}

#[tokio::test]
async fn a_layer_that_cannot_do_conditional_writes_declines_loudly() {
    // The default `update_versioned` refuses rather than silently degrading to
    // an unconditional update — the failure mode this feature exists to avoid.
    struct NoConditionalWrites(InMemoryDataLayer);
    #[async_trait::async_trait]
    impl DataLayer for NoConditionalWrites {
        async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
            self.0.create(r, pk, rec).await
        }
        async fn read(&self, q: &Query) -> Result<Vec<Record>> {
            self.0.read(q).await
        }
        async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
            self.0.get(r, pk, id).await
        }
        async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
            self.0.update(r, pk, id, c).await
        }
        async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
            self.0.destroy(r, pk, id).await
        }
        // update_versioned deliberately left at its default.
    }

    let d = acct_domain();
    let layer = Arc::new(NoConditionalWrites(InMemoryDataLayer::new()));
    layer
        .create(
            "acct",
            "id",
            Record::from_iter([
                ("id", Value::from("a1")),
                ("balance", Value::Int(1)),
                ("version", Value::Int(1)),
            ]),
        )
        .await
        .unwrap();
    let mut ctx = Context::new(layer);
    let err = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("a1"),
            Record::from_iter([("balance", Value::Int(2)), ("version", Value::Int(1))]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err:?}");
}

#[test]
fn a_version_attribute_that_is_not_declared_is_rejected() {
    struct Bad;
    impl Resource for Bad {
        const NAME: &'static str = "bad";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![Attribute::scalar::<String>("id")]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
        fn version_attribute() -> Option<String> {
            Some("nope".into())
        }
    }
    let err = Domain::try_new(
        DomainConfig {
            resources: vec![erase::<Bad>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
    .err()
    .expect("a bad version attribute must fail construction");
    assert!(
        err.to_string().contains("not a declared attribute"),
        "{err}"
    );
}

#[test]
fn a_non_integer_version_attribute_is_rejected_at_construction() {
    struct Bad;
    impl Resource for Bad {
        const NAME: &'static str = "bad";
        type Data = Record;
        fn attributes() -> Vec<Attribute> {
            vec![
                Attribute::scalar::<String>("id"),
                Attribute::scalar::<String>("version"),
            ]
        }
        fn actions() -> Vec<ActionDef> {
            vec![ActionDef::read("read")]
        }
        fn version_attribute() -> Option<String> {
            Some("version".into())
        }
    }
    let err = Domain::try_new(
        DomainConfig {
            resources: vec![erase::<Bad>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    )
    .err()
    .expect("a bad version attribute must fail construction");
    assert!(err.to_string().contains("must be an integer"), "{err}");
}

#[tokio::test]
async fn a_create_seeds_the_initial_version() {
    // A versioned resource must be updatable straight after creation: if create
    // left the version unset, the first update would fail on a missing version
    // and the row would be permanently unwritable.
    let d = acct_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let created = d
        .create::<Acct>(
            &mut ctx,
            "create",
            Record::from_iter([("id", Value::from("n1")), ("balance", Value::Int(10))]),
        )
        .await
        .unwrap();
    let seeded = created.get("version").cloned();
    assert_eq!(seeded, Some(Value::Int(1)), "create must seed the version");

    // And the row is immediately updatable at that version.
    let updated = d
        .update::<Acct>(
            &mut ctx,
            "update",
            Value::from("n1"),
            Record::from_iter([("balance", Value::Int(20)), ("version", Value::Int(1))]),
        )
        .await
        .unwrap();
    assert_eq!(updated.get("version"), Some(&Value::Int(2)));
}

#[tokio::test]
async fn a_batch_create_seeds_the_version_on_every_row() {
    // Batch creates run the same per-row staging, so every row must come out
    // updatable — not just the ones made through the single-row path.
    let d = acct_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let rows = create_many::<Acct>(
        &d,
        &mut ctx,
        vec![
            Record::from_iter([("id", Value::from("b1")), ("balance", Value::Int(1))]),
            Record::from_iter([("id", Value::from("b2")), ("balance", Value::Int(2))]),
        ],
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(row.get("version"), Some(&Value::Int(1)));
    }
}

// ── nested load depth bound ──────────────────────────────────────────────

/// Seed one author with one post, enough for any depth of load to resolve.
async fn blog_ctx() -> Context<Arc<InMemoryDataLayer>> {
    let layer = Arc::new(InMemoryDataLayer::new());
    layer
        .create(
            "author",
            "id",
            Record::from_iter([("id", Value::from("a1")), ("name", Value::from("ada"))]),
        )
        .await
        .unwrap();
    layer
        .create(
            "post",
            "id",
            Record::from_iter([
                ("id", Value::from("p1")),
                ("author_id", Value::from("a1")),
                ("title", Value::from("t")),
            ]),
        )
        .await
        .unwrap();
    Context::new(layer)
}

#[tokio::test]
async fn a_load_within_the_depth_ceiling_resolves() {
    // Three hops, under the default ceiling of four.
    let d = domain();
    let ctx = blog_ctx().await;
    let rows = d
        .read_loaded::<Author>(
            &ctx,
            "read",
            Query::new("author").load(["posts.author.posts"]),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_load_past_the_depth_ceiling_is_refused() {
    // Five hops against the default ceiling of four. The relationship graph
    // here is cyclic (author → posts → author → …), so without a bound this
    // walks as far as the caller cares to type.
    let d = domain();
    let ctx = blog_ctx().await;
    let err = d
        .read_loaded::<Author>(
            &ctx,
            "read",
            Query::new("author").load(["posts.author.posts.author.posts"]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert!(err.to_string().contains("max_load_depth"), "{err}");
}

#[tokio::test]
async fn the_depth_ceiling_is_configurable() {
    // A domain that allows only one hop refuses two, and permits one.
    let d = Domain::builder()
        .register::<Author>()
        .register::<Post>()
        .register::<Tag>()
        .register::<PostTag>()
        .permissive()
        .max_load_depth(std::num::NonZeroU32::new(1).unwrap())
        .build();
    let ctx = blog_ctx().await;

    let ok = d
        .read_loaded::<Author>(&ctx, "read", Query::new("author").load(["posts"]))
        .await
        .unwrap();
    assert_eq!(ok.len(), 1);

    let err = d
        .read_loaded::<Author>(&ctx, "read", Query::new("author").load(["posts.author"]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
}

#[tokio::test]
async fn a_too_deep_load_is_refused_before_any_read_runs() {
    // The refusal must be a refusal, not a truncation: nothing may reach the
    // layer, or the caller has paid for a partial answer they cannot detect.
    #[derive(Default)]
    struct CountingLayer {
        inner: InMemoryDataLayer,
        reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl DataLayer for CountingLayer {
        async fn create(&self, r: &str, pk: &str, rec: Record) -> Result<Record> {
            self.inner.create(r, pk, rec).await
        }
        async fn read(&self, q: &Query) -> Result<Vec<Record>> {
            self.reads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.read(q).await
        }
        async fn get(&self, r: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
            self.inner.get(r, pk, id).await
        }
        async fn update(&self, r: &str, pk: &str, id: &Value, c: &Record) -> Result<Record> {
            self.inner.update(r, pk, id, c).await
        }
        async fn destroy(&self, r: &str, pk: &str, id: &Value) -> Result<()> {
            self.inner.destroy(r, pk, id).await
        }
    }

    let d = domain();
    let layer = Arc::new(CountingLayer::default());
    layer
        .create(
            "author",
            "id",
            Record::from_iter([("id", Value::from("a1")), ("name", Value::from("ada"))]),
        )
        .await
        .unwrap();
    let ctx = Context::new(layer.clone());

    let err = d
        .read_loaded::<Author>(
            &ctx,
            "read",
            Query::new("author").load(["posts.author.posts.author.posts"]),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Invalid { .. }), "{err:?}");
    assert_eq!(
        layer.reads.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a too-deep load must be refused before any read reaches the layer"
    );
}

// ── redaction skip: Policy::redacts ──────────────────────────────────────

/// A policy that gates operations only and declares so — the common case the
/// per-row redaction pass should skip entirely. It counts any field call it
/// receives so a test can prove it received none.
struct OpsOnly(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait::async_trait]
impl Policy for OpsOnly {
    fn redacts(&self) -> bool {
        false
    }
    async fn authorize_read(
        &self,
        _resource: &str,
        _action: &str,
        _query: &Query,
        _actor: Option<&Record>,
    ) -> Decision {
        Decision::Allow
    }
    async fn authorize_attribute_read(
        &self,
        _resource: &str,
        _action: &str,
        _attribute: &str,
        _record: &Record,
        _actor: Option<&Record>,
    ) -> Decision {
        // Must never be reached: this policy declared it does not redact.
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Decision::NotApplicable
    }
}

#[tokio::test]
async fn a_non_redacting_policy_is_not_consulted_per_field() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::new().with(ScopedPolicy::domain(
                "ops-only",
                Arc::new(OpsOnly(calls.clone())),
            )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    for i in 0..5 {
        layer
            .create(
                "doc",
                "id",
                Record::from_iter([
                    ("id", Value::from(format!("d{i}"))),
                    ("title", Value::from("t")),
                    ("done", Value::Bool(true)),
                ]),
            )
            .await
            .unwrap();
    }
    let ctx = Context::new(layer);
    let rows: Vec<Record> = d.read::<Doc>(&ctx).await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a policy declaring redacts()==false must not be consulted per field"
    );
}

#[tokio::test]
async fn skipping_a_non_redacting_policy_does_not_change_redaction() {
    // The optimization must be invisible: a redacting policy still redacts when
    // a non-redacting one sits beside it in the same set.
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let d = Domain::new(
        DomainConfig {
            resources: vec![erase::<Doc>()],
            policies: PolicySet::new()
                .with(ScopedPolicy::domain(
                    "ops-only",
                    Arc::new(OpsOnly(calls.clone())),
                ))
                .with(ScopedPolicy::domain(
                    "admit",
                    Arc::new(crate::policy::Admit),
                ))
                .with(ScopedPolicy::attribute(
                    "doc",
                    "title",
                    "hide-title",
                    Arc::new(Deny("hidden".into())),
                )),
            ..DomainConfig::default()
        },
        DomainContext::new(),
    );
    let layer = Arc::new(InMemoryDataLayer::new());
    layer
        .create(
            "doc",
            "id",
            Record::from_iter([
                ("id", Value::from("d1")),
                ("title", Value::from("secret")),
                ("done", Value::Bool(true)),
            ]),
        )
        .await
        .unwrap();
    let ctx = Context::new(layer);
    let rows: Vec<Record> = d.read::<Doc>(&ctx).await.unwrap();
    // The attribute-scoped Deny still redacts `title`...
    assert_eq!(rows[0].get("title"), Some(&Value::Null));
    // ...while `id` is untouched, and the ops-only policy was never asked.
    assert_eq!(rows[0].get("id"), Some(&Value::from("d1")));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn a_set_of_only_non_redacting_policies_skips_the_pass() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let ops_only =
        PolicySet::new().with(ScopedPolicy::domain("ops-only", Arc::new(OpsOnly(calls))));
    assert!(!ops_only.has_attribute_policies());

    // Admit and AllowAll are operation-only too.
    assert!(
        !PolicySet::new()
            .with(ScopedPolicy::domain(
                "admit",
                Arc::new(crate::policy::Admit)
            ))
            .has_attribute_policies()
    );

    // A policy that may redact still forces the pass.
    assert!(
        PolicySet::new()
            .with(ScopedPolicy::resource(
                "doc",
                "deny",
                Arc::new(Deny("no".into()))
            ))
            .has_attribute_policies()
    );
}
