//! A minimal todo app: **make a todo, get a todo**, backed by real SQLite.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example todo_sqlite
//! ```
//!
//! Two pieces:
//!
//! 1. A [`Todo`] resource — `id`, `owner`, `message`, `status` — declared the
//!    ordinary way (attributes + a `create` write and a `read`). `status` is a
//!    [`Status`] enum: even though this example flows raw [`Record`]s (not a
//!    derived struct), `#[derive(ValueEnum)]` still gives it a typed, closed set
//!    of states that convert to/from a [`Value`] at the layer boundary.
//! 2. A `SqliteDataLayer` that implements the [`DataLayer`] seam over `sqlx`.
//!    The core defines no query language, so this layer defines its own tiny
//!    interpretation of the param bag: it honours an `eq` map (used here to get
//!    a todo by owner) and the reserved key-set load. That is the whole contract
//!    — CRUD plus "interpret the bag however you can".
//!
//! Built with the `sandbox` feature, a third piece runs after the app: the same
//! domain driven under the **sandbox**, with the real SQLite layer wrapped in a
//! recording, fault-injectable `SpyLayer` — see `sandbox_demo` at the bottom.
//!
//! ```sh
//! cargo run --example todo_sqlite --features sandbox
//! # add `trace` to also collect the pipeline's execution trace into the log
//! cargo run --example todo_sqlite --features sandbox,trace
//! ```

use std::sync::Arc;

use ash_domain::attribute::Attribute;
use ash_domain::datalayer::DataLayer;
use ash_domain::query::Query;
use ash_domain::value::{FromValue, Record, Value, value_key};
use ash_domain::{ActionDef, Context, Domain, Error, Resource, Result, ValueEnum};
use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};

// ── the resource: id, owner, message, status ─────────────────────────────────

/// A closed set of todo states. `#[derive(ValueEnum)]` generates the
/// `Value` <-> `Status` conversions, so the enum round-trips as a string
/// (`"open"`, `"done"`, `"in_progress"`) — typed in Rust, plain TEXT in SQLite.
#[derive(ValueEnum, Clone, Copy, PartialEq, Debug, Default)]
enum Status {
    #[default]
    Open,
    Done,
    #[value(rename = "in_progress")]
    InProgress,
}

/// A todo item. Resources are *types*, not values — the schema is metadata.
struct Todo;

impl Resource for Todo {
    const NAME: &'static str = "todo";
    type Data = Record;

    fn attributes() -> Vec<Attribute> {
        vec![
            Attribute::scalar::<String>("id"),
            Attribute::scalar::<String>("owner"),
            Attribute::scalar::<String>("message"),
            // The enum is a scalar of its own Rust type; the layer stores its
            // string form.
            Attribute::scalar::<Status>("status"),
        ]
    }

    fn actions() -> Vec<ActionDef> {
        // A write to make one, a read to get them. No custom logic — the
        // defaults (stamp an id on create, hand the param bag to the layer on
        // read) are all this app needs.
        vec![ActionDef::write("create"), ActionDef::read("read")]
    }
}

// ── the seam: a DataLayer over SQLite ────────────────────────────────────────

/// A [`DataLayer`] that stores todos in a single SQLite table. This layer's
/// private interpretation of the param bag is deliberately tiny: it honours the
/// core reserved key-set (so relationship loads would work) and an `eq` map of
/// `column -> value` (so "get this owner's todos" is one `WHERE`).
struct SqliteDataLayer {
    pool: SqlitePool,
}

impl SqliteDataLayer {
    /// Open an in-memory database and create the `todo` table.
    async fn connect() -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .map_err(to_data_layer)?;
        sqlx::query(
            "CREATE TABLE todo (\
               id TEXT PRIMARY KEY, \
               owner TEXT NOT NULL, \
               message TEXT NOT NULL, \
               status TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .map_err(to_data_layer)?;
        Ok(Self { pool })
    }
}

/// Decode a `todo` row back into the dynamic [`Record`] the domain speaks.
///
/// Fallible on the `status` column: the stored string is validated back through
/// [`Status::from_value`], so a value that names no variant (a bad migration, a
/// hand-edited row) surfaces as an error here instead of flowing on as an opaque
/// string. The other columns are free text and can't be malformed.
fn row_to_record(row: &sqlx::sqlite::SqliteRow) -> Result<Record> {
    let status_str = Value::from(row.get::<String, _>("status"));
    // Round-trip through the enum to reject an unknown status, then store the
    // canonical string form.
    let status: Status = Status::from_value(&status_str)?;

    let mut rec = Record::new();
    rec.insert("id".to_string(), Value::from(row.get::<String, _>("id")));
    rec.insert(
        "owner".to_string(),
        Value::from(row.get::<String, _>("owner")),
    );
    rec.insert(
        "message".to_string(),
        Value::from(row.get::<String, _>("message")),
    );
    rec.insert("status".to_string(), Value::from(status));
    Ok(rec)
}

/// A required string column off a `Record`. Caller data is validated, never
/// unwrapped — a missing/mistyped field is an `Invalid`, not a panic.
fn required_str(rec: &Record, field: &str) -> Result<String> {
    rec.get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid(format!("todo record is missing string field `{field}`")))
}

fn to_data_layer(e: sqlx::Error) -> Error {
    Error::data_layer(e.to_string())
}

#[async_trait]
impl DataLayer for SqliteDataLayer {
    async fn create(&self, _resource: &str, _pk: &str, record: Record) -> Result<Record> {
        let id = required_str(&record, "id")?;
        let owner = required_str(&record, "owner")?;
        let message = required_str(&record, "message")?;
        // Read the status as a typed `Status` (rejecting an unknown one), then
        // bind its canonical string. `Value::from(status)` is the enum's wire form.
        let status = record
            .get("status")
            .map(Status::from_value)
            .transpose()?
            .unwrap_or_default();
        sqlx::query("INSERT INTO todo (id, owner, message, status) VALUES (?, ?, ?, ?)")
            .bind(&id)
            .bind(&owner)
            .bind(&message)
            .bind(value_key(&Value::from(status)))
            .execute(&self.pool)
            .await
            .map_err(to_data_layer)?;
        Ok(record)
    }

    async fn read(&self, query: &Query) -> Result<Vec<Record>> {
        // This layer understands two things: the reserved key-set (`id IN (…)`)
        // and an `eq` map. Anything else in the bag has no effect here.
        let mut sql = String::from("SELECT id, owner, message, status FROM todo");
        let mut binds: Vec<String> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();

        if let Some((attr, keys)) = query.as_key_set() {
            let placeholders = vec!["?"; keys.len()].join(", ");
            clauses.push(format!("{attr} IN ({placeholders})"));
            for k in keys {
                binds.push(value_key(k));
            }
        }
        if let Some(eq) = query.params.get("eq").and_then(Value::as_map) {
            for (col, want) in eq.iter() {
                clauses.push(format!("{col} = ?"));
                binds.push(value_key(want));
            }
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }

        let mut q = sqlx::query(&sql);
        for b in &binds {
            q = q.bind(b);
        }
        let rows = q.fetch_all(&self.pool).await.map_err(to_data_layer)?;
        rows.iter().map(row_to_record).collect()
    }

    async fn get(&self, _resource: &str, _pk: &str, id: &Value) -> Result<Option<Record>> {
        let row = sqlx::query("SELECT id, owner, message, status FROM todo WHERE id = ?")
            .bind(value_key(id))
            .fetch_optional(&self.pool)
            .await
            .map_err(to_data_layer)?;
        row.as_ref().map(row_to_record).transpose()
    }

    async fn update(
        &self,
        _resource: &str,
        _pk: &str,
        _id: &Value,
        _changes: &Record,
    ) -> Result<Record> {
        // This tiny app only makes and gets todos; editing is out of scope.
        Err(Error::Unsupported("todo update".into()))
    }

    async fn destroy(&self, _resource: &str, _pk: &str, _id: &Value) -> Result<()> {
        Err(Error::Unsupported("todo destroy".into()))
    }
}

// ── the sandbox, over the real layer ─────────────────────────────────────────

/// The same domain, driven under the sandbox — against the *real* SQLite layer.
///
/// The bundled `sb.context()` would hand out an in-memory store; this scenario
/// wants the actual `SqliteDataLayer`, so it hand-wires the parts instead: the
/// layer goes inside a [`SpyLayer`](ash_domain::sandbox::SpyLayer) sharing the
/// sandbox's effect log and fault plan. Every call is then recorded — values
/// and outcome — and can be failed on purpose, while `sb.config()` keeps time
/// and ids deterministic (manual clock, seeded generator).
#[cfg(feature = "sandbox")]
async fn sandbox_demo() -> Result<()> {
    use ash_domain::sandbox::{Sandbox, SpyLayer};
    use ash_domain::{DomainConfig, DomainContext, erase, expect_count, expect_one};

    let sb = Sandbox::new();
    // With `--features sandbox,trace`, also collect the execution trace: the
    // pipeline's spans and events join the same effect log (kinds "trace.span"
    // and "trace"), assertable like any other effect.
    #[cfg(feature = "trace")]
    let _trace_guard = sb.collect_tracing();
    let layer = Arc::new(
        SpyLayer::new(SqliteDataLayer::connect().await?, sb.effects.clone())
            .with_faults(sb.faults.clone()),
    );
    let domain = Domain::new(
        DomainConfig {
            resources: vec![erase::<Todo>()],
            ..sb.config()
        },
        DomainContext::new(),
    );
    #[cfg(feature = "trace")]
    domain.enable_tracing();
    let mut ctx = Context::new(layer);
    let mut todos = domain.bind(&mut ctx);

    // A committed create: recorded as a "write" (outcome "ok") and — post-commit
    // — as an "event". The id comes from the seeded generator: same seed, same id.
    let mut todo = Record::new();
    todo.insert("owner".to_string(), Value::from("alice"));
    todo.insert("message".to_string(), Value::from("water plants"));
    let created = todos.create::<Todo>(todo.clone()).await?;
    println!(
        "sandbox    created {:?} (deterministic id)",
        created.get("id")
    );

    // Script a fault: the db "goes down" for writes. The call still happens —
    // and is still recorded (outcome "fault") — but never reaches SQLite.
    sb.faults
        .set(|call| (call.kind == "write").then(|| Error::data_layer("injected: db down")));
    let err = todos.create::<Todo>(todo).await.unwrap_err();
    sb.faults.clear();
    println!("sandbox    second create failed on purpose: {err}");

    // Assert over the log: one committed write, one faulted write — and exactly
    // one post-commit event, because the faulted create never committed.
    expect_one!(sb.effects, "write", op: "create", outcome: "ok")?;
    expect_one!(sb.effects, "write", op: "create", outcome: "fault")?;
    expect_count!(sb.effects, 1, "event")?;

    // The trace corroborates the story: both creates were staged and
    // authorized, but exactly one reached "persisted".
    #[cfg(feature = "trace")]
    {
        expect_count!(sb.effects, 2, "trace", step: "staged")?;
        expect_count!(sb.effects, 1, "trace", step: "persisted")?;
        let traced = sb
            .effects
            .all()
            .iter()
            .filter(|e| e.kind == "trace")
            .count();
        println!("sandbox    trace: {traced} pipeline events, one \"persisted\"");
    }

    // The real layer agrees with the log: only the committed todo is in SQLite.
    let rows = todos.read::<Todo>(Query::new(Todo::NAME)).await?;
    println!(
        "sandbox    sqlite holds {} row(s), as the log said",
        rows.len()
    );
    Ok(())
}

// ── the app ──────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Wire the domain over the SQLite layer. `Arc<L: DataLayer>` is already a
    // `Store`, so the layer *is* the backend — no wrapper needed. `permissive()`
    // keeps this focused example about persistence, not authorization.
    let layer = SqliteDataLayer::connect().await?;
    let domain = Domain::builder().register::<Todo>().permissive().build();
    let mut ctx = Context::new(Arc::new(layer));
    let mut todos = domain.bind(&mut ctx);

    // ── make a todo ──────────────────────────────────────────────────────────
    // This example flows raw `Record`s (rather than a derived struct) to keep the
    // spotlight on the `DataLayer` seam; the ergonomic verbs still apply. The
    // status is set as a typed `Status` — `Value::from(status)` is its wire form.
    let mut new_todo = Record::new();
    new_todo.insert("owner".to_string(), Value::from("alice"));
    new_todo.insert("message".to_string(), Value::from("buy milk"));
    new_todo.insert("status".to_string(), Value::from(Status::InProgress));

    let created = todos.create::<Todo>(new_todo).await?;
    let id = created.get("id").cloned().unwrap_or(Value::Null);
    println!("made todo  {id:?} -> {:?}", created.get("message"));

    // ── get a todo (by id) ───────────────────────────────────────────────────
    // `get` issues the reserved key-set query, which this layer honours (the
    // `id IN (…)` branch in `read`) — no hand-built param map. The `status`
    // column comes back as a typed `Status`, exhaustively matchable.
    match todos.get::<Todo>(id.clone()).await? {
        Some(todo) => {
            let status = todo
                .get("status")
                .map(Status::from_value)
                .transpose()?
                .unwrap_or_default();
            let label = match status {
                Status::Open => "not started",
                Status::InProgress => "under way",
                Status::Done => "finished",
            };
            println!(
                "got todo   owner={:?} message={:?} status={status:?} ({label})",
                todo.get("owner"),
                todo.get("message"),
            );
        }
        None => println!("no todo with id {id:?}"),
    }

    #[cfg(feature = "sandbox")]
    sandbox_demo().await?;

    Ok(())
}
