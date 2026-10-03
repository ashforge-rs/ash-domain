//! A minimal todo app: **make a todo, get a todo** — the ergonomic path.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example todo
//! ```
//!
//! This is the front door of the framework, shown with its most ergonomic
//! surface:
//!
//! * A [`Todo`] is a plain Rust struct; `#[derive(Resource)]` reads its fields
//!   and generates the schema, the default CRUD actions, and typed accessors.
//! * `Domain::builder()` assembles the domain without a config struct literal.
//! * `domain.bind(&mut ctx)` threads the domain + context **once**, so each call
//!   is just the verb: `todos.create(..)`, `todos.get(id)`.
//! * `domain.read::<Todo>(&ctx)` is the composable read builder — filter params
//!   (a bag the data layer interprets), relationship loads, and the redaction
//!   report are all chained on one request, and what you request decides what
//!   `.await` returns.
//!
//! Storage is the built-in in-memory layer, so the example stays about the
//! declarative surface. Swapping in a real database means implementing one trait
//! — see the `todo_sqlite` example for a SQLite [`DataLayer`].

use std::sync::Arc;

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{Context, Domain, Resource, Result, ValueEnum};

/// A closed set of states for a todo. `#[derive(ValueEnum)]` makes it a real
/// enum that stores as a string (`"open"`, `"done"`, …) but is typed and
/// exhaustively matchable in Rust — no stray status strings, no typos.
#[derive(ValueEnum, Clone, Copy, PartialEq, Debug, Default)]
enum Status {
    #[default]
    Open,
    Done,
    #[value(rename = "in_progress")]
    InProgress,
}

/// A todo item. The struct *is* the schema: each field becomes an attribute, and
/// `#[derive(Resource)]` infers the rest (default CRUD actions, typed accessors,
/// `Record` <-> `Todo` conversions).
///
/// `id` is the primary key. A `Default`-constructed `Todo` leaves it `""`, which
/// the framework treats as "not supplied" and stamps a fresh id on `create` —
/// so a plain `String` primary key just works; you don't reach for
/// `Option<String>`.
///
/// `message` is free text, so it stays a `String`. `status` is a fixed set, so
/// it is a typed [`Status`] enum via `#[attribute(enumerate)]` — the escape from
/// "everything is a string".
#[derive(Resource, Default, Debug)]
#[resource(name = "todo")]
struct Todo {
    #[attribute(primary_key)]
    id: String,
    owner: String,
    message: String,
    #[attribute(enumerate)]
    status: Status,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Assemble the domain fluently. `permissive()` is an explicit opt-out of the
    // fail-closed default, so this example is about the resource, not auth.
    let domain = Domain::builder().register::<Todo>().permissive().build();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // Bind the domain to this context once; call verbs on the handle.
    let mut todos = domain.bind(&mut ctx);

    // ── make a todo ──────────────────────────────────────────────────────────
    // Build the struct with real fields; `create` returns a typed `Todo` back,
    // with the stamped `id` filled in.
    let created = todos
        .create::<Todo>(Todo {
            owner: "alice".into(),
            message: "buy milk".into(),
            status: Status::InProgress,
            ..Default::default()
        })
        .await?;
    println!(
        "made todo  {} -> {} [{:?}]",
        created.id, created.message, created.status
    );

    // ── get a todo (by id) ───────────────────────────────────────────────────
    // `get` fetches one row by primary key and hands it back as a typed `Todo` —
    // `status` comes back as the `Status` enum, exhaustively matchable.
    match todos.get::<Todo>(created.id.clone()).await? {
        Some(todo) => {
            let state = match todo.status {
                Status::Open => "not started",
                Status::InProgress => "under way",
                Status::Done => "finished",
            };
            println!(
                "got todo   message={} status={:?} ({state})",
                todo.message, todo.status
            );
        }
        None => println!("no todo with id {}", created.id),
    }

    // ── list todos ───────────────────────────────────────────────────────────
    // The read builder: the resource is named once, by type, and the request is
    // refined by chaining. Filter params are a bag the data layer interprets —
    // `"limit"` here is the in-memory layer's convention, not core vocabulary.
    // Reads borrow the context shared (`&ctx`), so they can also run
    // concurrently.
    let listed: Vec<Todo> = domain.read::<Todo>(&ctx).filter("limit", 10).await?;
    println!("listed     {} todo(s)", listed.len());

    Ok(())
}
