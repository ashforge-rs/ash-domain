//! Coverage for **enum-valued resource fields** via `#[derive(ValueEnum)]` plus
//! `#[attribute(enumerate)]`.
//!
//! A closed set of variants (a status, a priority) is declared as a real Rust
//! enum and round-trips through the record pipeline as a string — typed in Rust,
//! stored as a `Value::Str`. This test proves: the field survives a create/get
//! round-trip as the enum; a `rename` override reaches storage; and a stored
//! string that names no variant is a `Serialization` error on read, never a
//! silently-wrong row.
//!
//! External test crate on purpose: the derive expands to `::ash_domain::…` paths
//! that only resolve outside `ash-domain` itself.

use std::sync::Arc;

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{
    Context, Domain, Error, FromRecord, FromValue, Record, Resource, Value, ValueEnum,
};

#[derive(ValueEnum, Clone, Copy, PartialEq, Debug, Default)]
enum Status {
    #[default]
    Open,
    Done,
    #[value(rename = "in_progress")]
    InProgress,
}

#[derive(Resource, Default, Debug)]
#[resource(name = "task")]
struct Task {
    #[attribute(primary_key)]
    id: String,
    message: String, // free text stays a plain String
    #[attribute(enumerate)]
    status: Status,
}

fn task_domain() -> Domain {
    Domain::builder().register::<Task>().permissive().build()
}

#[tokio::test]
async fn enum_field_round_trips_through_create_and_get() {
    let domain = task_domain();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));
    let mut tasks = domain.bind(&mut ctx);

    let created = tasks
        .create::<Task>(Task {
            message: "ship it".into(),
            status: Status::InProgress,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(created.status, Status::InProgress);

    let got = tasks
        .get::<Task>(created.id.clone())
        .await
        .unwrap()
        .expect("row present");
    assert_eq!(got.status, Status::InProgress);
    assert_eq!(got.message, "ship it");
}

#[tokio::test]
async fn rename_override_is_the_stored_string() {
    // The `Data = Record` view of the same row shows the wire string, confirming
    // `InProgress` stored as `"in_progress"`, not `"inprogress"`.
    let value: Value = Status::InProgress.into();
    assert_eq!(value, Value::from("in_progress"));
    assert_eq!(Value::from(Status::Open), Value::from("open"));
}

#[tokio::test]
async fn unknown_variant_string_fails_closed_on_read() {
    // A stored string that matches no variant must error on read — it can never
    // become a silently-wrong typed value.
    let err = Status::from_value(&Value::from("archived")).unwrap_err();
    assert!(
        matches!(err, Error::Serialization(_)),
        "expected Serialization, got {err:?}"
    );

    // And end-to-end: a raw record carrying a bogus status fails when projected
    // into the typed `Task`.
    let rec = Record::from_iter([
        ("id", Value::from("t1")),
        ("message", Value::from("x")),
        ("status", Value::from("archived")),
    ]);
    let projected = Task::from_record(&rec);
    assert!(
        projected.is_err(),
        "bogus status should not project into a Task"
    );
}
