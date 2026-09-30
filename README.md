# ash-domain

**Model your domain, derive the rest.**

`ash-domain` is a declarative, resource-oriented application core for Rust,
inspired by the Elixir [Ash Framework](https://ash-hq.org). You declare a
**resource** once — its attributes, relationships, and actions — register it in a
**`Domain`**, and run actions through a per-request **`Context<B>`**.

Everything else — storage, authorization, side effects, time, id generation — is a
pluggable **seam** behind an object-safe, `Send + Sync` trait. The core defines
*what* each thing is and *how* it is requested; it ships almost no engine (one
in-memory data layer, a system clock, a UUID generator). You extend it by
implementing a trait on your own type.

```rust
use std::sync::Arc;

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{Context, Domain, Query, Record, Resource, Result};

#[derive(Resource, Default, Debug)]
#[resource(name = "note")]
struct Note {
    #[attribute(primary_key)]
    id: Option<String>,
    title: String,
}

async fn run() -> Result<()> {
    // Default-deny is the default; this domain is gated elsewhere, so it says so.
    let domain = Domain::builder().register::<Note>().permissive().build();
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    let note = Note::create(&domain, &mut ctx, Record::from_iter([("title", "buy milk")])).await?;
    let all = Note::read(&domain, &mut ctx, Query::new("note")).await?;

    println!("{} of {} notes", note.title, all.len());
    Ok(())
}
```

`ARCHITECTURE.md` is the source of truth for the design — the layered view, the
per-path execution diagrams, the context model, and the typed-query seam.

## Three convictions

**The context is a generic you build.** `Context<B>` carries a backend `B` of your
choosing, and `B`'s trait impls decide what the context can do. A store-less
`Context<()>` has no CRUD methods — the compiler removes them, rather than a
runtime error rejecting them.

**Fail closed, always.** Authorization runs **first** on every path, before
anything observable: no extension hook, no lock, no validation report, no
storage. It is default-deny with admit-on-affirmative — an operation is forbidden
unless some policy affirmatively allows it, any matching policy may veto, and
running open takes an explicit `PolicySet::permissive()`. A policy-backend
failure is a distinct `PolicyError`, never quietly a denial.

**Degradation is explicit, never silent.** Every read is bounded; a result set
past the ceiling is *refused*, not truncated. A layer that overruns a bound is
reported, not trusted. An unbounded read, a cross-tenant read, an in-memory
fallback: each is opt-in and visible, never something you default into.

## What you get

| | |
|---|---|
| **Resources** | attributes, relationships, aggregates, computed fields, embedded resources — declared once, as types |
| **Actions** | `Read` / `Create` / `Update` / `Destroy` / `Generic`, driven through one uniform pipeline |
| **Pipeline** | composable `Preparation` · `Change` · `Validation` · `GenericHandler` steps |
| **Authorization** | scoped policies gating operations, attribute reads (redaction) and attribute writes, with a side-effect-free dry run |
| **Multi-tenancy** | tenant-scoped by default, cross-tenant access opt-in and marked |
| **Batch writes** | per-row pipeline, batched persist, bounded — one denied row persists none |
| **Transactions** | optional `Store::begin`; the write and its `after_action` roll back together |
| **Outbox** | `EventHandler::stage` writes the event inside the write's own transaction |
| **Schema export** | the validated registry as data, and as a JSON Schema document |
| **Observability** | a `Metrics` seam, `tracing` instrumentation, and a deterministic side-effect sandbox for tests |

## Seams

Each is an object-safe trait you implement on your own type. The core ships the
minimum needed to run and test.

| Seam | What it is | Shipped |
|---|---|---|
| `DataLayer` | persistence — CRUD over `Record`s, plus optional aggregate push-down and batch persist | in-memory |
| `Store` | the context's backend; optionally hands out a `Transaction` | any `Arc<dyn DataLayer>` |
| `Policy` / `PolicySet` | authorization and redaction | `Admit`, `Deny` |
| `Extension` | trusted, deployment-installed hooks around every action | fsm, audit, lock (opt-in) |
| `EventHandler` | what a committed action becomes | — |
| `Publisher` / `JobQueue` | consumer-side transports for events and deferred work | — |
| `Clock` / `IdGenerator` | time and identity | system clock, UUIDv4 |
| `Metrics` | one sample per action | — |
| `TypedQuery` | a read whose result is a shape of your own | — |

Writing a `DataLayer`? `datalayer::conformance` turns the trait's prose contract
into a runnable check — key-sets, the row bound, batch persist, and, through
`check_store`, commit and rollback.

## Cargo features

The core links no `ash-*` code. Every shipped extension is opt-in.

| Feature | Default | What it adds |
|---|---|---|
| `derive` | **on** | `#[derive(Resource)]` and friends |
| `fsm` | off | the `ash-fsm` state-machine guard extension |
| `audit` | off | the `ash-log` tamper-evident audit extension |
| `lock` | off | the `ash-lock` single-writer extension |
| `hlc` | off | the `ash-time` Hybrid Logical Clock |
| `extensions` | off | `fsm` + `audit` + `lock` |
| `trace` | off | `tracing` spans and per-stage events |
| `sandbox` | off | the deterministic side-effect harness for tests |
| `flare` | off | the `ash-flare` supervisor adapter |

## Building

The optional `ash-*` integrations come from crates.io, so plain `cargo build`
works on a fresh checkout. The `Makefile` has shortcuts:

```sh
make build       # cargo build
make test        # unit, integration, and doc tests
make test-all    # …with every feature enabled
make clippy      # warnings as errors
```

Edition 2024, MSRV 1.85. Doctests are load-bearing — public items carry runnable
examples that double as smoke tests.

## License

Apache-2.0. See `LICENSE`.
