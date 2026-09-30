# CLAUDE.md

Guidance for working in `ash-domain`. Read this before making changes.

## What this crate is

`ash-domain` is a declarative, resource-oriented domain framework for Rust,
inspired by the Elixir Ash Framework. You declare **resources** (typed
attributes + actions), register them in a **`Domain`**, and run actions through a
per-request **`Context<B>`**. Everything else — storage, authorization, side
effects, time, id generation — is a pluggable **seam** behind an object-safe,
`Send + Sync` trait. The core ships almost no engine (one in-memory data layer, a
system clock, a UUID generator); you extend it by implementing a trait on your
own type.

`ARCHITECTURE.md` is the source of truth for the design (the layered view, the
per-path execution diagrams, the context model, and the typed-query seam). Read
it before touching the executor or the seams. This file is about *how to work in
the code*, not what it does.

## Layout

- `src/lib.rs` — crate root: the public prelude and module wiring. Every public
  type is re-exported here.
- `src/resource.rs`, `src/attribute.rs`, `src/aggregate.rs`, `src/value.rs` — the
  declarative domain entity (resources are **types**, not values; data flows as
  dynamic `Record`s).
- `src/action.rs`, `src/query.rs`, `src/query_type.rs`, `src/repository.rs` —
  actions, reads, typed queries, and the optional façade.
- `src/domain/` — the registry + executor, split by pipeline stage: `mod.rs`
  (the `Domain` struct, construction, lifecycle, `handle_action`/`dispatch_action`),
  `write.rs`, `read.rs`, `typed_query.rs`, `tenant.rs`, `authorize.rs`,
  `registry.rs` (the `try_new` checks), `config.rs` (`DomainConfig`/`DomainBuilder`/`Bound`),
  and `tests.rs`. A method private to one stage is `pub(super)` so its siblings
  can call it; nothing here widened beyond the `domain` module.
- `src/context.rs` — `Context<B>`, `DomainContext`, `HandlerContext`, the
  `Store`/`Backend`/`FromRef` traits.
- `src/policy.rs` — authorization (default-deny, admit-on-affirmative).
- `src/datalayer.rs` (+ `datalayer/memory.rs`), `src/clock.rs`, `src/id.rs`,
  `src/event.rs`, `src/notify.rs`, `src/extension.rs` (+ `extension/*`) — the
  seams and the shipped extensions.
- `src/error.rs`, `src/sandbox.rs` — the error type and the (feature-gated)
  deterministic side-effect harness.
- `ash-macros/` — the derive macros (`Resource`, `Projection`, `TypedQuery`,
  `Embeddable`, `FromRecord`, `IntoRecord`). Proc-macro crates must stand alone,
  so this is a sibling workspace member; `ash-domain` re-exports the macros behind
  the `derive` feature.

## Build, test, lint

Use the `Makefile` (or plain cargo). The optional `ash-*` crates (`ash-fsm`,
`ash-time`, `ash-log`, `ash-lock`, `ash-flare`) come from crates.io.

```sh
make build     # cargo build
make test      # unit, integration, and doc tests
make clippy    # clippy, warnings as errors
make check     # type-check only
```

- **Edition 2024, MSRV 1.85.** Both `ash-domain` and `ash-macros` are on edition
  2024; do not use features newer than Rust 1.85 without bumping `rust-version`
  in both `Cargo.toml`s.
- **Doctests are load-bearing.** Public items carry runnable doctests that double
  as smoke tests (the `TypedQuery`/`Projection` macros are *only* validated by
  doctests, because their generated `::ash_domain::…` paths can't resolve inside
  the crate itself). Keep them runnable, not `ignore`, unless there's a real
  reason.
- **`cargo fmt --check` is not clean repo-wide** — there is pre-existing
  formatting drift. Do **not** run `cargo fmt` across the tree; it would reformat
  hundreds of untouched lines and bury your change. Match the surrounding
  hand-formatting instead.
- Run `make clippy` before finishing. Clippy is clean today; keep it that way.

## Conventions that are load-bearing (don't break these)

- **Object safety.** Every seam trait is used as `Arc<dyn Trait>` and must stay
  object-safe. A generic method breaks that — if you need per-type dispatch (as
  `TypedQuery` does), put the generic on the **executor** (`Domain`) or thread it
  through `Context<B>`, never on the erased trait.
- **`Send + Sync` throughout.** Every seam requires it; `Domain` holds only `Arc`
  fields so it stays `Send + Sync + Clone`. Don't introduce a non-`Send` field or
  a `Rc`/`RefCell` on any path that crosses an `.await`.
- **Fail closed.** Authorization runs **first** on every path, before anything
  observable. A denied caller must see `Forbidden` — never `Invalid`, never a
  side effect (no extension hook, no lock, no constraint report). A policy-backend
  failure fails closed as `PolicyError`, distinct from a deliberate `Forbidden`.
  If you add a path, authorize before you do anything the caller could observe.
- **Degradation is explicit, never silent.** A data layer declares its
  `QueryCapabilities`; the domain never hands it a query beyond them. Anything
  that would degrade (an in-memory fallback, a cross-tenant bare read) is opt-in
  and visible, not automatic.
- **A versioned resource is never updated unconditionally.** A resource that
  declares `version_attribute` must go through `DataLayer::update_versioned`,
  whose compare-and-write is one atomic step. Never "fall back" to plain `update`
  when a layer declines the conditional write — declining is a `Unsupported`
  error, not a licence to lose an update.
- **A page needs an order.** `Query::limit` without `Query::sort` is an arbitrary
  subset, not a page. If you add a paging path, carry the sort with it, and build
  cursors from the **raw** rows (before redaction) so a hidden sort key cannot
  move where the next page starts.
- **Loads are depth-bounded, and the bound is checked before any read.** The
  relationship graph can be cyclic, so `max_load_depth` caps caller-supplied load
  paths. Check it on the requested paths up front — a too-deep request is refused
  whole, never served to the ceiling and truncated.
- **`Policy::redacts()` defaults to `true` and must stay fail-safe.** It skips a
  policy in the per-row redaction pass. Only ever return `false` from a policy
  that genuinely leaves `authorize_attribute_read` at its default; never use it to
  skip a field *write* check.
- **Data flows as `Record`s**, not concrete structs. The core is type-agnostic;
  don't reach for a concrete resource type inside the executor.
- **The registry is validated at `Domain::new`.** New kinds of misconfiguration
  (a dangling reference, a name collision, a mis-scoped policy) should fail at
  construction via `try_new`, not at request time.
- **Extensions run inside the authorization gate** — they are trusted,
  deployment-installed code. Keep that trust boundary intact: don't move a hook
  outside the gate, and remember `before_action` edits are *not* re-authorized
  and `after_read` sees *un-redacted* rows.

---

## TigerBeetle-style programming, tailored for Rust

Take the *spirit* of TigerBeetle's TIGER_STYLE and NASA's Power of Ten —
**safety and clarity first, then performance, then developer experience** — but
apply it the Rust way, not the Zig way. TIGER_STYLE's "assert everything, ~2
asserts per function" is a heuristic for a language with no borrow checker, no
sum types, no `Option`/`Result`. Rust already machine-checks most of what those
asserts buy, at compile time. Blindly porting the heuristic here produces
redundant `debug_assert!`s that restate the type system and add noise. So the
rule below is deliberately re-cast for Rust.

### Encode invariants in types first; assert only what types can't

The Rust form of "assert everything" is **make invalid states
unrepresentable** — push each invariant into a type so it's checked *once at
construction*, not asserted *at every use*.

- **Reach for a type before an assert.** A newtype, `NonZeroU32`, an enum, a
  `Option`/`Result`, or a constructor that can only yield valid values beats a
  runtime check scattered across call sites. If it type-checks, it's valid — no
  assert needed.
- **Never assert what the compiler already proves.** A `debug_assert!` that a
  `&T` is non-null, that an exhaustive `match` covered its cases, or that a value
  of type `X` "is a valid X" is pure noise — it cannot fail, and it misleads the
  reader into thinking there's a real runtime risk there. Delete such asserts;
  don't add them.
- **Do assert the invariants types can't express** — this is where asserts earn
  their place:
  - **Cross-value relationships.** Two `Vec`s the code assumes are the same
    length (`debug_assert_eq!(related_by_index.len(), records.len())` before
    indexing by `i`); a key that must exist in a map you just populated.
  - **Numeric / domain ranges** Rust has no range type for — an offset within
    bounds, a non-zero count, a sorted slice.
  - **Postconditions on computed results.** After redaction, every row still
    carries its primary key; after staging, the changeset has the pk. Assert it
    before returning.
- **Prefer `debug_assert!` for these** — they document the invariant and catch
  its violation in tests/CI without a release cost. Use a plain `assert!` only
  when the check is cheap and the invariant is catastrophic if wrong.
- **A safety or authorization invariant is a `Result`, never a `debug_assert!`.**
  Anything that gates access or upholds fail-closed must hold in *release*, so it
  is real control flow returning `Forbidden`/`PolicyError`/`Invalid` — not an
  assertion the compiler strips.
- **Assertions fire on a programmer error; caller data is a `Result::Err`.**
  Never `assert!`/`panic!`/`unwrap()` on data you don't control — a `Record` off
  the wire, a param bag, a policy-backend reply. Validate it and return an error.
- **A "genuinely impossible" `unwrap()` is an assertion — write it as one.**
  Use `.expect("<the invariant that makes this impossible>")` (or a
  `debug_assert!` on the guard), not a bare `.unwrap()`. The message names the
  invariant, e.g. `filter.take().expect("Some: guarded by is_some_and above")`.

### Bound everything

- **All loops and all queues must have a fixed upper bound.** Unbounded loops and
  unbounded buffering are how you get a hang or an OOM under load. If a loop's
  bound isn't statically obvious, cap it and handle the cap (or `debug_assert!`
  the bound where a cap doesn't fit).
- **No unbounded resource use.** A read that could pull an unbounded result set,
  a fan-out with no ceiling, a retry with no limit — each needs an explicit bound
  and an explicit behavior when the bound is hit. (This is exactly why
  `QueryCapabilities` and `allow_in_memory` exist: an unbounded full-table load
  is opt-in and visible, never the default.)
- Prefer explicit limits (`limit`, a cursor, a cap constant) over "it's usually
  small." "Usually" is where incidents live.

### Keep functions small and control flow simple

- **Functions should fit on a screen (~70 lines).** A long function is doing too
  much; split it. The executor in `domain.rs` is already large — when you touch
  it, extract a well-named helper rather than growing a method further.
- **No recursion where a bound isn't obvious.** Bounded, shallow recursion over a
  known-small structure (a nested embed, a load path a few levels deep) is fine;
  unbounded recursion over caller-shaped data is not.
- Keep nesting shallow. Prefer early `return`/`?` and `let … else` guards (the
  codebase already uses these) over deep `if`/`match` pyramids.
- One level of indirection per line where you can. Don't chain effects across an
  `.await` in a way that hides the order.

### Be explicit about types, units, and order

- **Use explicit, sized types.** Name the unit in the identifier when it isn't
  obvious (`now_millis`, `timeout_ms`). **No silent truncating `as` casts on
  data** — use checked `TryFrom` and surface an error on overflow, as both the
  derived `FromRecord` read path and the `IntoRecord` write path now do. An
  out-of-`i64`-range integer field fails with `Error::Serialization` rather than
  wrapping; consequently `IntoRecord::into_record` and
  `ActionInput::create`/`update`/`generic` are **fallible** (`-> Result<…>`).
- **Handle every case.** Match exhaustively; don't `_ =>` away a variant you
  should think about. A new `Decision`/`ActionKind`/`AttrType` variant should
  make the compiler force you to visit every site.
- **Order matters and must be intentional.** Authorization before side effects,
  redaction after extensions, events post-commit. When you add a step, place it
  deliberately in the pipeline and say why in a comment.

### Errors and failure

- **A CRUD call can return `Err` on a row that is nonetheless committed** (event
  delivery is post-commit, best-effort, un-retried). Don't "fix" this by
  swallowing errors — it's the documented contract of a core with no transaction
  seam. Preserve the distinction between the four error meanings (`Forbidden` vs
  `PolicyError` vs `Invalid` vs `Unsupported`).
- **Fail closed on ambiguity.** If you can't prove an operation is allowed, deny
  it. If a decision can't be resolved, treat it as deny, not allow.
- No `unwrap()`/`expect()` on anything that can fail at runtime with real data.
  `expect()` is acceptable only for a genuinely-impossible case, and then the
  message states the invariant that makes it impossible.

### Test for real

- **Tests assert observable behavior, not internal structure.** The `sandbox`
  feature exists to make *"this was called, this was not"* always answerable —
  use it for side-effect-ordering tests rather than reaching into privates.
- Prefer deterministic tests. No wall-clock sleeps, no reliance on map iteration
  order, no flakiness. The `Clock`/`IdGenerator` seams exist so time and ids can
  be made deterministic in a test.
- Cover the fail-closed paths explicitly: a new authorized path needs a
  "denied by default" test and a "redacted" test, not just a happy path.

### Simplicity and zero technical debt

- **Do it right the first time.** Land a change complete — with its invariants
  encoded (in types or asserts), its bounds, its tests, and its doc — rather than
  a stub plus a TODO. There is no "clean it up later" budget.
- **The best code is no code.** Before adding a mechanism, check whether an
  existing seam already expresses it. This crate deliberately ships seams, not
  engines; a new feature is usually a trait impl on the consumer side, not new
  core surface.
- Name things precisely. A good name is a bound and a contract; a vague one hides
  a bug. Match the existing vocabulary (`stage`, `authorize`, `redact`,
  `push-down`, `admit`) rather than inventing synonyms.
