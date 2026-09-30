# Design: the Transaction seam

Status: **implemented.** Closes the one correctness gap `ARCHITECTURE.md`
documented as "out of scope": the core had no transaction / unit-of-work seam, so
`persist → events` was non-atomic. The seam is now optional on `Store`; event
delivery stays post-commit and best-effort by design.

**How open question #1 resolved.** Rather than a separate `handle_action_tx` entry
with a `B: Transactional` bound, `begin` is an **optional method on the `Store`
trait itself** (defaulting to `Ok(None)`), the same shape as `DataLayer`'s
optional `aggregate` push-down. The one write path checks it; a store that
declines is byte-for-byte the old path. No forked entry point, one bound
(`Store`), and existing backends are untouched.

The goal is a **seam, not an engine** — the same shape as every M1–M5 addition.
The core gains an *optional* atomicity boundary it uses **when the `Store`
provides one** and degrades from **explicitly** when it does not. No transaction
runtime, no two-phase commit, no distributed coordination in the core.

## The gap, precisely

Today the write pipeline (`exec_create` / `exec_update` / `exec_destroy`) is:

```
stage → authorize → before_action (+locks) → validate
      → persist (layer op, COMMITS NOW)
      → after_action (extensions)
      → emit_event (handlers, best-effort)
      → return
```

Two consequences, both documented in `ARCHITECTURE.md`:

1. **persist commits immediately.** The in-memory layer has no transaction, so
   the row is durable the instant `layer.create` returns — before extensions or
   events run.
2. **a committed row can still return `Err`.** If an `after_action` extension or
   an event handler fails, the caller gets `Err` on a row that is committed and
   readable. That is the "CRUD call can return `Err` on a committed row" contract.

The gap is *only* that the core cannot ask the layer to **defer the commit** until
the pipeline has succeeded. Everything else about the pipeline is already correct.

## The seam

Add an **optional** transaction capability to the persistence side. Two moving
parts, both object-safe and `Send + Sync` (like every seam):

```rust
/// An open unit of work over a DataLayer. CRUD ops issued through it are staged
/// and only durable on `commit`; dropping without commit rolls back.
#[async_trait]
pub trait Transaction: DataLayer {   // it *is* a DataLayer for the ops it scopes
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Optional capability on a Store: hand the executor a transaction to run in.
pub trait Transactional {
    async fn begin(&self) -> Result<Box<dyn Transaction>>;
}
```

`Transaction: DataLayer` is the key move — inside the boundary the executor
issues the *same* CRUD calls it already makes, just against the transaction
handle instead of the bare layer. No second code path for the layer ops.

## Where it hooks (and the new order)

When the backend is `Transactional`, the write pipeline becomes:

```
stage → authorize → before_action (+locks) → validate
      → begin()                      ← NEW
      → persist (via the txn handle)
      → after_action (extensions)    ← now inside the boundary
      → commit()                     ← NEW; the durable point
      → emit_event (handlers)        ← still post-commit, still best-effort
      → return
```

- **Everything the caller can undo moves inside the boundary.** A failing
  `after_action` now rolls back the row instead of leaving it committed.
- **Events stay post-commit and best-effort — deliberately.** Event delivery is
  the one thing that genuinely *cannot* be atomic with the commit without an
  outbox (which is a consumer concern, per `ARCHITECTURE.md`). Rolling the write
  back because a webhook failed would be worse. So the `Err`-on-committed-row
  contract narrows to *just* the event phase, which is the honest boundary.
- **Authorization still runs first**, before `begin()`. A denied write never
  opens a transaction — fail-closed, unchanged.

## Degradation (the explicit, not-silent part)

A `Store` that is **not** `Transactional` keeps today's exact path: no `begin`,
persist commits immediately, best-effort events. The executor picks the path by
whether the backend offers the capability — the same "opt-in, visible" pattern as
`allow_in_memory` / `QueryCapabilities`. There is **no** in-core emulation of a
transaction over a non-transactional layer (that would be an engine, and a lying
one). The degradation is a real, documented behavioural difference, not a silent
fallback.

## Open questions (decide before building)

1. **How does the executor reach the capability?** `Context<B>`'s backend is `B`.
   Options: (a) a `FromRef`-style projection so `ctx.extract::<Box<dyn Transaction>>`
   … no — transactions aren't cloneable handles. (b) a `Transactional` bound
   discovered by trying to downcast the `Store` — not object-safe-friendly. (c) a
   separate executor entry (`Domain::handle_action_tx`) taking a `B: Transactional`
   bound, mirroring how CRUD already requires `B: Store`. **(c) is the likely
   answer** — it keeps the non-transactional path untouched and the bound honest,
   exactly as `Store` gates persistence today.

2. **Nested / re-entrant actions.** The generic path can re-enter the domain
   (`HandlerContext::domain()`). Does a nested action join the outer transaction or
   open its own? Simplest correct answer: **join** — thread the open transaction
   through the context so re-entry uses it; forbid a second `begin`. Needs the
   transaction handle to live on the context for the action's span.

3. **Locks vs. transaction ordering.** `before_action` acquires locks *before*
   `begin` today. Under the seam, should the lock span the transaction (acquire →
   begin → … → commit → release)? Almost certainly yes — a lock that releases
   before commit defeats single-writer. The `ActionHold` guards already live to
   the end of the method, so this mostly falls out, but it must be stated.

4. **Read-your-writes inside the boundary.** After `persist` via the txn handle,
   does a `get`/`read` *through the same handle* see the uncommitted row? That is
   the layer's contract (a real SQL txn: yes). The core should document that reads
   issued on the transaction handle are read-your-writes and reads on the bare
   layer are not — and never mix them within one action.

5. **Does `emit_event` get the commit outcome?** A handler may want to know the
   write is durable. It already runs post-commit, so "durable" is implied; no new
   surface needed unless we want an explicit `committed_at`.

## What this is NOT

- Not a transaction *manager* / connection pool — the `Store` owns connections.
- Not distributed transactions, sagas, or 2PC — out of scope, forever.
- Not an outbox *engine* — but the seam is now open to one. `EventHandler::stage`
  is handed each event before the commit, with the transaction's own `DataLayer`
  view, so a consumer's handler can write an outbox row in the same transaction
  as the write. The core supplies no outbox table, no relay and no delivery
  guarantee of its own; it only lends the transaction it already had. See
  "The outbox seam" below.
- Not mandatory — a `Store` opts in; every existing backend keeps working
  unchanged.

## Test matrix (when built)

- Transactional backend: `after_action` failure **rolls back** the row (not
  committed, not readable) — the new guarantee, the headline test.
- Transactional backend: event-handler failure still returns `Err` but the row
  **is** committed (the narrowed contract holds).
- Non-transactional backend: today's path is byte-for-byte unchanged (regression).
- Authorization denial never calls `begin` (fail-closed, no transaction opened).
- Nested action joins the outer transaction (no double `begin`; outer rollback
  undoes the nested write).
- A `sandbox` ordering assertion: `begin → persist → after_action → commit → emit`.
```

## The outbox seam (follow-on, implemented)

The transaction above made *row + extensions* atomic and left events as the
explicit best-effort tail. `EventHandler::stage` closes that tail for handlers
that need it, without the core growing a delivery engine.

```
stage → authorize → before_action (+locks) → validate
      → persist (through the txn)
      → after_action (extensions)
      → stage (event handlers, THROUGH THE SAME TXN)   ← new
      → commit                                          ← the durable point
      → handle (event handlers, post-commit best-effort)
      → return
```

- **What the core gives.** The open transaction, as a `&dyn DataLayer`, plus the
  same `DomainEvent` the post-commit path will deliver — built once and shared,
  so both describe the action identically.
- **What the consumer owns.** The outbox table (it is just another resource the
  layer can write), the relay that publishes from it, idempotency at the
  consumer, and retention. None of that is domain logic.
- **Failure rolls back.** Staging runs before the commit, so a handler that
  cannot record the event fails the action rather than leaving a committed row
  nothing announced. That is the opposite of `handle`, which cannot undo
  anything, and it is the whole reason to stage.
- **No silent downgrade.** A handler declares `stages() -> true`. If the `Store`
  offers no transaction, the write fails with `Unsupported` at `begin`, before
  anything is persisted — never a quiet fallback to best-effort delivery for a
  deployment that asked for atomicity.
- **Batches stage per row, commit once.** A batch create stages one event per row
  inside the single transaction that covers the whole batch.
- **Generic actions do not stage.** A generic action performs no write and opens
  no transaction, so there is nothing for its event to be atomic with; it is
  delivered through `handle` only.
