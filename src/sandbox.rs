//! The **sandbox** — record what your domain did, and make it fail on purpose.
//!
//! Enabled by the `sandbox` cargo feature. The workflow it serves:
//!
//! 1. **You code the side effect and/or the fault in the sandbox first** — a
//!    scenario that shows a call being made (or not made), or a fault you
//!    inject with a [`FaultPlan`] you script yourself.
//! 2. The sandbox **records what happened**: every call, in order, with the
//!    values passed and its outcome (`ok` / `error` / `fault`). Calls that
//!    failed are recorded too — *this was called, this was not called* is
//!    always answerable from the log.
//! 3. **You assert yourself** — over [`EffectLog::all`] with plain Rust, or the
//!    thin `expect_*` helpers — then go back to your domain code (a change,
//!    policy, extension, notifier) to handle the effect, and re-run until green.
//!
//! [`Sandbox`] bundles the parts for the common case:
//!
//! ```no_run
//! # use ash_domain::sandbox::Sandbox;
//! # use ash_domain::{Domain, DomainConfig, DomainContext};
//! # fn demo() -> ash_domain::Result<()> {
//! let sb = Sandbox::new();
//! let domain = Domain::new(DomainConfig {
//!     // resources: vec![erase::<Order>()], extensions: ...,
//!     ..sb.config() // deterministic clock + ids, recording notifier
//! }, DomainContext::new());
//! let mut ctx = sb.context(); // recording, fault-injectable in-memory store
//!
//! // ... drive the domain, then assert yourself over what was recorded:
//! ash_domain::expect_one!(sb.effects, "shipment.requested")?;
//! ash_domain::expect_none!(sb.effects, "write", op: "destroy")?;
//! sb.effects.dump(); // print the ordered log — colorized on a terminal
//! # Ok(()) }
//! ```
//!
//! The [`expect_one!`](crate::expect_one), [`expect_none!`](crate::expect_none),
//! and [`expect_count!`](crate::expect_count) macros are thin sugar over the
//! same-named [`EffectLog`] methods — they derive the failure label from the
//! pattern, nothing more.
//!
//! The sandbox can also collect the domain's **diagnostics**, so a scenario can
//! assert over them like any other effect:
//!
//! - the execution trace (`trace` feature): [`Sandbox::collect_tracing`]
//!   installs a [`RecordingSubscriber`] that lands every `tracing` span and
//!   event in the log (kinds `"trace.span"` / `"trace"`);
//! - the audit log (`audit` feature): `Sandbox::audit_backend` is a
//!   `RecordingAuditBackend` to wire into an `AuditExtension`
//!   (kinds `"audit"` / `"audit.security"`).
//!
//! **Transactional scenarios.** A plain [`Sandbox::context`] declines
//! transactions, so it cannot reach the domain's transactional path at all — and
//! the [`EventHandler::stage`] outbox pass is
//! *refused outright* by the domain against such a store.
//! [`Sandbox::transactional_context`] hands you one that offers a recorded,
//! fault-injectable transaction instead: lifecycle lands as kind `"txn"`
//! (`begin` / `commit` / `rollback`), writes inside one carry `in_txn: true`, and
//! the [`FaultPlan`] is consulted for each step — so *"the commit fails, does the
//! row survive?"* is a scripted scenario:
//!
//! ```no_run
//! # use ash_domain::sandbox::Sandbox;
//! # use ash_domain::Error;
//! # fn demo() {
//! let sb = Sandbox::new();
//! sb.faults.set(|call| {
//!     (call.kind == "txn" && call.get_str("op") == Some("commit"))
//!         .then(|| Error::data_layer("injected: commit failed"))
//! });
//! let _ctx = sb.transactional_context();
//! # }
//! ```
//!
//! Every part is also usable on its own — [`ManualClock`], [`SeededIdGenerator`],
//! [`EffectLog`], [`FaultPlan`], [`SpyLayer`], [`TransactionalSpyStore`],
//! [`RecordingHandler`],
//! `RecordingAuditBackend`, `RecordingSubscriber` — when a scenario needs
//! hand-wiring (a shared store, your own layer, custom kinds via
//! [`EffectLog::record`]). The sandbox prescribes no effect taxonomy and no
//! fault schedule; those are yours to code.

use std::collections::HashMap;
#[cfg(feature = "trace")]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::aggregate::Aggregate;
use crate::clock::Clock;
use crate::context::Context;
use crate::datalayer::DataLayer;
use crate::datalayer::memory::InMemoryDataLayer;
use crate::domain::DomainConfig;
use crate::error::{Error, Result};
use crate::event::{DomainEvent, EventHandler};
use crate::id::IdGenerator;
use crate::query::Query;
use crate::value::{Record, Value};

// ── The bundle ───────────────────────────────────────────────────────────────

/// The parts of a sandbox scenario, wired together: an effect log, a fault
/// plan, a manual clock, and a seed for ids.
///
/// This is a *constructor*, not a wrapper — you still build the
/// [`Domain`](crate::Domain) and drive it yourself. [`config`](Sandbox::config)
/// hands you a pre-wired [`DomainConfig`] to extend with your resources;
/// [`context`](Sandbox::context) hands you a context over a recording,
/// fault-injectable in-memory store. The handles are public: script faults via
/// `sb.faults`, tick time via `sb.clock`, assert via `sb.effects`.
pub struct Sandbox {
    /// The ordered record of every call — assert over this.
    pub effects: EffectLog,
    /// The user-scripted fault source, consulted by the store and the notifier.
    pub faults: FaultPlan,
    /// The clock the domain stamps timestamps from — advance it by hand.
    pub clock: Arc<ManualClock>,
    seed: u64,
}

impl Sandbox {
    /// A sandbox with seed `0` and the clock at `0`.
    pub fn new() -> Self {
        Self::seeded(0)
    }

    /// A sandbox whose ids derive from `seed` (same seed ⇒ same run).
    pub fn seeded(seed: u64) -> Self {
        Self {
            effects: EffectLog::new(),
            faults: FaultPlan::new(),
            clock: Arc::new(ManualClock::new(0)),
            seed,
        }
    }

    /// A [`DomainConfig`] pre-wired for this sandbox: the manual clock, seeded
    /// ids, and a [`RecordingHandler`] (so every committed action's
    /// [`DomainEvent`] lands in the log). Extend it with your resources,
    /// extensions, and policy: `DomainConfig { resources: vec![…], ..sb.config() }`.
    ///
    /// The config ships **permissive** policies — the sandbox exercises side
    /// effects, and the domain's operation gate is default-deny, so an empty set
    /// would forbid every scenario action. Override `policies` (with a
    /// default-deny [`PolicySet`](crate::PolicySet) plus your rules) when the
    /// authorization behavior itself is what the scenario asserts.
    ///
    /// Each call builds a fresh id generator from the same seed — use one
    /// config per scenario so id sequences stay reproducible.
    pub fn config(&self) -> DomainConfig {
        DomainConfig {
            clock: self.clock.clone(),
            id_generator: Arc::new(SeededIdGenerator::new(self.seed)),
            event_handlers: vec![Arc::new(
                RecordingHandler::new(self.effects.clone()).with_faults(self.faults.clone()),
            )],
            policies: crate::policy::PolicySet::permissive(),
            ..DomainConfig::default()
        }
    }

    /// A context over a fresh in-memory store whose every call is recorded and
    /// fault-injectable. Each call makes a *new, empty* store; to share one
    /// store across contexts, build the [`SpyLayer`] yourself and clone its `Arc`.
    pub fn context(&self) -> Context<Arc<SpyLayer<InMemoryDataLayer>>> {
        Context::new(Arc::new(
            SpyLayer::new(InMemoryDataLayer::new(), self.effects.clone())
                .with_faults(self.faults.clone()),
        ))
    }

    /// A fresh recording, fault-injectable layer — the same one
    /// [`context`](Sandbox::context) wraps, handed over directly so a scenario
    /// can share one store across contexts, or wrap it in a
    /// [`TransactionalSpyStore`].
    pub fn layer(&self) -> Arc<SpyLayer<InMemoryDataLayer>> {
        Arc::new(
            SpyLayer::new(InMemoryDataLayer::new(), self.effects.clone())
                .with_faults(self.faults.clone()),
        )
    }

    /// A context over a **transactional** recording store — the same recording
    /// and fault injection as [`context`](Sandbox::context), plus a real
    /// `begin`/`commit`/`rollback` the domain can run inside.
    ///
    /// Use this whenever the scenario touches the transactional path: rollback
    /// ordering, an `after_action` failure undoing a write, or the
    /// [`EventHandler::stage`] outbox pass —
    /// which the domain *refuses to run at all* against a store that declines
    /// transactions, so a plain [`context`](Sandbox::context) cannot reach it.
    ///
    /// Transaction lifecycle lands in the log as kind `"txn"`, and writes made
    /// inside one carry `in_txn: true`.
    pub fn transactional_context(
        &self,
    ) -> Context<TransactionalSpyStore<SpyLayer<InMemoryDataLayer>>> {
        Context::new(
            TransactionalSpyStore::over_spy(self.layer(), self.effects.clone())
                .with_faults(self.faults.clone()),
        )
    }

    /// Collect the domain's execution trace into [`effects`](Sandbox::effects):
    /// installs a [`RecordingSubscriber`] as this thread's default `tracing`
    /// subscriber, returning the guard — **hold it for the whole scenario**
    /// (`let _guard = sb.collect_tracing();`); collection stops when it drops.
    ///
    /// Spans land as kind `"trace.span"`, events as kind `"trace"` (the
    /// pipeline's events carry a `step` field — `"staged"`, `"authorized"`,
    /// `"denied"`, `"persisted"`, …), so trace assertions read like any other:
    /// `expect_one!(sb.effects, "trace", step: "denied")`.
    ///
    /// Two switches must be on for the domain to emit at all: the `trace`
    /// cargo feature (which also gates this method) and the runtime gate —
    /// call [`enable_tracing`](crate::Domain::enable_tracing) on the domain
    /// after building it.
    ///
    /// The guard is **thread-local**, which fits the sandbox's single-threaded
    /// scenario model (the same assumption seeded ids rest on). For a
    /// multi-threaded run, install the subscriber process-wide yourself:
    /// `tracing::subscriber::set_global_default(RecordingSubscriber::new(log))`.
    #[cfg(feature = "trace")]
    pub fn collect_tracing(&self) -> tracing::subscriber::DefaultGuard {
        tracing::subscriber::set_default(RecordingSubscriber::new(self.effects.clone()))
    }

    /// An audit backend recording into [`effects`](Sandbox::effects) — wire it
    /// into an [`AuditExtension`](crate::extension::audit::AuditExtension) and
    /// the paper trail joins the ordered effect log (kind `"audit"`):
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use ash_domain::extension::audit::AuditExtension;
    /// # use ash_domain::sandbox::Sandbox;
    /// let sb = Sandbox::new();
    /// let audit = Arc::new(AuditExtension::new(sb.audit_backend()));
    /// // DomainConfig { extensions: vec![audit], ..sb.config() }
    /// ```
    #[cfg(feature = "audit")]
    pub fn audit_backend(&self) -> Arc<RecordingAuditBackend> {
        Arc::new(RecordingAuditBackend::new(self.effects.clone()))
    }
}

impl Default for Sandbox {
    fn default() -> Self {
        Self::new()
    }
}

// ── Deterministic seams ──────────────────────────────────────────────────────

/// A [`Clock`] you advance by hand, so time in a scenario is fully controlled.
///
/// Starts at whatever millisecond you construct it with; [`set`](ManualClock::set)
/// jumps to an absolute time and [`advance`](ManualClock::advance) moves it
/// forward. It never moves backward on its own, so it exercises timestamp
/// monotonicity honestly.
#[derive(Debug)]
pub struct ManualClock {
    millis: AtomicI64,
}

impl ManualClock {
    /// A clock reading `start_millis` (milliseconds since the Unix epoch).
    pub fn new(start_millis: i64) -> Self {
        Self {
            millis: AtomicI64::new(start_millis),
        }
    }

    /// Jump to an absolute time (milliseconds since the Unix epoch).
    pub fn set(&self, millis: i64) {
        self.millis.store(millis, Ordering::SeqCst);
    }

    /// Move the clock forward by `delta_millis`.
    pub fn advance(&self, delta_millis: i64) {
        self.millis.fetch_add(delta_millis, Ordering::SeqCst);
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> i64 {
        self.millis.load(Ordering::SeqCst)
    }
}

/// An [`IdGenerator`] that mints reproducible UUID-string keys from a seed.
///
/// Each key is a UUID built from a seeded PRNG (same seed ⇒ same id sequence),
/// so a scenario reproduces the same ids given the same *order* of calls (a
/// single-threaded run guarantees the order). Unlike
/// [`DefaultIdGenerator`](crate::DefaultIdGenerator) (a `"{resource}-{n}"`
/// string), it emits UUIDs, but with no OS entropy.
pub struct SeededIdGenerator {
    rng: Mutex<u64>,
}

impl SeededIdGenerator {
    /// A generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            rng: Mutex::new(seed),
        }
    }

    /// Next value from the internal SplitMix64 PRNG.
    fn next_u64(&self) -> u64 {
        let mut state = self.rng.lock().expect("seeded rng mutex poisoned");
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl IdGenerator for SeededIdGenerator {
    fn next_id(&self, _resource: &str, _pk: &str) -> Value {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.next_u64().to_be_bytes());
        bytes[8..].copy_from_slice(&self.next_u64().to_be_bytes());
        Value::Str(uuid::Uuid::from_bytes(bytes).to_string())
    }
}

// ── Effects ──────────────────────────────────────────────────────────────────

/// A recorded call. `kind` is a label (the built-in recorders use `"write"`,
/// `"read"`, `"event"`, `"audit"`/`"audit.security"`, and — with the `trace`
/// feature — `"trace"`/`"trace.span"`; your own code may record any kind); `data`
/// carries the values passed and — for calls the recorders log — an `outcome`:
/// `"ok"`, `"error"` (the inner implementation failed), or `"fault"` (a
/// [`FaultPlan`] failed it on purpose), with the message under `"error"`.
#[derive(Clone, Debug)]
pub struct Effect {
    /// The call's category (e.g. `"write"`, `"event"`, `"email.sent"`).
    pub kind: String,
    /// The values passed, plus outcome detail.
    pub data: Record,
}

impl Effect {
    /// An effect of `kind` with no data yet.
    pub fn new(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            data: Record::new(),
        }
    }

    /// Attach a `key`/`value` detail (builder style).
    #[must_use]
    pub fn with(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.data.insert(key, value);
        self
    }

    /// A detail value by key.
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// A detail value as `&str`, if present and a string — the common match case.
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.data.get(key).and_then(Value::as_str)
    }

    /// The one-line ANSI form of this effect: kind in its taxonomy hue, keys
    /// dimmed, the `outcome` value in its result color. [`EffectLog::dump`]
    /// prints these when stdout is a terminal.
    fn line_colored(&self) -> String {
        let mut out = styled(kind_style(&self.kind), &self.kind);
        for (key, value) in self.data.iter() {
            let text = value_text(value);
            let text = if key == "outcome" {
                styled(outcome_style(value.as_str().unwrap_or("")), &text)
            } else {
                text
            };
            out.push_str(&format!(" {}{text}", styled("2", &format!("{key}="))));
        }
        out
    }
}

/// One plain line: the kind, then every field as `key=value` (alphabetical —
/// [`Record`] is ordered). This is the form the `expect_*` failure listings
/// use; [`EffectLog::dump`] prints the colorized variant on a terminal.
///
/// ```
/// use ash_domain::sandbox::Effect;
/// let e = Effect::new("write").with("op", "create").with("outcome", "ok");
/// assert_eq!(e.to_string(), r#"write op="create" outcome="ok""#);
/// ```
impl std::fmt::Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)?;
        for (key, value) in self.data.iter() {
            write!(f, " {key}={}", value_text(value))?;
        }
        Ok(())
    }
}

/// The compact text of a value on a log line: strings quoted, scalars bare,
/// structures rendered recursively (embeds are shallow by construction).
fn value_text(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Float(x) => x.to_string(),
        Value::Str(s) => format!("{s:?}"),
        Value::Bytes(_) => "<bytes>".to_string(),
        Value::Timestamp(t) => format!("{t}ms"),
        Value::Map(map) => {
            let inner: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", value_text(v)))
                .collect();
            format!("{{{}}}", inner.join(", "))
        }
        Value::List(items) => {
            let inner: Vec<String> = items.iter().map(value_text).collect();
            format!("[{}]", inner.join(", "))
        }
    }
}

/// `text` wrapped in ANSI style `code`.
fn styled(code: &str, text: &str) -> String {
    format!("\x1b[{code}m{text}\x1b[0m")
}

/// The stable hue for an effect kind: each built-in recorder's kind has its
/// own, and a custom kind — the ones *you* record — is bold green so your own
/// side effects stand out in the dump.
fn kind_style(kind: &str) -> &'static str {
    match kind {
        "write" => "1;33",                        // yellow
        "read" => "1;36",                         // cyan
        "event" => "1;35",                        // magenta
        _ if kind.starts_with("audit") => "1;34", // blue
        _ if kind.starts_with("trace") => "2",    // dim — diagnostics, not effects
        _ => "1;32",                              // bold green — your own kinds
    }
}

/// The color of an `outcome` value: green when it went through, yellow for a
/// scripted fault, red for a real failure.
fn outcome_style(outcome: &str) -> &'static str {
    match outcome {
        "ok" => "32",
        "fault" => "33",
        _ => "31",
    }
}

/// An ordered, shared record of the [`Effect`]s produced during a scenario.
///
/// Clone it freely — every clone points at the same underlying log — so one
/// handle can feed a [`SpyLayer`], a [`RecordingHandler`], your own recording
/// code, and your assertions. [`all`](EffectLog::all) hands you the raw ordered
/// calls to assert over yourself (order, values, anything); the `expect_*`
/// helpers cover the *was called / was not called* staples, take a label and a
/// plain predicate, return `Err` so they slot into the red→green loop, and
/// include the recorded calls in their failure message — the log shows you what
/// *did* happen when an expectation fails.
#[derive(Clone, Default)]
pub struct EffectLog {
    inner: Arc<Mutex<Vec<Effect>>>,
}

impl EffectLog {
    /// An empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append an effect. Recorders and your own code call this.
    pub fn record(&self, effect: Effect) {
        self.inner.lock().expect("effect log poisoned").push(effect);
    }

    /// A snapshot of every effect recorded so far, in order. The primary API:
    /// assert over this however you like.
    pub fn all(&self) -> Vec<Effect> {
        self.inner.lock().expect("effect log poisoned").clone()
    }

    /// Print the ordered log to stdout, one effect per line — **colorized**
    /// (kind by taxonomy, `outcome` by result) when stdout is a terminal and
    /// `NO_COLOR` is unset; plain otherwise, so piped/captured output stays
    /// clean. For one effect, or your own sink, use its `Display` form
    /// (`println!("{effect}")`) — always plain.
    ///
    /// ```
    /// # use ash_domain::sandbox::{Effect, EffectLog};
    /// let log = EffectLog::new();
    /// log.record(Effect::new("shipment.requested").with("order", "o-1"));
    /// log.dump(); // [0] shipment.requested order="o-1"
    /// ```
    pub fn dump(&self) {
        use std::io::IsTerminal;
        let color = std::env::var_os("NO_COLOR").is_none() && std::io::stdout().is_terminal();
        for (i, effect) in self.all().iter().enumerate() {
            if color {
                println!(
                    "  {} {}",
                    styled("2", &format!("[{i}]")),
                    effect.line_colored()
                );
            } else {
                println!("  [{i}] {effect}");
            }
        }
    }

    /// Drop every recorded effect (e.g. between phases of a scenario).
    pub fn clear(&self) {
        self.inner.lock().expect("effect log poisoned").clear();
    }

    /// Assert that **exactly one** effect matches `pred`, returning it. On
    /// failure the error names `label` and lists everything recorded.
    pub fn expect_one(&self, label: &str, pred: impl Fn(&Effect) -> bool) -> Result<Effect> {
        let all = self.all();
        let mut found: Vec<Effect> = all.iter().filter(|e| pred(e)).cloned().collect();
        match found.len() {
            1 => Ok(found.remove(0)),
            n => Err(Error::invalid(format!(
                "expected exactly one effect matching `{label}`, found {n}{}",
                render(&all)
            ))),
        }
    }

    /// Assert that **no** effect matches `pred`. On failure the error names
    /// `label` and lists everything recorded.
    pub fn expect_none(&self, label: &str, pred: impl Fn(&Effect) -> bool) -> Result<()> {
        let all = self.all();
        match all.iter().filter(|e| pred(e)).count() {
            0 => Ok(()),
            n => Err(Error::invalid(format!(
                "expected no effect matching `{label}`, found {n}{}",
                render(&all)
            ))),
        }
    }

    /// Assert that **exactly `n`** effects match `pred`. On failure the error
    /// names `label` and lists everything recorded.
    pub fn expect_count(
        &self,
        n: usize,
        label: &str,
        pred: impl Fn(&Effect) -> bool,
    ) -> Result<()> {
        let all = self.all();
        let got = all.iter().filter(|e| pred(e)).count();
        if got == n {
            Ok(())
        } else {
            Err(Error::invalid(format!(
                "expected {n} effects matching `{label}`, found {got}{}",
                render(&all)
            )))
        }
    }
}

/// Assert **exactly one** matching effect, returning it — sugar over
/// [`EffectLog::expect_one`] that derives the failure label for you.
///
/// Two forms:
///
/// ```ignore
/// // kind plus field equalities (values convert via `Value::from` —
/// // use e.g. `5i64` for integer fields):
/// expect_one!(sb.effects, "write", op: "update", outcome: "fault")?;
/// // any predicate, labelled with its own source text:
/// expect_one!(sb.effects, |e| e.get_str("order") == Some("o-1"))?;
/// ```
#[macro_export]
macro_rules! expect_one {
    ($log:expr, $kind:literal $(, $key:ident : $val:expr)* $(,)?) => {
        $log.expect_one(
            concat!($kind $(, " ", stringify!($key), ":", stringify!($val))*),
            |e| e.kind == $kind
                $(&& e.get(stringify!($key)) == Some(&$crate::Value::from($val)))*,
        )
    };
    ($log:expr, $pred:expr $(,)?) => {
        $log.expect_one(stringify!($pred), $pred)
    };
}

/// Assert **no** matching effect — sugar over [`EffectLog::expect_none`].
/// Takes the same two forms as [`expect_one!`].
#[macro_export]
macro_rules! expect_none {
    ($log:expr, $kind:literal $(, $key:ident : $val:expr)* $(,)?) => {
        $log.expect_none(
            concat!($kind $(, " ", stringify!($key), ":", stringify!($val))*),
            |e| e.kind == $kind
                $(&& e.get(stringify!($key)) == Some(&$crate::Value::from($val)))*,
        )
    };
    ($log:expr, $pred:expr $(,)?) => {
        $log.expect_none(stringify!($pred), $pred)
    };
}

/// Assert **exactly `n`** matching effects — sugar over
/// [`EffectLog::expect_count`]. Takes the same two forms as [`expect_one!`],
/// with the count first: `expect_count!(log, 2, "write", op: "create")`.
#[macro_export]
macro_rules! expect_count {
    ($log:expr, $n:expr, $kind:literal $(, $key:ident : $val:expr)* $(,)?) => {
        $log.expect_count(
            $n,
            concat!($kind $(, " ", stringify!($key), ":", stringify!($val))*),
            |e| e.kind == $kind
                $(&& e.get(stringify!($key)) == Some(&$crate::Value::from($val)))*,
        )
    };
    ($log:expr, $n:expr, $pred:expr $(,)?) => {
        $log.expect_count($n, stringify!($pred), $pred)
    };
}

/// The recorded calls, one per line, for failure messages.
fn render(effects: &[Effect]) -> String {
    if effects.is_empty() {
        return "; nothing was recorded".to_string();
    }
    let mut out = format!("; recorded {}:", effects.len());
    for (i, e) in effects.iter().enumerate() {
        out.push_str(&format!("\n  [{i}] {e}"));
    }
    out
}

// ── Faults ───────────────────────────────────────────────────────────────────

/// The user-coded fault decision: given the call about to run (as the [`Effect`]
/// that would be recorded for it), return `Some(error)` to fail it instead.
type FaultFn = Box<dyn FnMut(&Effect) -> Option<Error> + Send>;

/// A scriptable fault source, consulted by [`SpyLayer`] and
/// [`RecordingHandler`] before each call they guard.
///
/// **You code the fault** — [`set`](FaultPlan::set) installs a closure that sees
/// every candidate call and decides whether to fail it. The closure is `FnMut`,
/// so schedules like *fail once* / *fail the Nth* are plain captured state:
///
/// ```
/// # use ash_domain::sandbox::FaultPlan;
/// # use ash_domain::Error;
/// let faults = FaultPlan::new();
/// let mut remaining = 2;
/// faults.set(move |call| {
///     (remaining > 0 && call.kind == "write").then(|| {
///         remaining -= 1;
///         Error::data_layer("injected: db down")
///     })
/// });
/// ```
///
/// A faulted call is still **recorded** (outcome `"fault"`) — the log always
/// answers what was attempted. Clone the plan freely; clones share the closure.
/// With no closure set (or after [`clear`](FaultPlan::clear)) nothing faults.
/// Your own fakes can honour the same plan via [`decide`](FaultPlan::decide).
#[derive(Clone, Default)]
pub struct FaultPlan {
    decide: Arc<Mutex<Option<FaultFn>>>,
}

impl FaultPlan {
    /// A plan with no fault armed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the fault decision, replacing any previous one.
    pub fn set(&self, decide: impl FnMut(&Effect) -> Option<Error> + Send + 'static) {
        *self.decide.lock().expect("fault plan poisoned") = Some(Box::new(decide));
    }

    /// Disarm: subsequent calls run normally.
    pub fn clear(&self) {
        *self.decide.lock().expect("fault plan poisoned") = None;
    }

    /// Ask the installed decision (if any) whether to fail `call`.
    pub fn decide(&self, call: &Effect) -> Option<Error> {
        self.decide
            .lock()
            .expect("fault plan poisoned")
            .as_mut()
            .and_then(|f| f(call))
    }
}

// ── Recorders for the built-in effect seams ──────────────────────────────────

/// An [`EventHandler`] that records every [`DomainEvent`] into an [`EffectLog`]
/// instead of acting on it — and can fail handling on demand via a
/// [`FaultPlan`] (exercising the *post-persist failure* path).
///
/// Each call becomes an [`Effect`] of kind `"event"` carrying the values
/// passed — `resource`, `action`, `kind`, the affected `records` (as a list),
/// the commit time `at`, the `actor` when present — and an `outcome`.
pub struct RecordingHandler {
    log: EffectLog,
    faults: Option<FaultPlan>,
}

impl RecordingHandler {
    /// Record domain events into `log`.
    pub fn new(log: EffectLog) -> Self {
        Self { log, faults: None }
    }

    /// Consult `faults` before each event; a `Some(error)` decision fails the
    /// handling (recorded with outcome `"fault"`).
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = Some(faults);
        self
    }
}

#[async_trait]
impl EventHandler for RecordingHandler {
    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        let mut call = Effect::new("event")
            .with("resource", event.resource.clone())
            .with("action", event.action.clone())
            .with("kind", format!("{:?}", event.kind))
            .with("at", event.at)
            .with(
                "records",
                Value::List(event.records.iter().cloned().map(Value::from).collect()),
            );
        if let Some(actor) = &event.actor {
            call = call.with("actor", actor.clone());
        }
        if let Some(err) = self.faults.as_ref().and_then(|f| f.decide(&call)) {
            self.log
                .record(call.with("outcome", "fault").with("error", err.to_string()));
            return Err(err);
        }
        self.log.record(call.with("outcome", "ok"));
        Ok(())
    }
}

/// A [`DataLayer`] decorator that records every **call** — with the values
/// passed and its outcome — and can fail calls on demand via a [`FaultPlan`].
///
/// Writes (create/update/destroy) are always recorded as kind `"write"` with
/// `op`, `resource`, the payload (`data` for create, `changes` for update), the
/// `id`, and an `outcome` of `"ok"`, `"error"` (the inner layer failed), or
/// `"fault"` (the plan failed it). *A failed call is still a recorded call* —
/// the log answers "was this called?" regardless of how it went. Reads pass
/// through silently unless [`record_reads`](SpyLayer::record_reads) is on, but
/// the fault plan is consulted for them either way (kind `"read"`).
///
/// Wrap any layer: `Arc::new(SpyLayer::new(InMemoryDataLayer::new(), log))` is
/// a ready [`Store`](crate::Store).
pub struct SpyLayer<L> {
    inner: L,
    log: EffectLog,
    faults: Option<FaultPlan>,
    reads: bool,
    /// Set while a [`TransactionalSpyStore`] replays buffered writes on commit.
    /// Those writes were already recorded (with `in_txn: true`) when they were
    /// issued, so recording them again at the durable point would double-count
    /// every write in a transaction.
    replaying: Arc<std::sync::atomic::AtomicBool>,
}

impl<L> SpyLayer<L> {
    /// Wrap `inner`, recording its calls into `log`.
    pub fn new(inner: L, log: EffectLog) -> Self {
        Self {
            inner,
            log,
            faults: None,
            reads: false,
            replaying: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The replay flag a [`TransactionalSpyStore`] raises while applying a
    /// committed transaction's buffered writes, so they are not recorded twice.
    pub(crate) fn replay_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.replaying.clone()
    }

    /// Consult `faults` before each call; a `Some(error)` decision fails the
    /// call (recorded with outcome `"fault"`) without reaching the inner layer.
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = Some(faults);
        self
    }

    /// Also record reads (`read`/`get`/`aggregate`) as kind `"read"` effects.
    /// Off by default — reads are not side effects — but useful when a scenario
    /// asserts *nothing touched storage at all*.
    #[must_use]
    pub fn record_reads(mut self) -> Self {
        self.reads = true;
        self
    }

    fn decide(&self, call: &Effect) -> Option<Error> {
        self.faults.as_ref().and_then(|f| f.decide(call))
    }

    /// Run one guarded call: consult the fault plan, delegate, and record the
    /// call with its outcome (`record` is off only for reads with recording
    /// disabled).
    async fn guarded<T, F>(&self, call: Effect, record: bool, run: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>> + Send,
        T: Send,
    {
        // A replay of already-recorded transactional writes: run it, record
        // nothing. The call was logged when it was issued.
        let record = record && !self.replaying.load(Ordering::Relaxed);
        if let Some(err) = self.decide(&call) {
            if record {
                self.log
                    .record(call.with("outcome", "fault").with("error", err.to_string()));
            }
            return Err(err);
        }
        match run.await {
            Ok(out) => {
                if record {
                    self.log.record(call.with("outcome", "ok"));
                }
                Ok(out)
            }
            Err(err) => {
                if record {
                    self.log
                        .record(call.with("outcome", "error").with("error", err.to_string()));
                }
                Err(err)
            }
        }
    }
}

#[async_trait]
impl<L: DataLayer> DataLayer for SpyLayer<L> {
    async fn create(&self, resource: &str, pk: &str, record: Record) -> Result<Record> {
        let call = Effect::new("write")
            .with("op", "create")
            .with("resource", resource.to_string())
            // The values passed — including the generated primary key.
            .with("id", record.get(pk).cloned().unwrap_or(Value::Null))
            .with("data", record.clone());
        self.guarded(call, true, self.inner.create(resource, pk, record))
            .await
    }

    async fn read(&self, query: &Query) -> Result<Vec<Record>> {
        let call = Effect::new("read")
            .with("op", "read")
            .with("resource", query.resource.clone());
        self.guarded(call, self.reads, self.inner.read(query)).await
    }

    async fn create_many(
        &self,
        resource: &str,
        pk: &str,
        records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        // Recorded as one `create_many` effect, not N `create`s: the point of a
        // batch is that it is *one* call, and a scenario asserting "the batch
        // was persisted in one round trip" must be able to see that. `rows`
        // carries the count so a size assertion needs no digging.
        let call = Effect::new("write")
            .with("op", "create_many")
            .with("resource", resource.to_string())
            .with("rows", records.len() as i64)
            .with(
                "data",
                Value::List(records.iter().cloned().map(Value::from).collect()),
            );
        self.guarded(call, true, self.inner.create_many(resource, pk, records))
            .await
    }

    async fn update_versioned(
        &self,
        resource: &str,
        pk: &str,
        id: &Value,
        changes: &Record,
        version_attribute: &str,
        expected: &Value,
    ) -> Result<Record> {
        // A distinct op from `update`: a scenario must be able to assert that a
        // versioned resource took the *conditional* path, since silently taking
        // the unconditional one is precisely the lost-update bug the seam exists
        // to prevent.
        let call = Effect::new("write")
            .with("op", "update_versioned")
            .with("resource", resource.to_string())
            .with("id", id.clone())
            .with("changes", changes.clone())
            .with("version_attribute", version_attribute.to_string())
            .with("expected_version", expected.clone());
        self.guarded(
            call,
            true,
            self.inner
                .update_versioned(resource, pk, id, changes, version_attribute, expected),
        )
        .await
    }

    async fn get(&self, resource: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        let call = Effect::new("read")
            .with("op", "get")
            .with("resource", resource.to_string())
            .with("id", id.clone());
        self.guarded(call, self.reads, self.inner.get(resource, pk, id))
            .await
    }

    async fn update(
        &self,
        resource: &str,
        pk: &str,
        id: &Value,
        changes: &Record,
    ) -> Result<Record> {
        let call = Effect::new("write")
            .with("op", "update")
            .with("resource", resource.to_string())
            .with("id", id.clone())
            .with("changes", changes.clone());
        self.guarded(call, true, self.inner.update(resource, pk, id, changes))
            .await
    }

    async fn destroy(&self, resource: &str, pk: &str, id: &Value) -> Result<()> {
        let call = Effect::new("write")
            .with("op", "destroy")
            .with("resource", resource.to_string())
            .with("id", id.clone());
        self.guarded(call, true, self.inner.destroy(resource, pk, id))
            .await
    }

    async fn aggregate(
        &self,
        destination: &str,
        destination_attribute: &str,
        keys: &[Value],
        aggregate: &Aggregate,
    ) -> Result<Option<HashMap<String, Value>>> {
        let call = Effect::new("read")
            .with("op", "aggregate")
            .with("resource", destination.to_string());
        self.guarded(
            call,
            self.reads,
            self.inner
                .aggregate(destination, destination_attribute, keys, aggregate),
        )
        .await
    }
}

/// A [`Store`](crate::Store) that offers a **recorded, fault-injectable
/// transaction** — the sandbox's answer to "did this write actually run inside a
/// transaction, and what happened when the commit failed?"
///
/// A plain `Arc<SpyLayer<_>>` is already a `Store`, but it *declines*
/// transactions (`begin` returns `None`, the blanket default), so a scenario
/// using one can never reach the domain's transactional path — the rollback
/// ordering, or the [`EventHandler::stage`]
/// outbox pass, which the domain refuses to run at all without a transaction to
/// join. Wrap the same layer in this and that whole path opens up.
///
/// Every lifecycle call is recorded as kind `"txn"` — `begin`, `commit`,
/// `rollback` — so an ordering assertion reads like any other effect
/// assertion, and the [`FaultPlan`] is consulted for each, so "the commit
/// fails" is a scripted scenario rather than a thought experiment.
///
/// The transaction is **buffered**: writes issued through it are held and
/// applied to the inner layer on `commit`, and dropped on `rollback`. That is
/// what makes a rolled-back write genuinely invisible afterwards, which is the
/// property the domain's rollback path depends on — and what
/// [`Conformance::check_store`](crate::datalayer::conformance::Conformance::check_store)
/// verifies.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use ash_domain::sandbox::{Sandbox, TransactionalSpyStore};
/// # use ash_domain::Error;
/// let sb = Sandbox::new();
/// let store = TransactionalSpyStore::new(sb.layer(), sb.effects.clone())
///     .with_faults(sb.faults.clone());
/// // Fail the commit and assert the write did not survive:
/// sb.faults.set(|call| {
///     (call.get_str("op") == Some("commit")).then(|| Error::data_layer("commit failed"))
/// });
/// # let _ = store;
/// ```
pub struct TransactionalSpyStore<L> {
    layer: Arc<L>,
    log: EffectLog,
    faults: Option<FaultPlan>,
    /// Raised while applying a committed transaction's buffered writes, so the
    /// wrapped [`SpyLayer`] does not record them a second time. `None` when the
    /// wrapped layer is not a `SpyLayer` and therefore records nothing anyway.
    replay: Option<Arc<std::sync::atomic::AtomicBool>>,
}

impl<L> TransactionalSpyStore<L> {
    /// Offer transactions over `layer`, recording their lifecycle into `log`.
    ///
    /// Use [`over_spy`](TransactionalSpyStore::over_spy) when `layer` is a
    /// [`SpyLayer`] — it suppresses the duplicate recording that would otherwise
    /// occur when buffered writes are applied at commit.
    pub fn new(layer: Arc<L>, log: EffectLog) -> Self {
        Self {
            layer,
            log,
            faults: None,
            replay: None,
        }
    }

    /// Consult `faults` before `begin`/`commit`/`rollback`; a `Some(error)`
    /// decision fails that step (recorded with outcome `"fault"`).
    #[must_use]
    pub fn with_faults(mut self, faults: FaultPlan) -> Self {
        self.faults = Some(faults);
        self
    }

    fn decide(&self, call: &Effect) -> Option<Error> {
        self.faults.as_ref().and_then(|f| f.decide(call))
    }
}

impl<L> TransactionalSpyStore<SpyLayer<L>> {
    /// Offer transactions over a [`SpyLayer`], recording the transaction
    /// lifecycle into `log` — and suppressing the layer's own recording while a
    /// commit applies buffered writes, so each write appears **once**, at the
    /// point it was issued.
    pub fn over_spy(layer: Arc<SpyLayer<L>>, log: EffectLog) -> Self {
        let replay = Some(layer.replay_flag());
        Self {
            layer,
            log,
            faults: None,
            replay,
        }
    }
}

#[async_trait]
impl<L: DataLayer + 'static> crate::context::Store for TransactionalSpyStore<L> {
    fn layer(&self) -> &dyn DataLayer {
        &*self.layer
    }

    async fn begin(&self) -> Result<Option<Box<dyn crate::datalayer::Transaction>>> {
        let call = Effect::new("txn").with("op", "begin");
        if let Some(err) = self.decide(&call) {
            self.log
                .record(call.with("outcome", "fault").with("error", err.to_string()));
            return Err(err);
        }
        self.log.record(call.with("outcome", "ok"));
        Ok(Some(Box::new(SpyTransaction {
            layer: self.layer.clone() as Arc<dyn DataLayer>,
            log: self.log.clone(),
            faults: self.faults.clone(),
            replay: self.replay.clone(),
            buffered: Mutex::new(Vec::new()),
        })))
    }
}

/// One buffered write held by a [`SpyTransaction`] until commit.
enum Staged {
    Create {
        resource: String,
        pk: String,
        record: Record,
    },
    Update {
        resource: String,
        pk: String,
        id: Value,
        changes: Record,
    },
    Destroy {
        resource: String,
        pk: String,
        id: Value,
    },
}

/// The transaction [`TransactionalSpyStore`] hands out: buffers writes, records
/// its lifecycle, and honours the [`FaultPlan`] on commit and rollback.
struct SpyTransaction {
    layer: Arc<dyn DataLayer>,
    log: EffectLog,
    faults: Option<FaultPlan>,
    replay: Option<Arc<std::sync::atomic::AtomicBool>>,
    buffered: Mutex<Vec<Staged>>,
}

impl SpyTransaction {
    fn decide(&self, call: &Effect) -> Option<Error> {
        self.faults.as_ref().and_then(|f| f.decide(call))
    }

    fn stage(&self, op: Staged) {
        self.buffered
            .lock()
            .expect("spy transaction poisoned")
            .push(op);
    }
}

#[async_trait]
impl DataLayer for SpyTransaction {
    async fn create(&self, resource: &str, pk: &str, record: Record) -> Result<Record> {
        self.log.record(
            Effect::new("write")
                .with("op", "create")
                .with("resource", resource.to_string())
                .with("in_txn", true)
                .with("id", record.get(pk).cloned().unwrap_or(Value::Null))
                .with("data", record.clone())
                .with("outcome", "ok"),
        );
        self.stage(Staged::Create {
            resource: resource.to_string(),
            pk: pk.to_string(),
            record: record.clone(),
        });
        // Echo the row as a layer would, without making it durable yet.
        Ok(record)
    }

    async fn read(&self, query: &Query) -> Result<Vec<Record>> {
        // Reads see the underlying layer: buffered writes are not yet visible,
        // which is exactly the isolation a real transaction's reader would have
        // from *outside* it, and keeps the fake honest about what it does not do.
        self.layer.read(query).await
    }

    async fn get(&self, resource: &str, pk: &str, id: &Value) -> Result<Option<Record>> {
        self.layer.get(resource, pk, id).await
    }

    async fn update(
        &self,
        resource: &str,
        pk: &str,
        id: &Value,
        changes: &Record,
    ) -> Result<Record> {
        self.log.record(
            Effect::new("write")
                .with("op", "update")
                .with("resource", resource.to_string())
                .with("in_txn", true)
                .with("id", id.clone())
                .with("changes", changes.clone())
                .with("outcome", "ok"),
        );
        self.stage(Staged::Update {
            resource: resource.to_string(),
            pk: pk.to_string(),
            id: id.clone(),
            changes: changes.clone(),
        });
        // The merged row a caller expects back: prior state plus the changes.
        let mut merged = self.layer.get(resource, pk, id).await?.unwrap_or_default();
        for (field, value) in changes.iter() {
            merged.insert(field.clone(), value.clone());
        }
        Ok(merged)
    }

    async fn destroy(&self, resource: &str, pk: &str, id: &Value) -> Result<()> {
        self.log.record(
            Effect::new("write")
                .with("op", "destroy")
                .with("resource", resource.to_string())
                .with("in_txn", true)
                .with("id", id.clone())
                .with("outcome", "ok"),
        );
        self.stage(Staged::Destroy {
            resource: resource.to_string(),
            pk: pk.to_string(),
            id: id.clone(),
        });
        Ok(())
    }
}

#[async_trait]
impl crate::datalayer::Transaction for SpyTransaction {
    async fn commit(self: Box<Self>) -> Result<()> {
        let call = Effect::new("txn").with("op", "commit");
        if let Some(err) = self.decide(&call) {
            // A failed commit discards the buffer: the unit of work did not take
            // effect, which is the contract the domain relies on.
            self.log
                .record(call.with("outcome", "fault").with("error", err.to_string()));
            return Err(err);
        }
        let staged = std::mem::take(&mut *self.buffered.lock().expect("spy transaction poisoned"));
        let count = staged.len();
        // Suppress the wrapped layer's recording for the replay: these writes
        // were logged when they were issued, and logging them again at the
        // durable point would double-count every write in the transaction.
        if let Some(flag) = &self.replay {
            flag.store(true, Ordering::Relaxed);
        }
        let applied = self.apply(staged).await;
        if let Some(flag) = &self.replay {
            flag.store(false, Ordering::Relaxed);
        }
        applied?;
        self.log
            .record(call.with("outcome", "ok").with("applied", count as i64));
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<()> {
        let call = Effect::new("txn").with("op", "rollback");
        let discarded = self
            .buffered
            .lock()
            .expect("spy transaction poisoned")
            .len();
        if let Some(err) = self.decide(&call) {
            self.log
                .record(call.with("outcome", "fault").with("error", err.to_string()));
            return Err(err);
        }
        // Dropping the buffer *is* the rollback — nothing reached the layer.
        self.buffered
            .lock()
            .expect("spy transaction poisoned")
            .clear();
        self.log.record(
            call.with("outcome", "ok")
                .with("discarded", discarded as i64),
        );
        Ok(())
    }
}

impl SpyTransaction {
    /// Apply the buffered writes in issue order — the durable point.
    async fn apply(&self, staged: Vec<Staged>) -> Result<()> {
        for op in staged {
            match op {
                Staged::Create {
                    resource,
                    pk,
                    record,
                } => {
                    self.layer.create(&resource, &pk, record).await?;
                }
                Staged::Update {
                    resource,
                    pk,
                    id,
                    changes,
                } => {
                    self.layer.update(&resource, &pk, &id, &changes).await?;
                }
                Staged::Destroy { resource, pk, id } => {
                    self.layer.destroy(&resource, &pk, &id).await?;
                }
            }
        }
        Ok(())
    }
}

// ── Collectors for the trace and the audit log ───────────────────────────────

/// An `AuditBackend` (`ash_log::AuditBackend`) that records every audit event
/// into an [`EffectLog`] instead of persisting it.
///
/// Wire it into an [`AuditExtension`](crate::extension::audit::AuditExtension)
/// — via [`Sandbox::audit_backend`] or by hand — and the paper trail joins the
/// ordered effect log. Each audit event becomes an [`Effect`] of kind
/// `"audit"` carrying `event_type` and `result` in their `Debug` form (e.g.
/// `"MethodInvocation"`, `"Success"`, `"Denied"`), `method`, and — when
/// present — `principal`, `error`, and the `resource`/`action` metadata the
/// extension attaches. Raw security events land as kind `"audit.security"`
/// with the JSON text under `"json"`.
///
/// ```
/// use ash_domain::ash_log::{AuditBackend, AuditEvent, AuditEventType, AuditResult};
/// use ash_domain::sandbox::{EffectLog, RecordingAuditBackend};
/// # fn main() -> ash_domain::Result<()> {
/// let log = EffectLog::new();
/// let backend = RecordingAuditBackend::new(log.clone());
/// backend.log_audit(
///     &AuditEvent::builder()
///         .event_type(AuditEventType::MethodInvocation)
///         .method("todo.create")
///         .result(AuditResult::Success)
///         .build(),
/// );
/// ash_domain::expect_one!(log, "audit", method: "todo.create", result: "Success")?;
/// # Ok(()) }
/// ```
#[cfg(feature = "audit")]
pub struct RecordingAuditBackend {
    log: EffectLog,
}

#[cfg(feature = "audit")]
impl RecordingAuditBackend {
    /// Record audit events into `log`.
    pub fn new(log: EffectLog) -> Self {
        Self { log }
    }
}

#[cfg(feature = "audit")]
impl ash_log::AuditBackend for RecordingAuditBackend {
    fn log_audit(&self, event: &ash_log::AuditEvent) {
        let mut call = Effect::new("audit")
            .with("event_type", format!("{:?}", event.event_type))
            .with("result", format!("{:?}", event.result));
        if let Some(method) = &event.method {
            call = call.with("method", method.clone());
        }
        if let Some(principal) = &event.principal {
            call = call.with("principal", principal.clone());
        }
        if let Some(error) = &event.error {
            call = call.with("error", error.clone());
        }
        // The audit extension files the resource/action under metadata — lift
        // them out so `expect_*` matches read the same as the other recorders.
        for key in ["resource", "action"] {
            if let Some(serde_json::Value::String(s)) = event.metadata.get(key) {
                call = call.with(key, s.clone());
            }
        }
        self.log.record(call);
    }

    fn security_log(&self, event: &serde_json::Value) {
        self.log
            .record(Effect::new("audit.security").with("json", event.to_string()));
    }
}

/// A `tracing` subscriber that records the domain's execution trace into an
/// [`EffectLog`] — every span as kind `"trace.span"` (its name plus its
/// declared fields), every event as kind `"trace"` (its fields, plus `span` =
/// the name of the enclosing entered span, when there is one). Numeric fields
/// keep their type (`u64` beyond `i64::MAX` falls back to its text form);
/// everything else lands as its `Debug`/`Display` text.
///
/// Install it however the scenario needs: [`Sandbox::collect_tracing`] sets it
/// as the current thread's default subscriber for the guard's lifetime; for a
/// multi-threaded run, `tracing::subscriber::set_global_default` installs it
/// process-wide. Remember the domain's own two switches — the `trace` cargo
/// feature and [`enable_tracing`](crate::Domain::enable_tracing).
///
/// The enclosing-span bookkeeping assumes spans enter and exit on one thread
/// (the sandbox's single-threaded scenario model); events from other threads
/// are still recorded, just without a `span` label from this thread's stack.
#[cfg(feature = "trace")]
pub struct RecordingSubscriber {
    log: EffectLog,
    /// The next span id to mint; `tracing::span::Id` is non-zero, so it starts at 1.
    next_span_id: AtomicU64,
    /// Live span id → span name, for labelling events with their enclosing span.
    span_names: Mutex<HashMap<u64, &'static str>>,
    /// The stack of entered span ids on the scenario thread.
    entered: Mutex<Vec<u64>>,
}

#[cfg(feature = "trace")]
impl RecordingSubscriber {
    /// Record spans and events into `log`.
    pub fn new(log: EffectLog) -> Self {
        Self {
            log,
            next_span_id: AtomicU64::new(1),
            span_names: Mutex::new(HashMap::new()),
            entered: Mutex::new(Vec::new()),
        }
    }

    /// The name of the innermost entered span, if any.
    fn current_span_name(&self) -> Option<&'static str> {
        let entered = self.entered.lock().expect("entered-span stack poisoned");
        let id = entered.last()?;
        self.span_names
            .lock()
            .expect("span-name map poisoned")
            .get(id)
            .copied()
    }
}

/// Flattens a `tracing` value set into an [`Effect`]'s data record.
#[cfg(feature = "trace")]
struct RecordFields<'a>(&'a mut Record);

#[cfg(feature = "trace")]
impl tracing::field::Visit for RecordFields<'_> {
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.0.insert(field.name(), value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        // No silent truncation: a count beyond i64 keeps its full text form.
        match i64::try_from(value) {
            Ok(v) => self.0.insert(field.name(), v),
            Err(_) => self.0.insert(field.name(), value.to_string()),
        }
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.0.insert(field.name(), value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.0.insert(field.name(), value);
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value);
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

#[cfg(feature = "trace")]
impl tracing::Subscriber for RecordingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let id = self.next_span_id.fetch_add(1, Ordering::Relaxed);
        let name = span.metadata().name();
        let mut effect = Effect::new("trace.span").with("name", name);
        span.record(&mut RecordFields(&mut effect.data));
        self.span_names
            .lock()
            .expect("span-name map poisoned")
            .insert(id, name);
        self.log.record(effect);
        tracing::span::Id::from_u64(id)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {
        // The executor declares every span field up front; late field values
        // never occur on the instrumented paths, so there is nothing to merge.
    }

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut effect = Effect::new("trace");
        if let Some(span) = self.current_span_name() {
            effect = effect.with("span", span);
        }
        event.record(&mut RecordFields(&mut effect.data));
        self.log.record(effect);
    }

    fn enter(&self, span: &tracing::span::Id) {
        self.entered
            .lock()
            .expect("entered-span stack poisoned")
            .push(span.into_u64());
    }

    fn exit(&self, span: &tracing::span::Id) {
        let popped = self
            .entered
            .lock()
            .expect("entered-span stack poisoned")
            .pop();
        debug_assert_eq!(
            popped,
            Some(span.into_u64()),
            "spans exit in LIFO order on a single-threaded scenario run"
        );
    }

    fn try_close(&self, id: tracing::span::Id) -> bool {
        // Drop the name binding so the map stays bounded by *live* spans.
        self.span_names
            .lock()
            .expect("span-name map poisoned")
            .remove(&id.into_u64());
        true
    }
}
