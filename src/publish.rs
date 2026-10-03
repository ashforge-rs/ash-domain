//! Consumer-side transports for carrying a domain event to the outside world: a
//! [`Notification`], a [`Publisher`] transport, and a [`JobQueue`] for deferred
//! work.
//!
//! Two directions meet here, and the vocabulary keeps them apart. The domain
//! **emits** a fact — it hands a [`DomainEvent`] to
//! its [`EventHandler`](crate::event::EventHandler)s (see
//! [`Emitter`](crate::event::Emitter)). A consumer then **publishes** it —
//! projects the event to a routing shape and carries it onto some transport.
//! `emit` is the domain→others verb; `publish` is the others→outside-world verb.
//!
//! These are **seams, not engines**, and they sit *downstream* of the
//! [`DomainEvent`] the [`Domain`](crate::Domain)
//! produces. The domain does not know about them; a consumer's
//! [`EventHandler`](crate::event::EventHandler) chooses to turn an emitted event
//! into one of these. The core ships no message broker or job runner — you
//! extend `ash-domain` by implementing a trait on your own type, over your own
//! infrastructure, with nothing about the transport or runtime baked in.
//!
//! * A [`Notification`] is one **projection** of a domain event: the routing
//!   shape a consumer publishes. Build it with
//!   [`DomainEvent::to_notification`](crate::event::DomainEvent::to_notification).
//!   A domain event is *not* a notification — it becomes one only if a consumer
//!   chooses.
//! * A [`Publisher`] is a **transport** — topic + bytes. A consumer's handler
//!   typically bridges the two: project the domain event to a notification,
//!   then to a topic + payload, and publish. The core keeps them separate so a
//!   publisher can also carry traffic that isn't a domain event.
//! * A [`JobQueue`] defers work to run **later / elsewhere**. A `Change`, a
//!   generic handler, or an [`EventHandler`](crate::event::EventHandler)
//!   enqueues a [`Job`]; the queue's worker side is entirely the consumer's.

use async_trait::async_trait;

use crate::action::ActionKind;
use crate::error::Result;
use crate::event::DomainEvent;
use crate::value::{Record, Value};

/// A publishable projection of a [`DomainEvent`].
///
/// This is **not** the domain event itself — it is one shape a consumer derives
/// from an event when they choose to broadcast it, carrying just what a
/// subscriber needs to route and render: the resource and action names, the
/// [`ActionKind`], the acting principal, the tenant, and the affected
/// record(s). Build it with
/// [`DomainEvent::to_notification`](crate::event::DomainEvent::to_notification),
/// or construct one directly for traffic that has no originating event.
#[derive(Clone, Debug)]
pub struct Notification {
    /// The resource the action ran on.
    pub resource: String,
    /// The action name.
    pub action: String,
    /// The action kind.
    pub kind: ActionKind,
    /// The acting principal, if one was set on the context.
    pub actor: Option<Record>,
    /// The tenant the action was scoped to, if any.
    pub tenant: Option<Value>,
    /// The record(s) the action produced or affected: the created/updated row,
    /// the destroyed row's prior state, or empty for a generic action with no
    /// record result.
    pub records: Vec<Record>,
}

impl Notification {
    /// A notification for a single-record action.
    pub fn new(
        resource: impl Into<String>,
        action: impl Into<String>,
        kind: ActionKind,
        record: Record,
    ) -> Self {
        Self {
            resource: resource.into(),
            action: action.into(),
            kind,
            actor: None,
            tenant: None,
            records: vec![record],
        }
    }
}

/// Project a domain event into a publishable notification, dropping the commit
/// timestamp (a routing concern, not a transport one — a consumer that needs it
/// reads it off the [`DomainEvent`] instead).
impl From<&DomainEvent> for Notification {
    fn from(event: &DomainEvent) -> Self {
        Self {
            resource: event.resource.clone(),
            action: event.action.clone(),
            kind: event.kind,
            actor: event.actor.clone(),
            tenant: event.tenant.clone(),
            records: event.records.clone(),
        }
    }
}

/// A message the [`Domain`](crate::Domain) never sends itself but that an
/// [`EventHandler`](crate::event::EventHandler) can broadcast: a topic plus an
/// opaque payload.
///
/// This is the transport seam — deliberately dumb. The core assigns no meaning
/// to the topic string or the payload bytes; a consumer's implementation carries
/// them to Redis, NATS, Postgres `LISTEN/NOTIFY`, an in-process bus, or a
/// WebSocket hub.
#[async_trait]
pub trait Publisher: Send + Sync {
    /// Publish `payload` to `topic`. The payload is opaque bytes so any encoding
    /// (JSON, protobuf, bincode) is the consumer's choice.
    async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()>;
}

/// A unit of deferred work: a named job with a payload, to run later or
/// elsewhere.
///
/// The core defines the envelope; the payload is an opaque [`Record`] so a job
/// carries whatever structured input its worker needs, in the framework's own
/// neutral representation.
#[derive(Clone, Debug)]
pub struct Job {
    /// The job kind — the worker dispatches on this.
    pub kind: String,
    /// The job's input.
    pub payload: Record,
    /// An optional delay in milliseconds before the job becomes runnable. `0`
    /// (the default) means "as soon as possible"; a queue that cannot schedule
    /// may ignore it. Set it relative with [`after`](Job::after) or from an
    /// absolute [`Clock`](crate::Clock) instant with [`at`](Job::at).
    pub delay_ms: u64,
}

impl Job {
    /// A job of `kind` carrying `payload`, runnable immediately.
    pub fn new(kind: impl Into<String>, payload: Record) -> Self {
        Self {
            kind: kind.into(),
            payload,
            delay_ms: 0,
        }
    }

    /// The same job, deferred by `delay_ms` milliseconds.
    pub fn after(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }

    /// The same job, scheduled to become runnable at an **absolute** instant —
    /// `target_millis` since the Unix epoch — measured against `clock`.
    ///
    /// This is the absolute-time companion to [`after`](Job::after): rather than
    /// making the caller compute `target - now` by hand, the delay is derived from
    /// the same [`Clock`](crate::Clock) seam the rest of the domain reads, so it is
    /// deterministic under a test clock. A target already in the past (or now)
    /// yields `delay_ms == 0` — runnable as soon as possible, never a negative or
    /// wrapped delay.
    ///
    /// Like [`after`](Job::after), this only sets the delay on the envelope; a
    /// [`JobQueue`] that cannot schedule may still ignore it (its contract).
    ///
    /// ```
    /// use ash_domain::publish::Job;
    /// use ash_domain::{Clock, Record};
    ///
    /// struct FixedClock(i64);
    /// impl Clock for FixedClock {
    ///     fn now_millis(&self) -> i64 { self.0 }
    /// }
    ///
    /// let clock = FixedClock(1_000);
    /// // 1500ms in the future → 500ms delay.
    /// let soon = Job::new("reap", Record::new()).at(1_500, &clock);
    /// assert_eq!(soon.delay_ms, 500);
    /// // A target in the past is clamped to "run now".
    /// let overdue = Job::new("reap", Record::new()).at(200, &clock);
    /// assert_eq!(overdue.delay_ms, 0);
    /// ```
    pub fn at(mut self, target_millis: i64, clock: &dyn crate::clock::Clock) -> Self {
        let now = clock.now_millis();
        // Clamp a past/now target to 0 rather than wrapping into a huge u64.
        self.delay_ms = target_millis.saturating_sub(now).max(0) as u64;
        self
    }
}

/// Enqueues [`Job`]s for asynchronous execution — the crate's seam for
/// **deferred and scheduled work**.
///
/// The enqueue side is the seam; the **worker side — polling, running,
/// retrying, dead-lettering — is entirely the consumer's**, as is the backing
/// store (an in-memory channel, Redis, Postgres, SQS, …). A `Change`, a generic
/// handler, or an [`EventHandler`](crate::event::EventHandler) enqueues; nothing
/// in the core drains the queue.
///
/// This is deliberate: the core ships **no scheduler runtime**. "Run this action
/// later / at an instant" is expressed by enqueuing a [`Job`] with a delay
/// ([`Job::after`] / [`Job::at`], the latter derived from the domain's
/// [`Clock`](crate::Clock)); *when* and *how* it then runs — the timer wheel, the
/// at-least-once redelivery, the dedup of a retried job — belong to the queue and
/// its worker, not to `ash-domain`. Keeping the runtime out is what lets the same
/// enqueue seam sit over an in-process channel or a durable broker unchanged.
///
/// ```
/// use ash_domain::publish::{Job, JobQueue};
/// use ash_domain::{Record, Result};
///
/// struct Enqueued(std::sync::Mutex<Vec<Job>>);
///
/// #[async_trait::async_trait]
/// impl JobQueue for Enqueued {
///     async fn enqueue(&self, job: Job) -> Result<()> {
///         self.0.lock().unwrap().push(job);
///         Ok(())
///     }
/// }
///
/// # async fn demo(q: &Enqueued) -> Result<()> {
/// q.enqueue(Job::new("send_welcome_email", Record::from_iter([("to", "a@b.c")]))).await?;
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait JobQueue: Send + Sync {
    /// Enqueue `job`. Returns once the job is durably accepted by the queue (what
    /// "durable" means is the implementation's contract).
    async fn enqueue(&self, job: Job) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;

    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_millis(&self) -> i64 {
            self.0
        }
    }

    #[test]
    fn at_future_target_sets_the_remaining_delay() {
        let clock = FixedClock(1_000);
        let job = Job::new("reap", Record::new()).at(1_500, &clock);
        assert_eq!(job.delay_ms, 500);
    }

    #[test]
    fn at_exact_now_is_runnable_immediately() {
        let clock = FixedClock(1_000);
        let job = Job::new("reap", Record::new()).at(1_000, &clock);
        assert_eq!(job.delay_ms, 0);
    }

    #[test]
    fn at_past_target_is_clamped_to_zero_not_wrapped() {
        // A target before `now` must clamp to 0, never wrap into a huge u64.
        let clock = FixedClock(1_000);
        let job = Job::new("reap", Record::new()).at(200, &clock);
        assert_eq!(job.delay_ms, 0);
    }

    #[test]
    fn at_handles_negative_now_without_underflow() {
        // Defensive: a clock reading before the epoch still yields a sane delay.
        let clock = FixedClock(-500);
        let job = Job::new("reap", Record::new()).at(500, &clock);
        assert_eq!(job.delay_ms, 1_000);
    }
}
