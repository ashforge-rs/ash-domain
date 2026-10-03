//! End-to-end coverage for **orderly domain shutdown** — the `Closable` seam,
//! the close gate, and `Domain::close`.
//!
//! These assert *observable behavior*: a closing domain refuses new work with
//! `Closing` (and does so having touched nothing), and `close` drives every
//! registered `Closable` in reverse registration order, best-effort, even when
//! one of them fails.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use ash_domain::datalayer::memory::InMemoryDataLayer;
use ash_domain::{
    Closable, Context, Domain, DomainConfig, DomainContext, Error, PolicySet, Record, Resource,
    erase,
};

#[derive(Resource, Default, Debug)]
#[resource(name = "note")]
struct Note {
    #[attribute(primary_key)]
    id: String,
    title: String,
}

fn note_domain(dc: DomainContext) -> Domain {
    Domain::new(
        DomainConfig {
            resources: vec![erase::<Note>()],
            policies: PolicySet::permissive(),
            ..DomainConfig::default()
        },
        dc,
    )
}

/// A `Closable` that appends its label to a shared log when closed, so the test
/// can assert *order*. Optionally fails, to exercise best-effort aggregation.
#[derive(Clone)]
struct Recorder {
    label: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
    fail: bool,
}

#[async_trait::async_trait]
impl Closable for Recorder {
    async fn close(&self) -> ash_domain::Result<()> {
        self.log.lock().unwrap().push(self.label);
        if self.fail {
            Err(Error::data_layer(format!("{} failed", self.label)))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn closing_domain_refuses_new_actions_fail_closed() {
    let domain = note_domain(DomainContext::new());
    let mut ctx = Context::new(Arc::new(InMemoryDataLayer::new()));

    // Before close: a create works.
    Note::create(&domain, &mut ctx, Record::from_iter([("title", "a")]))
        .await
        .expect("create before close");

    domain.begin_close();
    assert!(domain.is_closing());

    // After the gate flips, a new action is refused with `Closing` — a lifecycle
    // signal, not a `Forbidden`/`Invalid`, and nothing was persisted.
    let err = Note::create(&domain, &mut ctx, Record::from_iter([("title", "b")]))
        .await
        .expect_err("create after close must fail");
    assert!(matches!(err, Error::Closing(r) if r == "note"));
}

#[tokio::test]
async fn close_drives_closables_in_reverse_registration_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let dc = DomainContext::new()
        .with_closable(Recorder {
            label: "first",
            log: log.clone(),
            fail: false,
        })
        .with_closable(Recorder {
            label: "second",
            log: log.clone(),
            fail: false,
        })
        .with_closable(Recorder {
            label: "third",
            log: log.clone(),
            fail: false,
        });

    let domain = note_domain(dc);
    domain.close().await.expect("close should succeed");

    // Reverse registration order: last registered, first torn down.
    assert_eq!(*log.lock().unwrap(), vec!["third", "second", "first"]);
    assert!(domain.is_closing(), "close also flips the gate");
}

#[tokio::test]
async fn close_is_best_effort_across_a_failing_closable() {
    let closed = Arc::new(AtomicUsize::new(0));
    let closed2 = closed.clone();

    #[derive(Clone)]
    struct Counting(Arc<AtomicUsize>, bool);
    #[async_trait::async_trait]
    impl Closable for Counting {
        async fn close(&self) -> ash_domain::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            if self.1 {
                Err(Error::data_layer("boom"))
            } else {
                Ok(())
            }
        }
    }

    let dc = DomainContext::new()
        .with_closable(Counting(closed.clone(), false)) // ok
        .with_closable(Counting(closed2, true)) // fails, closed first (reverse)
        .with_closable(Counting(closed.clone(), false)); // ok

    let domain = note_domain(dc);
    let err = domain
        .close()
        .await
        .expect_err("a failing closable surfaces");
    assert!(matches!(err, Error::DataLayer { .. }));

    // Every closable still ran despite the middle one failing.
    assert_eq!(closed.load(Ordering::SeqCst), 3);
}
