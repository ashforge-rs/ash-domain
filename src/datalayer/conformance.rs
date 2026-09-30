//! A reusable **conformance check** for [`DataLayer`] implementations.
//!
//! The [`DataLayer`] contract has load-bearing points that are stated in prose
//! but exercised nowhere a layer author can reach: the reserved key-set param
//! ([`reserved::IN`](crate::query::reserved::IN)) **must** be honoured — on the
//! primary key *and* on plain attributes — or every relationship load silently
//! breaks; [`Query::limit`] **must** bound what comes back, or the domain's row
//! ceiling turns into a wall of errors; a bare read returns the resource's rows;
//! an empty key set matches nothing; update merges and persists; destroy is
//! idempotent. This module
//! turns those sentences into a runnable check: point a [`Conformance`] at a
//! **scratch resource** your layer can store and run
//! [`check`](Conformance::check) inside your own test — the returned
//! [`ConformanceReport`] names exactly which contract points held and which
//! failed.
//!
//! The check **writes to the layer** (it creates, updates, and destroys a
//! fixed handful of rows whose primary keys are prefixed `__ash-conf-`), and
//! its key-set assertions are exact, so give it an **empty scratch resource**
//! — for a SQL layer, an empty table with a string primary key and one string
//! column (named `label` unless renamed via
//! [`attribute`](Conformance::attribute)). It is a test-support facility, not
//! a runtime one.
//!
//! [`check_store`](Conformance::check_store) is the second half, for a
//! [`Store`]: transactions live there, not on [`DataLayer`], and declining them
//! is legal — a store whose [`begin`](Store::begin) returns `None` is
//! conformant and the report names that rather than failing.
//!
//! What it deliberately does **not** check: tenancy and any layer-private param
//! convention (those are yours — the core attaches no semantics to them).
//! Tenancy is not an omission that could be fixed here: under the
//! [`Attribute`](crate::TenantStrategy::Attribute) strategy the discriminator is
//! an ordinary param the domain folds in, and under
//! [`Layer`](crate::TenantStrategy::Layer) the write path passes no tenant at
//! all — a partitioning store is bound to its tenant. So there is no way to
//! *seed* two tenants' rows through this trait, and a check that cannot seed
//! cannot assert isolation. Test partitioning against your own store, where the
//! binding is visible.
//!
//! ```
//! use ash_domain::datalayer::conformance::Conformance;
//! use ash_domain::datalayer::memory::InMemoryDataLayer;
//!
//! let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
//! rt.block_on(async {
//!     let layer = InMemoryDataLayer::new();
//!     let report = Conformance::new("scratch", "id").check(&layer).await;
//!     assert!(report.is_conformant(), "{report}");
//! });
//! ```

use std::collections::BTreeSet;
use std::fmt;

use super::{DataLayer, Transaction};
use crate::context::Store;
use crate::error::Error;
use crate::query::{Cursor, Query};
use crate::value::{Record, Value, value_key};

/// The fixed primary keys the check seeds — prefixed so they cannot collide
/// with real data even if the scratch resource is shared.
const IDS: [&str; 3] = ["__ash-conf-a", "__ash-conf-b", "__ash-conf-c"];
/// The seeded attribute value per row, index-matched to [`IDS`]. Two rows
/// share `"one"` so the attribute key-set check has a multi-row match.
const LABELS: [&str; 3] = ["one", "one", "two"];
/// The primary keys the batch-create check seeds, distinct from [`IDS`] so the
/// key-set assertions above stay exact.
const BATCH_IDS: [&str; 2] = ["__ash-conf-x", "__ash-conf-y"];
/// The primary keys the transaction checks use, distinct from every other set.
const TXN_IDS: [&str; 2] = ["__ash-conf-txn-a", "__ash-conf-txn-b"];
/// The attribute the conditional-write check uses as a row version. Prefixed
/// like the ids so it cannot collide with a real column.
const VERSION_ATTR: &str = "__ash_conf_version";

/// A conformance run over one scratch resource: which resource to write into,
/// its primary-key attribute, and the name of the one string attribute the
/// seeded rows carry.
pub struct Conformance {
    resource: String,
    pk: String,
    attribute: String,
}

/// One contract point that failed, and how.
#[derive(Debug)]
pub struct ConformanceFailure {
    /// The contract point, e.g. `"key-set-on-primary-key"`.
    pub check: &'static str,
    /// What was expected vs what the layer did.
    pub detail: String,
}

/// The outcome of [`Conformance::check`]: which contract points held and which
/// failed. [`Display`](fmt::Display) renders a summary with one line per
/// failure, so `assert!(report.is_conformant(), "{report}")` shows exactly
/// what broke.
#[derive(Debug, Default)]
pub struct ConformanceReport {
    passed: Vec<&'static str>,
    failures: Vec<ConformanceFailure>,
}

impl ConformanceReport {
    /// Whether every exercised contract point held.
    pub fn is_conformant(&self) -> bool {
        self.failures.is_empty()
    }

    /// The contract points that held, in the order they ran.
    pub fn passed(&self) -> &[&'static str] {
        &self.passed
    }

    /// The contract points that failed, with detail.
    pub fn failures(&self) -> &[ConformanceFailure] {
        &self.failures
    }

    fn record(&mut self, check: &'static str, result: Result<(), String>) {
        match result {
            Ok(()) => self.passed.push(check),
            Err(detail) => self.failures.push(ConformanceFailure { check, detail }),
        }
    }
}

impl fmt::Display for ConformanceReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "DataLayer conformance: {} passed, {} failed",
            self.passed.len(),
            self.failures.len()
        )?;
        for failure in &self.failures {
            writeln!(f, "  FAIL {}: {}", failure.check, failure.detail)?;
        }
        Ok(())
    }
}

impl Conformance {
    /// A check against `resource`, whose primary-key attribute is `pk`. The
    /// seeded rows also carry one string attribute, named `label` unless
    /// renamed with [`attribute`](Conformance::attribute).
    pub fn new(resource: impl Into<String>, pk: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            pk: pk.into(),
            attribute: "label".into(),
        }
    }

    /// Rename the seeded string attribute (default `"label"`), for a layer
    /// whose scratch schema calls its column something else.
    pub fn attribute(mut self, name: impl Into<String>) -> Self {
        self.attribute = name.into();
        self
    }

    /// Run every check against `layer`, returning the report. Never panics —
    /// assert on [`is_conformant`](ConformanceReport::is_conformant) yourself
    /// so the report's detail reaches your test output. Seeded rows are
    /// removed (best-effort) before returning.
    pub async fn check(&self, layer: &dyn DataLayer) -> ConformanceReport {
        let mut report = ConformanceReport::default();
        // Everything depends on the seed rows; a failed create ends the run.
        if self.seed(layer, &mut report).await {
            self.check_get(layer, &mut report).await;
            self.check_bare_read(layer, &mut report).await;
            self.check_row_bound(layer, &mut report).await;
            self.check_sort(layer, &mut report).await;
            self.check_conditional_write(layer, &mut report).await;
            self.check_key_sets(layer, &mut report).await;
            self.check_absent_get(layer, &mut report).await;
            self.check_batch_create(layer, &mut report).await;
            self.check_update(layer, &mut report).await;
            self.check_destroy(layer, &mut report).await;
        }
        self.cleanup(layer).await;
        report
    }

    /// Run the **transaction** checks against a [`Store`], returning their own
    /// report.
    ///
    /// Separate from [`check`](Conformance::check) because transactions live on
    /// [`Store`], not [`DataLayer`], and because declining them is legal: a
    /// store whose [`begin`](Store::begin) returns `None` is conformant, and
    /// the report says so (`transactions-declined`) instead of failing.
    ///
    /// What a store that *does* offer one must uphold:
    ///
    /// * a committed write is visible through the bare layer afterwards;
    /// * a rolled-back write is not — this is the guarantee the whole seam
    ///   exists for, and the one the domain relies on to undo a write whose
    ///   `after_action` or event staging failed;
    /// * an uncommitted write is not visible through the bare layer, since a
    ///   write that is already durable cannot be rolled back.
    ///
    /// Writes to the same scratch resource as [`check`](Conformance::check),
    /// under its own `__ash-conf-txn-` keys, and cleans up after itself.
    ///
    /// ```
    /// use ash_domain::datalayer::conformance::Conformance;
    /// use ash_domain::datalayer::memory::InMemoryDataLayer;
    /// use std::sync::Arc;
    ///
    /// let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    /// rt.block_on(async {
    ///     // The in-memory layer declines transactions — conformant, and named.
    ///     let store = Arc::new(InMemoryDataLayer::new());
    ///     let report = Conformance::new("scratch", "id").check_store(&store).await;
    ///     assert!(report.is_conformant(), "{report}");
    ///     assert_eq!(report.passed(), ["transactions-declined"]);
    /// });
    /// ```
    pub async fn check_store(&self, store: &dyn Store) -> ConformanceReport {
        let mut report = ConformanceReport::default();
        match store.begin().await {
            Ok(None) => {
                report.record("transactions-declined", Ok(()));
                return report;
            }
            Ok(Some(txn)) => {
                // Nothing to assert about this probe transaction itself; drop it
                // by rolling back so the checks below start from a clean state.
                let _ = txn.rollback().await;
            }
            Err(e) => {
                report.record("transactions-declined", Err(format!("begin failed: {e}")));
                return report;
            }
        }
        self.check_rollback(store, &mut report).await;
        self.check_commit(store, &mut report).await;
        self.cleanup_txn(store.layer()).await;
        report
    }

    /// A rolled-back write leaves nothing behind — and was not visible while it
    /// was open.
    async fn check_rollback(&self, store: &dyn Store, report: &mut ConformanceReport) {
        let id = TXN_IDS[0];
        let Some(txn) = self.begin(store, report, "rollback-undoes-the-write").await else {
            return;
        };
        if let Err(e) = txn.create(&self.resource, &self.pk, self.txn_row(id)).await {
            report.record(
                "rollback-undoes-the-write",
                Err(format!("create inside the transaction failed: {e}")),
            );
            let _ = txn.rollback().await;
            return;
        }
        // Before the commit the row must not be durable — a write already
        // visible outside the transaction is a write that cannot be undone.
        let leaked = matches!(
            store
                .layer()
                .get(&self.resource, &self.pk, &Value::from(id))
                .await,
            Ok(Some(_))
        );
        let rolled_back = txn.rollback().await;
        let result = match (leaked, rolled_back) {
            (true, _) => Err("an uncommitted write was visible outside the transaction".into()),
            (false, Err(e)) => Err(format!("rollback failed: {e}")),
            (false, Ok(())) => match store
                .layer()
                .get(&self.resource, &self.pk, &Value::from(id))
                .await
            {
                Ok(None) => Ok(()),
                Ok(Some(row)) => Err(format!("the rolled-back row is still readable: {row:?}")),
                Err(e) => Err(format!("get after rollback failed: {e}")),
            },
        };
        report.record("rollback-undoes-the-write", result);
    }

    /// A committed write is visible through the bare layer afterwards.
    async fn check_commit(&self, store: &dyn Store, report: &mut ConformanceReport) {
        let id = TXN_IDS[1];
        let Some(txn) = self
            .begin(store, report, "commit-makes-the-write-durable")
            .await
        else {
            return;
        };
        if let Err(e) = txn.create(&self.resource, &self.pk, self.txn_row(id)).await {
            report.record(
                "commit-makes-the-write-durable",
                Err(format!("create inside the transaction failed: {e}")),
            );
            let _ = txn.rollback().await;
            return;
        }
        let result = match txn.commit().await {
            Err(e) => Err(format!("commit failed: {e}")),
            Ok(()) => match store
                .layer()
                .get(&self.resource, &self.pk, &Value::from(id))
                .await
            {
                Ok(Some(_)) => Ok(()),
                Ok(None) => Err("the committed row is not readable".into()),
                Err(e) => Err(format!("get after commit failed: {e}")),
            },
        };
        report.record("commit-makes-the-write-durable", result);
    }

    /// Open a transaction, recording a failure under `check` if the store
    /// suddenly declines or errors mid-run.
    async fn begin(
        &self,
        store: &dyn Store,
        report: &mut ConformanceReport,
        check: &'static str,
    ) -> Option<Box<dyn Transaction>> {
        match store.begin().await {
            Ok(Some(txn)) => Some(txn),
            Ok(None) => {
                report.record(
                    check,
                    Err("begin returned None after offering a transaction".into()),
                );
                None
            }
            Err(e) => {
                report.record(check, Err(format!("begin failed: {e}")));
                None
            }
        }
    }

    fn txn_row(&self, id: &str) -> Record {
        let mut row = Record::new();
        row.insert(self.pk.as_str(), id);
        row.insert(self.attribute.as_str(), "txn");
        row
    }

    async fn cleanup_txn(&self, layer: &dyn DataLayer) {
        for id in TXN_IDS {
            let _ = layer
                .destroy(&self.resource, &self.pk, &Value::from(id))
                .await;
        }
    }

    /// Create the three seed rows; each created row must echo its primary key.
    async fn seed(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) -> bool {
        let mut result = Ok(());
        for (id, label) in IDS.iter().zip(LABELS) {
            let mut row = Record::new();
            row.insert(self.pk.as_str(), *id);
            row.insert(self.attribute.as_str(), label);
            match layer.create(&self.resource, &self.pk, row).await {
                Ok(created) if created.get(&self.pk) == Some(&Value::from(*id)) => {}
                Ok(created) => {
                    result = Err(format!(
                        "created row does not echo its primary key `{id}`: {created:?}"
                    ));
                    break;
                }
                Err(e) => {
                    result = Err(format!("create failed: {e}"));
                    break;
                }
            }
        }
        let ok = result.is_ok();
        report.record("create-returns-row", result);
        ok
    }

    /// A created primary key reads back with its stored attributes.
    async fn check_get(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let result = match layer
            .get(&self.resource, &self.pk, &Value::from(IDS[0]))
            .await
        {
            Ok(Some(row)) if row.get(&self.attribute) == Some(&Value::from(LABELS[0])) => Ok(()),
            Ok(Some(row)) => Err(format!(
                "row came back without its stored attributes: {row:?}"
            )),
            Ok(None) => Err("get on a created primary key returned None".to_string()),
            Err(e) => Err(format!("get failed: {e}")),
        };
        report.record("get-round-trip", result);
    }

    /// A primary key that was never created reads back as `Ok(None)`, not an
    /// error: "no such row" is a normal answer, and the domain relies on it to
    /// tell a missing row from a broken layer.
    async fn check_absent_get(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let absent = Value::from("__ash-conf-absent");
        let result = match layer.get(&self.resource, &self.pk, &absent).await {
            Ok(None) => Ok(()),
            Ok(Some(row)) => Err(format!("get on an uncreated primary key returned {row:?}")),
            Err(e) => Err(format!("get on an uncreated primary key errored: {e}")),
        };
        report.record("absent-get-is-none", result);
    }

    /// [`DataLayer::create_many`] returns **one row per input row, in order**.
    ///
    /// The default implementation loops over `create`, so a layer that does not
    /// override it passes for free; a layer that does override it (a multi-row
    /// `INSERT`) is where order and count are easy to get wrong — and the domain
    /// zips the returned rows against the staged changesets positionally.
    async fn check_batch_create(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let rows: Vec<Record> = BATCH_IDS
            .iter()
            .map(|id| {
                let mut row = Record::new();
                row.insert(self.pk.as_str(), *id);
                row.insert(self.attribute.as_str(), "batch");
                row
            })
            .collect();
        let result = match layer.create_many(&self.resource, &self.pk, rows).await {
            Ok(created) if created.len() != BATCH_IDS.len() => Err(format!(
                "create_many returned {} rows for {} inputs",
                created.len(),
                BATCH_IDS.len()
            )),
            Ok(created) => {
                let got: Vec<Option<&Value>> = created.iter().map(|r| r.get(&self.pk)).collect();
                let want: Vec<Value> = BATCH_IDS.iter().map(|id| Value::from(*id)).collect();
                if got.iter().zip(&want).all(|(g, w)| *g == Some(w)) {
                    Ok(())
                } else {
                    Err(format!(
                        "create_many returned rows out of order: {got:?} (wanted {want:?})"
                    ))
                }
            }
            Err(e) => Err(format!("create_many failed: {e}")),
        };
        report.record("batch-create-round-trip", result);
    }

    /// [`Query::limit`] is honoured: a read bounded to one row returns at most
    /// one.
    ///
    /// This is a contract point, not a hint. The domain bounds every read that
    /// carries no limit of its own, and **refuses** a result set that overruns
    /// the bound rather than truncating it — so a layer that ignores the field
    /// turns ordinary reads into errors as soon as a table grows past the
    /// domain's ceiling.
    async fn check_row_bound(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let bounded = Query::new(self.resource.as_str()).limit(1);
        let result = match layer.read(&bounded).await {
            Ok(rows) if rows.len() <= 1 => Ok(()),
            Ok(rows) => Err(format!(
                "read bounded to 1 row returned {} — Query::limit was ignored",
                rows.len()
            )),
            Err(e) => Err(format!("bounded read failed: {e}")),
        };
        report.record("honours-row-limit", result);
    }

    /// The **conditional-write contract**: `update_versioned` applies the change
    /// only when the stored version still matches, and reports
    /// [`Error::Conflict`](crate::Error::Conflict) when it does not.
    ///
    /// A layer that declines with
    /// [`Error::Unsupported`](crate::Error::Unsupported) is **conformant** and
    /// named as such (`conditional-writes-declined`) — declining is legal, and
    /// the domain refuses to serve a versioned resource from such a layer rather
    /// than degrading. What is *not* legal is accepting the call and ignoring the
    /// version, which is the case this check catches.
    async fn check_conditional_write(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let id = Value::from(IDS[0]);
        // Seed a version onto the scratch row, through the ordinary update.
        let mut seed = Record::new();
        seed.insert(VERSION_ATTR, Value::Int(1));
        if let Err(e) = layer.update(&self.resource, &self.pk, &id, &seed).await {
            report.record(
                "honours-conditional-write",
                Err(format!(
                    "could not seed a version onto the scratch row: {e}"
                )),
            );
            return;
        }

        // A matching version must apply the change.
        let mut change = Record::new();
        change.insert(VERSION_ATTR, Value::Int(2));
        let matched = layer
            .update_versioned(
                &self.resource,
                &self.pk,
                &id,
                &change,
                VERSION_ATTR,
                &Value::Int(1),
            )
            .await;
        match matched {
            Err(Error::Unsupported(_)) => {
                report.record("conditional-writes-declined", Ok(()));
                return;
            }
            Err(e) => {
                report.record(
                    "honours-conditional-write",
                    Err(format!("a matching-version update failed: {e}")),
                );
                return;
            }
            Ok(row) if row.get(VERSION_ATTR) != Some(&Value::Int(2)) => {
                report.record(
                    "honours-conditional-write",
                    Err(format!(
                        "a matching-version update did not apply the change: {row:?}"
                    )),
                );
                return;
            }
            Ok(_) => {}
        }
        report.record("honours-conditional-write", Ok(()));

        // A stale version must be refused — the lost-update guarantee itself.
        let mut clobber = Record::new();
        clobber.insert(VERSION_ATTR, Value::Int(99));
        let stale = layer
            .update_versioned(
                &self.resource,
                &self.pk,
                &id,
                &clobber,
                VERSION_ATTR,
                &Value::Int(1),
            )
            .await;
        let result = match stale {
            Err(Error::Conflict { .. }) => Ok(()),
            Err(e) => Err(format!(
                "a stale-version update must fail with Error::Conflict, got: {e}"
            )),
            Ok(row) => Err(format!(
                "a stale-version update was applied instead of refused: {row:?} — this is the \
                 lost update the seam exists to prevent"
            )),
        };
        report.record("refuses-a-stale-conditional-write", result);
    }

    /// The **sort contract**: rows come back in the requested order, and both
    /// paging models resume from it — a cursor picks up strictly after a
    /// position, an offset skips a count of rows before the row bound applies.
    ///
    /// Only the seeded rows are considered — the scratch resource may hold
    /// others — so this checks the *relative* order of the three ids it wrote,
    /// which is what a sort must preserve regardless of what else is stored.
    async fn check_sort(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        // Ascending on the primary key: IDS is already in lexicographic order.
        let asc = Query::new(self.resource.as_str()).sort_asc(self.pk.as_str());
        let ascending = match layer.read(&asc).await {
            Ok(rows) => {
                let seeded = seeded_ids_in_order(&rows, &self.pk);
                if seeded == IDS {
                    Ok(())
                } else {
                    Err(format!(
                        "ascending sort on `{}` returned the seeded rows as {seeded:?}, want {IDS:?}",
                        self.pk
                    ))
                }
            }
            Err(e) => Err(format!(
                "sorted read failed: {e} (a layer that cannot sort \
                                   must return Error::Unsupported, which is also a failure \
                                   here — declare the capability or implement it)"
            )),
        };
        report.record("honours-sort-order", ascending);

        // Descending must be the exact reverse.
        let desc = Query::new(self.resource.as_str()).sort_desc(self.pk.as_str());
        let mut want_desc = IDS;
        want_desc.reverse();
        let descending = match layer.read(&desc).await {
            Ok(rows) => {
                let seeded = seeded_ids_in_order(&rows, &self.pk);
                if seeded == want_desc {
                    Ok(())
                } else {
                    Err(format!(
                        "descending sort on `{}` returned the seeded rows as {seeded:?}, want {want_desc:?}",
                        self.pk
                    ))
                }
            }
            Err(e) => Err(format!("descending sorted read failed: {e}")),
        };
        report.record("honours-sort-direction", descending);

        // A cursor at the first seeded id must exclude it and everything before
        // it — the keyset-resume guarantee paging is built on.
        let resumed = Query::new(self.resource.as_str())
            .sort_asc(self.pk.as_str())
            .after(Cursor::from_keys(vec![Value::from(IDS[0])]));
        let cursor = match layer.read(&resumed).await {
            Ok(rows) => {
                let seeded = seeded_ids_in_order(&rows, &self.pk);
                if seeded == IDS[1..] {
                    Ok(())
                } else {
                    Err(format!(
                        "resuming after `{}` returned the seeded rows as {seeded:?}, want {:?} \
                         (a cursor is exclusive: the row it names belongs to the previous page)",
                        IDS[0],
                        &IDS[1..]
                    ))
                }
            }
            Err(e) => Err(format!("cursor read failed: {e}")),
        };
        report.record("honours-resume-cursor", cursor);

        // An offset skips rows of the requested order, so offsetting past the
        // first seeded row must drop exactly it — the same boundary the cursor
        // above lands on, reached by counting instead of by key. Only the
        // *relative* order of the seeded rows is checked, so a scratch resource
        // holding other rows would make the counted offset land elsewhere;
        // filter to the seeded key-set first so both models see the same set.
        let offset_keys: Vec<Value> = IDS.iter().map(|id| Value::from(*id)).collect();
        let offset = Query::key_set(self.resource.as_str(), self.pk.as_str(), offset_keys)
            .sort_asc(self.pk.as_str())
            .offset(1);
        let offset_result = match layer.read(&offset).await {
            Ok(rows) => {
                let seeded = seeded_ids_in_order(&rows, &self.pk);
                if seeded == IDS[1..] {
                    Ok(())
                } else {
                    Err(format!(
                        "offsetting a sorted read by 1 returned the seeded rows as {seeded:?}, \
                         want {:?} (an offset skips that many rows of the requested order)",
                        &IDS[1..]
                    ))
                }
            }
            Err(e) => Err(format!(
                "offset read failed: {e} (a layer that cannot offset must return \
                 Error::Unsupported, which is also a failure here — implement the skip \
                 or document that offset paging is unavailable)"
            )),
        };
        report.record("honours-offset", offset_result);

        // The skip happens *before* the row bound: offset 1 with limit 1 is the
        // second row, not the first. A layer that truncates before skipping
        // would return the first row here, or nothing at all.
        let offset_keys: Vec<Value> = IDS.iter().map(|id| Value::from(*id)).collect();
        let paged = Query::key_set(self.resource.as_str(), self.pk.as_str(), offset_keys)
            .sort_asc(self.pk.as_str())
            .offset(1)
            .limit(1);
        let paged_result = match layer.read(&paged).await {
            Ok(rows) => {
                let seeded = seeded_ids_in_order(&rows, &self.pk);
                if seeded == IDS[1..2] {
                    Ok(())
                } else {
                    Err(format!(
                        "an offset-1 limit-1 read returned the seeded rows as {seeded:?}, want \
                         {:?} (the offset is applied to the order first, and the row bound \
                         then bounds what is left)",
                        &IDS[1..2]
                    ))
                }
            }
            Err(e) => Err(format!("offset-with-limit read failed: {e}")),
        };
        report.record("honours-offset-before-limit", paged_result);
    }

    /// An empty param bag returns the resource's rows (at least the seeded ones).
    async fn check_bare_read(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let result = match layer.read(&Query::new(self.resource.as_str())).await {
            Ok(rows) => {
                let got = id_set(&rows, &self.pk);
                let missing: Vec<&str> = IDS
                    .iter()
                    .copied()
                    .filter(|id| !got.contains(*id))
                    .collect();
                if missing.is_empty() {
                    Ok(())
                } else {
                    Err(format!(
                        "an empty param bag must return the resource's rows; missing {missing:?}"
                    ))
                }
            }
            Err(e) => Err(format!("read failed: {e}")),
        };
        report.record("bare-read-returns-rows", result);
    }

    /// The reserved key-set param, in the three shapes relationship loading
    /// depends on: on the primary key, on a plain attribute, and empty.
    async fn check_key_sets(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        // On the primary key: how the executor fetches related rows by id.
        let query = Query::key_set(
            self.resource.as_str(),
            self.pk.as_str(),
            vec![Value::from(IDS[0]), Value::from(IDS[2])],
        );
        report.record(
            "key-set-on-primary-key",
            self.expect_ids(layer, &query, &[IDS[0], IDS[2]]).await,
        );

        // On a plain attribute: loads filter on the relationship's destination
        // attribute, which is usually not the primary key.
        let query = Query::key_set(
            self.resource.as_str(),
            self.attribute.as_str(),
            vec![Value::from(LABELS[0])],
        );
        report.record(
            "key-set-on-attribute",
            self.expect_ids(layer, &query, &[IDS[0], IDS[1]]).await,
        );

        // Empty matches nothing — not everything.
        let query = Query::key_set(self.resource.as_str(), self.pk.as_str(), Vec::new());
        report.record(
            "key-set-empty-matches-nothing",
            self.expect_ids(layer, &query, &[]).await,
        );
    }

    /// Update returns the merged row (primary key intact) and persists it.
    async fn check_update(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let id = Value::from(IDS[2]);
        let mut changes = Record::new();
        changes.insert(self.attribute.as_str(), "updated");
        let updated = Value::from("updated");
        let result = match layer.update(&self.resource, &self.pk, &id, &changes).await {
            Ok(row) if row.get(&self.attribute) != Some(&updated) => Err(format!(
                "updated row does not carry the merged change: {row:?}"
            )),
            Ok(row) if row.get(&self.pk) != Some(&id) => {
                Err(format!("updated row lost its primary key: {row:?}"))
            }
            Ok(_) => match layer.get(&self.resource, &self.pk, &id).await {
                Ok(Some(row)) if row.get(&self.attribute) == Some(&updated) => Ok(()),
                Ok(Some(row)) => Err(format!("update did not persist: {row:?}")),
                Ok(None) => Err("row vanished after update".to_string()),
                Err(e) => Err(format!("get after update failed: {e}")),
            },
            Err(e) => Err(format!("update failed: {e}")),
        };
        report.record("update-merges-and-persists", result);
    }

    /// Destroy removes the row and is idempotent, as the trait documents.
    async fn check_destroy(&self, layer: &dyn DataLayer, report: &mut ConformanceReport) {
        let id = Value::from(IDS[0]);
        let result = match layer.destroy(&self.resource, &self.pk, &id).await {
            Ok(()) => match layer.get(&self.resource, &self.pk, &id).await {
                Ok(None) => match layer.destroy(&self.resource, &self.pk, &id).await {
                    Ok(()) => Ok(()),
                    Err(e) => Err(format!(
                        "destroy is documented idempotent; the second call failed: {e}"
                    )),
                },
                Ok(Some(_)) => Err("row still readable after destroy".to_string()),
                Err(e) => Err(format!("get after destroy failed: {e}")),
            },
            Err(e) => Err(format!("destroy failed: {e}")),
        };
        report.record("destroy-removes-idempotently", result);
    }

    /// Read `query` and require exactly the rows whose primary keys are
    /// `expected` — order-free, but no extras and no omissions.
    async fn expect_ids(
        &self,
        layer: &dyn DataLayer,
        query: &Query,
        expected: &[&str],
    ) -> Result<(), String> {
        match layer.read(query).await {
            Ok(rows) => {
                let got = id_set(&rows, &self.pk);
                let want: BTreeSet<String> = expected.iter().map(|s| s.to_string()).collect();
                if got == want {
                    Ok(())
                } else {
                    Err(format!("expected rows {want:?}, got {got:?}"))
                }
            }
            Err(e) => Err(format!("read failed: {e}")),
        }
    }

    /// Best-effort removal of every seeded row; failures here are not part of
    /// the contract (the layer may already have failed hard).
    async fn cleanup(&self, layer: &dyn DataLayer) {
        for id in IDS.iter().chain(BATCH_IDS.iter()) {
            let _ = layer
                .destroy(&self.resource, &self.pk, &Value::from(*id))
                .await;
        }
    }
}

/// The primary-key strings of `rows`, as a set — for order-free comparison.
/// The seeded ids among `rows`, in the order the layer returned them — the
/// projection the sort checks compare against. Rows the scratch resource may
/// hold from elsewhere are ignored: a sort must order the seeded rows correctly
/// relative to one another, whatever else is interleaved.
fn seeded_ids_in_order(rows: &[Record], pk: &str) -> Vec<String> {
    rows.iter()
        .filter_map(|r| r.get(pk).map(value_key))
        .filter(|id| IDS.contains(&id.as_str()))
        .collect()
}

fn id_set(rows: &[Record], pk: &str) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|r| r.get(pk).map(value_key))
        .collect()
}
