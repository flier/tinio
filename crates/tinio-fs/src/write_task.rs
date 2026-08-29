//! The DB write-pipeline batch task (pipeline-spec.md §3.1, task 2).
//!
//! [`MetaWriteBatchTask`] commits one producer batch through
//! [`meta::Store::set_batch`] — a single write transaction for the whole
//! batch (per-entry last-write-wins), so the write-transaction count
//! drops from N to ≈N/batch. Completion is the [`pipeline::Completion`]
//! `enqueue` returns (Q3b/R8): the list producer awaits it; the scanner
//! drops it (fire-and-forget).

use async_trait::async_trait;
use tinio_core::{bucket, pipeline};

use crate::meta;

/// A unit of DB write-pipeline work: one batch upsert, one write
/// transaction (pipeline-spec.md §3.2, Q3b, R8).
///
/// Data-only: completion is the [`pipeline::Completion`] `enqueue`
/// returns (list awaits it; scanner drops it).
///
/// Constructed by the task-4 producers (list/scanner); the unit tests
/// construct it directly.
pub(crate) struct MetaWriteBatchTask {
    /// The meta store (a clone of the shared handle).
    pub meta: meta::Store,
    /// The batch's bucket.
    pub bucket: bucket::Name,
    /// The batch entries.
    pub entries: Vec<meta::BatchEntry>,
}

#[async_trait]
impl pipeline::Task for MetaWriteBatchTask {
    type Output = Result<(), crate::Error>;

    fn kind(&self) -> &'static str {
        "meta_write"
    }

    async fn run(&mut self) -> Self::Output {
        // The owned form: the task's batch is moved into the 'static
        // write closure, not cloned (data-path review 2026-08-29,
        // finding 2).
        self.meta
            .set_batch_owned(&self.bucket, std::mem::take(&mut self.entries))
            .await
    }
}

// `pipeline::Outcome` for `Result<(), crate::Error>` comes from the
// blanket `Result` impl in tinio-core pipeline.rs — the DB-pipeline
// runtime logs batch failures through it (R8) even when the producer
// dropped the handle (scanner fire-and-forget, Q3b), with the original
// error kept (P7).

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        time::{Duration, SystemTime},
    };

    use tinio_core::{
        bucket, object,
        pipeline::{InlineRunner, Runner, Task},
        to_nanos,
    };
    use tinio_util::testing::etag;

    use super::*;
    use crate::{Error, database};

    fn mtime(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn entries() -> Vec<meta::BatchEntry> {
        vec![
            meta::BatchEntry {
                key: object::key("a.txt").unwrap(),
                etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
                size: 1,
                mtime: mtime(1),
                identity: 11,
            },
            meta::BatchEntry {
                key: object::key("dir/b.txt").unwrap(),
                etag: etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                size: 2,
                mtime: mtime(2),
                identity: 22,
            },
        ]
    }

    fn task(store: meta::Store, entries: Vec<meta::BatchEntry>) -> MetaWriteBatchTask {
        MetaWriteBatchTask {
            meta: store,
            bucket: bucket::name("data").unwrap(),
            entries,
        }
    }

    /// A store whose `object_meta` table was created with incompatible
    /// types: every `set_batch` fails deterministically (redb
    /// `TableTypeMismatch` — a genuine write failure, otherwise
    /// untriggerable in tests).
    fn failing_store(state_dir: &Path) -> meta::Store {
        let db = redb::Database::create(state_dir.join("meta.redb")).unwrap();
        {
            let txn = db.begin_write().unwrap();
            txn.open_table::<(&str, &str), u64>(redb::TableDefinition::new("object_meta"))
                .unwrap();
            txn.commit().unwrap();
        }
        meta::Store::from_handle(database::Handle::new(db))
    }

    #[test]
    fn kind_is_meta_write() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let task = task(store, entries());
        assert_eq!(task.kind(), "meta_write");
    }

    #[test]
    fn writes_the_batch_through_the_runner() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let runner = InlineRunner::default();
        let verify = store.clone();
        crate::testutil::rt(async move {
            runner
                .enqueue(Box::new(task(store, entries())))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        });
        // The batch landed: one write transaction, every entry readable
        // back with its identity.
        let b = bucket::name("data").unwrap();
        let rows = crate::testutil::rt(async move {
            verify
                .load_entries(
                    &b,
                    [
                        object::key("a.txt").unwrap(),
                        object::key("dir/b.txt").unwrap(),
                    ]
                    .iter(),
                )
                .await
                .unwrap()
        });
        assert_eq!(rows.len(), 2);
        for (row, entry) in rows.iter().zip(&entries()) {
            let stored = row.as_ref().unwrap();
            assert_eq!(stored.etag, entry.etag);
            assert_eq!(stored.size, entry.size);
            assert_eq!(stored.mtime, to_nanos(entry.mtime));
            assert_eq!(stored.file_identity, entry.identity);
        }
    }

    #[test]
    fn batch_failure_is_reported_through_the_completion_once() {
        // enqueue Ok = accepted; the handle carries run()'s Err (R8).
        let state = tempfile::tempdir().unwrap();
        let store = failing_store(state.path());
        let runner = InlineRunner::default();
        let err = crate::testutil::rt(async move {
            runner
                .enqueue(Box::new(task(store, entries())))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap_err()
        });
        assert!(
            matches!(err, Error::Database(_)),
            "the completion must carry the original write error: {err}"
        );
        assert!(
            err.to_string().contains("table"),
            "Display must keep the original redb error: {err}"
        );
    }

    #[test]
    fn inline_runner_success_returns_ok() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let runner = InlineRunner::default();
        crate::testutil::rt(async move {
            runner
                .enqueue(Box::new(task(store, entries())))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        });
    }

    #[test]
    fn inline_runner_failure_returns_run_err() {
        // Scanner fire-and-forget drops the handle; inline tests await it
        // so the Err is visible (the concurrent runtime logs instead).
        let state = tempfile::tempdir().unwrap();
        let store = failing_store(state.path());
        let runner = InlineRunner::default();
        let err = crate::testutil::rt(async move {
            runner
                .enqueue(Box::new(task(store, entries())))
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap_err()
        });
        assert!(
            matches!(err, Error::Database(_)),
            "the completion must carry the original write error: {err}"
        );
    }
}
