//! The cleanup scans over pg's own tables (PG7).
//!
//! The five statements under `sql/cleanup/` live in this crate because they read **this crate's**
//! tables — `content`, `content_heads`, `uploads`, `version_rows`, `current_slots` — which is
//! the same reason [`PgStore::live_len`](crate::PgStore::live_len) lives here: a statement's home
//! is the crate that owns the schema it reads. `tinio-pg`'s `Cleanup` keeps the **policy** —
//! which scans run, the thresholds, the dry-run shape, the live-set fold and the block sweep —
//! and calls these methods for the row facts.
//!
//! None of these is a contract operation: the four persistence contracts carry no cleanup
//! surface, so the scans are the store's own, exactly as `live_len` is. The two content scans
//! and the retention scan are reads; the two reclamations are removals, and the caller's
//! `dry_run` decides between them.

use std::time::Duration;

use tinio_core::bucket;

use crate::{Error, PgStore, content::ContentKind};

/// One orphan chunk generation: a `content` row older than the grace window that no live head
/// names and that is not the head's **current allocation** (so a publish in flight — including
/// one re-homing a generation a later allocation superseded — is never read as residue). The
/// row's own `content.published` stamp carries the age; the head gate protects the current
/// generation whose rows an old upload may already have aged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanChunk {
    /// The bucket the generation belongs to.
    pub bucket: bucket::Name,
    /// The content kind the generation's rows are stored under.
    pub kind: ContentKind,
    /// The encoded content key (PG20).
    pub ckey: String,
    /// The generation's sequence.
    pub seq: i64,
}

/// One staged part whose `uploads` row is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanPart {
    /// The bucket the part belongs to.
    pub bucket: bucket::Name,
    /// The encoded part content key (PG20).
    pub ckey: String,
}

/// One version row that is **history**, with the facts the retention rule reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    /// The bucket the key belongs to.
    pub bucket: bucket::Name,
    /// The key's wire (the contract `object::Key`'s encoding), validated by the caller that
    /// expiry addresses.
    pub key: String,
    /// The per-key sequence the row is stored at.
    pub seq: i64,
    /// When the version became noncurrent (`0` = no supersede stamped it).
    pub became_noncurrent: i64,
    /// When the version was created, in nanoseconds.
    pub created_nanos: i64,
}

/// The grace window in whole seconds, saturating at the `bigint` ceiling.
fn seconds(grace: Duration) -> i64 {
    i64::try_from(grace.as_secs()).unwrap_or(i64::MAX)
}

impl PgStore {
    /// The orphan chunk generations: `content` rows no live head names, older than `grace` by
    /// their own `published` stamp — and, since a publish commits its chunks before it moves the
    /// head's `live_seq`, the head's current allocation is protected for `grace` too, so a body
    /// still being staged (even one a later allocation superseded) is never read as residue.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// # async fn example(store: tinio_store_pg::PgStore) -> Result<(), tinio_store_pg::Error> {
    /// let orphans = store.orphan_chunks(Duration::from_secs(600)).await?;
    /// # let _ = orphans;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// (`ignore`: needs a live server, like every store call.)
    pub async fn orphan_chunks(&self, grace: Duration) -> Result<Vec<OrphanChunk>, Error> {
        let rows =
            sqlx::query_file_as!(OrphanChunk, "sql/cleanup/orphan_chunks.sql", seconds(grace))
                .fetch_all(self.pool())
                .await?;
        Ok(rows)
    }

    /// Remove the orphan chunk rows `rows` named — the keys the scan returned — answering how
    /// many rows went. An empty scan removes nothing.
    ///
    /// The delete takes the scan's keys rather than re-evaluating its predicate: the scan is the
    /// predicate's one home, so the two can never drift apart. A `(bucket, kind, ckey, seq)` a
    /// publish moved on in the meantime simply matches no row — the generation's chunks were
    /// re-keyed by that move, so its old sequence is gone whatever the sweep read.
    pub async fn reclaim_orphan_chunks(&self, rows: &[OrphanChunk]) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }
        let buckets: Vec<String> = rows.iter().map(|row| row.bucket.to_string()).collect();
        let kinds: Vec<String> = rows.iter().map(|row| row.kind.to_string()).collect();
        let ckeys: Vec<String> = rows.iter().map(|row| row.ckey.clone()).collect();
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        let done = sqlx::query_file!(
            "sql/cleanup/reclaim_orphan_chunks.sql",
            &buckets,
            &kinds,
            &ckeys,
            &seqs
        )
        .execute(self.pool())
        .await?;
        Ok(done.rows_affected())
    }

    /// The staged part heads whose upload row is gone — the residue of a drain interrupted
    /// between the `uploads` rows and the content removal.
    pub async fn orphan_part_heads(&self) -> Result<Vec<OrphanPart>, Error> {
        let rows = sqlx::query_file_as!(OrphanPart, "sql/cleanup/orphan_part_heads.sql")
            .fetch_all(self.pool())
            .await?;
        Ok(rows)
    }

    /// Remove every staged part `rows` named — the `(bucket, ckey)` keys the scan returned —
    /// its chunks and its head together, in one statement.
    pub async fn reclaim_orphan_parts(&self, rows: &[OrphanPart]) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }
        let buckets: Vec<String> = rows.iter().map(|row| row.bucket.to_string()).collect();
        let ckeys: Vec<String> = rows.iter().map(|row| row.ckey.clone()).collect();
        let done = sqlx::query_file!("sql/cleanup/reclaim_orphan_parts.sql", &buckets, &ckeys)
            .execute(self.pool())
            .await?;
        Ok(done.rows_affected())
    }

    /// Every version row that is history — a row the key's current slot has moved off, or one a
    /// supersede stamped — newest-first per key, with the retention facts.
    pub async fn retention_rows(&self) -> Result<Vec<HistoryRow>, Error> {
        let rows = sqlx::query_file_as!(HistoryRow, "sql/cleanup/retention_rows.sql")
            .fetch_all(self.pool())
            .await?;
        Ok(rows)
    }
}
