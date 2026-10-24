//! The doctor's Object Lock inspection vocabulary (OL31).
//!
//! The scan itself belongs to each store — redb walks tables and the pg store
//! reads SQL — but the rows it reports and the words an operator reads are one
//! vocabulary. The result types lived twice, verbatim, in
//! `tinio-store-redb::lock_inspect` and `tinio-store-pg::lock_inspect`; this
//! module owns them once and both stores re-export them, so every existing
//! `lock_inspect::Scan` path still resolves.
//!
//! [`Scan::findings`] is the single rendering of a scan: the five operator
//! rows (four finding kinds plus the clean case), with the wording the
//! filesystem and pg doctors both emit. A caller maps each [`InspectKind`] to
//! its own severity and row type, so the two doctors keep their own vocabulary
//! while the sentences cannot drift apart.
//!
//! The decoders self-heal: a corrupt bucket rule loses the rule and keeps the
//! enabled bit, and a corrupt per-version row fails closed only on the paths
//! that read it. These rows **report** what the readers silently heal or
//! refuse (OL31). Nothing here writes.

/// A stored bucket configuration whose rule wire does not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptConfig {
    /// The bucket whose configuration row is corrupt.
    pub bucket: String,
    /// The enabled bit — the fact the decoder keeps through the corruption
    /// and the one that drives enforcement (OL4).
    pub enabled: bool,
}

/// A stored per-version lock row that fails closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptLock {
    /// The bucket the row is keyed under.
    pub bucket: String,
    /// The object key the row is keyed under.
    pub key: String,
    /// The version identity wire the row is keyed by.
    pub version: String,
    /// The model's refusal detail (a corrupt mode, date or legal hold, or a
    /// partial mode/date pair) — the same text the store logs.
    pub detail: &'static str,
}

/// A stored multipart lock intent that fails closed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptIntent {
    /// The bucket the intent is keyed under.
    pub bucket: String,
    /// The upload id the intent is keyed by.
    pub upload: String,
    /// The model's refusal detail.
    pub detail: &'static str,
}

/// A lock row that resolves to neither a version row nor a current slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanLock {
    /// The bucket the row is keyed under.
    pub bucket: String,
    /// The object key the row is keyed under.
    pub key: String,
    /// The version identity wire the row is keyed by.
    pub version: String,
}

/// What one scan found, in the table's own row order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Scan {
    /// Bucket configuration rows whose rule wire does not decode.
    pub corrupt_configs: Vec<CorruptConfig>,
    /// Per-version lock rows that fail closed.
    pub corrupt_locks: Vec<CorruptLock>,
    /// Multipart lock intents that fail closed.
    pub corrupt_intents: Vec<CorruptIntent>,
    /// Lock rows with no version row and no current slot.
    pub orphan_locks: Vec<OrphanLock>,
}

/// The kind of row a [`Scan::findings`] entry renders.
///
/// The first four name the scan's own row kinds, in the order the scan reports
/// them; [`InspectKind::Clean`] is the one row a clean scan emits, so a caller
/// can map severity without a second empty-case branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectKind {
    /// A bucket configuration whose rule wire does not decode.
    CorruptConfig,
    /// A per-version lock row that fails closed.
    CorruptLock,
    /// A multipart lock intent that fails closed.
    CorruptIntent,
    /// A lock row that resolves to neither a version row nor a current slot.
    OrphanLock,
    /// No corrupt or orphan row: the scan ran and found nothing.
    Clean,
}

impl Scan {
    /// Whether every row of the three tables decodes and resolves.
    pub fn is_clean(&self) -> bool {
        self.corrupt_configs.is_empty()
            && self.corrupt_locks.is_empty()
            && self.corrupt_intents.is_empty()
            && self.orphan_locks.is_empty()
    }

    /// The operator-facing rows, in the scan's own order, or the single clean
    /// row.
    ///
    /// One renderer for both doctors (the fs scan and the pg scan): the
    /// wording is the operator's, not either store's. A caller maps each
    /// [`InspectKind`] to its own severity and row type.
    pub fn findings(&self) -> Vec<(InspectKind, String)> {
        let mut findings = Vec::new();
        for row in &self.corrupt_configs {
            findings.push((
                InspectKind::CorruptConfig,
                format!(
                    "bucket `{}`: the stored default-retention rule is corrupt (the reader \
                     discards it); the enabled bit is {}",
                    row.bucket,
                    if row.enabled {
                        "enabled — the row still enforces"
                    } else {
                        "not set"
                    }
                ),
            ));
        }
        for row in &self.corrupt_locks {
            findings.push((
                InspectKind::CorruptLock,
                format!(
                    "bucket `{}` key `{}` version `{}`: {} (the row fails closed)",
                    row.bucket, row.key, row.version, row.detail
                ),
            ));
        }
        for row in &self.corrupt_intents {
            findings.push((
                InspectKind::CorruptIntent,
                format!(
                    "bucket `{}` upload `{}`: {} (the intent fails closed)",
                    row.bucket, row.upload, row.detail
                ),
            ));
        }
        for row in &self.orphan_locks {
            findings.push((
                InspectKind::OrphanLock,
                format!(
                    "bucket `{}` key `{}` version `{}`: an orphan lock row (no version row and \
                     no current slot)",
                    row.bucket, row.key, row.version
                ),
            ));
        }
        if findings.is_empty() {
            findings.push((
                InspectKind::Clean,
                "no corrupt or orphan lock rows".to_owned(),
            ));
        }
        findings
    }
}
