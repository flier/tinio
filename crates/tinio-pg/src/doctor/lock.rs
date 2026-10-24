//! Object Lock rows for a pg diagnosis (OL31).
//!
//! The scan runs in [`super::diagnose`]; this module only turns a successful
//! scan into check rows. It writes nothing.

use super::{Check, OBJECT_LOCKS, Severity};
use crate::_store_pg::lock_inspect::{self, InspectKind};

/// The Object Lock rows (OL31): one finding per corrupt or orphan row, or one
/// `ok` row when the three tables are clean.
///
/// **Report-only in every mode.** The scan writes nothing, and this module has
/// no repair arm: the fs startup repair is the only writer of these shapes,
/// and a pg bucket is created atomically (OL31). The wording mirrors the
/// filesystem path's rows, because a script reads the same check name and an
/// operator reads the same facts on either backend.
pub(super) fn lock_findings(scan: &lock_inspect::Scan) -> Vec<Check> {
    scan.findings()
        .into_iter()
        .map(|(kind, detail)| {
            let severity = match kind {
                InspectKind::CorruptConfig
                | InspectKind::CorruptLock
                | InspectKind::CorruptIntent => Severity::Error,
                InspectKind::OrphanLock => Severity::Warn,
                InspectKind::Clean => Severity::Ok,
            };
            Check::new(OBJECT_LOCKS, severity, detail)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        super::{OBJECT_LOCKS, Severity, StoreError, schema_check},
        lock_findings,
    };
    use crate::_store_pg::lock_inspect;

    /// The Object Lock row's name is the filesystem path's own check name (OL31): a script sees
    /// one vocabulary on either backend.
    #[test]
    fn the_object_lock_rows_use_the_filesystem_check_name() {
        assert_eq!(OBJECT_LOCKS, "object locks");
    }

    /// A clean scan is one `ok` row, never silence: the leg ran and found nothing.
    #[test]
    fn a_clean_scan_is_one_ok_row() {
        let rows = lock_findings(&lock_inspect::Scan::default());
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].name, OBJECT_LOCKS);
        assert_eq!(rows[0].severity, Severity::Ok);
        assert!(rows[0].detail.contains("no corrupt or orphan"));
    }

    /// Each corrupt or orphan shape gets its own row, at the documented severity, naming the
    /// bucket (and key, version or upload) an operator has to act on.
    #[test]
    fn each_corrupt_or_orphan_row_is_rendered_with_its_subject() {
        let scan = lock_inspect::Scan {
            corrupt_configs: vec![lock_inspect::CorruptConfig {
                bucket: "locked".to_owned(),
                enabled: true,
            }],
            corrupt_locks: vec![lock_inspect::CorruptLock {
                bucket: "locked".to_owned(),
                key: "k".to_owned(),
                version: "v".to_owned(),
                detail: "the stored mode/date pair is partial",
            }],
            corrupt_intents: vec![lock_inspect::CorruptIntent {
                bucket: "locked".to_owned(),
                upload: "u".to_owned(),
                detail: "the stored mode is not a mode wire",
            }],
            orphan_locks: vec![lock_inspect::OrphanLock {
                bucket: "locked".to_owned(),
                key: "k".to_owned(),
                version: "gone".to_owned(),
            }],
        };
        let rows = lock_findings(&scan);
        assert_eq!(rows.len(), 4, "{rows:?}");
        assert!(rows.iter().all(|row| row.name == OBJECT_LOCKS));

        // The corrupt rule keeps the enabled bit beside the corruption (OL4).
        assert_eq!(rows[0].severity, Severity::Error);
        assert!(rows[0].detail.contains("locked"), "{}", rows[0].detail);
        assert!(
            rows[0].detail.contains("still enforces"),
            "{}",
            rows[0].detail
        );

        assert_eq!(rows[1].severity, Severity::Error);
        assert!(rows[1].detail.contains("version `v`"), "{}", rows[1].detail);
        assert!(
            rows[1].detail.contains("fails closed"),
            "{}",
            rows[1].detail
        );

        assert_eq!(rows[2].severity, Severity::Error);
        assert!(rows[2].detail.contains("upload `u`"), "{}", rows[2].detail);

        // An orphan is a warning: it protects nothing, but it is a residue.
        assert_eq!(rows[3].severity, Severity::Warn);
        assert!(rows[3].detail.contains("orphan"), "{}", rows[3].detail);
        assert!(rows[3].detail.contains("gone"), "{}", rows[3].detail);
    }

    /// The catch-all arm: a store error that is neither of the two version faults is "not
    /// inspected" with the store's own message, never a fault. The two version faults' own rows
    /// are pinned by the schema tests, which drive [`schema_check`] with the store's real errors.
    #[test]
    fn an_unreadable_schema_is_not_inspected_with_the_stores_message() {
        let row = schema_check(Err(StoreError::Database("boom".into())));
        assert_eq!(row.severity, Severity::Warn);
        assert!(row.detail.contains("not inspected"), "{}", row.detail);
        assert!(row.detail.contains("boom"), "{}", row.detail);
    }
}
