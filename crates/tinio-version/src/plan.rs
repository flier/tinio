//! The write path's pure decision: what one write does to the key's outgoing
//! version, and which version the incoming one becomes.
//!
//! Both backends run this one policy — [`plan_write`] says whether to
//! supersede, whether the incoming version is a delete marker, whether it is
//! the null version or a fresh one, whether the one-null-version convergence
//! applies, and which sequence it takes — and each executes the answer with
//! its own transactions, writes and I/O. Nothing here reads a table, opens a
//! file, mints an identity or awaits: the decision is a function of the
//! bucket's versioning state, the key's gated current slot and the key's
//! sequence view, so it is a table of values the tests below walk rather than
//! a path through two backends.
//!
//! The two backends' representations differ in ways this decision must not:
//! the fs backend's current slot is a file plus its `object_meta` row (which
//! is why its gate can *degrade* a slot), mem's is a row, and mem's current
//! delete marker is a row of the version table while the fs backend keeps a
//! marker's identity in `object_meta` and writes no row for it at all. The
//! decision sees only [`Current`], and the backends translate.

use crate::{
    _core::version::{self, State},
    ports::Current,
};

/// What the caller is about to write at the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// An object write: the incoming version is content.
    Put,
    /// A simple `DELETE`: the incoming version is a delete marker.
    Delete,
}

/// The key's sequence view: the two facts a writer needs to place the
/// incoming version on a free sequence.
///
/// The fs backend reads both from the version table in one read transaction;
/// mem reads them off the rows it already holds. `latest` is the newest
/// version row's sequence — the crash window's adopted row included, because
/// that row occupies its sequence for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Seqs {
    /// The newest version row's sequence, or `0` when the key has none.
    pub latest: u64,
    /// Whether a version row occupies sequence `0` — the null version's own
    /// sequence, which a later null version may take only while it is free.
    pub null_at_zero: bool,
}

/// The identity the incoming version records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity {
    /// A fresh identity: the caller mints it — the only place randomness
    /// enters this policy.
    Fresh,
    /// The null version: no identity.
    Null,
    /// No version at all: an unversioned bucket's write, today's shape.
    Unversioned,
}

/// The write path's decision for one key.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::State;
/// use tinio_version::{Current, Identity, Intent, Seqs, plan_write};
///
/// let current = Current {
///     exists: true,
///     version: Some("v1".parse().unwrap()),
///     is_delete_marker: false,
///     seq: 7,
///     degraded: false,
/// };
/// let plan = plan_write(Intent::Put, State::Enabled, &current, &Seqs::default());
/// assert!(plan.supersede, "the outgoing version moves into history");
/// assert_eq!(plan.seq, 8, "one past the sequence the slot records");
/// assert_eq!(plan.identity, Identity::Fresh);
/// assert!(!plan.marker);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    /// Whether the outgoing current version moves into history before the
    /// incoming version is published.
    pub supersede: bool,
    /// Whether the row the supersede commits carries the null identity, so
    /// the key's one-`null`-row convergence must run (an older `null`-id row
    /// the index names at another sequence is displaced).
    pub converge_null: bool,
    /// Whether the incoming version is a delete marker.
    pub marker: bool,
    /// The identity the incoming version records.
    pub identity: Identity,
    /// The sequence the incoming version records.
    pub seq: u64,
}

/// Whether a supersede of the version `id` commits the null version — the
/// one-null-version convergence's trigger.
///
/// A key holds at most one `null`-id row: the index is keyed by the identity
/// and can name only one of two rows sharing the `null` wire, so a supersede
/// that commits the null identity displaces the older `null`-id row the index
/// names (at another sequence) rather than stranding it unaddressable.
///
/// # Examples
///
/// ```rust
/// use tinio_version::converges_null;
///
/// assert!(
///     converges_null(None),
///     "the null version's supersede converges"
/// );
/// assert!(!converges_null(Some(&"v1".parse().unwrap())));
/// ```
pub fn converges_null(id: Option<&version::Id>) -> bool {
    id.is_none()
}

/// The write path's decision: what `intent` does at a key whose bucket is in
/// `state`, whose gated current slot is `current`, and whose version table
/// answers `seqs`.
///
/// The three arms are the design's write paths. An unversioned bucket writes
/// exactly as it always has (no version, sequence `0`). An enabled bucket
/// supersedes the outgoing version — a degraded slot is no version the history
/// can name, so it is left alone — and mints a fresh identity; a write onto a
/// delete marker is a normal versioned write, because a marker is current
/// state, not content. A suspended bucket replaces an existing recorded null
/// version in place (no row, no sequence bump) and supersedes anything else
/// before the incoming null version becomes current; a suspended `DELETE`
/// removes the null version (and the null-identity marker a previous suspended
/// delete left current) rather than moving it, so repeated deletes keep one
/// marker and add no history row.
///
/// A delete marker always takes the key's next sequence, never the outgoing
/// version's: reusing the outgoing sequence would ask the fs backend's row
/// guard to overwrite an object version's row with a marker's.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::State;
/// use tinio_version::{Current, Identity, Intent, Seqs, plan_write};
///
/// // A suspended bucket's write over its recorded null version replaces it
/// // in place: no supersede, no history, the sequence the slot records.
/// let null = Current {
///     exists: true,
///     version: None,
///     is_delete_marker: false,
///     seq: 0,
///     degraded: false,
/// };
/// let plan = plan_write(Intent::Put, State::Suspended, &null, &Seqs::default());
/// assert!(!plan.supersede);
/// assert_eq!(plan.identity, Identity::Null);
/// assert_eq!(plan.seq, 0);
/// ```
pub fn plan_write(intent: Intent, state: State, current: &Current, seqs: &Seqs) -> Plan {
    // The next version's sequence: one past the highest the key records —
    // the current slot's own sequence or the newest history row's, whichever
    // is greater — so it is free by construction.
    let next_seq = current.seq.max(seqs.latest).saturating_add(1);
    // The null version's own sequence is 0, taken while 0 is free; when a
    // superseded null version's row already occupies it, the incoming null
    // takes the key's next sequence instead, so no row that names different
    // content is ever overwritten.
    let null_seq = if seqs.null_at_zero { next_seq } else { 0 };
    // A delete marker is the incoming version only where a version is
    // recorded at all: an unversioned bucket's delete is today's.
    let marker = intent == Intent::Delete && state != State::Unversioned;
    let (identity, supersede, seq) = match (intent, state) {
        (_, State::Unversioned) => (Identity::Unversioned, false, 0),
        (Intent::Put, State::Enabled) => {
            // A degraded slot is no version the history can name: it is not
            // superseded, and the incoming version takes a free sequence.
            (
                Identity::Fresh,
                current.exists && !current.degraded,
                next_seq,
            )
        }
        (Intent::Put, State::Suspended) => {
            // The in-place replacement of an existing **recorded** null
            // version creates no version: it keeps the sequence the slot
            // already records. A degraded slot records none, so its 0 is the
            // null's canonical sequence rather than a free one, and the free
            // sequence is resolved like any later null's. Everything else —
            // a real version, a delete marker (the null-identity marker a
            // previous suspended delete left current included) — turns
            // noncurrent first.
            let in_place = current.exists && current.is_null() && !current.degraded;
            let supersede = current.exists && !current.is_null();
            (
                Identity::Null,
                supersede,
                if in_place { current.seq } else { null_seq },
            )
        }
        (Intent::Delete, State::Enabled) => (
            Identity::Fresh,
            current.exists && !current.degraded,
            next_seq,
        ),
        (Intent::Delete, State::Suspended) => {
            // The null version is removed rather than moved, and so is the
            // null-identity marker a previous suspended delete left current;
            // anything carrying a real identity turns noncurrent first.
            (
                Identity::Null,
                current.exists && current.version.is_some(),
                next_seq,
            )
        }
    };
    Plan {
        supersede,
        converge_null: supersede && converges_null(current.version.as_ref()),
        marker,
        identity,
        seq,
    }
}

/// The decision's own tests: the state × current × sequences matrix, one row
/// per reachable shape, so the policy is pinned as values rather than through
/// either backend's path.
#[cfg(test)]
mod tests {
    use super::*;

    /// The slot a row describes.
    fn slot(
        exists: bool,
        version: Option<&str>,
        marker: bool,
        seq: u64,
        degraded: bool,
    ) -> Current {
        Current {
            exists,
            version: version.map(|id| id.parse().unwrap()),
            is_delete_marker: marker,
            seq,
            degraded,
        }
    }

    /// One matrix row: what a write does at `current` with `seqs`.
    struct Case {
        label: &'static str,
        intent: Intent,
        state: State,
        current: Current,
        seqs: Seqs,
        supersede: bool,
        converge_null: bool,
        marker: bool,
        identity: Identity,
        seq: u64,
    }

    /// Assert one row, naming it on failure.
    #[track_caller]
    fn check(case: &Case) {
        let plan = plan_write(case.intent, case.state, &case.current, &case.seqs);
        assert_eq!(
            (
                plan.supersede,
                plan.converge_null,
                plan.marker,
                plan.identity,
                plan.seq
            ),
            (
                case.supersede,
                case.converge_null,
                case.marker,
                case.identity,
                case.seq
            ),
            "{}",
            case.label
        );
    }

    /// The matrix: every arm of every state, and the sequence each one takes.
    #[test]
    fn the_write_decision_matrix_is_the_one_policy() {
        // A fresh key: no current version, no rows.
        let fresh = Current::default();
        let no_rows = Seqs::default();
        // A key whose newest history row sits at 2 (a superseded version).
        let _history = Seqs {
            latest: 2,
            null_at_zero: false,
        };
        // The null version's row occupies sequence 0.
        let null_row = Seqs {
            latest: 2,
            null_at_zero: true,
        };
        let real = slot(true, Some("v1"), false, 7, false);
        let marker = slot(true, Some("v2"), true, 5, false);
        let null_version = slot(true, None, false, 0, false);
        let later_null = slot(true, None, false, 3, false);
        let null_marker = slot(true, None, true, 4, false);
        // The gate's degraded slot: the recorded identity is gone, so the
        // slot records none and its 0 is the null's canonical sequence.
        let degraded = slot(true, None, false, 0, true);

        let cases = [
            // An unversioned bucket writes (and deletes) exactly as it
            // always has: no version, no supersede, sequence 0.
            Case {
                label: "put, unversioned, any slot",
                intent: Intent::Put,
                state: State::Unversioned,
                current: real.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Unversioned,
                seq: 0,
            },
            Case {
                label: "delete, unversioned, any slot",
                intent: Intent::Delete,
                state: State::Unversioned,
                current: real.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Unversioned,
                seq: 0,
            },
            // An enabled bucket: the outgoing version is superseded and the
            // incoming one mints a fresh identity one past the highest
            // sequence the key records.
            Case {
                label: "put, enabled, fresh key",
                intent: Intent::Put,
                state: State::Enabled,
                current: fresh.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Fresh,
                seq: 1,
            },
            Case {
                label: "put, enabled, real current at 7",
                intent: Intent::Put,
                state: State::Enabled,
                current: real.clone(),
                seqs: Seqs {
                    latest: 7,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: false,
                identity: Identity::Fresh,
                seq: 8,
            },
            Case {
                label: "put, enabled, null current at 0 occupies no row yet",
                intent: Intent::Put,
                state: State::Enabled,
                current: null_version.clone(),
                seqs: no_rows,
                supersede: true,
                converge_null: true,
                marker: false,
                identity: Identity::Fresh,
                seq: 1,
            },
            Case {
                label: "put, enabled, degraded slot is no version to name",
                intent: Intent::Put,
                state: State::Enabled,
                current: degraded.clone(),
                seqs: Seqs {
                    latest: 1,
                    null_at_zero: true,
                },
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Fresh,
                seq: 2,
            },
            Case {
                label: "delete, enabled, real current at 7",
                intent: Intent::Delete,
                state: State::Enabled,
                current: real.clone(),
                seqs: Seqs {
                    latest: 7,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: true,
                identity: Identity::Fresh,
                seq: 8,
            },
            Case {
                label: "delete, enabled, fresh key",
                intent: Intent::Delete,
                state: State::Enabled,
                current: fresh.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: true,
                identity: Identity::Fresh,
                seq: 1,
            },
            Case {
                label: "delete, enabled, real-identity marker current",
                intent: Intent::Delete,
                state: State::Enabled,
                current: marker.clone(),
                seqs: Seqs {
                    latest: 5,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: true,
                identity: Identity::Fresh,
                seq: 6,
            },
            Case {
                label: "delete, enabled, null current is superseded too",
                intent: Intent::Delete,
                state: State::Enabled,
                current: null_version.clone(),
                seqs: no_rows,
                supersede: true,
                converge_null: true,
                marker: true,
                identity: Identity::Fresh,
                seq: 1,
            },
            // A suspended bucket: an existing recorded null version is
            // replaced in place; anything else turns noncurrent first, and a
            // delete removes the null version rather than moving it.
            Case {
                label: "put, suspended, fresh key records the null version at 0",
                intent: Intent::Put,
                state: State::Suspended,
                current: fresh.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 0,
            },
            Case {
                label: "put, suspended, recorded null at 0 is replaced in place",
                intent: Intent::Put,
                state: State::Suspended,
                current: null_version.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 0,
            },
            Case {
                label: "put, suspended, later null at 3 keeps its sequence",
                intent: Intent::Put,
                state: State::Suspended,
                current: later_null.clone(),
                seqs: null_row,
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 3,
            },
            Case {
                label: "put, suspended, real current supersedes and takes 0",
                intent: Intent::Put,
                state: State::Suspended,
                current: slot(true, Some("v1"), false, 1, false),
                seqs: Seqs {
                    latest: 1,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 0,
            },
            Case {
                label: "put, suspended, occupied 0 pushes the incoming null to the next",
                intent: Intent::Put,
                state: State::Suspended,
                current: slot(true, Some("v1"), false, 1, false),
                seqs: Seqs {
                    latest: 1,
                    null_at_zero: true,
                },
                supersede: true,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 2,
            },
            Case {
                label: "put, suspended, null-identity marker turns noncurrent first",
                intent: Intent::Put,
                state: State::Suspended,
                current: null_marker.clone(),
                seqs: Seqs {
                    latest: 4,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: true,
                marker: false,
                identity: Identity::Null,
                seq: 0,
            },
            Case {
                label: "put, suspended, degraded slot resolves a free null sequence",
                intent: Intent::Put,
                state: State::Suspended,
                current: degraded.clone(),
                seqs: Seqs {
                    latest: 3,
                    null_at_zero: true,
                },
                supersede: false,
                converge_null: false,
                marker: false,
                identity: Identity::Null,
                seq: 4,
            },
            Case {
                label: "delete, suspended, fresh key",
                intent: Intent::Delete,
                state: State::Suspended,
                current: fresh.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: true,
                identity: Identity::Null,
                seq: 1,
            },
            Case {
                label: "delete, suspended, null version is removed, not moved",
                intent: Intent::Delete,
                state: State::Suspended,
                current: null_version.clone(),
                seqs: no_rows,
                supersede: false,
                converge_null: false,
                marker: true,
                identity: Identity::Null,
                seq: 1,
            },
            Case {
                label: "delete, suspended, real current turns noncurrent",
                intent: Intent::Delete,
                state: State::Suspended,
                current: real.clone(),
                seqs: Seqs {
                    latest: 7,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: true,
                identity: Identity::Null,
                seq: 8,
            },
            Case {
                label: "delete, suspended, null-identity marker is replaced in place",
                intent: Intent::Delete,
                state: State::Suspended,
                current: null_marker.clone(),
                seqs: Seqs {
                    latest: 4,
                    null_at_zero: false,
                },
                supersede: false,
                converge_null: false,
                marker: true,
                identity: Identity::Null,
                seq: 5,
            },
            Case {
                label: "delete, suspended, real-identity marker turns noncurrent",
                intent: Intent::Delete,
                state: State::Suspended,
                current: marker.clone(),
                seqs: Seqs {
                    latest: 5,
                    null_at_zero: false,
                },
                supersede: true,
                converge_null: false,
                marker: true,
                identity: Identity::Null,
                seq: 6,
            },
        ];
        for case in &cases {
            check(case);
        }
    }

    /// The convergence trigger is the superseded version's identity, not its
    /// shape: only the null identity displaces the older `null`-id row.
    #[test]
    fn only_the_null_identity_converges() {
        assert!(converges_null(None));
        assert!(!converges_null(Some(&"v1".parse().unwrap())));
        let null_version = slot(true, None, false, 0, false);
        let plan = plan_write(Intent::Put, State::Enabled, &null_version, &Seqs::default());
        assert!(
            plan.converge_null,
            "the superseded row carries the null wire"
        );
        let real = slot(true, Some("v1"), false, 1, false);
        let plan = plan_write(Intent::Put, State::Enabled, &real, &Seqs::default());
        assert!(!plan.converge_null);
    }

    /// The sequence rules are structural: one past the highest the key
    /// records, saturating at the top of `u64` rather than wrapping onto a
    /// row a version already occupies.
    #[test]
    fn the_next_sequence_never_wraps_onto_an_occupied_one() {
        let current = slot(true, Some("v1"), false, u64::MAX, false);
        let plan = plan_write(Intent::Put, State::Enabled, &current, &Seqs::default());
        assert_eq!(plan.seq, u64::MAX, "a saturated next sequence never wraps");
    }
}
