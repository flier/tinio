//! The version-scoped operations of the fs backend.
//!
//! The state machine is `tinio-version`'s (the `Versions` engine) running over
//! this backend's ports (`crate::version_adapter`): the engine decides what a
//! supersede, a materialize and an expire mean, and this module is fs's own
//! part of the contract surface — the namespace and key checks, the locks, the
//! writes the D37 row-slot model owns, and the policy the engine deliberately
//! does not own: which sequence a new version (or marker) takes, and what a
//! version-addressed **removal** does.
//!
//! The version-addressed **reads** and the per-version **tag/ACL pairs** are
//! the ops engine's (`tinio_ops::ObjectOps`'s version group): each trait method
//! below is that body's call plus fs's own checks. Two consequences shape this
//! module:
//!
//! - The engine's read-heal **preserves** an object row's recorded identity
//!   (the contract's preserving write carries it over) while fs's gate
//!   **clears** one it judged stale, so the version-addressed reads run the
//!   gate's judgement first — the same pre-step, and the same ordering, the
//!   plain read's `version_facts` takes. That step has **one home**
//!   (`objects::resolve_object_slot_stamped`); each entry keeps only its own
//!   address guard ([`FsStorage::retire_degraded_identity`] here, the
//!   bucket's recorded state there).
//! - The version-addressed **delete** stays here ([`FsStorage::delete_versioned`]
//!   is its simple-delete sibling): its current-version arm restores whatever
//!   version becomes current again, which moves the slot **backwards to a real
//!   identity** — a move `advance_slot` refuses — and the commit is the
//!   contract's own call ([`VersionMeta::restore_slot`], this plane's only
//!   caller): one transaction over the object row the restored version
//!   becomes, the version row and index entry it consumes and the cleared
//!   retained part list. The scan, the publish and the prune stay fs's.
//!
//! # The current version, and the marker's home
//!
//! The fs authority for "what is current" is the same one the engine reads:
//! the key path's file plus the `object_meta` row, gated by the recorded
//! size/mtime/identity (`CurrentRows::current`). Two consequences shape this
//! module:
//!
//! - **A noncurrent version is a version-table row.** The current version has
//!   no row of its own: its identity lives in the `object_meta` row, exactly
//!   as the adapter reads it. Superseding it is what creates its row.
//! - **A current delete marker is an `object_meta` row with no file.** The
//!   marker's identity and sequence are durable there before the file is
//!   removed, so a crash can never leave "file gone + row still naming an
//!   object version" — the state the adapter's row guard refuses to
//!   overwrite. The marker's version-table row appears when the marker is
//!   superseded, like any other outgoing version. The engine's marker read
//!   answers the marker's stamp from this row (its version row at the slot's
//!   sequence first, which a **converged** store has and this one does not).
//!
//! # Sequences
//!
//! A version-creating write takes one past the highest the key records — the
//! current slot's own sequence or the newest history row's, whichever is
//! greater (`next_seq`) — and a delete marker always takes a fresh sequence:
//! reusing the outgoing version's sequence would ask the adapter's row guard
//! to overwrite an object version's row with a marker's. The **null version's
//! sequence is 0**: a write that produces the null version records 0, and a
//! suspended bucket's in-place replacement keeps the sequence the slot already
//! records. (The plan states the well-formed case, `current.version_seq + 1`;
//! the row maximum is the same number there and is what keeps a degraded slot
//! — an out-of-band edit reports the null version at sequence 0 while history
//! rows survive — from minting a sequence that already holds a different
//! version's row, which the adapter's row guard would refuse.) The same
//! reasoning gives the one exception to the null rule: when a superseded null
//! version's row already sits at 0 — this model lets a later null version
//! follow an earlier one into history — the incoming null takes the key's next
//! sequence instead of 0, so no row that names different content is ever
//! overwritten.
//!
//! A key also holds **at most one `null`-id row** (AWS's model): when a
//! supersede moves a null version into history and an older null-id row
//! already sits there, the older row is replaced rather than a second one
//! added — the shared `null` index entry can name only one of them, so the
//! other would be unaddressable. The live null version is not that row: it
//! lives in the slot (a marker's identity in the slot's meta row), so a live
//! null and one superseded null row still coexist exactly as the design
//! states.
//!
//! A **degraded slot** — the gate's null whose `object_meta` row still records
//! an identity, so the null-ness is the content drift's rather than a recorded
//! null version — is not moved into history at all: the row it would write
//! claims the null identity while the slot records none, and the 0 the gate
//! reports is the null's canonical sequence, not one the slot took, so writing
//! there would collide with a superseded null's row (the row guard rightly
//! refuses that). The incoming version takes `next_seq` instead, and a
//! suspended replacement resolves the free null sequence the same way a later
//! null's does, so the key never becomes unwritable while the guard keeps its
//! promise.
//!
//! # Feature gating
//!
//! The module is always compiled; the version machinery inside it is
//! `cfg(feature = "versioning")`. With the feature off, the empty
//! `impl VersionOps` below is all this backend says about versions and the
//! trait's own default bodies answer the contract's refusal — the
//! access-denied class for the nine, `Unversioned` for `bucket_versioning` —
//! so no twin exists to drift from the trait's doc.

use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

// One import for both cfg arms of `publish_versioned`'s commit call (the
// unix arm constructs it from the bucket owner's uid, the portable arm is the
// unit value); spelling it once keeps the two arms from drifting.
use fsutil::DirOwnerUid;
use tokio::fs;

// The feature-off `VersionOps` impl below is the only user, so the gate is
// the import's too: the feature-on build (whose impl lives in `enabled`)
// does not carry an unused import.
#[cfg(not(feature = "versioning"))]
use crate::_core::storage::VersionOps;
use crate::{
    _core::{bucket::Name, object, to_nanos, version::Marker},
    _store_redb::{Error as StoreError, version as rows},
    _util::lockmap,
    Error, FsStorage, fsutil,
    write::AtomicWriter,
};

/// Stamp the superseded version's row noncurrent in the caller's state
/// transaction — the publish's half of the rule that a row which is history
/// says so.
///
/// A supersede commits the outgoing version's row **before** the current slot
/// moves off it (the design's crash-safety order), so that row is written with
/// `became_noncurrent == 0`: while the crash window is open the version is
/// still current, and the row says so ([`rows::Row::is_history`]). The
/// transaction that records the new current version moves the slot off the
/// row, so it stamps the row in the same commit — a crash never leaves a row
/// that is history without saying so, and the retention clock starts when the
/// version actually stopped being current. A sequence that holds no row is a
/// no-op, so a row a concurrent path already dropped is not an error.
pub(crate) fn stamp_superseded(
    txn: &mut redb::WriteTransaction,
    bucket: &Name,
    key: &object::Key,
    superseded: Option<u64>,
) -> Result<(), StoreError> {
    if let Some(seq) = superseded {
        rows::Table::open(txn)?.stamp_noncurrent(
            bucket.as_ref().as_str(),
            key.as_ref().as_str(),
            seq,
            to_nanos(SystemTime::now()),
        )?;
    }
    Ok(())
}

/// The feature-off `VersionOps`: the trait's default bodies own every answer
/// (the refusal class for the nine, `Unversioned` for `bucket_versioning`),
/// so there is nothing to write here and nothing to forget.
#[cfg(not(feature = "versioning"))]
impl VersionOps for FsStorage {}

/// The version branch's answer for a write that is about to publish content
/// onto a key path: the identity the response carries and the sequence the
/// write's state row records.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    /// The version the write creates (`Marker::none()` on an unversioned
    /// bucket — today's write shape).
    pub(crate) marker: Marker,
    /// The version's per-key sequence: `0` for an unversioned write and for a
    /// write that produces the null version (the null version's own sequence —
    /// or the key's next, when a superseded null version's row already
    /// occupies 0), the sequence the slot already records for a suspended
    /// in-place replacement (or the key's next for a degraded slot, whose
    /// recorded sequence is the null's canonical 0 rather than a free one),
    /// and the key's next sequence for a version-creating write, for a delete
    /// marker, and for a write onto a degraded slot (which supersedes nothing:
    /// the gate's null records no identity for history to name).
    pub(crate) seq: u64,
    /// The sequence of the outgoing version whose row the supersede committed
    /// — `became_noncurrent` still `0`, because a row is not history until
    /// the state transaction below moves the current slot off it. The
    /// write's own state transaction stamps it (`Row::is_history`'s rule);
    /// `None` when no supersede ran.
    pub(crate) superseded: Option<u64>,
    /// Whether the bucket records versions (state enabled or suspended) —
    /// the state this plan resolved, carried so the committed
    /// [`Info`](crate::_core::object::Info) reports the version-header rule
    /// without a second read. `false` on an unversioned bucket, and in a
    /// stripped build (the state machinery is not compiled in).
    pub(crate) records_versions: bool,
}

impl Plan {
    /// The row-less plan: no identity, no sequence, no supersede. It is what a
    /// publish that records nothing answers — an unversioned bucket, the
    /// content contract's visibility-point publish, and every write in a
    /// stripped build.
    pub(crate) fn rowless() -> Self {
        Self {
            marker: Marker::none(),
            seq: 0,
            superseded: None,
            records_versions: false,
        }
    }
}

/// How a publish treats the version tables: the fs write paths supersede the
/// outgoing version on the way (its row committed before the rename — the
/// engine's protocol step 3), while the content contract's [`ObjectContent::publish`]
/// is the visibility point alone, so the row describing the bytes is the
/// caller's own write after it returns.
///
/// [`ObjectContent::publish`]: crate::_core::content::ObjectContent::publish
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Publication {
    /// Supersede the outgoing version, then rename: the write paths' shape.
    Versioned,
    /// The rename alone, with no row written: the content contract's shape,
    /// with the bucket mutation lock the publish's own.
    Bytes,
    /// The rename alone under a bucket mutation lock the **caller** already
    /// holds: the version engine's port. Taking the lock again on the task
    /// that holds it is a deadlock, not a critical section, and the engine's
    /// fs callers (the delete path's restore) hold it across the whole move.
    BytesUnderLock,
}

/// One published write: the version branch's answer plus the landed file's
/// stamp — everything the caller's own state transaction records.
pub(crate) struct Published {
    /// The version identity the state row must record.
    pub(crate) marker: Marker,
    /// The sequence the state row must record.
    pub(crate) seq: u64,
    /// The sequence of the outgoing version's committed row, which the
    /// state row's transaction stamps noncurrent (see [`Plan::superseded`]).
    pub(crate) superseded: Option<u64>,
    /// The key path the content landed at (the caller's ownership policy).
    pub(crate) path: PathBuf,
    /// The landed file's byte count.
    pub(crate) size: u64,
    /// The landed file's mtime.
    pub(crate) mtime: SystemTime,
    /// The landed file's identity (`0` when the platform has none).
    pub(crate) identity: u64,
    /// Whether the bucket records versions — the plan's answer, carried to
    /// the committed [`Info`](crate::_core::object::Info).
    pub(crate) records_versions: bool,
    /// The bucket mutation lock this publish took, held from before the
    /// version branch until this value is dropped: the caller keeps it alive
    /// while it writes the state row, so the branch, the rename and the stamp
    /// are one critical section. None for a publish under the caller's own
    /// lock (Publication::BytesUnderLock).
    pub(crate) _guard: Option<lockmap::Guard<Name>>,
}

impl FsStorage {
    /// The version branch, as one call: the state read, the current read, the
    /// supersede and the identity mint every write path runs before it
    /// publishes content. With the `versioning` feature off a write answers
    /// today's shape exactly — no state read, no supersede, no sequence.
    async fn plan_publish(&self, bucket: &Name, key: &object::Key) -> Result<Plan, Error> {
        #[cfg(feature = "versioning")]
        {
            self.plan_write(bucket, key).await
        }
        #[cfg(not(feature = "versioning"))]
        {
            let _ = (bucket, key);
            Ok(Plan::rowless())
        }
    }

    /// Whether the bucket records versions (state enabled or suspended) —
    /// the callers' carrier for
    /// [`Info::bucket_records_versions`](crate::_core::object::Info) on the
    /// paths that resolve no write plan: one point read of the bucket
    /// state, no bucket-existence check (every caller resolved the bucket
    /// already). The version-addressed reads carry the same flag from the
    /// engine's own state read, not from here. The feature off answers
    /// `false`: a stripped build answers no version identity at all, exactly
    /// as its write plan does.
    pub(crate) async fn records_versions(&self, bucket: &Name) -> Result<bool, Error> {
        #[cfg(feature = "versioning")]
        {
            use crate::error::projected;
            Ok(self
                .versions(bucket)
                .state(bucket)
                .await
                .map_err(projected)?
                .records_versions())
        }
        #[cfg(not(feature = "versioning"))]
        {
            let _ = bucket;
            Ok(false)
        }
    }

    /// Publish a staged temp onto `key` — the ordering every fs write path
    /// shares, owned in one place: take the bucket mutation lock, re-resolve
    /// the bucket and the key under it (a followed symlink retargeted since
    /// the caller's fail-fast must not send the rename to a stale target),
    /// run the version branch, rename the staged temp onto the key path, and
    /// read back the landed file's stamp.
    ///
    /// The caller owns what the landed content *stamps* — its own state
    /// transaction and its ownership policy, plus the outgoing version's row
    /// stamp ([`stamp_superseded`], which rides that same transaction) — and
    /// must keep the returned [`Published`] alive until that write is done:
    /// the guard it carries is the bucket mutation lock, so the supersede
    /// (row durable before the rename) and the state row are one critical
    /// section against the key's other writers.
    ///
    /// No write path lands content any other way: a path that forgot the
    /// version branch would overwrite the current version without superseding
    /// it — the previous content would never enter history — so the branch
    /// cannot be omitted without omitting the publish itself.
    pub(crate) async fn publish_versioned(
        &self,
        bucket: &Name,
        key: &object::Key,
        temp: &Path,
        publication: Publication,
    ) -> Result<Published, Error> {
        let guard = match publication {
            Publication::Versioned | Publication::Bytes => {
                Some(self.lock_bucket_mutations(bucket).await)
            }
            Publication::BytesUnderLock => None,
        };
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let target = self.resolve_key(&bucket_dir, key).await?;
        // The contract's publish is the visibility point alone: the row
        // describing the bytes is the caller's own write, after this returns,
        // so its plan is the row-less one — no supersede, no sequence, no
        // state read.
        let plan = match publication {
            Publication::Versioned => self.plan_publish(bucket, key).await?,
            Publication::Bytes | Publication::BytesUnderLock => Plan::rowless(),
        };
        // OL7/C1 (the 2026-10-21 review): the write paths' plan resolved the
        // identity the row will record. When that identity is the null
        // version, the write is an in-place replacement, and the outgoing
        // identity's active lock must refuse it **before the rename** — after
        // it, the bytes would already have replaced the protected content.
        // The row transaction that records the write re-checks the same rule
        // (`record_committed`), so this is the early refusal, not the
        // authority. `plan.records_versions` is true for enabled **and**
        // suspended buckets; the row-less arms (`Bytes`/`BytesUnderLock`)
        // record their own rows and keep their own checks.
        if matches!(publication, Publication::Versioned)
            && plan.marker.id.is_none()
            && plan.records_versions
        {
            self.refuse_protected_null(bucket, key).await?;
        }
        // The unix dir hardening (spec 2026-09-05 §5a): a prefix dir
        // implicitly created by this write lands `0700` and chowned to the
        // bucket owner's mapped uid (the lookup runs once per publish).
        #[cfg(unix)]
        let dir_owner_uid = self.bucket_owner_uid(bucket).await;
        // F03: the bucket root bounds the first-into-a-new-prefix ancestor
        // sync.
        AtomicWriter::commit(
            temp,
            &target,
            Some(&bucket_dir),
            #[cfg(unix)]
            DirOwnerUid(dir_owner_uid),
            #[cfg(not(unix))]
            DirOwnerUid,
        )
        .await?;
        let metadata = fs::metadata(&target).await?;
        Ok(Published {
            marker: plan.marker,
            seq: plan.seq,
            superseded: plan.superseded,
            records_versions: plan.records_versions,
            size: metadata.len(),
            mtime: metadata.modified()?,
            identity: fsutil::file_identity(&target, &metadata),
            path: target,
            _guard: guard,
        })
    }
}

#[cfg(feature = "versioning")]
mod enabled {
    use std::{io::Error as IoError, path::Path, time::SystemTime};

    use async_trait::async_trait;
    use tokio::fs;
    use tracing::instrument;

    // The version identity a write path must record for the version it is
    // about to publish: the top-level `Plan`, shared with the feature-off
    // branch so the two can never drift — and the stamping helper the state
    // transactions share.
    use super::{Plan, StoreError, stamp_superseded};
    use crate::{
        _core::{
            ByteRange, ETag,
            acl::Acl,
            bucket::Name,
            from_nanos,
            meta::{VersionMeta, object as object_row},
            object::{self, Tags},
            object_lock, storage,
            storage::VersionOps,
            to_nanos,
            version::{
                self, CurrentVersion, HeadResult, Id, ListParams, Listing, Marker, ReadResult,
                State, wire_of,
            },
        },
        _store_redb::{
            bucket_versioning, object_meta, object_part, version as rows, version_index,
        },
        _version::{Current, Identity, Intent, MarkerWrite, Seqs, plan_write as write_plan},
        Error, FsStorage,
        backend::objects::{prune_parents, remove_object_file},
        error::projected,
        fsutil,
        version::list,
        version_adapter::Rows,
    };

    /// The version a request resolved to — the version-addressed **delete**'s
    /// resolution.
    ///
    /// The engine's version group resolves its own reads through the
    /// contracts (its own `resolve`), so this one is the delete's: it carries
    /// only what a removal acts on — the identity and sequence it removes, and
    /// the row a noncurrent one names. The identity is the **gated** one
    /// throughout: `None` is the null version, either recorded as null or
    /// degraded there by the staleness rule, and a recorded identity the
    /// content no longer matches is never acted on as if it still named it.
    enum Found {
        /// The key's current version is content.
        Current {
            /// The gated current identity.
            id: Option<Id>,
            /// The gated current sequence, for the crash window's
            /// adopted-row match.
            seq: u64,
        },
        /// The key's current version is a delete marker: no file, but a
        /// recorded identity and stamp (the gate has no content to compare a
        /// marker's identity against, so the recorded one is the gated one).
        Marker {
            /// The gated marker identity (`None` is the null-identity
            /// marker).
            id: Option<Id>,
            /// The gated marker sequence.
            seq: u64,
        },
        /// A noncurrent version: the version-table row is the whole truth —
        /// its content lives in the bucket's block store.
        Row(Box<rows::Row>),
    }

    impl FsStorage {
        /// The version identity the incoming content must record, and the
        /// outgoing version's move into history — the write path's branch,
        /// run under the caller's bucket mutation lock so the state read, the
        /// current read, the supersede and the commit's record are one
        /// critical section. The slot this branch read is the one the
        /// supersede is given (`Versions::supersede_with`), so it is read
        /// once per write, never once for the branch and again inside the
        /// engine.
        ///
        /// The three arms are the design's write paths: an unversioned bucket
        /// writes exactly as it always has; an enabled bucket supersedes the
        /// outgoing version and mints a fresh identity (even onto a new key —
        /// a write onto a marker is a normal versioned write); a suspended
        /// bucket replaces an existing null version in place (no row, no
        /// blob, no sequence bump) and supersedes anything else before the
        /// new null version becomes current.
        /// The key's gated current slot, and the `object_meta` row the gate was
        /// applied to. The read goes through the adapter's `current_with_row`,
        /// whose row is the same one the gate judged, so a caller that needs
        /// both never reads the row twice. The identity is the gate's, never
        /// the row's: the adapter answers `Current { exists: true, version:
        /// None }` for a recorded identity the content no longer matches (an
        /// out-of-band edit, or a best-effort `record_committed` that never
        /// landed), and it **degrades** the slot — `Current::degraded` — so a
        /// null current whose row still records an identity is told apart
        /// from a recorded null version. A degraded slot is no version the
        /// history can name — the row a supersede would write claims the null
        /// identity while the slot records none, and its 0 is the null's
        /// canonical sequence, not the one the slot took — so the write paths
        /// neither supersede it into history nor reuse 0 for it, and the
        /// shared write decision (`crate::_version::plan_write`) is where both
        /// rules live. Every gated read of the current slot goes through the
        /// adapter's `current_with_row` — this helper here, the engine's reads
        /// through the `CurrentRows` port — so no two callers can disagree
        /// about what is current.
        async fn gated_current(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<(Current, Option<object_meta::Stored>), Error> {
            let judged = Rows::new(self.storage_handle())
                .current_with_row(bucket, key)
                .await
                .map_err(Error::from)?;
            Ok((judged.current, judged.stored))
        }

        /// The bucket's versioning state and the key's sequence view from
        /// **one** point-read transaction (2026-10-06 review: the versioned
        /// write path paid one read for the state and another for the
        /// sequences; both values feed the same plan, so one transaction
        /// answers them together).
        fn state_and_seqs(&self, bucket: &Name, key: &object::Key) -> Result<(State, Seqs), Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .read(move |txn| {
                    let state = bucket_versioning::Table::open_readonly(txn)?
                        .get(bucket.as_ref().as_str())?
                        .unwrap_or_default();
                    Ok((
                        state,
                        seqs_in(txn, bucket.as_ref().as_str(), key.as_ref().as_str())?,
                    ))
                })
                .map_err(Error::from)
        }

        /// The key's sequence view, as the shared write decision takes it: the
        /// newest version row's sequence (the crash window's adopted row
        /// included — that row occupies its sequence for good) and whether a
        /// row occupies sequence 0 — the null version's own sequence, which a
        /// later null version may take only while it is free. Both ride one
        /// point-read transaction.
        fn seq_view(&self, bucket: &Name, key: &object::Key) -> Result<Seqs, Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .read(move |txn| {
                    Ok(seqs_in(
                        txn,
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                    )?)
                })
                .map_err(Error::from)
        }

        pub(crate) async fn plan_write(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<Plan, Error> {
            let versions = self.versions(bucket);
            // One transaction answers the state and the sequence view (see
            // `state_and_seqs`); the early return below skips only the slot
            // gate.
            let (state, seqs) = self.state_and_seqs(bucket, key)?;
            if state == State::Unversioned {
                return Ok(Plan::rowless());
            }
            // The gated current slot with the row it was judged from (the
            // degraded-slot rule's home is `gated_current`) and the key's
            // sequence view: the two values the shared write-path decision
            // takes. The policy is `tinio_version::plan_write`'s — one pure
            // function both backends execute — and this backend owns only the
            // reads, the supersede and the writes below.
            let (current, _) = self.gated_current(bucket, key).await?;
            let plan = write_plan(Intent::Put, state, &current, &seqs);
            if plan.supersede {
                versions
                    .supersede_with(bucket, key, &current)
                    .await
                    .map_err(projected)?;
            }
            let id = match plan.identity {
                Identity::Fresh => Some(version::mint()),
                Identity::Null | Identity::Unversioned => None,
            };
            Ok(Plan {
                marker: Marker {
                    id,
                    is_delete_marker: plan.marker,
                },
                seq: plan.seq,
                // The committed row's sequence, which the caller's own state
                // transaction stamps noncurrent with the rest of the publish.
                superseded: plan.supersede.then_some(current.seq),
                records_versions: state.records_versions(),
            })
        }

        /// The simple `DELETE` on a versioned bucket: move the current version
        /// into history, make a delete marker the current version, leave the
        /// marker's identity durable in `object_meta`, and remove the file —
        /// all under one bucket mutation lock, so no write can interleave
        /// between the marker's row and the file's removal.
        ///
        /// On a suspended bucket the current **null** version — an object
        /// version or the null-identity marker a previous suspended delete
        /// left current — is removed in place rather than moved, and the new
        /// marker carries the null version ID: repeated deletes leave exactly
        /// one listed marker and add no history row, as the design's
        /// suspended rule states. Anything that carries a real identity is
        /// superseded into history first.
        ///
        /// `Ok(None)` means the bucket is unversioned — the caller then runs
        /// the ops engine's version-free delete.
        ///
        /// This body stays **backend-side**. A shared body that wrote the
        /// marker as a **version row** (the shape whose slot resolves the
        /// marker bit off that row) would let the store's one-null-version
        /// rule displace the superseded null version's own row — losing a
        /// version the shared conformance leg pins ("the superseded null
        /// version must survive a suspended DELETE"). fs's model keeps the
        /// current version in the **object row** (D37) and the version table
        /// for history alone, so the marker is written there.
        pub(crate) async fn delete_versioned(
            &self,
            bucket: &Name,
            key: &object::Key,
            path: &Path,
        ) -> Result<Option<Marker>, Error> {
            let versions = self.versions(bucket);
            if versions.state(bucket).await.map_err(projected)? == State::Unversioned {
                return Ok(None);
            }
            let _guard = self.lock_bucket_mutations(bucket).await;
            // Re-read under the lock: a concurrent state change must not slip
            // a marker into a bucket that just stopped versioning.
            let state = versions.state(bucket).await.map_err(projected)?;
            if state == State::Unversioned {
                return Ok(None);
            }
            // The gated slot and the key's sequence view: the shared write
            // decision's two inputs. A degraded null (the row still records an
            // identity the content no longer matches) is no version to move
            // into history, and a null version is removed rather than moved —
            // the decision states both.
            let (current, _) = self.gated_current(bucket, key).await?;
            let seqs = self.seq_view(bucket, key)?;
            let plan = write_plan(Intent::Delete, state, &current, &seqs);
            if plan.supersede {
                versions
                    .supersede_with(bucket, key, &current)
                    .await
                    .map_err(projected)?;
            }
            // OL7: a suspended delete of a recorded null object removes the
            // only copy. The same protected-null rule the publish applies
            // refuses that before the marker row and the unlink. Replacing a
            // current delete marker does not, and a supersede has already
            // moved the bytes into history.
            if !plan.supersede && current.is_null() && !current.degraded {
                self.refuse_protected_null(bucket, key).await?;
            }
            let id = match plan.identity {
                Identity::Fresh => Some(version::mint()),
                Identity::Null | Identity::Unversioned => None,
            };
            let seq = plan.seq;
            let superseded = plan.supersede.then_some(current.seq);
            // The marker's identity is durable BEFORE the caller removes the
            // file, so a crash cannot leave "file gone + row still naming the
            // object version" — the state the row guard refuses. The row
            // carries no file match (size 0, a fresh mtime, no identity): a
            // still-present file reads as the null version through the gate,
            // never as the marker's content. The superseded row is stamped in
            // this same transaction: the slot moves off it here, so it is
            // history from here (`Row::is_history`).
            let stored = object_meta::Stored {
                version_id: id.clone(),
                version_seq: seq,
                is_delete_marker: true,
                etag: ETag::EMPTY,
                size: 0,
                mtime: to_nanos(SystemTime::now()),
                file_identity: 0,
                tags: Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            };
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .write(move |txn| {
                    object_meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                    stamp_superseded(txn, &bucket, &key, superseded)?;
                    Ok(())
                })
                .await
                .map_err(Error::from)?;
            // The file goes last, still under the lock: the marker is durable
            // first (above), and no write can slip between the two.
            remove_object_file(path, *self.follow_symlinks()).await?;
            Ok(Some(Marker {
                id,
                is_delete_marker: true,
            }))
        }

        /// The version-addressed reads' **address guard** over the G3
        /// pre-step, whose one home is `objects::resolve_object_slot_stamped`
        /// (the judgement itself, and why it must run before the engine body,
        /// are documented there).
        ///
        /// The address here is a version, not "the key": the step serves it
        /// only while the key's slot still **names** that version, because
        /// the engine serves the current arm through its read-heal alone — a
        /// history version and a marker come from their own rows and never
        /// touch the current one. The plain read's entry serves the other
        /// address ([`Self::version_facts`], "the current version, whatever
        /// it is") under its own guard, the bucket's recorded state. The
        /// resolution's answer is discarded here: the engine body answers from
        /// the version plane.
        ///
        /// A key whose file is gone is left to the body except where the
        /// address is the **null version** and the row still records one: the
        /// slot names it (a degraded row's gated identity *is* the null
        /// version), so the step runs and answers the `NoSuchKey` its file
        /// resolution finds — the same answer the body's content probe would
        /// give for that state.
        async fn retire_degraded_identity(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<(), Error> {
            if let Some(Found::Current { id, .. }) = self.slot(bucket, key).await?
                && id.as_ref() == version
            {
                // The step's one home, taken exactly as the element paths
                // take it — the version-addressed reads simply carry no facts
                // to fill, so the answer is dropped.
                self.resolve_object_slot_stamped(bucket, key).await?;
            }
            Ok(())
        }

        /// The key's current slot, as the adapter's gate answers it. `None`
        /// covers "no current version" and a domain-invalid recorded row (the
        /// read path's self-heal territory); the gated read and its rules live
        /// in [`Self::gated_current`].
        async fn slot(&self, bucket: &Name, key: &object::Key) -> Result<Option<Found>, Error> {
            let judged = Rows::new(self.storage_handle())
                .current_with_row(bucket, key)
                .await
                .map_err(Error::from)?;
            let current = judged.current;
            if !current.exists {
                return Ok(None);
            }
            Ok(Some(if current.is_delete_marker {
                Found::Marker {
                    id: current.version,
                    seq: current.seq,
                }
            } else {
                Found::Current {
                    id: current.version,
                    seq: current.seq,
                }
            }))
        }

        /// Resolve the version a request addresses: the current slot first (a
        /// current version carries no row of its own, and the null version's
        /// index entry may name a superseded row while the live null version
        /// sits in the object store), then the version table through its
        /// identity index.
        async fn locate(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Found, Error> {
            if let Some(found) = self.slot(bucket, key).await? {
                let identity = match &found {
                    Found::Current { id, .. } | Found::Marker { id, .. } => id.as_ref(),
                    Found::Row(_) => None,
                };
                if identity == version {
                    return Ok(found);
                }
            }
            let wire = wire_of(version);
            // The identity index and the row it names from **one** read
            // transaction (the `state_and_seqs` shape): the resolve used to
            // pay one read for the index and another for the row, and both
            // feed the same answer.
            let bucket = bucket.clone();
            let key = key.clone();
            let indexed = wire.clone();
            let row = self
                .handle()
                .read(move |txn| {
                    let Some(seq) = version_index::Table::open_readonly(txn)?.seq_of(
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                        &indexed,
                    )?
                    else {
                        return Ok(None);
                    };
                    let row = rows::Table::open_readonly(txn)?.get(
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                        seq,
                    )?;
                    Ok(row.filter(|row| row.id == indexed))
                })
                .map_err(Error::from)?;
            if let Some(row) = row {
                return Ok(Found::Row(Box::new(row)));
            }
            Err(storage::no_such_version(wire).into())
        }

        /// The two newest version rows of the key, newest first — everything
        /// the restore's successor lookup needs: the row it is deleting (in
        /// the supersede crash window the outgoing version's row is already
        /// committed) and the one below it. The store's bounded read walks
        /// only that prefix of the key's history; a full `for_key` walk would
        /// decode every row's tags, checksum, owner and ACL to inspect at
        /// most two of them.
        async fn successor_rows(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<Vec<(u64, rows::Row)>, Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .read(move |txn| {
                    Ok(rows::Table::open_readonly(txn)?.newest(
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                        2,
                    )?)
                })
                .map_err(Error::from)
        }

        /// Make the newest remaining version current again — the materialize
        /// half of the move, and the only path that turns history back into
        /// the key path. The version's content is published FIRST (a crash
        /// between the publish and the commit leaves the version's row — and
        /// so its content — recoverable); then ONE transaction — the
        /// contract's [`VersionMeta::restore_slot`], this plane's one caller
        /// of it — drops its row and index entry, writes the object row the
        /// restored version becomes (the slot columns included), and clears
        /// the stale part rows; with no version remaining the key's state is
        /// removed instead ([`Self::clear_current`]).
        ///
        /// The successor lookup reads only the two newest rows (the version
        /// being deleted and the one below it — the only two a restore can
        /// reach); the state row takes its size from the publish's own answer
        /// and its ETag from the restored row (the version's authoritative
        /// value, so a multipart-composed form survives the restore), and the
        /// published file is never re-read.
        ///
        /// `removed` names the version the caller is deleting right now, and
        /// its **own** row is dropped on the way: in the supersede crash
        /// window the outgoing version's row is already committed while it is
        /// still current, and materializing that adopted row would resurrect —
        /// or, skipped, orphan — the very version being deleted. That row is
        /// the one row in the window that is not history (`Row::is_history`
        /// with the removed version's own sequence), and the successor is the
        /// newest row that is.
        async fn restore(
            &self,
            bucket: &Name,
            key: &object::Key,
            removed: Option<&(String, u64)>,
        ) -> Result<(), Error> {
            let versions = self.versions(bucket);
            // The caller's own version is the one being removed, so it is
            // also the current version: the slot sits at its sequence while
            // the removal is in flight, and a caller that names no removed
            // version holds no slot fact at all.
            let slot = removed.map_or(rows::Slot::Unknown, |(_, seq)| rows::Slot::At(*seq));
            let mut next = None;
            for (seq, row) in self.successor_rows(bucket, key).await? {
                if removed == Some(&(row.id.clone(), seq)) {
                    versions.expire(bucket, key, seq).await.map_err(projected)?;
                    continue;
                }
                // A row that is not history is the current version's own row
                // — and the current version is the one being removed, handled
                // above — so it is never a successor: nothing else may become
                // current while the caller's own version delete is in flight.
                if !row.is_history(slot) {
                    continue;
                }
                next = Some((seq, row));
                break;
            }
            let Some((seq, row)) = next else {
                return self.clear_current(bucket, key).await;
            };
            // The engine materializes a *contract* row, so the one the raw
            // scan found is read back through the contract by the sequence it
            // named. The scan itself stays raw: the successor decision needs
            // the wire-level `is_history` and the bounded two-row prefix read
            // that the contract's decoded rows do not expose.
            let adapter = Rows::new(self.storage_handle());
            let Some(stored_row) = adapter
                .get_version(bucket, key, seq)
                .await
                .map_err(projected)?
            else {
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    seq,
                    "the version row vanished between the successor scan and the restore"
                );
                return Err(Error::Io(IoError::other(
                    "a version row vanished between the successor scan and the restore",
                )));
            };
            if row.kind == rows::Kind::Marker {
                // The marker that becomes current holds no content: re-stamp
                // the slot onto the marker's own row, publish nothing. The row
                // the restore installs takes the marker's own created stamp
                // (its object row's) and matches no file.
                let restored = restored_row(&row, 0, from_nanos(row.created), 0);
                return adapter
                    .restore_slot(bucket, key, &restored, row.id(), seq)
                    .await
                    .map_err(projected);
            }
            // Publish the version's content back onto the key path through
            // the engine's own streaming materialize, then describe what
            // landed in the state transaction. The publish answers the byte
            // count its write path computed while streaming, so the file is
            // neither re-opened nor re-hashed here. A port that publishes
            // nothing for a row whose kind is `object` has lost the row's
            // blob: that is a case to answer with an error, never a panic.
            let Some(published) = versions
                .materialize(bucket, key, &stored_row)
                .await
                .map_err(projected)?
            else {
                // The context goes to the log, not the wire (2026-10-06
                // review): the client answers the generic internal error.
                tracing::warn!(
                    bucket = %bucket,
                    key = %key,
                    seq,
                    "the version row names content, but its materialize published none"
                );
                return Err(Error::Io(IoError::other(
                    "a version row names content, but its materialize published none",
                )));
            };
            let dir = self.ensure_bucket(bucket).await?;
            let path = self.resolve_key(&dir, key).await?;
            // One stat for the landed file's stamp and identity — the
            // staleness gate's own inputs, which must describe the file the
            // rename left at the key path (the writer's staging loop saw only
            // the pre-rename temp, and a cross-volume commit replaces it with
            // a copy). The size comes from the publish.
            let metadata = fs::metadata(&path).await.map_err(Error::from)?;
            // The version's own recorded ETag rides the row (never the
            // publish's plain content MD5): restoring a version must not
            // change what its ETag is, and a multipart-composed
            // `MD5-of-MD5s-N` form has no content-derived equivalent (the
            // publish's MD5 is the single-upload form of the same bytes).
            let restored = restored_row(
                &row,
                published.size,
                metadata.modified().map_err(Error::from)?,
                fsutil::file_identity(&path, &metadata),
            );
            adapter
                .restore_slot(bucket, key, &restored, row.id(), seq)
                .await
                .map_err(projected)
        }

        /// Remove the key's current-version state (its `object_meta` row and
        /// stale part rows) — the "no version remains" arm of a version
        /// delete: the key disappears from the tree.
        async fn clear_current(&self, bucket: &Name, key: &object::Key) -> Result<(), Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .write(move |txn| {
                    {
                        let mut table = object_meta::Table::open(txn)?;
                        table.remove(&bucket, &key)?;
                    }
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.remove_key(&bucket, &key)?;
                    }
                    Ok(())
                })
                .await
                .map_err(Error::from)
        }
    }

    /// The key's sequence view from within `txn` — the two facts the shared
    /// write decision takes ([`Seqs`]), read together because `state_and_seqs`
    /// answers the bucket's state in the same transaction.
    fn seqs_in(txn: &redb::ReadTransaction, bucket: &str, key: &str) -> Result<Seqs, StoreError> {
        let table = rows::Table::open_readonly(txn)?;
        let latest = table.latest(bucket, key)?.map_or(0, |(seq, _)| seq);
        let null_at_zero = table.get(bucket, key, 0)?.is_some();
        Ok(Seqs {
            latest,
            null_at_zero,
        })
    }

    /// The contract row a restore installs, from the version row the successor
    /// scan read plus the published file's facts: the version's own recorded
    /// elements (the ETag included — a multipart-composed form has no
    /// content-derived equivalent), the `size`/`mtime`/`identity` arguments
    /// describing what landed on the key path, and no version columns — the
    /// contract's object row carries none, so the commit
    /// ([`VersionMeta::restore_slot`]) adds them. A marker publishes nothing,
    /// so its arm passes no size, its own created stamp and no file identity.
    fn restored_row(
        row: &rows::Row,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> object_row::Row {
        object_row::Row {
            etag: row.etag(),
            size,
            mtime,
            identity,
            tags: row.tags(),
            checksum: row.checksum(),
            owner: row.owner(),
            acl: row.acl(),
        }
    }

    #[async_trait]
    impl VersionOps for FsStorage {
        /// The state engine's read, over the bucket's version plane. The
        /// namespace check stays here: fs answers `NoSuchBucket` for a key in
        /// a bucket that is not there, and the engine knows no path.
        async fn bucket_versioning(&self, bucket: &Name) -> Result<State, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .bucket_versioning(&self.version_plane(bucket), bucket)
                .await
                .map_err(Error::from)
        }

        /// The guarded engine state write, under fs's bucket mutation lock:
        /// every write path reads the state inside its own hold of this lock,
        /// so the flip is serialized with them — without it a write that
        /// overlapped the flip could act on the old state after the flip
        /// answered (orderable, but only by an argument; the lock makes the
        /// serialization the obvious reading).
        ///
        /// The write is the store's **guarded** one (R21/OL13), like pg's: a
        /// suspend over an enabled Object Lock configuration refuses
        /// `InvalidBucketState` inside the same transaction that reads the
        /// configuration, so a lock-enabled bucket can never be suspended. A
        /// lazily-created directory with no bucket row yet is the guarded
        /// write's documented opt-out (see the arm below).
        #[instrument(
            level = "debug",
            target = "tinio::storage",
            skip(self, state),
            fields(bucket = %bucket)
        )]
        async fn set_bucket_versioning(&self, bucket: &Name, state: State) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            let _guard = self.lock_bucket_mutations(bucket).await;
            let rows = self.version_plane(bucket).version_rows;
            match VersionMeta::set_bucket_state_guarded(&*rows, bucket, state).await {
                Ok(()) => Ok(()),
                // A lazy plane's bucket row is materialized by its writers,
                // not by a directory: a state write on a directory no writer
                // has recorded yet is the plain state write (the contract
                // leaves the guard opt-in for exactly this shape). Under this
                // arm's mutation guard a concurrent bucket removal cannot
                // interleave, and an enabled lock configuration implies a
                // recorded bucket row (`put_bucket_object_lock` records it
                // first), so the R21 cross-check is not lost here.
                Err(storage::Error::NoSuchBucket(_)) => {
                    VersionMeta::set_bucket_state(&*rows, bucket, state)
                        .await
                        .map_err(Error::from)
                }
                Err(err) => Err(err.into()),
            }
        }

        async fn get_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            range: Option<ByteRange>,
        ) -> Result<ReadResult, Error> {
            self.ensure_bucket(bucket).await?;
            if key.is_reserved() || key.is_folder_marker() {
                return Err(storage::no_such_key(key).into());
            }
            // The G3 pre-step, run where — and only where — the address is
            // the key's current version: `retire_degraded_identity` holds the
            // guard and the reason the step must precede the engine body, and
            // the step itself is `objects::resolve_object_slot_stamped`. A
            // history version or a marker address never touches the current
            // row, so it skips the step; the pre-step's own answer is
            // discarded.
            self.retire_degraded_identity(bucket, key, version).await?;
            self.objects()
                .get_object_version(&self.version_plane(bucket), bucket, key, version, range)
                .await
                .map_err(Error::from)
        }

        async fn head_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<HeadResult, Error> {
            self.ensure_bucket(bucket).await?;
            if key.is_reserved() || key.is_folder_marker() {
                return Err(storage::no_such_key(key).into());
            }
            // The same pre-step as the get's, for the same reason.
            self.retire_degraded_identity(bucket, key, version).await?;
            self.objects()
                .head_object_version(&self.version_plane(bucket), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        /// Kept backend-side — with `delete_versioned` beside it — for fs's own
        /// half of the removal: the successor scan over the key's raw rows, the
        /// content removal and the prefix prune, under the bucket mutation
        /// lock. The **commit** is the contract's: the restore's slot move is
        /// now expressible through [`VersionMeta::restore_slot`], whose only
        /// caller on this plane is the removal's own restore step. That call
        /// carries the move **backwards to a real identity** — the successor's
        /// own sequence, below the version being removed — that `advance_slot`
        /// refuses by its absolute guard, and commits it together with the
        /// object row the successor becomes, the consumed version row and its
        /// index entry, and the cleared retained part list, all-or-nothing.
        /// The window a split would open stays closed: with the slot moved and
        /// the row dropped in separate calls, the restored version's row would
        /// outlive its own materialization while the old slot still named it,
        /// so the key could list its current version twice. The marker-creating
        /// simple delete is a **different** gap and stays fs's own
        /// `delete_versioned`: a marker row that is a version row lets the
        /// store's one-null rule displace the superseded null version's history
        /// row, and closing that is a model decision — where a current marker's
        /// identity lives — not a call.
        #[instrument(
            level = "debug",
            target = "tinio::storage",
            skip(self, version),
            fields(bucket = %bucket, key = %key)
        )]
        async fn delete_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            bypass: object_lock::Bypass,
        ) -> Result<Marker, Error> {
            self.ensure_bucket(bucket).await?;
            if key.is_reserved() || key.is_folder_marker() {
                return Err(storage::no_such_key(key).into());
            }
            // The whole removal — the resolve, the file removal and the
            // restore of what becomes current — is one critical section
            // against the key's writers.
            let _guard = self.lock_bucket_mutations(bucket).await;
            let dir = self.ensure_bucket(bucket).await?;
            let path = self.resolve_key(&dir, key).await?;
            match self.locate(bucket, key, version).await? {
                Found::Current { id, seq } => {
                    // Object Lock (OL8, R11, R19): `expire_with` is the delete
                    // policy and it answers before the content is touched. Its
                    // no-row fallback judges the current slot even when the
                    // addressed sequence differs from the slot's (the fs
                    // degraded null), so a refusal precedes every removal and
                    // the file is unlinked only after the store's verdict.
                    let version_rows = self.version_plane(bucket).version_rows;
                    version_rows.expire_with(bucket, key, seq, bypass).await?;
                    // The verdict is deliberately not consulted: an `Err` (the
                    // lock refusal) has already returned, so **any** success
                    // means the addressed current version has no live row after
                    // this call — a removed row or a row-less current slot that
                    // `restore` clears below — and the key-path file is
                    // unreferenced. Unlinking here is what reclaims the
                    // degraded-null file the sweep does not see (it only
                    // reclaims unreferenced blocks). The order is authoritative
                    // answer first, file second; an I/O failure propagates
                    // rather than hiding, and the row is already gone, so the
                    // leftover file is unreferenced debris, never a protected
                    // version's content.
                    remove_object_file(&path, *self.follow_symlinks()).await?;
                    let removed = (wire_of(id.as_ref()), seq);
                    self.restore(bucket, key, Some(&removed)).await?;
                    // The same empty-prefix pruning the plain delete does: a
                    // version delete that emptied the key path must not leave
                    // a directory behind (an empty prefix would make the
                    // emptied bucket undeletable). A restore that published a
                    // successor leaves the directory non-empty, and the prune
                    // stops there.
                    prune_parents(&path, &dir).await;
                    Ok(Marker {
                        id,
                        is_delete_marker: false,
                    })
                }
                Found::Marker { id, seq } => {
                    // Removing the current marker shows the version it hid.
                    remove_object_file(&path, *self.follow_symlinks()).await?;
                    let removed = (wire_of(id.as_ref()), seq);
                    self.restore(bucket, key, Some(&removed)).await?;
                    prune_parents(&path, &dir).await;
                    Ok(Marker {
                        id,
                        is_delete_marker: true,
                    })
                }
                Found::Row(row) => {
                    // No file is touched — the noncurrent version lives in the
                    // block store — so nothing can have been emptied here.
                    self.version_plane(bucket)
                        .version_rows
                        .expire_with(bucket, key, row.seq, bypass)
                        .await
                        .map_err(projected)?;
                    Ok(Marker {
                        id: row.id(),
                        is_delete_marker: row.kind == rows::Kind::Marker,
                    })
                }
            }
        }

        /// The bounded per-key probe behind the plain read's 404
        /// disclosure: the engine's own current resolution
        /// (`Rows::current_with_row` — the gate's one home, shared with
        /// the version-addressed readers), so the probe and every read
        /// agree about what is current. Exact-key reads: one
        /// `object_meta` row plus one path stat, never a bucket walk.
        async fn current_version(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<CurrentVersion, Error> {
            self.ensure_bucket(bucket).await?;
            if key.is_reserved() || key.is_folder_marker() {
                return Ok(CurrentVersion::Absent);
            }
            let judged = Rows::new(self.storage_handle())
                .current_with_row(bucket, key)
                .await?;
            Ok(if !judged.current.exists {
                CurrentVersion::Absent
            } else if judged.current.is_delete_marker {
                CurrentVersion::Marker
            } else {
                CurrentVersion::Object
            })
        }

        async fn list_object_versions(
            &self,
            bucket: &Name,
            params: ListParams,
        ) -> Result<Listing, Error> {
            list::page(self, bucket, &params).await
        }

        /// The version's tags, from the row that records them: the object
        /// row's element for the current version (a current version has no
        /// version-table row of its own), the version row's for a noncurrent
        /// one — and the contract's not-found class, naming the version, for
        /// a delete marker, which holds no metadata.
        async fn version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Tags, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .version_tags(&self.version_plane(bucket), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        async fn set_version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            tags: Tags,
        ) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .set_version_tags(&self.version_plane(bucket), bucket, key, version, tags)
                .await
                .map_err(Error::from)
        }

        async fn version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Acl, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .version_acl(&self.version_plane(bucket), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        /// Replace-all on the grants, the version's owner preserved — the
        /// object ACL put's contract, per version.
        async fn set_version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            acl: &Acl,
        ) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .set_version_acl(&self.version_plane(bucket), bucket, key, version, acl)
                .await
                .map_err(Error::from)
        }

        /// The retention stored on a version — the server's read and the
        /// GET/HEAD lock-header echo (OL8, OL28). One store read, fail closed.
        async fn get_object_retention(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Option<object_lock::Retention>, Error> {
            self.ensure_bucket(bucket).await?;
            VersionMeta::get_object_retention(
                &Rows::new(self.storage_handle()),
                bucket,
                key,
                version,
            )
            .await
            .map_err(Error::from)
        }

        /// The legal hold stored on a version (OL8, OL28) — the same one
        /// store read as the retention's.
        async fn get_object_legal_hold(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<object_lock::LegalHold, Error> {
            self.ensure_bucket(bucket).await?;
            VersionMeta::get_object_legal_hold(
                &Rows::new(self.storage_handle()),
                bucket,
                key,
                version,
            )
            .await
            .map_err(Error::from)
        }

        /// A version's whole lock in **one** store read — the GET/HEAD echo's
        /// shape (OL8, OL28). The default composes the two single-value reads
        /// above; the store's lock row answers both together, so this pays one.
        async fn object_lock(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Option<object_lock::Lock>, Error> {
            self.ensure_bucket(bucket).await?;
            VersionMeta::version_lock(&Rows::new(self.storage_handle()), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        /// Set or clear a version's retention (R22) — the engine body's
        /// address resolution and the store's one atomic write. The `Rows`
        /// adapter takes the bucket mutation lock inside, so the write
        /// serializes with the versioned write paths and the delete arms.
        async fn set_retention(
            &self,
            bucket: &Name,
            key: &object::Key,
            address: &version::Address,
            requested: Option<&object_lock::Retention>,
            bypass: object_lock::Bypass,
            now: SystemTime,
        ) -> Result<Option<object_lock::Lock>, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .set_retention(
                    &self.version_plane(bucket),
                    bucket,
                    key,
                    address,
                    requested,
                    bypass,
                    now,
                )
                .await
                .map_err(Error::from)
        }

        /// Set or clear a version's legal hold (R22) — the same delegation as
        /// [`VersionOps::set_retention`].
        async fn set_legal_hold(
            &self,
            bucket: &Name,
            key: &object::Key,
            address: &version::Address,
            hold: object_lock::LegalHold,
        ) -> Result<object_lock::Lock, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .set_legal_hold(&self.version_plane(bucket), bucket, key, address, hold)
                .await
                .map_err(Error::from)
        }

        /// The combined retention and legal-hold update (OL23) — the engine
        /// body's address resolution and the store's one atomic write, so a
        /// refused retention leaves neither member applied.
        async fn update_lock(
            &self,
            bucket: &Name,
            key: &object::Key,
            address: &version::Address,
            retention: Option<Option<&object_lock::Retention>>,
            legal_hold: Option<object_lock::LegalHold>,
            bypass: object_lock::Bypass,
            now: SystemTime,
        ) -> Result<Option<object_lock::Lock>, Error> {
            self.ensure_bucket(bucket).await?;
            self.objects()
                .update_lock(
                    &self.version_plane(bucket),
                    bucket,
                    key,
                    address,
                    retention,
                    legal_hold,
                    bypass,
                    now,
                )
                .await
                .map_err(Error::from)
        }
    }

    /// The engine's marker port: a current marker is a slot and an empty object
    /// row, not a version row. The undo of a versioned put that superseded a
    /// marker calls this to put that marker back.
    #[async_trait]
    impl MarkerWrite for FsStorage {
        async fn mark_current_marker(
            &self,
            bucket: &Name,
            key: &object::Key,
            id: Option<Id>,
            seq: u64,
            moved_off: Option<u64>,
        ) -> Result<(), storage::Error> {
            let stored = object_meta::Stored {
                version_id: id,
                version_seq: seq,
                is_delete_marker: true,
                etag: ETag::EMPTY,
                size: 0,
                mtime: to_nanos(SystemTime::now()),
                file_identity: 0,
                tags: Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            };
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .write(move |txn| {
                    object_meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
                    stamp_superseded(txn, &bucket, &key, moved_off)?;
                    Ok(())
                })
                .await
                .map_err(Error::from)
                .map_err(storage::Error::from)
        }
    }
}
