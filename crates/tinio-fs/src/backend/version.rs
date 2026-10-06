//! The version-scoped operations of the fs backend.
//!
//! The state machine is `tinio-version`'s (the `Versions` engine) running over
//! this backend's ports (`crate::version_adapter`): the engine decides what a
//! supersede, a materialize and an expire mean, and this module is the
//! contract surface plus the policy the engine deliberately does not own —
//! which version a request addresses, what a delete marker answers, and which
//! sequence a new version (or marker) takes.
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
//!   superseded, like any other outgoing version.
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
    _store::{Error as StoreError, version as rows},
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
    /// The bucket mutation lock, held from before the version branch until
    /// this value is dropped: the caller keeps it alive while it writes the
    /// state row, so the branch, the rename and the stamp are one critical
    /// section.
    pub(crate) _guard: lockmap::Guard<Name>,
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
            Ok(Plan {
                marker: Marker::none(),
                seq: 0,
                superseded: None,
                records_versions: false,
            })
        }
    }

    /// Whether the bucket records versions (state enabled or suspended) —
    /// the producers' carrier for
    /// [`Info::bucket_records_versions`](crate::_core::object::Info) on the
    /// paths that resolve no write plan: one point read of the bucket
    /// state, no bucket-existence check (every caller resolved the bucket
    /// already). The feature off answers `false`: a stripped build answers
    /// no version identity at all, exactly as its write plan does.
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
    ) -> Result<Published, Error> {
        let guard = self.lock_bucket_mutations(bucket).await;
        let bucket_dir = self.ensure_bucket(bucket).await?;
        let target = self.resolve_key(&bucket_dir, key).await?;
        let plan = self.plan_publish(bucket, key).await?;
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
    use std::{
        io::Error as IoError,
        path::{Path, PathBuf},
        time::SystemTime,
    };

    use async_trait::async_trait;
    use tokio::fs;

    // The version identity a write path must record for the version it is
    // about to publish: the top-level `Plan`, shared with the feature-off
    // branch so the two can never drift — and the stamping helper the state
    // transactions share.
    use super::{Plan, stamp_superseded};
    use crate::{
        _core::{
            ByteRange, ETag, GetObjectResult,
            acl::Acl,
            bucket::Name,
            from_nanos,
            object::{self, Tags},
            storage,
            storage::VersionOps,
            to_nanos,
            version::{
                self, CurrentVersion, HeadResult, Id, ListParams, Listing, Marker, ReadResult,
                State, wire_of,
            },
        },
        _store::{bucket_versioning, meta, object_part, version as rows, version_index},
        _version::{Current, Identity, Intent, Seqs, plan_write as write_plan},
        Error, FsStorage,
        backend::objects::{prune_parents, remove_object_file},
        error::projected,
        fsutil,
        version::list,
        version_adapter::Rows,
    };

    /// The version a request resolved to.
    enum Found {
        /// The key's current version is content. `id` is the **gated**
        /// identity: `None` is the null version, either recorded as null or
        /// degraded there by the staleness rule — a recorded identity the
        /// content no longer matches is never served as if it still named
        /// it. `seq` is the gated sequence, for the crash window's
        /// adopted-row match.
        Current {
            /// The gated current identity.
            id: Option<Id>,
            /// The gated current sequence.
            seq: u64,
            /// The `object_meta` row the gate was applied to (the raw row —
            /// the metadata elements the served version carries).
            stored: Box<meta::Stored>,
            /// The key path the gate resolved and judged, when the key has a
            /// file. The read arms open THIS path (one policy open) and
            /// describe it from the handle, so a version-addressed GET or
            /// HEAD resolves the key once — never by calling the plain read
            /// again. `None` is the slot whose file is gone (an out-of-band
            /// removal): there is no object to serve.
            path: Option<PathBuf>,
        },
        /// The key's current version is a delete marker: no file, an identity
        /// and a stamp in the `object_meta` row. A marker's recorded identity
        /// is the gated one (the gate has no content to compare it against).
        Marker(Box<meta::Stored>),
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
        /// rules live. Every gated read of the current slot goes through here:
        /// the callers cannot disagree about what is current.
        async fn gated_current(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<(Current, Option<meta::Stored>), Error> {
            let judged = Rows::new(self.clone())
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
                    let table = rows::Table::open_readonly(txn)?;
                    let (bucket, key) = (bucket.as_ref().as_str(), key.as_ref().as_str());
                    let latest = table.latest(bucket, key)?.map_or(0, |(seq, _)| seq);
                    let null_at_zero = table.get(bucket, key, 0)?.is_some();
                    Ok((
                        state,
                        Seqs {
                            latest,
                            null_at_zero,
                        },
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
                    let table = rows::Table::open_readonly(txn)?;
                    let (bucket, key) = (bucket.as_ref().as_str(), key.as_ref().as_str());
                    let latest = table.latest(bucket, key)?.map_or(0, |(seq, _)| seq);
                    let null_at_zero = table.get(bucket, key, 0)?.is_some();
                    Ok(Seqs {
                        latest,
                        null_at_zero,
                    })
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
                return Ok(Plan {
                    marker: Marker::none(),
                    seq: 0,
                    superseded: None,
                    records_versions: false,
                });
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
        /// today's delete unchanged.
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
            let stored = meta::Stored {
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
                    meta::Table::open(txn)?.put(&bucket, &key, &stored)?;
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

        /// The key's current slot, as the adapter's gate answers it, with the
        /// stored row the gate was applied to and the key path it resolved.
        /// `None` covers "no current version" and a domain-invalid recorded
        /// row (the read path's self-heal territory); the gated read and its
        /// rules live in [`Self::gated_current`].
        ///
        /// The path rides in the [`Found::Current`] answer so the read arms
        /// open it directly: one resolution of the key per request, the same
        /// one the plain read's `resolve_object_info` takes, never a second
        /// resolve behind the first.
        async fn slot(&self, bucket: &Name, key: &object::Key) -> Result<Option<Found>, Error> {
            let judged = Rows::new(self.clone())
                .current_with_row(bucket, key)
                .await
                .map_err(Error::from)?;
            let (current, stored) = (judged.current, judged.stored);
            if !current.exists {
                return Ok(None);
            }
            let Some(stored) = stored else {
                return Ok(None);
            };
            Ok(Some(if current.is_delete_marker {
                Found::Marker(Box::new(stored))
            } else {
                Found::Current {
                    id: current.version,
                    seq: current.seq,
                    stored: Box::new(stored),
                    path: judged.path,
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
                    Found::Current { id, .. } => id.as_ref(),
                    Found::Marker(stored) => stored.version_id.as_ref(),
                    Found::Row(_) => None,
                };
                if identity == version {
                    return Ok(found);
                }
            }
            let wire = wire_of(version);
            if let Some(seq) = self.index_seq(bucket, key, &wire).await?
                && let Some(row) = self.row_at(bucket, key, seq).await?
                && row.id == wire
            {
                return Ok(Found::Row(Box::new(row)));
            }
            Err(storage::no_such_version(wire).into())
        }

        /// The sequence the identity index holds for `(bucket, key, wire)`.
        async fn index_seq(
            &self,
            bucket: &Name,
            key: &object::Key,
            wire: &str,
        ) -> Result<Option<u64>, Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            let wire = wire.to_owned();
            self.handle()
                .read(move |txn| {
                    Ok(version_index::Table::open_readonly(txn)?.seq_of(
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                        &wire,
                    )?)
                })
                .map_err(Error::from)
        }

        /// One version row by its sequence.
        async fn row_at(
            &self,
            bucket: &Name,
            key: &object::Key,
            seq: u64,
        ) -> Result<Option<rows::Row>, Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            self.handle()
                .read(move |txn| {
                    Ok(rows::Table::open_readonly(txn)?.get(
                        bucket.as_ref().as_str(),
                        key.as_ref().as_str(),
                        seq,
                    )?)
                })
                .map_err(Error::from)
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
        /// between the publish and the state transaction leaves the version's
        /// row — and so its content — recoverable); then ONE transaction
        /// drops its row and index entry and writes the current-version
        /// columns naming it, clears the stale part rows, or removes the
        /// key's state when no version remains.
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
            if row.kind == rows::Kind::Marker {
                // The marker that becomes current holds no content: re-stamp
                // the slot, publish nothing.
                let stored = marker_row(seq, &row);
                return self.commit_restore(bucket, key, seq, &row.id, stored).await;
            }
            // Publish the version's content back onto the key path through
            // the engine's own streaming materialize, then describe what
            // landed in the state transaction. The publish answers the byte
            // count its write path computed while streaming, so the file is
            // neither re-opened nor re-hashed here. A port that publishes
            // nothing for a row whose kind is `object` has lost the row's
            // blob: that is a case to answer with an error, never a panic.
            let Some(published) = versions
                .materialize(bucket, key, &row)
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
            let stored = row.stored(
                seq,
                published.size,
                to_nanos(metadata.modified().map_err(Error::from)?),
                fsutil::file_identity(&path, &metadata),
            );
            self.commit_restore(bucket, key, seq, &row.id, stored).await
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
                        let mut table = meta::Table::open(txn)?;
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

        /// The state transaction of a restore: drop the materialized
        /// version's row and index entry, and write the current-version
        /// columns — ONE transaction, so the two never disagree. The caller
        /// only reaches it with a version to restore (no successor is
        /// `clear_current`'s arm), so `stored` is taken by value, never
        /// absent.
        async fn commit_restore(
            &self,
            bucket: &Name,
            key: &object::Key,
            seq: u64,
            id: &str,
            stored: meta::Stored,
        ) -> Result<(), Error> {
            let bucket = bucket.clone();
            let key = key.clone();
            let id = id.to_owned();
            self.handle()
                .write(move |txn| {
                    {
                        let mut table = meta::Table::open(txn)?;
                        table.put(&bucket, &key, &stored)?;
                    }
                    {
                        let mut parts = object_part::Table::open(txn)?;
                        parts.remove_key(&bucket, &key)?;
                    }
                    {
                        let mut rows = rows::Table::open(txn)?;
                        rows.remove(&bucket, &key, seq)?;
                    }
                    {
                        let mut index = version_index::Table::open(txn)?;
                        index.remove_pointing_at(&bucket, &key, &id, seq)?;
                    }
                    Ok(())
                })
                .await
                .map_err(Error::from)
        }

        /// Rewrite one version row's tags or ACL element (the row's other
        /// elements untouched) — one write transaction; the identity and
        /// sequence are unchanged, so the index needs nothing. The write
        /// failure propagates: the caller must not answer success for a tag
        /// or ACL set that was never stored (the same call against the
        /// current version already fails loudly).
        async fn rewrite_row<F>(
            &self,
            bucket: &Name,
            key: &object::Key,
            row: &rows::Row,
            edit: F,
        ) -> Result<(), Error>
        where
            F: FnOnce(&mut rows::Row) + Send + 'static,
        {
            let bucket = bucket.clone();
            let key = key.clone();
            let mut row = row.clone();
            edit(&mut row);
            self.handle()
                .write(move |txn| {
                    rows::Table::open(txn)?.put(&bucket, &key, &row)?;
                    Ok(())
                })
                .await
                .map_err(Error::from)
        }
    }

    /// The `object_meta` row a restored marker takes: its identity and stamp,
    /// the empty-content ETag (`Row::stored` reads the row's own empty
    /// element), and no file match (a marker publishes nothing).
    fn marker_row(seq: u64, row: &rows::Row) -> meta::Stored {
        row.stored(seq, 0, row.created, 0)
    }

    #[async_trait]
    impl VersionOps for FsStorage {
        async fn bucket_versioning(&self, bucket: &Name) -> Result<State, Error> {
            self.ensure_bucket(bucket).await?;
            self.versions(bucket).state(bucket).await.map_err(projected)
        }

        async fn set_bucket_versioning(&self, bucket: &Name, state: State) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            // The state flip is serialized with the write path: every write
            // reads the state inside its own hold of this lock, so without
            // the same lock here a write that overlapped the flip could act
            // on the old state after the flip answered (orderable, but only
            // by an argument; the lock makes the serialization the obvious
            // reading).
            let _guard = self.lock_bucket_mutations(bucket).await;
            self.versions(bucket)
                .set_state(bucket, state)
                .await
                .map_err(projected)
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
            // The version-addressed answer's `Info` reports the bucket's
            // state too (the carrier the server reads for the identity
            // rule): the resolution runs no write plan, so the state is
            // read here — one point read per version-addressed read.
            let records_versions = self.records_versions(bucket).await?;
            match self.locate(bucket, key, version).await? {
                Found::Marker(stored) => Ok(ReadResult::Marker {
                    id: stored.version_id,
                    last_modified: from_nanos(stored.mtime),
                }),
                Found::Current { stored, path, .. } => {
                    // The current object's read is the plain read's, exactly —
                    // one policy open, the shared staleness gate on the open
                    // handle, the read path's own heal where the gate degrades
                    // the recorded identity — but from the slot already
                    // resolved: the key is never resolved twice
                    // (`serve_current`). A drifted slot serves the current
                    // bytes as the null version, never under the stale
                    // identifier.
                    let Some(path) = path else {
                        // The slot's row describes content whose file is gone
                        // (an out-of-band removal): the plain read's own
                        // answer for a key with no object.
                        return Err(storage::no_such_key(key).into());
                    };
                    let (file, size, mtime, row, version_id) = self
                        .serve_current(bucket, key, &path, stored.as_ref())
                        .await?;
                    let (body, served_range) = FsStorage::object_body(file, size, range).await?;
                    Ok(ReadResult::Object(GetObjectResult {
                        info: FsStorage::info_of(
                            key,
                            size,
                            mtime,
                            row,
                            version_id,
                            records_versions,
                        ),
                        body,
                        served_range,
                    }))
                }
                Found::Row(row) if row.kind == rows::Kind::Marker => Ok(ReadResult::Marker {
                    id: row.id(),
                    last_modified: from_nanos(row.created),
                }),
                Found::Row(row) => {
                    // The resolve runs before the open (a bad range is
                    // answered without touching the store); the open then
                    // takes the caller's own option — one call, two
                    // argument values.
                    let served_range = range.map(|range| range.resolve(row.size)).transpose()?;
                    let body = self
                        .versions(bucket)
                        .blobs()
                        .open(&row.blob(), range)
                        .await
                        .map_err(projected)?;
                    Ok(ReadResult::Object(GetObjectResult {
                        info: row.info(key, records_versions),
                        body,
                        served_range,
                    }))
                }
            }
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
            let records_versions = self.records_versions(bucket).await?;
            match self.locate(bucket, key, version).await? {
                Found::Marker(stored) => Ok(HeadResult::Marker {
                    id: stored.version_id,
                    last_modified: from_nanos(stored.mtime),
                }),
                Found::Current { stored, path, .. } => {
                    // The same resolution the get serves (and the plain
                    // head): one open, one gate, one heal — the two can never
                    // disagree about the current object's size or identity.
                    let Some(path) = path else {
                        return Err(storage::no_such_key(key).into());
                    };
                    let (_file, size, mtime, row, version_id) = self
                        .serve_current(bucket, key, &path, stored.as_ref())
                        .await?;
                    Ok(HeadResult::Object(FsStorage::info_of(
                        key,
                        size,
                        mtime,
                        row,
                        version_id,
                        records_versions,
                    )))
                }
                Found::Row(row) if row.kind == rows::Kind::Marker => Ok(HeadResult::Marker {
                    id: row.id(),
                    last_modified: from_nanos(row.created),
                }),
                Found::Row(row) => Ok(HeadResult::Object(row.info(key, records_versions))),
            }
        }

        async fn delete_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
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
                Found::Current { id, seq, .. } => {
                    // The current object version is permanently removed: the
                    // newest remaining version becomes current again, or the
                    // key disappears with it. The removal goes through the
                    // delete path's own helper and PROPAGATES: a file that
                    // could not be removed must not be reported as a
                    // permanently deleted version (the surviving file would
                    // self-heal into a fresh null version while the caller
                    // believed it gone).
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
                Found::Marker(stored) => {
                    // Removing the current marker shows the version it hid.
                    remove_object_file(&path, *self.follow_symlinks()).await?;
                    let removed = (wire_of(stored.version_id.as_ref()), stored.version_seq);
                    self.restore(bucket, key, Some(&removed)).await?;
                    prune_parents(&path, &dir).await;
                    Ok(Marker {
                        id: stored.version_id,
                        is_delete_marker: true,
                    })
                }
                Found::Row(row) => {
                    // No file is touched — the noncurrent version lives in the
                    // block store — so nothing can have been emptied here.
                    self.versions(bucket)
                        .expire(bucket, key, row.seq)
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
            let judged = Rows::new(self.clone())
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

        async fn version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Tags, Error> {
            self.ensure_bucket(bucket).await?;
            match self.locate(bucket, key, version).await? {
                Found::Current { stored, .. } => Ok(stored.tags),
                Found::Row(row) if row.kind == rows::Kind::Object => Ok(row.tags()),
                _ => Err(storage::marker_has_no_metadata(version)),
            }
        }

        async fn set_version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            tags: Tags,
        ) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            match self.locate(bucket, key, version).await? {
                Found::Current { .. } => self
                    .meta_store()
                    .set_tags(bucket, key, &tags)
                    .await
                    .map(|_| ()),
                Found::Row(row) if row.kind == rows::Kind::Object => {
                    self.rewrite_row(bucket, key, &row, move |row| {
                        row.tags = tags.to_wire();
                    })
                    .await
                }
                _ => Err(storage::marker_has_no_metadata(version)),
            }
        }

        async fn version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Acl, Error> {
            self.ensure_bucket(bucket).await?;
            match self.locate(bucket, key, version).await? {
                Found::Current { stored, .. } => Ok(Acl {
                    owner: stored.owner,
                    grants: stored.acl.grants,
                }),
                Found::Row(row) if row.kind == rows::Kind::Object => Ok(Acl {
                    owner: row.owner(),
                    grants: row.acl().grants,
                }),
                _ => Err(storage::marker_has_no_metadata(version)),
            }
        }

        async fn set_version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            acl: &Acl,
        ) -> Result<(), Error> {
            self.ensure_bucket(bucket).await?;
            let grants = acl.grants.clone();
            match self.locate(bucket, key, version).await? {
                // Replace-all on the grants, owner preserved — the object ACL
                // put's contract, per version.
                Found::Current { .. } => self
                    .meta_store()
                    .set_acl(bucket, key, &grants)
                    .await
                    .map(|_| ()),
                Found::Row(row) if row.kind == rows::Kind::Object => {
                    let wire = Acl {
                        owner: None,
                        grants,
                    }
                    .to_grants_wire();
                    self.rewrite_row(bucket, key, &row, move |row| {
                        row.acl = wire;
                    })
                    .await
                }
                _ => Err(storage::marker_has_no_metadata(version)),
            }
        }
    }
}
