//! The version plane: the version-scoped operations, the gated slot port and the row plane
//! the state engine runs on.
//!
//! `tinio-version` owns the state machine and the operation bodies the engine carries; what this
//! module adds is what only a backend can:
//!
//! - **the [`Slots`] gate** — the object row decides whether the key exists, and the slot
//!   beside it contributes the recorded identity and the marker bit. The store is the row
//!   plane; the gate is the only type this crate adds, because [`CurrentRows`] lives in
//!   `tinio-version` and `tinio-store-pg` does not depend on it;
//! - **the marker-write port** ([`MarkerWrite`] over [`PgStorage`]) — the version group's
//!   `create_delete_marker` carries the ordering a versioned `DELETE` takes, and the one step it
//!   cannot is the slot write: a marker is a recorded slot with **no** version row and no
//!   contract call writes one (the `put_version` a row-shaped marker would take displaces the
//!   null version a suspended delete must leave in history, and `advance_slot` derives the bit
//!   from a row). So the store's `mark_current_marker` — the statement that writes the slot and
//!   materializes the marker's object row (PG23) — is this crate's [`MarkerWrite`]
//!   implementation, and `PgStorage::create_delete_marker` is one call into the engine body;
//! - **`list_object_versions`**, backend-side by decision (D44) because it needs the
//!   backend's "what is the current object" truth beside the slot;
//! - **the version facts a plane-free body cannot fill** (the read's identity and the
//!   records-versions bit, G3) and the write-side branch the plain engine bodies leave to
//!   their caller: the slot-carrying plan the multipart completion opens.
//!
//! Everything here is the crate's `versioning` feature: without it the module is the trait's
//! default impl, which is why [`PgStorage`]'s version surface then refuses by contract rather
//! than by accident.

#[cfg(not(feature = "versioning"))]
use tinio_core::{
    bucket::Name,
    object,
    storage::{TagWriteResult, VersionOps},
};

#[cfg(not(feature = "versioning"))]
impl VersionOps for crate::PgStorage {}

/// The stripped build's version facts: nothing, and the engine's own answer is already the
/// unversioned one — every read carries no identity and `records_versions: false`, and the
/// tag pair's write answers the contract's identity-free shape. The bodies exist so the
/// operation paths need no `#[cfg]` of their own (the seam must be the same call in every
/// build, or a stripped build's reads and tag writes would silently skip the step a full
/// build runs).
#[cfg(not(feature = "versioning"))]
impl crate::PgStorage {
    /// The version facts a read carries without a version plane: none.
    pub(crate) async fn fill_version_facts(
        &self,
        _bucket: &Name,
        _key: &object::Key,
        _info: &mut object::Info,
    ) -> Result<(), crate::Error> {
        Ok(())
    }

    /// The version facts a tag write carries without a version plane: none.
    pub(crate) async fn fill_tag_facts(
        &self,
        _bucket: &Name,
        _key: &object::Key,
        _answer: &mut TagWriteResult,
    ) -> Result<(), crate::Error> {
        Ok(())
    }
}

#[cfg(feature = "versioning")]
pub(crate) use self::enabled::{OpenedWrite, Plane, plane};

#[cfg(feature = "versioning")]
mod enabled {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tinio_blob::Store;
    use tinio_core::{
        acl::Acl,
        bucket::Name,
        content::ContentKey,
        meta::{BucketMeta, ObjectMeta, VersionMeta, object::Row as ObjectRow, version::Kind},
        object,
        storage::{self, ByteRange, TagWriteResult, VersionOps},
        version::{
            self, CurrentVersion, Entry, HeadResult, Id, ListParams, Listing, Marker, ReadResult,
            State,
        },
    };
    use tinio_ops::VersionPlane;
    use tinio_version::{
        Current, CurrentRows, Identity, Intent, MarkerWrite, Plan, Seqs, Versions, plan_write,
    };

    use crate::{
        _store_pg::{PgStore, blocks::PgBlocks, content::LiveGeneration},
        Error,
        readonly::Subject,
        storage::PgStorage,
    };

    /// The version plane every body takes: `tinio-version`'s state engine over the store's
    /// rows, pg's gated current slot, the content plane and the block plane.
    pub(crate) type Plane =
        VersionPlane<Versions<PgStore, PgStore, PgBlocks, Slots>, PgStore, PgBlocks>;

    /// Build the plane over `store` — once per storage, beside it.
    pub(crate) fn plane(store: Arc<PgStore>) -> Plane {
        let gate = Slots {
            store: Arc::clone(&store),
        };
        // The block plane and the state engine share one `Store` handle (version 1 stores a
        // whole blob as one block), so the engine's supersede and materialize calls and the
        // backend's own sweep read the same address space.
        let blobs = Store::new(PgBlocks::new((*store).clone()));
        VersionPlane {
            versions: Arc::new(Versions::new(
                (*store).clone(),
                gate,
                (*store).clone(),
                blobs.clone(),
            )),
            version_rows: store,
            blobs,
        }
    }

    /// pg's gated current slot.
    ///
    /// The engine's row plane is the store. This type is only the gate: the object row decides
    /// whether the key exists, and the slot beside it contributes the identity and the marker
    /// bit. The store cannot implement [`CurrentRows`] itself — the port is `tinio-version`'s
    /// and `tinio-store-pg` does not depend on the version engine.
    #[derive(Debug, Clone)]
    pub(crate) struct Slots {
        /// The store the gate reads.
        pub(crate) store: Arc<PgStore>,
    }

    /// The gated current slot: the object row's presence — the caller's fact, or the port's own
    /// probe — beside the slot row's own record, read in one statement.
    ///
    /// **The row decides `exists`.** For pg a key with no row holds nothing (PG23: a chunk
    /// without one is residue, never an object), while the slot row beside it contributes the
    /// two facts the row does not carry: the recorded identity and the marker bit. The two are
    /// one state — `ObjectMeta::remove` retires the slot with the row it belongs to — so a
    /// row-less key is a key no version was ever recorded for, and a key with a row but no slot
    /// is the null version's own state: content a bucket that was not recording versions
    /// received. The sequence the contract's [`Current`] does not carry is read with the slot,
    /// so a write's monotonic plan and the marker bit come from one round trip.
    async fn gated_current(
        store: &PgStore,
        bucket: &Name,
        key: &object::Key,
        exists: bool,
    ) -> Result<Current, storage::Error> {
        if !exists {
            return Ok(Current::default());
        }
        let slot = store
            .slot_record(bucket, key)
            .await
            .map_err(storage::Error::from)?;
        Ok(match slot {
            None => Current {
                exists: true,
                version: None,
                is_delete_marker: false,
                seq: 0,
                degraded: false,
            },
            Some((seq, current)) => Current {
                exists: true,
                version: current.version,
                is_delete_marker: current.is_delete_marker,
                seq,
                degraded: false,
            },
        })
    }

    #[async_trait]
    impl CurrentRows for Slots {
        async fn current(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<Current, storage::Error> {
            // The gate's own row probe decides whether the key exists; [`gated_current`] then
            // resolves the slot beside it.
            let exists = ObjectMeta::get(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
                .is_some();
            gated_current(&self.store, bucket, key, exists).await
        }
    }

    #[async_trait]
    impl VersionOps for PgStorage {
        async fn bucket_versioning(&self, bucket: &Name) -> Result<State, Error> {
            self.objects()
                .bucket_versioning(self.plane(), bucket)
                .await
                .map_err(Error::from)
        }

        async fn set_bucket_versioning(&self, bucket: &Name, state: State) -> Result<(), Error> {
            self.writable(Subject::Bucket(bucket))?;
            self.objects()
                .set_bucket_versioning(self.plane(), bucket, state)
                .await
                .map_err(Error::from)
        }

        // `ensure_rename_allowed` stays the trait's default: the rule is one function
        // (`storage::rename_allowed` applied to the state the trait reads), and a body here
        // could only spell it a second time. `rename_object` calls it.

        async fn get_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            range: Option<ByteRange>,
        ) -> Result<ReadResult, Error> {
            self.objects()
                .get_object_version(self.plane(), bucket, key, version, range)
                .await
                .map_err(Error::from)
        }

        async fn head_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<HeadResult, Error> {
            self.objects()
                .head_object_version(self.plane(), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        async fn delete_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Marker, Error> {
            self.writable(Subject::Key(key))?;
            self.objects()
                .delete_object_version(self.plane(), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        async fn list_object_versions(
            &self,
            bucket: &Name,
            params: ListParams,
        ) -> Result<Listing, Error> {
            self.list_all_versions(bucket, params).await
        }

        async fn current_version(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<CurrentVersion, Error> {
            // The bounded per-key probe behind the plain read's delete-marker disclosure: one
            // row read and, only when the row is there, one slot read. The **row** is the
            // key's current version (PG23), so its absence is `Absent` — reporting the slot of
            // a key whose row was removed would invent a marker or an object the key does not
            // hold.
            if ObjectMeta::get(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
                .is_none()
            {
                return Ok(CurrentVersion::Absent);
            }
            let marker = VersionMeta::current_slot(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
                .is_some_and(|slot| slot.is_delete_marker);
            Ok(if marker {
                CurrentVersion::Marker
            } else {
                CurrentVersion::Object
            })
        }

        async fn version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<object::Tags, Error> {
            self.objects()
                .version_tags(self.plane(), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        async fn set_version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            tags: object::Tags,
        ) -> Result<(), Error> {
            self.writable(Subject::Key(key))?;
            self.objects()
                .set_version_tags(self.plane(), bucket, key, version, tags)
                .await
                .map_err(Error::from)
        }

        async fn version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Acl, Error> {
            self.objects()
                .version_acl(self.plane(), bucket, key, version)
                .await
                .map_err(Error::from)
        }

        async fn set_version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            acl: &Acl,
        ) -> Result<(), Error> {
            self.writable(Subject::Key(key))?;
            self.objects()
                .set_version_acl(self.plane(), bucket, key, version, acl)
                .await
                .map_err(Error::from)
        }
    }

    /// The engine's marker-write port over this storage's store: a marker is a recorded slot
    /// with no version row, and the store's `mark_current_marker` is the one statement that
    /// writes that state (the slot's marker bit, the key's materialized object row and the
    /// stamp the move owes the version it supersedes, in one transaction).
    #[async_trait]
    impl MarkerWrite for PgStorage {
        async fn mark_current_marker(
            &self,
            bucket: &Name,
            key: &object::Key,
            id: Option<Id>,
            seq: u64,
            moved_off: Option<u64>,
        ) -> Result<(), storage::Error> {
            self.store
                .mark_current_marker(bucket, key, id, seq, moved_off)
                .await
                .map_err(storage::Error::from)
        }
    }

    /// A content-writing operation's version branch: the plan it opened with, the identity the
    /// plan minted and the state the plan read.
    ///
    /// The multipart completion takes the pair because its row write is its own (the retained
    /// part list, which the engine's versioned put deliberately clears), so it cannot call that
    /// body; everything else — the state read, the gated slot, the sequences, the supersede and
    /// the identity — is the shared policy, taken here rather than re-spelled.
    pub(crate) struct OpenedWrite {
        /// The version plan, or `None` on a bucket that records no versions.
        plan: Option<Plan>,
        /// The identity the plan minted (`None` on the null and unversioned shapes).
        id: Option<Id>,
        /// Whether the bucket records versions, as the plan's own state read answered.
        records: bool,
        /// The outgoing version's sequence when this open moved one into history.
        history_seq: Option<u64>,
    }

    impl OpenedWrite {
        /// The identity this write records (`None` = the null version, or no version at all).
        pub(crate) fn identity(&self) -> Option<Id> {
            self.id.clone()
        }

        /// Whether the bucket this write ran against records versions.
        pub(crate) fn records(&self) -> bool {
            self.records
        }

        /// The outgoing version's sequence when this open moved one into history.
        pub(crate) fn history_seq(&self) -> Option<u64> {
            self.history_seq
        }
    }

    impl PgStorage {
        /// The version plane this storage holds, as the engine bodies' parameter.
        pub(crate) fn plane(&self) -> &Plane {
            &self.plane
        }

        /// The two sequence facts a write plan needs: the newest version row's sequence and
        /// whether sequence `0` is taken.
        async fn write_seqs(&self, bucket: &Name, key: &object::Key) -> Result<Seqs, Error> {
            Ok(Seqs {
                latest: VersionMeta::latest(&*self.store, bucket, key)
                    .await
                    .map_err(storage::Error::from)?
                    .map(|(seq, _)| seq)
                    .unwrap_or(0),
                null_at_zero: VersionMeta::version_seq_of(&*self.store, bucket, key, None)
                    .await
                    .map_err(storage::Error::from)?
                    == Some(0),
            })
        }

        /// Open a content write's version branch: read the state and the gated slot, plan the
        /// write, and **supersede** the outgoing version — which has to happen before the caller
        /// publishes, because the publish overwrites the bytes the supersede needs.
        ///
        /// A bucket that records no versions answers `plan: None` and the caller writes as it
        /// always would: the version-free path is a branch of the same operation, exactly as it
        /// is in the engine's own `put_object_versioned`.
        pub(crate) async fn open_put_write(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<OpenedWrite, Error> {
            let state = VersionMeta::bucket_state(&*self.store, bucket)
                .await
                .map_err(storage::Error::from)?;
            if !state.records_versions() {
                return Ok(OpenedWrite {
                    plan: None,
                    id: None,
                    records: false,
                    history_seq: None,
                });
            }
            let current = self.plane().versions.current(bucket, key).await?;
            let seqs = self.write_seqs(bucket, key).await?;
            let plan = plan_write(Intent::Put, state, &current, &seqs);
            if plan.supersede {
                self.plane()
                    .versions
                    .supersede_with(bucket, key, &current)
                    .await?;
            }
            let id = match plan.identity {
                Identity::Fresh => Some(version::mint()),
                Identity::Null | Identity::Unversioned => None,
            };
            let history_seq = (plan.supersede && current.exists).then_some(current.seq);
            Ok(OpenedWrite {
                plan: Some(plan),
                id,
                records: true,
                history_seq,
            })
        }

        /// Close it: move the current slot to the identity the plan minted, after the row that
        /// records the version has landed. A no-op on a bucket that records none.
        pub(crate) async fn close_put_write(
            &self,
            bucket: &Name,
            key: &object::Key,
            write: &OpenedWrite,
        ) -> Result<(), Error> {
            if let Some(plan) = &write.plan {
                VersionMeta::advance_slot(&*self.store, bucket, key, write.id.clone(), plan.seq)
                    .await
                    .map_err(storage::Error::from)?;
            }
            Ok(())
        }

        /// Take a completion's publish back when its record did not land: a recording bucket
        /// goes through the engine's versioned undo, and any other bucket restores the content
        /// head to the generation that was live before the publish.
        ///
        /// This is the version plane's half of the rollback — the unversioned restore lives in
        /// `complete.rs`, where a build without the plane can reach it — so the completion body
        /// holds no `#[cfg]` parameters and no suppression.
        #[expect(
            clippy::too_many_arguments,
            reason = "the undo takes the published generation, the one it replaces, and the versioned write's before-image"
        )]
        pub(crate) async fn take_back(
            &self,
            bucket: &Name,
            key: &object::Key,
            content_key: &ContentKey,
            published: LiveGeneration,
            previous: Option<LiveGeneration>,
            write: &OpenedWrite,
            before: Option<&ObjectRow>,
        ) -> Result<(), Error> {
            if write.records() {
                return self
                    .objects()
                    .undo_versioned_put(self.plane(), bucket, key, before, write.history_seq())
                    .await
                    .map_err(Error::from);
            }
            self.restore_published_live(bucket, content_key, published, previous)
                .await
        }

        /// Fill the version facts an engine read cannot (G3): the key's current identity and
        /// the bucket's records-versions bit.
        ///
        /// The identity is the slot row's own `version_id` — the same row the gated port
        /// ([`CurrentRows::current`]) reads — but read **without** the gate's object-row probe
        /// and sequence, neither of which this caller uses: for pg a slot row is always
        /// accompanied by its key's object row (`ObjectMeta::remove` retires the pair, and
        /// `mark_current_marker` materializes the row), so the two answers name the same version
        /// while this one costs a query instead of three. A bucket that records none reports the
        /// engine's own pair, which is what the unversioned wire expects.
        pub(crate) async fn fill_version_facts(
            &self,
            bucket: &Name,
            key: &object::Key,
            info: &mut object::Info,
        ) -> Result<(), Error> {
            let state = VersionMeta::bucket_state(&*self.store, bucket)
                .await
                .map_err(storage::Error::from)?;
            info.bucket_records_versions = state.records_versions();
            if !state.records_versions() {
                info.version_id = None;
                return Ok(());
            }
            let current = VersionMeta::current_slot(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?;
            info.version_id = current.and_then(|slot| slot.version);
            Ok(())
        }

        /// Fill a tag write's version fields: the identity the tags landed on and the
        /// records-versions bit.
        ///
        /// A write that touched nothing keeps the engine's own answer — the contract's
        /// identity-free pair — which is the one case the caller can see: the idempotent tag
        /// delete that met no object answers `(None, false)` and must not start naming a state
        /// it never touched.
        pub(crate) async fn fill_tag_facts(
            &self,
            bucket: &Name,
            key: &object::Key,
            answer: &mut TagWriteResult,
        ) -> Result<(), Error> {
            if ObjectMeta::get(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
                .is_none()
            {
                return Ok(());
            }
            let state = VersionMeta::bucket_state(&*self.store, bucket)
                .await
                .map_err(storage::Error::from)?;
            answer.bucket_records_versions = state.records_versions();
            if state.records_versions() {
                // The slot row alone: the `ObjectMeta::get` above is the gate's own probe, so
                // asking the gated port here would read the row this call already read.
                let current = VersionMeta::current_slot(&*self.store, bucket, key)
                    .await
                    .map_err(storage::Error::from)?;
                answer.version_id = current.and_then(|slot| slot.version);
            }
            Ok(())
        }

        /// Create the delete marker a versioned `DELETE` makes current.
        ///
        /// The ordering — supersede, remove the key's bytes and its object row, write the
        /// marker's slot, materialize the marker's own object row — is the engine body's
        /// ([`tinio_ops::ObjectOps::create_delete_marker`]). What stays here is the one step
        /// the engine cannot take: a marker is a recorded slot with **no** version row, and
        /// [`PgStorage`] is the [`MarkerWrite`] port whose store statement writes that state.
        pub(crate) async fn create_delete_marker(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<Marker, Error> {
            self.objects()
                .create_delete_marker(self.plane(), self, bucket, key)
                .await
                .map_err(Error::from)
        }

        /// `list_object_versions`: the bucket's keys, the current version first and its history
        /// newest-first after it, cut into a page by the contract's own pager.
        ///
        /// **The read is the bucket's rows, and it is deliberately unbounded.** fs walks the
        /// rows one window at a time — its listing reads a page's worth of keys and nothing
        /// else. pg's store carries no bounded per-key walk with a slot view (`ObjectMeta::walk`
        /// is the whole bucket, and the content plane's `scan` sees only keys with live bytes,
        /// which a marker is not), so this body reads the bucket's rows — key, gated slot and
        /// version rows joined into one answer per key, one statement for the whole bucket —
        /// then hands the entries to `version::paginate`. That is correct on every page and
        /// unbounded in cost — a bucket-wide read per listing page — which is a finding about
        /// the store's surface rather than a licence to guess: the bounded walk belongs with a
        /// statement that can express it, and this note is where a reader finds out. What the
        /// join does settle is the round trips: one per page rather than one per key.
        async fn list_all_versions(
            &self,
            bucket: &Name,
            params: ListParams,
        ) -> Result<Listing, Error> {
            if BucketMeta::row(&*self.store, bucket)
                .await
                .map_err(storage::Error::from)?
                .is_none()
            {
                return Err(storage::no_such_bucket(bucket).into());
            }
            let listed = self
                .store
                .bucket_versions(bucket)
                .await
                .map_err(storage::Error::from)?;
            let mut entries: Vec<Entry> = Vec::new();
            for versions in listed {
                // The current version's entry: the slot's identity, sequence and marker bit,
                // and the object row's own elements — a marker's row carries the empty ones
                // `advance_slot` materialized, so no stale element of the object it hid can be
                // served. The statement resolved the slot beside the row it read, so the gate's
                // own object-row probe has nothing left to decide.
                entries.push(Entry {
                    key: versions.key.clone(),
                    id: versions.version,
                    seq: versions.seq,
                    is_delete_marker: versions.is_delete_marker,
                    size: versions.elements.size,
                    last_modified: versions.elements.mtime,
                    etag: versions.elements.etag.clone(),
                });
                // Then the key's history, newest first: a row at the slot's own sequence is the
                // current version's own row (a marker's), never history — the contract's own
                // predicate, so two readers cannot disagree about one row.
                for stored in versions.history {
                    entries.push(Entry {
                        key: versions.key.clone(),
                        id: stored.id.clone(),
                        seq: stored.seq,
                        is_delete_marker: stored.kind == Kind::Marker,
                        size: stored.size,
                        last_modified: stored.created,
                        etag: stored.etag.clone(),
                    });
                }
            }
            Ok(version::paginate(&entries, &params))
        }
    }
}

/// The versioned delete's stamp leg — the second instance of the rule the `advance_slot` fix
/// closed: a versioned `DELETE` makes a delete marker current and moves the outgoing version
/// into history, and that history row must carry `became_noncurrent` (the RG5 rule stated as
/// `advance_slot`'s, discharged on fs's delete path by `stamp_superseded`). pg cleared the key's
/// slot before `mark_current_marker` ran, so the marker's own transaction never stamped the row
/// it moved off. Runs only with `TINIO_PG_DSN` set; offline the leg skips.
#[cfg(all(test, feature = "versioning"))]
#[expect(
    clippy::absolute_paths,
    reason = "test support: the leg spells the crate's public path absolutely on purpose"
)]
mod tests {
    use tinio_core::{
        acl::Acl,
        bucket,
        meta::VersionMeta,
        object,
        storage::{BucketOps, ObjectOps, VersionOps},
        version::State,
    };
    use tinio_util::testing::body;

    use crate::{
        _store_pg::{pool::Pool, test_support::TestDb},
        Access, PgStorage,
    };

    /// A versioned `DELETE` stamps the version it supersedes. The current version's history row
    /// carries `became_noncurrent` after the delete — exactly as fs's delete leaves it
    /// (`stamp_superseded`). Before the fix the row stayed `0`: the version the marker's slot
    /// moved off never said it was history, so its retention clock never started.
    #[tokio::test]
    async fn a_versioned_delete_stamps_the_version_it_supersedes() {
        let Ok(_) = std::env::var("TINIO_PG_DSN") else {
            eprintln!("skipping: TINIO_PG_DSN is not set");
            return;
        };
        let db = TestDb::create().await.expect("a scratch database");
        let storage = PgStorage::connect_with_pool(
            Pool::open(&db.dsn, 4).expect("a pool over the scratch database"),
            Access::ReadWrite,
        )
        .await
        .expect("a writable store over the scratch database");

        let bucket = bucket::name("delete-stamp").expect("a valid bucket name");
        let key = object::key("stamped.txt").expect("a valid key");
        let acl = Acl::default_private(None);
        storage
            .create_bucket(&bucket, None, &acl)
            .await
            .expect("the bucket");
        storage
            .set_bucket_versioning(&bucket, State::Enabled)
            .await
            .expect("versioning enabled");
        let staged = storage
            .stage_body(&bucket, &key, body("seed content"), None, None)
            .await
            .expect("the object is staged");
        let committed = storage
            .commit_object(&bucket, &key, staged, object::Tags::empty(), None, &acl)
            .await
            .expect("the versioned commit");
        let superseded = committed
            .version_id
            .clone()
            .expect("a versioned commit mints an identity");

        // The DELETE makes a marker current and moves the version just written into history.
        storage
            .delete_object(&bucket, &key)
            .await
            .expect("the versioned delete");

        let rows = VersionMeta::list_versions(storage.store(), &bucket, &key)
            .await
            .expect("the key's version rows");
        let history = rows
            .iter()
            .find(|row| row.id.as_ref() == Some(&superseded))
            .expect("the superseded version's history row");
        assert_ne!(
            history.became_noncurrent, 0,
            "the versioned delete stamps the version it supersedes"
        );

        db.drop_db().await;
    }
}
