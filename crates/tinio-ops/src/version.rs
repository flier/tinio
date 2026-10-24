//! The `version` group of the ops engine: the versioning-state pair, the
//! version-addressed reads and deletes and the per-version tag/ACL pairs.
//!
//! The group's inventory is nine operations (E14); this module writes all
//! nine — the two state operations, the version-addressed `get`, `head` and
//! `delete`, and the four per-version tag/ACL bodies. The delete's
//! current-version arm restores whatever version becomes current again, which
//! **moves the slot backwards to a real identity** — the one move
//! `advance_slot` refuses by its absolute guard — and that move is
//! [`VersionMeta::restore_slot`]'s: its one transaction commits the successor's
//! content elements, the slot, the dropped version row, its index entry and the
//! cleared retained part list. The successor itself is the key's newest
//! remaining row ([`VersionMeta::latest`] — the one-row read, never a walk of
//! the history). The removed version's bytes go through
//! `ObjectContent::remove` when nothing replaces them — no successor, or a
//! successor that is itself a marker — and an object successor's
//! `materialize` overwrites them. The no-successor arm's object row goes
//! through `ObjectMeta::remove`. The **marker-creating** delete a versioned `DELETE`
//! takes is [`ObjectOps::create_delete_marker`], a body beside the nine: its
//! ordering (supersede, remove the bytes and the row, write the marker's slot,
//! materialize the marker's object row) is shared here, while the slot write
//! itself is the backend's [`MarkerWrite`] port — a marker is a recorded slot
//! with **no version row**, a state no contract write expresses, and the
//! null-identity marker a suspended delete makes must not displace the null
//! version the same delete leaves in history. A marker-port failure after a
//! supersede puts that version back through [`VersionMeta::restore_slot`].
//! `list_object_versions` is the backend's own (E14), because it needs the
//! backend's "what is the current object" truth beside the F01/F04 gate — and
//! so are the two methods the trait keeps outside the group: `current_version`
//! (the bounded per-key probe behind the plain read's marker disclosure) and
//! `ensure_rename_allowed` (the rename guard the trait's own default body
//! answers).
//!
//! # The handles
//!
//! The bodies are methods on [`ObjectOps`] taking the version plane beside the
//! receiver — the shape [`ObjectOps::put_object_versioned`] already has. The version group needs exactly the handles that receiver
//! carries: the **object rows** (a current version's metadata home on the D37
//! model, which keeps the current version in the object row and the version
//! table for history) and the **object content** (a current version's bytes),
//! beside the plane's state engine, version rows and block plane.
//!
//! # The resolution, and why the gate is the plane's
//!
//! Which version a request addresses is resolved from the state engine's
//! **gated** current slot first (`Versions::current`, the backend's
//! `CurrentRows` port — the F01/F04 staleness rule a read applies) and from
//! the version rows' own identity index second. The gate is the whole reason
//! the first read is the port's and not the rows': a recorded identity the
//! content no longer matches must never be served as if it still named it, and
//! `Metadata::get` deliberately does not gate (E14). A recorded identity the
//! gate degraded therefore resolves **no** version at all — it is not the
//! current version any more, and it has no row of its own in history.
//!
//! # What answers which arm
//!
//! - **The current version's content** is served by the plain read
//!   ([`ObjectOps::get_object`], [`ObjectOps::head_object`]): one policy open,
//!   the shared staleness gate, the read path's own heal — the engine's one
//!   home for the read-heal (E13) — and this group only labels the answer with
//!   the identity the slot resolved and the bucket's versioning state.
//! - **A noncurrent version** is served from its own version row and the block
//!   plane, with the row's recorded elements (the ETag a multipart-composed
//!   form carries included) — never the current object's.
//! - **A delete marker** is a **success**, not an error (`ReadResult::Marker`,
//!   `HeadResult::Marker`): the caller needs the marker's identity and stamp to
//!   render its own answer. The marker's metadata, though, is a refusal: the
//!   tag/ACL pairs answer the contract's not-found class naming the marker
//!   ([`storage::marker_has_no_metadata`], the rule's one home).
//!
//! # The heal's identity rule, and what a backend owes it
//!
//! The read-heal these bodies reach is the plain read's (`healed_row`), and
//! that heal **preserves** the object row's
//! recorded version identity: the contract's preserving write carries it over,
//! because the engine must never un-version an object. A backend whose gate
//! **clears** a stale identity instead — fs's does, in the same write that
//! heals — therefore owes the version group one thing: its clear must run
//! **before** the engine body, or the healed row would keep naming the identity
//! the gate retired, and the next request would resolve that identity against
//! the refreshed row and serve the drift under it. fs takes that step in its
//! trait implementation (`retire_degraded_identity`) for exactly the addresses
//! that reach the current-version arm. A backend that only ever preserves (one
//! whose rows and content are one transaction) owes nothing.
//!
//! # The marker's stamp
//!
//! The current marker's `last_modified` is the object row's `mtime`: a current
//! marker is a slot with no version row, and the marker port materializes that
//! row. A version row at the slot's sequence wins when an earlier shape left
//! one. A superseded marker is a history row; the slot move stamps it.

use std::time::SystemTime;

use tracing::instrument;

use crate::{
    _core::{
        ByteRange, ETag, GetObjectResult,
        acl::{self, Acl},
        blob::BlockStore,
        bucket,
        content::{ContentKey, ObjectContent, PublishId},
        meta::{
            Metadata, ObjectMeta, VersionMeta,
            object::{PartRow, Row},
            version::{Kind, Stored},
        },
        object, object_lock,
        storage::{self, Error, marker_has_no_metadata},
        version::{self, HeadResult, Id, Marker, ReadResult, State, wire_of},
    },
    _version::{
        Current, CurrentRows, Identity as WriteIdentity, Intent, MarkerWrite, Plan, Seqs, Versions,
        plan_write,
    },
    ObjectOps, StagedBody,
    object::mint_publish_id,
    ops::{VersionPlane, projected},
};

/// The version plane this group's bodies take: the state engine over the
/// backend's ports, the version rows beside it and the block plane.
///
/// `G` defaults to `R` for a backend whose rows are also the gate. pg passes
/// the store as the rows and a separate gate.
type Plane<R, C, B, G = R> = VersionPlane<Versions<R, C, B, G>, R, B>;

/// What the plane resolved the addressed version to.
enum Found {
    /// The key's current version, and it is content. The object row the gate
    /// judged rides along: a current version has no version-table row of its
    /// own (D37), so that row **is** the version's metadata, read once per
    /// resolution so the bodies that answer from it never re-read it.
    Current(Box<Row>),
    /// The key's current version, and it is a delete marker: no content and no
    /// metadata, with the stamp the marker was written at.
    Marker { last_modified: SystemTime },
    /// A noncurrent version: its version-table row is the whole truth.
    Row(Box<Stored>),
}

/// What a version-addressed read resolved to, after the bucket's state is
/// known. The current arm is not opened here: get opens a body, head does not.
enum Addressed {
    /// The current version is content. The caller serves it through the
    /// plain read and labels the answer.
    Current { records_versions: bool },
    /// A delete marker, current or historical.
    Marker {
        id: Option<Id>,
        last_modified: SystemTime,
    },
    /// A noncurrent object version. The row is the whole truth.
    History {
        records_versions: bool,
        row: Box<Stored>,
    },
}

/// Where a version's tags or ACL live. A marker has none.
enum VersionHome {
    /// The current version: the object row.
    Current(Box<Row>),
    /// A noncurrent object version: its version row.
    History(Box<Stored>),
}

impl<M, C> ObjectOps<M, C>
where
    M: Metadata,
    C: ObjectContent,
{
    /// Resolve the version a request addresses: the gated current slot first,
    /// the version rows' identity index second.
    ///
    /// The gate is the port's (`Versions::current`), never the rows': a
    /// recorded identity its content no longer matches is **not** the current
    /// version, so a request that names it resolves nothing (`NoSuchVersion`)
    /// instead of serving the drift under a stale identifier.
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    async fn resolve<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Found, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let current = plane
            .versions
            .current(bucket, key)
            .await
            .map_err(projected)?;
        if current.exists && current.version.as_ref() == version {
            if !current.is_delete_marker {
                let Some(row) = ObjectMeta::get(&*self.meta, bucket, key)
                    .await
                    .map_err(projected)?
                else {
                    // The slot's row is gone from under the gate's read: the
                    // key has no current version to describe.
                    return Err(storage::no_such_key(key));
                };
                return Ok(Found::Current(Box::new(row)));
            }
            let last_modified = self.marker_stamp(plane, bucket, key, current.seq).await?;
            return Ok(Found::Marker { last_modified });
        }
        let wire = wire_of(version);
        // The identity index names the row's sequence; the row must still
        // carry the identity the index claims, or the entry is stale and the
        // version it named is not there.
        let Some(seq) = VersionMeta::version_seq_of(&*plane.version_rows, bucket, key, version)
            .await
            .map_err(projected)?
        else {
            return Err(storage::no_such_version(wire));
        };
        let Some(row) = VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?
        else {
            return Err(storage::no_such_version(wire));
        };
        if row.id.as_ref() != version {
            return Err(storage::no_such_version(wire));
        }
        Ok(Found::Row(Box::new(row)))
    }

    /// The stamp a version-addressed read of the current delete marker
    /// answers, read where the model records it (see the module doc).
    async fn marker_stamp<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<SystemTime, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        if let Some(row) = VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?
        {
            return Ok(row.created);
        }
        match ObjectMeta::get(&*self.meta, bucket, key)
            .await
            .map_err(projected)?
        {
            Some(row) => Ok(row.mtime),
            None => Err(storage::no_such_key(key)),
        }
    }

    /// One noncurrent version row's [`object::Info`] — the version-addressed
    /// read and head's answer for a history version. The row's recorded
    /// elements are served verbatim (the ETag a multipart-composed
    /// `MD5-of-MD5s-N` form carries included), and the identity is the row's
    /// own (the null version answers `None`).
    fn version_info(
        key: &object::Key,
        row: &Stored,
        bucket_records_versions: bool,
    ) -> object::Info {
        object::Info {
            key: key.clone(),
            size: row.size,
            last_modified: row.created,
            etag: row.etag.clone(),
            tags: row.tags.clone(),
            checksum: row.checksum.clone(),
            owner: row.owner.clone(),
            acl: row.acl.clone(),
            version_id: row.id.clone(),
            bucket_records_versions,
        }
    }

    /// The identity a current-version answer carries: the requested one while
    /// the slot still names it after the content was served, and the **null**
    /// version when it does not.
    ///
    /// This arm takes no lock (the locks are the backend's `Storage`
    /// implementation's), so a concurrent overwrite can commit between the
    /// resolution and the plain read's own resolution inside
    /// [`ObjectOps::get_object`]/[`ObjectOps::head_object`]. The answer then
    /// describes the bytes it actually served — which are not the version the
    /// request named — and the pre-engine version-addressed reads carried
    /// exactly this rule: a drifted slot serves the current bytes as the null
    /// version, never under the stale identifier. Labelling from the request
    /// alone would stamp the newer content with the older identity.
    fn served_identity(requested: Option<&Id>, served: &Current) -> Option<Id> {
        match served.version.as_ref() == requested {
            true => requested.cloned(),
            false => None,
        }
    }

    /// The bucket's versioning state (S3 GetBucketVersioning).
    ///
    /// # Errors
    ///
    /// The state engine's failures; the backend's own bucket checks stay with
    /// its trait implementation.
    pub async fn bucket_versioning<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
    ) -> Result<State, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        plane.versions.state(bucket).await.map_err(projected)
    }

    /// Record the bucket's versioning state (S3 PutBucketVersioning).
    ///
    /// The transition rule (a versioned bucket may be suspended but never
    /// returns to `Unversioned`) is the caller's, above this body; the
    /// engine's write is the state engine's own.
    ///
    /// # Errors
    ///
    /// The state engine's failures.
    pub async fn set_bucket_versioning<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        plane
            .versions
            .set_state(bucket, state)
            .await
            .map_err(projected)
    }

    /// Resolve one addressed version into the three answers get and head share.
    async fn address<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Addressed, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let records_versions = plane
            .versions
            .state(bucket)
            .await
            .map_err(projected)?
            .records_versions();
        match self.resolve(plane, bucket, key, version).await? {
            Found::Current(_) => Ok(Addressed::Current { records_versions }),
            Found::Marker { last_modified } => Ok(Addressed::Marker {
                id: version.cloned(),
                last_modified,
            }),
            Found::Row(row) => match row.kind {
                Kind::Marker => Ok(Addressed::Marker {
                    id: row.id.clone(),
                    last_modified: row.created,
                }),
                Kind::Object => Ok(Addressed::History {
                    records_versions,
                    row,
                }),
            },
        }
    }

    /// Project [`Found`] onto the row a tag or ACL body writes. Markers refuse.
    async fn version_home<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<VersionHome, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.resolve(plane, bucket, key, version).await? {
            Found::Current(row) => Ok(VersionHome::Current(row)),
            Found::Row(row) if row.kind == Kind::Object => Ok(VersionHome::History(row)),
            Found::Marker { .. } | Found::Row(_) => Err(marker_has_no_metadata(version)),
        }
    }

    /// Read one version of an object (S3 GetObject with a `versionId`).
    ///
    /// The bucket's state is read once, before the resolution: the answer's
    /// [`Info`](object::Info) reports it (`bucket_records_versions`), because
    /// this path resolves no write plan.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for a well-formed identity naming no version,
    /// `NoSuchKey` when the addressed current version's content is gone, and
    /// the planes' own failures.
    pub async fn get_object_version<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        range: Option<ByteRange>,
    ) -> Result<ReadResult, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.address(plane, bucket, key, version).await? {
            Addressed::Current { records_versions } => {
                let mut got = self.get_object(bucket, key, range).await?;
                // The label comes from the slot as the served content leaves
                // it, never from the request alone (`served_identity`): an
                // overwrite that commits inside the plain read makes these the
                // newer bytes, and they are then answered as the null version.
                let served = plane
                    .versions
                    .current(bucket, key)
                    .await
                    .map_err(projected)?;
                got.info.version_id = Self::served_identity(version, &served);
                got.info.bucket_records_versions = records_versions;
                Ok(ReadResult::Object(got))
            }
            Addressed::Marker { id, last_modified } => Ok(ReadResult::Marker { id, last_modified }),
            Addressed::History {
                records_versions,
                row,
            } => {
                // The range resolves against the version's own length
                // before anything opens: a bad range is answered without
                // touching the block plane.
                let served_range = range.map(|range| range.resolve(row.size)).transpose()?;
                let body = plane
                    .blobs
                    .open(&row.blob, range)
                    .await
                    .map_err(projected)?;
                Ok(ReadResult::Object(GetObjectResult {
                    info: Self::version_info(key, &row, records_versions),
                    body,
                    served_range,
                }))
            }
        }
    }

    /// The metadata of one version of an object (S3 HeadObject with a
    /// `versionId`), with the same resolution and the same carrier as
    /// [`get_object_version`](Self::get_object_version).
    ///
    /// # Errors
    ///
    /// As the version-addressed read: `NoSuchVersion`, `NoSuchKey` and the
    /// planes' failures.
    pub async fn head_object_version<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<HeadResult, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.address(plane, bucket, key, version).await? {
            Addressed::Current { records_versions } => {
                let mut info = self.head_object(bucket, key).await?;
                // The same label rule as the get's (`served_identity`).
                let served = plane
                    .versions
                    .current(bucket, key)
                    .await
                    .map_err(projected)?;
                info.version_id = Self::served_identity(version, &served);
                info.bucket_records_versions = records_versions;
                Ok(HeadResult::Object(info))
            }
            Addressed::Marker { id, last_modified } => Ok(HeadResult::Marker { id, last_modified }),
            Addressed::History {
                records_versions,
                row,
            } => Ok(HeadResult::Object(Self::version_info(
                key,
                &row,
                records_versions,
            ))),
        }
    }

    /// Permanently remove one version — S3 `DeleteObject` with a `versionId`.
    ///
    /// Two shapes, decided by the key's gated current:
    ///
    /// - **the addressed identity is the current version** — it leaves and the newest
    ///   remaining version becomes current again (its bytes stream back onto the key path,
    ///   and `restore_slot` commits the row, the slot and the consumption of its own row in
    ///   one transaction). The current version's row leaves first when it has one (a marker
    ///   does; a content version holds no row of its own), so the removed marker cannot be
    ///   mistaken for its own successor. With no version left the key disappears: its bytes
    ///   and its object row go, and its slot with them — a row-less key is a key no version
    ///   was ever recorded for, so a later write plans from zero rather than being refused
    ///   by a sequence nothing records any more;
    /// - **a noncurrent version** — its row and its index entry leave in one transaction
    ///   ([`VersionMeta::expire_with`]). Its blocks stay: liveness is derived from the rows, and
    ///   `Cleanup`'s sweep drops what no row names.
    ///
    /// `bypass` is forwarded to the store's atomic removal, which enforces the
    /// identity's Object Lock (OL8, OL11): a COMPLIANCE retention or a legal
    /// hold refuses in either mode, an unexpired GOVERNANCE retention refuses
    /// unless the request's authority granted the bypass. A refusal removes
    /// nothing; a delete marker is never protected (R19).
    ///
    /// The successor scan is [`VersionMeta::latest`]'s one row, never a walk of the history,
    /// and a row that is the current version's own — the sequence the slot records — is
    /// consumed before the scan so the scan cannot name it.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` (the contract's not-found class, naming the wire the caller asked
    /// for) when the identity names no version of the key, and `Locked` when the
    /// identity's lock refuses the removal.
    pub async fn delete_object_version<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        bypass: object_lock::Bypass,
    ) -> Result<Marker, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let current = plane
            .versions
            .current(bucket, key)
            .await
            .map_err(projected)?;
        if current.exists && current.version.as_ref() == version {
            let removed = Marker {
                id: version.cloned(),
                is_delete_marker: current.is_delete_marker,
            };
            // The current version's own row: a marker's sits at the slot's sequence, a
            // content version has none. `expire_with` is idempotent, so the content arm's
            // call is the one that finds nothing; it enforces the identity's lock and
            // removes it with the version (OL8).
            VersionMeta::expire_with(&*plane.version_rows, bucket, key, current.seq, bypass)
                .await
                .map_err(projected)?;
            if let Some(row) = Self::successor(plane, bucket, key).await? {
                self.restore_version(plane, bucket, key, row.seq, row)
                    .await?;
            } else {
                self.content
                    .remove(bucket, &ContentKey::Object(key.clone()))
                    .await
                    .map_err(projected)?;
                let _ = ObjectMeta::remove(&*self.meta, bucket, key)
                    .await
                    .map_err(projected)?;
            }
            return Ok(removed);
        }
        let wire = wire_of(version);
        let Some(seq) = VersionMeta::version_seq_of(&*plane.version_rows, bucket, key, version)
            .await
            .map_err(projected)?
        else {
            return Err(storage::no_such_version(wire));
        };
        let Some(row) = VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?
        else {
            return Err(storage::no_such_version(wire));
        };
        // The index can name a sequence whose row carries another identity.
        // Expiring it would drop that other version; the read path already
        // answers `NoSuchVersion` for the same disagreement.
        if row.id.as_ref() != version {
            return Err(storage::no_such_version(wire));
        }
        VersionMeta::expire_with(&*plane.version_rows, bucket, key, seq, bypass)
            .await
            .map_err(projected)?;
        Ok(Marker {
            id: row.id.clone(),
            is_delete_marker: row.kind == Kind::Marker,
        })
    }

    /// Set, clear or leave a version's retention — S3 `PutObjectRetention` and
    /// the management plane's `put_retention` (OL5, OL11, OL23, OL29).
    ///
    /// The address names the version: [`version::Address::Current`] resolves
    /// the current slot's identity (a key with no current version is
    /// `NoSuchVersion`), [`version::Address::Null`] the null version, and
    /// [`version::Address::Id`] a specific id. The store's one-transaction
    /// write asserts the identity exists and is not a marker, requires an
    /// enabled bucket configuration and `Enabled` versioning, and applies the
    /// model's `apply_retention`: `Some` sets the retention, `None` clears it
    /// (the legal hold is preserved), and a COMPLIANCE retention cannot be
    /// shortened or removed — the governance bypass applies only to an
    /// unexpired GOVERNANCE retention.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for a marker or an absent identity, `InvalidBucketState`
    /// for a bucket without an enabled configuration (or without `Enabled`
    /// versioning), `NoSuchBucket` for a missing bucket, and `Locked` for a
    /// refused shortening or removal.
    #[expect(
        clippy::too_many_arguments,
        reason = "the retention write's field set — plane, bucket, key, address, requested retention, bypass and clock — is the request's own, forwarded to the store's atomic write"
    )]
    pub async fn set_retention<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        requested: Option<&object_lock::Retention>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let identity = Self::lock_identity(plane, bucket, key, address).await?;
        VersionMeta::set_retention(
            &*plane.version_rows,
            bucket,
            key,
            identity.as_ref(),
            requested,
            bypass,
            now,
        )
        .await
        .map_err(projected)
    }

    /// Set a version's legal hold — S3 `PutObjectLegalHold` and the plane's
    /// `put_legal_hold` (OL5, OL11, OL23, OL29).
    ///
    /// The address resolution and the store's identity, configuration and
    /// versioning assertions are [`ObjectOps::set_retention`]'s. The retention
    /// is preserved and a legal hold has no bypass; [`object_lock::LegalHold::Off`]
    /// clears it.
    ///
    /// # Errors
    ///
    /// The same classes as [`ObjectOps::set_retention`].
    pub async fn set_legal_hold<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        hold: object_lock::LegalHold,
    ) -> Result<object_lock::Lock, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let identity = Self::lock_identity(plane, bucket, key, address).await?;
        VersionMeta::set_legal_hold(&*plane.version_rows, bucket, key, identity.as_ref(), hold)
            .await
            .map_err(projected)
    }

    /// Apply the retention member and the legal-hold member in **one** store
    /// transaction — the management plane's atomic object `PUT` (OL23).
    ///
    /// The address resolution is [`ObjectOps::set_retention`]'s. `retention`
    /// is three-state (`None` leaves, `Some(None)` clears, `Some(Some(_))`
    /// sets) and `legal_hold` is two-state (`None` leaves, a value sets). A
    /// refused retention applies neither member, and the legal hold itself
    /// never refuses.
    ///
    /// # Errors
    ///
    /// The same classes as [`ObjectOps::set_retention`].
    #[expect(
        clippy::too_many_arguments,
        reason = "the combined write's field set — plane, bucket, key, address, both members, bypass and clock — is the request's own"
    )]
    pub async fn update_lock<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
        retention: Option<Option<&object_lock::Retention>>,
        legal_hold: Option<object_lock::LegalHold>,
        bypass: object_lock::Bypass,
        now: SystemTime,
    ) -> Result<Option<object_lock::Lock>, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let identity = Self::lock_identity(plane, bucket, key, address).await?;
        VersionMeta::set_lock(
            &*plane.version_rows,
            bucket,
            key,
            identity.as_ref(),
            retention,
            legal_hold,
            bypass,
            now,
        )
        .await
        .map_err(projected)
    }

    /// Resolve a lock address to the store's identity spelling: `None` is the
    /// null version, `Some(id)` a specific version. `Current` reads the slot —
    /// the store then asserts the identity is a non-marker object version — and
    /// a key with no current version is `NoSuchVersion`.
    async fn lock_identity<R, B, G>(
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        address: &version::Address,
    ) -> Result<Option<Id>, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match address {
            version::Address::Current => {
                let current = VersionMeta::current_slot(&*plane.version_rows, bucket, key)
                    .await
                    .map_err(projected)?;
                match current {
                    Some(current) if current.exists => Ok(current.version),
                    _ => Err(storage::no_such_version(wire_of(None))),
                }
            }
            version::Address::Null => Ok(None),
            version::Address::Id(id) => Ok(Some(id.clone())),
        }
    }

    /// The version that becomes current after the current one is removed.
    ///
    /// `latest` is the one-row read. It fails closed when the newest row is
    /// corrupt, answering `None` rather than an older row. `list_versions`
    /// skips that corrupt row, so the newest row it still returns is the
    /// successor. No decodable row means the key has no version left.
    async fn successor<R, B, G>(
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Stored>, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        if let Some((_, row)) = VersionMeta::latest(&*plane.version_rows, bucket, key)
            .await
            .map_err(projected)?
        {
            return Ok(Some(row));
        }
        Ok(
            VersionMeta::list_versions(&*plane.version_rows, bucket, key)
                .await
                .map_err(projected)?
                .into_iter()
                .max_by_key(|row| row.seq),
        )
    }

    /// Make one version current again: its content back onto the key path, then
    /// `restore_slot`'s one transaction.
    ///
    /// The order is the restore's contract — the bytes first, so a crash between the two
    /// leaves the version's row, and so its content, recoverable — and the row this call
    /// writes is the version's own recorded elements (a marker's is the empty row
    /// `advance_slot` materializes, with the marker's own creation stamp).
    async fn restore_version<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        row: Stored,
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let acl = Acl {
            owner: row.owner.clone(),
            grants: row.acl.grants.clone(),
        };
        let elements = if row.kind == Kind::Marker {
            // The key still holds the deleted version's bytes. A marker
            // publishes nothing over them, so a plain read would keep serving
            // the deleted object. Remove them before the slot moves: a crash
            // between the two leaves the successor's row, and the key reads
            // as gone.
            self.content
                .remove(bucket, &ContentKey::Object(key.clone()))
                .await
                .map_err(projected)?;
            Row {
                etag: ETag::EMPTY,
                size: 0,
                mtime: row.created,
                identity: 0,
                tags: row.tags.clone(),
                checksum: None,
                owner: row.owner.clone(),
                acl,
            }
        } else {
            // The version's bytes stream back onto the key path (the restore's content
            // half), and the entry that publish left carries the length and the identity the
            // row records.
            let published = plane
                .versions
                .materialize(bucket, key, &row)
                .await
                .map_err(projected)?;
            let entry = self.content.stat(bucket, key).await.map_err(projected)?;
            Row {
                etag: row.etag.clone(),
                size: published.as_ref().map_or(row.size, |body| body.size),
                mtime: entry
                    .as_ref()
                    .and_then(|entry| entry.mtime)
                    .unwrap_or(row.created),
                identity: entry.as_ref().map_or(0, |entry| entry.identity),
                tags: row.tags.clone(),
                checksum: row.checksum.clone(),
                owner: row.owner.clone(),
                acl,
            }
        };
        VersionMeta::restore_slot(
            &*plane.version_rows,
            bucket,
            key,
            &elements,
            row.id.clone(),
            seq,
        )
        .await
        .map_err(projected)
    }

    /// The slot, the sequence facts and the write plan one versioned write reads.
    ///
    /// Both versioned writes take this same triple. The caller has already
    /// read `state`; an unversioned put returns before this, so the extra
    /// reads stay on the versioned path.
    async fn load_plan<R, B, G>(
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        intent: Intent,
        state: State,
    ) -> Result<(Current, Plan), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let current = plane
            .versions
            .current(bucket, key)
            .await
            .map_err(projected)?;
        // `latest` fails closed on a corrupt newest row and answers `None`,
        // which is not the same as no history. The highest decodable row
        // still occupies its sequence, so the next write starts past that
        // one. A key with nothing current has no history to walk.
        let recorded = VersionMeta::latest(&*plane.version_rows, bucket, key)
            .await
            .map_err(projected)?
            .map(|(seq, _)| seq);
        let latest = match recorded {
            Some(seq) => seq,
            None if current.exists => VersionMeta::list_versions(&*plane.version_rows, bucket, key)
                .await
                .map_err(projected)?
                .into_iter()
                .map(|row| row.seq)
                .max()
                .unwrap_or(0),
            None => 0,
        };
        let seqs = Seqs {
            latest,
            null_at_zero: VersionMeta::version_seq_of(&*plane.version_rows, bucket, key, None)
                .await
                .map_err(projected)?
                == Some(0),
        };
        let plan = plan_write(intent, state, &current, &seqs);
        Ok((current, plan))
    }

    /// Create the delete marker a versioned `DELETE` makes current.
    ///
    /// The ordering is the engine's, over the contracts:
    ///
    /// 1. the plan ([`plan_write`] with [`Intent::Delete`]): whether the
    ///    outgoing version turns noncurrent, the marker's identity and its
    ///    sequence — the suspended shapes included, where the null version is
    ///    **removed** rather than moved and the marker takes the key's next
    ///    free sequence instead of the history row's;
    /// 2. the supersede, before the content goes (the outgoing version's bytes
    ///    are still the key's);
    /// 3. the key's bytes and its object row leave — a marker holds no
    ///    content, and leaving the row would leave the deleted object's
    ///    elements for the listing to read;
    /// 4. the marker becomes current **without a version row**, through the
    ///    backend's [`MarkerWrite`] port: the port records the marker bit,
    ///    materializes the key's (empty) object row and stamps the version the
    ///    slot moves off. A current marker this call did **not** supersede —
    ///    the null marker a repeated suspended delete replaces — expires any
    ///    row an earlier shape left at its sequence, so that residue cannot
    ///    grow a phantom history. A marker this call **did** supersede stays:
    ///    that row is the history the next delete restores.
    ///
    /// A marker-port failure after a supersede puts that version back — its
    /// bytes, its object row and its retained parts — and drops the history
    /// row this call wrote. The slot never moved, so the restore is
    /// [`VersionMeta::restore_slot`].
    ///
    /// A bucket that records no versions takes the version-free delete instead
    /// ([`delete_object`](Self::delete_object)): the state can flip between the
    /// caller's read and this body's, and the plan's own `marker` bit is the
    /// answer.
    ///
    /// # Errors
    ///
    /// The version engine's failures (a supersede race included), the row
    /// plane's, the content plane's and the marker port's.
    #[instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    pub async fn create_delete_marker<R, B, G, W>(
        &self,
        plane: &Plane<R, C, B, G>,
        markers: &W,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Marker, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
        W: MarkerWrite,
    {
        let state = plane.versions.state(bucket).await.map_err(projected)?;
        let (current, plan) = Self::load_plan(plane, bucket, key, Intent::Delete, state).await?;
        if !plan.marker {
            // The state is not recording versions, so the version-free delete
            // is the answer.
            return self.delete_object(bucket, key).await;
        }
        // A live current version the plan does not supersede — the suspended
        // null version, and the null marker a repeated suspended delete
        // replaces — is held in history for this call only. The marker's
        // success drops that row; a failure puts the version back. A crash
        // between the two leaves the row.
        let hold = !plan.supersede && current.exists && !current.degraded;
        let superseded = plan.supersede || hold;
        // Read before the supersede, so a parts-read failure leaves no history
        // row behind. `remove` and `restore_slot` both clear this list.
        let parts = if superseded {
            Some(
                ObjectMeta::parts(&*self.meta, bucket, key)
                    .await
                    .map_err(projected)?,
            )
        } else {
            None
        };
        if superseded {
            plane
                .versions
                .supersede_with(bucket, key, &current)
                .await
                .map_err(projected)?;
        }
        let id = match plan.identity {
            WriteIdentity::Fresh => Some(version::mint()),
            WriteIdentity::Null | WriteIdentity::Unversioned => None,
        };
        self.content
            .remove(bucket, &ContentKey::Object(key.clone()))
            .await
            .map_err(projected)?;
        let _ = ObjectMeta::remove(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        // A current marker this call did not hold can still carry a row an
        // earlier shape left at its sequence. Expire that residue so a
        // repeated suspended delete cannot grow a phantom history. A version
        // this call did supersede — the plan's own, or the held null version —
        // is the row at `current.seq`, and a failure still has to find it.
        if current.is_delete_marker && !superseded {
            VersionMeta::expire(&*plane.version_rows, bucket, key, current.seq)
                .await
                .map_err(projected)?;
        }
        match markers
            .mark_current_marker(
                bucket,
                key,
                id.clone(),
                plan.seq,
                // The row the marker's slot moves off: the version the supersede
                // above committed into history, stamped noncurrent in the
                // marker's own transaction (RG5 — the rule `advance_slot` and
                // `restore_slot` apply). Present only when a version was
                // superseded, a content version and a delete marker alike.
                superseded.then_some(current.seq),
            )
            .await
            .map_err(projected)
        {
            Ok(()) => {
                if hold {
                    // The held row is not history. Drop it now that the marker
                    // is current; a failure puts the held version back, so the
                    // delete does not land either.
                    if let Err(err) =
                        VersionMeta::expire(&*plane.version_rows, bucket, key, current.seq)
                            .await
                            .map_err(projected)
                    {
                        self.restore_held(
                            plane,
                            bucket,
                            key,
                            current.seq,
                            &parts.unwrap_or_default(),
                        )
                        .await?;
                        return Err(err);
                    }
                }
                Ok(Marker {
                    id,
                    is_delete_marker: true,
                })
            }
            Err(err) => {
                // The slot did not move. The supersede's history row is that
                // version, and `restore_slot` makes it current again — the
                // object row this call already removed is the slot's home, so
                // a plain row write would record it with no identity.
                if superseded {
                    self.restore_held(plane, bucket, key, current.seq, &parts.unwrap_or_default())
                        .await?;
                }
                Err(err)
            }
        }
    }

    /// Put back the version a delete held at `seq`, including the retained
    /// parts `restore_slot` clears.
    async fn restore_held<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        seq: u64,
        parts: &[PartRow],
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let Some(stored) = VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?
        else {
            return Ok(());
        };
        let kind = stored.kind;
        self.restore_version(plane, bucket, key, seq, stored)
            .await?;
        if kind == Kind::Object
            && !parts.is_empty()
            && let Some(row) = ObjectMeta::get(&*self.meta, bucket, key)
                .await
                .map_err(projected)?
        {
            ObjectMeta::put_with_parts(&*self.meta, bucket, key, &row, parts)
                .await
                .map_err(projected)?;
        }
        Ok(())
    }

    /// The tags of one version (S3 GetObjectTagging with a `versionId`).
    ///
    /// A delete marker holds no metadata: the refusal is the contract's
    /// not-found class naming the marker.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for an unknown identity or a marker address, and the
    /// planes' own failures.
    pub async fn version_tags<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object::Tags, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.version_home(plane, bucket, key, version).await? {
            VersionHome::Current(row) => Ok(row.tags),
            VersionHome::History(row) => Ok(row.tags),
        }
    }

    /// Replace one version's tags (S3 PutObjectTagging with a `versionId`),
    /// replace-all with no merge.
    ///
    /// A marker address is the same refusal as the read's.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for an unknown identity or a marker address, and the
    /// planes' own failures.
    pub async fn set_version_tags<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        tags: object::Tags,
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.version_home(plane, bucket, key, version).await? {
            // The current version's tags are the object row's own element.
            VersionHome::Current(_) => {
                ObjectMeta::set_tags(&*self.meta, bucket, key, &tags)
                    .await
                    .map_err(projected)?;
                Ok(())
            }
            VersionHome::History(row) => {
                let mut row = *row;
                row.tags = tags;
                VersionMeta::put_version(&*plane.version_rows, bucket, key, &row)
                    .await
                    .map_err(projected)
            }
        }
    }

    /// One version's ACL (S3 GetObjectAcl with a `versionId`).
    ///
    /// A marker address is the same refusal as the tag read's.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for an unknown identity or a marker address, and the
    /// planes' own failures.
    pub async fn version_acl<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Acl, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.version_home(plane, bucket, key, version).await? {
            VersionHome::Current(row) => Ok(Acl {
                owner: row.owner,
                grants: row.acl.grants,
            }),
            VersionHome::History(row) => Ok(Acl {
                owner: row.owner,
                grants: row.acl.grants,
            }),
        }
    }

    /// Replace one version's grants (S3 PutObjectAcl with a `versionId`) —
    /// replace-all on the grants, the version's owner preserved.
    ///
    /// A marker address is the same refusal as the tag write's.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` for an unknown identity or a marker address, and the
    /// planes' own failures.
    pub async fn set_version_acl<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        acl: &Acl,
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        match self.version_home(plane, bucket, key, version).await? {
            // The grants replace, the owner rides the row's own element —
            // the object ACL put's contract, per version.
            VersionHome::Current(row) => {
                ObjectMeta::set_acl(&*self.meta, bucket, key, row.owner.as_ref(), &acl.grants)
                    .await
                    .map_err(projected)?;
                Ok(())
            }
            VersionHome::History(row) => {
                let mut row = *row;
                row.acl = Acl {
                    owner: None,
                    grants: acl.grants.clone(),
                };
                VersionMeta::put_version(&*plane.version_rows, bucket, key, &row)
                    .await
                    .map_err(projected)
            }
        }
    }

    /// Take back a versioned put that published and then failed.
    ///
    /// `history_seq` is the outgoing version's row when this call moved one
    /// into history. That row's bytes go back onto the key and the row itself
    /// is dropped, so the version stays current. `parts` is the retained list
    /// that object held: the row write records it again, because a plain row
    /// write clears the list. `publish` is the identity the failed publish
    /// carried.
    ///
    /// A plane that records that identity removes the published bytes only
    /// while the head still names it, and answers `false` when a newer publish
    /// owns the key — the row and the slot stay that writer's. A first write
    /// on such a plane, with no object to put back, clears the bytes and the
    /// row. A plane that records no identity leaves a first write's bytes in
    /// place: the next read serves them as the null version. Answers whether
    /// this call still owned the publish and compensated it.
    #[expect(
        clippy::too_many_arguments,
        reason = "the versioned undo's field set — plane, marker port, bucket, key, the row it replaces, the history sequence, the retained parts and the publish — is the operation's own"
    )]
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    pub async fn undo_versioned_put<R, B, G, W>(
        &self,
        plane: &Plane<R, C, B, G>,
        markers: &W,
        bucket: &bucket::Name,
        key: &object::Key,
        before: Option<&Row>,
        history_seq: Option<u64>,
        parts: &[PartRow],
        publish: PublishId,
    ) -> Result<bool, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
        W: MarkerWrite,
    {
        let content_key = ContentKey::Object(key.clone());
        let Some(seq) = history_seq else {
            return self
                .clear_first_publish(bucket, key, before, &content_key, publish)
                .await;
        };
        // A matching tag deletes every generation of the key. The outgoing
        // version's bytes are already in the block plane, and materialize
        // writes them back. A tag that does not match is a newer publish:
        // leave it, and leave the row and the slot.
        let cleared = if self.content.records_publish_identity() {
            let owned = self
                .content
                .remove_if_tagged(bucket, &content_key, &[], publish)
                .await
                .map_err(projected)?;
            if !owned {
                return Ok(false);
            }
            true
        } else {
            false
        };
        match VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?
        {
            Some(stored) if stored.kind == Kind::Marker => {
                // The failed put's bytes and row are not the marker. Drop
                // both, then the port writes the marker's slot and its
                // empty row. The history row is that marker's own record;
                // a current marker has none, so it expires after the slot
                // names it. A tagged clear already dropped the bytes.
                if !cleared {
                    self.content
                        .remove(bucket, &content_key)
                        .await
                        .map_err(projected)?;
                }
                let _ = ObjectMeta::remove(&*self.meta, bucket, key)
                    .await
                    .map_err(projected)?;
                markers
                    .mark_current_marker(bucket, key, stored.id.clone(), seq, None)
                    .await
                    .map_err(projected)?;
                plane
                    .versions
                    .expire(bucket, key, seq)
                    .await
                    .map_err(projected)?;
                return Ok(true);
            }
            Some(stored) if stored.kind == Kind::Object => {
                plane
                    .versions
                    .materialize(bucket, key, &stored)
                    .await
                    .map_err(projected)?;
                if let Some(old) = before {
                    let entry = self.require_content(bucket, key).await?;
                    // The list the outgoing object held. `put` and
                    // `put_guarded` clear it; this write puts it back
                    // with the row.
                    ObjectMeta::put_with_parts(
                        &*self.meta,
                        bucket,
                        key,
                        &Row {
                            etag: old.etag.clone(),
                            size: entry.len,
                            mtime: entry.mtime.unwrap_or(old.mtime),
                            identity: entry.identity,
                            tags: old.tags.clone(),
                            checksum: old.checksum.clone(),
                            owner: old.owner.clone(),
                            acl: old.acl.clone(),
                        },
                        parts,
                    )
                    .await
                    .map_err(projected)?;
                }
            }
            _ => {
                if !cleared {
                    self.content
                        .remove(bucket, &content_key)
                        .await
                        .map_err(projected)?;
                }
            }
        }
        plane
            .versions
            .expire(bucket, key, seq)
            .await
            .map_err(projected)?;
        Ok(true)
    }

    /// Clear a first versioned publish that has no outgoing version to restore.
    ///
    /// A plane that records the publish identity does so only while the head
    /// still names this publish, and only when there is no object row to put
    /// back. A row without a restorable slot stays: deleting the bytes would
    /// drop content the next read cannot rebuild. A plane that records no
    /// identity leaves the bytes, and the next read serves them as the null
    /// version.
    async fn clear_first_publish(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        before: Option<&Row>,
        content_key: &ContentKey,
        publish: PublishId,
    ) -> Result<bool, Error> {
        if !self.content.records_publish_identity() || before.is_some() {
            return Ok(false);
        }
        let owned = self
            .content
            .remove_if_tagged(bucket, content_key, &[], publish)
            .await
            .map_err(projected)?;
        if !owned {
            return Ok(false);
        }
        let _ = ObjectMeta::remove(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        Ok(true)
    }

    /// The version-creating `put_object`: the write that moves the outgoing
    /// version into the block plane before it publishes over it.
    ///
    /// The sequence is the design's (obligation 1), and every step is one
    /// contract call:
    ///
    /// 1. the bucket's versioning state and the key's **gated** current slot
    ///    decide, through the shared write policy ([`plan_write`]), whether the
    ///    outgoing version turns noncurrent, which identity the incoming one
    ///    takes and which sequence it records;
    /// 2. the outgoing content moves into the block plane
    ///    ([`Versions::supersede_with`]) — **before** the publish, because the
    ///    publish overwrites the bytes the supersede needs;
    /// 3. the staged content is published (the visibility point);
    /// 4. the object row **and its cleared retained part list** are written in
    ///    one call (`put_with_parts` — the overwrite is a fresh object);
    /// 5. the current slot advances.
    ///
    /// Steps 4 and 5 are two transactions, and the sequence is accepted as
    /// such: the contract's row write clears the stale parts, the slot move
    /// records the incoming identity, and a crash between them can leave the
    /// superseded version's parts uncleared — a stale retained list, never a
    /// lost object, and the repair pipeline's territory rather than a reason
    /// to widen the row write. A **returned** error from either step puts the
    /// outgoing version back — its bytes are republished, its history row is
    /// dropped, and the slot stays where it was. A crash still cannot: no
    /// process is left to run that undo.
    ///
    /// A bucket that records no versions takes the version-free path instead
    /// ([`commit_object`](Self::commit_object)): one body, two shapes, and the
    /// **caller** decides which one runs from its own store's state.
    ///
    /// Unlike the version-free row, a versioned row write that fails is the
    /// operation's answer whatever [`MetadataFailure`](crate::MetadataFailure) says: the identity this
    /// call is about to answer is real only once the row that records it is
    /// durable, and a read-heal cannot reconstruct a version identity.
    ///
    /// `lock` is the effective Object Lock
    /// [`ObjectOps::resolve_write_lock`](tinio_core::storage::ObjectOps::resolve_write_lock)
    /// answered. It rides into the slot advance as
    /// [`VersionMeta::advance_slot_with`]'s lock (OL7): the lock row commits
    /// with the current version, `None` never clears an existing row, and a
    /// null in-place replacement of an identity that owns an active lock is
    /// refused by the store. A bucket that records no versions takes the
    /// version-free body, which has no identity to lock.
    ///
    /// # Errors
    ///
    /// The version engine's failures (a supersede race included), the row
    /// plane's, and the content plane's.
    #[expect(
        clippy::too_many_arguments,
        reason = "the versioned put's field set — plane, bucket, key, staged body, tags, owner, ACL, lock and the marker port — is the operation's own, forwarded once into the version engine"
    )]
    #[instrument(
        target = "tinio::storage",
        level = "info",
        skip_all,
        fields(bucket = %bucket, key = %key)
    )]
    pub async fn put_object_versioned<R, B, G, W>(
        &self,
        plane: &VersionPlane<Versions<R, C, B, G>, R, B>,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody<C::Staged>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        lock: Option<&object_lock::Lock>,
        markers: &W,
    ) -> Result<object::Info, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
        W: MarkerWrite,
    {
        let state = plane.versions.state(bucket).await.map_err(projected)?;
        if !state.records_versions() {
            return self
                .commit_object(bucket, key, staged, tags, owner, acl, lock)
                .await;
        }
        let (current, plan) = Self::load_plan(plane, bucket, key, Intent::Put, state).await?;
        let before_row = ObjectMeta::get(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        // An in-place null replacement supersedes nothing in the plan: the
        // sequence stays and no history row survives a success. Hold the
        // outgoing bytes anyway, so a returned error can put them back, and
        // drop that row once the write has landed. A crash between the two
        // leaves it.
        let hold = !plan.supersede && current.exists && !current.degraded;
        let parts = ObjectMeta::parts(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        if plan.supersede || hold {
            plane
                .versions
                .supersede_with(bucket, key, &current)
                .await
                .map_err(projected)?;
        }
        // The history row this call just wrote, when it moved a current
        // version. A later failure takes that row back: the version stays
        // current instead of living only in history.
        let history_seq = (plan.supersede || hold).then_some(current.seq);
        let (staged, _staged_key, _len, etag, checksum) = staged.into_parts();
        let publish = mint_publish_id();
        self.content
            .publish_tagged(bucket, staged, &ContentKey::Object(key.clone()), publish)
            .await
            .map_err(projected)?;
        let Some(entry) = self.content.stat(bucket, key).await.map_err(projected)? else {
            // A publish that left no content behind is a folder marker: no
            // row, no version. The supersede already moved the outgoing
            // version into history, so take that back before answering the
            // marker — otherwise the history row outlives a write that
            // recorded nothing.
            let _owned = self
                .undo_versioned_put(
                    plane,
                    markers,
                    bucket,
                    key,
                    before_row.as_ref(),
                    history_seq,
                    &parts,
                    publish,
                )
                .await?;
            return Ok(Self::marker_info(key));
        };
        let id = match plan.identity {
            WriteIdentity::Fresh => Some(version::mint()),
            WriteIdentity::Null | WriteIdentity::Unversioned => None,
        };
        let mtime = entry.mtime.unwrap_or_else(SystemTime::now);
        let row = Row {
            etag: etag.clone(),
            size: entry.len,
            mtime,
            identity: entry.identity,
            tags: tags.clone(),
            checksum: checksum.clone(),
            owner: owner.cloned(),
            acl: Acl {
                owner: None,
                grants: acl.grants.clone(),
            },
        };
        // The row **and** its retained part list in one call: a versioned
        // write is an overwrite, so the parts of the object it replaces must
        // not survive it. The refusal this answers is the contract's own
        // (the bucket row is asserted inside the write's transaction).
        if let Err(err) = ObjectMeta::put_with_parts(&*self.meta, bucket, key, &row, &[])
            .await
            .map_err(projected)
        {
            let _owned = self
                .undo_versioned_put(
                    plane,
                    markers,
                    bucket,
                    key,
                    before_row.as_ref(),
                    history_seq,
                    &parts,
                    publish,
                )
                .await?;
            return Err(err);
        }
        if let Err(err) = plane
            .version_rows
            .advance_slot_with(bucket, key, id.clone(), plan.seq, lock)
            .await
            .map_err(projected)
        {
            let _owned = self
                .undo_versioned_put(
                    plane,
                    markers,
                    bucket,
                    key,
                    before_row.as_ref(),
                    history_seq,
                    &parts,
                    publish,
                )
                .await?;
            return Err(err);
        }
        if hold {
            // The held row is the outgoing null version, not history. Drop it
            // now that the replacement is current.
            if let Err(err) = VersionMeta::expire(&*plane.version_rows, bucket, key, current.seq)
                .await
                .map_err(projected)
            {
                let _owned = self
                    .undo_versioned_put(
                        plane,
                        markers,
                        bucket,
                        key,
                        before_row.as_ref(),
                        Some(current.seq),
                        &parts,
                        publish,
                    )
                    .await?;
                return Err(err);
            }
        }
        Ok(object::Info {
            key: key.clone(),
            size: entry.len,
            last_modified: mtime,
            etag,
            tags,
            checksum,
            owner: owner.cloned(),
            acl: row.acl,
            version_id: id,
            bucket_records_versions: true,
        })
    }
}

#[cfg(test)]
#[path = "version_tests.rs"]
mod tests;
