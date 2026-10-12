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
//! the history), the removed version's bytes go through
//! `ObjectContent::remove` and the no-successor arm's object row through
//! `ObjectMeta::remove`. The **marker-creating** delete a versioned `DELETE`
//! takes is [`ObjectOps::create_delete_marker`], a body beside the nine: its
//! ordering (supersede, remove the bytes and the row, write the marker's slot,
//! materialize the marker's object row) is shared here, while the slot write
//! itself is the backend's [`MarkerWrite`] port — a marker is a recorded slot
//! with **no version row**, a state no contract write expresses, and the
//! null-identity marker a suspended delete makes must not displace the null
//! version the same delete leaves in history.
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
//! The current marker's `last_modified` is read where the model records it:
//! the version row at the slot's sequence first — that row is the marker's own
//! record, and it is what a marker write records and what
//! `advance_slot` resolves the marker bit from — and the **object row**
//! second, which is fs's home for it (its D37 row-slot model keeps a current
//! version in the object row, so a live marker has no version-table row at
//! all). A store where both exist answers from the version row: the object row
//! `advance_slot` materializes carries no stamp of its own.

use std::time::SystemTime;

use crate::{
    _core::{
        ByteRange, ETag, GetObjectResult,
        acl::{self, Acl},
        blob::BlockStore,
        bucket,
        content::{ContentKey, ObjectContent},
        meta::{
            Metadata, ObjectMeta, VersionMeta,
            object::Row,
            version::{Kind, Stored},
        },
        object,
        storage::{self, Error, marker_has_no_metadata},
        version::{self, HeadResult, Id, Marker, ReadResult, State, wire_of},
    },
    _version::{
        Current, CurrentRows, Identity as WriteIdentity, Intent, MarkerWrite, Seqs, Versions,
        plan_write,
    },
    ObjectOps, StagedBody,
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
    ///   ([`VersionMeta::expire`]). Its blocks stay: liveness is derived from the rows, and
    ///   `Cleanup`'s sweep drops what no row names.
    ///
    /// The successor scan is [`VersionMeta::latest`]'s one row, never a walk of the history,
    /// and a row that is the current version's own — the sequence the slot records — is
    /// consumed before the scan so the scan cannot name it.
    ///
    /// # Errors
    ///
    /// `NoSuchVersion` (the contract's not-found class, naming the wire the caller asked
    /// for) when the identity names no version of the key.
    pub async fn delete_object_version<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
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
            // content version has none. `expire` is idempotent, so the content arm's call
            // is the one that finds nothing.
            VersionMeta::expire(&*plane.version_rows, bucket, key, current.seq)
                .await
                .map_err(projected)?;
            match VersionMeta::latest(&*plane.version_rows, bucket, key)
                .await
                .map_err(projected)?
            {
                Some((seq, row)) => self.restore_version(plane, bucket, key, seq, row).await?,
                None => {
                    self.content
                        .remove(bucket, &ContentKey::Object(key.clone()))
                        .await
                        .map_err(projected)?;
                    let _ = ObjectMeta::remove(&*self.meta, bucket, key)
                        .await
                        .map_err(projected)?;
                }
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
        VersionMeta::expire(&*plane.version_rows, bucket, key, seq)
            .await
            .map_err(projected)?;
        Ok(Marker {
            id: row.id.clone(),
            is_delete_marker: row.kind == Kind::Marker,
        })
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

    /// Create the delete marker a versioned `DELETE` makes current.
    ///
    /// The ordering is the engine's, and it is the engine's own test scaffold
    /// (`plant_delete_marker`) in production order, over the contracts:
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
    ///    slot moves off, and a current marker's own row, if an earlier shape
    ///    left one, expires first so repeated suspended deletes cannot grow a
    ///    phantom history.
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
        let current = plane
            .versions
            .current(bucket, key)
            .await
            .map_err(projected)?;
        let seqs = Seqs {
            latest: VersionMeta::latest(&*plane.version_rows, bucket, key)
                .await
                .map_err(projected)?
                .map(|(seq, _)| seq)
                .unwrap_or(0),
            null_at_zero: VersionMeta::version_seq_of(&*plane.version_rows, bucket, key, None)
                .await
                .map_err(projected)?
                == Some(0),
        };
        let plan = plan_write(Intent::Delete, state, &current, &seqs);
        if !plan.marker {
            // The state is not recording versions, so the version-free delete
            // is the answer.
            return self.delete_object(bucket, key).await;
        }
        if plan.supersede {
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
        // A **current marker's** own row, if an earlier shape left one, expires
        // before the new slot names the new one: repeated suspended deletes
        // must not grow a phantom history, and the sequence this body is about
        // to leave would otherwise read as the slot's own record. The branch is
        // the marker bit's — a content version's sequence holds no row of its
        // own, and the one the supersede has just written there is the history
        // the successor scan must find.
        if current.is_delete_marker {
            VersionMeta::expire(&*plane.version_rows, bucket, key, current.seq)
                .await
                .map_err(projected)?;
        }
        markers
            .mark_current_marker(
                bucket,
                key,
                id.clone(),
                plan.seq,
                // The row the marker's slot moves off: the version the supersede
                // above committed into history, stamped noncurrent in the
                // marker's own transaction (RG5 — the rule `advance_slot` and
                // `restore_slot` apply). Available only when a real version was
                // superseded: when `current` is a marker, the row at its
                // sequence is expired just above, so there is nothing to stamp.
                plan.supersede.then_some(current.seq),
            )
            .await
            .map_err(projected)?;
        Ok(Marker {
            id,
            is_delete_marker: true,
        })
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
    /// is dropped, so the version stays current. A first write has no outgoing
    /// version: the published bytes stay, and a read that finds no row serves
    /// them as the null version.
    pub async fn undo_versioned_put<R, B, G>(
        &self,
        plane: &Plane<R, C, B, G>,
        bucket: &bucket::Name,
        key: &object::Key,
        before: Option<&Row>,
        history_seq: Option<u64>,
    ) -> Result<(), Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let content_key = ContentKey::Object(key.clone());
        if let Some(seq) = history_seq {
            match VersionMeta::get_version(&*plane.version_rows, bucket, key, seq)
                .await
                .map_err(projected)?
            {
                Some(stored) if stored.kind == Kind::Object => {
                    plane
                        .versions
                        .materialize(bucket, key, &stored)
                        .await
                        .map_err(projected)?;
                    if let Some(old) = before {
                        let entry = self.require_content(bucket, key).await?;
                        self.write_row(
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
                        )
                        .await?;
                    }
                }
                _ => {
                    self.content
                        .remove(bucket, &content_key)
                        .await
                        .map_err(projected)?;
                }
            }
            plane
                .versions
                .expire(bucket, key, seq)
                .await
                .map_err(projected)?;
            return Ok(());
        }
        // A first write superseded nothing. The published bytes stay: the
        // row plane may be unable to record an identity (a read-only store),
        // and the next read serves them as the null version.
        Ok(())
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
    /// # Errors
    ///
    /// The version engine's failures (a supersede race included), the row
    /// plane's, and the content plane's.
    #[expect(
        clippy::too_many_arguments,
        reason = "the versioned put's field set — plane, bucket, key, staged body, tags, owner and ACL — is the operation's own, forwarded once into the version engine"
    )]
    pub async fn put_object_versioned<R, B, G>(
        &self,
        plane: &VersionPlane<Versions<R, C, B, G>, R, B>,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody<C::Staged>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error>
    where
        R: Metadata + VersionMeta,
        G: CurrentRows,
        B: BlockStore,
    {
        let state = plane.versions.state(bucket).await.map_err(projected)?;
        if !state.records_versions() {
            return self
                .commit_object(bucket, key, staged, tags, owner, acl)
                .await;
        }
        let current = plane
            .versions
            .current(bucket, key)
            .await
            .map_err(projected)?;
        let seqs = Seqs {
            latest: VersionMeta::latest(&*plane.version_rows, bucket, key)
                .await
                .map_err(projected)?
                .map(|(seq, _)| seq)
                .unwrap_or(0),
            null_at_zero: VersionMeta::version_seq_of(&*plane.version_rows, bucket, key, None)
                .await
                .map_err(projected)?
                == Some(0),
        };
        let plan = plan_write(Intent::Put, state, &current, &seqs);
        let before_row = ObjectMeta::get(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        if plan.supersede {
            plane
                .versions
                .supersede_with(bucket, key, &current)
                .await
                .map_err(projected)?;
        }
        // The history row this call just wrote, when the supersede had a
        // current version to move. A later failure takes that row back: the
        // version stays current instead of living only in history.
        let history_seq = (plan.supersede && current.exists).then_some(current.seq);
        let (staged, _staged_key, _len, etag, checksum) = staged.into_parts();
        self.content
            .publish(bucket, staged, &ContentKey::Object(key.clone()))
            .await
            .map_err(projected)?;
        let Some(entry) = self.content.stat(bucket, key).await.map_err(projected)? else {
            // A publish that left no content behind is a folder marker: no
            // row, no version — markers stay outside the version model.
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
            self.undo_versioned_put(plane, bucket, key, before_row.as_ref(), history_seq)
                .await?;
            return Err(err);
        }
        if let Err(err) = plane
            .version_rows
            .advance_slot(bucket, key, id.clone(), plan.seq)
            .await
            .map_err(projected)
        {
            self.undo_versioned_put(plane, bucket, key, before_row.as_ref(), history_seq)
                .await?;
            return Err(err);
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
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::SystemTime,
    };

    use async_trait::async_trait;

    use super::*;
    use crate::{
        _core::{
            ETag,
            acl::{AclGrants, Grant, Grantee, OwnerId, Permission},
            content::{ContentEntry, ContentKey},
            storage::{BodyStream, Error as StorageError, collect_body},
            version::State,
        },
        _store_redb::RedbContent,
        BucketRow, MetadataFailure,
        testutil::{Base, BlockMem, TestMeta, body},
    };

    /// The group's receiver over the test base — the row-backed store's
    /// policies, so a row write is the operation's answer.
    fn ops(base: &Base) -> ObjectOps<TestMeta, RedbContent> {
        base.objects(BucketRow::Guaranteed, MetadataFailure::Fail)
    }

    /// The content plane that makes the race the label rule exists for
    /// deterministic: the first `stat` it serves moves the key's slot to a
    /// fresh identity — the concurrent overwrite landing **inside** the
    /// version-addressed read, after its resolution and before the plain read
    /// it delegates to has finished. Every other call, and every `stat` once
    /// the move has happened (or when the test pre-fires it), is the real
    /// in-memory plane's.
    #[derive(Clone)]
    struct MovesOnFirstStat {
        inner: RedbContent,
        rows: Arc<TestMeta>,
        bucket: bucket::Name,
        key: object::Key,
        fresh: Id,
        moved: Arc<AtomicBool>,
    }

    #[async_trait]
    impl ObjectContent for MovesOnFirstStat {
        type Error = <RedbContent as ObjectContent>::Error;
        type Staged = <RedbContent as ObjectContent>::Staged;

        async fn stat(
            &self,
            bucket: &bucket::Name,
            key: &object::Key,
        ) -> Result<Option<ContentEntry>, Self::Error> {
            if !self.moved.swap(true, Ordering::SeqCst) {
                <TestMeta as VersionMeta>::advance_slot(
                    &self.rows,
                    &self.bucket,
                    &self.key,
                    Some(self.fresh.clone()),
                    9,
                )
                .await?;
            }
            self.inner.stat(bucket, key).await
        }

        async fn stage(
            &self,
            bucket: &bucket::Name,
            key: &ContentKey,
        ) -> Result<Self::Staged, Self::Error> {
            self.inner.stage(bucket, key).await
        }

        async fn publish(
            &self,
            bucket: &bucket::Name,
            staged: Self::Staged,
            key: &ContentKey,
        ) -> Result<(), Self::Error> {
            self.inner.publish(bucket, staged, key).await
        }

        async fn open(
            &self,
            bucket: &bucket::Name,
            key: &ContentKey,
            range: Option<ByteRange>,
        ) -> Result<BodyStream, Self::Error> {
            self.inner.open(bucket, key, range).await
        }

        async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
            self.inner.remove(bucket, key).await
        }

        async fn rename(
            &self,
            bucket: &bucket::Name,
            from: &object::Key,
            to: &object::Key,
        ) -> Result<(), Self::Error> {
            self.inner.rename(bucket, from, to).await
        }

        async fn copy(
            &self,
            bucket: &bucket::Name,
            from: &object::Key,
            to: &object::Key,
        ) -> Result<(), Self::Error> {
            self.inner.copy(bucket, from, to).await
        }

        async fn scan(
            &self,
            bucket: &bucket::Name,
            prefix: &str,
            after: Option<&object::Key>,
            limit: usize,
        ) -> Result<Vec<ContentEntry>, Self::Error> {
            self.inner.scan(bucket, prefix, after, limit).await
        }

        async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
            self.inner.create_bucket(bucket).await
        }

        async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
            self.inner.remove_bucket(bucket).await
        }

        async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
            self.inner.is_empty(bucket).await
        }
    }

    /// A bucket with a recorded row — the guarded policy's requirement.
    async fn bucket(base: &Base, name: &str) -> bucket::Name {
        let name = bucket::name(name).unwrap();
        base.ops(BucketRow::Guaranteed)
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        name
    }

    /// One versioned write through the version-creating object body.
    async fn put_versioned(
        base: &Base,
        plane: &Plane<TestMeta, RedbContent, BlockMem>,
        name: &bucket::Name,
        key: &object::Key,
        bytes: &'static [u8],
        owner: Option<&OwnerId>,
    ) -> object::Info {
        let staged = ops(base)
            .stage_body(name, key, body(&[bytes]), None, None)
            .await
            .unwrap();
        ops(base)
            .put_object_versioned(
                plane,
                name,
                key,
                staged,
                object::Tags::empty(),
                owner,
                &Acl::default_private(None),
            )
            .await
            .unwrap()
    }

    /// Plant a current delete marker the way a backend's versioned delete does.
    ///
    /// The engine has no version-creating delete: a marker written as a version
    /// row from the shared body would displace the one null version. The read
    /// tests still need a marker in place, so they build one here.
    async fn plant_delete_marker(
        base: &Base,
        plane: &Plane<TestMeta, RedbContent, BlockMem>,
        name: &bucket::Name,
        key: &object::Key,
    ) -> version::Marker {
        use crate::_core::blob::BlobRef;

        let state = plane.versions.state(name).await.unwrap();
        let current = plane.versions.current(name, key).await.unwrap();
        let seqs = Seqs {
            latest: VersionMeta::latest(&*plane.version_rows, name, key)
                .await
                .unwrap()
                .map(|(seq, _)| seq)
                .unwrap_or(0),
            null_at_zero: VersionMeta::version_seq_of(&*plane.version_rows, name, key, None)
                .await
                .unwrap()
                == Some(0),
        };
        let plan = plan_write(Intent::Delete, state, &current, &seqs);
        if plan.supersede {
            plane
                .versions
                .supersede_with(name, key, &current)
                .await
                .unwrap();
        }
        let id = match plan.identity {
            WriteIdentity::Fresh => Some(version::mint()),
            WriteIdentity::Null | WriteIdentity::Unversioned => None,
        };
        let marker = Stored {
            id: id.clone(),
            size: 0,
            blob: BlobRef::empty(),
            etag: ETag::EMPTY,
            created: SystemTime::now(),
            became_noncurrent: 0,
            kind: Kind::Marker,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            seq: plan.seq,
        };
        VersionMeta::put_version(&*plane.version_rows, name, key, &marker)
            .await
            .unwrap();
        plane
            .version_rows
            .advance_slot(name, key, id.clone(), plan.seq)
            .await
            .unwrap();
        ops(base)
            .content
            .remove(name, &ContentKey::Object(key.clone()))
            .await
            .unwrap();
        version::Marker {
            id,
            is_delete_marker: plan.marker,
        }
    }

    /// A versioned bucket whose key holds one enabled version.
    async fn versioned(
        base: &Base,
        key: &object::Key,
        bytes: &'static [u8],
    ) -> (
        bucket::Name,
        Plane<TestMeta, RedbContent, BlockMem>,
        object::Info,
    ) {
        let name = bucket(base, "data").await;
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Enabled)
            .await
            .unwrap();
        let info = put_versioned(base, &plane, &name, key, bytes, None).await;
        (name, plane, info)
    }

    /// A served body's bytes.
    async fn read(stream: BodyStream) -> Vec<u8> {
        collect_body(stream).await.unwrap().to_vec()
    }

    /// A canonical owner id.
    fn owner() -> OwnerId {
        OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
    }

    /// A grant set a write replaces the version's with.
    fn grants() -> AclGrants {
        vec![Grant {
            grantee: Grantee::Canonical(owner()),
            permission: Permission::Read,
        }]
    }

    #[tokio::test]
    async fn a_version_addressed_read_serves_the_version_it_names() {
        let base = Base::new();
        let key = object::key("notes.txt").unwrap();
        let (name, plane, first) = versioned(&base, &key, b"first").await;
        let first_id = first
            .version_id
            .clone()
            .expect("an enabled write mints an identity");
        let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
        let second_id = second
            .version_id
            .clone()
            .expect("the overwrite mints its own identity");
        assert_ne!(first_id, second_id);

        // The superseded version: its own row and the block plane, never the
        // current object.
        match ops(&base)
            .get_object_version(&plane, &name, &key, Some(&first_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"first");
                assert_eq!(got.info.version_id.as_ref(), Some(&first_id));
                assert_eq!(got.info.etag, ETag::from_content(b"first"));
                assert_eq!(got.info.size, 5);
                assert!(got.info.bucket_records_versions);
                assert_eq!(got.served_range, None);
            }
            ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        // The current version: the plain read's own serve, labelled with the
        // identity the gated slot resolved.
        match ops(&base)
            .get_object_version(&plane, &name, &key, Some(&second_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"second");
                assert_eq!(got.info.version_id.as_ref(), Some(&second_id));
                assert_eq!(got.info.etag, ETag::from_content(b"second"));
            }
            ReadResult::Marker { .. } => panic!("the current version is content"),
        }
        // The head agrees with the get, on every element it answers.
        match ops(&base)
            .head_object_version(&plane, &name, &key, Some(&first_id))
            .await
            .unwrap()
        {
            HeadResult::Object(info) => {
                assert_eq!(info.version_id.as_ref(), Some(&first_id));
                assert_eq!(info.size, 5);
                assert_eq!(info.etag, ETag::from_content(b"first"));
                assert!(info.bucket_records_versions);
            }
            HeadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        // A range resolves against the version's own length and names the
        // window it served.
        match ops(&base)
            .get_object_version(
                &plane,
                &name,
                &key,
                Some(&first_id),
                Some(ByteRange::Inclusive(1, 3)),
            )
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"irs");
                assert_eq!(got.served_range, Some((1, 3)));
            }
            ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        // An identity that names no version is the contract's own refusal.
        let unknown = Id::new("no-such-version").unwrap();
        let err = ops(&base)
            .get_object_version(&plane, &name, &key, Some(&unknown), None)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::NoSuchVersion(ref named) if named == "no-such-version"),
            "{err:?}"
        );
        let err = ops(&base)
            .head_object_version(&plane, &name, &key, Some(&unknown))
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::NoSuchVersion(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_version_addressed_read_of_a_delete_marker_answers_the_marker() {
        let base = Base::new();
        let key = object::key("notes.txt").unwrap();
        let (name, plane, first) = versioned(&base, &key, b"first").await;
        let first_id = first.version_id.clone().unwrap();
        let marker = plant_delete_marker(&base, &plane, &name, &key).await;
        let marker_id = marker
            .id
            .clone()
            .expect("an enabled bucket's marker carries an identity");
        assert!(marker.is_delete_marker);

        // The marker's stamp is its own row's creation — the version row the
        // version-creating delete records.
        let (seq, row) = plane
            .versions
            .latest(&name, &key)
            .await
            .unwrap()
            .expect("the marker's row");
        assert_eq!(row.kind, Kind::Marker);
        assert_eq!(row.id.as_ref(), Some(&marker_id));

        // A marker is a **success**, never an error, for the reads.
        match ops(&base)
            .get_object_version(&plane, &name, &key, Some(&marker_id), None)
            .await
            .unwrap()
        {
            ReadResult::Marker { id, last_modified } => {
                assert_eq!(id.as_ref(), Some(&marker_id));
                assert_eq!(last_modified, row.created, "the marker's own stamp");
            }
            ReadResult::Object(_) => panic!("the current version is a marker, not content"),
        }
        match ops(&base)
            .head_object_version(&plane, &name, &key, Some(&marker_id))
            .await
            .unwrap()
        {
            HeadResult::Marker { id, last_modified } => {
                assert_eq!(id.as_ref(), Some(&marker_id));
                assert_eq!(last_modified, row.created);
            }
            HeadResult::Object(_) => panic!("the current version is a marker, not content"),
        }
        // The version the marker hid is still served by its own identity.
        match ops(&base)
            .get_object_version(&plane, &name, &key, Some(&first_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"first");
                assert_eq!(got.info.version_id.as_ref(), Some(&first_id));
            }
            ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        // A marker holds no metadata: every pair refuses, naming the marker.
        let refusals = [
            ops(&base)
                .version_tags(&plane, &name, &key, Some(&marker_id))
                .await
                .unwrap_err(),
            ops(&base)
                .set_version_tags(&plane, &name, &key, Some(&marker_id), object::Tags::empty())
                .await
                .unwrap_err(),
            ops(&base)
                .version_acl(&plane, &name, &key, Some(&marker_id))
                .await
                .unwrap_err(),
            ops(&base)
                .set_version_acl(
                    &plane,
                    &name,
                    &key,
                    Some(&marker_id),
                    &Acl::default_private(None),
                )
                .await
                .unwrap_err(),
        ];
        for err in refusals {
            assert!(
                matches!(&err, StorageError::NoSuchVersion(named) if named == marker_id.as_str()),
                "a marker must refuse metadata with its own identity: {err:?}"
            );
        }
        assert_eq!(seq, 2, "the marker takes the key's next sequence");
    }

    #[tokio::test]
    async fn a_null_identity_marker_answers_the_null_version_id() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("notes.txt").unwrap();
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Suspended)
            .await
            .unwrap();
        let null = put_versioned(&base, &plane, &name, &key, b"null", None).await;
        assert_eq!(
            null.version_id, None,
            "a suspended write records the null version"
        );
        let marker = plant_delete_marker(&base, &plane, &name, &key).await;
        assert!(
            marker.is_delete_marker && marker.id.is_none(),
            "a suspended delete makes a null-identity marker current"
        );

        // `versionId=null` addresses the marker: a successful marker answer
        // carrying the null identity and the marker row's own stamp — never the
        // object version's identity, and never an error.
        let (_, row) = plane
            .versions
            .latest(&name, &key)
            .await
            .unwrap()
            .expect("the marker's row");
        assert_eq!(row.kind, Kind::Marker);
        assert_eq!(row.id, None);
        match ops(&base)
            .get_object_version(&plane, &name, &key, None, None)
            .await
            .unwrap()
        {
            ReadResult::Marker { id, last_modified } => {
                assert_eq!(id, None, "the null-identity marker carries the null id");
                assert_eq!(last_modified, row.created, "the marker row's own stamp");
            }
            ReadResult::Object(_) => panic!("the null-identity marker is not content"),
        }
        match ops(&base)
            .head_object_version(&plane, &name, &key, None)
            .await
            .unwrap()
        {
            HeadResult::Marker { id, .. } => assert_eq!(id, None),
            HeadResult::Object(_) => panic!("the null-identity marker is not content"),
        }
        // The per-version metadata pairs refuse it, naming the null identity.
        for err in [
            ops(&base)
                .version_tags(&plane, &name, &key, None)
                .await
                .unwrap_err(),
            ops(&base)
                .version_acl(&plane, &name, &key, None)
                .await
                .unwrap_err(),
        ] {
            assert!(
                matches!(&err, StorageError::NoSuchVersion(named) if named == "null"),
                "a marker must refuse metadata as the null version: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn the_current_slot_is_the_gated_one_not_the_rows_recorded_identity() {
        let base = Base::new();
        let key = object::key("notes.txt").unwrap();
        let (name, plane, first) = versioned(&base, &key, b"first").await;
        let first_id = first.version_id.clone().unwrap();

        // The content drifts from the row — an out-of-band edit's shape,
        // written with the contract's own row writer (the version columns
        // ride the write untouched, so the recorded identity survives while
        // the content-derived elements no longer describe the key).
        let stored = base.meta.get(&name, &key).await.unwrap().expect("the row");
        base.meta
            .put(
                &name,
                &key,
                &Row {
                    etag: ETag::from_content(b"drifted"),
                    size: 7,
                    mtime: SystemTime::now(),
                    identity: 0,
                    tags: stored.tags.clone(),
                    checksum: stored.checksum.clone(),
                    owner: stored.owner.clone(),
                    acl: stored.acl.clone(),
                },
            )
            .await
            .unwrap();

        // The recorded identity is not the current version any more: the gate
        // degraded the slot, so the address resolves nothing — a body that
        // reached for the row's recorded identity (an ungated `Metadata::get`)
        // would serve the drift under it.
        let err = ops(&base)
            .get_object_version(&plane, &name, &key, Some(&first_id), None)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::NoSuchVersion(_)), "{err:?}");
        assert!(
            ops(&base)
                .head_object_version(&plane, &name, &key, Some(&first_id))
                .await
                .is_err(),
            "the head takes the same gate verdict"
        );
        // The null version is the gated truth, and it serves the current
        // bytes without an identity.
        match ops(&base)
            .get_object_version(&plane, &name, &key, None, None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"first");
                assert_eq!(
                    got.info.version_id, None,
                    "the degraded slot carries no identity"
                );
                assert_eq!(got.info.size, 5, "the served size is the content's");
            }
            ReadResult::Marker { .. } => panic!("the drift is content, not a marker"),
        }
    }

    #[tokio::test]
    async fn a_current_version_read_labels_the_content_it_served() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Enabled)
            .await
            .unwrap();
        // The read's plane and receiver over the moving content: `fires` puts
        // the slot move on the read's own first content probe (the concurrent
        // overwrite landing inside the read), `!fires` leaves the plane still.
        let reader = |key: &object::Key, fires: bool| {
            let meta = Arc::clone(&ops(&base).meta);
            let content = MovesOnFirstStat {
                inner: (*base.content).clone(),
                rows: Arc::clone(&meta),
                bucket: name.clone(),
                key: key.clone(),
                fresh: Id::new("moved-on").unwrap(),
                moved: Arc::new(AtomicBool::new(!fires)),
            };
            let plane = base.plane_with(content.clone());
            let ops = ObjectOps {
                meta,
                content: Arc::new(content),
                metadata_failure: MetadataFailure::Fail,
                bucket_row: BucketRow::Guaranteed,
            };
            (plane, ops)
        };

        // The control: the slot stands still, and the answer carries the
        // identity the request named.
        let still = object::key("still.txt").unwrap();
        let first = put_versioned(&base, &plane, &name, &still, b"still", None).await;
        let still_id = first
            .version_id
            .clone()
            .expect("an enabled write mints an identity");
        let (still_plane, still_ops) = reader(&still, false);
        match still_ops
            .get_object_version(&still_plane, &name, &still, Some(&still_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"still");
                assert_eq!(
                    got.info.version_id.as_ref(),
                    Some(&still_id),
                    "the version the request named is still the current one"
                );
            }
            ReadResult::Marker { .. } => panic!("the current version is content"),
        }

        // The race: the resolution sees `moving` current, the read's own
        // content probe then takes the slot to a fresh identity, and the plain
        // read serves the bytes it was about to serve — so those bytes are no
        // longer the version the request named, and the answer says the null
        // version instead of stamping them with the identifier that moved off.
        let moving = object::key("moving.txt").unwrap();
        let first = put_versioned(&base, &plane, &name, &moving, b"moving", None).await;
        let moving_id = first
            .version_id
            .clone()
            .expect("an enabled write mints an identity");
        let (moving_plane, moving_ops) = reader(&moving, true);
        match moving_ops
            .get_object_version(&moving_plane, &name, &moving, Some(&moving_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"moving");
                assert_eq!(
                    got.info.version_id, None,
                    "the served bytes are not the named version any more"
                );
            }
            ReadResult::Marker { .. } => panic!("the current version is content"),
        }
        // The head takes the same rule, from its own resolution.
        let head_key = object::key("moving-head.txt").unwrap();
        let first = put_versioned(&base, &plane, &name, &head_key, b"head", None).await;
        let head_id = first
            .version_id
            .clone()
            .expect("an enabled write mints an identity");
        let (head_plane, head_ops) = reader(&head_key, true);
        match head_ops
            .head_object_version(&head_plane, &name, &head_key, Some(&head_id))
            .await
            .unwrap()
        {
            HeadResult::Object(info) => assert_eq!(
                info.version_id, None,
                "the head labels what its own resolution served"
            ),
            HeadResult::Marker { .. } => panic!("the current version is content"),
        }
    }

    #[tokio::test]
    async fn the_per_version_tag_and_acl_pairs_answer_from_the_versions_own_row() {
        let base = Base::new();
        let key = object::key("notes.txt").unwrap();
        let name = bucket(&base, "data").await;
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Enabled)
            .await
            .unwrap();
        let first = put_versioned(&base, &plane, &name, &key, b"first", Some(&owner())).await;
        let first_id = first.version_id.clone().unwrap();
        let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
        let second_id = second.version_id.clone().unwrap();

        // The history version's tags and ACL are its version row's, and the
        // write lands on that row alone — the row's own stamp survives it.
        let before = plane
            .version_rows
            .get_version(&name, &key, 1)
            .await
            .unwrap()
            .expect("the superseded version's row");
        assert_ne!(before.became_noncurrent, 0, "the row is history");
        let tags = object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())]).unwrap();
        ops(&base)
            .set_version_tags(&plane, &name, &key, Some(&first_id), tags.clone())
            .await
            .unwrap();
        assert_eq!(
            ops(&base)
                .version_tags(&plane, &name, &key, Some(&first_id))
                .await
                .unwrap(),
            tags
        );
        let after = plane
            .version_rows
            .get_version(&name, &key, 1)
            .await
            .unwrap()
            .expect("the superseded version's row");
        assert_eq!(after.tags, tags);
        assert_eq!(
            after.became_noncurrent, before.became_noncurrent,
            "the rewrite preserves the row's noncurrent stamp"
        );
        ops(&base)
            .set_version_acl(
                &plane,
                &name,
                &key,
                Some(&first_id),
                &Acl {
                    owner: None,
                    grants: grants(),
                },
            )
            .await
            .unwrap();
        let acl = ops(&base)
            .version_acl(&plane, &name, &key, Some(&first_id))
            .await
            .unwrap();
        assert_eq!(acl.grants, grants());
        assert_eq!(
            acl.owner.as_ref(),
            Some(&owner()),
            "the row's owner element is its own"
        );

        // The current version's tags and ACL are the object row's — the same
        // elements the plain tag/ACL bodies read, which is the cross-check
        // that the two paths answer one row.
        ops(&base)
            .set_version_tags(&plane, &name, &key, Some(&second_id), tags.clone())
            .await
            .unwrap();
        assert_eq!(
            ops(&base)
                .version_tags(&plane, &name, &key, Some(&second_id))
                .await
                .unwrap(),
            tags
        );
        assert_eq!(
            ops(&base).get_object_tags(&name, &key).await.unwrap(),
            tags,
            "the version-addressed write landed on the object row"
        );
        ops(&base)
            .set_version_acl(
                &plane,
                &name,
                &key,
                Some(&second_id),
                &Acl {
                    owner: Some(owner()),
                    grants: grants(),
                },
            )
            .await
            .unwrap();
        let current_acl = ops(&base)
            .version_acl(&plane, &name, &key, Some(&second_id))
            .await
            .unwrap();
        assert_eq!(current_acl.grants, grants());
        // The current version's row records no owner (the overwrite wrote
        // none), and the ACL put never changes it.
        assert_eq!(current_acl.owner, None);
        // The slot still names the same version: the element writes touch
        // neither the version columns nor the content.
        let slot = plane
            .version_rows
            .current_slot(&name, &key)
            .await
            .unwrap()
            .expect("a slot");
        assert_eq!(slot.version.as_ref(), Some(&second_id));
        match ops(&base)
            .get_object_version(&plane, &name, &key, Some(&second_id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => assert_eq!(read(got.body).await, b"second"),
            ReadResult::Marker { .. } => panic!("the current version is content"),
        }
    }

    #[tokio::test]
    async fn a_suspended_replacement_leaves_no_history_row_and_serves_the_null_version() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("notes.txt").unwrap();
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Suspended)
            .await
            .unwrap();
        let first = put_versioned(&base, &plane, &name, &key, b"first", None).await;
        assert_eq!(
            first.version_id, None,
            "a suspended write records the null version"
        );
        let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
        assert_eq!(second.version_id, None);
        assert!(
            plane.versions.latest(&name, &key).await.unwrap().is_none(),
            "the in-place replacement leaves no history row"
        );
        // `versionId=null` is the current null version, serving the newest
        // bytes under no identity.
        match ops(&base)
            .get_object_version(&plane, &name, &key, None, None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"second");
                assert_eq!(got.info.version_id, None);
                assert!(
                    got.info.bucket_records_versions,
                    "a suspended bucket records them"
                );
            }
            ReadResult::Marker { .. } => panic!("the replacement is content, not a marker"),
        }
        // The per-version pairs address the same null version.
        assert_eq!(
            ops(&base)
                .version_tags(&plane, &name, &key, None)
                .await
                .unwrap(),
            object::Tags::empty()
        );
        assert!(
            ops(&base)
                .version_tags(&plane, &name, &key, Some(&Id::new("gone").unwrap()))
                .await
                .is_err(),
            "an identity naming no version is refused"
        );
    }

    #[tokio::test]
    async fn an_unversioned_bucket_serves_the_null_version_and_reports_no_recording() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("notes.txt").unwrap();
        let plane = base.version_plane();
        let info = put_versioned(&base, &plane, &name, &key, b"plain", None).await;
        assert_eq!(info.version_id, None);

        assert_eq!(
            ops(&base).bucket_versioning(&plane, &name).await.unwrap(),
            State::Unversioned
        );
        match ops(&base)
            .get_object_version(&plane, &name, &key, None, None)
            .await
            .unwrap()
        {
            ReadResult::Object(got) => {
                assert_eq!(read(got.body).await, b"plain");
                assert_eq!(got.info.version_id, None);
                assert!(
                    !got.info.bucket_records_versions,
                    "an unversioned bucket records no versions"
                );
            }
            ReadResult::Marker { .. } => panic!("an unversioned write is content, not a marker"),
        }
        // The state pair's write is visible, and the version pairs then answer
        // the null version's own row.
        ops(&base)
            .set_bucket_versioning(&plane, &name, State::Enabled)
            .await
            .unwrap();
        assert_eq!(
            ops(&base).bucket_versioning(&plane, &name).await.unwrap(),
            State::Enabled
        );
        match ops(&base)
            .head_object_version(&plane, &name, &key, None)
            .await
            .unwrap()
        {
            HeadResult::Object(info) => {
                assert!(info.bucket_records_versions);
                assert_eq!(info.version_id, None);
            }
            HeadResult::Marker { .. } => panic!("the null version is content"),
        }
    }

    /// The engine's versioned write stamps the row it supersedes.
    ///
    /// The mechanism is the store's (`advance_slot` records when the row it
    /// moved off stopped being current, in the same commit that records the
    /// incoming identity — pinned in redb's own suite), and the close-out
    /// filed the missing half: nothing asserted it **through the engine
    /// body**, so a composition that superseded without stamping, or stamped
    /// in a second transaction, would have gone unnoticed at this level.
    ///
    /// The ordering is the point: the stamp and the slot move are one commit,
    /// so a reader that sees the incoming current version has already seen the
    /// outgoing one stamped as history.
    #[tokio::test]
    async fn a_versioned_write_stamps_the_row_it_supersedes() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("stamped.txt").unwrap();
        let plane = base.version_plane();
        plane
            .versions
            .set_state(&name, State::Enabled)
            .await
            .unwrap();

        let first = put_versioned(&base, &plane, &name, &key, b"first", None).await;
        let first_id = first
            .version_id
            .clone()
            .expect("an enabled write mints an identity");
        let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
        assert_ne!(second.version_id, first.version_id);

        // The write moved the first version into history: its row is at the
        // sequence the slot left, and it says when it stopped being current.
        let (history_seq, history) = plane
            .versions
            .latest(&name, &key)
            .await
            .unwrap()
            .expect("the superseded version's history row");
        assert_eq!(
            history.id.as_ref(),
            Some(&first_id),
            "the history row is the version the write moved off"
        );
        assert_ne!(
            history.became_noncurrent, 0,
            "the engine's write stamps the row it supersedes"
        );

        // The same advance recorded the incoming identity: the engine's own
        // view of the slot names the version the write minted, at a sequence
        // the stamped row does not hold — the two halves of one commit cannot
        // be seen apart.
        let slot = plane.versions.current(&name, &key).await.unwrap();
        assert_eq!(
            slot.version.as_ref(),
            second.version_id.as_ref(),
            "the slot names the version the write minted"
        );
        assert_ne!(
            slot.seq, history_seq,
            "the slot moved off the sequence the stamped row sits at"
        );
    }
}
