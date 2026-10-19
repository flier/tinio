//! Bucket operations of the fs backend (task T041).
//!
//! Buckets are top-level directories of the storage root. Creation times
//! come from the `BUCKETS` table, lazily recorded on first sight of a
//! pre-existing directory. Names are re-validated on create (defensive
//! backstop, FR-012); the reserved `.tinio/` directory is never a bucket.

#[cfg(unix)]
use std::path::Path;
use std::{io::ErrorKind, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use tokio::fs;

use super::{Error, FsStorage};
use crate::{
    _core::{
        BucketsListing, ListBucketsParams, acl,
        acl::{Acl, AclGrants},
        bucket::{self, Bucket},
        cors, lifecycle, object,
        storage::{BucketOps, already_exists, no_such_bucket},
    },
    _ops::bucket::paginate_buckets,
    path::{STATE_DIR_NAME, bucket_path_lexical},
    tombstone,
};

/// The **content plane's** emptiness predicate: a bucket counts as empty when
/// it has no files and no directories anywhere (folder-marker directories are
/// content, per the conformance harness) and no `.tinio` staging residue
/// (FR-020 — the reserved segment is never served or listed, so a
/// crashed/failed cross-volume commit's staging dir must not make the bucket
/// undeletable; the startup repair clears it).
///
/// This is one **plane's** answer, deliberately not the whole emptiness
/// decision: the row planes' halves — an in-progress upload, and an
/// `object_meta` row (a delete marker writes no file under the tree and is
/// one) — are the ops engine's, which composes content, uploads and rows in
/// [`BucketOps::delete_bucket`](tinio_ops::BucketOps::delete_bucket). The
/// rule therefore has one home for every backend, instead of an fs-local
/// copy beside the directory walk.
///
/// The walk answers off one `read_dir`: the common non-empty bucket never
/// reaches the row planes at all.
pub(crate) async fn bucket_is_empty(
    storage: &FsStorage,
    name: &bucket::Name,
) -> Result<bool, Error> {
    let bucket_dir = storage.bucket_dir(name).await?;
    let mut entries = match fs::read_dir(&bucket_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            return Err(Error::Storage(no_such_bucket(name)));
        }
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name() == STATE_DIR_NAME {
            continue; // staging residue (FR-020): not content
        }
        return Ok(false); // any other file or directory is content
    }
    Ok(true)
}

impl FsStorage {
    /// The directory half of a bucket create: prove the name is free, create
    /// the directory and apply the unix ownership/mode hardening (FAIL-CLOSED:
    /// a hardening failure removes the just-created directory).
    ///
    /// This shape takes the bucket mutation lock itself: the directory is the
    /// content plane's whole create, and the ops engine's `create_bucket`
    /// reaches it through here too (`ObjectContent::create_bucket`), its row
    /// write following the lock's release.
    pub(crate) async fn create_bucket_dir(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
    ) -> Result<(), Error> {
        let _guard = self.lock_bucket_mutations(name).await;
        // The owner is the unix dir-hardening input only.
        #[cfg(not(unix))]
        let _ = owner;
        // The lexical mapping: the validation supplements (the reserved
        // `.tinio` refusal — FR-020 — and the Windows charset/aliasing
        // refusal — F21, the same clean `InvalidBucketName` the object
        // ops answer) plus the plain join; the containment-proven
        // `bucket_dir` answers NoSuchBucket for a missing bucket, so it
        // cannot build the create target (a name just proven absent is
        // safe to create under — path.rs, one home for the supplements).
        let dir = bucket_path_lexical(self.root(), name)?;
        // Defensive re-validation (FR-012) before any FS access; the
        // checked constructor is authoritative (it rejects the reserved
        // `.tinio` name, FR-020). A pre-existing entry of ANY type — a
        // directory, a symlinked/junction bucket directory resolving
        // outside the root, a stray file — is a *taken name*:
        // AlreadyExists, never a name-validation error.
        match fs::symlink_metadata(&dir).await {
            Ok(_) => return Err(already_exists(name).into()),
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        // The directory creation runs inside the guard taken above — the whole
        // of this namespace step, for both callers (the content contract's
        // `create_bucket` and the ops engine's `create_bucket`). The guard is
        // one critical section against `delete_bucket` (a delete must never
        // remove a bucket a create just reported as created).
        //
        // It does **not** span the create's record write, and that window is
        // open, not closed: on the lazy policy the engine's create writes its
        // row with an unconditional full-row upsert after returning from this
        // lock, so a concurrent `put_bucket_tags`/`put_bucket_acl` taken under
        // the same guard can still be overwritten by that upsert (the C1
        // lost-update window the wider section used to close). The narrowing is
        // RF2's: the engine's content side takes this non-re-entrant mutex
        // itself, so the guard cannot span the engine call, and the create's row
        // write is outside it. Recorded as a known exception to P4's
        // zero-observable-change acceptance.
        match fs::create_dir(&dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                return Err(already_exists(name).into());
            }
            Err(err) => return Err(err.into()),
        }
        // The unix ownership hardening (spec 2026-09-05 §5a): the fresh
        // bucket dir lands `0700` and chowned to the bucket owner's
        // mapped uid (an unmapped owner stays server-user-owned, still
        // `0700`). FAIL-CLOSED (review round 1): a hardening failure
        // removes the just-created dir (the mutation lock is held —
        // nothing else lives in it) and errors the create — a
        // wrongly-owned `0700` bucket dir must not be left behind with
        // the create reporting success, or every object of the mapped
        // owner under it is stranded.
        #[cfg(unix)]
        if let Err(err) = harden_bucket_dir(&dir, self.owner_uid(owner)).await {
            let _ = fs::remove_dir(&dir).await;
            return Err(err);
        }
        Ok(())
    }

    /// The bucket teardown: unpublish the directory (the tombstone rename),
    /// drop the bucket's rows and derived state — `remove_bucket_state` owns
    /// the `BUCKETS` row and the version/blob tables — and hand the tree to
    /// the removal pipeline. It is the whole of `delete_bucket`, and the one
    /// teardown the S3 surface's `DeleteBucket` and the content contract's
    /// `remove_bucket` both end in: the emptiness rule is not restated beside
    /// it, both enter through [`BucketOps::delete_bucket`] (the guard, the
    /// namespace probe and the ops engine's three-probe composition).
    ///
    /// The caller holds the bucket's mutation guard — [`BucketOps::delete_bucket`]
    /// takes it and runs the ops engine's probes inside it, and
    /// `Hooks::before_bucket_delete` (this method) runs under that same hold
    /// — and has established that the bucket holds no content. Taking the
    /// lock again here would deadlock the per-bucket mutex, which the map
    /// hands out non-re-entrantly.
    pub(crate) async fn unpublish_bucket_locked(&self, name: &bucket::Name) -> Result<(), Error> {
        // Unpublish: rename the lexical root entry onto
        // `<root>/.tinio/deleting/<id>` (same volume as the name — a
        // relocated state dir must not receive this rename). The live
        // name is gone before the tree walk; a crash leaves private
        // residue the startup repair reclaims. Followed-symlink buckets:
        // the directory *entry* under root moves (the link), not the
        // canonical target.
        let live = bucket_path_lexical(self.root(), name)?;
        let dest = tombstone::prepare(self.root()).await?;
        fs::rename(&live, &dest).await?;
        // The name is gone — the delete has succeeded — so a state
        // failure must NOT fail the response (the client would see an
        // error for a delete that happened); leaked rows are
        // reclaimed by the startup repair.
        if let Err(err) = self.remove_bucket_state(name).await {
            tracing::warn!(error = %err, "bucket state not removed after delete");
        }
        // The directory is unpublished: the per-bucket mutex still
        // serializes a parked PUT against recreate (the waiter holds the
        // slot until it fails `ensure_bucket`). Tree delete is slow IO —
        // fire-and-forget on the REMOVAL pipeline (D-A, Q4 blocking
        // `remove_dir_all`), physically isolated from ETag compute so a
        // large tree walk can never occupy the IO workers. The request
        // does not wait; a leftover is reclaimed by doctor / the scanner.
        tombstone::reclaim(Arc::clone(&self.remove_pipeline), dest);
        Ok(())
    }

    /// The unix ownership/mode hardening of a bucket directory the ops
    /// engine's create just made (spec 2026-09-05 §5a), as its own step
    /// because the content contract carries no owner — the same trade
    /// `publish` documents for object files. Inert off unix.
    ///
    /// FAIL-CLOSED: a failure removes the record the engine wrote and the
    /// directory, and errors the create — a wrongly-owned `0700` bucket dir
    /// must not be left behind with the create reporting success.
    ///
    /// The bucket's mutation guard is taken **around this step alone**. It
    /// cannot span the engine call above it (see [`BucketOps::create_bucket`],
    /// which takes the engine's content-side lock internally and is
    /// non-re-entrant), so the rollback is the part that must be serialized:
    /// without the guard it could remove a concurrent create's fresh directory.
    /// **Residual race, stated**: a create that ran between the engine call and
    /// this guard is not serialized against the rollback — it would lose its
    /// directory and its record to a hardening failure of a *different*
    /// generation's create, which is the price of the engine's create ordering
    /// and is filed against the plan's "the lock is acquired by the trait impl
    /// around the engine call".
    #[cfg(unix)]
    async fn harden_created_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
    ) -> Result<(), Error> {
        use crate::{_core::meta::BucketMeta, _store_redb::RedbMetadata};

        let _guard = self.lock_bucket_mutations(name).await;
        let dir = bucket_path_lexical(self.root(), name)?;
        if let Err(err) = harden_bucket_dir(&dir, self.owner_uid(owner)).await {
            let _ = fs::remove_dir(&dir).await;
            // The record the engine just wrote goes with it: the create answers
            // an error, so the bucket must not stay recorded either.
            if let Err(cleanup) =
                <RedbMetadata as BucketMeta>::remove(&self.meta_store().engine(), name).await
            {
                tracing::warn!(
                    error = %cleanup,
                    "the refused create's bucket record was not removed"
                );
            }
            return Err(err);
        }
        Ok(())
    }

    /// [`Self::harden_created_bucket`] off unix: nothing to harden, and so
    /// nothing to serialize — the guarded step exists for the rollback the
    /// unix arm may run, and this arm has neither.
    #[cfg(not(unix))]
    async fn harden_created_bucket(
        &self,
        _name: &bucket::Name,
        _owner: Option<&acl::OwnerId>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

/// The unix ownership/mode hardening of a fresh bucket directory
/// (spec 2026-09-05 §5a): `0700`, chowned to the owner's mapped uid when the
/// configuration maps one (an unmapped owner stays server-user-owned, still
/// `0700`). One home for the two create paths — the content plane's
/// `create_bucket` and the ops engine's create under the backend's own step.
#[cfg(unix)]
async fn harden_bucket_dir(dir: &Path, uid: Option<u32>) -> Result<(), Error> {
    use std::{fs::Permissions, os::unix::fs::PermissionsExt};

    use crate::{fsutil, fsutil::DIR_MODE_PRIVATE};

    fs::set_permissions(dir, Permissions::from_mode(DIR_MODE_PRIVATE)).await?;
    if let Some(uid) = uid {
        fsutil::chown_file(dir, uid).await?;
    }
    Ok(())
}

#[async_trait]
impl BucketOps for FsStorage {
    async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        // The namespace and its record are the ops engine's (one logical
        // create: the directory then its row, and a record failure leaves the
        // directory the next create reports as taken). The **unix ownership
        // hardening** is fs's own step, because the content contract carries no
        // owner — the same trade `publish` documents for object files — and it
        // is fail-closed (`harden_created_bucket`).
        //
        // No bucket-mutation guard spans this call: the engine's content side
        // takes that lock for the directory itself (`create_bucket_dir`), and
        // the per-bucket mutex is not re-entrant. The accept-and-document
        // ruling (RF2/RF4): the guard's narrow home is
        // `harden_created_bucket`, and the record write this call makes on the
        // lazy policy — an unconditional full-row upsert — is outside it, so a
        // concurrent element write under the guard can be overwritten by it.
        // That residual window is recorded as a known exception to P4's
        // zero-observable-change acceptance, not apologized for: the mutex's
        // non-re-entrancy leaves no ordering that closes it without giving up
        // the engine's create.
        self.ops()
            .create_bucket(name, owner, acl)
            .await
            .map_err(Error::from)?;
        self.harden_created_bucket(name, owner).await
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // The engine's probes and its commit point, inside the bucket's
        // mutation guard: the guard must span the emptiness check and the
        // unpublish rename (a concurrent PUT's rename cannot land in a
        // directory about to disappear with 200 returned), and the teardown the
        // hook runs takes no lock of its own. `ensure_bucket` is the namespace
        // probe the content plane has no call for — a missing directory is
        // `NoSuchBucket`, never one of the probes' answers.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ensure_bucket(name).await?;
        self.ops().delete_bucket(name).await.map_err(Error::from)
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        // The namespace probe is fs's (the directory IS the bucket); the row
        // work is the engine's, and the lazy row policy records the first sight
        // when a pre-existing directory has no row yet.
        self.ensure_bucket(name).await?;
        self.ops().head_bucket(name).await.map_err(Error::from)
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // fs's own body, not the engine's: the engine's `list_buckets` reads
        // the BUCKET ROWS, and fs's buckets exist as root directories — a
        // pre-existing directory has no row until it is sighted, so the
        // candidate set has to be the content-side sweep. The candidate
        // **source** is what stays fs's; the owner filter, the exclusive
        // marker and the page cut are the engine's own rules, taken from their
        // one home ([`paginate_buckets`]) over this sweep's candidates.
        //
        // `max = 0` still short-circuits the root sweep — no dirent scan, no
        // stats, no metadata reads (the cost-profile parity with the mem
        // backend, which never drains the table for an empty page); the shared
        // rule produces the empty page itself, so the contract has one home.
        //
        // The `owner` filter applies during the walk, before pagination
        // (contract review P2#4 — the continuation-token math applies to
        // the filtered set): the candidate names collect first (the
        // filtered walk cannot stream into the pager — each name's row
        // must be read before the offer), then one read transaction
        // resolves every candidate's recorded owner element, and only
        // the principal's buckets reach the page. The comparison uses
        // the EFFECTIVE owner — the recorded element, or `lazy_default`
        // for a row whose wire is empty (the uniform lazy-default
        // semantics, review B4 — such rows belong to the default
        // principal); `lazy_default = None` is the strict compare.
        // `None` owner = no filter, the legacy listing, with no owners
        // read at all.
        let mut names = Vec::new();
        if params.max_buckets > 0 {
            self.for_each_bucket_name(&params.prefix, |name| names.push(name))
                .await?;
        }
        let recorded = match owner {
            Some(_) => self.bucket_store.owners(&names).await?,
            None => vec![None; names.len()],
        };
        let (page, truncated, next) = paginate_buckets(
            names.into_iter().zip(recorded).collect(),
            &params,
            owner,
            lazy_default,
            |name: &bucket::Name| name.as_ref().as_str(),
        );
        // Creation times resolve only for the page's buckets — the
        // metadata-per-page analogue of the object listing's page-driven
        // ETag gate (P3) — in ONE read transaction (`load_many`, the
        // `meta::Store::load_entries` pattern; the old per-bucket
        // `get_or_record` opened one transaction per bucket). First-sight
        // recording stays page-driven: a bucket not reached by
        // pagination stays unrecorded until listed; still lazy, no
        // visible behavior change. A miss keeps the atomic upsert of
        // `get_or_record` — through the record-only `get_or_insert` (one
        // write transaction, no pre-read; `load_many` already
        // established the row is missing), so concurrent first-sights
        // converge; misses are rare in steady state, so their own
        // transactions are fine.
        let created = self.bucket_store.load_many(&page).await?;
        let mut buckets = Vec::with_capacity(page.len());
        for (name, creation_time) in page.into_iter().zip(created) {
            let creation_time = match creation_time {
                Some(created) => created,
                None => {
                    self.bucket_store
                        .get_or_insert(&name, SystemTime::now())
                        .await?
                }
            };
            buckets.push(Bucket {
                name,
                creation_time,
            });
        }
        Ok(BucketsListing {
            buckets,
            truncated,
            next_start_after: next,
        })
    }

    async fn get_bucket_tags(&self, name: &bucket::Name) -> Result<object::Tags, Error> {
        // Existence is the bucket directory (`NoSuchBucket` when missing —
        // mirroring head_bucket); the tags are the engine's lazy-row read: the
        // `BUCKETS` row's element, empty when the bucket has no row yet (a
        // pre-existing bucket has never been tagged through the API).
        self.ensure_bucket(name).await?;
        self.ops().get_bucket_tags(name).await.map_err(Error::from)
    }

    async fn put_bucket_tags(&self, name: &bucket::Name, tags: &object::Tags) -> Result<(), Error> {
        // The bucket's mutation lock, the `put_bucket_acl` shape: the tags
        // write is element-targeted, and the engine's lazy-row body records a
        // row-less bucket's row WITH the tags in one transaction. The lock
        // serializes it against the other guard-takers — but **not** against the
        // create's record write, which on the lazy policy is a full-row upsert
        // running outside this guard (RF2/RF4's stated residual): a create
        // landing after this write still overwrites the tag element, the C1
        // lost update. Recorded as a known exception; the guard is kept because
        // it is what the delete teardown and the element writes coordinate on.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ensure_bucket(name).await?;
        self.ops()
            .put_bucket_tags(name, tags)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete-object/bucket leniency). The row's creation
        // time is preserved; a row-less bucket has nothing to clear. The
        // bucket's mutation lock serializes the clear against the other
        // guard-takers (the teardown and the sibling element writes) — it does
        // **not** reach a concurrent create's record write, which on the lazy
        // policy is a full-row upsert running outside the guard (RF2/RF4's
        // stated residual): a create landing after this clear restores its
        // create-time row, tag element included.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ops()
            .delete_bucket_tags(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_cors(&self, name: &bucket::Name) -> Result<Option<cors::Config>, Error> {
        // Existence is the bucket directory (`NoSuchBucket` when missing —
        // mirroring `get_bucket_tags`); the configuration is the engine's
        // lazy-row read, None when the bucket has no configuration (the `''`
        // wire — a bucket that was cleared or never configured).
        self.ensure_bucket(name).await?;
        self.ops().get_bucket_cors(name).await.map_err(Error::from)
    }

    async fn put_bucket_cors(&self, name: &bucket::Name, cors: &cors::Config) -> Result<(), Error> {
        self.ensure_bucket(name).await?;
        self.ops()
            .put_bucket_cors(name, cors)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), Error> {
        // S3 semantics: `NoSuchBucket` when the bucket is missing — the
        // delete-tagging idempotent leniency does NOT extend here (the
        // CORS spec mandates `NoSuchBucket` for a missing bucket); a
        // bucket that simply has no configuration clears to the same
        // state (idempotent). The row's creation time is preserved. The
        // bucket's mutation lock serializes the clear against the other
        // guard-takers — not against a concurrent create's record write, which
        // on the lazy policy is a full-row upsert running outside the guard
        // (RF2/RF4's stated residual): a create landing after this clear
        // restores its create-time row, the CORS configuration cleared with it.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ensure_bucket(name).await?;
        self.ops()
            .delete_bucket_cors(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, Error> {
        // Existence is the bucket directory, the `get_bucket_cors` shape; the
        // configuration is the engine's, None when the bucket stores none.
        self.ensure_bucket(name).await?;
        self.ops()
            .get_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn put_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), Error> {
        self.ensure_bucket(name).await?;
        self.ops()
            .put_bucket_lifecycle(name, config)
            .await
            .map_err(Error::from)
    }

    async fn merge_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        self.ensure_bucket(name).await?;
        self.ops()
            .merge_bucket_lifecycle(name, rules)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_lifecycle(&self, name: &bucket::Name) -> Result<(), Error> {
        // `NoSuchBucket` for a missing bucket (the CORS shape, not the
        // delete-tagging leniency); a bucket that stores none clears to the
        // same state. The mutation lock is the `delete_bucket_cors` one, with
        // its stated residual against a concurrent create's record write.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ensure_bucket(name).await?;
        self.ops()
            .delete_bucket_lifecycle(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, Error> {
        // Existence is the bucket directory (`NoSuchBucket` when missing —
        // mirroring head_bucket); the ACL is the engine's lazy-row read: the
        // recorded owner element recombined with the stored grant set, and a
        // row-less bucket (a legacy directory with no `BUCKETS` row until first
        // sight) serves the default private ACL rather than refusing.
        self.ensure_bucket(name).await?;
        self.ops().get_bucket_acl(name).await.map_err(Error::from)
    }

    async fn put_bucket_acl(&self, name: &bucket::Name, grants: &AclGrants) -> Result<(), Error> {
        // The bucket's mutation lock, the `put_bucket_tags` shape: the ACL
        // write is element-targeted, but a concurrent `create_bucket` writes
        // the create-time record — the lazy store's arm is an **upsert** — so
        // unserialized it can land after this write and replace the grants and
        // the recorded owner with the create-time row. The store's setter
        // itself no longer holds a cross-transaction read-modify-write to
        // guard:
        // `set_grants_or_create` reads the row, replaces the grants and
        // carries the recorded owner over **inside one write transaction**
        // (the contract's setter replaces both ACL elements), so no writer
        // can slip between its read and its write. What the guard still
        // serializes is this write against the other guard-takers (the delete
        // teardown, the tag/CORS element writes); it does **not** reach the
        // create's record write, which on the lazy policy is a full-row upsert
        // running outside it (RF2/RF4's stated residual): a create landing
        // after this write replaces the grants and the recorded owner with its
        // create-time row. The owner drives GetBucketAcl and the ListBuckets
        // owner filter, which is why that residual is the one the round
        // records rather than hides.
        let _guard = self.lock_bucket_mutations(name).await;
        self.ensure_bucket(name).await?;
        self.ops()
            .put_bucket_acl(name, grants)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs,
        future::pending,
        time::{Duration, SystemTime},
    };

    use tokio::time::timeout;

    use super::*;
    use crate::{
        _core::{
            acl,
            acl::Acl,
            object,
            storage::{
                BucketsListing, Error as StorageError, Error::*, ListBucketsParams, MultipartOps,
                ObjectOps, VersionOps,
            },
            version::State,
        },
        _store_redb::version as version_rows,
        _util::testing::{assert_conformance, body, etag, metadata::lifecycle_config},
        database,
        testutil::{
            all_users_read_grant, other_owner_id, owner_id, storage, wait_for, wait_for_lock_waiter,
        },
        tombstone,
    };

    #[tokio::test]
    async fn conformance_green() {
        let (_root, storage) = storage();
        assert_conformance(&storage).await;
    }

    #[tokio::test]
    async fn a_read_only_store_serves_a_rowless_bucket_without_recording_it() {
        // FR-023 amended ("Read paths"): a read over a planted root **degrades**
        // — the lazy first sight answers `now` without writing the row back —
        // while every other refusal still propagates. The fixture is the
        // cucumber read-only one: a directory-only bucket, no `BUCKETS` row,
        // and a state database the read-only handle cannot open, so every
        // metadata write is refused at the handle.
        use crate::{_core::mode::AccessMode, FsOptions, testutil::fs_options};

        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("data")).unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        let before = SystemTime::now();
        let head = storage.head_bucket(&b).await.unwrap();
        assert_eq!(head.name, b);
        assert!(
            head.creation_time >= before,
            "the first sight answers `now`, unrecorded"
        );
        assert!(
            storage.bucket_store().load_all().await.unwrap().is_empty(),
            "a read wrote nothing back"
        );
    }

    #[tokio::test]
    async fn a_read_only_store_refuses_a_marker_only_bucket() {
        // Invariant 8 of the shared-emptiness change: the refusal must not
        // depend on the store's ability to write. A delete marker is an
        // `object_meta` row with no file, so the tree alone reads empty and
        // only the row probe can refuse — and that probe is read-only, so it
        // answers `NotEmpty` on a store whose every write is refused at the
        // handle, before the unpublish's first write is reached. The
        // read-only **delete** path was otherwise untested on fs.
        use crate::{_core::mode::AccessMode, FsOptions, testutil::fs_options};

        // The marker state is built by a writable handle over the root; the
        // read-only arm below is the same root reopened.
        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        let key = object::key("notes.txt").unwrap();
        {
            let storage = FsStorage::new(root.path(), fs_options()).unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"payload"))
                .await
                .unwrap();
            let marker = storage.delete_object(&b, &key).await.unwrap();
            assert!(marker.is_delete_marker, "the marker is the live version");
        }
        assert_eq!(
            fs::read_dir(root.path().join("data")).unwrap().count(),
            0,
            "the marker leaves no file: the tree alone reads empty"
        );

        // Reopen the root read-only: the state database is the root's own,
        // opened read-only (FR-023 amended), so the row probe still answers.
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                ..fs_options()
            },
        )
        .unwrap();
        // The handle reads the root's own database, not the degraded empty
        // view: the bucket row is visible through it — which is what makes
        // the row probe below the whole of the refusal.
        assert!(
            !storage.bucket_store().load_all().await.unwrap().is_empty(),
            "the read-only handle reads the root's recorded state"
        );
        // The premise the leg rests on: this handle really cannot write — a
        // bucket-row element write is refused at the state layer (nothing is
        // published to the tree, so the fixture is untouched).
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        let refused = storage.put_bucket_tags(&b, &tags).await.unwrap_err();
        assert!(
            refused.is_read_only(),
            "a read-only handle must refuse the element write: {refused:?}"
        );

        let err: StorageError = storage.delete_bucket(&b).await.unwrap_err().into();
        assert!(matches!(err, NotEmpty(_)), "{err:?}");
        assert!(
            root.path().join("data").is_dir(),
            "the refusal ran before the unpublish: the bucket is still there"
        );
    }

    #[tokio::test]
    async fn create_head_list_delete() {
        let (root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        assert!(storage.head_bucket(&b).await.is_err());

        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        assert_eq!(storage.head_bucket(&b).await.unwrap().name, b);

        // The bucket is a real directory.
        assert!(root.path().join("my-bucket").is_dir());

        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(listing.buckets.len(), 1);
        assert_eq!(listing.buckets[0].name, b);

        // Duplicate create.
        let err: StorageError = storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, AlreadyExists(_)));

        // Delete.
        storage.delete_bucket(&b).await.unwrap();
        assert!(storage.head_bucket(&b).await.is_err());
        assert!(!root.path().join("my-bucket").exists());
    }

    #[tokio::test]
    async fn mutation_lock_is_per_bucket() {
        let (_root, storage) = storage();
        let a = bucket::name("alpha").unwrap();
        let b = bucket::name("beta").unwrap();
        storage
            .create_bucket(&a, None, &Acl::default_private(None))
            .await
            .unwrap();
        let _guard = storage.lock_bucket_mutations(&a).await;
        let storage2 = storage.clone();
        let create_b = tokio::spawn(async move {
            storage2
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
        });
        let created = timeout(Duration::from_millis(500), create_b)
            .await
            .expect("create of a different bucket must not wait on another bucket's lock")
            .unwrap();
        created.unwrap();
    }

    #[tokio::test]
    async fn mutation_lock_serializes_same_bucket() {
        let (_root, storage) = storage();
        let a = bucket::name("alpha").unwrap();
        let _guard = storage.lock_bucket_mutations(&a).await;
        let storage2 = storage.clone();
        let create_a = tokio::spawn(async move {
            storage2
                .create_bucket(&a, None, &Acl::default_private(None))
                .await
        });
        assert!(
            timeout(Duration::from_millis(80), create_a).await.is_err(),
            "create of the same bucket must wait on the mutation lock"
        );
    }

    #[tokio::test]
    async fn delete_bucket_unpublishes_into_deleting_dir() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(!root.path().join("data").exists());
        let deleting = tombstone::dir(root.path());
        assert!(
            deleting.is_dir(),
            "delete must stage the bucket under .tinio/deleting"
        );
        wait_for(|| {
            fs::read_dir(&deleting)
                .map(|mut d| d.next().is_none())
                .unwrap_or(false)
        })
        .await;
    }

    #[tokio::test]
    async fn delete_bucket_tombstone_stays_on_the_data_volume() {
        // A relocated state dir may be another volume (FR-023); the
        // tombstone must stay under the storage root so the unpublish
        // rename cannot hit EXDEV.
        use crate::{FsOptions, testutil::fs_options};
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(!root.path().join("data").exists());
        assert!(tombstone::dir(root.path()).is_dir());
        assert!(!state.path().join("deleting").exists());
    }

    #[tokio::test]
    async fn delete_bucket_does_not_split_the_mutation_lock() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_d = storage.clone();
        let bd = b.clone();
        let delete = tokio::spawn(async move { storage_d.delete_bucket(&bd).await });
        wait_for_lock_waiter().await;
        let storage_w = storage.clone();
        let bw = b.clone();
        let waiter = tokio::spawn(async move {
            let _guard = storage_w.lock_bucket_mutations(&bw).await;
            pending::<()>().await;
        });
        wait_for_lock_waiter().await;
        drop(held);
        delete.await.unwrap().unwrap();
        let storage_c = storage.clone();
        let bc = b.clone();
        let created = timeout(
            Duration::from_millis(80),
            tokio::spawn(async move {
                storage_c
                    .create_bucket(&bc, None, &Acl::default_private(None))
                    .await
            }),
        )
        .await;
        waiter.abort();
        assert!(
            created.is_err(),
            "recreate must wait on the doomed waiter of the deleted name"
        );
    }

    #[tokio::test]
    async fn delete_create_put_hammer_keeps_successful_puts_in_the_live_generation() {
        let (root, storage) = storage();
        let b = bucket::name("hammered").unwrap();
        let k = object::key("victim.bin").unwrap();
        for _ in 0..25 {
            let round = timeout(Duration::from_secs(15), async {
                let mut handles = Vec::new();
                for i in 0..6 {
                    let storage = storage.clone();
                    let b = b.clone();
                    let k = k.clone();
                    handles.push(tokio::spawn(async move {
                        match i % 3 {
                            // delete / create: tolerated to fail
                            // (`NotEmpty`, `Io`, `AlreadyExists`).
                            0 => {
                                let _ = storage.delete_bucket(&b).await;
                                None
                            }
                            1 => {
                                let _ = storage.create_bucket(&b, None, &Acl::default_private(None)).await;
                                None
                            }
                            // PUT: only `NoSuchBucket` is benign.
                            _ => Some(
                                storage
                                    .put_object(&b, &k, body(b"payload"))
                                    .await
                                    .map(|_| ()),
                            ),
                        }
                    }));
                }
                for handle in handles {
                    let Some(result) = handle.await.unwrap() else {
                        continue;
                    };
                    match result {
                        Ok(()) => {
                            assert!(
                                storage.head_object(&b, &k).await.is_ok(),
                                "a successful PUT must be readable from the live bucket"
                            );
                        }
                        Err(err) => {
                            let err: StorageError = err.into();
                            assert!(
                                matches!(err, NoSuchBucket(_) | NoSuchKey(_)),
                                "a failed PUT must be NoSuchBucket/NoSuchKey (it never committed), not {err:?}"
                            );
                        }
                    }
                }
            })
            .await;
            assert!(round.is_ok(), "one hammer round must finish in time");
        }
        // The removal lane drains the tombstones: the unpublished
        // trees disappear from `.tinio/deleting/` (reclaim is async,
        // like `delete_bucket_unpublishes_into_deleting_dir`).
        let deleting = tombstone::dir(root.path());
        wait_for(|| {
            fs::read_dir(&deleting)
                .map(|mut d| d.next().is_none())
                .unwrap_or(false)
        })
        .await;
    }

    #[tokio::test]
    async fn delete_non_empty_is_not_empty() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .put_object(&b, &"a.txt".into(), body(b"x"))
            .await
            .unwrap();
        let err: StorageError = storage.delete_bucket(&b).await.unwrap_err().into();
        assert!(matches!(err, NotEmpty(_)));
        // Deleting the object frees the bucket.
        storage.delete_object(&b, &"a.txt".into()).await.unwrap();
        storage.delete_bucket(&b).await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinked_bucket_follows_when_enabled_and_invisible_when_disabled() {
        use crate::{
            _core::{acl::Acl, storage::ListObjectsParams},
            _util::testing::read_body,
            FsOptions,
            testutil::fs_options,
        };
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), root.path().join("linked")).unwrap();
        let b = bucket::name("linked").unwrap();
        let k = object::key("a.txt").unwrap();

        // follow_symlinks = false: the bucket is invisible — direct
        // ops answer NoSuchBucket (not a path error), the name is
        // taken for create, and discovery omits it.
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: false,
                ..fs_options()
            },
        )
        .unwrap();
        let err: StorageError = storage.head_bucket(&b).await.unwrap_err().into();
        assert!(matches!(err, NoSuchBucket(_)), "{err:?}");
        let err: StorageError = storage
            .put_object(&b, &k, body(b"x"))
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)), "{err:?}");
        let err: StorageError = storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, AlreadyExists(_)), "{err:?}");
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert!(listing.buckets.is_empty(), "{listing:?}");

        // follow_symlinks = true: the bucket IS the target — full
        // CRUD through the link, listed and discovered like any
        // other bucket.
        drop(storage);
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                ..fs_options()
            },
        )
        .unwrap();
        storage.put_object(&b, &k, body(b"hello")).await.unwrap();
        assert!(outside.path().join("a.txt").exists());
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.size, 5);
        let got = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"hello");
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(listing.buckets.len(), 1);
        assert_eq!(listing.buckets[0].name, b);
        let page = storage
            .list_objects(ListObjectsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 100,
            })
            .await
            .unwrap();
        assert_eq!(page.objects.len(), 1);
        // Delete resolves through the link (the follow policy).
        storage.delete_object(&b, &k).await.unwrap();
        assert!(!outside.path().join("a.txt").exists());
    }

    #[tokio::test]
    async fn bucket_with_only_staging_residue_is_empty() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        fs::create_dir_all(root.path().join("data/.tinio")).unwrap();
        fs::write(root.path().join("data/.tinio/aaaa"), b"residue").unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(!root.path().join("data").exists());
    }

    #[tokio::test]
    async fn delete_bucket_with_uploads_is_not_empty() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let err: StorageError = storage.delete_bucket(&b).await.unwrap_err().into();
        assert!(matches!(err, NotEmpty(_)));
        storage
            .abort_multipart_upload(&b, &"big.bin".into(), &upload.upload_id)
            .await
            .unwrap();
        storage.delete_bucket(&b).await.unwrap();
    }

    #[tokio::test]
    async fn pre_existing_directories_are_buckets_with_lazy_creation_time() {
        let (root, storage) = storage();
        fs::create_dir(root.path().join("existing")).unwrap();
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(listing.buckets.len(), 1);
        assert_eq!(listing.buckets[0].name.as_ref(), "existing");
        // The lazy record persists.
        let head = storage
            .head_bucket(&bucket::name("existing").unwrap())
            .await
            .unwrap();
        assert_eq!(head.name.as_ref(), "existing");
    }

    #[tokio::test]
    async fn tinio_and_root_files_are_not_buckets() {
        let (root, storage) = storage();
        // The state dir already exists (constructed with the backend);
        // a `.tinio` directory must never surface as a bucket.
        fs::create_dir_all(root.path().join(".tinio")).unwrap();
        fs::write(root.path().join("file.txt"), b"x").unwrap();
        fs::create_dir(root.path().join("Big")).unwrap(); // invalid name
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert!(listing.buckets.is_empty(), "{listing:?}");
    }

    #[tokio::test]
    async fn bucket_delete_prunes_private_state() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .put_object(&b, &"a.txt".into(), body(b"x"))
            .await
            .unwrap();
        storage.delete_object(&b, &"a.txt".into()).await.unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(storage.bucket_store().load_all().await.unwrap().is_empty());
        assert!(storage.meta_store().walk(&b).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn remove_bucket_state_clears_every_derived_table_and_the_blocks() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        let k = object::key("a.txt").unwrap();
        // Bucket row (and the bucket directory the versioned write needs).
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        // Object meta row.
        storage
            .meta_store()
            .set(
                &b,
                &k,
                &etag("9dd4e461268c8034f5c8564e155c67a6"),
                3,
                SystemTime::now(),
                0,
            )
            .await
            .unwrap();
        // Upload + part rows.
        let upload = storage
            .multipart_store()
            .create(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .multipart_store()
            .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"), None)
            .await
            .unwrap();
        // A versioned key: two PUTs leave a history row naming a stored
        // block — the rows and the block subtree the teardown must take.
        storage
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();
        // The bucket's lifecycle row (LC19). The fs bucket delete never
        // reaches `BucketMeta::remove`, so this teardown is where the row has
        // to leave — and `Cleanup`'s stale-bucket stage runs this same call.
        storage
            .put_bucket_lifecycle(&b, &lifecycle_config("one"))
            .await
            .unwrap();
        storage.put_object(&b, &k, body(b"one")).await.unwrap();
        storage.put_object(&b, &k, body(b"two")).await.unwrap();
        assert!(storage.blobs_dir().join("my-bucket").is_dir());

        storage.remove_bucket_state(&b).await.unwrap();

        assert!(storage.bucket_store().load_all().await.unwrap().is_empty());
        assert!(storage.meta_store().walk(&b).await.unwrap().is_empty());
        assert!(!storage.multipart_store().has_uploads(&b).await.unwrap());
        assert!(
            storage
                .multipart_store()
                .walk_uploads()
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage.bucket_versioning(&b).await.unwrap(),
            Default::default(),
            "the bucket's versioning state is gone"
        );
        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            None,
            "the bucket's lifecycle row is gone (LC19)"
        );
        let drained = b.clone();
        let rows_left = storage
            .handle()
            .read(move |txn| -> Result<Vec<String>, database::Error> {
                let table = version_rows::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket_of(drained.as_ref(), |key, _| {
                    out.push(key.to_owned());
                    Ok(())
                })?;
                Ok(out)
            })
            .unwrap();
        assert!(
            rows_left.is_empty(),
            "version rows drained with the bucket: {rows_left:?}"
        );
        assert!(
            !storage.blobs_dir().join("my-bucket").exists(),
            "the bucket's block subtree goes with its state"
        );
    }

    #[tokio::test]
    async fn many_buckets_list_sorted() {
        let (_root, storage) = storage();
        for name in ["zeta", "alpha", "mid"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        let names: Vec<&str> = listing
            .buckets
            .iter()
            .map(|b| b.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["alpha", "mid", "zeta"]);
    }

    #[tokio::test]
    async fn list_buckets_max_zero_short_circuits_the_root_sweep() {
        // The state dir is relocated so nothing open lives under the
        // root: the empty-page request must not touch the root at all —
        // it succeeds even when any sweep would fail (the cost-profile
        // parity with the mem backend, which pages without draining).
        use crate::{FsOptions, testutil::fs_options};
        let root = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                state_dir: Some(state.path().to_path_buf()),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        // Move the root aside: `bucket_names` would fail its `read_dir`.
        let moved = root.path().with_extension("swept");
        fs::rename(root.path(), &moved).unwrap();
        let listing = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 0,
                },
                None,
                None,
            )
            .await;
        fs::rename(&moved, root.path()).unwrap();
        let listing = listing.expect("max_buckets = 0 must not touch the storage root");
        assert!(listing.buckets.is_empty());
        assert!(!listing.truncated);
        assert_eq!(listing.next_start_after, None);
    }

    #[tokio::test]
    async fn fs_bucket_tags_round_trip_and_replace() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        assert!(
            storage.get_bucket_tags(&b).await.unwrap().is_empty(),
            "an untagged bucket answers the empty set"
        );

        // Put → Get round-trip (replace-all, no merge).
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        storage.put_bucket_tags(&b, &tags).await.unwrap();
        assert_eq!(storage.get_bucket_tags(&b).await.unwrap(), tags);
        let replaced = object::Tags::from_pairs([("team".into(), "edge".into())]).unwrap();
        storage.put_bucket_tags(&b, &replaced).await.unwrap();
        assert_eq!(storage.get_bucket_tags(&b).await.unwrap(), replaced);

        // head_bucket still reports the creation time (the row's other
        // element survives the tag writes).
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(
            head.creation_time <= SystemTime::now(),
            "the creation time must survive bucket tagging"
        );

        // Delete clears.
        storage.delete_bucket_tags(&b).await.unwrap();
        assert!(storage.get_bucket_tags(&b).await.unwrap().is_empty());

        // Missing bucket: get/put → NoSuchBucket; delete succeeds
        // (idempotent, like the object tagging delete).
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage.get_bucket_tags(&ghost).await.unwrap_err().into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage
            .put_bucket_tags(&ghost, &tags)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
        storage.delete_bucket_tags(&ghost).await.unwrap();
    }

    #[tokio::test]
    async fn fs_bucket_cors_round_trip_and_replace() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        assert!(
            storage.get_bucket_cors(&b).await.unwrap().is_none(),
            "an unconfigured bucket answers None"
        );

        let config = cors::Config {
            rules: vec![
                cors::Rule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: Some(vec!["x-amz-*".into()]),
                    expose_headers: Some(vec!["ETag".into()]),
                    max_age_seconds: Some(60),
                },
                cors::Rule {
                    id: None,
                    allowed_methods: vec!["PUT".into(), "DELETE".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        // Put -> Get round-trip (replace-all, no merge; order + fields kept).
        storage.put_bucket_cors(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_cors(&b).await.unwrap(),
            Some(config.clone())
        );

        // op-review G2: a zero-rule config through the whole backend must
        // be indistinguishable from "no configuration".
        storage
            .put_bucket_cors(&b, &cors::Config::default())
            .await
            .unwrap();
        assert_eq!(storage.get_bucket_cors(&b).await.unwrap(), None);
        storage.put_bucket_cors(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_cors(&b).await.unwrap(),
            Some(config.clone())
        );

        // head_bucket still reports the creation time (the row's other
        // elements survive the CORS writes).
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(
            head.creation_time <= SystemTime::now(),
            "the creation time must survive bucket CORS writes"
        );

        // Delete clears; the delete is idempotent on the stored config.
        storage.delete_bucket_cors(&b).await.unwrap();
        assert_eq!(storage.get_bucket_cors(&b).await.unwrap(), None);
        storage.delete_bucket_cors(&b).await.unwrap();

        // Missing bucket: get/put/delete -> NoSuchBucket (the CORS spec -
        // unlike the delete-bucket-tags idempotent Ok).
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage.get_bucket_cors(&ghost).await.unwrap_err().into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage
            .put_bucket_cors(&ghost, &config)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage.delete_bucket_cors(&ghost).await.unwrap_err().into();
        assert!(matches!(err, NoSuchBucket(_)));
    }

    #[tokio::test]
    async fn lifecycle_round_trips_and_needs_a_bucket() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let config = lifecycle_config("one");

        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            None,
            "an unconfigured bucket answers None"
        );
        storage.put_bucket_lifecycle(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            Some(config.clone()),
            "the configuration round-trips"
        );
        // The bool is bucket presence, not change: an identical re-put is
        // Ok, and so is a delete of a bucket that stores nothing.
        storage.put_bucket_lifecycle(&b, &config).await.unwrap();
        storage.delete_bucket_lifecycle(&b).await.unwrap();
        assert_eq!(storage.get_bucket_lifecycle(&b).await.unwrap(), None);
        storage.delete_bucket_lifecycle(&b).await.unwrap();

        // The row's other elements survive the lifecycle writes.
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(head.creation_time <= SystemTime::now());

        // All three need a bucket: the CORS shape, not the delete-tagging
        // idempotent Ok.
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage
            .get_bucket_lifecycle(&ghost)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage
            .put_bucket_lifecycle(&ghost, &config)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage
            .delete_bucket_lifecycle(&ghost)
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
    }

    /// A directory planted before the API ever sighted it is still a bucket
    /// (`BucketRow::Lazy`), so the lifecycle write must not answer
    /// `NoSuchBucket` for it: the write records the first-sight row and the
    /// configuration together, and the clear is idempotent on the row-less
    /// state.
    #[tokio::test]
    async fn lifecycle_works_on_a_planted_bucket() {
        let (root, storage) = storage();
        let b = bucket::name("planted").unwrap();
        fs::create_dir(root.path().join("planted")).unwrap();
        let config = lifecycle_config("one");

        assert_eq!(storage.get_bucket_lifecycle(&b).await.unwrap(), None);
        storage.put_bucket_lifecycle(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            Some(config),
            "the planted bucket takes its configuration"
        );
        storage.delete_bucket_lifecycle(&b).await.unwrap();
        assert_eq!(storage.get_bucket_lifecycle(&b).await.unwrap(), None);
        storage
            .delete_bucket_lifecycle(&b)
            .await
            .expect("a clear with nothing stored is idempotent on a lazy store");
    }

    /// FR-023 with LC18: a read-only run refuses both lifecycle writes, and the
    /// refusal keeps the store's read-only marker through this crate's own
    /// conversion — the plane's admin hands `storage::Error` to the router,
    /// which answers the contract's `403` for that marker and nothing else.
    #[tokio::test]
    async fn a_read_only_run_refuses_a_lifecycle_write() {
        use crate::{_core::mode::AccessMode, FsOptions, testutil::fs_options};

        let root = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        let config = lifecycle_config("one");
        {
            let storage = FsStorage::new(root.path(), fs_options()).unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage.put_bucket_lifecycle(&b, &config).await.unwrap();
        }

        // The same root reopened read-only: the stored configuration is
        // served, which is the half of LC18 that is not a refusal.
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                ..fs_options()
            },
        )
        .unwrap();
        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            Some(config.clone()),
            "a read-only run serves the stored configuration"
        );

        let refused = [
            storage.put_bucket_lifecycle(&b, &config).await,
            storage.delete_bucket_lifecycle(&b).await,
        ];
        for outcome in refused {
            let err: StorageError = outcome.unwrap_err().into();
            assert!(
                err.is_read_only(),
                "the store's read-only refusal must keep its marker across the conversion: {err:?}"
            );
        }
        assert_eq!(
            storage.get_bucket_lifecycle(&b).await.unwrap(),
            Some(config),
            "nothing was written: the configuration the refusal would have replaced still stands"
        );
    }

    #[tokio::test]
    async fn fs_bucket_acl_round_trip_preserves_owner() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        let owner = owner_id();
        let grants = vec![all_users_read_grant()];
        storage
            .create_bucket(&b, Some(&owner), &Acl::default_private(Some(owner.clone())))
            .await
            .unwrap();
        // The create-time ACL round-trips (owner + the owner's own
        // FULL_CONTROL).
        let created = storage.get_bucket_acl(&b).await.unwrap();
        assert_eq!(created.owner.as_ref(), Some(&owner));
        assert_eq!(created, Acl::default_private(Some(owner.clone())));
        // A put replaces the grant set; the owner element is preserved,
        // never changed by a put.
        storage.put_bucket_acl(&b, &grants).await.unwrap();
        let got = storage.get_bucket_acl(&b).await.unwrap();
        assert_eq!(got.owner.as_ref(), Some(&owner));
        assert_eq!(got.grants, grants);
        // head_bucket still reports the creation time (the row's other
        // elements survive the ACL write).
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(
            head.creation_time <= SystemTime::now(),
            "the creation time must survive the ACL put"
        );
    }

    #[tokio::test]
    async fn put_bucket_acl_waits_for_the_bucket_mutation_lock() {
        // This pins that the ACL path really sits inside the per-bucket lock:
        // a held guard parks it. The section's reason is the other
        // guard-takers — the delete teardown and the sibling element writes
        // (the C1 review pinned the section; the create's own record write is
        // outside the guard now — RF2/RF4's residual — so this test pins the
        // ACL path's guard, never a defence against a concurrent create).
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_put = storage.clone();
        let bp = b.clone();
        let mut put = tokio::spawn(async move {
            storage_put
                .put_bucket_acl(&bp, &vec![all_users_read_grant()])
                .await
        });
        wait_for_lock_waiter().await;
        assert!(
            timeout(Duration::from_millis(50), &mut put).await.is_err(),
            "the ACL write must wait for the bucket's mutation lock"
        );
        drop(held);
        put.await.unwrap().unwrap();
        assert_eq!(
            storage.get_bucket_acl(&b).await.unwrap().grants,
            vec![all_users_read_grant()]
        );
    }

    #[tokio::test]
    async fn put_bucket_tags_waits_for_the_bucket_mutation_lock() {
        // This pins that the tags path sits inside the per-bucket lock: a held
        // guard parks it. The lock serializes it against the other
        // guard-takers; it does **not** reach a concurrent `create_bucket`,
        // whose create-time row write is a full-row upsert running outside the
        // guard (RF2/RF4's residual), so the create can land after this write
        // and leave the bucket untagged — the C1 lost update, recorded rather
        // than denied.
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_put = storage.clone();
        let bp = b.clone();
        let put_tags = tags.clone();
        let mut put =
            tokio::spawn(async move { storage_put.put_bucket_tags(&bp, &put_tags).await });
        wait_for_lock_waiter().await;
        assert!(
            timeout(Duration::from_millis(50), &mut put).await.is_err(),
            "the tags write must wait for the bucket's mutation lock"
        );
        drop(held);
        put.await.unwrap().unwrap();
        assert_eq!(storage.get_bucket_tags(&b).await.unwrap(), tags);
    }

    #[tokio::test]
    async fn delete_bucket_tags_waits_for_the_bucket_mutation_lock() {
        // This pins that the clear sits inside the per-bucket lock: a held
        // guard parks it. It is serialized against the other guard-takers, not
        // against a concurrent create's outside-guard record write
        // (RF2/RF4's residual).
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        storage.put_bucket_tags(&b, &tags).await.unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_clear = storage.clone();
        let bp = b.clone();
        let mut clear = tokio::spawn(async move { storage_clear.delete_bucket_tags(&bp).await });
        wait_for_lock_waiter().await;
        assert!(
            timeout(Duration::from_millis(50), &mut clear)
                .await
                .is_err(),
            "the tag clear must wait for the bucket's mutation lock"
        );
        drop(held);
        clear.await.unwrap().unwrap();
        assert_eq!(
            storage.get_bucket_tags(&b).await.unwrap(),
            object::Tags::empty()
        );
    }

    #[tokio::test]
    async fn delete_bucket_cors_waits_for_the_bucket_mutation_lock() {
        // This pins that the CORS clear sits inside the per-bucket lock: a held
        // guard parks it. It is serialized against the other guard-takers, not
        // against a concurrent create's outside-guard record write
        // (RF2/RF4's residual).
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let config = cors::Config {
            rules: vec![cors::Rule {
                id: Some("one".into()),
                allowed_methods: vec!["GET".into()],
                allowed_origins: vec!["*".into()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        storage.put_bucket_cors(&b, &config).await.unwrap();
        let held = storage.lock_bucket_mutations(&b).await;
        let storage_clear = storage.clone();
        let bp = b.clone();
        let mut clear = tokio::spawn(async move { storage_clear.delete_bucket_cors(&bp).await });
        wait_for_lock_waiter().await;
        assert!(
            timeout(Duration::from_millis(50), &mut clear)
                .await
                .is_err(),
            "the CORS clear must wait for the bucket's mutation lock"
        );
        drop(held);
        clear.await.unwrap().unwrap();
        assert!(storage.get_bucket_cors(&b).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fs_bucket_acl_missing_bucket_no_such_bucket() {
        let (_root, storage) = storage();
        let ghost = bucket::name("ghost").unwrap();
        let err: StorageError = storage.get_bucket_acl(&ghost).await.unwrap_err().into();
        assert!(matches!(err, NoSuchBucket(_)));
        let err: StorageError = storage
            .put_bucket_acl(&ghost, &Vec::new())
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, NoSuchBucket(_)));
    }

    #[tokio::test]
    async fn fs_list_buckets_filters_by_owner_during_the_walk() {
        let (_root, storage) = storage();
        let owner_a = owner_id();
        let owner_b = other_owner_id();
        for (name, owner) in [
            ("alpha-a", &owner_a),
            ("alpha-b", &owner_a),
            ("beta-b", &owner_b),
        ] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    Some(owner),
                    &Acl::default_private(Some(owner.clone())),
                )
                .await
                .unwrap();
        }
        async fn list_filtered(
            storage: &FsStorage,
            owner: &acl::OwnerId,
            start_after: Option<String>,
            max: usize,
        ) -> BucketsListing {
            storage
                .list_buckets(
                    ListBucketsParams {
                        prefix: String::new(),
                        start_after,
                        max_buckets: max,
                    },
                    Some(owner),
                    None,
                )
                .await
                .unwrap()
        }
        // The filtered listing holds exactly A's buckets.
        let full = list_filtered(&storage, &owner_a, None, 1000).await;
        let names: Vec<&str> = full
            .buckets
            .iter()
            .map(|x| x.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["alpha-a", "alpha-b"], "{names:?}");
        // The continuation-token math applies to the FILTERED set: a
        // page of 1 over the filtered walk yields exactly A's two
        // buckets across two pages - B's bucket never leaks in.
        let page1 = list_filtered(&storage, &owner_a, None, 1).await;
        assert_eq!(page1.buckets.len(), 1);
        assert!(page1.truncated);
        let resume = page1.next_start_after.clone();
        assert!(
            resume.is_some(),
            "a truncated page must carry a resume marker"
        );
        let page2 = list_filtered(&storage, &owner_a, resume, 1).await;
        assert_eq!(page2.buckets.len(), 1);
        assert!(!page2.truncated);
        assert_eq!(page2.buckets[0].name.as_ref(), "alpha-b");
        // The lazy-default leg (contract amendment): a legacy row (no
        // recorded owner element) matches the `lazy_default` principal
        // and nobody else — the expected-owner comparison is on the
        // EFFECTIVE owner.
        storage
            .create_bucket(
                &bucket::name("legacy").unwrap(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let lazy = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                Some(&owner_b),
                Some(&owner_b),
            )
            .await
            .unwrap();
        let names: Vec<&str> = lazy
            .buckets
            .iter()
            .map(|x| x.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["beta-b", "legacy"], "{names:?}");
        // The strict compare (`lazy_default = None`) excludes the
        // legacy row even for the lazy-default principal.
        let strict = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                Some(&owner_b),
                None,
            )
            .await
            .unwrap();
        assert_eq!(strict.buckets.len(), 1, "{:?}", strict.buckets);
        // The legacy listing (`None`, `None`) still shows everything.
        let legacy = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(legacy.buckets.len(), 4);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fs_bucket_dir_mode_0700() {
        use std::os::unix::fs::PermissionsExt;

        use crate::_core::acl::Acl;
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let mode = fs::metadata(root.path().join("data"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "a fresh bucket dir must be 0700");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fs_bucket_dir_chown_failure_removes_dir_and_errors() {
        use std::collections::HashMap;

        use crate::{
            _core::acl::Acl,
            testutil::{euid, owner_id},
        };
        // Review round 1: the bucket-dir chown failure is fail-closed —
        // the create removes the just-created dir and errors (a
        // wrongly-owned 0700 bucket dir must not be left behind with
        // the create reporting success). Runs UNPRIVILEGED (the chown
        // to uid 0 fails with EPERM — the failure the test needs); as
        // root the chown succeeds, so the test skips.
        if euid() == 0 {
            eprintln!("skipped: the fail-closed tests need an UNPRIVILEGED process");
            return;
        }
        use crate::{FsOptions, testutil::fs_options};
        let root = tempfile::tempdir().unwrap();
        let owner = owner_id();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                owner_uids: HashMap::from([(owner.clone(), 0)]),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        let err = storage
            .create_bucket(&b, Some(&owner), &Acl::default_private(Some(owner.clone())))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert!(
            !root.path().join("data").exists(),
            "a wrongly-owned bucket dir must not be left behind"
        );
    }
}
