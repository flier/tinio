//! Bucket operations of the fs backend (task T041).
//!
//! Buckets are top-level directories of the storage root. Creation times
//! come from the `BUCKETS` table, lazily recorded on first sight of a
//! pre-existing directory. Names are re-validated on create (defensive
//! backstop, FR-012); the reserved `.tinio/` directory is never a bucket.

use std::{io::ErrorKind, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use tokio::fs;

use super::{Error, FsStorage};
use crate::{
    _core::{
        BucketsListing, ListBucketsParams, acl,
        bucket::{self, Bucket},
        cors, object,
        storage::{BucketOps, UnorderedPager, already_exists, no_such_bucket, not_empty},
    },
    path::{STATE_DIR_NAME, bucket_path_lexical},
    tombstone,
};

/// A bucket counts as empty when it has no files and no directories
/// anywhere (folder-marker directories are content, per the conformance
/// harness), no in-progress multipart uploads, and no `.tinio` staging
/// residue (FR-020 — the reserved segment is never served or listed, so
/// a crashed/failed cross-volume commit's staging dir must not make the
/// bucket undeletable; the startup repair clears it).
async fn bucket_is_empty(storage: &FsStorage, name: &bucket::Name) -> Result<bool, Error> {
    if storage.multipart_store().has_uploads(name).await? {
        return Ok(false);
    }
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

#[async_trait]
impl BucketOps for FsStorage {
    async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &acl::Acl,
    ) -> Result<(), Error> {
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
        // The directory creation and its `BUCKETS` record are one
        // critical section against `delete_bucket` (a delete must never
        // remove a bucket a create just reported as created).
        let _guard = self.lock_bucket_mutations(name).await;
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
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(err) = fs::set_permissions(
                &dir,
                std::fs::Permissions::from_mode(crate::fsutil::DIR_MODE_PRIVATE),
            )
            .await
            {
                let _ = fs::remove_dir(&dir).await;
                return Err(err.into());
            }
            if let Some(uid) = self.owner_uid(owner) {
                if let Err(err) = crate::fsutil::chown_file(&dir, uid).await {
                    let _ = fs::remove_dir(&dir).await;
                    return Err(err.into());
                }
            }
        }
        self.bucket_store
            .record_full(name, SystemTime::now(), owner, acl)
            .await?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // The emptiness check and the unpublish rename are one critical
        // section against every write into this bucket: a concurrent
        // PUT's rename cannot land in a directory about to disappear
        // with 200 returned. Other buckets are not serialized here.
        let dest = {
            let _guard = self.lock_bucket_mutations(name).await;
            self.ensure_bucket(name).await?;
            if !bucket_is_empty(self, name).await? {
                return Err(not_empty(name).into());
            }
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
            dest
        };
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

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        self.ensure_bucket(name).await?;
        let creation_time = self
            .bucket_store
            .get_or_record(name, SystemTime::now())
            .await?;
        Ok(Bucket {
            name: name.clone(),
            creation_time,
        })
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // The bounded pagination engine (`UnorderedPager`, the
        // uploads-page engine) consumes the root sweep incrementally:
        // the streaming walk offers only prefix-matching, name-valid
        // candidates, and the engine keeps only the page — a max-heap of
        // `max + 1` entries — so neither the full collection nor the
        // O(N log N) sort of the old `bucket_names` sweep survives
        // (memory is O(page), never O(matches)). `max = 0` still
        // short-circuits the root sweep — no dirent scan, no stats, no
        // metadata reads (the cost-profile parity with the mem backend,
        // which never drains the table for an empty page); the engine
        // produces the empty page itself, so the contract has one home.
        //
        // The `owner` filter applies DURING the walk, before pagination
        // (contract review P2#4 — the continuation-token math applies to
        // the filtered set): the candidate names collect first (the
        // filtered walk cannot stream into the pager — each name's row
        // must be read before the offer), then one read transaction
        // resolves every candidate's owner element, and only the
        // principal's buckets reach the pager. `None` = no filter, the
        // legacy listing, with the streaming walk unchanged.
        let max = params.max_buckets;
        let mut pager = UnorderedPager::new(
            &params.prefix,
            None,
            params.start_after.as_deref(),
            max,
            |name: &bucket::Name| name.as_ref().as_str(),
        );
        if max > 0 {
            match owner {
                Some(owner) => {
                    let mut names = Vec::new();
                    self.for_each_bucket_name(&params.prefix, |name| names.push(name))
                        .await?;
                    let recorded = self.bucket_store.owners(&names).await?;
                    for (name, recorded_owner) in names.into_iter().zip(recorded) {
                        if recorded_owner.as_ref() == Some(owner) {
                            pager.offer_keyed(name);
                        }
                    }
                }
                None => {
                    self.for_each_bucket_name(&params.prefix, |name| pager.offer_keyed(name))
                        .await?;
                }
            }
        }
        let (page, _prefixes, truncated, next) = pager.finish();
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
        // Existence is the bucket directory (`NoSuchBucket` when missing
        // — mirroring head_bucket); the tags come from the `BUCKETS`
        // row, empty when the bucket has no row yet (a pre-existing
        // bucket has never been tagged through the API).
        self.ensure_bucket(name).await?;
        self.bucket_store.tags(name).await
    }

    async fn put_bucket_tags(&self, name: &bucket::Name, tags: &object::Tags) -> Result<(), Error> {
        self.ensure_bucket(name).await?;
        self.bucket_store.set_tags(name, tags).await
    }

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete-object/bucket leniency). The row's creation
        // time is preserved; a row-less bucket has nothing to clear.
        self.bucket_store.clear_tags(name).await
    }

    async fn get_bucket_cors(&self, name: &bucket::Name) -> Result<Option<cors::Config>, Error> {
        // Existence is the bucket directory (`NoSuchBucket` when missing —
        // mirroring `get_bucket_tags`); the configuration comes from the
        // `BUCKETS` row, None when the bucket has no configuration (`''`
        // wire — a bucket that was cleared or never configured).
        self.ensure_bucket(name).await?;
        self.bucket_store.cors(name).await
    }

    async fn put_bucket_cors(&self, name: &bucket::Name, cors: &cors::Config) -> Result<(), Error> {
        self.ensure_bucket(name).await?;
        self.bucket_store.set_cors(name, cors).await
    }

    async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), Error> {
        // S3 semantics: `NoSuchBucket` when the bucket is missing — the
        // delete-tagging idempotent leniency does NOT extend here (the
        // CORS spec mandates `NoSuchBucket` for a missing bucket); a
        // bucket that simply has no configuration clears to the same
        // state (idempotent). The row's creation time is preserved.
        self.ensure_bucket(name).await?;
        self.bucket_store.clear_cors(name).await
    }

    async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<acl::Acl, Error> {
        // Existence is the bucket directory (`NoSuchBucket` when missing
        // — mirroring head_bucket); the ACL comes from the `BUCKETS`
        // row, which answers `NoSuchBucket` itself when unrecorded (a
        // legacy bucket has no ACL row until first sight).
        self.ensure_bucket(name).await?;
        self.bucket_store.acl(name).await
    }

    async fn put_bucket_acl(&self, name: &bucket::Name, grants: &acl::AclGrants) -> Result<(), Error> {
        self.ensure_bucket(name).await?;
        self.bucket_store.set_acl(name, grants).await
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
            object,
            storage::{
                BucketsListing, Error as StorageError, Error::*, ListBucketsParams, MultipartOps,
                ObjectOps,
            },
        },
        _util::testing::{assert_conformance, body, etag},
        testutil::{storage, wait_for, wait_for_lock_waiter},
        tombstone,
    };

    #[tokio::test]
    async fn conformance_green() {
        let (_root, storage) = storage();
        assert_conformance(&storage).await;
    }

    #[tokio::test]
    async fn create_head_list_delete() {
        let (root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        assert!(storage.head_bucket(&b).await.is_err());

        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        assert_eq!(storage.head_bucket(&b).await.unwrap().name, b);

        // The bucket is a real directory.
        assert!(root.path().join("my-bucket").is_dir());

        let listing = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
            .await
            .unwrap();
        assert_eq!(listing.buckets.len(), 1);
        assert_eq!(listing.buckets[0].name, b);

        // Duplicate create.
        let err: StorageError = storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap_err().into();
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
        storage.create_bucket(&a, None, &acl::Acl::default_private(None)).await.unwrap();
        let _guard = storage.lock_bucket_mutations(&a).await;
        let storage2 = storage.clone();
        let create_b = tokio::spawn(async move { storage2.create_bucket(&b, None, &acl::Acl::default_private(None)).await });
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
        let create_a = tokio::spawn(async move { storage2.create_bucket(&a, None, &acl::Acl::default_private(None)).await });
        assert!(
            timeout(Duration::from_millis(80), create_a).await.is_err(),
            "create of the same bucket must wait on the mutation lock"
        );
    }

    #[tokio::test]
    async fn delete_bucket_unpublishes_into_deleting_dir() {
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(!root.path().join("data").exists());
        assert!(tombstone::dir(root.path()).is_dir());
        assert!(!state.path().join("deleting").exists());
    }

    #[tokio::test]
    async fn delete_bucket_does_not_split_the_mutation_lock() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
            tokio::spawn(async move { storage_c.create_bucket(&bc, None, &acl::Acl::default_private(None)).await }),
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
                                let _ = storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await;
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
            _core::storage::ListObjectsParams, _util::testing::read_body, FsOptions,
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
        let err: StorageError = storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap_err().into();
        assert!(matches!(err, AlreadyExists(_)), "{err:?}");
        let listing = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
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
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        fs::create_dir_all(root.path().join("data/.tinio")).unwrap();
        fs::write(root.path().join("data/.tinio/aaaa"), b"residue").unwrap();
        storage.delete_bucket(&b).await.unwrap();
        assert!(!root.path().join("data").exists());
    }

    #[tokio::test]
    async fn delete_bucket_with_uploads_is_not_empty() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &"big.bin".into(),
                None,
                object::Tags::empty(),
                None,
                &acl::Acl::default_private(None),
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
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
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
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
            .await
            .unwrap();
        assert!(listing.buckets.is_empty(), "{listing:?}");
    }

    #[tokio::test]
    async fn bucket_delete_prunes_private_state() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
    async fn remove_bucket_state_clears_all_four_tables_atomically() {
        let (_root, storage) = storage();
        let b = bucket::name("my-bucket").unwrap();
        let k = object::key("a.txt").unwrap();
        // Bucket row.
        storage
            .bucket_store()
            .record(&b, SystemTime::now())
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
                &acl::Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .multipart_store()
            .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"), None)
            .await
            .unwrap();

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
    }

    #[tokio::test]
    async fn many_buckets_list_sorted() {
        let (_root, storage) = storage();
        for name in ["zeta", "alpha", "mid"] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    None,
                    &acl::Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let listing = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            },
            None)
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        // Move the root aside: `bucket_names` would fail its `read_dir`.
        let moved = root.path().with_extension("swept");
        fs::rename(root.path(), &moved).unwrap();
        let listing = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 0,
            },
            None)
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
            head.creation_time <= std::time::SystemTime::now(),
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
            .create_bucket(&b, None, &acl::Acl::default_private(None))
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
            head.creation_time <= std::time::SystemTime::now(),
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
    async fn fs_bucket_acl_round_trip_preserves_owner() {
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        let owner = crate::testutil::owner_id();
        let grants = vec![crate::testutil::all_users_read_grant()];
        storage
            .create_bucket(&b, Some(&owner), &acl::Acl::default_private(Some(owner.clone())))
            .await
            .unwrap();
        // The create-time ACL round-trips (owner + the owner's own
        // FULL_CONTROL).
        let created = storage.get_bucket_acl(&b).await.unwrap();
        assert_eq!(created.owner.as_ref(), Some(&owner));
        assert_eq!(created, acl::Acl::default_private(Some(owner.clone())));
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
            head.creation_time <= std::time::SystemTime::now(),
            "the creation time must survive the ACL put"
        );
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
        let owner_a = crate::testutil::owner_id();
        let owner_b = crate::testutil::other_owner_id();
        for (name, owner) in [
            ("alpha-a", &owner_a),
            ("alpha-b", &owner_a),
            ("beta-b", &owner_b),
        ] {
            storage
                .create_bucket(
                    &bucket::name(name).unwrap(),
                    Some(owner),
                    &acl::Acl::default_private(Some(owner.clone())),
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
        assert!(resume.is_some(), "a truncated page must carry a resume marker");
        let page2 = list_filtered(&storage, &owner_a, resume, 1).await;
        assert_eq!(page2.buckets.len(), 1);
        assert!(!page2.truncated);
        assert_eq!(page2.buckets[0].name.as_ref(), "alpha-b");
        // The legacy listing (`None`) still shows everything.
        let legacy = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(legacy.buckets.len(), 3);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fs_bucket_dir_mode_0700() {
        use std::os::unix::fs::PermissionsExt;
        let (root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &acl::Acl::default_private(None))
            .await
            .unwrap();
        let mode = std::fs::metadata(root.path().join("data"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o700, "a fresh bucket dir must be 0700");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fs_bucket_dir_chown_failure_removes_dir_and_errors() {
        // Review round 1: the bucket-dir chown failure is fail-closed —
        // the create removes the just-created dir and errors (a
        // wrongly-owned 0700 bucket dir must not be left behind with
        // the create reporting success). Runs UNPRIVILEGED (the chown
        // to uid 0 fails with EPERM — the failure the test needs); as
        // root the chown succeeds, so the test skips.
        if crate::testutil::euid() == 0 {
            eprintln!("skipped: the fail-closed tests need an UNPRIVILEGED process");
            return;
        }
        use crate::{FsOptions, testutil::fs_options};
        let root = tempfile::tempdir().unwrap();
        let owner = crate::testutil::owner_id();
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                owner_uids: std::collections::HashMap::from([(owner.clone(), 0)]),
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        let err = storage
            .create_bucket(&b, Some(&owner), &acl::Acl::default_private(Some(owner.clone())))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        assert!(
            !root.path().join("data").exists(),
            "a wrongly-owned bucket dir must not be left behind"
        );
    }
}
