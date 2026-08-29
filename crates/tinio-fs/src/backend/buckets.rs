//! Bucket operations of the fs backend (task T041).
//!
//! Buckets are top-level directories of the storage root. Creation times
//! come from the `BUCKETS` table, lazily recorded on first sight of a
//! pre-existing directory. Names are re-validated on create (defensive
//! backstop, FR-012); the reserved `.tinio/` directory is never a bucket.

use std::{collections::HashMap, io, time::SystemTime};

use async_trait::async_trait;
use tinio_core::{
    bucket::{self, Bucket},
    storage::{BucketOps, already_exists, no_such_bucket, not_empty},
};
use tokio::fs;

use super::{Error, FsStorage};
use crate::path::{STATE_DIR_NAME, bucket_path_lexical};

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
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
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
    async fn create_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
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
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        // The directory creation and its `BUCKETS` record are one
        // critical section against `delete_bucket` (a delete must never
        // remove a bucket a create just reported as created).
        let _guard = self.bucket_mutation_lock.lock().await;
        match fs::create_dir(&dir).await {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                return Err(already_exists(name).into());
            }
            Err(err) => return Err(err.into()),
        }
        self.bucket_store.record(name, SystemTime::now()).await?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // The emptiness check and the removal are one critical section
        // against every write into the bucket: a concurrent PUT's rename
        // cannot land between them and be destroyed with 200 returned.
        let _guard = self.bucket_mutation_lock.lock().await;
        let dir = self.ensure_bucket(name).await?;
        if !bucket_is_empty(self, name).await? {
            return Err(not_empty(name).into());
        }
        // Empty by the object walk (no files anywhere) — only empty
        // directories remain, so a recursive remove is safe and handles
        // leftover folder-marker directories.
        fs::remove_dir_all(&dir).await?;
        // Lazy cleanup of the private state (data-model.md) — one write
        // transaction over BUCKETS + OBJECT_META + UPLOADS + PARTS (G2:
        // a bucket's whole derived state dies atomically). The directory
        // is already gone — the delete has succeeded — so a state
        // failure must NOT fail the response (the client would see an
        // error for a delete that happened); the leaked rows are
        // reclaimed by the startup repair (stale bucket records drain
        // the whole derived state).
        if let Err(err) = self.remove_bucket_state(name).await {
            tracing::warn!(error = %err, "bucket state not removed after delete");
        }
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

    async fn list_buckets(&self) -> Result<Vec<Bucket>, Error> {
        let names = self.bucket_names().await?;
        // Load the creation-time file once (not once per bucket): the
        // recorded map, then lazily record first-sight buckets.
        let recorded: HashMap<String, SystemTime> =
            self.bucket_store.load_all().await?.into_iter().collect();
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let creation_time = match recorded.get(name.as_ref()) {
                Some(created) => *created,
                None => {
                    self.bucket_store
                        .get_or_record(&name, SystemTime::now())
                        .await?
                }
            };
            out.push(Bucket {
                name,
                creation_time,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{rt, storage};
    use std::{fs, time::SystemTime};
    use tinio_core::{
        object,
        storage::{Error as StorageError, Error::*, MultipartOps, ObjectOps},
    };
    use tinio_util::testing::{assert_conformance, body, etag};

    #[test]
    fn conformance_green() {
        rt(async {
            let (_root, storage) = storage();
            assert_conformance(&storage).await;
        });
    }

    #[test]
    fn create_head_list_delete() {
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("my-bucket").unwrap();
            assert!(storage.head_bucket(&b).await.is_err());

            storage.create_bucket(&b).await.unwrap();
            assert_eq!(storage.head_bucket(&b).await.unwrap().name, b);

            // The bucket is a real directory.
            assert!(root.path().join("my-bucket").is_dir());

            let buckets = storage.list_buckets().await.unwrap();
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].name, b);

            // Duplicate create.
            let err: StorageError = storage.create_bucket(&b).await.unwrap_err().into();
            assert!(matches!(err, AlreadyExists(_)));

            // Delete.
            storage.delete_bucket(&b).await.unwrap();
            assert!(storage.head_bucket(&b).await.is_err());
            assert!(!root.path().join("my-bucket").exists());
        });
    }

    #[test]
    fn delete_non_empty_is_not_empty() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("my-bucket").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage
                .put_object(&b, &"a.txt".into(), body(b"x"))
                .await
                .unwrap();
            let err: StorageError = storage.delete_bucket(&b).await.unwrap_err().into();
            assert!(matches!(err, NotEmpty(_)));
            // Deleting the object frees the bucket.
            storage.delete_object(&b, &"a.txt".into()).await.unwrap();
            storage.delete_bucket(&b).await.unwrap();
        });
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_bucket_follows_when_enabled_and_invisible_when_disabled() {
        use crate::{FsOptions, testutil::fs_options};
        use tinio_core::storage::ListObjectsParams;
        use tinio_util::testing::read_body;
        rt(async {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();
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
            let err: StorageError = storage.create_bucket(&b).await.unwrap_err().into();
            assert!(matches!(err, AlreadyExists(_)), "{err:?}");
            let buckets = storage.list_buckets().await.unwrap();
            assert!(buckets.is_empty(), "{buckets:?}");

            // follow_symlinks = true: the bucket IS the target — full
            // CRUD through the link, listed and discovered like any
            // other bucket.
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
            let buckets = storage.list_buckets().await.unwrap();
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].name, b);
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
        });
    }

    #[test]
    fn bucket_with_only_staging_residue_is_empty() {
        // A crashed cross-volume commit leaves `<bucket>/.tinio/` — the
        // reserved segment is not content (FR-020), so the bucket stays
        // deletable (the startup repair later reclaims the bytes).
        rt(async {
            let (root, storage) = storage();
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
            fs::create_dir_all(root.path().join("data/.tinio")).unwrap();
            fs::write(root.path().join("data/.tinio/aaaa"), b"residue").unwrap();
            storage.delete_bucket(&b).await.unwrap();
            assert!(!root.path().join("data").exists());
        });
    }

    #[test]
    fn delete_bucket_with_uploads_is_not_empty() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("my-bucket").unwrap();
            storage.create_bucket(&b).await.unwrap();
            let upload = storage
                .create_multipart_upload(&b, &"big.bin".into())
                .await
                .unwrap();
            let err: StorageError = storage.delete_bucket(&b).await.unwrap_err().into();
            assert!(matches!(err, NotEmpty(_)));
            storage
                .abort_multipart_upload(&b, &"big.bin".into(), &upload.upload_id)
                .await
                .unwrap();
            storage.delete_bucket(&b).await.unwrap();
        });
    }

    #[test]
    fn pre_existing_directories_are_buckets_with_lazy_creation_time() {
        rt(async {
            let (root, storage) = storage();
            fs::create_dir(root.path().join("existing")).unwrap();
            let buckets = storage.list_buckets().await.unwrap();
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].name.as_ref(), "existing");
            // The lazy record persists.
            let head = storage
                .head_bucket(&bucket::name("existing").unwrap())
                .await
                .unwrap();
            assert_eq!(head.name.as_ref(), "existing");
        });
    }

    #[test]
    fn tinio_and_root_files_are_not_buckets() {
        rt(async {
            let (root, storage) = storage();
            // The state dir already exists (constructed with the backend);
            // a `.tinio` directory must never surface as a bucket.
            fs::create_dir_all(root.path().join(".tinio")).unwrap();
            fs::write(root.path().join("file.txt"), b"x").unwrap();
            fs::create_dir(root.path().join("Big")).unwrap(); // invalid name
            let buckets = storage.list_buckets().await.unwrap();
            assert!(buckets.is_empty(), "{buckets:?}");
        });
    }

    #[test]
    fn bucket_delete_prunes_private_state() {
        rt(async {
            let (_root, storage) = storage();
            let b = bucket::name("my-bucket").unwrap();
            storage.create_bucket(&b).await.unwrap();
            storage
                .put_object(&b, &"a.txt".into(), body(b"x"))
                .await
                .unwrap();
            storage.delete_object(&b, &"a.txt".into()).await.unwrap();
            storage.delete_bucket(&b).await.unwrap();
            assert!(storage.bucket_store().load_all().await.unwrap().is_empty());
            assert!(storage.meta_store().walk(&b).await.unwrap().is_empty());
        });
    }

    #[test]
    fn remove_bucket_state_clears_all_four_tables_atomically() {
        rt(async {
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
            let upload = storage.multipart_store().create(&b, &k).await.unwrap();
            storage
                .multipart_store()
                .put_part(&b, &k, &upload.upload_id, 1.into(), body(b"x"))
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
        });
    }

    #[test]
    fn many_buckets_list_sorted() {
        rt(async {
            let (_root, storage) = storage();
            for name in ["zeta", "alpha", "mid"] {
                storage
                    .create_bucket(&bucket::name(name).unwrap())
                    .await
                    .unwrap();
            }
            let buckets = storage.list_buckets().await.unwrap();
            let names: Vec<&str> = buckets.iter().map(|b| b.name.as_ref().as_str()).collect();
            assert_eq!(names, ["alpha", "mid", "zeta"]);
        });
    }
}
