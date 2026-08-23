//! Bucket operations of the fs backend (task T041).
//!
//! Buckets are top-level directories of the storage root. Creation times
//! come from `buckets.json`, lazily recorded on first sight of a
//! pre-existing directory. Names are re-validated on create (defensive
//! backstop, FR-012); the reserved `.tinio/` directory is never a bucket.

use std::{collections::HashMap, io, time::SystemTime};

use async_trait::async_trait;
use tinio_core::{
    bucket::{self, Bucket},
    storage::{BucketOps, already_exists, invalid_bucket_name, no_such_bucket, not_empty},
};

use crate::path::bucket_path;

use super::{Error, FsStorage};

/// A bucket counts as empty when it has no files and no directories
/// anywhere (folder-marker directories are content, per the conformance
/// harness) and no in-progress multipart uploads.
async fn bucket_is_empty(storage: &FsStorage, name: &bucket::Name) -> Result<bool, Error> {
    if storage.multipart_store().has_uploads(name).await? {
        return Ok(false);
    }
    let bucket_dir = storage.bucket_dir(name)?;
    let mut entries = match tokio::fs::read_dir(&bucket_dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            return Err(Error::Storage(no_such_bucket(name)));
        }
        Err(err) => return Err(err.into()),
    };
    if entries.next_entry().await?.is_some() {
        return Ok(false); // any file or directory is content
    }
    Ok(true)
}

#[async_trait]
impl BucketOps for FsStorage {
    async fn create_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        // Defensive re-validation (FR-012) before any FS access; the
        // checked constructor is authoritative. `bucket_path` rejects the
        // reserved `.tinio` collision (FR-020).
        if bucket_path(self.root(), name).is_err() {
            return Err(invalid_bucket_name(name.to_string()).into());
        }
        // The directory creation and its `buckets.json` record are one
        // critical section against `delete_bucket` (a delete must never
        // remove a bucket a create just reported as created).
        let _guard = self.bucket_mutation_lock.lock().await;
        let dir = bucket_path(self.root(), name)?;
        match tokio::fs::create_dir(&dir).await {
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
        tokio::fs::remove_dir_all(&dir).await?;
        // Lazy cleanup of the private state (data-model.md).
        let _ = self.bucket_store.remove(name).await;
        let _ = self.meta_store.remove_bucket(name).await;
        let _ = self.multipart_store.remove_bucket(name).await;
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
    use crate::testutil::rt;
    use std::fs;
    use tinio_core::storage::{Error as StorageError, Error::*, MultipartOps, ObjectOps};
    use tinio_core::testing::{assert_conformance, body};

    fn storage() -> (tempfile::TempDir, FsStorage) {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), Default::default()).unwrap();
        (root, storage)
    }

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
            fs::create_dir(root.path().join(".tinio")).unwrap();
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
