//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Bucket lifecycle over the `buckets` table; the empty-check and removal of
//! `delete_bucket` are one atomic write transaction (see [`crate::storage`]).

use async_trait::async_trait;
use redb::{ReadableDatabase, ReadableTable};

use tinio_core::{Bucket, BucketOps, bucket, from_nanos, now_nanos};

use crate::{
    Error,
    error::{already_exists, no_such_bucket, not_empty},
    storage::{BUCKETS, MemoryStorage, OBJECTS, UPLOADS},
};

#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            let mut buckets = txn.open_table(BUCKETS)?;
            if buckets.get(name.as_ref().as_str())?.is_some() {
                return Err(already_exists(name));
            }
            buckets.insert(name.as_ref().as_str(), now_nanos())?;
        }
        txn.commit()?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            // The empty-check and the removal are one atomic write
            // transaction (redb serializes writers), so a concurrent
            // put_object can never slip an object in between.
            let objects = txn.open_table(OBJECTS)?;
            let prefix = format!("{}\0", name.as_ref().as_str());
            let mut range = objects.range(prefix.as_str()..)?;
            if let Some(entry) = range.next() {
                let (k, _) = entry?;
                if k.value().starts_with(&prefix) {
                    return Err(not_empty(name));
                }
            }
            // In-progress multipart uploads are bucket-level state — S3
            // answers BucketNotEmpty for them too. The compound UPLOADS key
            // (`bucket\0...`) makes this a bounded range probe; the first
            // key must belong to THIS bucket (`\0` sorts below every
            // bucket-name character, so the probe's first entry can be a
            // later bucket's upload — the OBJECTS probe above guards, the
            // UPLOADS probe must too).
            let uploads = txn.open_table(UPLOADS)?;
            let upload_prefix = format!("{}\0", name.as_ref().as_str());
            let mut uploads_range = uploads.range(upload_prefix.as_str()..)?;
            if let Some(entry) = uploads_range.next() {
                let (k, _) = entry?;
                if k.value().starts_with(&upload_prefix) {
                    return Err(not_empty(name));
                }
            }
            let mut buckets = txn.open_table(BUCKETS)?;
            if buckets.remove(name.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(name));
            }
        }
        txn.commit()?;
        Ok(())
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        buckets
            .get(name.as_ref().as_str())?
            .map(|g| Bucket {
                name: name.clone(),
                creation_time: from_nanos(g.value()),
            })
            .ok_or_else(|| no_such_bucket(name))
    }

    async fn list_buckets(&self) -> Result<Vec<Bucket>, Error> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        let out: Vec<Bucket> = buckets
            .iter()?
            .map(|entry| {
                let (name, created) = entry?;
                Ok(Bucket {
                    name: name.value().into(),
                    creation_time: from_nanos(created.value()),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // BUCKETS is keyed by name, so iteration is already name order.
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use tinio_core::{MultipartOps, ObjectOps, bucket, object, storage::Error::*};
    use tinio_util::testing::body;

    use super::*;

    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for name in ["zeta", "alpha", "mu-1"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let names: Vec<_> = storage
            .list_buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, ["alpha", "mu-1", "zeta"]);
    }

    #[tokio::test]
    async fn delete_empty_bucket_succeeds_when_a_later_bucket_has_objects() {
        let storage = MemoryStorage::new().unwrap();
        let alpha = bucket::name("alpha").unwrap();
        let zeta = bucket::name("zeta").unwrap();
        storage.create_bucket(&alpha).await.unwrap();
        storage.create_bucket(&zeta).await.unwrap();
        storage
            .put_object(&zeta, &object::key("a.txt").unwrap(), body(b"x".to_vec()))
            .await
            .unwrap();
        storage.delete_bucket(&alpha).await.unwrap();
        assert!(matches!(
            storage.head_bucket(&alpha).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        storage.head_bucket(&zeta).await.unwrap();
    }

    #[tokio::test]
    async fn delete_bucket_with_in_progress_uploads_is_not_empty() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("data").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("pending.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key)
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"part".to_vec()),
            )
            .await
            .unwrap();
        assert!(matches!(
            storage.delete_bucket(&bucket).await.unwrap_err(),
            Error::Storage(NotEmpty(_))
        ));
        // The upload stays intact and usable after the failed delete.
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[tinio_core::CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(completed.size, 4);
        // A bucket with an upload but no parts yet is also not empty.
        let idle = storage
            .create_multipart_upload(&bucket, &object::key("idle.bin").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            storage.delete_bucket(&bucket).await.unwrap_err(),
            Error::Storage(NotEmpty(_))
        ));
        storage
            .abort_multipart_upload(&bucket, &object::key("idle.bin").unwrap(), &idle.upload_id)
            .await
            .unwrap();
        storage.delete_object(&bucket, &key).await.unwrap();
        storage.delete_bucket(&bucket).await.unwrap();
    }
}
