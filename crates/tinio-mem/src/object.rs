//! The `ObjectOps` implementation for [`MemoryStorage`].
//!
//! Object put/get/head/delete/listing over the `objects` + `object_meta`
//! tables. Reads use read transactions with zero-copy `&str` / `&[u8]`
//! access; bodies are copied out before the transaction ends (streams are
//! `'static` and cannot borrow the transaction guard).

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::iter;
use redb::{ReadableDatabase, ReadableTable};

use tinio_core::{
    BodyStream, ByteRange, ETag, GetObjectResult, ListObjectsParams, ObjectListing, ObjectOps,
    PutObjectResult, bucket, collect_body, from_nanos, group_and_paginate, now_nanos, object,
};

use crate::{
    Error,
    error::{access_denied, database_storage, no_such_bucket, no_such_key},
    storage::{BUCKETS, MemoryStorage, OBJECT_META, OBJECTS, object_key},
};

#[async_trait]
impl ObjectOps for MemoryStorage {
    async fn put_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
    ) -> Result<PutObjectResult, Error> {
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // Fast-fail on a missing bucket before buffering the body (the write
        // transaction re-checks, closing the race).
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        // Stream the body before opening the transaction (the body future
        // cannot borrow the transaction guard).
        let data = collect_body(body).await?;
        let etag = ETag::from_content(&data);
        let txn = self.db.begin_write()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let etag_str = etag.as_str();
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            let mut objects = txn.open_table(OBJECTS)?;
            let mut meta = txn.open_table(OBJECT_META)?;
            objects.insert(ok.as_str(), data.as_slice())?;
            meta.insert(
                ok.as_str(),
                (etag_str.as_str(), data.len() as u64, now_nanos()),
            )?;
        }
        txn.commit()?;
        Ok(PutObjectResult { etag })
    }

    async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_reserved() || key.is_folder_marker() {
            return Err(no_such_key(key));
        }
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let objects = txn.open_table(OBJECTS)?;
        let meta = txn.open_table(OBJECT_META)?;
        let meta_guard = meta.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let (etag_str, size, mtime) = meta_guard.value();
        let etag: ETag = etag_str.parse()?;
        let served_range = match range {
            None => None,
            Some(r) => Some(r.resolve(size)?),
        };
        // The served slice is copied straight out of the zero-copy redb
        // guard — a range request never copies the full object.
        let data_guard = objects.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let served = match served_range {
            Some((start, end)) => data_guard.value()[start as usize..=end as usize].to_vec(),
            None => data_guard.value().to_vec(),
        };
        let body: BodyStream = Box::pin(iter(vec![Ok(Bytes::from(served))]));
        Ok(GetObjectResult {
            info: object::Info {
                key: key.clone(),
                size,
                last_modified: from_nanos(mtime),
                etag,
            },
            body,
            served_range,
        })
    }

    async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
        }
        if key.is_folder_marker() || key.is_reserved() {
            return Err(no_such_key(key));
        }
        let meta = txn.open_table(OBJECT_META)?;
        let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
        let meta_guard = meta.get(ok.as_str())?.ok_or_else(|| no_such_key(key))?;
        let (etag_str, size, mtime) = meta_guard.value();
        let etag: ETag = etag_str.parse()?;
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(mtime),
            etag,
        })
    }

    async fn delete_object(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(bucket));
            }
            let ok = object_key(bucket.as_ref().as_str(), key.as_ref().as_str());
            let mut objects = txn.open_table(OBJECTS)?;
            let mut meta = txn.open_table(OBJECT_META)?;
            objects.remove(ok.as_str())?;
            meta.remove(ok.as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    async fn list_objects(&self, params: ListObjectsParams) -> Result<ObjectListing, Error> {
        let txn = self.db.begin_read()?;
        {
            let buckets = txn.open_table(BUCKETS)?;
            if buckets.get(params.bucket.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(&params.bucket));
            }
        }
        let meta = txn.open_table(OBJECT_META)?;
        let scan_prefix = object_key(params.bucket.as_ref().as_str(), &params.prefix);
        let bucket_prefix = format!("{}\0", params.bucket.as_ref().as_str());
        let objects: Vec<object::Info> = meta
            .range(scan_prefix.as_str()..)?
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(k, _)| k.value().starts_with(&scan_prefix))
                    .unwrap_or(false)
            })
            .filter_map(|entry| match entry {
                Ok((k, v)) => {
                    let key = k.value()[bucket_prefix.len()..].to_string();
                    let (etag, size, mtime) = v.value();
                    let key = object::key(key).expect("stored object key is validated");
                    if key.is_folder_marker() || key.is_reserved() {
                        return None;
                    }
                    let etag = etag.parse().expect("stored etag is validated");
                    Some(Ok(object::Info {
                        key,
                        size,
                        last_modified: from_nanos(mtime),
                        etag,
                    }))
                }
                Err(e) => Some(Err(database_storage(e))),
            })
            .collect::<Result<Vec<_>, Error>>()?;
        // The scan is in `bucket\0key` order, so results are already
        // lexicographic — no sort needed before grouping/pagination.

        let (keys, common_prefixes, truncated, next_start_after) = group_and_paginate(
            objects,
            &params.prefix,
            params.delimiter.as_deref(),
            params.start_after.as_deref(),
            params.max_keys,
            |o| o.key.as_ref(),
        );
        Ok(ObjectListing {
            objects: keys,
            common_prefixes,
            truncated,
            next_start_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream::iter;
    use tinio_core::{
        BodyStream, BucketOps, ByteRange, ETag, ObjectOps, bucket, object,
        storage::Error::*,
        testing::{body, read_body},
    };

    use super::*;

    async fn with_bucket() -> (MemoryStorage, bucket::Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage.create_bucket(&name).await.unwrap();
        (storage, name)
    }

    fn chunked(parts: &[&[u8]]) -> BodyStream {
        let owned: Vec<_> = parts
            .iter()
            .map(|p| Ok(Bytes::from((*p).to_vec())))
            .collect();
        Box::pin(iter(owned))
    }

    #[tokio::test]
    async fn object_ops_on_missing_bucket_are_no_such_bucket() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("gone").unwrap();
        let key = object::key("a.txt").unwrap();
        assert!(matches!(
            storage
                .put_object(&bucket, &key, body(b"x".to_vec()))
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.get_object(&bucket, &key, None).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.head_object(&bucket, &key).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage.delete_object(&bucket, &key).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        assert!(matches!(
            storage
                .list_objects(tinio_core::ListObjectsParams {
                    bucket: bucket.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    start_after: None,
                    max_keys: 1000,
                })
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn put_overwrites_existing_object() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.txt").unwrap();
        storage
            .put_object(&bucket, &key, body(b"old".to_vec()))
            .await
            .unwrap();
        let put = storage
            .put_object(&bucket, &key, body(b"new-bytes".to_vec()))
            .await
            .unwrap();
        assert_eq!(put.etag, ETag::from_content(b"new-bytes"));
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"new-bytes");
        assert_eq!(got.info.size, 9);
    }

    #[tokio::test]
    async fn put_concatenates_body_chunks() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("chunked").unwrap();
        storage
            .put_object(&bucket, &key, chunked(&[b"hel", b"lo", b"", b"!"]))
            .await
            .unwrap();
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), b"hello!");
    }

    #[tokio::test]
    async fn get_empty_object_returns_empty_body() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("empty").unwrap();
        storage
            .put_object(&bucket, &key, body(b"".to_vec()))
            .await
            .unwrap();
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert!(got.served_range.is_none());
        assert_eq!(got.info.size, 0);
        assert!(read_body(got.body).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_missing_key_is_no_such_key() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("missing").unwrap();
        assert!(matches!(
            storage.get_object(&bucket, &key, None).await.unwrap_err(),
            Error::Storage(NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn get_clamps_inclusive_range_to_object_size() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("digits").unwrap();
        storage
            .put_object(&bucket, &key, body(b"0123456789".to_vec()))
            .await
            .unwrap();
        let got = storage
            .get_object(&bucket, &key, Some(ByteRange::Inclusive(8, 99)))
            .await
            .unwrap();
        assert_eq!(got.served_range, Some((8, 9)));
        assert_eq!(read_body(got.body).await.unwrap(), b"89");
    }

    #[tokio::test]
    async fn get_suffix_larger_than_object_returns_all() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("digits").unwrap();
        storage
            .put_object(&bucket, &key, body(b"0123456789".to_vec()))
            .await
            .unwrap();
        let got = storage
            .get_object(&bucket, &key, Some(ByteRange::Suffix(100)))
            .await
            .unwrap();
        assert_eq!(got.served_range, Some((0, 9)));
        assert_eq!(read_body(got.body).await.unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn unsatisfiable_ranges_are_invalid_range() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("digits").unwrap();
        storage
            .put_object(&bucket, &key, body(b"0123456789".to_vec()))
            .await
            .unwrap();
        for range in [
            ByteRange::From(10),
            ByteRange::Inclusive(10, 20),
            ByteRange::Suffix(0),
        ] {
            assert!(
                matches!(
                    storage
                        .get_object(&bucket, &key, Some(range))
                        .await
                        .unwrap_err(),
                    Error::Storage(InvalidRange { .. })
                ),
                "{range:?}"
            );
        }
    }

    #[tokio::test]
    async fn head_folder_marker_and_reserved_are_no_such_key() {
        let (storage, bucket) = with_bucket().await;
        let marker = object::key("dir/").unwrap();
        storage
            .put_object(&bucket, &marker, body(b"".to_vec()))
            .await
            .unwrap();
        assert!(matches!(
            storage.head_object(&bucket, &marker).await.unwrap_err(),
            Error::Storage(NoSuchKey(_))
        ));
        let reserved = object::key("a/.tinio/b").unwrap();
        assert!(matches!(
            storage.head_object(&bucket, &reserved).await.unwrap_err(),
            Error::Storage(NoSuchKey(_))
        ));
    }

    #[tokio::test]
    async fn list_objects_skips_folder_markers() {
        let (storage, bucket) = with_bucket().await;
        storage
            .put_object(&bucket, &object::key("dir/").unwrap(), body(b"".to_vec()))
            .await
            .unwrap();
        storage
            .put_object(
                &bucket,
                &object::key("dir/a.txt").unwrap(),
                body(b"a".to_vec()),
            )
            .await
            .unwrap();
        let page = storage
            .list_objects(tinio_core::ListObjectsParams {
                bucket: bucket.clone(),
                prefix: String::new(),
                delimiter: None,
                start_after: None,
                max_keys: 1000,
            })
            .await
            .unwrap();
        let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_ref()).collect();
        assert_eq!(keys, ["dir/a.txt"]);
    }
}
