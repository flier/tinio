//! The listing path: the content scan, the page assembly and the batch
//! enrichment behind [`ObjectOps::list_objects`](crate::ObjectOps::list_objects).
//!
//! The path is **content-side and enriched in one batch** (spec obligation 6):
//!
//! 1. `ObjectContent::scan(bucket, prefix, after, limit)` enumerates keys in
//!    ascending order — the prefix is a bound the scan must not walk past, so a
//!    prefixed page never rescans the bucket — and the engine feeds what it
//!    yields into [`UnorderedPager`], the shared bounded-k page engine. The
//!    scan stops as soon as one more key cannot change the page: the pager holds
//!    at most `max_keys + 1` entries, and the views it orders by (a key, or a
//!    delimiter rollup) never decrease over an ascending scan, so once more than
//!    `max_keys` distinct entries are in hand the rest of the bucket is beyond
//!    the page.
//! 2. The **emitted page's** keys are enriched through **one**
//!    [`ObjectMeta::load`] call — never a point read per key.
//! 3. A key whose row is absent or stale is **healed** exactly as a read heals
//!    it (the same rule, the same persist, the same read-only degradation), so a
//!    file the store has never recorded still appears with its content ETag.
//!    A key whose content vanished between the scan and the load is skipped —
//!    the content is the truth a listing lists.
//!
//! The scan window is a fixed constant, not a parameter: a backend may cap what
//! one call walks (fs's `scan` window and the scanner's batch accumulator stay
//! fs-side), and the engine simply asks again from the last key it saw.
//!
//! # Examples
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use futures::stream;
//! use tinio_core::{ListObjectsParams, acl::Acl, bucket, object};
//! use tinio_ops::{BucketOps, BucketRow, MetadataFailure, ObjectOps};
//! use tinio_store_redb::{RedbContent, RedbMetadata};
//!
//! # struct NoHooks;
//! # #[async_trait::async_trait]
//! # impl tinio_ops::Hooks for NoHooks {
//! #     async fn before_bucket_delete(
//! #         &self,
//! #         _bucket: &bucket::Name,
//! #     ) -> Result<(), tinio_core::storage::Error> {
//! #         Ok(())
//! #     }
//! #     async fn lock_part(
//! #         &self,
//! #         _bucket: &bucket::Name,
//! #         _upload_id: &str,
//! #     ) -> Box<dyn tinio_ops::Guard> {
//! #         Box::new(tinio_ops::NoGuard)
//! #     }
//! # }
//! # #[tokio::main]
//! # async fn main() {
//! let meta = Arc::new(RedbMetadata::in_memory().unwrap());
//! let content = Arc::new(RedbContent::from_driver(meta.driver()));
//! let buckets = BucketOps {
//!     meta: Arc::clone(&meta),
//!     content: Arc::clone(&content),
//!     hooks: NoHooks,
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//! let objects = ObjectOps {
//!     meta: Arc::clone(&meta),
//!     content,
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//! let name = bucket::name("data").unwrap();
//! buckets
//!     .create_bucket(&name, None, &Acl::default_private(None))
//!     .await
//!     .unwrap();
//! for key in ["a.txt", "dir/x.txt", "e.txt"] {
//!     let body = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"x"))]));
//!     let staged = objects
//!         .stage_body(&name, &object::key(key).unwrap(), body, None, None)
//!         .await
//!         .unwrap();
//!     objects
//!         .commit_object(
//!             &name,
//!             &object::key(key).unwrap(),
//!             staged,
//!             object::Tags::empty(),
//!             None,
//!             &Acl::default_private(None),
//!             None,
//!         )
//!         .await
//!         .unwrap();
//! }
//!
//! let page = objects
//!     .list_objects(
//!         ListObjectsParams {
//!             bucket: name,
//!             prefix: String::new(),
//!             delimiter: Some("/".to_owned()),
//!             start_after: None,
//!             max_keys: 1000,
//!         },
//!         false,
//!     )
//!     .await
//!     .unwrap();
//! let keys: Vec<&str> = page.objects.iter().map(|info| &**&info.key).collect();
//! assert_eq!(keys, ["a.txt", "e.txt"]);
//! assert_eq!(page.common_prefixes, ["dir/"]);
//! assert!(!page.truncated);
//! # }
//! ```

use tracing::instrument;

use crate::{
    _core::{
        ListObjectsParams, ObjectListing,
        content::{ContentEntry, ObjectContent},
        meta::{Metadata, ObjectMeta},
        object,
        storage::{Error, UnorderedPager},
    },
    ObjectOps,
    object::row_matches,
    ops::projected,
};

/// How many content entries one `scan` call is asked for.
///
/// The engine asks again from the last key it saw when the page is not decided
/// yet, so this is a batching constant, not a page bound: a backend may cap what
/// one call walks and the engine pays one contract call per window.
const SCAN_WINDOW: usize = 512;

impl<M, C> ObjectOps<M, C>
where
    M: Metadata,
    C: ObjectContent,
{
    /// List objects with prefix filtering, delimiter grouping and pagination
    /// (S3 semantics).
    ///
    /// `records_versions` is the bucket's versioning bit, which rides every
    /// returned [`object::Info`] (`ObjectListing` carries no state of its own).
    /// The identity of each listed version is **not** resolved here: the object
    /// row carries no version columns (D37) and this receiver has no version
    /// handle, so a backend whose bucket records versions fills
    /// `Info::version_id` in its own adapter — exactly as `tinio-mem` does
    /// today.
    ///
    /// `max_keys = 0` requests an empty, untruncated page and scans nothing.
    /// A `start_after` that is not a valid object key cannot name a stored key
    /// (every stored key went through the same constructor), so the scan starts
    /// at the beginning and the page engine's exclusive filter drops what sorts
    /// up to the marker — S3's `start-after` accepts any client string.
    ///
    /// # Errors
    ///
    /// Whatever the content plane or the row plane answers, and a heal's own
    /// failures other than a vanished key (which is skipped).
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bucket = %params.bucket)
    )]
    pub async fn list_objects(
        &self,
        params: ListObjectsParams,
        records_versions: bool,
    ) -> Result<ObjectListing, Error> {
        let bucket = params.bucket.clone();
        let delimiter = params.delimiter.as_deref();
        if params.max_keys == 0 {
            return Ok(ObjectListing {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                truncated: false,
                next_start_after: None,
            });
        }
        // The scan resume point: the marker when it is a valid key (the cheap
        // resume the contract's `after` offers), else nothing.
        let mut after = params
            .start_after
            .as_deref()
            .and_then(|start| object::key(start).ok());
        // The scan streams **into** the page engine, one entry at a time —
        // nothing between the two accumulates, so a page of a huge bucket (a
        // delimited listing whose keys all roll up included) costs the window
        // it walks, never the bucket it walked.
        //
        // The scan stops once the pager holds `max_keys + 1` distinct views.
        // Admission, the marker and the rollup are the pager's
        // ([`UnorderedPager::saturated`]); an ascending scan never counts the
        // same view a second time.
        let mut pager = UnorderedPager::new(
            &params.prefix,
            delimiter,
            params.start_after.as_deref(),
            params.max_keys,
            |entry: &ContentEntry| &entry.key,
        );
        loop {
            let window = self
                .content
                .scan(&bucket, &params.prefix, after.as_ref(), SCAN_WINDOW)
                .await
                .map_err(projected)?;
            if window.is_empty() {
                break;
            }
            let exhausted = window.len() < SCAN_WINDOW;
            let last = window.last().map(|entry| entry.key.clone());
            for entry in window {
                // A folder marker (a key ending in `/`) and a reserved key (a
                // `.tinio` segment) are content, never objects: the same key's
                // head answers `NoSuchKey`, so a listing that served it would
                // contradict the read surface — and the skip happens **here**,
                // before the page engine sees the entry, because a marker that
                // reached it under a delimiter would contribute a common prefix
                // of its own that no post-page filter could undo. (fs's `scan`
                // filters both inside its walk, so its entries arrive clean;
                // memory's plane stores a marker as content and yields it.)
                if entry.key.is_folder_marker() || entry.key.is_reserved() {
                    continue;
                }
                pager.offer_keyed(entry);
                if pager.saturated() {
                    break;
                }
            }
            if exhausted || pager.saturated() {
                break;
            }
            after = last;
            if after.is_none() {
                break;
            }
        }
        let (entries, common_prefixes, truncated, next_start_after) = pager.finish();
        // One batch read for the page — never a point read per key.
        let keys: Vec<object::Key> = entries.iter().map(|entry| entry.key.clone()).collect();
        let rows = ObjectMeta::load(&*self.meta, &bucket, &keys)
            .await
            .map_err(projected)?;
        let mut objects = Vec::with_capacity(entries.len());
        for (entry, row) in entries.iter().zip(rows) {
            let healed = match row {
                Some(row) if row_matches(&row, entry) => row,
                _ => match self.healed_row(&bucket, &entry.key, entry).await {
                    Ok(row) => row,
                    // The key vanished between the scan and the load: the
                    // content is the truth a listing lists, so the entry is
                    // skipped (a concurrent delete, not a listing failure).
                    Err(Error::NoSuchKey(_)) => continue,
                    Err(err) => return Err(err),
                },
            };
            objects.push(Self::info_of(&entry.key, entry, &healed, records_versions));
        }
        Ok(ObjectListing {
            objects,
            common_prefixes,
            truncated,
            next_start_after,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::SystemTime};

    use bytes::Bytes;
    use futures::stream::iter;
    use tinio_core::{
        ETag,
        acl::Acl,
        content::{ContentKey, StagedObject},
        meta::object::Row,
        storage::Error as StorageError,
    };

    use super::*;
    use crate::{
        _core::bucket,
        BucketRow, MetadataFailure,
        testutil::{Base, ContentCall, RecordingContent},
    };

    /// One listing request.
    fn params(
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        start_after: Option<&str>,
        max_keys: usize,
    ) -> ListObjectsParams {
        ListObjectsParams {
            bucket: bucket.clone(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.map(ToOwned::to_owned),
            start_after: start_after.map(ToOwned::to_owned),
            max_keys,
        }
    }

    /// The keys a page served, in order.
    fn keys(page: &ObjectListing) -> Vec<String> {
        page.objects
            .iter()
            .map(|info| info.key.to_string())
            .collect()
    }

    /// A bucket with a recorded row.
    async fn bucket(base: &Base, name: &str) -> bucket::Name {
        let name = bucket::name(name).unwrap();
        base.ops(BucketRow::Guaranteed)
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        name
    }

    /// Plant content at a key **without any row**.
    async fn plant(base: &Base, bucket: &bucket::Name, key: &str, bytes: &[u8]) {
        let content_key = ContentKey::Object(object::key(key).unwrap());
        let mut staged = base.content.stage(bucket, &content_key).await.unwrap();
        staged.write(Bytes::copy_from_slice(bytes)).await.unwrap();
        base.content
            .publish(bucket, staged, &content_key)
            .await
            .unwrap();
    }

    /// A five-key bucket: two flat keys, two under `dir/`, one under
    /// `dir/sub/`.
    async fn fixture(base: &Base) -> bucket::Name {
        let name = bucket(base, "data").await;
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
        for (key, bytes) in [
            ("a.txt", b"a".as_slice()),
            ("dir/x.txt", b"x".as_slice()),
            ("dir/y.txt", b"y".as_slice()),
            ("dir/sub/z.txt", b"z".as_slice()),
            ("e.txt", b"e".as_slice()),
        ] {
            let body = Box::pin(iter([Ok(Bytes::from_static(bytes))]));
            let staged = ops
                .stage_body(&name, &object::key(key).unwrap(), body, None, None)
                .await
                .unwrap();
            ops.commit_object(
                &name,
                &object::key(key).unwrap(),
                staged,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap();
        }
        name
    }

    #[tokio::test]
    async fn a_page_lists_by_prefix_delimiter_and_marker_with_one_load_each() {
        let base = Base::new();
        let name = fixture(&base).await;
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);

        // A full page in key order, enriched by one batch read.
        let full = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(
            keys(&full),
            ["a.txt", "dir/sub/z.txt", "dir/x.txt", "dir/y.txt", "e.txt"],
            "the page is in ascending key order"
        );
        assert!(full.common_prefixes.is_empty());
        assert!(!full.truncated);
        assert_eq!(full.next_start_after, None);
        assert_eq!(base.counts().loads, 1, "one load enriches the whole page");
        assert_eq!(
            full.objects[0].etag,
            ETag::from_content(b"a"),
            "the ETag is the content's"
        );
        assert!(
            full.objects[0].last_modified <= SystemTime::now(),
            "the served mtime is a real instant"
        );

        // The prefix filters the page.
        let prefixed = ops
            .list_objects(params(&name, "dir/", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&prefixed), ["dir/sub/z.txt", "dir/x.txt", "dir/y.txt"]);
        assert_eq!(base.counts().loads, 2);

        // The delimiter rolls the under-directory keys up, and a rolled-up
        // group counts as one entry of the page.
        let grouped = ops
            .list_objects(params(&name, "", Some("/"), None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&grouped), ["a.txt", "e.txt"]);
        assert_eq!(grouped.common_prefixes, ["dir/"]);

        let nested = ops
            .list_objects(params(&name, "dir/", Some("/"), None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&nested), ["dir/x.txt", "dir/y.txt"]);
        assert_eq!(nested.common_prefixes, ["dir/sub/"]);

        // Pagination: a full page is truncated, its marker resumes exactly
        // after the last entry, and the pages tile the listing.
        let first = ops
            .list_objects(params(&name, "", None, None, 2), false)
            .await
            .unwrap();
        assert_eq!(keys(&first), ["a.txt", "dir/sub/z.txt"]);
        assert!(first.truncated);
        let resume = first.next_start_after.clone().expect("a resume marker");
        assert_eq!(resume, "dir/sub/z.txt", "the marker is the last entry");
        let second = ops
            .list_objects(params(&name, "", None, Some(&resume), 2), false)
            .await
            .unwrap();
        assert_eq!(keys(&second), ["dir/x.txt", "dir/y.txt"]);
        assert!(second.truncated);
        let resume = second.next_start_after.clone().expect("a resume marker");
        let third = ops
            .list_objects(params(&name, "", None, Some(&resume), 2), false)
            .await
            .unwrap();
        assert_eq!(keys(&third), ["e.txt"]);
        assert!(!third.truncated);

        // A marker below the prefix does not shorten the page: S3's
        // `start-after` is any client string.
        let below = ops
            .list_objects(params(&name, "dir/", None, Some("a.txt"), 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&below).len(), 3, "the prefix's keys all list");

        // A marker that is not a valid key cannot name a stored key: the page
        // is the same as an unfiltered one.
        let invalid = ops
            .list_objects(params(&name, "", None, Some("a\u{0}b"), 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&invalid).len(), 5);

        // max_keys = 0 is an empty, untruncated page — and it scans nothing.
        let (recorded, recording) = base.recording_objects(BucketRow::Guaranteed);
        let empty = recorded
            .list_objects(params(&name, "", None, None, 0), false)
            .await
            .unwrap();
        assert!(empty.objects.is_empty() && empty.common_prefixes.is_empty());
        assert!(!empty.truncated && empty.next_start_after.is_none());
        assert!(
            !recording.saw(|call| matches!(call, ContentCall::Scan(_))),
            "no scan is paid for an empty page"
        );
    }

    #[tokio::test]
    async fn a_delimited_listing_rolls_a_group_up_and_resumes_past_it() {
        // A group several scan windows deep: the engine walks it (a rollup is
        // one page entry however many keys it holds), and the page it answers
        // is the rollup plus the flat key — with the resume marker naming the
        // rollup, which is not a key at all.
        let base = Base::new();
        let name = bucket(&base, "data").await;
        for index in 0..(SCAN_WINDOW * 3) {
            plant(&base, &name, &format!("big/{index:05}.bin"), b"x").await;
        }
        plant(&base, &name, "top.txt", b"t").await;
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);

        let grouped = ops
            .list_objects(params(&name, "", Some("/"), None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&grouped), ["top.txt"]);
        assert_eq!(grouped.common_prefixes, ["big/"]);
        assert!(!grouped.truncated);
        assert_eq!(
            base.counts().loads,
            1,
            "the rollup is one entry: one batch read for the page"
        );

        let (recorded, recording) = base.recording_objects(BucketRow::Guaranteed);
        let first = recorded
            .list_objects(params(&name, "", Some("/"), None, 1), false)
            .await
            .unwrap();
        assert!(keys(&first).is_empty());
        assert_eq!(first.common_prefixes, ["big/"]);
        assert!(first.truncated, "a truncated page has a probe");
        assert_eq!(
            first.next_start_after.as_deref(),
            Some("big/"),
            "the resume marker is the last emitted entry, rollup form included"
        );
        let scans = recording
            .calls()
            .iter()
            .filter(|call| matches!(call, ContentCall::Scan(_)))
            .count();
        assert_eq!(
            scans,
            (SCAN_WINDOW * 3) / SCAN_WINDOW + 1,
            "the walk crossed the whole group to find the page's second entry"
        );

        // The resume marker is any client string, not necessarily a key: the
        // walk starts at the beginning again and the page engine's exclusive
        // filter drops the group it already answered.
        let resume = first.next_start_after.clone().expect("a marker");
        let second = ops
            .list_objects(params(&name, "", Some("/"), Some(&resume), 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&second), ["top.txt"]);
        assert!(second.common_prefixes.is_empty(), "the group is behind us");
        assert!(!second.truncated);
    }

    #[tokio::test]
    async fn a_page_whose_entries_all_vanish_keeps_the_resume_marker() {
        // The entries were scanned (so the page is truncated) and every one of
        // them is gone before the page is assembled. The pager's
        // `(truncated, next)` is answered **unconditionally**, after the heal
        // loop: a dead page still names where to resume, so a client's sweep
        // terminates instead of looping on the same empty answer.
        //
        // Characterisation, not a RED: the rule holds by construction in the
        // current body, and this test is its only pin (fs's
        // `page_whose_entries_all_vanish_keeps_the_resume_marker` died with the
        // listing pipeline).
        let base = Base::new();
        let name = bucket(&base, "data").await;
        for key in ["a.txt", "b.txt", "c.txt"] {
            plant(&base, &name, key, b"x").await;
        }
        let content =
            RecordingContent::new(Arc::clone(&base.content), Arc::new(base.counted().clone()))
                .with_vanish_after_scan();
        let (ops, _recording) = base.recording_objects_with(BucketRow::Guaranteed, content);

        let page = ops
            .list_objects(params(&name, "", None, None, 1), false)
            .await
            .unwrap();
        assert!(
            page.objects.is_empty(),
            "every entry vanished before the page was built: {:?}",
            keys(&page)
        );
        assert!(
            page.truncated,
            "the entries were scanned, so the page is truncated and resumable"
        );
        assert_eq!(
            page.next_start_after.as_deref(),
            Some("a.txt"),
            "the resume marker is the page's last entry, dead or not"
        );

        // Resuming past it terminates: the walk is not handed the same page
        // for ever.
        let resume = page.next_start_after.clone().expect("a marker");
        let rest = ops
            .list_objects(params(&name, "", None, Some(&resume), 1), false)
            .await
            .unwrap();
        assert!(rest.objects.is_empty() && !rest.truncated);
        assert!(rest.next_start_after.is_none());
    }

    #[tokio::test]
    async fn a_hard_content_failure_fails_the_page() {
        // The other arm of the vanish rule: a key whose content cannot be read
        // (an I/O error, not a not-found) is a **listing failure**, never a
        // silently skipped entry — the page must not answer "no such object"
        // for a store that is broken.
        //
        // Characterisation, not a RED (the rule is `?`-propagation today).
        let base = Base::new();
        let name = bucket(&base, "data").await;
        plant(&base, &name, "a.txt", b"a").await;
        let content =
            RecordingContent::new(Arc::clone(&base.content), Arc::new(base.counted().clone()))
                .with_hard_open_failure();
        let (ops, _recording) = base.recording_objects_with(BucketRow::Guaranteed, content);

        let err = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::Io(_)),
            "an unreadable content plane is the page's answer: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_refused_row_write_fails_the_page() {
        // The heal that a cold entry needs is a **write**, and a write the row
        // plane refuses is the page's answer: the engine must not serve an
        // entry it could not record (and must not skip it either — the object
        // is there).
        //
        // Characterisation, not a RED (the row write's error propagates by
        // `?` today).
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("a.txt").unwrap();
        plant(&base, &name, &key, b"a").await;
        base.counted().refuse_writes_for(&name, &key);
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);

        let err = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::Io(_)),
            "a refused healing write is the page's answer: {err:?}"
        );
    }

    #[tokio::test]
    async fn a_folder_marker_is_never_a_listed_object() {
        // A marker is content, not an object (S3 semantics): a store may hold
        // bytes under a key ending in `/` — memory's plane stores exactly
        // that — and the listing must skip them, because the same key's head
        // answers `NoSuchKey` and the two surfaces cannot disagree. A reserved
        // key (a `.tinio` segment) is skipped for the same reason. The skip
        // happens **before** the page engine sees the entry: a marker that
        // reached it under a delimiter would contribute a common prefix of its
        // own, which no post-page filter could undo.
        let base = Base::new();
        let name = bucket(&base, "data").await;
        plant(&base, &name, "dir/", b"").await;
        plant(&base, &name, "a.txt", b"a").await;
        plant(&base, &name, ".tinio/state", b"x").await;
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);

        let plain = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(
            keys(&plain),
            ["a.txt"],
            "neither a marker nor a reserved key is a listed object"
        );

        let delimited = ops
            .list_objects(params(&name, "", Some("/"), None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&delimited), ["a.txt"], "the marker is not an object");
        assert!(
            delimited.common_prefixes.is_empty(),
            "and it rolls up to nothing: a marker that reached the page engine \
             would contribute `dir/` here — {:?}",
            delimited.common_prefixes
        );

        let under = ops
            .list_objects(params(&name, "dir/", Some("/"), None, 1000), false)
            .await
            .unwrap();
        assert!(keys(&under).is_empty() && under.common_prefixes.is_empty());

        // The read path needs no rule of its own, but **not** because a
        // content plane hides markers: memory's plane is
        // `CountingContent<RedbContent>`, whose `stat` is a plain key read
        // (exactly what the engine's reads call), and `FsContent::stat`
        // answers only for files. What hides the marker on the read path is
        // each backend's own **object-adapter** gate (mem's adapter refuses a
        // marker key, fs's answers `NoSuchKey` for a directory), so
        // `head_object` of the same key answers `NoSuchKey` on both and agrees
        // with this listing. The in-memory **reference** plane keeps every key
        // it is given and has no such adapter — which is exactly why the skip
        // has to be here, in the listing, rather than left to a content rule.
        assert!(
            ops.head_object(&name, &object::key("dir/").unwrap())
                .await
                .is_ok(),
            "the reference plane holds the marker as content"
        );
    }

    #[tokio::test]
    async fn a_cold_listing_heals_a_key_with_no_row() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        plant(&base, &name, "dropped.txt", b"dropped").await;
        assert!(
            base.meta
                .get(&name, &object::key("dropped.txt").unwrap())
                .await
                .unwrap()
                .is_none()
        );

        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
        let page = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&page), ["dropped.txt"], "the planted key appears");
        assert_eq!(
            page.objects[0].etag,
            ETag::from_content(b"dropped"),
            "with its recomputed content ETag"
        );
        assert_eq!(page.objects[0].size, 7, "the served size is the content's");
        let row = base
            .meta
            .get(&name, &object::key("dropped.txt").unwrap())
            .await
            .unwrap()
            .expect("the listing healed the row");
        assert_eq!(row.etag, ETag::from_content(b"dropped"));
        assert_eq!(base.counts().put_preserving, 1, "one heal for the page");

        // The second listing is a hit: the row now describes the content.
        let again = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap();
        assert_eq!(keys(&again), ["dropped.txt"]);
        assert_eq!(base.counts().put_preserving, 1, "a fresh row heals nothing");
    }

    #[tokio::test]
    async fn a_listing_skips_a_row_whose_content_vanished() {
        let base = Base::new();
        let name = bucket(&base, "data").await;
        let key = object::key("ghost.txt").unwrap();
        // A row with no content behind it: the content side is the authority on
        // what a listing lists, so the key does not appear.
        base.meta
            .put(
                &name,
                &key,
                &Row {
                    etag: ETag::EMPTY,
                    size: 3,
                    mtime: SystemTime::now(),
                    identity: 0,
                    tags: object::Tags::empty(),
                    checksum: None,
                    owner: None,
                    acl: Acl::default_private(None),
                },
            )
            .await
            .unwrap();

        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
        let page = ops
            .list_objects(params(&name, "", None, None, 1000), false)
            .await
            .unwrap();
        assert!(page.objects.is_empty(), "{:?}", keys(&page));
        assert!(!page.truncated);
        assert_eq!(
            base.counts().put_preserving,
            0,
            "nothing was healed: the scan never yielded the key"
        );
    }

    #[tokio::test]
    async fn the_records_versions_bit_rides_every_entry() {
        let base = Base::new();
        let name = fixture(&base).await;
        let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
        let page = ops
            .list_objects(params(&name, "a", None, None, 1000), true)
            .await
            .unwrap();
        assert_eq!(keys(&page), ["a.txt"]);
        assert!(page.objects[0].bucket_records_versions);
        assert_eq!(
            page.objects[0].version_id, None,
            "the identity is the caller's to fill"
        );

        // A bucket the content plane does not hold is an empty page, not an
        // error: buckets are content-side.
        let missing = bucket::name("never").unwrap();
        let page = ops
            .list_objects(params(&missing, "", None, None, 1000), false)
            .await
            .unwrap();
        assert!(page.objects.is_empty());
        assert!(
            ops.get_object_tags(&missing, &object::key("a.txt").unwrap())
                .await
                .is_err(),
            "a direct read still reports the missing bucket"
        );
        assert!(matches!(
            ops.head_object(&missing, &object::key("a.txt").unwrap())
                .await
                .unwrap_err(),
            StorageError::NoSuchKey(_)
        ));
    }
}
