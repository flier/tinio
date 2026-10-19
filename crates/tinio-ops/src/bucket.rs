//! The bucket group: the bucket lifecycle, the listing page and the
//! tag/CORS/ACL element reads and writes, over the row plane and the content
//! plane.
//!
//! Every body is a method of [`BucketOps`], and every one of them is written
//! against the two contracts only: the namespace and the content are the
//! content plane's ([`ObjectContent`]), the rows are the row plane's
//! ([`Metadata`], with [`BucketMeta`] and
//! [`UploadMeta`](tinio_core::meta::UploadMeta)), and the two steps only a
//! backend can take go through [`Hooks`].
//!
//! Two rules shape the group. **Buckets are content-side**: `create_bucket`
//! creates the namespace and then records the row, and `delete_bucket` asks
//! the content plane whether the bucket is empty — a folder-marker directory
//! is content, so a key-only scan would wrongly report an empty bucket.
//! **The destructive step is the commit point**: once the backend's hook
//! returns, the delete has happened; the engine touches nothing afterwards
//! and the backend owns the whole teardown.
//!
//! # Examples
//!
//! The group runs over the in-memory reference planes. This is the shape a
//! backend wires — its own row plane, its own content plane, its own hooks:
//!
//! ```rust
//! use std::sync::{Arc, Mutex};
//!
//! use async_trait::async_trait;
//! use tinio_core::{acl::Acl, bucket, meta::BucketMeta, storage};
//! use tinio_ops::{BucketOps, BucketRow, Guard, Hooks, MetadataFailure, NoGuard};
//! use tinio_store_redb::{RedbContent, RedbMetadata};
//!
//! #[derive(Clone)]
//! struct Deleted(Arc<Mutex<bool>>);
//!
//! #[async_trait]
//! impl Hooks for Deleted {
//!     async fn before_bucket_delete(&self, _bucket: &bucket::Name) -> Result<(), storage::Error> {
//!         *self.0.lock().unwrap() = true;
//!         Ok(())
//!     }
//!
//!     async fn lock_part(&self, _bucket: &bucket::Name, _upload_id: &str) -> Box<dyn Guard> {
//!         Box::new(NoGuard)
//!     }
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let meta = Arc::new(RedbMetadata::in_memory().unwrap());
//! let content = Arc::new(RedbContent::from_driver(meta.driver()));
//! let deleted = Arc::new(Mutex::new(false));
//! let buckets = BucketOps {
//!     meta: Arc::clone(&meta),
//!     content: Arc::clone(&content),
//!     hooks: Deleted(Arc::clone(&deleted)),
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//!
//! let name = bucket::name("data").unwrap();
//! buckets
//!     .create_bucket(&name, None, &Acl::default_private(None))
//!     .await
//!     .unwrap();
//! assert_eq!(buckets.head_bucket(&name).await.unwrap().name, name);
//! assert!(meta.row(&name).await.unwrap().is_some());
//!
//! buckets.delete_bucket(&name).await.unwrap();
//! assert!(*deleted.lock().unwrap(), "the hook is the commit point");
//! # }
//! ```

use std::time::SystemTime;

use crate::{
    _core::{
        Bucket, BucketsListing, ListBucketsParams, acl,
        acl::{Acl, AclGrants},
        bucket,
        content::ObjectContent,
        cors, lifecycle,
        meta::{BucketMeta, Metadata, bucket as row},
        object,
        storage::{self, Error, UnorderedPager},
    },
    hooks::Hooks,
    ops::{BucketOps, BucketRow, projected, served_without_writing},
};

impl<M, C, H> BucketOps<M, C, H>
where
    M: Metadata,
    C: ObjectContent,
    H: Hooks,
{
    /// Create `name`, recording the create-time `owner` and `acl`.
    ///
    /// The namespace is the content plane's and comes first; the row follows,
    /// carrying the owner element and the grant set beside it (the row form
    /// keeps the two apart). How the row is written is the receiver's
    /// [`BucketRow`] policy: a row-backed store inserts or refuses
    /// (`AlreadyExists`, decided inside the contract's transaction so a
    /// concurrent create cannot be raced), while a store whose buckets outlive
    /// their rows **adopts** an existing record — the directory is the bucket,
    /// and a record left behind by a failed teardown or a hand-removed
    /// directory says nothing against recreating it. The adoption is logged
    /// once, so the state a repair pass has to prune is observable instead of
    /// silent.
    ///
    /// # Errors
    ///
    /// `AlreadyExists` when the policy inserts and the store already has the
    /// bucket; the content plane's or the row plane's own failures otherwise.
    pub async fn create_bucket(
        &self,
        name: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        self.content.create_bucket(name).await.map_err(projected)?;
        let now = SystemTime::now();
        let mut recorded = row::Row::at(now);
        recorded.owner = owner.cloned();
        recorded.acl = Acl {
            owner: None,
            grants: acl.grants.clone(),
        };
        match self.bucket_row {
            BucketRow::Lazy => {
                if self
                    .meta
                    .created_at(name)
                    .await
                    .map_err(projected)?
                    .is_some()
                {
                    tracing::warn!(
                        bucket = %name,
                        "adopting a bucket record whose directory was gone"
                    );
                }
                self.meta
                    .record_full(name, &recorded)
                    .await
                    .map_err(projected)?;
            }
            // The qualified call: `Metadata` carries two `create`s (the bucket
            // row and the upload row), and this one is the bucket's.
            BucketRow::Guaranteed => {
                <M as BucketMeta>::create(&*self.meta, name, &recorded)
                    .await
                    .map_err(projected)?;
            }
        }
        Ok(())
    }

    /// Delete an empty bucket.
    ///
    /// The emptiness decision is the composition of **three** probes, one per
    /// plane, taken for every backend here rather than written into each
    /// backend's own teardown: the content plane's
    /// [`is_empty`](tinio_core::content::ObjectContent::is_empty) — a
    /// folder-marker directory is content, so a key-only scan would wrongly
    /// report an empty bucket — the in-progress upload rows, and the object
    /// rows ([`has_objects`](tinio_core::meta::ObjectMeta::has_objects), which
    /// a **delete marker** is: a marker writes no content, so the tree alone
    /// reads empty). All three are read-only and all three answer before
    /// the commit point. The backend's
    /// [`before_bucket_delete`](crate::hooks::Hooks::before_bucket_delete) then
    /// owns the whole teardown; the engine touches the bucket no further.
    ///
    /// # Errors
    ///
    /// `NotEmpty` while the bucket holds content, an object row or an
    /// in-progress upload — and whatever the backend's hook answers, including
    /// `NoSuchBucket` for a store whose rows are the buckets.
    pub async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        if !self.content.is_empty(name).await.map_err(projected)? {
            return Err(storage::not_empty(name));
        }
        if self.meta.has_uploads(name).await.map_err(projected)? {
            return Err(storage::not_empty(name));
        }
        if self.meta.has_objects(name).await.map_err(projected)? {
            return Err(storage::not_empty(name));
        }
        self.hooks.before_bucket_delete(name).await
    }

    /// The bucket's header: its name and recorded creation time.
    ///
    /// A row-less bucket follows the receiver's [`BucketRow`] policy — a lazy
    /// store records the first sight now (its namespace is the bucket, and the
    /// row merely had never been written), a guaranteed one answers
    /// `NoSuchBucket`.
    ///
    /// The first-sight record is a **write on a read path**: a store that
    /// refuses it because it is read-only serves the header anyway, answering
    /// the very `now` it would have recorded, with one event noting the
    /// degradation (the crate's shared `served_without_writing` rule, which
    /// every write-on-read path takes). Every other failure propagates.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        let creation_time = match self.meta.created_at(name).await.map_err(projected)? {
            Some(created) => created,
            None => match self.bucket_row {
                BucketRow::Lazy => {
                    let now = SystemTime::now();
                    served_without_writing(
                        self.meta
                            .record_if_absent(name, now)
                            .await
                            .map_err(projected),
                        now,
                        "the bucket head's first sight",
                    )?
                }
                BucketRow::Guaranteed => return Err(storage::no_such_bucket(name)),
            },
        };
        Ok(Bucket {
            name: name.clone(),
            creation_time,
        })
    }

    /// List the buckets, in name order, per S3's listing semantics: the
    /// `prefix` filter, an exclusive `start_after` marker, a page of at most
    /// `params.max_buckets` (`0` requests an empty, untruncated page).
    ///
    /// The `owner` filter applies to the **filtered** set before pagination
    /// (the continuation marker's arithmetic), and a bucket whose recorded
    /// owner wire is empty belongs to `lazy_default` — the effective owner the
    /// comparison uses; `lazy_default = None` is the strict compare, and
    /// `owner = None` the unfiltered listing. Those rules, the marker and the
    /// page cut are [`paginate_buckets`]'s, which fs's sweep-fed body takes
    /// too.
    ///
    /// # Errors
    ///
    /// Whatever the row plane answers, and `InvalidBucketName` for a stored
    /// name no longer valid (a hand-edited store, never a panic on the way out
    /// of a listing).
    pub async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // The row plane's enumeration, narrowed to the prefix. The stored
        // names are the ones `create_bucket` validated, so a stored name that
        // does not convert is a hand-edited store: report it rather than
        // panicking on the way out of a listing.
        let candidates: Vec<(bucket::Name, SystemTime)> = self
            .meta
            .load_all()
            .await
            .map_err(projected)?
            .into_iter()
            .filter(|(name, _)| name.starts_with(&params.prefix))
            .map(|(name, creation_time)| Ok((bucket::name(name)?, creation_time)))
            .collect::<Result<Vec<(bucket::Name, SystemTime)>, Error>>()?;
        // One batch read resolves every candidate's recorded owner element;
        // the unfiltered listing reads none.
        let recorded = match owner {
            Some(_) => {
                let names: Vec<bucket::Name> =
                    candidates.iter().map(|(name, _)| name.clone()).collect();
                self.meta.owners(&names).await.map_err(projected)?
            }
            None => vec![None; candidates.len()],
        };
        let (page, truncated, next) = paginate_buckets(
            candidates.into_iter().zip(recorded).collect(),
            &params,
            owner,
            lazy_default,
            |(name, _): &(bucket::Name, SystemTime)| name.as_ref().as_str(),
        );
        Ok(BucketsListing {
            buckets: page
                .into_iter()
                .map(|(name, creation_time)| Bucket {
                    name,
                    creation_time,
                })
                .collect(),
            truncated,
            next_start_after: next,
        })
    }

    /// The row-backed element read's one decision, the home of the
    /// [`BucketRow`] policy: a recorded row is projected through `project`,
    /// and a bucket with no row is the lazy store's `default` (the row merely
    /// was never written) or the guaranteed store's `NoSuchBucket`.
    async fn row_or<T>(
        &self,
        name: &bucket::Name,
        default: T,
        project: impl FnOnce(row::Row) -> T,
    ) -> Result<T, Error> {
        match self.meta.row(name).await.map_err(projected)? {
            Some(recorded) => Ok(project(recorded)),
            None => match self.bucket_row {
                BucketRow::Lazy => Ok(default),
                BucketRow::Guaranteed => Err(storage::no_such_bucket(name)),
            },
        }
    }

    /// The presence decision alone: the row read whose answer is the
    /// [`BucketRow`] policy's, with no payload to fabricate.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when a guaranteed store has no row.
    async fn present(&self, name: &bucket::Name) -> Result<(), Error> {
        self.row_or(name, (), |_| ()).await
    }

    /// The bucket's tag set (S3 GetBucketTagging).
    ///
    /// A row-less bucket follows the receiver's [`BucketRow`] policy: the lazy
    /// store serves the empty set (a bucket never tagged through the API), the
    /// guaranteed one answers `NoSuchBucket`.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn get_bucket_tags(&self, name: &bucket::Name) -> Result<object::Tags, Error> {
        self.row_or(name, object::Tags::empty(), |recorded| recorded.tags)
            .await
    }

    /// Replace the bucket's tag set (S3 PutBucketTagging — replace-all, no
    /// merge).
    ///
    /// The presence decision is the contract's own: the lazy store's setter
    /// records a first-sight row **with** the tags in one transaction, the
    /// guaranteed store's answers `false` for a bucket with no row.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn put_bucket_tags(
        &self,
        name: &bucket::Name,
        tags: &object::Tags,
    ) -> Result<(), Error> {
        match self.bucket_row {
            BucketRow::Lazy => self
                .meta
                .set_tags_or_create(name, tags, SystemTime::now())
                .await
                .map_err(projected)?,
            BucketRow::Guaranteed => {
                // The qualified call: `Metadata` carries two `set_tags` (the
                // bucket row's and the object row's), and this one is the
                // bucket's.
                if !<M as BucketMeta>::set_tags(&*self.meta, name, tags)
                    .await
                    .map_err(projected)?
                {
                    return Err(storage::no_such_bucket(name));
                }
            }
        }
        Ok(())
    }

    /// Remove the bucket's tag set (S3 DeleteBucketTagging).
    ///
    /// S3's delete-tagging is lenient: a missing bucket is `Ok`, and so is a
    /// bucket with no tags to clear — this is the one element write that does
    /// not consult the row policy.
    ///
    /// # Errors
    ///
    /// Whatever the row plane answers for a real fault; never `NoSuchBucket`.
    pub async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), Error> {
        // Qualified like the tag put: `Metadata` carries two `clear_tags`.
        <M as BucketMeta>::clear_tags(&*self.meta, name)
            .await
            .map_err(projected)?;
        Ok(())
    }

    /// The bucket's CORS configuration (S3 GetBucketCors), `None` when it has
    /// none.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn get_bucket_cors(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<cors::Config>, Error> {
        self.row_or(name, None, |recorded| recorded.cors).await
    }

    /// Replace the bucket's CORS configuration (S3 PutBucketCors — replace-all,
    /// no merge).
    ///
    /// The caller guarantees at least one rule; a zero-rule configuration **is**
    /// "no configuration" and normalizes to the empty wire by the codec itself,
    /// so writing one and clearing the configuration are the same stored state
    /// on every backend.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the bucket has no row — the CORS element has no
    /// first-sight write, on either row policy (S3 mandates the refusal).
    pub async fn put_bucket_cors(
        &self,
        name: &bucket::Name,
        config: &cors::Config,
    ) -> Result<(), Error> {
        if !self.meta.set_cors(name, config).await.map_err(projected)? {
            return Err(storage::no_such_bucket(name));
        }
        Ok(())
    }

    /// Remove the bucket's CORS configuration (S3 DeleteBucketCors): idempotent
    /// on the stored configuration, `NoSuchBucket` for a bucket with no row —
    /// the delete-tagging leniency does not extend here.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the bucket has no row.
    pub async fn delete_bucket_cors(&self, name: &bucket::Name) -> Result<(), Error> {
        if !self.meta.clear_cors(name).await.map_err(projected)? {
            return Err(storage::no_such_bucket(name));
        }
        Ok(())
    }

    /// The bucket's lifecycle configuration (S3 GetBucketLifecycleConfiguration),
    /// `None` when it stores none.
    ///
    /// The configuration lives in a table of its own, not in the bucket row, so
    /// this body takes **two** reads where the CORS read takes one: the row
    /// policy's presence probe first — a missing bucket is `NoSuchBucket`, which
    /// is the contract's obligation and what keeps the three backends' answers
    /// identical — and then the configuration.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn get_bucket_lifecycle(
        &self,
        name: &bucket::Name,
    ) -> Result<Option<lifecycle::Config>, Error> {
        // A lazy store's bucket is its directory, which the caller has already
        // proved; only the guaranteed policy needs the row probe.
        match self.bucket_row {
            BucketRow::Lazy => {}
            BucketRow::Guaranteed => self.present(name).await?,
        }
        self.meta.lifecycle(name).await.map_err(projected)
    }

    /// Replace the bucket's lifecycle configuration (S3
    /// PutBucketLifecycleConfiguration — replace-all, no merge).
    ///
    /// The presence decision follows the receiver's [`BucketRow`] policy. A
    /// guaranteed store takes the contract's own bool — bucket presence, not
    /// change, so an identical re-write is `Ok` — inside the writing
    /// transaction, with no first-sight write (S3 mandates the refusal). A lazy
    /// store's bucket is its directory, so a row-less name is a bucket that
    /// was never sighted: the configuration write records the row first, in
    /// the same transaction ([`BucketMeta::set_lifecycle_or_create`]) — the
    /// shape the tag and ACL first-sight writes already use.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when a guaranteed store has no bucket row.
    pub async fn put_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<(), Error> {
        match self.bucket_row {
            BucketRow::Lazy => self
                .meta
                .set_lifecycle_or_create(name, config, SystemTime::now())
                .await
                .map_err(projected),
            BucketRow::Guaranteed => {
                if !self
                    .meta
                    .set_lifecycle(name, config)
                    .await
                    .map_err(projected)?
                {
                    return Err(storage::no_such_bucket(name));
                }
                Ok(())
            }
        }
    }

    /// Upsert `rules` into the stored configuration in the store's one
    /// transaction (the seed's write, LC13). The presence decision is
    /// [`BucketOps::put_bucket_lifecycle`]'s: a guaranteed store refuses a
    /// missing bucket, a lazy store records the row on first sight, and
    /// either way a merge [`lifecycle::Config::validate`] refuses is not
    /// stored.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when a guaranteed store has no bucket row.
    pub async fn merge_bucket_lifecycle(
        &self,
        name: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        let merged = match self.bucket_row {
            BucketRow::Lazy => self
                .meta
                .merge_lifecycle_or_create(name, rules, SystemTime::now())
                .await
                .map_err(projected)?,
            BucketRow::Guaranteed => {
                self.meta
                    .merge_lifecycle(name, rules)
                    .await
                    .map_err(projected)?
            }
        };
        match merged {
            lifecycle::Merged::Absent => Err(storage::no_such_bucket(name)),
            other => Ok(other),
        }
    }

    /// Remove the bucket's lifecycle configuration (S3 DeleteBucketLifecycle):
    /// idempotent on the stored configuration — a bucket that stores none
    /// answers `Ok`, never `NoSuchLifecycleConfiguration` — and `NoSuchBucket`
    /// for a guaranteed store with no bucket row. A lazy store's bucket is its
    /// directory, so the clear is unconditionally idempotent there: no row is
    /// exactly "stores none".
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when a guaranteed store has no bucket row.
    pub async fn delete_bucket_lifecycle(&self, name: &bucket::Name) -> Result<(), Error> {
        let cleared = self.meta.clear_lifecycle(name).await.map_err(projected)?;
        if matches!(self.bucket_row, BucketRow::Guaranteed) && !cleared {
            return Err(storage::no_such_bucket(name));
        }
        Ok(())
    }

    /// The bucket's ACL (S3 GetBucketAcl): the recorded owner element
    /// recombined with the stored grant set.
    ///
    /// A row-less bucket follows the receiver's [`BucketRow`] policy — the lazy
    /// store serves the default private ACL with no owner, the guaranteed one
    /// answers `NoSuchBucket`.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn get_bucket_acl(&self, name: &bucket::Name) -> Result<Acl, Error> {
        self.row_or(name, Acl::default_private(None), |recorded| Acl {
            owner: recorded.owner,
            grants: recorded.acl.grants,
        })
        .await
    }

    /// Replace the bucket's grant set (S3 PutBucketAcl — replace-all, no
    /// merge). The owner element is preserved, never changed by a put.
    ///
    /// The two row policies decide how the owner survives: the lazy store's
    /// setter reads the recorded owner and carries it over **inside** the one
    /// writing transaction (recording a first-sight row with the grants and no
    /// owner when the bucket was never sighted), while the guaranteed store
    /// reads the row first and hands the owner back to the setter, answering
    /// `NoSuchBucket` when the row is missing.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` when the store's row policy says the bucket is missing.
    pub async fn put_bucket_acl(
        &self,
        name: &bucket::Name,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        match self.bucket_row {
            BucketRow::Lazy => self
                .meta
                .set_grants_or_create(name, grants, SystemTime::now())
                .await
                .map_err(projected)?,
            BucketRow::Guaranteed => {
                let Some(recorded) = self.meta.row(name).await.map_err(projected)? else {
                    return Err(storage::no_such_bucket(name));
                };
                // The qualified call: `Metadata` carries two `set_acl`s (the
                // bucket row's and the object row's), and this one is the
                // bucket's.
                if !<M as BucketMeta>::set_acl(&*self.meta, name, recorded.owner.as_ref(), grants)
                    .await
                    .map_err(projected)?
                {
                    return Err(storage::no_such_bucket(name));
                }
            }
        }
        Ok(())
    }
}

/// The bucket-listing rule over one candidate set: the effective-owner
/// filter, the exclusive `start_after` marker and the page cut with its
/// truncation marker — the half of [`BucketOps::list_buckets`] that does not
/// depend on where the candidates came from, nor on how a page's creation
/// times resolve.
///
/// One home for the rules both backends apply. The engine's row-fed body
/// calls this over the `BUCKETS` rows it enumerated, and `tinio-fs` calls it
/// over the root-directory sweep that *is* its candidate set (a bucket the
/// API has never sighted has no row to enumerate — the candidate **source**
/// is the filed gap, never these rules). Each caller supplies its own
/// candidates, already narrowed to `params.prefix` — the engine filters the
/// row scan, fs's walk takes the prefix — and resolves the page's creation
/// times its own way afterwards: the engine's rows carry theirs, while fs
/// resolves the page's after the cut (its rows may not exist yet, and a
/// bucket the page did not reach stays unrecorded until listed).
///
/// `candidates` pairs each candidate with the row plane's recorded owner
/// element **for that candidate** (`None` for a candidate with no row, and
/// `None` throughout when `owner` is `None` — the unfiltered listing reads no
/// owners at all); `key_of` is the name the S3 listing orders and pages by.
/// The owner filter applies to the filtered set **before** pagination, and a
/// candidate whose recorded wire is empty belongs to `lazy_default` — the
/// effective owner this compares; `lazy_default = None` is the strict
/// compare and `owner = None` the unfiltered listing.
///
/// The page is the unordered pager's bounded one (`max + 1` entries, the
/// `max` smallest names after the marker), so an unordered candidate stream —
/// fs's dirent walk, one entry at a time — pages exactly as the engine's
/// ordered row scan does: the same exclusive marker, the same page, the same
/// truncation probe and resume marker. `max_buckets = 0` requests nothing:
/// the page is empty, untruncated and carries no marker.
pub fn paginate_buckets<T>(
    candidates: Vec<(T, Option<acl::OwnerId>)>,
    params: &ListBucketsParams,
    owner: Option<&acl::OwnerId>,
    lazy_default: Option<&acl::OwnerId>,
    key_of: impl for<'a> Fn(&'a T) -> &'a str,
) -> (Vec<T>, bool, Option<String>) {
    let mut pager = UnorderedPager::new(
        &params.prefix,
        None,
        params.start_after.as_deref(),
        params.max_buckets,
        key_of,
    );
    for (candidate, recorded) in candidates {
        // The effective owner: the recorded element, or `lazy_default` for a
        // row whose wire is empty.
        if let Some(owner) = owner
            && recorded.as_ref().or(lazy_default) != Some(owner)
        {
            continue;
        }
        pager.offer_keyed(candidate);
    }
    let (page, _prefixes, truncated, next) = pager.finish();
    (page, truncated, next)
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use bytes::Bytes;
    use tinio_util::testing::metadata::lifecycle_config_with;

    use crate::{
        _core::{
            ETag,
            acl::{Acl, AclGrants, Grant, Grantee, OwnerId, Permission},
            bucket,
            content::{ContentKey, ObjectContent, StagedObject},
            cors,
            meta::{
                BucketMeta, ObjectMeta, UploadMeta, object as meta_object, upload as upload_row,
            },
            object,
            storage::{Error as StorageError, ListBucketsParams},
        },
        BucketRow,
        bucket::paginate_buckets,
        testutil::{Base, Call},
    };

    /// A canonical owner id.
    fn owner() -> OwnerId {
        OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
    }

    /// A second canonical principal (the grant a put replaces the set with).
    fn other_owner() -> OwnerId {
        OwnerId::new("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210").unwrap()
    }

    /// The grant set a write replaces a bucket's grants with.
    fn grants() -> AclGrants {
        vec![Grant {
            grantee: Grantee::Canonical(other_owner()),
            permission: Permission::FullControl,
        }]
    }

    /// One rule of CORS configuration, in declaration order.
    fn cors_config() -> cors::Config {
        cors::Config {
            rules: vec![cors::Rule {
                id: Some("one".to_owned()),
                allowed_methods: vec!["GET".to_owned()],
                allowed_origins: vec!["*".to_owned()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        }
    }

    /// The listing request a test pages with.
    fn params(prefix: &str, start_after: Option<&str>, max_buckets: usize) -> ListBucketsParams {
        ListBucketsParams {
            prefix: prefix.to_owned(),
            start_after: start_after.map(ToOwned::to_owned),
            max_buckets,
        }
    }

    /// The bucket group's created bucket.
    async fn create(base: &Base, bucket_row: crate::BucketRow, name: &bucket::Name) {
        base.ops(bucket_row)
            .create_bucket(name, None, &Acl::default_private(None))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_bucket_records_the_row_and_refuses_a_second_create() {
        let base = Base::new();
        let ops = base.ops(BucketRow::Guaranteed);
        let name = bucket::name("data").unwrap();
        let acl = Acl::default_private(Some(owner()));

        ops.create_bucket(&name, Some(&owner()), &acl)
            .await
            .unwrap();

        let row = base.meta.row(&name).await.unwrap().expect("the row");
        assert_eq!(
            row.owner.as_ref(),
            Some(&owner()),
            "the owner rides the row"
        );
        assert_eq!(row.acl.grants, acl.grants, "the grants ride the row");
        assert!(
            row.acl.owner.is_none(),
            "the row form keeps the owner and the grants apart"
        );

        // The create-or-refuse is one decision: a second create of the same
        // name answers `AlreadyExists`, never a silent re-record.
        let err: StorageError = ops
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::AlreadyExists(_)), "{err:?}");
        assert_eq!(
            base.meta.row(&name).await.unwrap().unwrap().owner.as_ref(),
            Some(&owner()),
            "the refused create left the recorded row alone"
        );
    }

    #[tokio::test]
    async fn a_missing_row_follows_the_bucket_row_policy() {
        let base = Base::new();
        let name = bucket::name("ghost").unwrap();

        // A row-backed store: the row IS the bucket, so every read refuses.
        let guaranteed = base.ops(BucketRow::Guaranteed);
        let refusals = [
            guaranteed.get_bucket_tags(&name).await.unwrap_err(),
            guaranteed.get_bucket_cors(&name).await.unwrap_err(),
            guaranteed.get_bucket_acl(&name).await.unwrap_err(),
            guaranteed.head_bucket(&name).await.unwrap_err(),
            guaranteed
                .put_bucket_tags(&name, &object::Tags::empty())
                .await
                .unwrap_err(),
            guaranteed
                .put_bucket_cors(&name, &cors_config())
                .await
                .unwrap_err(),
            guaranteed.delete_bucket_cors(&name).await.unwrap_err(),
            guaranteed
                .put_bucket_acl(&name, &grants())
                .await
                .unwrap_err(),
        ];
        for err in refusals {
            assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
        }
        // The one lenient write: S3's delete-tagging is idempotent.
        guaranteed.delete_bucket_tags(&name).await.unwrap();
        assert!(base.meta.row(&name).await.unwrap().is_none());

        // A store whose buckets outlive their rows: a row-less bucket was
        // never sighted, so the reads serve the empty defaults and record
        // nothing.
        let lazy = base.ops(BucketRow::Lazy);
        assert!(lazy.get_bucket_tags(&name).await.unwrap().is_empty());
        assert_eq!(lazy.get_bucket_cors(&name).await.unwrap(), None);
        assert_eq!(
            lazy.get_bucket_acl(&name).await.unwrap(),
            Acl::default_private(None)
        );
        assert!(
            base.meta.row(&name).await.unwrap().is_none(),
            "a read records nothing"
        );
        // `head_bucket` is the first-sight record: the lazy store answers the
        // creation time it records.
        let head = lazy.head_bucket(&name).await.unwrap();
        assert_eq!(head.name, name);
        assert!(
            base.meta.row(&name).await.unwrap().is_some(),
            "head records the first sight"
        );
    }

    #[tokio::test]
    async fn a_lazy_row_records_the_first_sight_with_the_element() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        let lazy = base.ops(BucketRow::Lazy);

        let tags = object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap();
        lazy.put_bucket_tags(&name, &tags).await.unwrap();
        let row = base.meta.row(&name).await.unwrap().expect("the row");
        assert_eq!(row.tags, tags, "the row is recorded WITH its tags");

        // The grants write records a first-sight row with the grants and no
        // owner — a bucket the API never sighted has none to carry.
        let name = bucket::name("grants").unwrap();
        lazy.put_bucket_acl(&name, &grants()).await.unwrap();
        let row = base.meta.row(&name).await.unwrap().expect("the row");
        assert_eq!(row.acl.grants, grants());
        assert_eq!(row.owner, None, "a first-sight row carries no owner");
    }

    #[tokio::test]
    async fn the_acl_put_replaces_the_grants_and_preserves_the_owner() {
        let base = Base::new();
        let ops = base.ops(BucketRow::Guaranteed);
        let name = bucket::name("data").unwrap();
        ops.create_bucket(&name, Some(&owner()), &Acl::default_private(Some(owner())))
            .await
            .unwrap();

        let created = ops.get_bucket_acl(&name).await.unwrap();
        assert_eq!(created.owner.as_ref(), Some(&owner()));
        assert_eq!(created, Acl::default_private(Some(owner())));

        // The other elements are recorded first, so the ACL write's
        // element-targeted replacement is visible as what it is.
        let tags = object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap();
        ops.put_bucket_tags(&name, &tags).await.unwrap();
        ops.put_bucket_cors(&name, &cors_config()).await.unwrap();
        let created = base.meta.row(&name).await.unwrap().unwrap().created;

        ops.put_bucket_acl(&name, &grants()).await.unwrap();
        let replaced = ops.get_bucket_acl(&name).await.unwrap();
        assert_eq!(replaced.owner.as_ref(), Some(&owner()), "the owner rides");
        assert_eq!(replaced.grants, grants(), "the grants are replaced");

        // The ACL write left the row's other elements alone: the tags, the
        // CORS configuration and the recorded creation time are untouched.
        let row = base.meta.row(&name).await.unwrap().expect("the row");
        assert_eq!(row.tags, tags, "the tag set survives the ACL write");
        assert_eq!(
            row.cors,
            Some(cors_config()),
            "the CORS configuration survives the ACL write"
        );
        assert_eq!(row.created, created, "the creation time is preserved");
    }

    #[tokio::test]
    async fn tags_round_trip_replace_and_clear() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        let ops = base.ops(BucketRow::Guaranteed);

        assert!(ops.get_bucket_tags(&name).await.unwrap().is_empty());
        let tags = object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap();
        ops.put_bucket_tags(&name, &tags).await.unwrap();
        assert_eq!(ops.get_bucket_tags(&name).await.unwrap(), tags);
        let replaced = object::Tags::from_pairs([("team".to_owned(), "edge".to_owned())]).unwrap();
        ops.put_bucket_tags(&name, &replaced).await.unwrap();
        assert_eq!(ops.get_bucket_tags(&name).await.unwrap(), replaced);

        ops.delete_bucket_tags(&name).await.unwrap();
        assert!(ops.get_bucket_tags(&name).await.unwrap().is_empty());
        ops.delete_bucket_tags(&name).await.unwrap(); // idempotent
        ops.delete_bucket_tags(&bucket::name("ghost").unwrap())
            .await
            .unwrap(); // lenient on a missing bucket too
    }

    #[tokio::test]
    async fn cors_round_trips_replaces_and_normalizes_a_zero_rule_config() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        let ops = base.ops(BucketRow::Guaranteed);
        let config = cors_config();

        assert_eq!(ops.get_bucket_cors(&name).await.unwrap(), None);
        ops.put_bucket_cors(&name, &config).await.unwrap();
        assert_eq!(
            ops.get_bucket_cors(&name).await.unwrap(),
            Some(config.clone())
        );
        // A zero-rule configuration IS "no configuration" — the codec's own
        // normalization, through the whole body.
        ops.put_bucket_cors(&name, &cors::Config::default())
            .await
            .unwrap();
        assert_eq!(ops.get_bucket_cors(&name).await.unwrap(), None);
        ops.put_bucket_cors(&name, &config).await.unwrap();
        ops.delete_bucket_cors(&name).await.unwrap();
        assert_eq!(ops.get_bucket_cors(&name).await.unwrap(), None);
        ops.delete_bucket_cors(&name).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn lifecycle_needs_a_bucket_and_round_trips() {
        // The presence probe is the get's own: the configuration lives in a
        // table of its own, so `lifecycle()` alone answers `None` for a ghost
        // bucket — the guaranteed store must refuse, and the probe is what
        // makes it.
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        let ghost = bucket::name("ghost").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        let ops = base.ops(BucketRow::Guaranteed);
        let config = lifecycle_config_with("one", None, 30);

        let refusals = [
            ops.get_bucket_lifecycle(&ghost).await.unwrap_err(),
            ops.put_bucket_lifecycle(&ghost, &config).await.unwrap_err(),
            ops.delete_bucket_lifecycle(&ghost).await.unwrap_err(),
        ];
        for err in refusals {
            assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
        }

        assert_eq!(ops.get_bucket_lifecycle(&name).await.unwrap(), None);
        ops.put_bucket_lifecycle(&name, &config).await.unwrap();
        assert_eq!(
            ops.get_bucket_lifecycle(&name).await.unwrap(),
            Some(config.clone())
        );
        // The bool is bucket presence, not change: an identical re-write and a
        // clear of a bucket storing none are both `Ok`.
        ops.put_bucket_lifecycle(&name, &config).await.unwrap();
        ops.delete_bucket_lifecycle(&name).await.unwrap();
        ops.delete_bucket_lifecycle(&name).await.unwrap();
        assert_eq!(ops.get_bucket_lifecycle(&name).await.unwrap(), None);
    }

    #[tokio::test]
    async fn list_buckets_pages_in_name_order() {
        let base = Base::new();
        let ops = base.ops(BucketRow::Guaranteed);
        for name in ["zeta", "alpha", "mid"] {
            create(&base, BucketRow::Guaranteed, &bucket::name(name).unwrap()).await;
        }

        let full = ops
            .list_buckets(params("", None, 1000), None, None)
            .await
            .unwrap();
        let names: Vec<&str> = full
            .buckets
            .iter()
            .map(|b| b.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["alpha", "mid", "zeta"]);
        assert!(!full.truncated);
        assert_eq!(full.next_start_after, None);

        let prefixed = ops
            .list_buckets(params("m", None, 1000), None, None)
            .await
            .unwrap();
        assert_eq!(prefixed.buckets.len(), 1, "the prefix filters the page");
        assert_eq!(prefixed.buckets[0].name.as_ref().as_str(), "mid");

        let first = ops
            .list_buckets(params("", None, 1), None, None)
            .await
            .unwrap();
        assert_eq!(first.buckets.len(), 1);
        assert!(first.truncated, "a page with more results is truncated");
        let resume = first.next_start_after.clone().expect("a resume marker");
        let second = ops
            .list_buckets(params("", Some(&resume), 5), None, None)
            .await
            .unwrap();
        let names: Vec<&str> = second
            .buckets
            .iter()
            .map(|b| b.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["mid", "zeta"], "the marker is exclusive");

        let empty = ops
            .list_buckets(params("", None, 0), None, None)
            .await
            .unwrap();
        assert!(empty.buckets.is_empty() && !empty.truncated);
    }

    #[test]
    fn paginate_buckets_is_the_owner_filter_and_the_page_cut() {
        // The rule's one home, exercised as itself: the owner filter (the
        // EFFECTIVE owner — a candidate with no recorded row belongs to
        // `lazy_default`), the exclusive marker and the bounded page cut. Both
        // backends' bodies call this, so the rule is pinned where it lives
        // rather than only through one caller's rows.
        let owner = owner();
        let other = other_owner();
        let candidates = vec![
            (bucket::name("alpha").unwrap(), Some(owner.clone())),
            (bucket::name("beta").unwrap(), None),
            (bucket::name("gamma").unwrap(), Some(other.clone())),
            (bucket::name("delta").unwrap(), Some(other.clone())),
        ];
        let page = |owner: Option<&OwnerId>,
                    lazy_default: Option<&OwnerId>,
                    start_after: Option<&str>,
                    max: usize| {
            let (page, truncated, next) = paginate_buckets(
                candidates.clone(),
                &params("", start_after, max),
                owner,
                lazy_default,
                |name: &bucket::Name| name.as_ref().as_str(),
            );
            let names: Vec<String> = page.iter().map(ToString::to_string).collect();
            (names, truncated, next)
        };

        // The unfiltered listing: every candidate, in name order — the
        // candidate stream itself is unordered, and the page is the bounded
        // heap's sort.
        let (names, truncated, next) = page(None, None, None, 10);
        assert_eq!(names, ["alpha", "beta", "delta", "gamma"]);
        assert!(!truncated);
        assert_eq!(next, None);

        // The effective owner: beta's empty wire is `lazy_default`'s, and the
        // strict compare (no default) drops it.
        let (names, ..) = page(Some(&owner), Some(&owner), None, 10);
        assert_eq!(names, ["alpha", "beta"]);
        let (names, ..) = page(Some(&owner), None, None, 10);
        assert_eq!(names, ["alpha"]);
        let (names, ..) = page(Some(&other), None, None, 10);
        assert_eq!(names, ["delta", "gamma"]);

        // The page cut: the marker is exclusive, and it is the last served
        // name.
        let (names, truncated, next) = page(None, None, None, 1);
        assert_eq!(names, ["alpha"]);
        assert!(truncated);
        assert_eq!(next.as_deref(), Some("alpha"));
        let (names, truncated, _) = page(None, None, next.as_deref(), 10);
        assert_eq!(names, ["beta", "delta", "gamma"]);
        assert!(!truncated);

        // `max = 0` requests nothing: no page, no truncation, no marker.
        let (names, truncated, next) = page(None, None, None, 0);
        assert!(names.is_empty() && !truncated && next.is_none());
    }

    #[tokio::test]
    async fn delete_bucket_refuses_a_bucket_whose_only_content_is_a_folder_marker() {
        // A folder-marker style directory-only bucket: content the content
        // plane counts, with no metadata row naming it — a key scan of the
        // rows would call this bucket empty. The emptiness probe must not.
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        let marker = ContentKey::Object(object::key("dir/").unwrap());
        let mut staged = base.content.stage(&name, &marker).await.unwrap();
        staged.write(Bytes::from_static(b"")).await.unwrap();
        base.content.publish(&name, staged, &marker).await.unwrap();

        let ops = base.ops(BucketRow::Guaranteed);
        let err: StorageError = ops.delete_bucket(&name).await.unwrap_err();
        assert!(matches!(err, StorageError::NotEmpty(_)), "{err:?}");
        assert!(
            base.hooks().calls().is_empty(),
            "the probe refuses before the commit point"
        );
        assert!(base.meta.row(&name).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn delete_bucket_refuses_a_bucket_with_an_upload_in_progress() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        UploadMeta::create(
            &*base.meta,
            &name,
            "upload-1",
            &upload_row::Row {
                upload_id: "upload-1".to_owned(),
                key: object::key("big.bin").unwrap(),
                initiated_at: SystemTime::now(),
                checksum: None,
                tags: object::Tags::empty(),
                owner: None,
                acl: Acl::default_private(None),
            },
        )
        .await
        .unwrap();

        let ops = base.ops(BucketRow::Guaranteed);
        let err: StorageError = ops.delete_bucket(&name).await.unwrap_err();
        assert!(matches!(err, StorageError::NotEmpty(_)), "{err:?}");
        assert!(base.hooks().calls().is_empty());
    }

    /// One object row for the emptiness legs — a **delete marker's** shape
    /// (zero bytes behind it, no identity), without the recorded marker bit,
    /// which is the backend's own and never crosses this contract.
    fn object_row() -> meta_object::Row {
        meta_object::Row {
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            size: 0,
            mtime: SystemTime::now(),
            identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
        }
    }

    #[tokio::test]
    async fn delete_bucket_refuses_a_bucket_that_holds_only_an_object_row() {
        // The **composition's third probe**, pinned at the ops layer, outside
        // any single backend's own tests (dropping it reddens fs's legs too,
        // which reach the composition through `FsStorage::delete_bucket`):
        // an object row with no content behind it — a delete marker writes no
        // bytes, so the content plane reads the bucket empty and there is no
        // upload either. With `has_objects` dropped from `delete_bucket` this
        // bucket passes both remaining probes, the hook runs and the delete
        // answers `Ok`, which is exactly the fs↔pg divergence the shared
        // composition exists to close. The folder-marker leg beside this one
        // pins the content probe and the upload leg the upload probe, so the
        // three together pin all three.
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;
        ObjectMeta::put(
            &*base.meta,
            &name,
            &object::key("notes.txt").unwrap(),
            &object_row(),
        )
        .await
        .unwrap();
        assert!(
            base.content.is_empty(&name).await.unwrap(),
            "the row is the bucket's only state: the content plane holds nothing"
        );

        let ops = base.ops(BucketRow::Guaranteed);
        let err: StorageError = ops.delete_bucket(&name).await.unwrap_err();
        assert!(matches!(err, StorageError::NotEmpty(_)), "{err:?}");
        assert!(
            base.hooks().calls().is_empty(),
            "the row probe refuses before the commit point"
        );
        assert!(
            base.meta.row(&name).await.unwrap().is_some(),
            "the refused delete left the bucket row in place"
        );
    }

    #[tokio::test]
    async fn delete_bucket_calls_the_hook_once_after_the_probes() {
        let base = Base::new();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;

        base.ops(BucketRow::Guaranteed)
            .delete_bucket(&name)
            .await
            .unwrap();

        assert_eq!(
            base.hooks().calls(),
            vec![Call::BeforeBucketDelete("data".to_owned())],
            "the hook is the delete's one action"
        );
        assert!(
            base.meta.row(&name).await.unwrap().is_some(),
            "the engine leaves the whole teardown — the row included — to the hook"
        );
    }

    #[tokio::test]
    async fn a_refused_hook_is_the_operations_answer() {
        let base = Base::refusing();
        let name = bucket::name("data").unwrap();
        create(&base, BucketRow::Guaranteed, &name).await;

        let err: StorageError = base
            .ops(BucketRow::Guaranteed)
            .delete_bucket(&name)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::NotEmpty(_)), "{err:?}");
        assert_eq!(base.hooks().calls().len(), 1, "the hook ran once");
    }
}
