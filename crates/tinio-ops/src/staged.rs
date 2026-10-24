//! The staged body: what a write streams into before it is published.
//!
//! [`StagedBody`] is the engine's `ObjectOps::StagedBody` (spec E8): the
//! content plane's own staged handle ([`StagedObject`]) beside everything the
//! later commit needs and nothing else — the key the bytes are headed for, the
//! number of bytes that landed, the content ETag computed while they streamed,
//! and the stage's recorded checksum. The value is the engine's, so
//! `stage_body` and `commit_object` are engine-owned on every backend and a
//! backend spells `ObjectOps::StagedBody = tinio_ops::StagedBody<Self::Staged>`.
//!
//! Dropping a staged body that was never committed publishes nothing: the
//! staged handle is dropped with it, and the stage's own teardown (a temp file
//! removed, a buffer released) is the content plane's.
//!
//! # Examples
//!
//! The stage/commit pair over the in-memory reference planes — the shape every
//! backend wires:
//!
//! ```rust
//! use futures::stream;
//! use tinio_core::{acl::Acl, bucket, object};
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
//! let meta = std::sync::Arc::new(RedbMetadata::in_memory().unwrap());
//! let content = std::sync::Arc::new(RedbContent::from_driver(meta.driver()));
//! let buckets = BucketOps {
//!     meta: std::sync::Arc::clone(&meta),
//!     content: std::sync::Arc::clone(&content),
//!     hooks: NoHooks,
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//! let objects = ObjectOps {
//!     meta: std::sync::Arc::clone(&meta),
//!     content,
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//!
//! let name = bucket::name("data").unwrap();
//! buckets
//!     .create_bucket(&name, None, &Acl::default_private(None))
//!     .await
//!     .unwrap();
//!
//! let key = object::key("notes.txt").unwrap();
//! let body = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"hello"))]));
//! let staged = objects
//!     .stage_body(&name, &key, body, None, None)
//!     .await
//!     .unwrap();
//! assert_eq!(staged.len(), 5, "the staged body counted what landed");
//! let info = objects
//!     .commit_object(
//!         &name,
//!         &key,
//!         staged,
//!         object::Tags::empty(),
//!         None,
//!         &Acl::default_private(None),
//!         None,
//!     )
//!     .await
//!     .unwrap();
//! assert_eq!(info.etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
//! # }
//! ```

use tinio_core::{ETag, checksum, content::StagedObject, object};

/// A staged object body: the content plane's staged handle, the key it is
/// headed for, and what the stream left behind.
///
/// # Examples
///
/// ```rust
/// use futures::stream;
/// use tinio_core::{acl::Acl, bucket, object};
/// use tinio_ops::{BucketOps, BucketRow, MetadataFailure, ObjectOps};
/// use tinio_store_redb::{RedbContent, RedbMetadata};
///
/// # struct NoHooks;
/// # #[async_trait::async_trait]
/// # impl tinio_ops::Hooks for NoHooks {
/// #     async fn before_bucket_delete(
/// #         &self,
/// #         _bucket: &bucket::Name,
/// #     ) -> Result<(), tinio_core::storage::Error> {
/// #         Ok(())
/// #     }
/// #     async fn lock_part(
/// #         &self,
/// #         _bucket: &bucket::Name,
/// #         _upload_id: &str,
/// #     ) -> Box<dyn tinio_ops::Guard> {
/// #         Box::new(tinio_ops::NoGuard)
/// #     }
/// # }
/// # #[tokio::main]
/// # async fn main() {
/// let meta = std::sync::Arc::new(RedbMetadata::in_memory().unwrap());
/// let content = std::sync::Arc::new(RedbContent::from_driver(meta.driver()));
/// let buckets = BucketOps {
///     meta: std::sync::Arc::clone(&meta),
///     content: std::sync::Arc::clone(&content),
///     hooks: NoHooks,
///     metadata_failure: MetadataFailure::Fail,
///     bucket_row: BucketRow::Guaranteed,
/// };
/// let objects = ObjectOps {
///     meta: std::sync::Arc::clone(&meta),
///     content,
///     metadata_failure: MetadataFailure::Fail,
///     bucket_row: BucketRow::Guaranteed,
/// };
/// let name = bucket::name("data").unwrap();
/// buckets
///     .create_bucket(&name, None, &Acl::default_private(None))
///     .await
///     .unwrap();
///
/// let key = object::key("a.txt").unwrap();
/// let body = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"abc"))]));
/// let staged = objects
///     .stage_body(&name, &key, body, None, None)
///     .await
///     .unwrap();
/// assert_eq!(staged.key(), &key);
/// assert_eq!(staged.len(), 3);
/// assert!(!staged.is_empty());
/// assert_eq!(staged.etag().as_str(), "900150983cd24fb0d6963f7d28e17f72");
/// assert_eq!(staged.checksum(), None, "no tee, no carried value");
/// # }
/// ```
#[derive(Debug)]
pub struct StagedBody<S> {
    /// The content plane's staged handle, consumed by the publish.
    staged: S,
    /// The key the staged bytes are headed for.
    key: object::Key,
    /// The number of bytes the stream landed in the stage.
    len: u64,
    /// The content ETag computed while the bytes streamed.
    etag: ETag,
    /// The stage's recorded checksum: the caller's carried value when one
    /// was given, else the tee's streamed digest as the write path's
    /// FULL_OBJECT kind — `None` when the stage fixed neither.
    checksum: Option<checksum::Recorded>,
}

impl<S: StagedObject> StagedBody<S> {
    /// Build a staged body from its parts — the engine's own constructor,
    /// called by [`ObjectOps::stage_body`](crate::ObjectOps::stage_body).
    pub(crate) fn new(
        staged: S,
        key: object::Key,
        len: u64,
        etag: ETag,
        checksum: Option<checksum::Recorded>,
    ) -> Self {
        Self {
            staged,
            key,
            len,
            etag,
            checksum,
        }
    }

    /// The key the staged bytes are headed for.
    pub fn key(&self) -> &object::Key {
        &self.key
    }

    /// The number of bytes the stream landed in the stage — the bytes the
    /// publish then makes visible, which is what the commit records.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the stream landed no bytes (an empty object).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The content ETag computed while the bytes streamed.
    pub fn etag(&self) -> &ETag {
        &self.etag
    }

    /// The content plane's staged handle, still owned by this body.
    pub fn staged(&self) -> &S {
        &self.staged
    }

    /// The stage's recorded checksum, when the stage fixed one: the explicit
    /// carried value takes precedence over the tee's streamed digest.
    pub fn checksum(&self) -> Option<&checksum::Recorded> {
        self.checksum.as_ref()
    }

    /// Take the staged body apart for the commit: the content handle, the
    /// key, the landed byte count, the ETag and the recorded checksum.
    pub(crate) fn into_parts(self) -> (S, object::Key, u64, ETag, Option<checksum::Recorded>) {
        (self.staged, self.key, self.len, self.etag, self.checksum)
    }
}
