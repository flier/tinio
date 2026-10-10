//! The group receivers and the two policies.
//!
//! The engine's bodies are methods on a **receiver per operation group**, and
//! each receiver carries only the handles that group uses: the bucket group
//! needs the row plane and the content plane, the version-touching bodies need
//! the version plane as well, and a backend without a block plane (memory)
//! builds no [`VersionPlane`] and takes no body that needs one.
//!
//! The two policies are fields of the receivers, declared by the backend and
//! read by the bodies — never inferred: [`MetadataFailure`] says what a
//! metadata row write that fails after the content is published means, and
//! [`BucketRow`] says whether the backend's bucket operations require the
//! bucket's row to exist.

use std::sync::Arc;

use crate::{_blob::Store, _core::storage::Error};

/// The engine's one rule for a **read** that wanted to write: a store that
/// refused because it is read-only serves the read anyway; every other failure
/// propagates.
///
/// FR-023 amended ("Read paths"): a read over a read-only store degrades — the
/// value it would have recorded is served, never persisted — and the
/// degradation is **observable** (one event per read) rather than silent.
///
/// The two arms answer what each of them knows. A write that **landed** answers
/// its own value, never the caller's candidate: the store's answer is the
/// truth (a first-sight write answers the *stored* creation time, so two
/// readers that raced a third writer's insert still agree). A write the store
/// **refused** answers `value` — the value the caller would have recorded and
/// now serves instead — because there is no stored answer to read back.
///
/// The rule is shared by every write-on-read path the engine owns — the lazy
/// bucket-row first sight here, the object read-heal next — so no body grows its
/// own reading of the store's refusal ([`Error::is_read_only`] is the
/// question).
pub(crate) fn served_without_writing<T>(
    result: Result<T, Error>,
    value: T,
    what: &str,
) -> Result<T, Error> {
    match result {
        Ok(recorded) => Ok(recorded),
        Err(err) if err.is_read_only() => {
            tracing::info!(
                what,
                "the store is read-only: the read is served without writing it back"
            );
            Ok(value)
        }
        Err(err) => Err(err),
    }
}

/// The one conversion the engine's bodies perform on a contract failure.
///
/// Every contract method answers its implementation's error, which each
/// contract requires to convert into [`storage::Error`]; the engine answers
/// with the layering error itself, so the projection happens once, here. It is
/// a function rather than `?` because an `Into` bound is not a `From` bound:
/// `?` cannot apply the contract's own guarantee.
pub(crate) fn projected(err: impl Into<Error>) -> Error {
    err.into()
}

/// What an operation body needs: the row plane, the content plane, the hooks
/// and the two policies.
///
/// The bucket group's bodies (in [`crate::bucket`]) are this receiver's; a
/// backend builds one over its own handles and calls them from its `Storage`
/// implementation.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
///
/// use tinio_ops::{BucketOps, BucketRow, MetadataFailure};
///
/// let buckets = BucketOps {
///     meta: Arc::clone(&rows),
///     content: Arc::clone(&content),
///     hooks: hooks.clone(),
///     metadata_failure: MetadataFailure::Fail,
///     bucket_row: BucketRow::Guaranteed,
/// };
/// buckets.create_bucket(&name, owner, acl).await?;
/// ```
pub struct BucketOps<M, C, H> {
    /// The row plane: bucket, object and upload rows (`Metadata`).
    pub meta: Arc<M>,
    /// The content plane: object and part bytes, and the bucket namespace.
    pub content: Arc<C>,
    /// The backend's seam ([`Hooks`](crate::hooks::Hooks)).
    pub hooks: H,
    /// What a metadata-row write that fails after the content is published
    /// means ([`MetadataFailure`]).
    pub metadata_failure: MetadataFailure,
    /// Whether the backend's buckets require their row ([`BucketRow`]).
    pub bucket_row: BucketRow,
}

/// The version plane: the handles only the version-touching bodies take.
///
/// A backend without a block plane builds none — `tinio-mem`'s version group
/// and its version-creating writes are one transaction by ruling, so it takes
/// no body that needs this value and its `Storage` implementation supplies
/// none. `tinio-fs` and `tinio-pg` carry it.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
///
/// use tinio_blob::{LocalBlocks, Store};
/// use tinio_ops::VersionPlane;
/// use tinio_version::Versions;
///
/// let plane = VersionPlane {
///     versions: Arc::new(Versions::new(rows.clone(), content.clone(), Store::new(LocalBlocks::new(dir)))),
///     version_rows: Arc::clone(&rows),
///     blobs: Store::new(LocalBlocks::new(dir)),
/// };
/// ```
pub struct VersionPlane<V, R, B> {
    /// `tinio-version`'s state engine (`Versions` over the backend's ports).
    pub versions: Arc<V>,
    /// The version rows (`VersionMeta`), beside the state engine.
    pub version_rows: Arc<R>,
    /// The shared block engine over this backend's block store.
    pub blobs: Store<B>,
}

/// What a metadata-row write that fails after the content is published means.
///
/// Declared by the backend, never inferred: the same failure is a warning on
/// one store (the object is committed; the row is recomputed on the next read)
/// and the caller's compensation on another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFailure {
    /// The write is best-effort: the object is committed, the failure is
    /// logged, and the next read heals the row.
    Warn,
    /// The write is the operation's answer: the engine compensates — it
    /// publishes through the content plane's tagged form and takes its own
    /// publish back with a compare-before-remove when the row write is
    /// refused, so a newer writer's bytes (identical ones included) are never
    /// taken. A backend that cannot take its bytes back leaves the content to
    /// `Cleanup` (pg).
    Fail,
}

/// Whether a backend's bucket operations require the bucket's row to exist.
///
/// Declared by the backend, never inferred. A row-backed store answers
/// `NoSuchBucket` from a missing row (`Guaranteed` — the row **is** the
/// bucket); a store whose buckets outlive their rows (a directory that was
/// never sighted through the API) serves the empty defaults and records the
/// first sight (`Lazy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketRow {
    /// The row must exist: a missing row is `NoSuchBucket`.
    Guaranteed,
    /// Rows are recorded lazily: a missing row is a bucket that was never
    /// sighted, so reads serve the empty default and the recording write
    /// inserts it.
    Lazy,
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    /// The read-on-a-read-only-store rule answers what each arm knows: a write
    /// that landed answers the store's own value (never the caller's
    /// candidate — a first-sight write answers the **stored** value, which a
    /// concurrent writer may have made different), the read-only refusal
    /// serves the candidate, and every other failure propagates.
    #[test]
    fn the_read_path_rule_answers_what_each_arm_knows() {
        assert!(
            matches!(served_without_writing(Ok(7), 3, "probe"), Ok(7)),
            "a landed write answers the store's value, not the candidate"
        );

        let refused = Error::from(io::Error::from(io::ErrorKind::ReadOnlyFilesystem));
        assert!(
            matches!(served_without_writing(Err(refused), 3, "probe"), Ok(3)),
            "the refusal serves the candidate it could not record"
        );

        let failed = Error::from(io::Error::other("boom"));
        let answered = served_without_writing::<u32>(Err(failed), 3, "probe");
        assert!(
            matches!(answered, Err(Error::Io(_))),
            "a real fault is never hidden by a read: {answered:?}"
        );
    }
}
