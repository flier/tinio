//! The version-scoped operations of the storage contract.

use async_trait::async_trait;

use crate::{
    ByteRange, acl, bucket, object,
    storage::{self, Storage},
    version::{self, CurrentVersion, Id, ListParams, Listing, Marker, State},
};

/// The version-scoped operations. Backends that do not version anything answer
/// [`State::Unversioned`] from [`VersionOps::bucket_versioning`] and refuse the
/// rest with `Error::AccessDenied`-class errors rather than pretending — every
/// method has a default body that answers exactly that, so a backend that does
/// not version anything implements nothing (its `impl VersionOps` may be empty,
/// or absent where the feature that compiles its versioning machinery is off).
/// The defaults build `<Self as Storage>::Error` through the
/// [`storage::Error`]-`From` bound [`Storage::Error`] carries.
///
/// # Examples
///
/// A backend that does not version anything keeps the default
/// [`VersionOps::bucket_versioning`] (answering [`State::Unversioned`]) and
/// refuses the other nine operations with an access-denied-class error — the
/// shape `tinio_mem::MemoryStorage` implements:
///
/// ```ignore
/// use tinio_core::{bucket, version};
/// use tinio_mem::MemoryStorage;
/// use tokio::runtime::Runtime;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let state = Runtime::new()
///     .unwrap()
///     .block_on(storage.bucket_versioning(&bucket))
///     .unwrap();
/// assert_eq!(state, version::State::Unversioned);
/// ```
///
/// (`ignore`, the same reason [`Storage`](crate::storage::Storage)'s own trait
/// example records: a runnable example would have to dev-depend on a backend
/// crate, and the dev-dependency direction runs from the backends to this one,
/// so that would be a cycle; the default's behaviour is pinned by the
/// conformance harness instead.)
#[async_trait]
pub trait VersionOps: Send + Sync + 'static {
    /// The bucket's versioning state.
    async fn bucket_versioning(
        &self,
        bucket: &bucket::Name,
    ) -> Result<State, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = bucket;
        Ok(State::Unversioned)
    }

    /// Set the bucket's versioning state.
    async fn set_bucket_versioning(
        &self,
        bucket: &bucket::Name,
        state: State,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = state;
        Err(bucket_refusal(bucket))
    }

    /// The rename guard: `Ok(())` on a bucket that records no versions, and
    /// the contract's `AccessDenied` class naming `dst` on one that does —
    /// [`rename_allowed`] applied to the state
    /// [`VersionOps::bucket_versioning`] reads.
    ///
    /// [`ObjectOps::rename_object`](crate::storage::ObjectOps::rename_object)
    /// on a versioned bucket is refused because the move has no versioning
    /// semantics: it would replace the destination's current version without
    /// superseding it and strand the source key's history. Every backend's
    /// `rename_object` must call this before it moves anything; the default
    /// body owns the rule, so the call is the whole of the backend's policy
    /// and cannot spell it wrong. The conformance suite's versioning leg pins
    /// the refusal.
    async fn ensure_rename_allowed(
        &self,
        bucket: &bucket::Name,
        dst: &object::Key,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        rename_allowed(self.bucket_versioning(bucket).await?, dst)
    }

    /// Read a specific version (`None` = the `null` version) of an object.
    ///
    /// A delete marker is a successful outcome, not an error: the caller needs
    /// the marker's identity to render its own answer (plan 3 maps it to the
    /// `404`/`405` wire rules).
    async fn get_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        range: Option<ByteRange>,
    ) -> Result<version::ReadResult, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version, range);
        Err(refusal(key))
    }

    /// The metadata of a specific version, with the same marker rule.
    async fn head_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<version::HeadResult, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version);
        Err(refusal(key))
    }

    /// Permanently remove one version and answer what was removed.
    async fn delete_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Marker, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version);
        Err(refusal(key))
    }

    /// List versions, newest first within each key.
    async fn list_object_versions(
        &self,
        bucket: &bucket::Name,
        params: ListParams,
    ) -> Result<Listing, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = params;
        Err(bucket_refusal(bucket))
    }

    /// The shape of one key's current version — the bounded per-key
    /// probe behind the plain read's 404 delete-marker disclosure: the
    /// plain storage reads see no marker (a marked key reads as
    /// `NoSuchKey`), so the caller asks this instead of listing the
    /// bucket. Exact-key reads only (one row, or one row range), never a
    /// listing page; [`CurrentVersion::Absent`] when the key has no
    /// current version. A backend that versions nothing keeps the
    /// default: a bucket that records no versions can hide no marker.
    async fn current_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<CurrentVersion, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, key);
        Ok(CurrentVersion::Absent)
    }

    /// The tags of a specific version (`None` = the `null` version).
    async fn version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object::Tags, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version);
        Err(refusal(key))
    }

    /// Replace a specific version's tags.
    async fn set_version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        tags: object::Tags,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version, tags);
        Err(refusal(key))
    }

    /// The ACL of a specific version.
    async fn version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<acl::Acl, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version);
        Err(refusal(key))
    }

    /// Replace a specific version's ACL.
    async fn set_version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        acl: &acl::Acl,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = (bucket, version, acl);
        Err(refusal(key))
    }
}

/// The rename rule the contract states for a bucket that records versions:
/// `Ok(())` when `state` records none, and the contract's `AccessDenied`
/// class naming `dst` otherwise. A pure function because two callers apply it
/// in different shapes — [`VersionOps::ensure_rename_allowed`] on a state it
/// reads itself, and a backend that must test the state inside the
/// transaction that moves the rows (`tinio-mem`'s rename, whose guard cannot
/// await) — and the rule has one home, so neither caller can apply a
/// different one.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{object, storage, version::State};
///
/// let dst = object::key("dst.txt").unwrap();
/// let allowed: Result<(), storage::Error> =
///     storage::rename_allowed(State::Unversioned, &dst);
/// assert!(
///     allowed.is_ok(),
///     "a bucket that records no versions moves the object as it always has"
/// );
///
/// let refused: Result<(), storage::Error> =
///     storage::rename_allowed(State::Suspended, &dst);
/// let err = refused.expect_err("a bucket that records versions refuses the move");
/// assert!(
///     matches!(
///         err,
///         storage::Error::AccessDenied(storage::Denied::Key(ref named)) if named == &dst
///     ),
///     "the refusal names the destination key: {err}"
/// );
/// ```
pub fn rename_allowed<E: From<storage::Error>>(state: State, dst: &object::Key) -> Result<(), E> {
    match state {
        State::Unversioned => Ok(()),
        State::Enabled | State::Suspended => Err(refusal(dst)),
    }
}

/// The rule a metadata operation addressed at a delete marker answers: a
/// marker holds no tags and no ACL — it is a row that records a deletion,
/// not content — so the operation answers the contract's not-found class
/// naming the marker's identity, exactly what an unknown version answers.
/// A pure function beside [`rename_allowed`] because both backends apply it
/// in their own error type (the [`Storage::Error`] `From` bound builds it),
/// and the rule has one home so the two can never answer different classes;
/// the conformance suite's versioning leg pins the shared answer.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{storage, version::Id};
///
/// let id = Id::new("abc-123").unwrap();
/// let err: storage::Error = storage::marker_has_no_metadata(Some(&id));
/// assert!(
///     matches!(err, storage::Error::NoSuchVersion(ref named) if named == "abc-123"),
///     "the refusal names the marker's identity: {err}"
/// );
/// ```
pub fn marker_has_no_metadata<E: From<storage::Error>>(version: Option<&Id>) -> E {
    storage::no_such_version(version::wire_of(version)).into()
}

/// The refusal a backend that does not version answers for a key-scoped
/// version operation: the contract's `AccessDenied` class, in whatever error
/// type the backend declares (the [`Storage::Error`] `From` bound builds it),
/// naming the key the operation named.
fn refusal<E: From<storage::Error>>(key: &object::Key) -> E {
    storage::access_denied(key).into()
}

/// The bucket-scoped twin of [`refusal`]: the operation named a bucket and no
/// key, so the refusal names the bucket — never a fabricated key (the
/// reserved `.tinio` segment a key-shaped payload would have needed was a lie
/// about what was refused). The class is the same `AccessDenied` the key-scoped
/// refusals answer, so the wire layer maps both identically.
fn bucket_refusal<E: From<storage::Error>>(bucket: &bucket::Name) -> E {
    storage::access_denied_bucket(bucket).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusal the default bodies answer: the access-denied class, naming
    /// the key for the key-scoped operations and the **bucket** for the
    /// bucket-scoped ones — which name no object, so the payload is never a
    /// fabricated key.
    #[test]
    fn the_default_refusal_is_the_access_denied_class() {
        let key = object::key("notes.txt").unwrap();
        let refused: storage::Error = refusal(&key);
        assert!(
            matches!(
                refused,
                storage::Error::AccessDenied(storage::Denied::Key(ref named)) if named == &key
            ),
            "a key-scoped version operation is refused with the key it named: {refused}"
        );
        let bucket = bucket::name("data").unwrap();
        let refused: storage::Error = bucket_refusal(&bucket);
        assert!(
            matches!(
                refused,
                storage::Error::AccessDenied(storage::Denied::Bucket(ref named)) if named == &bucket
            ),
            "a bucket-scoped version operation is refused with the bucket it named: {refused}"
        );
    }
}
