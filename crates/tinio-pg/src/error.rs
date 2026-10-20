//! The crate's error type: the one projection every failure crosses.

use tinio_core::storage;

/// What a `tinio-pg` operation failed with.
///
/// The store, the engine and the version plane all answer the contract's own
/// [`storage::Error`] already — the store's `Error` projects into it (host and database named,
/// the dsn redacted, PG5), and the other two are written in it — so this crate adds no arm of
/// its own: a pg failure is one of those, and the type exists because
/// [`Storage::Error`](tinio_core::storage::Storage::Error) must name the backend's own error
/// and every category method must answer it.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use tinio_core::storage;
/// use tinio_pg::Error;
///
/// // The projection is total: every failure arrives as the contract's own answer, and the
/// // backend's error unwraps back to it unchanged.
/// let refused = Error::from(storage::Error::NoSuchBucket(
///     tinio_core::bucket::name("data").expect("a valid name"),
/// ));
/// assert!(matches!(
///     storage::Error::from(refused),
///     storage::Error::NoSuchBucket(_)
/// ));
/// // A rendering carries no dsn: the store's own redaction is what composes it (PG5).
/// let io = Error::from(storage::Error::Io(io::Error::other(
///     "postgres connect to db.example/tinio",
/// )));
/// assert!(!io.to_string().contains("postgres://"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The store, the engine or the version plane refused.
    #[error(transparent)]
    Storage(#[from] storage::Error),
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(inner) => inner,
        }
    }
}

impl Error {
    /// Whether this is the read-only refusal: the store's `25006` projection, which the
    /// contract's own `is_read_only` already recognises.
    pub fn is_read_only(&self) -> bool {
        match self {
            Self::Storage(inner) => inner.is_read_only(),
        }
    }
}

impl From<tinio_store_pg::Error> for Error {
    /// The store's own error, projected the way the store projects it: its
    /// `From<Error> for storage::Error` is the one mapping (host and database named, the dsn
    /// redacted — PG5), so a `?` on a store call needs no arm of its own.
    fn from(err: tinio_store_pg::Error) -> Self {
        Self::Storage(storage::Error::from(err))
    }
}

#[cfg(feature = "versioning")]
impl From<tinio_version::Error> for Error {
    /// The version engine's own error, projected the way every other failure is: the plane's
    /// refusals carry the contract error it wrapped (a port's, a content plane's) and project
    /// onto it here, so a `?` on a body that answers the plane's error needs no arm of its own.
    fn from(err: tinio_version::Error) -> Self {
        Self::Storage(storage::Error::from(err))
    }
}
