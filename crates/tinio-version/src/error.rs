//! The engine's error type and its projection onto the contract error.

use std::io::Error as IoError;

use crate::_core::{object, storage};

/// A version-engine failure.
///
/// Every failure reaching a caller is a contract condition: a port or block
/// failure is projected onto the contract error [`storage::Error`], the type
/// the S3 layer classifies, and the supersede race projects onto an I/O error
/// whose kind is [`std::io::ErrorKind::Other`] and whose message names the
/// concurrent modification — never a not-found, which the layer would read as
/// a missing object.
///
/// # Examples
///
/// ```rust
/// use std::io::ErrorKind;
///
/// use tinio_core::{object, storage};
/// use tinio_version::Error;
///
/// let race = Error::SupersedeRace {
///     key: object::key("notes.txt").unwrap(),
/// };
/// let projected: storage::Error = race.into();
/// let storage::Error::Io(io) = projected else {
///     panic!("the race projects onto an I/O error");
/// };
/// assert_eq!(io.kind(), ErrorKind::Other, "never a not-found");
/// assert!(
///     io.to_string().contains("modified concurrently"),
///     "the message names the concurrent modification"
/// );
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The outgoing content changed under the supersede read: the version's
    /// gated current slot moved between the resolve and the read's end, so
    /// the bytes just stored are not the version the resolve named. Nothing
    /// is committed for the version — the row would stamp a version's
    /// identity onto content of unknown provenance.
    #[error("the content of `{key}` was modified concurrently while it was being superseded")]
    SupersedeRace {
        /// The key whose content changed.
        key: object::Key,
    },
    /// A port or block-engine failure, already projected onto the contract
    /// error.
    #[error(transparent)]
    Storage(#[from] storage::Error),
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Storage(err) => err,
            race @ Error::SupersedeRace { .. } => storage::io(IoError::other(race)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::*;
    use crate::_core::storage::Error as StorageError;

    #[test]
    fn a_supersede_race_projects_onto_an_other_io_error() {
        let race = Error::SupersedeRace {
            key: object::key("a.txt").unwrap(),
        };
        let message = race.to_string();
        let projected: StorageError = race.into();
        let StorageError::Io(io) = projected else {
            panic!("expected an I/O projection");
        };
        assert_eq!(io.kind(), ErrorKind::Other);
        assert_ne!(
            io.kind(),
            ErrorKind::NotFound,
            "never a not-found: the S3 layer would read it as a missing object"
        );
        assert_eq!(
            io.to_string(),
            "the content of `a.txt` was modified concurrently while it was being superseded"
        );
        assert_eq!(
            io.to_string(),
            message,
            "the projection carries the error's own message, never a rewritten one"
        );
        assert!(
            io.to_string().contains("a.txt"),
            "the message names the key whose content moved"
        );
        assert!(
            io.to_string().contains("modified concurrently"),
            "…and the concurrent modification itself"
        );
    }

    #[test]
    fn a_contract_error_passes_through_the_projection_unchanged() {
        let contract = storage::no_such_key(&object::key("missing.txt").unwrap());
        let expected = contract.to_string();
        let err: StorageError = Error::Storage(contract).into();
        assert_eq!(err.to_string(), expected);
    }
}
