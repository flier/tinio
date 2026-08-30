//! Facade error re-exports.
//!
//! The facade exposes exactly one public error type per implementation crate
//! (per plan.md Project Structure). The conversion chain runs one way —
//! backend → core → S3 error codes → HTTP statuses → CLI exit codes — so no
//! crate leaks another crate's error type.
//!
//! # Examples
//!
//! ```rust
//! use tinio::{StorageError, storage::Error::*};
//!
//! let err: StorageError = NoSuchBucket("data".into());
//! assert_eq!(err.to_string(), "no such bucket: `data`");
//! ```

/// The management-plane error (tinio-api, feature `api`).
#[cfg(feature = "api")]
pub use crate::_api::Error as ApiError;
/// The CLI error (tinio-cli).
pub use crate::_cli::Error as CliError;
/// The configuration error (tinio-config).
pub use crate::_config::Error as ConfigError;
/// The storage-contract error (tinio-core).
pub use crate::_core::storage::Error as StorageError;
/// The server error (tinio-server).
pub use crate::_server::Error as ServerError;
