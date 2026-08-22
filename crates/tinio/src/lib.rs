//! Tinio: an S3-compatible local storage server.
//!
//! This facade crate is the only public API surface of the project (the
//! semver-checks target and rustdoc-example contract, per the constitution).
//! It curates re-exports from the implementation crates — the storage
//! contract from tinio-core, the configuration type from tinio-config, the
//! S3 compatibility layer from tinio-server, the management plane from
//! tinio-api, and the CLI entry from tinio-cli.
//!
//! # Examples
//!
//! ```rust
//! use tinio::{Config, Storage};
//!
//! // The configuration schema and the storage contract are the public
//! // extension seams.
//! let config = Config::parse("version = 1").unwrap();
//! assert_eq!(config.server.port, 9000);
//!
//! fn accepts_any_backend<S: Storage>(backend: &S) -> &S {
//!     backend
//! }
//! ```

mod error;

pub use tinio_config::Config;
pub use tinio_core::{
    BodyStream, Bucket, BucketOps, ByteRange, ETag, MultipartOps, MultipartUpload, ObjectOps,
    PartInfo, Storage, bucket, cleanup, etag, object, storage,
};

#[cfg(feature = "api")]
pub use self::error::ApiError;
pub use self::error::{CliError, ConfigError, ServerError, StorageError};
