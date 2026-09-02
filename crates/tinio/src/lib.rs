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

#[cfg(feature = "api")]
#[doc(hidden)]
pub extern crate tinio_api as _api;
#[doc(hidden)]
pub extern crate tinio_cli as _cli;
#[doc(hidden)]
pub extern crate tinio_config as _config;
#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_server as _server;

mod error;

#[cfg(feature = "api")]
pub use self::error::ApiError;
pub use self::error::{CliError, ConfigError, ServerError, StorageError};
pub use crate::{
    _config::Config,
    _core::{
        BodyStream, Bucket, BucketOps, ByteRange, ETag, MultipartOps, MultipartUpload, ObjectOps,
        PartInfo, Storage, bucket, cleanup, etag, object, storage,
    },
};

#[cfg(test)]
mod tests {
    //! The facade is the only public API surface — these tests pin the
    //! re-export seams so a refactor cannot silently drop them.
    use super::*;

    #[test]
    fn config_re_export_parses() {
        let config = Config::parse("version = 1").unwrap();
        assert_eq!(config.server.port, 9000);
    }

    #[test]
    fn storage_contract_types_are_re_exported() {
        // The checked constructors must stay reachable through the
        // facade, alongside the contract types.
        let name = bucket::name("data").unwrap();
        assert_eq!(name.to_string(), "data");
        let key = object::key("dir/file.txt").unwrap();
        assert_eq!(key.to_string(), "dir/file.txt");
        // The range and etag types are reachable and functional.
        assert_eq!(ByteRange::Inclusive(2, 5).resolve(10).unwrap(), (2, 5));
        // MD5("hello") = 5d41402abc4b2a76b9719d911017c592.
        assert_eq!(
            ETag::from_content(b"hello").as_str(),
            "5d41402abc4b2a76b9719d911017c592"
        );
    }
}
