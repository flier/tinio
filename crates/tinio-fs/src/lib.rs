//! Filesystem backend for tinio.
//!
//! Implements the `tinio-core` `Storage` contract over the local filesystem:
//! buckets map to top-level subdirectories of the storage root, objects to
//! files. Private state lives in the reserved `<root>/.tinio/` directory
//! (meta store, buckets.json, multipart parts, temp files).
//!
//! Module layout is populated by the US1 tasks (path, write, meta, buckets,
//! listing, multipart, scanner, sweep, cleanup, backend/); nothing is public
//! yet.
