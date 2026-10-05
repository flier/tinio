//! The fs side of `ListObjectVersions`.
//!
//! The engine owns the state machine; the bucket's *file* truth is the
//! backend's, so the version listing lives here: the walk over the bucket's
//! current objects (the `object_meta` rows, gated against the key path's file
//! so a delete marker is told from content) merged with each key's version
//! rows, newest first. Module `list` is the walk and the page; the tests
//! beside it drive the whole `VersionOps` surface.
//!
//! Why the object walk leads: `ListObjectVersions` lists every current
//! version, including one that was never overwritten and so has no history
//! row at all — the object store's truth is the only thing that guarantees
//! such a key appears. History rows add the noncurrent versions (and any key
//! whose current slot is gone).

pub(crate) mod list;

#[cfg(test)]
mod tests;
