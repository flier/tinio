//! The fs-local `STATE` table and the fs-only strict object-meta walk.

use std::{ops::Deref, path::Path};

use super::error::{Error, corrupt_meta, unsupported_version};
use crate::{
    _core::{etag::ETag, object},
    _store::{meta, scan::for_each_pair, state, table::TableDef},
    bucket,
};

/// Strict per-bucket walk: a corrupt row fails with `CorruptMeta` (the
/// doctor must not skip them — meta.rs `Store::walk`), and the rows the
/// object-file orphan passes must never see are invisible: a recorded
/// delete marker's row has no file by design (the rule's home is
/// [`meta::orphan_candidate`]), so it is skipped after its key and etag
/// have been validated — a corrupt marker row still fails the walk. The
/// row's raw etag is decoded directly; the `meta::validate` self-heal is
/// not used here. Composes on the shared walk; the backend error
/// accumulates via the derived `Redb` `#[from]`.
pub(crate) fn for_bucket_strict(
    table: &meta::Table<
        '_,
        impl redb::ReadableTable<<meta::Def as TableDef>::Key, <meta::Def as TableDef>::Value>,
    >,
    bucket: &bucket::Name,
    mut visit: impl FnMut(object::Key, ETag, u64, u64) -> Result<(), Error>,
) -> Result<(), Error> {
    let bucket = &**bucket;
    for_each_pair(
        table.deref(),
        (bucket, ""),
        |b, _| b == bucket,
        |_, raw_key, (etag, size, mtime, _, _, _, _, _, _, _, is_delete_marker)| {
            let key = object::key(raw_key).map_err(|err| corrupt_meta(raw_key, err))?;
            let etag = ETag::new(etag).map_err(|err| corrupt_meta(raw_key, err))?;
            if !meta::orphan_candidate(is_delete_marker) {
                return Ok(()); // a current delete marker is no orphan candidate
            }
            visit(key, etag, size, mtime)
        },
    )
}

/// Check the format version: write `state::FORMAT_VERSION` on first open,
/// reject ANY mismatch (one current version — F06; no migration). The
/// mismatch error is fs-specific (`UnsupportedVersion` carries the state
/// file path) — the shared state module stays error-neutral.
pub(crate) fn ensure_version<'txn>(
    state: &mut state::Table<'txn>,
    path: &Path,
) -> Result<u64, Error> {
    match state.version()? {
        None => {
            state.write_version(state::FORMAT_VERSION)?;
            Ok(state::FORMAT_VERSION)
        }
        Some(found) if found == state::FORMAT_VERSION => Ok(found),
        Some(found) => Err(unsupported_version(path, found, state::FORMAT_VERSION)),
    }
}
