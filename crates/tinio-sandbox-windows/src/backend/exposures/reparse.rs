//! Reparse-point refusals. A grant through a junction or a symlink lands on the target.

use std::{fs, os::windows::fs::MetadataExt, path::Path};

use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

use crate::{
    _core::{Result, launch},
    acl::reparse_refusal,
};

/// Refuse `path` when it, or any directory above it, is a reparse point.
///
/// The whole chain rather than the last component alone: `E:\link\tinio` names a
/// directory whose *parent* is a junction, and a grant on it lands wherever that
/// junction points. `symlink_metadata` is the call that reports on the link
/// itself rather than on its target, which is the property this check needs and
/// the reason a plain `metadata` would not do.
///
/// **The chain is the half `acl::Object` cannot cover**, and that is why this
/// check is still here beside it: the handle check refuses the object the *last*
/// component names, while a link above it is followed by every open there is —
/// measured on this host 2026-09-27, `icacls /grant:r` on `…\hop\tinio`, where
/// `hop` is a junction, wrote the ACE on `…\real\tinio`, outside the tree the
/// caller spelled.
///
/// **The refusal is [`reparse_refusal`]'s, shared with the sibling check and with
/// the grant's own handle check** so the sites cannot come to two spellings of one
/// rule. Its event is emitted there rather than through `launch`, which builds
/// [`Error::Launch`] and would put a second line in the log for one failure.
///
/// # Errors
///
/// [`Error::BadPath`] naming the component that is a reparse point.
///
/// [`Error::Launch`]: crate::_core::Error::Launch
/// [`Error::BadPath`]: crate::_core::Error::BadPath
pub(super) fn refuse_reparse_points(path: &Path) -> Result<()> {
    for component in path.ancestors() {
        // A component that cannot be read is not this check's business: the
        // resolution that follows reports it, with the OS error attached.
        let Ok(metadata) = fs::symlink_metadata(component) else {
            continue;
        };
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(reparse_refusal(component, "resolve an exposed path"));
        }
    }
    Ok(())
}

/// Refuse a sibling DLL that is **itself** a reparse point.
///
/// **The sibling half of [`refuse_reparse_points`], with the walk it does not
/// need.** A sibling lies directly in the program's directory, and that directory
/// is resolved — and so walked, ancestors included — by [`canonical_whitelist`](super::canonical_whitelist)
/// on
/// the same pass; what the resolution does *not* cover is the sibling entries
/// themselves, which are enumerated by **name** and each granted by handle. The one
/// component the rule still owes is therefore the DLL's own, and one
/// `symlink_metadata` per DLL answers it: **an attribute read, and no
/// `fs::canonicalize`** — the cost that took the DLLs out of the resolution
/// pipeline in the first place.
///
/// **The refusal is fail-closed and it cannot leave a DLL ungranted**: what it
/// stops is the launch, and what it prevents is the grant the rule exists for —
/// `icacls` followed a file symlink named `x.dll` and wrote the ACE on its
/// **target**, outside the program's directory, as the batch grant's own
/// measurement records (`acl`'s module doc). Nothing is dropped from
/// [`SiblingScan::files`](crate::backend::SiblingScan::files), so a refused whitelist is never a whitelist missing a
/// DLL.
///
/// **A stat that fails is a refusal**, which is what the route this replaced did:
/// `canonical_host` propagated the resolution's error, and a `symlink_metadata`
/// that fails here would otherwise read as "not a reparse point" — the one answer
/// this check must never invent, because it is the answer that grants through
/// whatever is really there. The error is [`Error::Launch`] with the OS code,
/// under the step a resolution that failed reports under.
///
/// # Errors
///
/// [`Error::BadPath`], in [`reparse_refusal`]'s shape, naming the sibling path;
/// [`Error::Launch`] when the sibling cannot be stat'd at all.
///
/// [`Error::Launch`]: crate::_core::Error::Launch
/// [`Error::BadPath`]: crate::_core::Error::BadPath
pub(super) fn refuse_a_reparse_sibling(dll: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(dll)
        .map_err(|source| launch("resolve an exposed path", dll, source))?;
    match metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT {
        0 => Ok(()),
        _ => Err(reparse_refusal(dll, "resolve an exposed path")),
    }
}
