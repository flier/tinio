//! Current-user-only Windows file DACLs.
//!
//! One writer for the security descriptor config files and the management plane's state file
//! both install. Named-pipe creation stays in `tinio-api`; it uses `CurrentUserDacl`.
//!
//! **The crate is also where the workspace's one ACE walk lives**: `walk_dacl` hands a
//! DACL's entries to a caller's own verdict, and that is how `tinio-sandbox-windows`'s ACL
//! whitelist reads a descriptor — by name for its oracle, by handle for its skip — without a
//! second copy of the Win32 sequence. The current-user read-back (`acl_is_current_user_only`)
//! is this crate's own use of the same walk.
//!
//! **The crate is also where the failure wording for a Win32 status lives**:
//! `win32_error_message` is the one sentence a failed call is reported with — the call's name,
//! the OS's message for the status and the decimal status itself — and `tinio-api`'s pipe
//! read-back wraps that sentence in its own `Error::State`, so the two crates' messages cannot
//! drift apart.
//!
//! **The manifest's `unsafe_code = "deny"` binds this file and every module a later change
//! adds; the one exception is `dacl`'s**, which is where the Win32 calls live and whose own
//! header states why. A crate-level `allow` — the spelling that stood here — lifted the lint
//! for the whole crate, so the manifest's `deny` had nothing left to enforce.

#[cfg(windows)]
mod dacl;
#[cfg(windows)]
pub use dacl::*;
