//! Current-user-only Windows file DACLs.
//!
//! One writer for the security descriptor config files and the management plane's state file
//! both install. Named-pipe creation stays in `tinio-api`; it uses [`CurrentUserDacl`].

#![cfg_attr(windows, allow(unsafe_code))]

#[cfg(windows)]
mod imp;
#[cfg(windows)]
pub use imp::*;
