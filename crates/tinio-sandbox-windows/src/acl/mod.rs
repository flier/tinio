//! ACL whitelist: grant the package SID an access mode on each exposed path, and
//! leave those grants standing — the launcher takes nothing back on the way out
//! (decisions 15, 21, §6).
//!
//! **The whitelist is two jobs, and this directory holds both**: it writes the package SID's ACE
//! on each exposed path — `grant`'s, in the shapes `spec` spells and `descriptor` writes through
//! a handle it opened itself — and it reads a descriptor back to decide whether a grant is
//! already satisfied, so a satisfied path costs one read and no write. Which paths are granted
//! and what each shape reaches is `grant`'s to state.

mod descriptor;
mod grant;
mod icacls;
mod lock;
mod refusal;
mod spec;

// **The workspace's one ACE traversal lives in its own module, and its three names stay
// crate-private** (D9): the walk, `Ace`'s three cases and the ACE type a fixture builds all
// reached their callers through `tinio-winsec` before that crate dissolved, and the old file
// imported those three **privately**. Nothing outside this crate named them then and nothing
// does now, so the facade re-exports none of them — the one reader that used the facade, the
// spec table's test, reaches `walk` directly. The examples that would have forced a wider
// surface are unit tests beside the type.
mod walk;
pub use grant::{Grants, revoke};
// **The test oracles behind the crate's `testing` feature** (D9's facade, narrowed): `grant`
// and `has_ace` are what a leg or a doctest asks the ACL layer, and a shipped launcher never
// calls either — so they ride the same gate as `crate::testing` rather than claiming runtime
// paths that do not exist.
#[cfg(any(test, feature = "testing"))]
pub use grant::{grant, has_ace};
pub use icacls::icacls_binary;
#[cfg(any(test, feature = "testing"))]
pub use icacls::mentions_sid;
// **The refusal the whitelist's own spelled-path walk reports**, re-exported at the crate's
// request because `backend`'s reparse check meets the same rule the grant's handle check does.
pub(crate) use refusal::reparse_refusal;
pub use spec::Target;
#[cfg(any(test, feature = "testing"))]
pub use spec::{ace_spec, grant_spec};

#[cfg(test)]
mod test_support;
