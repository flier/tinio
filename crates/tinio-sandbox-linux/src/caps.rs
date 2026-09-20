//! Which capabilities the confined process keeps.

use rustix::thread::CapabilitySet;

use crate::_core::AccessMode;

/// A capability the confinement may retain.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Cap;
///
/// // The name is the kernel's, and the set is what the drop hands it.
/// assert_eq!(Cap::Chown.name(), "CAP_CHOWN");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cap {
    /// `CAP_CHOWN` — needed only by FR-033's `local_uid` chown on newly
    /// written objects.
    Chown,
}

impl Cap {
    /// The kernel's name for it.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Cap;
    ///
    /// assert_eq!(Cap::Chown.name(), "CAP_CHOWN");
    /// ```
    pub const fn name(self) -> &'static str {
        match self {
            Self::Chown => "CAP_CHOWN",
        }
    }

    /// The `rustix` bit for it: a single-bit [`CapabilitySet`].
    ///
    /// **Not `rustix::thread::Capability`**, which is what the obvious spelling
    /// would be. The type carries `#[deprecated(since = "1.1.0", note = "Use
    /// CapabilitySet with a single bit instead")]`, and `rustix::thread` *does*
    /// re-export it — the module is `mod prctl;` plus `pub use prctl::*;`, so it
    /// is the deep `rustix::thread::prctl::Capability` path that fails to
    /// resolve and the short one that compiles. Naming it therefore buys a
    /// deprecation warning under `-D warnings`, which is the workspace's gate,
    /// for nothing: `remove_capability_from_bounding_set` takes `impl
    /// CompatCapability`, which `CapabilitySet` implements, so the whole policy
    /// is expressible without it.
    ///
    /// # Examples
    ///
    /// ```
    /// use rustix::thread::CapabilitySet;
    /// use tinio_sandbox_linux::Cap;
    ///
    /// assert!(Cap::Chown.set().contains(CapabilitySet::CHOWN));
    /// assert!(!Cap::Chown.set().contains(CapabilitySet::SYS_ADMIN));
    /// ```
    pub const fn set(self) -> CapabilitySet {
        match self {
            Self::Chown => CapabilitySet::CHOWN,
        }
    }
}

/// The capabilities to keep for this mode.
///
/// Everything else goes — including `CAP_SYS_ADMIN`, which the mount table
/// needed while it was being built and nothing needs afterwards. Read-only
/// mode needs no capability at all: it never writes, so it never chowns.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_linux::retained;
///
/// assert!(retained(AccessMode::ReadOnly, true).is_empty());
/// assert_eq!(retained(AccessMode::ReadWrite, true).len(), 1);
/// ```
pub const fn retained(mode: AccessMode, local_uid_configured: bool) -> &'static [Cap] {
    match (mode, local_uid_configured) {
        (AccessMode::ReadOnly, _) => &[],
        (AccessMode::ReadWrite, false) => &[],
        (AccessMode::ReadWrite, true) => &[Cap::Chown],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::AccessMode;

    #[test]
    fn read_only_retains_nothing() {
        assert!(retained(AccessMode::ReadOnly, false).is_empty());
        assert!(retained(AccessMode::ReadOnly, true).is_empty());
    }

    #[test]
    fn read_write_retains_chown_only_when_a_uid_mapping_exists() {
        assert!(retained(AccessMode::ReadWrite, false).is_empty());
        assert_eq!(retained(AccessMode::ReadWrite, true), &[Cap::Chown]);
    }

    #[test]
    fn every_capability_has_a_kernel_name() {
        assert_eq!(Cap::Chown.name(), "CAP_CHOWN");
    }
}
