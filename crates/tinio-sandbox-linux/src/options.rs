//! The Linux backend's construction-time options.
//!
//! Platform-specific knobs live here rather than in a shared type, so the
//! `Backend` trait's signature stays the same on every platform (design
//! decision 4 of the 2026-09-18 grilling — a shared options type would
//! accumulate a field per platform that no other platform reads).

use std::{env, path::PathBuf};

use rustix::process::geteuid;

use crate::{
    _core::{AccessMode, Result},
    Mechanism,
};

/// How to confine, and the facts the backend needs that the sandbox does not
/// own.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::{Mechanism, Options};
///
/// let opts = Options::default().with_model(Mechanism::Root);
/// assert_eq!(opts.model(), Mechanism::Root);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Options {
    model: Mechanism,
    local_uid_configured: bool,
    tmpdir: Option<PathBuf>,
    euid: Option<u32>,
    served_root_access: AccessMode,
}

impl Options {
    /// The defaults, with the model read from `TINIO_SANDBOX_MODEL`.
    ///
    /// **This is the only thing that reads that variable.** It is what makes
    /// the two verification legs actually different — the WSL2 script sets
    /// `TINIO_SANDBOX_MODEL=userns` and `=root`, and without a caller for
    /// `Mechanism::from_env` both legs would run `Auto` and the root model would
    /// never be exercised (design §12). `Default` stays free of the
    /// environment so it cannot fail and cannot depend on how the test process
    /// was started.
    ///
    /// # Errors
    ///
    /// [`tinio_sandbox_core::Error::Unsupported`] for a value that is neither
    /// `userns` nor `root`, so a typo cannot silently pick a mechanism.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// // Both arms, so the example turns on the call rather than on how the
    /// // test process happened to be started: the options it built, or the
    /// // refusal that names the variable whose value was wrong.
    /// match Options::from_env() {
    ///     Ok(options) => {
    ///         let _ = options.model();
    ///     }
    ///     Err(error) => assert!(error.to_string().contains("TINIO_SANDBOX_MODEL")),
    /// }
    /// ```
    pub fn from_env() -> Result<Self> {
        let raw = env::var("TINIO_SANDBOX_MODEL").ok();
        Ok(Self::default().with_model(Mechanism::from_env(raw.as_deref())?))
    }

    /// The requested model (design decision 7).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::{Mechanism, Options};
    ///
    /// let options = Options::default().with_model(Mechanism::UserNamespace);
    /// assert_eq!(options.model(), Mechanism::UserNamespace);
    /// ```
    pub fn with_model(mut self, model: Mechanism) -> Self {
        self.model = model;
        self
    }

    /// Whether FR-033's `local_uid` mapping is configured — the one reason a
    /// read-write jail keeps a capability (design §8).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert!(
    ///     Options::default()
    ///         .with_local_uid(true)
    ///         .local_uid_configured()
    /// );
    /// ```
    pub fn with_local_uid(mut self, configured: bool) -> Self {
        self.local_uid_configured = configured;
        self
    }

    /// Where to put the jail directory. Unset uses `std::env::temp_dir()`,
    /// which is `$TMPDIR` on a normal run — and that directory is a
    /// precondition of the whole confinement rather than a detail: the jail
    /// tmpfs is *created* inside it, so a read-only, missing or full one fails
    /// at the first `create_dir_all` with nothing else in the run to blame.
    /// The failure names it: `create jail root in $TMPDIR` (Task 5's `enter`
    /// says so in full).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// use tinio_sandbox_linux::Options;
    ///
    /// let options = Options::default().with_tmpdir(PathBuf::from("/var/tmp"));
    /// assert_eq!(options.resolved_tmpdir(), PathBuf::from("/var/tmp"));
    /// ```
    pub fn with_tmpdir(mut self, tmpdir: PathBuf) -> Self {
        self.tmpdir = Some(tmpdir);
        self
    }

    /// Override the effective uid. Tests use it; production reads the kernel.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert_eq!(Options::default().with_euid(1000).euid(), 1000);
    /// ```
    pub fn with_euid(mut self, euid: u32) -> Self {
        self.euid = Some(euid);
        self
    }

    /// The access the served root gets; decides the capability set.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::AccessMode;
    /// use tinio_sandbox_linux::Options;
    ///
    /// let options = Options::default().with_served_root_access(AccessMode::ReadOnly);
    /// assert!(options.served_root_access().is_read_only());
    /// ```
    pub fn with_served_root_access(mut self, access: AccessMode) -> Self {
        self.served_root_access = access;
        self
    }

    /// The requested model.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::{Mechanism, Options};
    ///
    /// assert_eq!(Options::default().model(), Mechanism::Auto);
    /// ```
    pub fn model(&self) -> Mechanism {
        self.model
    }

    /// The jail directory's parent, when one was set.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert_eq!(Options::default().tmpdir(), None, "unset reads $TMPDIR");
    /// let set = Options::default().with_tmpdir(PathBuf::from("/tmp"));
    /// assert_eq!(set.tmpdir(), Some(&PathBuf::from("/tmp")));
    /// ```
    pub fn tmpdir(&self) -> Option<&PathBuf> {
        self.tmpdir.as_ref()
    }

    /// The access the served root gets.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::AccessMode;
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert_eq!(
    ///     Options::default().served_root_access(),
    ///     AccessMode::ReadWrite
    /// );
    /// ```
    pub fn served_root_access(&self) -> AccessMode {
        self.served_root_access
    }

    /// Whether a `local_uid` mapping is configured.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert!(!Options::default().local_uid_configured());
    /// ```
    pub fn local_uid_configured(&self) -> bool {
        self.local_uid_configured
    }

    /// The effective uid to resolve the model against.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// // An injected uid is what the model is resolved against; unset, the
    /// // kernel's own answer is used.
    /// assert_eq!(Options::default().with_euid(0).euid(), 0);
    /// ```
    pub fn euid(&self) -> u32 {
        self.euid.unwrap_or_else(|| geteuid().as_raw())
    }

    /// The tmpdir to place the jail in.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// use tinio_sandbox_linux::Options;
    ///
    /// let set = Options::default().with_tmpdir(PathBuf::from("/var/tmp"));
    /// assert_eq!(set.resolved_tmpdir(), PathBuf::from("/var/tmp"));
    /// assert_eq!(
    ///     Options::default().resolved_tmpdir(),
    ///     std::env::temp_dir(),
    ///     "unset means the environment's answer"
    /// );
    /// ```
    pub fn resolved_tmpdir(&self) -> PathBuf {
        self.tmpdir.clone().unwrap_or_else(env::temp_dir)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::_core::AccessMode;

    #[test]
    fn defaults_are_a_read_write_root_with_an_automatic_model() {
        let opts = Options::default();
        assert_eq!(opts.model(), Mechanism::Auto);
        assert_eq!(opts.served_root_access(), AccessMode::ReadWrite);
        assert!(!opts.local_uid_configured());
    }

    #[test]
    fn every_setter_round_trips() {
        let opts = Options::default()
            .with_model(Mechanism::Root)
            .with_local_uid(true)
            .with_tmpdir(PathBuf::from("/tmp"))
            .with_euid(1000)
            .with_served_root_access(AccessMode::ReadOnly);
        assert_eq!(opts.model(), Mechanism::Root);
        assert!(opts.local_uid_configured());
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
        assert_eq!(opts.euid(), 1000);
        assert_eq!(opts.served_root_access(), AccessMode::ReadOnly);
    }
}
