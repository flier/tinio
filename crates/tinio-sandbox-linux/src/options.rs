//! The Linux backend's construction-time options.
//!
//! Platform-specific knobs live here rather than in a shared type, so the
//! `Backend` trait's signature stays the same on every platform (design
//! decision 4 of the 2026-09-18 grilling — a shared options type would
//! accumulate a field per platform that no other platform reads).

use std::{env, path::PathBuf};

use rustix::process::geteuid;

use crate::{_core::Result, Mechanism};

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
    device_files: bool,
    tmpdir: Option<PathBuf>,
    euid: Option<u32>,
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

    /// Bind the four [`DEVICE_FILES`](crate::DEVICE_FILES) into the jail.
    ///
    /// **This is the request's flag moved onto this crate's own options, with
    /// the default it had there.** `Sandbox` used to carry it — a
    /// cross-platform type holding a knob only this backend reads, and a no-op
    /// on the other two — while the facts the plan needs that the sandbox does
    /// not own belong here (this module's own doc, and design decision 4).
    /// **Unset means no device nodes**: a caller that does not ask gets the
    /// smaller jail, which is what the `Sandbox` builder answered before the
    /// move, and `plan` reads the flag from here.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert!(!Options::default().device_files());
    /// assert!(Options::default().with_device_files(true).device_files());
    /// ```
    pub fn with_device_files(mut self, device_files: bool) -> Self {
        self.device_files = device_files;
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

    /// Whether the four device files are bound into the jail.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Options;
    ///
    /// assert!(
    ///     !Options::default().device_files(),
    ///     "unset binds no device node"
    /// );
    /// ```
    pub fn device_files(&self) -> bool {
        self.device_files
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
    use crate::_core::Error;

    #[test]
    fn defaults_are_an_automatic_model_and_a_jail_with_nothing_extra_in_it() {
        // **The served root's access is deliberately absent from this list** —
        // it is the request's first exposure, and `enter` reads it from there
        // rather than from a second spelling that could disagree with it. What
        // is left is what `Options` really owns: the model, the `local_uid`
        // mapping, and the device-file flag.
        let opts = Options::default();
        assert_eq!(opts.model(), Mechanism::Auto);
        assert!(!opts.local_uid_configured());
        assert!(!opts.device_files());
    }

    #[test]
    fn every_setter_round_trips() {
        let opts = Options::default()
            .with_model(Mechanism::Root)
            .with_local_uid(true)
            .with_device_files(true)
            .with_tmpdir(PathBuf::from("/tmp"))
            .with_euid(1000);
        assert_eq!(opts.model(), Mechanism::Root);
        assert!(opts.local_uid_configured());
        assert!(opts.device_files());
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
        assert_eq!(opts.euid(), 1000);
    }

    #[test]
    fn the_environment_is_read_through_its_one_reader() {
        // **The production entry, and the only caller of `Mechanism::from_env`**:
        // the WSL2 script's two legs differ exactly by this variable (design
        // §12), so a reader that quietly picked `Auto` would run the same leg
        // twice and the root model would never be exercised.
        //
        // **The variable is read here and never written**, because `set_var` is
        // `unsafe` in edition 2024 and this crate forbids `unsafe` — so the
        // expectation is spelled out from the environment beside the call, as the
        // function's own doc example does, and the test holds whether this
        // process was started with the variable absent, empty, or naming a model.
        // What it pins is the mapping and the refusal's own words, which is
        // everything the function owns.
        let raw = env::var("TINIO_SANDBOX_MODEL").ok();
        let expected = match raw.as_deref() {
            None | Some("") => Ok(Mechanism::Auto),
            Some("userns") => Ok(Mechanism::UserNamespace),
            Some("root") => Ok(Mechanism::Root),
            Some(_) => Err("a value that names no model"),
        };
        match (Options::from_env(), expected) {
            (Ok(options), Ok(model)) => assert_eq!(options.model(), model),
            (Err(error), Err(_)) => assert!(
                matches!(error, Error::Unsupported { .. })
                    && error.to_string().contains("TINIO_SANDBOX_MODEL"),
                "{error}"
            ),
            (options, expected) => panic!("{options:?} against {expected:?}"),
        }
    }

    #[test]
    fn the_tmpdir_resolves_to_the_environments_answer_when_unset() {
        // **Unset is `env::temp_dir()`, and that is the jail's parent rather than
        // a default**: the jail tmpfs is created inside it, so the `$TMPDIR` this
        // answers is the path the `create jail root in $TMPDIR` refusal names.
        assert_eq!(
            Options::default().resolved_tmpdir(),
            env::temp_dir(),
            "unset means the environment's answer"
        );
        let set = PathBuf::from("/var/tmp");
        assert_eq!(
            Options::default()
                .with_tmpdir(set.clone())
                .resolved_tmpdir(),
            set,
            "and a set one is handed back unchanged"
        );
    }
}
