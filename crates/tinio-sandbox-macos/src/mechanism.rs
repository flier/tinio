//! Which macOS confinement mechanism to use — `sandbox-exec`, or an in-process
//! apply — and the options that carry the answer.
//!
//! `Mechanism` (the request), `from_env` and `Options` live here, mirroring
//! `tinio-sandbox-linux`'s `mechanism.rs` and `options.rs`: a platform's own
//! vocabulary stays in its own crate, and the crate root re-exports the public
//! names, so a caller keeps writing `tinio_sandbox_macos::Mechanism`.
//!
//! **Two mechanisms, one profile.** Both render the same SBPL text from the same
//! exposure set; what differs is which process applies it and therefore *when*.
//! `sandbox-exec` applies a profile to a process it `exec`s, so the confinement
//! lands before the child's `main` and the child needs the `process-exec` rule
//! that lets that exec happen. The in-process route applies it to the process
//! that asks, so the exec and the dynamic loader have already run and that rule
//! is not needed — see `apply`'s module doc for what the route costs.

use std::{env, fmt};

use tracing::error;

use crate::_core::{Error, Result};

/// Which mechanism confines a macOS child (design decision 6).
///
/// **`SandboxExec` is the default, and the alternative is opt-in.** The helper
/// is what this crate has always used and what every allowance in
/// [`RUNTIME_BASELINE`](crate::profile::RUNTIME_BASELINE) was measured against;
/// the in-process apply is a second route with its own measured cost (it is
/// one-shot, irreversible, and leaves the confined process unable to `fork` or
/// `exec`), so it is never selected by accident and never selected as a fallback.
///
/// **`Display` is the wire spelling**, and it is the same string
/// [`Self::from_env`] accepts and [`Self::as_str`] returns: the launch payload
/// carries it so the child can report which mechanism its launcher resolved,
/// which would be three spellings of one fact if any of them were written
/// separately.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::Mechanism;
///
/// assert_eq!(Mechanism::default(), Mechanism::SandboxExec);
/// assert_eq!(Mechanism::SandboxInit.to_string(), "sandbox-init");
/// assert_ne!(Mechanism::SandboxExec, Mechanism::SandboxInit);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mechanism {
    /// Apply the profile with `/usr/bin/sandbox-exec -p <text> -D …`, which
    /// confines the child at the `exec` the helper performs.
    #[default]
    SandboxExec,
    /// Render the profile and hand it to the child, which applies it to itself
    /// with `sandbox_init_with_parameters` before it serves.
    SandboxInit,
}

impl Mechanism {
    /// The environment variable that selects the mechanism.
    ///
    /// The default is [`Mechanism::SandboxExec`] and the alternative is
    /// [`Mechanism::SandboxInit`]; **anything else is a usage error naming this
    /// variable and its two values**, because a typo that silently ran the
    /// default mechanism would be a confinement nobody chose.
    pub const ENV: &'static str = "TINIO_SANDBOX_MACOS_MECHANISM";
    /// The **one** name for the `sandbox-exec` mechanism, and the one for the
    /// other: the spelling the environment variable takes, the payload carries,
    /// `Display` prints and every refusal names.
    ///
    /// It is an associated constant rather than a literal at each of those sites
    /// for the reason `launch::PREFLIGHT_STEP` is one: a rename that reached
    /// three of the four spellings would leave an operator reading a mechanism
    /// name the parser does not accept.
    pub const SANDBOX_EXEC: &'static str = "sandbox-exec";
    /// The in-process mechanism's name, paired with [`Self::SANDBOX_EXEC`].
    pub const SANDBOX_INIT: &'static str = "sandbox-init";

    /// The mechanism's name, in the one spelling every surface shares.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::Mechanism;
    ///
    /// assert_eq!(Mechanism::SandboxExec.as_str(), "sandbox-exec");
    /// assert_eq!(
    ///     Mechanism::from_env(Some(Mechanism::SandboxInit.as_str())).unwrap(),
    ///     Mechanism::SandboxInit
    /// );
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SandboxExec => Self::SANDBOX_EXEC,
            Self::SandboxInit => Self::SANDBOX_INIT,
        }
    }

    /// Read [`Self::ENV`]. Absent or empty means [`Mechanism::SandboxExec`].
    ///
    /// **Absent and empty are the same answer, and neither is guessed**: the
    /// variable is how an operator *asks*, so saying nothing takes the default
    /// rather than being an error — while a value this build does not know is
    /// refused with both values named, so a typo cannot silently confine a run
    /// with the mechanism the operator did not pick.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`](tinio_sandbox_core::Error::Unsupported) for any
    /// other value.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::Mechanism;
    ///
    /// assert_eq!(
    ///     Mechanism::from_env(Some("sandbox-init")).unwrap(),
    ///     Mechanism::SandboxInit
    /// );
    /// // Absent or empty means the default, which is the helper.
    /// assert_eq!(Mechanism::from_env(None).unwrap(), Mechanism::SandboxExec);
    /// assert_eq!(
    ///     Mechanism::from_env(Some("")).unwrap(),
    ///     Mechanism::SandboxExec
    /// );
    /// ```
    pub fn from_env(raw: Option<&str>) -> Result<Self> {
        match raw {
            None | Some("") => Ok(Self::default()),
            Some(Self::SANDBOX_EXEC) => Ok(Self::SandboxExec),
            Some(Self::SANDBOX_INIT) => Ok(Self::SandboxInit),
            Some(wrong) => {
                // Logged where the `Error` is built (shared §7), for the reason
                // the Linux model's refusal is: the caller's `?` only prints it
                // on the way out, and the value that was wrong is the whole
                // diagnosis.
                error!(
                    step = Self::ENV,
                    value = wrong,
                    "unsupported confinement mechanism"
                );
                Err(Error::Unsupported {
                    detail: "TINIO_SANDBOX_MACOS_MECHANISM must be `sandbox-exec` or `sandbox-init`",
                })
            }
        }
    }

    /// Whether this mechanism confines the child by wrapping its `exec` —
    /// `sandbox-exec`'s shape, and the one the launcher's pre-flight serves.
    ///
    /// **This is the one difference between the two renders, and it is measured rather
    /// than assumed.** On the `sandbox-exec` route the `process-exec` rule is load-bearing:
    /// without it the helper exits `71` before any other rule is consulted, so the exec the
    /// profile must permit is the helper's own. On the in-process route the exec and the
    /// dynamic loader happen **before** the apply, so nothing is left to permit — a probe
    /// that applied the same text in-process with the rule removed served a real request
    /// unchanged — and keeping it would grant an exec the confined process can no longer
    /// make, which is a dead grant rather than a live narrowing. It is what the profile
    /// filters `RUNTIME_BASELINE`'s `process-exec` entry by (`Allowance::keeps`).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::Mechanism;
    ///
    /// assert!(Mechanism::SandboxExec.wraps_the_exec());
    /// assert!(!Mechanism::SandboxInit.wraps_the_exec());
    /// ```
    pub fn wraps_the_exec(self) -> bool {
        match self {
            Self::SandboxExec => true,
            Self::SandboxInit => false,
        }
    }
}

impl fmt::Display for Mechanism {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The macOS backend's construction-time options.
///
/// Platform-specific knobs live here rather than in a shared type, so the
/// `Backend` trait's signature stays the same on every platform (design decision
/// 4 of the 2026-09-18 grilling — a shared options type would accumulate a field
/// per platform that no other platform reads). The shape mirrors
/// `tinio-sandbox-linux`'s `options`: the request, read from the environment by
/// one function, and a setter for the callers that inject it.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::{Mechanism, Options};
///
/// let options = Options::default().with_mechanism(Mechanism::SandboxInit);
/// assert_eq!(options.mechanism(), Mechanism::SandboxInit);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct Options {
    mechanism: Mechanism,
}

impl Options {
    /// The defaults, with the mechanism read from [`Mechanism::ENV`].
    ///
    /// **This is the only thing that reads that variable**, and that is what
    /// makes the two mechanisms separately drivable: a run selects one here and
    /// the value travels with the launch, so the child never re-reads the
    /// environment it was started in. `Default` stays free of the environment so
    /// it cannot fail and cannot depend on how the test process was started —
    /// the same split the Linux `Options` makes.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`](tinio_sandbox_core::Error::Unsupported) for a value
    /// that is neither `sandbox-exec` nor `sandbox-init`, so a typo cannot
    /// silently select the other mechanism.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::Options;
    ///
    /// // Both arms, so the example turns on the call rather than on how the
    /// // test process happened to be started: the options it built, or the
    /// // refusal naming the variable whose value was wrong.
    /// match Options::from_env() {
    ///     Ok(options) => {
    ///         let _ = options.mechanism();
    ///     }
    ///     Err(error) => assert!(error.to_string().contains("TINIO_SANDBOX_MACOS_MECHANISM")),
    /// }
    /// ```
    pub fn from_env() -> Result<Self> {
        let raw = env::var(Mechanism::ENV).ok();
        Ok(Self::default().with_mechanism(Mechanism::from_env(raw.as_deref())?))
    }

    /// The requested mechanism (design decision 6).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::{Mechanism, Options};
    ///
    /// let options = Options::default().with_mechanism(Mechanism::SandboxInit);
    /// assert_eq!(options.mechanism(), Mechanism::SandboxInit);
    /// ```
    pub fn with_mechanism(mut self, mechanism: Mechanism) -> Self {
        self.mechanism = mechanism;
        self
    }

    /// The requested mechanism.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::{Mechanism, Options};
    ///
    /// assert_eq!(Options::default().mechanism(), Mechanism::SandboxExec);
    /// ```
    pub fn mechanism(&self) -> Mechanism {
        self.mechanism
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_helper() {
        // **The default is the mechanism every allowance was measured against**,
        // so a caller that says nothing gets the route with the longer record
        // rather than the newer one.
        assert_eq!(Mechanism::default(), Mechanism::SandboxExec);
        assert_eq!(Options::default().mechanism(), Mechanism::SandboxExec);
    }

    #[test]
    fn env_selects_the_mechanism() {
        assert_eq!(
            Mechanism::from_env(Some("sandbox-init")).unwrap(),
            Mechanism::SandboxInit
        );
        assert_eq!(
            Mechanism::from_env(Some("sandbox-exec")).unwrap(),
            Mechanism::SandboxExec
        );
        assert_eq!(Mechanism::from_env(None).unwrap(), Mechanism::SandboxExec);
        assert_eq!(
            Mechanism::from_env(Some("")).unwrap(),
            Mechanism::SandboxExec
        );
    }

    #[test]
    fn a_typo_is_refused_and_names_the_variable_and_both_values() {
        // **Never a silent fallback**: the refusal is the whole reason this
        // parser exists rather than a `==` at the call site, and it is a
        // *usage* error naming the variable, because the operator's shell is
        // where the wrong value was typed.
        let error = Mechanism::from_env(Some("sandbox_init")).expect_err("a typo must not select");
        let rendered = error.to_string();
        assert!(
            rendered.contains("TINIO_SANDBOX_MACOS_MECHANISM"),
            "{rendered}"
        );
        assert!(rendered.contains("sandbox-exec"), "{rendered}");
        assert!(rendered.contains("sandbox-init"), "{rendered}");
    }

    #[test]
    fn the_display_spelling_is_the_parsed_spelling() {
        // One fact, one set of spellings: the payload's mechanism marker, the
        // environment variable's values and `Display` all round-trip through
        // each other, so a launch cannot report a mechanism the parser does not
        // accept.
        for mechanism in [Mechanism::SandboxExec, Mechanism::SandboxInit] {
            assert_eq!(mechanism.to_string(), mechanism.as_str());
            assert_eq!(
                Mechanism::from_env(Some(mechanism.as_str())).unwrap(),
                mechanism
            );
        }
    }

    #[test]
    fn only_the_in_process_mechanism_renders_without_process_exec() {
        // **The mechanism's one profile decision, and the pair that keeps it
        // honest.** `sandbox-exec` cannot exec its target without the rule (exit
        // `71`, measured on the real product path), so the helper needs it and
        // the in-process route — whose exec and loader have already run — must
        // not keep it. One assertion per mechanism: the pair is the property, and
        // a second copy of either half asserted nothing the first did not.
        assert!(Mechanism::SandboxExec.wraps_the_exec());
        assert!(!Mechanism::SandboxInit.wraps_the_exec());
    }
}
