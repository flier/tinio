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

use std::{env, result};

use parse_display::{Display, FromStr};
use tracing::error;

use crate::_core::{Error, MACOS_MECHANISM_USAGE, Result};

/// Which mechanism confines a macOS child (design decision 6).
///
/// **`SandboxExec` is the default, and the alternative is opt-in.** The helper
/// is what this crate has always used and what every allowance in
/// [`RUNTIME_BASELINE`](crate::profile::RUNTIME_BASELINE) was measured against;
/// the in-process apply is a second route with its own measured cost (it is
/// one-shot, irreversible, and leaves the confined process unable to `fork` or
/// `exec`), so it is never selected by accident and never selected as a fallback.
///
/// **`Display` is the wire spelling, and it is the parser's spelling too**: the
/// `kebab-case` render is derived from the variant names, so what this crate prints
/// and what [`Self::from_env`] accepts cannot drift apart. [`Self::as_str`], the
/// `&'static str` projection the launch payload and the facade's `Applied` take, is
/// pinned to both by the test at the foot of this file.
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
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Display,
    FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
#[display(style = "kebab-case")]
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

    /// The wire spelling as a `&'static str`.
    ///
    /// **`Display` is the home of the spelling and this is its projection**, not a second list:
    /// the derive renders each variant's name in `kebab-case`, and the two literals below are held
    /// to it by the round-trip test at the foot of this file, so a rename that reached one side
    /// fails there rather than in an operator's event.
    ///
    /// **The `'static` borrow is what this is for**: `tinio-sandbox`'s `Applied.mechanism` is a
    /// `&'static str` — its `apply_self_confinement` reads this — and a rendered `String` cannot
    /// serve that field. The launcher takes it the same way for the payload's mechanism marker.
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
            Self::SandboxExec => "sandbox-exec",
            Self::SandboxInit => "sandbox-init",
        }
    }

    /// Read [`Self::ENV`]. Absent or empty means [`Mechanism::SandboxExec`].
    ///
    /// **The accepted values are `Display`'s, through the derived `FromStr`**: a spelling the
    /// parser takes is a spelling the payload's marker and the operator's variable share, because
    /// there is no second list of literals here to fall out of step with the render.
    ///
    /// **Absent and empty are the same answer, and neither is guessed**: the
    /// variable is how an operator *asks*, so saying nothing takes the default
    /// rather than being an error — while a value this build does not know is
    /// refused with both values named, so a typo cannot silently confine a run
    /// with the mechanism the operator did not pick.
    ///
    /// # Errors
    ///
    /// [`Error::Usage`] for any
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
            // **The parse is the derive's and the refusal is this arm's**: a value one of the
            // derived spellings accepts is the value `Display` writes, so the only thing left to
            // decide here is what to say about a value none of them accepts.
            Some(wrong) => wrong.parse().map_err(|_| {
                // Logged where the `Error` is built (shared §7), for the reason
                // the Linux model's refusal is: the caller's `?` only prints it
                // on the way out, and the value that was wrong is the whole
                // diagnosis.
                error!(
                    step = Self::ENV,
                    value = wrong,
                    "unsupported confinement mechanism"
                );
                Error::Usage {
                    detail: MACOS_MECHANISM_USAGE,
                }
            }),
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
    /// **A value that is present but not valid Unicode is refused too**, rather
    /// than read as unset: it is a value this build cannot compare against either
    /// spelling, and treating it as absent would silently run the mechanism the
    /// operator did not pick.
    ///
    /// # Errors
    ///
    /// [`Error::Usage`] for a value
    /// that is neither `sandbox-exec` nor `sandbox-init` — and for one that is not
    /// valid Unicode at all — so neither a typo nor an unreadable value can
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
        Ok(Self::default().with_mechanism(read_mechanism(env::var(Mechanism::ENV))?))
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

/// The mechanism the environment asks for, from the variable's raw answer.
///
/// **`NotPresent` and `NotUnicode` are two different answers**, which is why the
/// match is written out rather than collapsed by `env::var(..).ok()`: an absent
/// (or empty) value is the operator saying nothing and takes
/// [`Mechanism::SandboxExec`], while a value that is *present* and not valid
/// Unicode is one this build cannot read at all. The two mechanisms are not
/// interchangeable — one confines the child at the `exec` the helper performs,
/// the other applies inside the child before it serves — so the unreadable value
/// is refused in the same shape as an unknown one rather than read as unset.
///
/// A free function rather than a body inside [`Options::from_env`] so that the
/// third answer is testable: putting a non-Unicode value into the environment
/// needs `unsafe` `env::set_var` (Rust 2024), which this crate may not call, so
/// the test hands this function the `VarError` instead.
fn read_mechanism(raw: result::Result<String, env::VarError>) -> Result<Mechanism> {
    match raw {
        Ok(value) => Mechanism::from_env(Some(&value)),
        Err(env::VarError::NotPresent) => Mechanism::from_env(None),
        // Logged where the `Error` is built (shared §7), for the reason the
        // unknown value's refusal is: the caller's `?` only prints the sentence
        // on the way out, and the variable that was wrong is the whole diagnosis.
        // The value itself is not printable — a non-Unicode variable is the
        // platform's own bytes — so the log names the error rather than a value.
        Err(error @ env::VarError::NotUnicode(_)) => {
            error!(
                step = Mechanism::ENV,
                error = %error,
                "unsupported confinement mechanism"
            );
            Err(Error::Usage {
                detail: MACOS_MECHANISM_USAGE,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

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
    fn an_unreadable_value_is_refused_and_never_the_default() {
        // **The one arm `env::var(..).ok()` used to swallow.** A value that is
        // present and not valid Unicode is one this build cannot read, and reading
        // it as unset would run `sandbox-exec` for an operator who asked for
        // something this build could not name — the silent fallback this module's
        // contract rules out. The refusal is driven through the `VarError` rather
        // than the environment, because putting a non-Unicode value there needs
        // `unsafe` `env::set_var` in Rust 2024.
        let raw = Err(env::VarError::NotUnicode(OsString::from("sandbox-init")));
        let error = read_mechanism(raw).expect_err("an unreadable value must not select");
        let rendered = error.to_string();
        assert!(
            rendered.contains("TINIO_SANDBOX_MACOS_MECHANISM"),
            "{rendered}"
        );
        assert!(rendered.contains("sandbox-exec"), "{rendered}");
        assert!(rendered.contains("sandbox-init"), "{rendered}");
    }

    #[test]
    fn only_an_absent_variable_is_unset() {
        // The other half of the same match, so the pair pins the distinction the
        // unreadable case turns on: `NotPresent` — and the empty string, which the
        // parser reads as the operator saying nothing — are unset, and every other
        // answer goes through [`Mechanism::from_env`].
        assert_eq!(
            read_mechanism(Err(env::VarError::NotPresent)).unwrap(),
            Mechanism::SandboxExec
        );
        assert_eq!(
            read_mechanism(Ok(String::new())).unwrap(),
            Mechanism::SandboxExec
        );
        assert_eq!(
            read_mechanism(Ok(String::from("sandbox-init"))).unwrap(),
            Mechanism::SandboxInit
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
        // **The pin between the two spellings that remain, and the reason `as_str` may keep its
        // literals**: `Display`/`FromStr` are one derivation, and `as_str` is the `&'static str`
        // projection the facade's `Applied` and this crate's payload need. The first two
        // assertions name the wire bytes every reader takes — those two, the environment variable
        // and the e2e legs — and the loop then holds the projection, the render and the parser to
        // each other, so a launch cannot report a mechanism the parser does not accept.
        assert_eq!(Mechanism::SandboxExec.to_string(), "sandbox-exec");
        assert_eq!(Mechanism::SandboxInit.to_string(), "sandbox-init");
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
