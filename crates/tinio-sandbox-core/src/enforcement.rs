//! What a confinement applied, and what it refused — the vocabulary a caller classifies with.

use std::path::PathBuf;

/// How much of its guarantee a mechanism actually applies.
///
/// **The mechanism's own answer about itself**, published by each backend as a `DIAGNOSTICS`
/// constant beside the facts it already states, so a caller reads the strength a backend claims
/// instead of assuming one. It answers a
/// *mechanism's* question: `Enforcement::Full` says "what this backend does, it does wholly",
/// which is a different statement from anything an [`Attribution`] makes about one command.
///
/// **The value a target with no backend answers is [`Enforcement::NotReported`]** rather than a
/// statement that nothing is applied: a backend that does not exist cannot claim to implement
/// nothing, and reading "no mechanism was asked" as "the mechanism applies nothing" is the one
/// way this enum could be used to make a false claim.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Enforcement;
///
/// // What the three landed backends answer, and the value that never stands for a claim:
/// // a backend that has not answered says so rather than claiming nothing is applied.
/// assert_ne!(Enforcement::Full, Enforcement::NotReported);
/// assert_eq!(
///     tinio_sandbox_core::Diagnostics::NOT_REPORTED.enforcement,
///     Enforcement::NotReported
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enforcement {
    /// The mechanism applies the whole guarantee this build's contract claims for it. Linux's
    /// user namespace + mount + `pivot_root` + capability drop + seccomp, macOS's Seatbelt and
    /// Windows' AppContainer each answer this today.
    Full,
    /// The backend has not answered the question. **Never a claim about a mechanism** — it is
    /// what [`Diagnostics::NOT_REPORTED`] carries, and what a caller that needs the strength
    /// must treat as unknown rather than as absent.
    NotReported,
}

/// Linux's one degradation, as the sentence both of its emitters spell — **the text is defined
/// once because two crates render it**.
///
/// `tinio-sandbox-linux`'s `seccomp::install` emits it where the skip is decided, and
/// `tinio-cli`'s in-place re-emit emits it again because that first event is written before the
/// arm's subscriber exists (the confinement runs first, sandbox design decision 26) — and the
/// re-emit appends its own clause (`; the directory confinement still holds`), which is that
/// arm's statement about what still stands rather than a second spelling of this sentence.
///
/// **It is a `warn!` message and not an `Error` payload**, which is why it lives beside
/// [`Enforcement`] rather than beside the `*_USAGE` sentences `error.rs` owns: the value that
/// answers how much of a mechanism's guarantee applied is where its one shortfall is stated.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::SECCOMP_SKIP_WARNING;
///
/// assert!(SECCOMP_SKIP_WARNING.contains("outbound connections are NOT blocked"));
/// ```
pub const SECCOMP_SKIP_WARNING: &str =
    "this architecture has no syscall table, so outbound connections are NOT blocked";

/// What a refusal was *about*, as precisely as the platform could name it.
///
/// **One variant per shape a refusal can name, and not one per surface it can arrive on.** A
/// refusal is the same fact whether it reached the caller as an `io::Error`, as a platform
/// error or as an exit status: what a caller needs is the subject — the path, or the operation
/// the platform refused — and reading it out of the sentence the platform rendered would be
/// text matching against an implementation detail.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// use tinio_sandbox_core::Refused;
///
/// let refused = Refused::Path(PathBuf::from("/srv/data/.tinio"));
/// assert!(matches!(refused, Refused::Path(path) if path.ends_with(".tinio")));
/// assert!(matches!(Refused::Channel("control"), Refused::Channel("control")));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A path the confinement refused access to.
    Path(PathBuf),
    /// A named channel or operation the platform refused — a control channel, or the step a
    /// platform error was raised at. `&'static str` because it is a name the platform owns
    /// rather than a value assembled at the refusal.
    Channel(&'static str),
}

/// What a command's end says about the confinement — **the one answer it can give, and the
/// value that is no answer at all**.
///
/// A caller that has only a sentence cannot tell "the confinement never applied, so nothing
/// ran confined" from an exit that says nothing about the mechanism, and those need different
/// responses: the first is a broken mechanism to report, the second is nothing to report.
///
/// **The other case is an exit with nothing to classify, and [`Self::Ran`] states it as a
/// value.** A child that reported readiness and then ended was confined — it announced a server
/// through the mechanism, so nothing this side observed says the mechanism failed — and nothing
/// this side observed refused it either, so the one word that stays true is the word that
/// classifies nothing. Holding that word in the vocabulary is what keeps one rule in one place:
/// the call site that frames a broken confinement matches on the arm rather than testing the
/// readiness record a second time, so the classification and the frame cannot drift apart.
///
/// **The launcher decides on the two things it observed** — the exit status, and whether the
/// child ever reported readiness — and only one of them can answer: a child that ended *before*
/// reporting is [`Self::Broken`], while a child that reported and then ended is [`Self::Ran`],
/// because a refusal after readiness and any other non-zero exit are one fact at this boundary.
/// The child's stderr is the operator's, unmediated (design decision 19), so a launcher that
/// read a refusal out of an exit code would be naming a fact it never saw.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Attribution;
///
/// let broken = Attribution::Broken {
///     detail: "the confined child exited (exit status: 3) without reporting readiness".to_owned(),
/// };
/// let ran = Attribution::Ran;
/// // The sentence is the detail's, kept verbatim, and the shape is told apart by its variant
/// // rather than by reading that sentence.
/// assert!(matches!(
///     broken,
///     Attribution::Broken { detail } if detail.ends_with("without reporting readiness")
/// ));
/// // The other case carries neither a subject nor a sentence — it is the absence of a
/// // classification — and a caller tells it apart the same way, by the variant alone.
/// assert!(matches!(ran, Attribution::Ran));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribution {
    /// The confinement never applied, so nothing ran confined. `detail` is the sentence the
    /// refusing call site already emits, so the classification adds a value without rewriting
    /// what an operator reads.
    ///
    /// **No platform field**: this shape's only reader is the launcher, which *is* the process
    /// `std::env::consts::OS` would spell, so the spelling would be a field no caller renders or
    /// branches on.
    Broken {
        /// What was observed instead: the exit, the refusal, the missing frame.
        detail: String,
    },
    /// The child reported readiness and then exited — **not a claim about the confinement, but
    /// the absence of a classification** for an exit this process has no evidence to read a
    /// refusal from.
    ///
    /// **The exit status cannot supply the missing half.** A child that announced a server
    /// through the mechanism and then ended could have been refused from inside the confinement
    /// or could have failed for any other reason, and the status alone does not say which. The
    /// child's stderr is the operator's, unmediated (design decision 19), so a launcher that
    /// read a refusal out of an exit code would be naming a subject it never saw — what is left
    /// is an exit with nothing to classify, and this arm is how the launcher says so without a
    /// second test of [`Child::terminal`] of its own.
    ///
    /// **A run frames nothing for it, and this arm is why**: a child that reported owes stdout no
    /// `error` frame — it already spoke on its own stream, and the status is forwarded beside
    /// that rather than reclassified — so the call site withholds the frame by matching here,
    /// and the rule is stated once rather than re-derived from the readiness record at the call
    /// site.
    ///
    /// **No field, because there is nothing this arm owes a caller**: the status is the caller's
    /// own argument to [`Child::attribution`], and an exit status carries no subject to report.
    ///
    /// [`Child::attribution`]: crate::Child::attribution
    /// [`Child::terminal`]: crate::Child::terminal
    Ran,
}

/// What a backend answers when it is asked how much of its guarantee it applies.
///
/// **A struct rather than a bare [`Enforcement`], because the question is the backend's and
/// only its own answer is here.** The types this item's design started from carried denial
/// signatures and runner-failure rules as well; those were dropped before landing, because
/// both are read from a child's stderr and the launcher must not capture it — the operator
/// keeps their child's stream unmediated (design decision 19), so the launcher has no such
/// text to match on and no rule to publish. What is left is the one question a launcher can
/// answer about itself.
///
/// **The answers in use are [`Enforcement::Full`] and [`Enforcement::NotReported`]**, and
/// [`Enforcement`]'s own doc says what each of them claims.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Diagnostics, Enforcement};
///
/// // A backend that has not been asked answers `NotReported`, never "no mechanism".
/// assert_eq!(
///     Diagnostics::NOT_REPORTED.enforcement,
///     Enforcement::NotReported
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Diagnostics {
    /// How much of its guarantee the backend applies.
    pub enforcement: Enforcement,
}

impl Diagnostics {
    /// The one answer that is not a claim: nothing has reported its enforcement.
    ///
    /// Every backend whose mechanism has landed publishes its own `DIAGNOSTICS` beside its
    /// facts, so this constant is what a backend that has not been answered yet — and a target
    /// whose selection resolves to no backend at all — returns.
    pub const NOT_REPORTED: Self = Self {
        enforcement: Enforcement::NotReported,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_unanswered_diagnostics_is_not_an_enforcement_claim() {
        assert_eq!(
            Diagnostics::NOT_REPORTED.enforcement,
            Enforcement::NotReported
        );
        assert_ne!(Diagnostics::NOT_REPORTED.enforcement, Enforcement::Full);
    }

    /// Two refusals are told apart **without reading a sentence**: the two are compared by the
    /// subject's own value and variant rather than by any rendering of it.
    #[test]
    fn a_refusal_is_classified_by_its_subject_rather_than_by_its_text() {
        let path = Refused::Path(PathBuf::from("/srv/data/.tinio"));

        // A channel refusal names the operation rather than a path, and the two are told apart
        // by the shape of the subject rather than by a rendering of it.
        let channel = Refused::Channel("control");
        assert_ne!(channel, path);
        assert_ne!(refused_label(&channel), refused_label(&path));
    }

    /// The variant a refusal is, as a name a test can compare — the two shapes are told apart
    /// by the *variant* and never by a rendering of it.
    fn refused_label(refused: &Refused) -> &'static str {
        match refused {
            Refused::Path(_) => "path",
            Refused::Channel(_) => "channel",
        }
    }

    #[test]
    fn broken_keeps_the_sentence_it_was_given() {
        let detail = "the confined child exited (exit status: 3) without reporting readiness";
        let attribution = Attribution::Broken {
            detail: detail.to_owned(),
        };
        let Attribution::Broken { detail } = &attribution else {
            panic!("the variant just built: {attribution:?}");
        };
        assert_eq!(
            detail,
            "the confined child exited (exit status: 3) without reporting readiness"
        );
    }
}
