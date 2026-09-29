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
/// **`Partial` and `Unimplemented` are reserved vocabulary, and nothing produces them today.**
/// They are the two answers the mechanism-selection survey's dual-mode routes need — a
/// mechanism that applies part of what it promises (the degraded Windows route), and one that
/// is selected but applies nothing at all — so the words exist before the routes do. That
/// survey is `docs/superpowers/plans/2026-09-25-sandbox-followups.md`'s "the mechanism-selection
/// question" section, whose dual-mode items are still open; until they land, the only two values
/// any backend answers are [`Enforcement::Full`] and [`Enforcement::NotReported`], and reading
/// either reserved word as a live state is reading a route this build does not have. The value
/// a target with no backend answers is [`Enforcement::NotReported`] rather than
/// `Unimplemented`: a backend that does not exist cannot claim to implement nothing, and
/// reading "no mechanism was asked" as "the mechanism is unimplemented" is the one way this
/// enum could be used to make a false claim.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Enforcement;
///
/// // What the three landed backends answer, and the value that never stands for a claim:
/// // a backend that has not answered says so rather than answering `Unimplemented`.
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
    /// The mechanism applies part of it, and the part is the platform's to name. Reserved for
    /// the mechanism-selection survey's degraded routes; nothing produces it today.
    Partial,
    /// The mechanism applies none of the guarantee. Reserved for a selected mechanism with
    /// nothing behind it; nothing produces it today.
    Unimplemented,
    /// The backend has not answered the question. **Never a claim about a mechanism** — it is
    /// what [`Diagnostics::NOT_REPORTED`] carries, and what a caller that needs the strength
    /// must treat as unknown rather than as absent.
    NotReported,
}

/// What a refusal was *about*, as precisely as the platform could name it.
///
/// **One variant per shape a refusal can name, and not one per surface it can arrive on.** A
/// refusal is the same fact whether it reached the caller as an `io::Error`, as a platform
/// error or as an exit status: what a caller needs is the subject — the path, or the operation
/// the platform refused — and reading it out of the sentence the platform rendered would be
/// text matching against an implementation detail.
///
/// [`Self::Unnamed`] is the honest answer when the platform named nothing, and the one this
/// crate's callers must be able to reach: a refusal that arrived without a subject has to be
/// reportable rather than guessed at. The launcher that forwards a confined child's exit is the
/// case it was written for — design decision 19 leaves that child's stderr the operator's,
/// unmediated, so nothing on this side reads what it was refused — **and what that launcher
/// answers for such a child today is [`Attribution::Ran`] rather than a denial**: a refusal this
/// side never read is left unclassified instead of being reported as an unnamed one, and the
/// variant stays reachable for a caller that holds a real denial with no subject on it.
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
/// assert!(matches!(Refused::Unnamed, Refused::Unnamed));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// A path the confinement refused access to.
    Path(PathBuf),
    /// A named channel or operation the platform refused — a control channel, or the step a
    /// platform error was raised at. `&'static str` because it is a name the platform owns
    /// rather than a value assembled at the refusal.
    Channel(&'static str),
    /// The platform did not name it: the operation was refused, and neither this process nor
    /// the platform can say which one.
    Unnamed,
}

/// One refusal: the platform that made it, what it was about, and what there is to show for it.
///
/// **`platform` is a plain `std::env::consts::OS` spelling** (`"linux"`, `"macos"`,
/// `"windows"`) rather than an enum, because it is a field of a report rather than a value any
/// code branches on: a caller that had to match an enum here would be re-deriving the
/// compilation target it already is.
///
/// **`evidence` is prose on purpose.** It is the one part of a refusal this crate cannot
/// type — a platform's own sentence, a status's rendering — and keeping it in its own field
/// is what lets everything else about the refusal stay structural.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Denial, Refused};
///
/// let denial = Denial {
///     platform: "windows",
///     refused: Refused::Channel("control"),
///     evidence: "CreateNamedPipeW: Access is denied. (os error 5)".to_owned(),
/// };
/// assert_eq!(denial.platform, "windows");
/// assert!(denial.evidence.contains("os error 5"), "{denial:?}");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    /// The platform whose confinement refused, as `std::env::consts::OS` spells it.
    pub platform: &'static str,
    /// What the refusal was about, as far as the platform named it.
    pub refused: Refused,
    /// What there is to show for it: the platform's own message, or the exit status the
    /// refusal arrived as.
    pub evidence: String,
}

/// What a command's end says about the confinement — **the two answers it can give, and the
/// third value that is no answer at all**.
///
/// A caller that has only a sentence cannot tell "the confinement never applied, so nothing
/// ran confined" from "it applied and the command was refused", and those need different
/// responses: the first is a broken mechanism to report, the second is a refusal to explain.
///
/// **The third case is an exit with nothing to classify, and [`Self::Ran`] states it as a
/// value.** A child that reported readiness and then ended was confined — it announced a server
/// through the mechanism, so nothing this side observed says the mechanism failed — and nothing
/// this side observed refused it either, so the one word that stays true is the word that
/// classifies nothing. Holding that word in the vocabulary is what keeps one rule in one place:
/// the call site that frames a broken confinement matches on the arm rather than testing the
/// readiness record a second time, so the classification and the frame cannot drift apart.
///
/// **No run frames a [`Self::Denied`] today, and that is this shape's settled limit rather than
/// a gap in the wiring.** The launcher decides on the two things it observed — the exit status,
/// and whether the child ever reported readiness — and only one of them can answer: a child that
/// ended *before* reporting is [`Self::Broken`], while a child that reported and then ended is
/// [`Self::Ran`], because a refusal after readiness and any other non-zero exit are one fact at
/// this boundary. The child's stderr is the operator's, unmediated (design decision 19), so a
/// launcher that read a refusal out of an exit code would be naming a fact it never saw.
///
/// **It stays the arm for a caller that *did* see the refusal and holds it as a [`Denial`]** —
/// the subject, the platform and the evidence — which is a value an exit status can never
/// supply. [`Child::attribution`] never builds it, and that limit is no reason to make it
/// unconstructible: it is the vocabulary's answer for a caller whose evidence is a platform's
/// own message rather than a status this process reaped.
///
/// [`Child::attribution`]: crate::Child::attribution
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// use tinio_sandbox_core::{Attribution, Denial, Refused};
///
/// let broken = Attribution::Broken {
///     detail: "the confined child exited (exit status: 3) without reporting readiness".to_owned(),
/// };
/// let denied = Attribution::Denied(Denial {
///     platform: "macos",
///     refused: Refused::Path(PathBuf::from("/srv/data/.tinio")),
///     evidence: "Operation not permitted (os error 1)".to_owned(),
/// });
/// let ran = Attribution::Ran;
/// // The sentence is the detail's, kept verbatim, and the shape is told apart by its variant
/// // rather than by reading that sentence.
/// assert!(matches!(
///     broken,
///     Attribution::Broken { detail } if detail.ends_with("without reporting readiness")
/// ));
/// assert!(matches!(
///     denied,
///     Attribution::Denied(Denial {
///         refused: Refused::Path(_),
///         ..
///     })
/// ));
/// // The third case carries neither a subject nor a sentence — it is the absence of a
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
    /// branches on — unlike a denial's, which a report's sentence names.
    Broken {
        /// What was observed instead: the exit, the refusal, the missing frame.
        detail: String,
    },
    /// The confinement applied and the command was refused. **No run frames this today**; the
    /// enum's own note states the limit, and [`Self::Ran`] is the arm the launcher builds
    /// instead for the child that reported and then ended.
    Denied(Denial),
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
    /// own argument to [`Child::attribution`], and the subject an exit status cannot supply is
    /// what [`Self::Denied`] is for.
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
/// **The answers in use are [`Enforcement::Full`] and [`Enforcement::NotReported`]**, and the
/// other two are not states any backend reports: [`Enforcement`]'s own doc says which survey
/// they are reserved for and that nothing produces them today.
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
        assert_ne!(
            Diagnostics::NOT_REPORTED.enforcement,
            Enforcement::Unimplemented
        );
        assert_ne!(Diagnostics::NOT_REPORTED.enforcement, Enforcement::Full);
        assert_ne!(Diagnostics::NOT_REPORTED.enforcement, Enforcement::Partial);
    }

    /// The three shapes a refusal can take, carried through a [`Denial`] and an
    /// [`Attribution`] and read back out — the round trip a caller classifying one has to be
    /// able to make **without reading a sentence**: the subject survives as the value it was
    /// built as, and `evidence` stays the only prose in the structure.
    #[test]
    fn a_refusal_is_classified_by_its_subject_rather_than_by_its_text() {
        let path = Refused::Path(PathBuf::from("/srv/data/.tinio"));
        let denial = Denial {
            platform: "linux",
            refused: path.clone(),
            evidence: "the state database could not be opened".to_owned(),
        };
        let Attribution::Denied(read_back) = Attribution::Denied(denial.clone()) else {
            unreachable!("the variant just built")
        };
        assert_eq!(read_back.refused, path);
        assert_eq!(read_back.platform, "linux");
        assert_eq!(read_back, denial);

        // A channel refusal names the operation rather than a path, and `Unnamed` is its own
        // answer rather than a channel whose name happens to be empty.
        let channel = Denial {
            platform: "windows",
            refused: Refused::Channel("control"),
            evidence: String::new(),
        };
        assert_ne!(channel.refused, path);
        assert_ne!(
            refused_label(&channel.refused),
            refused_label(&Refused::Unnamed)
        );
        assert_ne!(Refused::Channel(""), Refused::Unnamed);
    }

    /// The variant a refusal is, as a name a test can compare — the three shapes are told apart
    /// by the *variant* and never by a rendering of it.
    fn refused_label(refused: &Refused) -> &'static str {
        match refused {
            Refused::Path(_) => "path",
            Refused::Channel(_) => "channel",
            Refused::Unnamed => "unnamed",
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
