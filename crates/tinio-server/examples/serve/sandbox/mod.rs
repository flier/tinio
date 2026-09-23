//! Design §10 steps 6 and 7: confinement, and the confined child.
//!
//! The exposure rule and the containment guard live here. Platform arms are
//! `linux` and `windows`. `main` dispatches them: a platform that confines in
//! place runs after the configuration is parsed; a launcher runs before that
//! parse, because the parent builds no `Config` (decision 18). Decision 5's
//! refusals ([`refuse_unhonoured_channels`], [`refuse`]) live here too.

use std::error::Error;
#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
use std::path::Path;
#[cfg(not(all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))))]
use std::process;

#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
use tinio_core::mode::AccessMode;
#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
use tinio_sandbox::Sandbox;
#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
use tinio_util::path::path_contains;

#[cfg(not(all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))))]
use crate::events::emit_error;

#[cfg(all(feature = "sandbox", target_os = "linux"))]
mod linux;
#[cfg(test)]
mod tests;
#[cfg(all(feature = "sandbox", target_os = "windows"))]
mod windows;

#[cfg(all(feature = "sandbox", target_os = "linux"))]
pub use linux::confine_in_place;
#[cfg(all(feature = "sandbox", target_os = "windows"))]
pub use windows::{confined, launch};

/// Decision 5's refusal for the two channels no phase implements:
/// `[server] sandbox` and `TINIO_SANDBOX`.
///
/// **A startup error rather than a usage error**, which is §11's own
/// distinction: exit 2 and the usage status belong to a command line that
/// cannot be honoured, and this is a run that may not proceed at all — so the
/// caller reports it through `emit_on_error`, which puts the sentence on stderr
/// and the `Error` frame on stdout before returning, and the process exits with
/// the failure status `Termination` gives an `Err`.
///
/// **Both channels refuse, and the flag does not excuse either.** A run in
/// which one of them is set is a run in which the operator may believe they are
/// confined — the design's own words are that such an operator "has been failed
/// by precisely the mechanism §11 exists to provide" — and the refusal is the
/// one answer that cannot be mistaken for a confinement. `--sandbox` alone is
/// untouched: it is the channel that confines anything today, and nothing here
/// is reached for it.
///
/// **The value decides, not the variable's presence.** `TINIO_SANDBOX=0` and
/// `[server] sandbox = false` are the operator saying they want no sandbox, and
/// they proceed exactly as an unset channel does — the reading `TINIO_READ_ONLY=0`
/// already gets. Only a channel that asks for a confinement this phase cannot
/// deliver is refused, which is decision 5's rule: never a silent unconfined
/// run, and never a refusal for a setting that asks for nothing.
///
/// **The key answers first when both are set.** They are one statement, the
/// harder one to miss is the file the run was started with, and removing it
/// leaves the environment variable refusing on its own name a run later.
pub fn refuse_unhonoured_channels(
    from_key: bool,
    from_env: bool,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let channel = match (from_key, from_env) {
        (false, false) => return Ok(()),
        (true, _) => "[server] sandbox",
        (false, true) => "TINIO_SANDBOX",
    };
    Err(format!(
        "error: the `{channel}` channel is not honoured yet; `--sandbox` is the only channel that \
         confines anything today — remove the setting, or pass the flag"
    )
    .into())
}

/// The refusal every arm but the command line's makes: one message said twice —
/// a line on stderr, and the `error` frame that goes with it — then exit 2.
///
/// **Both registers, because a consumer reads one and the operator reads the
/// other** (design decision 19): stdout is the protocol channel in every build,
/// feature or no feature, so a consumer that asked for a confinement hears why
/// it did not get one instead of waiting out a timeout. Exit 2 is the code
/// [`crate::args::refuse_command_line`] exits with and the one the harness
/// reads, so a refusal is one thing however it was reached — the call sites say
/// only *what* was refused, not how to say it.
///
/// **The gate is the complement of the two arms', because they are the whole of
/// what calls it**: on the configurations that have a backend — Linux and
/// Windows, with the feature — a `--sandbox` request is honoured and there is
/// nothing to refuse, so a helper compiled there would be a dead function and the
/// lint that says so is right. Stated as the negation of that configuration
/// rather than as the arms' predicates, so it cannot drift from them: a platform
/// that gains an arm narrows this line in the same edit, which is what keeps the
/// feature from dying at a stale refusal before the new arm is reached (P2's
/// Windows arm was behind exactly that while this line still named Linux alone).
///
/// **Decision 5's third refusal is not this one.** A channel this phase does not
/// implement is a startup error rather than a usage error
/// ([`refuse_unhonoured_channels`]), and it is stated on every platform and in
/// every feature configuration rather than at the two call sites below — so it
/// does not live behind this gate.
#[cfg(not(all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))))]
pub fn refuse(message: &str) -> ! {
    eprintln!("{message}");
    emit_error(message);
    process::exit(2)
}

/// The confinement request `serve` makes, built from the paths the operator
/// gave and the mode that was resolved for them.
///
/// **The exposure rule lives here and nowhere else** (design §9): the served
/// root carries the access the mode dictates, the state directory is always
/// read-write — the access log, the redb metadata and the staging areas live
/// there — and an in-root state directory needs no entry of its own, because
/// the served root's exposure already covers it. `Sandbox::expose` keeps the
/// most permissive access for a path given twice, so two entries for one path
/// could not fight even if the rule above ever let them meet.
///
/// **"In-root" is [`path_contains`]'s answer and not a `Path::starts_with`'s**,
/// and on Windows that is the difference between an exposure set that holds and
/// one that leaks: NTFS resolves `e:\Data\.tinio` and `E:\Data\` to one
/// directory while a literal component comparison calls them unrelated, so a
/// state directory the operator spelled in another case would be judged outside
/// the root and granted a **read-write** entry inside it — the hole this rule
/// exists to close, left open by the comparison the rule was once spelled with.
/// The containment question has one definer, which is what keeps this call site
/// and the read-only refusal below from answering it differently.
///
/// **One name for the construction, because the device-file flag is an argument
/// rather than something a rebuilt set remembers.** With the call inline in an
/// arm and a test building its own `Sandbox`, flipping the flag to `false`
/// would leave every test green while the jail silently lost `/dev/null` and
/// friends — design decision 12's whole point, and invisible until something
/// reads a device file. The test is about the code that runs only if the test
/// calls the code that runs.
// The gate is the two places that read this: the arms, and the tests below —
// which exercise it on every platform, and are what keeps the exposure rule a
// property this example can lose. A build with neither would otherwise carry a
// dead function, and every build compiles this example now that it has no
// `required-features`.
//
// **The `.device_files()` below is a Linux concept and a no-op on Windows**
// (the exposures this crate's backend computes are the whole whitelist), and it
// stays where it is: the flag is an argument of the construction rather than
// something the set remembers, so the two arms share one construction rather
// than each stating the flag — a second copy of either the flag or the rule
// beside it is the second definer this function exists to prevent (design
// decision 12).
#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
pub fn sandbox_request(root: &Path, state_dir: &Path, access: AccessMode) -> Sandbox {
    let sandbox = Sandbox::new().expose(root, access);
    let sandbox = if path_contains(state_dir, root) {
        sandbox
    } else {
        sandbox.expose(state_dir, AccessMode::ReadWrite)
    };
    sandbox.device_files()
}

/// Design §9's refused combination, refused in the launcher rather than inside
/// the confinement: a read-only root with a state directory under it cannot be
/// served, and `FsStorage` would reject it a process too late.
///
/// **Refused here because the launcher is what would have granted the wider
/// access** — a read-write entry inside a read-only root is a hole in the
/// exposure set, not a storage error — and it is the same containment question
/// [`sandbox_request`] answers, through the same definer: a state directory
/// judged "outside" a root it is actually inside is exactly the case this guard
/// exists for, and on Windows two spellings of one directory are how that
/// mistake arrives (`TINIO_STATE_DIR=e:\Data\state` under a read-only
/// `E:\Data`). A literal comparison would let that run through to the exposure
/// set, where the state directory would be granted read-write inside a root the
/// operator asked to be read-only.
///
/// **The two paths are the ones the operator wrote, not their canonical
/// forms.** The refusal is about the pair the run names — the root as the
/// exposure set carries it, and `TINIO_STATE_DIR` as written or the derived
/// `<root>/.tinio` — because that is the pair the grant it prevents is built
/// from. A guard decided over canonical forms would be deciding about paths the
/// exposure set never sees: a second question wearing the first one's name,
/// which is what the shared definer exists to stop rather than to add.
///
/// **Nothing here is Windows-specific, and the gate says so** — it is the arms'
/// gate widened by `test`, the same shape [`sandbox_request`] carries. The
/// example above names Windows because that is where two spellings of one
/// directory actually arrive, but the *rule* is the shared definer's, and a
/// guard under `cfg(target_os = "windows")` could be unit-tested on one host
/// only — while its call site is the launcher's arm, which reaches it with a
/// root and a state directory that come from the operator's own strings. Three
/// pure values in, a decision out; a test that pays for a sandbox feature, a
/// read-only mode and a spawned process to ask it is testing the plumbing
/// rather than the rule.
#[cfg(any(
    test,
    all(feature = "sandbox", any(target_os = "linux", target_os = "windows"))
))]
pub fn refuse_in_root_state_dir(
    read_only: bool,
    root: &Path,
    state: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if read_only && path_contains(state, root) {
        return Err(
            "read-only mode cannot serve a root that holds the state directory; set \
             TINIO_STATE_DIR outside the served root"
                .into(),
        );
    }
    Ok(())
}
