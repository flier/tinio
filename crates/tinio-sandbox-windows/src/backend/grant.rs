//! The grant phase's budget, its warning threshold, and the event that reports
//! the measurement.
//!
//! **This is the half of `backend`'s module root that is prose rather than
//! orchestration** (`docs/style.md`'s thin-module-root rule): `grant_whitelist`
//! runs the pass, and this module owns the two durations that pass is measured
//! against and the one function that reports it, so the root is left with the
//! stages and their `use`s. Nothing here calls Win32, and so nothing here is an
//! `unsafe` site (design decision 14).

use std::time::Duration;

/// The readiness budget a confined launch has to fit inside — **30 s**, the
/// number design §6's ACL measurement is stated against and the deadline a
/// consumer of a launch times it to. The number has one definer and two
/// restatements beside this one already: `crates/tinio-e2e/tests/steps/clients.rs`
/// defines it as `START_TIMEOUT` — the receive timeout `wait_for_ready` reads the
/// readiness frame under — and this crate's own suites restate it as
/// `tests/common/mod.rs`'s `TIMEOUT`, in the same words about why.
///
/// **It is a restatement and not a shared constant, which is the honest shape
/// rather than a shortcut.** The budget belongs to the consumer: a backend is
/// handed a [`Sandbox`] and a [`LaunchPlan`] and nothing else, so no launcher is
/// ever told how long it has. What this crate can do with the number is measure
/// its own phase against it and say so when the phase takes a share of it, which
/// is [`report_grant_phase`]'s job.
///
/// [`Sandbox`]: crate::_core::Sandbox
/// [`LaunchPlan`]: crate::_core::LaunchPlan
const READINESS_BUDGET: Duration = Duration::from_secs(30);

/// How long the ACL grant phase may take before the run says so at `warn!`:
/// **half the readiness budget**, so 15 s.
///
/// **A share of the budget rather than the budget, and the share is a
/// judgement** — stated as one, because no divisor of the budget is derivable
/// from the measurement (one 200,200-object tree's rollback differed by a factor
/// of 1.7 between two runs of it, so §6's numbers are magnitudes rather than
/// constants). Three reasons decide it, in the order they matter:
///
/// - **The grant is one phase of a launch and not the whole of it.**
///   `CreateProcessW`, the job, the relay, the child's own startup and its
///   readiness handshake all come after this phase and run against what it left,
///   so a phase that has spent half the budget has halved the room everything
///   else was given.
/// - **A threshold *at* the budget would speak only after the deadline the
///   number exists to explain.** A 20 s grant — a run that reads as a hang to
///   anyone watching it — would cross no line and leave nothing in the log.
/// - **The measured case is far past every share of it.** 200,200 objects cost
///   32–38 s of granting on NTFS (design §6, 2026-09-22), twice this threshold
///   and past the budget itself, so any divisor between a third and the whole of
///   it separates that case from a healthy run; half is the one that is
///   unambiguous rather than marginal.
///
/// **What the threshold is not is a pre-check's input.** It compares a duration
/// this launcher has already spent; nothing here walks a tree to predict one,
/// because that walk is the cost the budget is being spent on (the function
/// below states it at length).
///
/// **The arithmetic is spelled `15` rather than a division of the constant
/// above, because it cannot be the latter on this toolchain**: `Duration`'s
/// `Div<u32>` is stable for values and not for constants, and a `const` that
/// divides one is rejected `E0658` ("`Div` is not yet stable as a const trait",
/// measured 2026-09-23 on `rustc 1.98.1`). The half is therefore stated here and
/// in the doc above, which is the one place the two constants can drift apart —
/// hence the sentence rather than a silent `15`.
pub(crate) const GRANT_PHASE_WARN_AFTER: Duration = Duration::from_secs(15);

/// Report what the ACL grant phase cost, and say so out loud when it took a
/// share of the readiness budget.
///
/// **Every run gets the measurement; only a slow one gets the warning.** The
/// `debug!` carries the figure for any run whose subscriber enables it (shared
/// §7's per-operation event, so a debugging operator has the number whatever it
/// was), and [`GRANT_PHASE_WARN_AFTER`] decides the `warn!` beside it. That
/// split is what makes the loud half visible where an operator already is:
/// these events reach the launcher's stderr through the subscriber its caller
/// installed, and the serving process builds that one through its resolved
/// `[log] verbosity` (`crates/tinio-cli/src/commands/start/run.rs`), so the
/// warning is what a default run shows and the measurement is opt-in.
///
/// **What the warning promises is the number and nothing else.** It cannot make
/// the run fit the budget and is written not to read as though it could: the
/// pass is the filesystem's own inheritance over every object under every
/// exposed directory (the `acl` module's doc carries the measurement — 200,200
/// objects, 32–38 s of granting on NTFS against 8.6–8.8 s on ReFS, with the
/// second pass of the same shape measured at 27–47 s before the grants became
/// standing), so what shortens it is the operator's choice of what to expose.
/// The message therefore points at the design's own remedy — expose the
/// narrowest directory that serves the corpus, or accept the cost — and says in
/// as many words that this line reports the pass rather than shortening it.
///
/// **It is emitted when the phase ends and before the child exists**, which is
/// both the earliest it can be accurate and late enough to explain a wait after
/// the fact: a consumer that has already timed out leaves the figure in the
/// launcher's log for whoever reads it, and that is the whole difference between
/// "nothing said why" and "the ACL pass took 34 s". A heartbeat *during* the
/// phase would need a timer thread, and what an operator does with the answer
/// does not change with the progress it would show.
///
/// **The fields carry what is cheap and true, and what is missing is the point.**
/// `elapsed_ms` is the phase's whole-millisecond duration; `exposures` is the
/// resolved exposure set's length — the same number `grant_whitelist`'s `info!`
/// event carries, and deliberately not the number of `icacls` invocations, which
/// the batched sibling grant makes a different number — and `threshold_ms` rides
/// the warning alone, so the rule a run crossed is in the line rather than only
/// in this source. **No field states the size of any tree**, because this
/// launcher does not know it and cannot learn it cheaply: an estimate is a walk
/// of the corpus, which is the O(corpus) pass on the very path whose budget is
/// short, and one `read_dir` of a directory is not the size of the tree beneath
/// it. **The program's own directory is not a field either, and that is a
/// decision**: it is the one exposed directory whose ACE does not propagate
/// (decision 22, as Ruling 78 reshaped it — one ACE on the directory and one
/// batched ACE on the `*.dll` files directly beside the program), so naming it
/// in a message about cost would point the reader at the one entry that is not
/// one.
///
/// **It is a free function rather than a block inside `confine`, so that a test
/// can pin the threshold and the level without a launch**: `backend::tests` calls
/// it with a chosen duration and reads the level back off its capture, which is
/// the only way to assert both halves of the decision at once.
///
/// **Measured on this host, 2026-09-23** (`D:\Temp`, where `%TEMP%` resolves:
/// NTFS, the filesystem §6's slow half was measured on): a freshly built tree of
/// **5,201 objects** granted in **272 ms, 283 ms and 313 ms** across three runs,
/// with rollbacks of **261 ms, 292 ms and 397 ms** — 0.052–0.060 ms of granting
/// per object. **A tree of a few thousand objects does not reach this
/// threshold**, and the two measurements read together put it in the 10^5
/// objects: 15 s of granting is ~250,000 objects at that small tree's per-object
/// rate and ~90,000 at §6's for its 200,200-object tree (0.16 ms/object) — the
/// two rates differ by a factor of three, and §6 records that the shape of the
/// curve is not yet measured, so what to carry away is the order of magnitude
/// rather than a count. What this warning is for is the corpus §6 measured, not
/// the one a launcher is usually handed.
pub(crate) fn report_grant_phase(elapsed: Duration, exposures: usize) {
    let elapsed_ms = elapsed.as_millis() as u64;
    tracing::debug!(step = "grant the ACLs", elapsed_ms, exposures);
    if elapsed < GRANT_PHASE_WARN_AFTER {
        return;
    }
    let threshold_ms = GRANT_PHASE_WARN_AFTER.as_millis() as u64;
    tracing::warn!(
        step = "grant the ACLs",
        elapsed_ms,
        exposures,
        threshold_ms,
        "the ACL grant phase took {elapsed:?}, half or more of the {READINESS_BUDGET:?} readiness budget a confined launch has to fit inside; the grant touches every object under an exposed directory (the filesystem's inheritance, not a walk this launcher makes), so expose the narrowest directory that serves the corpus or accept the cost — this line reports the pass, it does not shorten it (design §6)"
    );
}
