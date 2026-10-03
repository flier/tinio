//! Whether **this host** can confine a process, and what to say when it cannot.
//!
//! **This is the run-time half of a question two other answers already own.** The build half
//! is the facade's `backend_available()`, a `const fn` over the target list; the failure
//! half is a bare HRESULT an operator cannot act on. What lives here is the third answer:
//! the platform supports confinement, and this token does not get to use it.

use std::sync::OnceLock;

use tracing::dispatcher;
use windows_sys::Win32::Foundation;

use crate::{
    _core::Error,
    sid::{PackageSid, ProfileLock, clear_profile},
};

/// `E_ACCESSDENIED`: the token is not allowed to do this. **The only HRESULT this module
/// treats as a capability answer** — see `classify`.
///
/// The value is the platform crate's own (`windows_sys::Win32::Foundation::E_ACCESSDENIED`)
/// rather than a transcription: this is a security boundary, and a hand-copied HRESULT is the
/// same class of mistake `docs/cargo.md` records for the hand-written `SID_AND_ATTRIBUTES`
/// layout that `windows-sys` replaced.
pub(crate) const E_ACCESSDENIED: i32 = Foundation::E_ACCESSDENIED;

/// The two actions a refused operator has, as one sentence.
///
/// **The second field of the core variant, and the tail of the sentence the CLI prints as
/// `{cause} — {remedy}`.** No `host_cannot` caller passes it as that function's third argument
/// (the plan-grilling ruling, round 2, Q10): the third argument is appended with no separator
/// (`tinio-util/src/testing.rs:2946-2953`), so each site keeps a pointer at its own subject
/// instead. It still reaches one message, inside a *reason*: the Windows harness hands
/// `host_can_confine`'s error — this module's rendering of both fields — to `host_cannot`
/// (`crates/tinio-server/tests/support/windows_e2e_harness.rs:555-558`), so the sentence a
/// skipped leg prints carries the same text a refused run prints.
pub(crate) const REMEDY: &str = "run in an ordinary user session, and check or clear the Low mandatory label on the tree the binary was built in";

/// The fact and its most likely cause, with the HRESULT kept verbatim.
pub(crate) fn cause(hr: i32) -> String {
    format!(
        "this token cannot create an AppContainer profile (hr=0x{:08X}), so no confined process could be started",
        hr as u32
    )
}

/// The error a capability refusal raises, built from the two halves above.
pub(crate) fn cannot_confine(hr: i32) -> Error {
    Error::HostCannotConfine {
        cause: cause(hr),
        remedy: REMEDY,
        hr,
    }
}

/// The verdict an HRESULT gets: the error it raises, or `None` when it is not a capability
/// answer at all.
///
/// **The narrowness is the point.** A greedy predicate turns a transient race into a
/// permanent claim about the machine, which is worse than the bare HRESULT it replaces.
pub(crate) fn classify(hr: i32) -> Option<Error> {
    (hr == E_ACCESSDENIED).then(|| cannot_confine(hr))
}

/// The classified refusal, **emitted once and returned** — the one producer of the
/// `step = "host cannot confine"` event.
///
/// **Why one function rather than a `warn!` at each site.** The refusal is reached from two
/// places — [`admit`], the gate that answers the probe's verdict, and `PackageSid::derive`'s
/// create arm (`sid.rs`), the floor that classifies the call that actually ran — and they used
/// to spell the event in two shapes, one carrying the profile's name and one carrying only the
/// HRESULT. This module's argument for the step name applies to the event it names: a copy at
/// each site is one more place the message and the fields can drift. `subject` is the profile
/// whose creation was refused — the probe's own fixed name ([`PROBE_NAME`]) at the gate, the
/// derived package name at the real call site — and the field keeps `derive`'s `name` spelling,
/// so the floor's event is unchanged and the gate's now carries the same field.
///
/// **The caller classifies.** Both call sites have already established that `hr` is the
/// capability answer, so this builds the refusal rather than re-deciding; an HRESULT
/// [`classify`] does not recognise is never announced as one.
pub(crate) fn refuse(hr: i32, subject: &str) -> Error {
    tracing::warn!(
        step = "host cannot confine",
        name = subject,
        hr = hr,
        "the token cannot create an AppContainer profile; the refusal names the cause and the remedy"
    );
    cannot_confine(hr)
}

/// The profile the probe creates and deletes. **Its own name, and that is a requirement
/// rather than a convenience.**
///
/// (It is also the `subject` [`refuse`] names when the gate refuses: the profile whose
/// creation the probe asked for is the one the event should name.)
///
/// `package_name(root)` derives a real root's exact profile name, so a probe using it would
/// take over a live run's profile and its `Drop` would then delete it — and `package_name`
/// also reads or creates `%LOCALAPPDATA%\tinio\acl-salt`, a side effect a capability answer has
/// no business having. A fixed name under `PACKAGE_PREFIX` is instead reaped by the
/// stale-profile sweep if a crash orphans it, and cannot collide with a root's derived
/// `<prefix><16 hex>` name — the sweep's filter is a bare `starts_with(PACKAGE_PREFIX)` on both
/// of its listings (`sweep.rs:393-401`, `:475-478`) with no hex-shape test at all, so this
/// non-hex leaf is still a candidate. The sweep is skipped while another `tinio` is alive
/// (`sweep.rs:298-300`), so a crashed probe's residue is reaped by a later process.
pub(crate) const PROBE_NAME: &str = "tinio.sandbox.capability-probe";

/// The cached verdict, or this probe's — defaulting its cost to once.
///
/// **The cell is the caller's, and the production one is a bare `static`.** A `OnceLock` in a
/// one-field wrapper would carry a const constructor and a derived `Default` for no reader, and
/// the workspace's shape for a process-wide memo is the bare `static` (`sid`'s
/// `ensure_state_directory`, `tinio-cli`'s `STOPPER`, `tinio-util`'s `capture`); taking the cell
/// as an argument is also what makes this testable without reaching for the static.
///
/// **Only a definitive answer is stored.** A token does not change under a running process, so
/// `Ok` and `E_ACCESSDENIED` are answers worth keeping; an HRESULT `classify` does not recognise
/// is not an answer at all, so nothing is stored for it and a later call re-probes.
///
/// **A probe that cannot answer lets the run through.** Answering `Err` here would make the
/// gate refuse a host whose only problem was a transient race inside `userenv`, and a false
/// negative on a machine that works is worse than the bare HRESULT this change exists to
/// replace. Nothing is lost by proceeding: the real `CreateAppContainerProfile` classifies
/// its own HRESULT, so a host that truly cannot confine still gets the cause and the two
/// remedies — one step later, after the state directory. The `warn` is what makes the
/// degradation visible instead of silent.
fn get_or_probe(
    cache: &OnceLock<Window<()>>,
    probe: impl FnOnce() -> Result<Window<()>, Error>,
) -> Result<(), Error> {
    if let Some(cached) = cache.get() {
        return verdict(*cached);
    }
    match probe() {
        Ok(probed) => {
            let _ = cache.set(probed);
            verdict(probed)
        }
        Err(err) => {
            tracing::warn!(
                step = "host cannot confine",
                error = %err,
                "the capability probe could not answer, so this run proceeds and the real profile creation will report any refusal"
            );
            Ok(())
        }
    }
}

/// The verdict, as the predicate's answer.
///
/// **The one rule, applied**: [`Window::into_result`] is where a window becomes an answer, and
/// this names the place the predicate reads it.
fn verdict(verdict: Window<()>) -> Result<(), Error> {
    verdict.into_result()
}

/// The process-wide verdict.
static VERDICT: OnceLock<Window<()>> = OnceLock::new();

/// Whether **this process's token** is refused an AppContainer profile.
///
/// **Host-level and process-scoped**: it is asked about the token, not about a root, a path or
/// an access mode, so its answer is one per process rather than one per run.
///
/// # Errors
///
/// `Error::HostCannotConfine` when the token is refused. Any other failure is the probe's own:
/// it is logged at `warn`, nothing is cached, and the answer is `Ok` — a probe that cannot
/// answer must not refuse a host that works.
///
/// # Examples
///
/// ```
/// // The answer is this host's: `Ok` where the token may create an AppContainer
/// // profile, and a classified refusal where it may not.
/// match tinio_sandbox_windows::capability::can_confine() {
///     Ok(()) => {}
///     Err(err) => eprintln!("{err}"),
/// }
/// ```
pub fn can_confine() -> Result<(), Error> {
    admit().map(|_admitted| ())
}

/// The proof that this process's token may create an AppContainer profile.
///
/// **A witness, and the launcher's ordering is what it buys.** Its field is private to this
/// module, so `admit` is the crate's only constructor and no call site can invent one;
/// [`prepare_launch`](crate::backend::prepare_launch) demands it as a parameter, so moving the
/// crate's first side effect above the host gate — the failure design §The gate exists to
/// prevent — stops compiling instead of reddening a test. That test read this module's source
/// text to pin the order until this type existed.
pub(crate) struct Admission(());

/// The predicate's answer as that witness — **through the refusal's one producer.**
///
/// **The step event belongs to this module rather than to either gate** (design §The message).
/// It sat on the predicate before the witness existed and it stays on the answer's only
/// producer: both gates reach the verdict — the CLI's `refuse_unhonourable` through
/// `can_confine`, the launcher through `prepare_launch`'s argument — so one emit covers every
/// refusal path, and a copy at each gate would be two more places the step name can drift.
/// [`refuse`] is the one producer, and `PackageSid::derive`'s floor calls it too, so the
/// early gate and the call that actually ran say the same thing. The probe's own events stay
/// silenced (`probe_once`); this call is outside that guard, because by the time it runs the
/// dispatcher is the caller's again.
pub(crate) fn admit() -> Result<Admission, Error> {
    match get_or_probe(&VERDICT, probe_once) {
        Ok(()) => Ok(Admission(())),
        // The only error `get_or_probe` answers with is the classified refusal: the probe's
        // own failures are logged and answered `Ok` inside it.
        Err(Error::HostCannotConfine { hr, .. }) => Err(refuse(hr, PROBE_NAME)),
        Err(other) => Err(other),
    }
}

/// What one trip through the probe's profile window produced.
///
/// **`pub(crate)`, because the window has one reader outside this module.** The crate's
/// `testing` module hands its `with_probe_profile` to [`Window::into_result`] — the device
/// `tinio-server`'s Windows e2e harness asks for the probe's live SID — while the enum stays
/// off the public surface, which is the predicate's alone.
///
/// **`Copy`, because the verdict is cached and read back by value**: `get_or_probe` stores one
/// `Window<()>` in a `OnceLock` and answers every later caller from it.
#[derive(Clone, Copy)]
pub(crate) enum Window<T> {
    /// The profile was created; this is the closure's answer.
    Used(T),
    /// The creation was refused for the reason that is this host's, not this run's.
    Cannot(i32),
}

impl<T> Window<T> {
    /// The window as a result: the closure's answer, or the refusal's classified error.
    ///
    /// **The one place `Cannot` becomes an error.** It has two callers — the predicate's
    /// `verdict` and `testing::with_probe_profile` — and each used to spell the
    /// `cannot_confine(hr)` translation for itself; this is the same rule in two shapes that
    /// could drift apart. It also retires `Probe`, the private enum that existed only to carry
    /// this translation one step further.
    ///
    /// **`pub(crate)` rather than private**, because the `testing` module is the second
    /// caller, and it matches the same window the predicate reads.
    pub(crate) fn into_result(self) -> Result<T, Error> {
        match self {
            Self::Used(used) => Ok(used),
            Self::Cannot(hr) => Err(cannot_confine(hr)),
        }
    }
}

/// The probe's recipe, with the caller's work running while the profile is alive.
///
/// **One recipe, two readings**: `probe_once` reads "this token could create a profile" out of
/// it, and the crate's `testing` module hands the same live `PackageSid` to a caller that needs a
/// real AppContainer SID — so neither has to derive a profile of its own.
/// `PackageSid`'s lifetime **is** create-then-delete, so this introduces no new side-effect
/// class and leaves no residue on the success path.
///
/// **`f` runs inside the silenced window and under `ProfileLock`**: it must not depend on being
/// logged, and it must not block, because a probe of the same name in another process is waiting
/// on that lock. It is not `async` for the reason the dispatcher guard below gives.
///
/// **`pub(crate)` and not public.** This module's public surface is the predicate
/// ([`can_confine`]) alone; the window the probe opens is served to test builds through
/// `testing::with_probe_profile`, which is where a device a `tests/` target must share belongs
/// (`docs/tests.md`).
pub(crate) fn probe_window<T>(f: impl FnOnce(&PackageSid) -> T) -> Result<Window<T>, Error> {
    // **The probe's acts are not any run's steps.** `PackageSid::derive` announces every profile
    // it creates (`step = "CreateAppContainerProfile"`) because that line is how an operator
    // answers "was this process confined, and to what" — and the probe's throwaway profile is not
    // what the process was confined to, so it must not appear in a run's log at all. The delete
    // half announces the same way, which is why this guard spans the drop below as well: it is a
    // thread-local default dispatcher, and every event the probe's own `derive`, use and drop
    // raise is discarded between here and this function's end.
    //
    // **No `await` and no other yield point may be introduced between entry and exit**, because
    // the dispatcher is thread-local: a suspension inside the guard could silence a *different*
    // task's events on this thread. This probe is synchronous and stays so.
    //
    // **The answer is unchanged.** The classified refusal still travels as the returned `Error`,
    // and the caller (CLI, doctor, harness) still prints the sentence. What is deliberately lost
    // is `sid.rs`'s failure-path `tracing::warn!(step = "host cannot confine", …)` — silent while
    // the probe runs, and that is acceptable because the refusal is the *error's* content, which
    // every caller surfaces, rather than the log line's.
    let _quiet = dispatcher::set_default(&dispatcher::Dispatch::none());

    // **Best-effort, and held across the clear, the create *and* the delete** (the
    // plan-grilling ruling).
    // The name is fixed, so without it two processes probing at once interleave: the second sees
    // `ALREADY_EXISTS` and takes the profile over, the first's `Drop` deletes it, and the
    // second's `Drop` then reports a deletion for a profile that had existed moments earlier —
    // `DeleteAppContainerProfile` answers `S_OK` for an absent name, so that arm is the
    // `absent = true` **info**, not a warn (`sid.rs:1395-1434`, pinned by `sid.rs:2334-2370`):
    // the log says "deleted" about someone else's probe. A third process can also catch a
    // transient `0x80070003` in the window. **Only the log and that window need the lock, so a
    // lock that cannot be created does not block the answer.** Re-entrant on the owning thread
    // (`sweep.rs:998-1001` states the rule), so `PackageSid`'s own `Drop`, which acquires the
    // same name, counts up rather than deadlocking.
    let _lock = ProfileLock::acquire(PROBE_NAME);

    // **Clear the name before creating it.** `CreateAppContainerProfile` is asked to
    // *create*, and it answers `ALREADY_EXISTS` for a name that is already there — `derive`
    // then takes that profile over instead of making one. Clearing first is what makes the
    // create below a real creation, so on this host the answer cannot be produced by the
    // residue a previous probe left. For a token the API admits, `DeleteAppContainerProfile`
    // answers `S_OK` for an absent name (the `Drop` measurement in `sid.rs`), so on a clean
    // machine the clear is a no-op. **The justification is the call's own property rather than
    // an observed failure**: this probe asks the API to create, not to take over.
    //
    // **For a token the profile API refuses, the clear is refused deterministically**, and
    // that is what makes the probe's answer robust rather than lucky. The wave-E review
    // measured the clear answering `0x80070005` **3000/3000** with a genuine Low token while
    // two Medium processes churned the same name, where the create *alone* hit the transient
    // `0x80070020` (`ERROR_SHARING_VIOLATION`) 1/3000 — so a raced create is no longer the
    // call that decides. Task 4 fix 3 measured the same refusal for an absent name under the
    // Low token.
    //
    // **The refusal can come from either half of the profile's life** — the clear or the
    // create — and the conclusion does not depend on which half refused: a token the profile
    // API refuses cannot confine, whichever call met the refusal. `clear_profile` answers the
    // refusal `classify` recognises as its only `Err`, so the operator reads the identical
    // sentence `derive`'s own refusal would produce (`cannot_confine`), and nothing
    // downstream has to know which call was refused.
    if let Err(Error::HostCannotConfine { hr, .. }) = clear_profile(PROBE_NAME) {
        return Ok(Window::Cannot(hr));
    }
    // **Every other clear outcome falls through to the create**, whose own classification
    // below is unchanged. The clear is best-effort: an unrecognised non-zero HRESULT leaves
    // the name possibly still present, so the create may take that profile over — the
    // pre-existing path, not a new one. Answering here instead would say `Ok` (warn, nothing
    // cached — `get_or_probe`) without the create ever being asked, and mask the refusal the
    // create would have caught. That was the wave-E review's finding 2.

    // **The window this leaves is real and is not claimed closed.** Another process's probe can
    // create the name between this clear and the create below — the lock is best-effort and
    // serialises the probes that take it rather than every writer of the name — so the create
    // can still take a live profile over. **On this host that cannot turn a refusal into
    // `Confined`**: the permission check precedes the existence check, so a token the API
    // refuses gets `E_ACCESSDENIED` even with the name present and never reaches the takeover
    // (measured by Task 4 fix 3 with the residue present: create `0x80070005`, never
    // `0x800700B7`, and again independently by the wave-E review). The residue/takeover is
    // therefore a *possible* hole, on a build that checks existence before permission, and this
    // host does not exhibit it. The seeded e2e scenario is a state fixture with a regression
    // role, not a reproduction of an observed defect.
    match PackageSid::derive(PROBE_NAME) {
        Ok(sid) => {
            let used = f(&sid);
            drop(sid);
            Ok(Window::Used(used))
        }
        Err(Error::HostCannotConfine { hr, .. }) => Ok(Window::Cannot(hr)),
        Err(other) => Err(other),
    }
}

/// One probe: clear the throwaway name, create the profile and drop it.
///
/// `PackageSid`'s lifetime **is** create-then-delete, so this introduces no new side-effect
/// class and leaves no residue on the success path. The `Window<()>` the recipe answers is the
/// predicate's reading of it — "the closure ran" is all this caller asked for.
fn probe_once() -> Result<Window<()>, Error> {
    probe_window(|_sid| ())
}

// **There is no test hook, and that is the ruling rather than an omission.** The first draft
// carried a `testing`-featured `override_probe`; it had no possible consumer —
// `tinio-cli` does not dev-depend on this crate at all — and the `OnceLock` verdict would
// have made a forced refusal order-dependent inside a shared test binary. What is exercised
// instead is the real thing: Task 11 runs `tinio doctor` under a genuinely Low token, where
// this predicate answers `Cannot(E_ACCESSDENIED)` on its own.

#[cfg(test)]
mod tests {
    use std::io;

    use tinio_util::testing::{assert_recorded_at, assert_step};

    use super::*;
    use crate::{
        _core::fail,
        sid::PACKAGE_PREFIX,
        testing::{capture, recorder},
    };

    #[test]
    fn only_access_denied_is_a_capability_answer() {
        // The one HRESULT that means "this token may not".
        assert!(classify(E_ACCESSDENIED).is_some());
        // `ERROR_PATH_NOT_FOUND` is a concurrent create inside `userenv`
        // (`backend/tests.rs`), and a transient one: caching it would declare a
        // healthy machine permanently incapable.
        assert!(classify(0x8007_0003u32 as i32).is_none());
        // Success is not a refusal, and neither is an unknown code.
        assert!(classify(0).is_none());
        assert!(classify(0x8007_00B7u32 as i32).is_none());
    }

    #[test]
    fn the_refusal_names_the_code_and_the_two_actions() {
        let err = classify(E_ACCESSDENIED).expect("E_ACCESSDENIED is the capability answer");
        // **The whole sentence as one literal.** `contains(REMEDY)` would pass with `REMEDY`
        // rewritten — the text is rendered *from* it — so the remedy half was pinned nowhere.
        assert_eq!(
            err.to_string(),
            "this token cannot create an AppContainer profile (hr=0x80070005), so no confined process could be started — run in an ordinary user session, and check or clear the Low mandatory label on the tree the binary was built in"
        );
        // And `0x80070005` has no A-F digit and is already eight digits wide, so it renders the
        // same under `{:08X}`, `{:X}` and `{:x}`. Pinning the format needs a value that shows
        // both its case **and** its width, and only a value shorter than eight digits shows the
        // width: `{:X}` renders `0x0007_00B7` as `700B7`, and the pin is `{:08X}`, not the
        // space-padded `{:8X}`, which would render it as `   700B7`.
        assert!(
            cause(0x0007_00B7u32 as i32)
                .ends_with("(hr=0x000700B7), so no confined process could be started"),
            "the HRESULT is not rendered as the brief's {{:08X}}"
        );
        assert!(
            matches!(err, Error::HostCannotConfine { hr, .. } if hr == E_ACCESSDENIED),
            "{err}"
        );
    }

    /// **The refusal's one producer emits the event the design's log surface promises**, and
    /// answers with the same classified error `classify` would.
    ///
    /// This is where `sid.rs`'s source-text assertion ("the refusal emits the step event")
    /// moved when the `warn!` left `derive` for [`refuse`]: a structural test on `derive` can
    /// only show the delegation now, so the event itself is recorded here, at its owner.
    #[test]
    fn the_refusal_is_emitted_by_its_one_producer() {
        let _serial = recorder();
        let capture = capture();
        let recording = capture.record();
        let err = refuse(E_ACCESSDENIED, "tinio.sandbox.0123456789abcdef");
        let logged = capture.text();
        drop(recording);

        assert!(
            matches!(err, Error::HostCannotConfine { hr, .. } if hr == E_ACCESSDENIED),
            "{err}"
        );
        assert_step(&logged, "host cannot confine");
        assert_recorded_at(
            &logged,
            "the refusal names the cause and the remedy",
            tracing::Level::WARN,
        );
        // The subject is a rendered field, and the profile it names is the one the caller
        // passed — the gate passes the probe's own name, the floor the derived package name.
        assert!(
            logged.contains("name=\"tinio.sandbox.0123456789abcdef\""),
            "the refused profile is a field of the event: {logged}"
        );
    }

    #[test]
    fn the_probe_name_is_its_own_and_sweep_eligible() {
        // Sweep-eligible: the reaper's candidate filter is `PACKAGE_PREFIX`.
        assert!(PROBE_NAME.starts_with(PACKAGE_PREFIX));
        // Its own: never a root's derived name, which is the prefix plus 16 hex digits.
        assert_eq!(PROBE_NAME, "tinio.sandbox.capability-probe");
        let leaf = PROBE_NAME.trim_start_matches(PACKAGE_PREFIX);
        assert!(
            !leaf.chars().all(|c| c.is_ascii_hexdigit()),
            "a hex leaf would collide with a real root's name"
        );
    }

    #[test]
    fn only_a_definitive_answer_is_cached() {
        // A fresh cell per test: the production one is a process-wide static.
        let confined = OnceLock::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = get_or_probe(&confined, || {
                calls += 1;
                Ok(Window::Used(()))
            });
            assert!(answer.is_ok());
        }
        assert_eq!(calls, 1, "a definitive answer is probed once");

        let refused = OnceLock::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = get_or_probe(&refused, || {
                calls += 1;
                Ok(Window::Cannot(E_ACCESSDENIED))
            });
            assert!(matches!(answer, Err(Error::HostCannotConfine { .. })));
        }
        assert_eq!(calls, 1, "a definitive refusal is probed once");

        let unknown = OnceLock::default();
        let mut calls = 0;
        for _ in 0..5 {
            let answer = get_or_probe(&unknown, || {
                calls += 1;
                Err(fail("probe", io::Error::from_raw_os_error(3)))
            });
            // **`Ok`, not `Err`**: the brief's own implementation text says so ("nothing is
            // stored and the answer is `Ok`"), and the `calls` assertion below is what pins the
            // property that matters — an unrecognised failure must never be remembered. Its
            // draft asserted `answer.is_err()` here, which contradicts the `Ok` the same brief
            // specifies and the `Err` the core variant already carries one step later.
            assert!(
                answer.is_ok(),
                "an unanswerable probe must not refuse the host"
            );
        }
        assert_eq!(calls, 5, "an unrecognised failure is never remembered");
    }
}
