//! Test-only helpers, kept because two test modules need the same one.

use std::{cell::RefCell, io, sync::OnceLock};

use tracing::{Level, callsite::rebuild_interest_cache};
use tracing_subscriber::fmt::MakeWriter;

thread_local! {
    /// What this thread is recording, or `None` when it is not recording at all.
    ///
    /// **A thread-local rather than a shared buffer behind a lock, and the reason
    /// is a fork.** `mechanism::userns_available`'s probe forks, and the child
    /// inherits whatever subscriber the parent had: an event the child emits
    /// through a *locked* buffer lands in a critical section a sibling thread may
    /// have been holding when the fork happened, and the child then blocks until
    /// the probe's five-second deadline — measured, on the first shape of this
    /// helper, as `userns_available`'s own test failing in five seconds. A
    /// thread-local takes no lock on the write path, and the common case is not
    /// even reached: a forking test is not a recording one, so the child inherits
    /// `None` and its event is dropped without allocating.
    static BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// Everything `body` logged, as the subscriber rendered it, beside what `body`
/// returned.
///
/// The crate installs no subscriber of its own (shared §7), so a test that wants
/// to read this crate's events has to be the one that captures them.
pub(crate) fn record_events<T>(body: impl FnOnce() -> T) -> (T, String) {
    install();
    // The buffer is emptied at both ends rather than dropped at the start: a
    // panic in the body leaves it in place, and the next recording on this
    // thread clears it, so a failed test cannot bleed into a later one.
    BUFFER.with(|buffer| *buffer.borrow_mut() = Some(Vec::new()));
    let value = body();
    let logged = BUFFER.with(|buffer| {
        let text = buffer.borrow_mut().take().unwrap_or_default();
        String::from_utf8(text).expect("the capture is UTF-8")
    });
    (value, logged)
}

/// Shared §7's check, as one assertion: the event carries `step` as a rendered
/// field.
///
/// **A rendered field and not a substring of the whole line**, which is the
/// distinction the core's own recorder test states at length: the operation's
/// name appears inside the message and inside any error the same call returns,
/// so `contains("jail_root")` would pass for an event whose `step` field said
/// something else. The log is in the failure message, because a recording that
/// came out empty is the other half of what can go wrong here.
pub(crate) fn assert_step(logged: &str, step: &str) {
    assert!(
        logged.contains(&format!("step=\"{step}\"")),
        "no `step=\"{step}\"` in:\n{logged}"
    );
}

/// Install the recording subscriber, once for the whole test binary.
///
/// **Global and installed once, rather than a thread-scoped `with_default`, and
/// that is a measured correction rather than a preference.** `tracing` caches
/// each callsite's `Interest` in a **process-global** cell, written both when a
/// callsite is first hit and when a dispatch is created — and while exactly one
/// dispatch is registered, which is the whole of a `with_default` recording,
/// `tracing-core`'s `Rebuilder::JustOne` consults *the calling thread's* default
/// rather than the registry. A thread-scoped subscriber is therefore invisible to
/// every other thread, so a parallel test that happens to be the first to hit the
/// same callsite writes `never` into the shared cell and the event this test
/// exists to observe is dropped in silence. **This crate's test modules share
/// callsites with a dozen other tests** (`resolve_model` is reached by the refusal
/// tests, `jail_root` by every `jail.rs` test), so the shape that flaked in
/// `tinio-sandbox-windows`'s capture is the shape this file would have had.
///
/// A global subscriber is visible from every thread, so the cell can only ever
/// say "interested". Which thread's lines a recording reads is still decided per
/// thread, by [`BUFFER`], so two tests can record at once.
///
/// **`try_init` and never `init`** (shared §7): a subscriber the caller already
/// installed wins, and if one ever is, the capture stays empty and the assertions
/// fail loudly rather than passing on a recording that never happened.
///
/// The level is `DEBUG` rather than §7's `Info`, because the per-item half is a
/// core operation too: a capture at `Info` would leave every `debug!` site with
/// no test that could fail if it were deleted.
fn install() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(Sink)
            // **Load-bearing for a field assertion, and measured rather than
            // assumed**: the default subscriber wraps each field *name* and the
            // equals sign after it in ANSI escapes, so a captured line carries
            // `step` only as coloured spans and never as `step="…"`.
            .with_ansi(false)
            .with_max_level(Level::DEBUG)
            .try_init();
        // **`set_global_default` stores the dispatch without re-evaluating the
        // callsite cache** — only `Dispatch::new` does that, and it runs before
        // the default is visible — so a callsite another test already hit stays
        // cached `never`. Rebuilding here, with the global default in place, is
        // what turns those cells back to "interested".
        rebuild_interest_cache();
    });
}

/// The subscriber's writer: it appends to the **calling thread's** buffer, or
/// drops the event when that thread is not recording.
///
/// A zero-sized type with no state of its own, because the state is the
/// thread-local: `MakeWriter`'s writer is created per event, and anything shared
/// here would be a lock on the event path — see [`BUFFER`] for what that cost.
struct Sink;

impl io::Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        BUFFER.with(|buffer| {
            if let Some(text) = buffer.borrow_mut().as_mut() {
                text.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Sink {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        Sink
    }
}
