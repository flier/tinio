//! The stdout contract, in one place: design decision 19's "stdout carries
//! event frames and nothing else".
//!
//! [`emit`] is the **only** writer of a frame in the whole example, and
//! [`emit_error`] and [`emit_on_error`] are the two shapes a refusal goes
//! through, so the property is one body's rather than a rule each call site
//! re-checks.

use std::{
    error::Error,
    io::{Write as _, stdout},
};

use tinio_sandbox::Event;

/// Write one event frame to stdout and flush it.
///
/// **The only write to stdout in this example** (design decision 19), and one
/// function rather than one per call site: the readiness frame and the
/// `Error` frames a refusal sends both go through it, so "stdout carries events
/// and nothing else" is a property of one body instead of a rule to re-check.
///
/// **"In this example" is the whole of what a source scan can promise, and that
/// gap is worth stating rather than leaving to the check.** The scan this
/// contract was written against — a `println!`/`print!`/`stdout()` grep with a
/// negative lookbehind for the `e` of `eprintln!` — cannot see a dependency, and
/// the parser is one now: `clap` writes to stdout through its own code when it
/// is allowed to, so a green grep would have kept passing while the guarantee
/// was gone. What holds the parser's half is its configuration and three
/// functions this example never calls — `Error::print`, `Error::exit`,
/// `Command::print_help` — none of which a grep over this source can check.
/// `crates/tinio-server/tests/serve_read_only.rs` counts the child's **stdout
/// lines** after a refused command line and requires exactly the frame's three,
/// which is a fact about the stream rather than about anyone's source, and is
/// what would fail if a clap version ever printed.
///
/// Flushing is not decoration — stdout is line-buffered to a terminal and
/// block-buffered to a pipe, and a harness reads a pipe, so an unflushed frame
/// is a readiness hang rather than a missing event. A failed write is ignored on
/// purpose: it means the reader is gone, and there is nowhere left to report it.
pub fn emit(event: &Event) {
    let mut out = stdout().lock();
    let _ = out.write_all(event.encode().as_bytes());
    let _ = out.flush();
}

/// A refusal, on stdout, **before** its `Err` is returned — the sandbox arm's
/// own and, through [`emit_on_error`], every other step of `main` that can
/// refuse before the listener exists.
///
/// Decision 19's "a startup failure is an event rather than an absence", which
/// the arm did not honour: a consumer of this stream waits for a frame, and
/// without one it reports a timeout where the launcher platforms report the
/// cause. Both of them send this same frame through a helper of this same name.
///
/// **The frame and the returned `Err` are two registers of one refusal, not one
/// string.** The frame carries the error's `Display` — the operator-facing
/// sentence — while `main`'s `Err` is printed by `Termination` as `Error: {:?}`,
/// its `Debug` (`Error: Io { path: … }`, `Error: InvalidValue(Report { … })`). A
/// consumer reading stdout gets the sentence; the log and the exit path get the
/// structured form.
pub fn emit_error(message: &str) {
    emit(&Event::Error {
        message: message.to_string(),
    });
}

/// A fallible startup step, with the `Error` frame a consumer of stdout is
/// waiting for.
///
/// **This is the rest of decision 19's "a startup failure is an event rather
/// than an absence".** The sandbox arm's refusals already send one; every other
/// step of `main` on the way to the listener — a config that will not load, a
/// root that cannot be created, a state directory that cannot be resolved, a
/// storage that refuses the mode it was handed — returned through a bare `?`,
/// so the process exited with an empty stdout and a consumer that is waiting for
/// a frame reported a timeout where the launcher platforms report the cause.
///
/// The frame carries the `Error`'s `Display`, the operator-facing sentence; the
/// `Err` handed back is printed by `main`'s `Termination` as its `Debug`. Two
/// registers of one refusal, not the same string — see [`emit_error`].
///
/// **The serving call is deliberately not wrapped.** By then this process has
/// emitted `Listening` itself; an `Error` frame *after* it would be a statement
/// about the serving phase rather than a startup refusal, and that is the
/// shared contract's to make rather than this step's.
pub fn emit_on_error<T, E>(result: Result<T, E>) -> Result<T, Box<dyn Error + Send + Sync>>
where
    E: Into<Box<dyn Error + Send + Sync>>,
{
    result.map_err(|err| {
        let err = err.into();
        emit_error(&err.to_string());
        err
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_endpoint_travels_as_an_event_and_round_trips() {
        let event = Event::Listening {
            addr: "127.0.0.1:9000".parse().expect("addr"),
        };
        let frame = event.encode();
        // SSE: an `event:` line, a `data:` line and a blank line — the blank one
        // is what stops a message containing a newline from splitting into two
        // frames (decision 19). The exact bytes are pinned in the core, where the
        // encoding lives, so this asserts the shape rather than repeating them.
        assert!(frame.starts_with("event: listening\n"), "{frame:?}");
        assert!(frame.contains("\ndata: "), "{frame:?}");
        assert!(frame.ends_with("\n\n"), "{frame:?}");
        assert_eq!(Event::decode(frame.trim_end()), Some(event));
    }
}
