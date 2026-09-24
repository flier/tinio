//! The stdout contract of the **serving path**, in one place: design decision 19's "stdout
//! carries event frames and nothing else".
//!
//! [`emit`] is the only writer of a frame, and [`emit_error`] is the shape a refusal goes
//! through, so the property is one body's rather than a rule each call site re-checks. The
//! client commands (`status`, `stop`, `doctor`) are not this module's:
//! their stdout is a report for a human, and nothing here is called on their path.
//!
//! **A consumer of a serving process is a harness or a launcher**, and both wait for a frame
//! rather than scraping a line: the readiness point is [`ready`], every startup refusal on the
//! way there is an [`emit_error`] frame beside the `Err` it returns, and text that is not one
//! of our frames (a dependency's log line, an `eprintln!`-free library writing to stdout) is
//! the consumer's to ignore rather than to fail on.

use std::{
    io::{Write as _, stdout},
    net::SocketAddr,
};

use crate::_sandbox::Event;

/// Write one event frame to stdout and flush it.
///
/// **The only write to stdout on the serving path** (design decision 19), and one function
/// rather than one per call site: the readiness frame and the `Error` frames a refusal sends
/// both go through it, so "stdout carries events and nothing else" is a property of one body
/// instead of a rule to re-check.
///
/// Flushing is not decoration — stdout is line-buffered to a terminal and block-buffered to a
/// pipe, and a harness reads a pipe, so an unflushed frame is a readiness hang rather than a
/// missing event. A failed write is ignored on purpose: it means the reader is gone, and there
/// is nowhere left to report it.
pub(crate) fn emit(event: &Event) {
    let mut out = stdout().lock();
    let _ = out.write_all(event.encode().as_bytes());
    let _ = out.flush();
}

/// The readiness event: what a caller waits for before it uses the endpoint.
///
/// Emitted by whichever process actually bound — the serving process itself, or a `--daemon`
/// parent printing it on its daemon's behalf, because the daemon's own stdout is the log file.
pub(crate) fn ready(address: SocketAddr) {
    emit(&Event::Listening { addr: address });
}

/// A refusal, on stdout.
///
/// Decision 19's "a startup failure is an event rather than an absence": a consumer of this
/// stream waits for a frame, and without one it reports a timeout where the refusal's own text
/// would have told it what happened.
///
/// **The frame and the `Err` it accompanies are two registers of one refusal, not one string.**
/// The frame carries the error's `Display` — the operator-facing sentence — while the process's
/// exit path prints the same sentence to stderr as `tinio: …`. A consumer reading stdout gets
/// the sentence; the operator at a terminal gets it on stderr.
///
/// **Sent once, at the boundary that owns it**: `run`'s serving arm frames the `Err` that
/// `start::run` returns, and a second frame from a step inside the sequence would put one
/// refusal on stdout twice (measured in the `serve` example this path came from, which framed at
/// the call site *and* through its wrapper). A step that refuses and is also framed by the
/// boundary therefore does not frame itself.
pub(crate) fn emit_error(message: &str) {
    emit(&Event::Error {
        message: message.to_string(),
    });
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
        // SSE: an `event:` line, a `data:` line and a blank line — the blank one is what stops
        // a message containing a newline from splitting into two frames (decision 19). The
        // exact bytes are pinned in the core, where the encoding lives, so this asserts the
        // shape rather than repeating them.
        assert!(frame.starts_with("event: listening\n"), "{frame:?}");
        assert!(frame.contains("\ndata: "), "{frame:?}");
        assert!(frame.ends_with("\n\n"), "{frame:?}");
        assert_eq!(Event::decode(frame.trim_end()), Some(event));
    }
}
