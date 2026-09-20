//! Relaying a confined child's stdout to the launcher's own.

use std::net::SocketAddr;

use bytes::BytesMut;
use sse_core::{SseDecoder, SseEvent};
use tokio::io::{AsyncBufRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tracing::debug;

use crate::{
    error::{Error, Result},
    event::{Event, MAX_FIELD},
};

/// The chunk size each read pulls. Small enough that a stalled child cannot make
/// the launcher hold a large buffer, large enough that an ordinary frame arrives
/// whole.
const CHUNK: usize = 8 * 1024;

/// Copy every decodable event from `child_stdout` to `parent_stdout`.
///
/// **`pub(crate)`, and the reason is worth stating: it is `Observer` minus the
/// record.** Its whole body is `Observer::new(writer).observe(reader).await`, so
/// the only thing it adds is brevity — and the only thing it takes away is the
/// answer to "did this child ever become ready?", which both launcher platforms
/// need (design decision 19). Publishing it would put a method on the surface
/// that looks like the relay and silently discards the half that matters, which
/// is exactly the gap this crate was just fixed for. It stays internal as the
/// one-line form the tests below use.
///
/// **Async, because the streams are.** A child launched by a launcher platform
/// is wired to pipes the runtime owns, so its stdout is an `AsyncBufRead` and
/// there is no blocking reader to hand a thread. The core stays out of the
/// runtime question entirely: these are plain async functions, and whichever
/// runtime the caller already has will poll them.
///
/// Decodes and re-encodes rather than forwarding bytes: without that, anything
/// the child or its dependencies print to stdout reaches the operator
/// indistinguishable from a real event, which is the whole reason stdout is a
/// protocol channel. **Bytes that do not form one of our event names produce no
/// output at all** — dropping them is the contract, not a convenience, because a
/// forwarded noise line is exactly the ambiguity this type exists to remove.
///
/// The SSE framing is **`sse-core`'s**, not this crate's: the decoder is a
/// resumable state machine fed byte chunks, which is the only way an event split
/// across two reads is assembled correctly rather than silently dropped. Its
/// `None` return means "waiting for more data" — the signal this loop reads a
/// new chunk on.
///
/// Returns when the stream ends. An incomplete trailing frame is discarded,
/// which is the standard's own rule and the right one here: the child that sent
/// it is gone.
///
/// **There is no `# Examples` block, and rustdoc makes that necessary rather
/// than stylistic**: a doctest is compiled as its own crate against this one's
/// *public* surface, so an example naming a `pub(crate)` item does not merely
/// fail to run — it fails to compile, and rustdoc collects it anyway (`E0603`,
/// "function `reemit` is private"). The behaviour it would have shown is pinned
/// by the tests below, which live inside the crate and can name it.
///
/// **`#[cfg(test)]`, because those tests are its only callers.** Both launcher
/// platforms drive [`Observer`] rather than this — they need the record, which
/// is the whole reason `reemit` stays internal — so in a non-test build the
/// function is dead code, which the workspace's `-D warnings` gate refuses.
/// Keeping it to the build that uses it says that plainly; an
/// `#[allow(dead_code)]` would not.
#[cfg(test)]
pub(crate) async fn reemit(
    reader: impl AsyncBufRead + Unpin,
    writer: impl AsyncWrite + Unpin,
) -> Result<()> {
    Observer::new(writer).observe(reader).await?;
    Ok(())
}

/// The terminal event a confined child sent, if any.
///
/// "Terminal" is the caller's notion, not the protocol's: a well-behaved child
/// sends `Listening`, a failed one sends `Error`, and `Observer` only has to
/// notice which. Deciding what to *do* about a missing one is the caller's
/// (design decision 19: the parent owns the "printed exactly once"
/// guarantee).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Terminal {
    /// The child reached its listener.
    Listening(SocketAddr),
    /// The child reported a fatal startup failure.
    Error(String),
}

/// Relays a child's stdout while remembering whether it ever became ready.
///
/// `reemit` alone cannot answer that question — it returns `Result<()>`, so a
/// parent that wants to emit its own `Event::Error` when the child never reached
/// its listener has nothing to consult. This wraps the writer and records what
/// went past, producing no output of its own.
///
/// **Only the reading is async; the recording is not.** [`Self::record`] takes a
/// frame and returns what it did with it, which is the part worth testing and
/// needs no runtime — so the unit tests below are ordinary `#[test]`s and the
/// async surface stays a thin shell around them.
///
/// # Examples
///
/// ```
/// use std::net::SocketAddr;
///
/// use tinio_sandbox_core::{Observer, Terminal};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
/// let input = format!("event: listening\ndata: {{\"addr\":\"{addr}\"}}\n\n");
/// let observer = Observer::new(Vec::new())
///     .observe(input.as_bytes())
///     .await
///     .unwrap();
/// assert_eq!(observer.terminal(), Some(&Terminal::Listening(addr)));
/// # }
/// ```
#[derive(Debug)]
pub struct Observer<W> {
    terminal: Option<Terminal>,
    inner: W,
}

impl<W> Observer<W> {
    /// Wrap the writer the relayed frames go to.
    pub fn new(inner: W) -> Self {
        Self {
            terminal: None,
            inner,
        }
    }

    /// Record one already-parsed frame and return it when it should be
    /// forwarded.
    ///
    /// The pure half: no I/O, no runtime, no writer. A name this protocol does
    /// not define returns `None` and is dropped.
    ///
    /// **The first terminal event is the one that sticks, whichever it is.** The
    /// question this answers is "did the child ever become ready?", which is a
    /// fact about the past: a child that reported `Listening` and then failed is
    /// still one that became ready, and the later frame reaches the consumer
    /// through the relay regardless. Letting `Error` overwrite a `Listening`
    /// made the answer depend on how far down the stream the reader got — a
    /// child that started successfully would be recorded as never having
    /// started, and the launcher's "it never became ready" rule would fire on a
    /// run that had gone perfectly well up to the failure.
    ///
    /// That first terminal event is traced (`step = "relay"`), because it is the
    /// fact a launcher's readiness rule reads; the frames that follow it are
    /// relayed and not re-logged.
    pub fn record(&mut self, name: &str, data: &str) -> Option<Event> {
        let event = Event::decode_fields(name, data)?;
        if self.terminal.is_none() {
            self.terminal = match &event {
                Event::Listening { addr } => Some(Terminal::Listening(*addr)),
                Event::Error { message } => Some(Terminal::Error(message.clone())),
            };
            debug!(step = "relay", terminal = ?self.terminal);
        }
        Some(event)
    }

    /// The terminal event the child sent, if it sent one.
    ///
    /// `None` means neither `Listening` nor `Error` arrived — the case a
    /// launcher turns into its own `Event::Error` once the child has exited.
    pub fn terminal(&self) -> Option<&Terminal> {
        self.terminal.as_ref()
    }

    /// Give the wrapped writer back.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: AsyncWrite + Unpin> Observer<W> {
    /// Relay `reader`, recording the first terminal event it carries.
    ///
    /// **One decoder, and the payload limit is pinned here rather than inherited
    /// from the dependency** — `SseDecoder::with_limit(MAX_FIELD)`, the constant
    /// the protocol module owns (`crate::event::MAX_FIELD`, beside the framing
    /// it bounds) rather than one this module keeps. An earlier version took the
    /// 512 KiB default that `SseDecoder::new()` carries and argued that a
    /// smaller number would be a number to defend; the argument for pinning is
    /// the opposite one. This protocol's entire vocabulary is a `SocketAddr`
    /// (some 40 bytes) and a short error string, so 512 KiB is four orders of
    /// magnitude of headroom for a bound whose only job is to stop a runaway
    /// child — and it was a *dependency default*, which no document in this
    /// workspace could check while every other constant in this plan is pinned.
    /// 64 KiB is the number this crate owns, and a frame larger than that is not
    /// a frame of this protocol.
    ///
    /// # Errors
    ///
    /// The reader's or the writer's I/O error, named as [`Error::Step`] with
    /// `step = "relay"`. The relay stops on the first one rather than retrying:
    /// a pipe that has failed will not recover, and the caller's next step is to
    /// wait for the child anyway.
    ///
    /// **Naming the step is what keeps this from reading as a failed wait.** The
    /// crate's only `From<io::Error>` is `Error::Wait`, whose message claims the
    /// confinement could not wait for the child — the wrong diagnosis for a
    /// parent whose stdout has closed, and one a bare `?` would have produced.
    pub async fn observe(mut self, mut reader: impl AsyncBufRead + Unpin) -> Result<Self> {
        // **One decoder for the whole stream, and it must not be hoisted into
        // `decode`.** The decoder holds the field values of the event it has
        // begun and not finished, so an instance built per call would consume
        // those bytes and then drop them: every frame whose chunk boundary
        // landed mid-value would vanish, silently, and only on a real pipe.
        // This is the resumability the framing was chosen for, and it lives in
        // the instance rather than in the buffer.
        let mut decoder = SseDecoder::with_limit(MAX_FIELD);
        // The buffer holds only what the decoder has not consumed — `next`
        // advances the cursor past everything it parsed, the partial value it
        // kept included — so a long-lived stream does not grow a buffer that
        // only ever gains.
        let mut buffer = BytesMut::with_capacity(CHUNK);
        loop {
            let mut wrote = false;
            // Drain everything the buffer already holds before reading again —
            // one chunk can carry several frames, and the final read can too.
            loop {
                match decode(&mut decoder, &mut buffer) {
                    Step::Frame(name, data) => {
                        if let Some(event) = self.record(&name, &data) {
                            self.inner
                                .write_all(event.encode().as_bytes())
                                .await
                                .map_err(|source| Error::Step {
                                    step: "relay",
                                    source,
                                })?;
                            wrote = true;
                        }
                    }
                    // A frame we cannot use, already skipped by the decoder,
                    // which resynchronizes at the next boundary on the following
                    // call. Stopping here would drop the good frames behind it
                    // in the same chunk.
                    Step::Skipped => continue,
                    Step::Incomplete => break,
                }
            }
            // **Only flush what was written.** The events of this protocol are
            // sparse — one `Listening`, at most one `Error` — so an idle stream
            // would otherwise take a flush per chunk for nothing.
            if wrote {
                self.inner.flush().await.map_err(|source| Error::Step {
                    step: "relay",
                    source,
                })?;
            }
            let read = reader
                .read_buf(&mut buffer)
                .await
                .map_err(|source| Error::Step {
                    step: "relay",
                    source,
                })?;
            if read == 0 {
                // EOF, and the drain above has already taken every complete
                // frame the last read delivered. Whatever is left is half a
                // frame: discarded, which is the standard's own rule and the
                // right one here — the sender is gone, so there is nothing
                // left to wait for.
                return Ok(self);
            }
        }
    }
}

/// What one turn of the drain produced.
enum Step {
    /// A complete event, with its two field values.
    Frame(String, String),
    /// A frame this protocol cannot use — larger than the decoder's limit, or a
    /// `retry:` field the protocol never sends. The decoder has already moved
    /// past it, so the caller keeps draining.
    Skipped,
    /// The buffer holds only part of a frame: the decoder's documented answer
    /// while it waits for more. The caller reads another chunk.
    Incomplete,
}

/// Take one step through `buffer` with the stream's `decoder`.
///
/// Takes the decoder rather than building one, because a fresh instance per call
/// would throw away the partial field values it is holding — see `observe`.
fn decode(decoder: &mut SseDecoder, buffer: &mut BytesMut) -> Step {
    match decoder.next(buffer) {
        // Too large to be one of ours: the decoder has discarded it and
        // resynchronized at the next event boundary.
        Some(Err(_)) => Step::Skipped,
        // The protocol is a one-way readiness handshake and never sends
        // `retry:`; ignoring it is the standard's own instruction for a field
        // the consumer does not use.
        Some(Ok(SseEvent::Retry(_))) => Step::Skipped,
        Some(Ok(SseEvent::Message(message))) => {
            Step::Frame(message.event.into_owned(), message.data)
        }
        None => Step::Incomplete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::Event,
        testing::{blocking, loopback},
    };

    /// Three bytes at a time: every frame the split-frame test relays is
    /// assembled from many reads, which is the property the resumable decoder
    /// exists for.
    const TRICKLE: usize = 3;

    #[tokio::test]
    async fn an_event_split_across_reads_is_assembled() {
        // The reason the decoding is a resumable state machine rather than a
        // line reader: a child's stdout is a pipe, and a frame can arrive in as
        // many pieces as the kernel feels like. Asserted against the original
        // frames rather than as a count, because a relay that lost the bytes its
        // decoder had already consumed mid-value would still emit something for
        // every frame that survived — the count would pass and the payload
        // would be gone.
        let frames = Event::Listening {
            addr: loopback(9000),
        }
        .encode()
        .repeat(2);
        let mut output = Vec::new();
        reemit(blocking(frames.as_bytes(), TRICKLE), &mut output)
            .await
            .unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), frames);
    }

    #[tokio::test]
    async fn an_incomplete_trailing_frame_is_discarded() {
        // A child killed mid-write leaves half a frame. The standard discards
        // it, and so does this: the sender is gone, so there is nothing to wait
        // for.
        let mut half = Event::Listening {
            addr: loopback(9000),
        }
        .encode()
        .into_bytes();
        half.truncate(half.len() - 3);
        let mut output = Vec::new();
        reemit(half.as_slice(), &mut output).await.unwrap();
        assert!(output.is_empty(), "{output:?}");
    }

    #[tokio::test]
    async fn several_frames_in_one_chunk_all_arrive() {
        // The EOF path is where a buffer-holding relay loses the last frames:
        // the final read can carry more than one event, and a loop that returns
        // on `read == 0` before draining would drop them.
        let input = Event::Error {
            message: "one".into(),
        }
        .encode()
            + &Event::Error {
                message: "two".into(),
            }
            .encode();
        let mut output = Vec::new();
        reemit(input.as_bytes(), &mut output).await.unwrap();
        let written = String::from_utf8(output).unwrap();
        assert_eq!(written.matches("event: error").count(), 2, "{written}");
    }

    #[tokio::test]
    async fn retry_frames_are_ignored() {
        // The protocol never sends one; the standard says a consumer ignores a
        // field it does not use, and this is that.
        let mut output = Vec::new();
        reemit("retry: 1000\n\n".as_bytes(), &mut output)
            .await
            .unwrap();
        assert!(output.is_empty(), "{output:?}");
    }

    #[tokio::test]
    async fn a_retry_frame_does_not_strand_the_event_behind_it() {
        // `Step::Skipped` and `Step::Incomplete` look identical on a quiet
        // stream — nothing is written either way — so the test above cannot tell
        // them apart, and the difference is load-bearing. One chunk carrying
        // both lines separates them: read as "needs more data", the buffer stops
        // being drained and the event behind the retry line is lost when the
        // stream ends; skipped, the decoder is already past it.
        let frame = Event::Listening {
            addr: loopback(9000),
        }
        .encode();
        let input = format!("retry: 1000\n{frame}");
        let mut output = Vec::new();
        reemit(input.as_bytes(), &mut output).await.unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            frame,
            "the event behind the retry frame must still arrive"
        );
    }

    #[tokio::test]
    async fn the_observer_records_the_listening_address() {
        let addr = loopback(9000);
        let input = format!("event: listening\ndata: {{\"addr\":\"{addr}\"}}\n\n");
        let observer = Observer::new(Vec::new())
            .observe(input.as_bytes())
            .await
            .unwrap();
        assert_eq!(observer.terminal(), Some(&Terminal::Listening(addr)));
        // It recorded, and it also forwarded: the two are not alternatives.
        let relayed = String::from_utf8(observer.into_inner()).unwrap();
        assert_eq!(relayed, input);
    }

    #[tokio::test]
    async fn the_observer_reports_nothing_when_the_child_never_became_ready() {
        // The case a launcher turns into its own `Event::Error`: the child
        // printed noise and died. `None` is the whole signal.
        let observer = Observer::new(Vec::new())
            .observe(b"starting up\nfailed to bind\n".as_slice())
            .await
            .unwrap();
        assert_eq!(observer.terminal(), None);
        assert!(
            observer.into_inner().is_empty(),
            "noise must not be forwarded"
        );
    }

    #[tokio::test]
    async fn the_observer_remembers_a_child_reported_error() {
        let input = "event: error\ndata: {\"message\":\"no such profile\"}\n\n";
        let observer = Observer::new(Vec::new())
            .observe(input.as_bytes())
            .await
            .unwrap();
        assert_eq!(
            observer.terminal(),
            Some(&Terminal::Error("no such profile".into()))
        );
    }

    #[tokio::test]
    async fn a_child_that_listened_and_then_failed_still_became_ready() {
        // Both frames reach the consumer through the relay, so nothing is
        // hidden; what the record answers is "did this child ever become ready?",
        // and a child whose listener was up did. Letting the later `Error` win
        // would make a launcher's never-became-ready rule fire on a run that
        // had started perfectly well — which is the same failure as not
        // recording `Listening` at all, one frame further along.
        let input = concat!(
            "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n",
            "event: error\ndata: {\"message\":\"the served root went away\"}\n\n",
        );
        let observer = Observer::new(Vec::new())
            .observe(input.as_bytes())
            .await
            .unwrap();
        assert_eq!(
            observer.terminal(),
            Some(&Terminal::Listening(loopback(9000)))
        );
        let relayed = String::from_utf8(observer.into_inner()).unwrap();
        assert_eq!(relayed.matches("event: ").count(), 2, "{relayed}");
    }

    #[tokio::test]
    async fn the_relay_drops_what_does_not_decode() {
        let input = concat!(
            "noise the child printed\n",
            "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n",
            "\n",
            "more noise\n",
        );
        let mut output = Vec::new();
        reemit(input.as_bytes(), &mut output).await.unwrap();
        let written = String::from_utf8(output).unwrap();
        assert!(!written.contains("noise"), "{written}");
        assert_eq!(written.matches("event: listening").count(), 1, "{written}");
    }

    #[tokio::test]
    async fn a_frame_over_the_field_limit_is_skipped_and_the_stream_continues() {
        // The only case that exercises `Some(Err(_)) => Step::Skipped`, and the
        // only one that would notice `MAX_FIELD`'s pin being wrong: at the
        // decoder's inherited 512 KiB this frame would decode and be relayed, and
        // the bytes below would be its frame *plus* the one behind it. The
        // oversized event is discarded rather than delivered, the decoder
        // resynchronizes at the next event boundary — which is where it clears
        // its corrupted flag — and the good frame behind it must still arrive.
        let oversized = Event::Error {
            message: "x".repeat(MAX_FIELD.get() + 1),
        }
        .encode();
        let frame = Event::Listening {
            addr: loopback(9000),
        }
        .encode();
        let input = format!("{oversized}{frame}");
        let mut output = Vec::new();
        reemit(input.as_bytes(), &mut output).await.unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            frame,
            "the good frame behind the oversized one must arrive"
        );
    }
}
