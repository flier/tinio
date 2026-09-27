//! Relaying a confined child's stdout to the launcher's own.

use std::{net::SocketAddr, sync::mpsc::Sender};

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
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Terminal;
///
/// // The two terminal events a confined child can send, and nothing else: which one arrived —
/// // or that neither did — is the whole of what a launcher's readiness rule reads.
/// let reported = Terminal::Listening("127.0.0.1:9000".parse().expect("addr"));
/// assert!(matches!(reported, Terminal::Listening(_)));
/// assert!(matches!(
///     Terminal::Error("no profile".into()),
///     Terminal::Error(_)
/// ));
/// ```
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
/// **The record is final only when the stream ends; the announcement is not.** A
/// launcher that has to *act* the moment its child becomes ready — the Windows
/// launcher publishes the child's `state` then — cannot wait for EOF, and the
/// record it reads after [`Child::wait`] is exactly that late. [`Self::with_notify`]
/// is the other half: the first terminal event is also sent, once, on a channel a
/// waiting launcher blocks on. Nothing about the record changes; the two answer
/// different questions at different times.
///
/// [`Child::wait`]: crate::Child::wait
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
    /// Where the first terminal event is announced, when a caller asked for it
    /// (see [`Self::with_notify`]).
    notify: Option<Sender<Terminal>>,
}

impl<W> Observer<W> {
    /// Wrap the writer the relayed frames go to.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Observer;
    ///
    /// // A writer, and nothing else yet: no record, no announcement, no runtime.
    /// let observer = Observer::new(Vec::<u8>::new());
    /// assert!(observer.terminal().is_none(), "nothing has been recorded");
    /// ```
    pub fn new(inner: W) -> Self {
        Self {
            terminal: None,
            inner,
            notify: None,
        }
    }

    /// Announce the first terminal event on `notify`, as well as recording it.
    ///
    /// **The one-shot, mid-stream half of "did this child become ready?"** — the
    /// call a launcher that must act while the child is still serving makes. The
    /// send happens inside [`Self::record`], so it happens when the frame is
    /// *seen* rather than when the stream ends, and it happens once: a second
    /// terminal event (`Listening` followed by `Error` is an ordinary sequence)
    /// updates nothing and announces nothing, which is the same
    /// first-event-wins rule the record obeys.
    ///
    /// **A failed send is ignored, not an error.** The only reason `send` fails is
    /// that the waiting half is already gone — a launcher that gave up on its bound
    /// and moved on to the wait — and there is nothing for the relay to do about a
    /// reader that left. The relay's own job, forwarding the frames, is unaffected.
    ///
    /// The sender is dropped with the `Observer`, which is what tells a waiting
    /// launcher that no announcement is coming: a child that exits without ever
    /// reporting readiness disconnects the channel rather than leaving the wait to
    /// run out its bound.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::mpsc;
    ///
    /// use tinio_sandbox_core::{Observer, Terminal};
    ///
    /// let (tx, rx) = mpsc::channel();
    /// let mut observer = Observer::new(Vec::<u8>::new()).with_notify(tx);
    /// observer.record("listening", "{\"addr\":\"127.0.0.1:9000\"}");
    /// // The mid-stream half, sent as the frame is *seen* rather than when the stream ends:
    /// // this is what a launcher that has to act while the child still serves reads.
    /// assert_eq!(
    ///     rx.recv().expect("the one announcement"),
    ///     Terminal::Listening("127.0.0.1:9000".parse().expect("addr"))
    /// );
    /// ```
    pub fn with_notify(mut self, notify: Sender<Terminal>) -> Self {
        self.notify = Some(notify);
        self
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
    /// relayed and not re-logged. It is also announced on the channel
    /// [`Self::with_notify`] installed, when one was — the launcher that has to act
    /// *now* rather than at end-of-stream reads that half.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{Event, Observer, Terminal};
    ///
    /// let mut observer = Observer::new(Vec::<u8>::new());
    /// // A name this protocol does not define is dropped rather than failing.
    /// assert_eq!(observer.record("unknown", "{}"), None);
    ///
    /// let addr = "127.0.0.1:9000".parse().expect("addr");
    /// let event = observer.record("listening", "{\"addr\":\"127.0.0.1:9000\"}");
    /// assert_eq!(event, Some(Event::Listening { addr }));
    /// // The first terminal event is the one that sticks: the question is "did it ever become
    /// // ready?", and a later `Error` reaching the consumer does not un-answer it.
    /// assert_eq!(observer.terminal(), Some(&Terminal::Listening(addr)));
    /// ```
    pub fn record(&mut self, name: &str, data: &str) -> Option<Event> {
        let event = Event::decode_fields(name, data)?;
        if self.terminal.is_none() {
            self.terminal = match &event {
                Event::Listening { addr } => Some(Terminal::Listening(*addr)),
                Event::Error { message } => Some(Terminal::Error(message.clone())),
            };
            debug!(step = "relay", terminal = ?self.terminal);
            if let (Some(terminal), Some(notify)) = (self.terminal.as_ref(), self.notify.as_ref()) {
                // A reader that left is not this relay's problem; see `with_notify`.
                let _ = notify.send(terminal.clone());
            }
        }
        Some(event)
    }

    /// The terminal event the child sent, if it sent one.
    ///
    /// `None` means neither `Listening` nor `Error` arrived — the case a
    /// launcher turns into its own `Event::Error` once the child has exited.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{Observer, Terminal};
    ///
    /// let mut observer = Observer::new(Vec::<u8>::new());
    /// assert!(observer.terminal().is_none(), "nothing has been recorded");
    /// observer.record("listening", "{\"addr\":\"127.0.0.1:9000\"}");
    /// assert_eq!(
    ///     observer.terminal(),
    ///     Some(&Terminal::Listening(
    ///         "127.0.0.1:9000".parse().expect("addr")
    ///     ))
    /// );
    /// ```
    pub fn terminal(&self) -> Option<&Terminal> {
        self.terminal.as_ref()
    }

    /// Give the wrapped writer back.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Observer;
    ///
    /// // The relay forwarded the frames into the writer it was handed, and this is how its
    /// // caller gets it back — the record is the `Observer`'s, the bytes are the writer's.
    /// let observer = Observer::new(Vec::<u8>::new());
    /// assert!(observer.into_inner().is_empty());
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Observer;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// // A whole stream, read to its end: the two field lines and the blank line that
    /// // dispatches the frame.
    /// let input = "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n";
    /// let observer = Observer::new(Vec::new())
    ///     .observe(input.as_bytes())
    ///     .await
    ///     .expect("the stream is I/O this reader can finish");
    /// assert!(
    ///     observer.terminal().is_some(),
    ///     "the frame carried the one event"
    /// );
    /// # }
    /// ```
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
    use std::{sync::mpsc, time::Duration};

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
    async fn the_observer_announces_the_first_terminal_event_once() {
        // **The half a launcher acts on while its child is still serving**, and the reason the
        // announcement is not the record: a child that reports `Listening` and then fails sends
        // two terminal events, and a launcher that published `state` on either of them would
        // publish on a run that had already gone wrong. First event wins, exactly as the record's
        // own rule says.
        let (notify, announced) = mpsc::channel();
        let addr = loopback(9000);
        let input = concat!(
            "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n",
            "event: error\ndata: {\"message\":\"the served root went away\"}\n\n",
        );
        Observer::new(Vec::new())
            .with_notify(notify)
            .observe(input.as_bytes())
            .await
            .unwrap();
        assert_eq!(
            announced.try_iter().collect::<Vec<_>>(),
            [Terminal::Listening(addr)],
            "one announcement, of the first terminal event"
        );
    }

    #[tokio::test]
    async fn an_observer_with_no_notifier_announces_nothing() {
        // The default: a relay nothing is waiting on sends nowhere, and the channel half is
        // absent rather than an extra object per relay. What is asserted here is that the *record*
        // still happens — the two halves are independent.
        let observer = Observer::new(Vec::new())
            .observe(b"event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n".as_slice())
            .await
            .unwrap();
        assert_eq!(
            observer.terminal(),
            Some(&Terminal::Listening(loopback(9000)))
        );
    }

    #[tokio::test]
    async fn a_waiting_announcement_is_disconnected_when_the_stream_ends_without_one() {
        // The failure a launcher's bound would otherwise have to cover: a child that dies before
        // reporting readiness drops the sender with the observer, so the waiting half is
        // **disconnected** rather than left blocking. This is the property `Child::wait_for_ready`
        // reads as "nothing reported".
        let (notify, announced) = mpsc::channel();
        Observer::new(Vec::new())
            .with_notify(notify)
            .observe(b"starting up\nfailed to bind\n".as_slice())
            .await
            .unwrap();
        assert_eq!(
            announced.recv_timeout(Duration::from_secs(1)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        );
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
