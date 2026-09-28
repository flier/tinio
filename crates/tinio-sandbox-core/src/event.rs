//! What the confined process reports on stdout.

use std::{borrow::Cow, net::SocketAddr, num::NonZeroUsize};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sse_core::{SseDecoder, SseEvent};

/// The SSE field prefixes. A frame is these two lines and a blank line.
const DATA: &str = "data: ";
const EVENT: &str = "event: ";

/// The pinned field limit, in the plain integer the compile-time assertion below can police.
const MAX_FIELD_LIMIT: usize = 64 * 1024;

// A compile-time assertion and not a runtime one: a zero here stops the build, which is
// what leaves the conversion below with no `None` arm any run could take.
const _: () = assert!(MAX_FIELD_LIMIT != 0);

/// The largest a single SSE field may be before the decoder discards the frame.
///
/// Pinned rather than inherited: `SseDecoder::new()` carries a 512 KiB default,
/// which is four orders of magnitude of headroom over what this protocol ever
/// sends (a `SocketAddr` and a short error string) and is a dependency default
/// no document here could check. See `Observer::observe`.
///
/// **It lives in the protocol module rather than beside either of the two
/// decoders it configures, and that is the point.** "How large may one frame
/// be" is a property of the protocol, so a per-call-site answer is a second
/// place to change it: `Event::decode` used to build the dependency's default
/// while the relay pinned 64 KiB — the same crate, two limits, and the one this
/// comment argues against at the site a consumer reaches.
///
/// **The conversion to `NonZeroUsize` is written so that no input can panic it.** The
/// limit is the plain integer above, and the assertion beside it refuses a zero *at
/// compile time* — so the match's `None` arm is a build failure rather than a branch
/// any run reaches. Constitution II forbids `unwrap()` in library code and the
/// alternatives are worse here: `NonZeroUsize::new_unchecked` is an `unsafe fn` this
/// crate may not call (`unsafe_code = "forbid"`), and a fallible spelling would have to
/// answer *something* for the zero that cannot occur — a second limit, silently
/// different from this one.
pub(crate) const MAX_FIELD: NonZeroUsize = match NonZeroUsize::new(MAX_FIELD_LIMIT) {
    Some(field) => field,
    None => unreachable!(),
};

/// The event name that means "the listener is bound".
const LISTENING: &str = "listening";
/// The event name that means "startup failed".
const ERROR: &str = "error";

/// What the confined process reports on stdout.
///
/// stdout carries this and nothing else; every log goes to stderr, so a
/// consumer never has to tell an event from a stray print (design decision 19).
///
/// **The event name and the payload travel in different SSE fields**, which is
/// what the framing standard asks for and what `sse-core` hands back in one
/// `MessageEvent { event, data }`:
///
/// ```text
/// event: listening
/// data: {"addr":"127.0.0.1:9000"}
/// ```
///
/// Spelling the name in both places — once in the `event:` field and again as a
/// `{"event":…}` member of the payload — would be one fact stored twice, and a
/// disagreement between the copies would have no tie-breaker. The payload keeps
/// only what the name does not already say.
///
/// # Examples
///
/// ```
/// use std::net::SocketAddr;
///
/// use tinio_sandbox_core::Event;
///
/// let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
/// let frame = Event::Listening { addr }.encode();
/// assert_eq!(
///     frame,
///     "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n"
/// );
/// assert_eq!(Event::decode(&frame), Some(Event::Listening { addr }));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The listener is bound and the data plane is serving.
    Listening {
        /// The bound address. `SocketAddr` rather than a string or a bare
        /// `IpAddr`: it is exactly what `TcpListener::local_addr` returns, and
        /// it is the only form that carries the port — which is what
        /// `--port 0` exists to discover.
        addr: SocketAddr,
    },
    /// A fatal startup failure, already rendered for the operator.
    Error {
        /// The operator-facing message.
        message: String,
    },
}

/// The payload of a `listening` frame. Private: the wire shape is this module's
/// to change, and consumers name [`Event`].
#[derive(Serialize, Deserialize)]
struct Listening {
    addr: SocketAddr,
}

/// The payload of an `error` frame.
#[derive(Serialize, Deserialize)]
struct Error<'a> {
    /// **Borrowed for the encoding, owned for the decoding.** Borrowed, because
    /// `encode` would otherwise clone the message into an owned payload only for
    /// `serde_json` to allocate the serialized text again. A `Cow` rather than
    /// the `&str` a borrowing type would ordinarily be, because `serde_json` can
    /// only hand out a borrowed string when the payload carries no escapes: a
    /// message with a quote or a newline in it — the two cases this module's
    /// tests pin — would then fail to decode at all, which is a frame lost to
    /// keep a borrow the decoding side does not need.
    message: Cow<'a, str>,
}

impl Event {
    /// The event name this frame carries in its `event:` line.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Event;
    ///
    /// // The name is what a consumer dispatches on, and the payload never repeats it: the two
    /// // spellings of one fact would have no tie-breaker if they disagreed.
    /// assert_eq!(
    ///     Event::Listening {
    ///         addr: "127.0.0.1:9000".parse().unwrap()
    ///     }
    ///     .name(),
    ///     "listening"
    /// );
    /// assert_eq!(
    ///     Event::Error {
    ///         message: "boom".into()
    ///     }
    ///     .name(),
    ///     "error"
    /// );
    /// ```
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Listening { .. } => LISTENING,
            Self::Error { .. } => ERROR,
        }
    }

    /// One SSE frame: an `event:` line, a `data:` line, and a blank line.
    ///
    /// The framing is not decoration. `data:` values are newline-delimited by
    /// the standard, so a message containing a newline would otherwise split one
    /// event into two and neither half would parse; and the blank line is what
    /// tells a reader the frame is complete rather than still arriving.
    ///
    /// `serde_json` writes the payload, so quotes, backslashes and control
    /// characters are its problem rather than `format!`'s — and because the
    /// payload is a single JSON object it never contains the newline that would
    /// need a second `data:` line.
    ///
    /// # Panics
    ///
    /// Not reachable: the two payloads are a `SocketAddr` and a `String`, and
    /// neither can fail to serialize.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Event;
    ///
    /// // A message with a newline in it stays **one** frame: the newline is an escape inside
    /// // the `data:` value rather than a line break, which is the whole reason the framing is
    /// // SSE and not bare JSONL.
    /// let frame = Event::Error {
    ///     message: "line one\nline two".into(),
    /// }
    /// .encode();
    /// assert_eq!(frame.matches("\n\n").count(), 1, "{frame:?}");
    /// assert_eq!(
    ///     Event::decode(&frame),
    ///     Some(Event::Error {
    ///         message: "line one\nline two".into()
    ///     })
    /// );
    /// ```
    pub fn encode(&self) -> String {
        let payload = match self {
            Self::Listening { addr } => serde_json::to_string(&Listening { addr: *addr }),
            Self::Error { message } => serde_json::to_string(&Error {
                message: Cow::Borrowed(message.as_str()),
            }),
        };
        // Unreachable for both payloads — a `SocketAddr` and a `String` always
        // serialize — and fail-closed if it ever were not: an empty `data:`
        // value is a frame every decoder drops, so a serialization mistake
        // would cost the frame rather than put a wrong one on the wire.
        let payload = payload.unwrap_or_default();
        format!("{EVENT}{}\n{DATA}{payload}\n\n", self.name())
    }

    /// Decode one frame from its `event:` and `data:` lines.
    ///
    /// Takes the two field values rather than the raw bytes because the SSE
    /// framing is `sse-core`'s: [`crate::Observer`] hands what the decoder
    /// produced to this, and reading the bytes twice would be a second parser to
    /// keep in step with the first.
    ///
    /// **`pub(crate)`, not `pub`.** Its only caller is this crate's own
    /// `Observer`, so it is an internal seam rather than part of the surface —
    /// and every consumer outside is holding a whole frame anyway, which is what
    /// [`Self::decode`] takes.
    ///
    /// Returns `None` for an event name this protocol does not define, and for a
    /// payload that does not parse. **A consumer must ignore what it cannot
    /// decode rather than fail** — that is what makes a stray write to stdout
    /// harmless instead of a protocol violation.
    pub(crate) fn decode_fields(name: &str, data: &str) -> Option<Self> {
        match name {
            LISTENING => serde_json::from_str::<Listening>(data)
                .ok()
                .map(|payload| Self::Listening { addr: payload.addr }),
            ERROR => serde_json::from_str::<Error<'_>>(data)
                .ok()
                .map(|payload| Self::Error {
                    message: payload.message.into_owned(),
                }),
            _ => None,
        }
    }

    /// Decode one frame from its text, or `None` if it is not one of ours.
    ///
    /// Parsed by **`sse-core`** — the same state machine the relay runs — and
    /// then handed to `decode_fields`, so this crate holds one SSE
    /// parser rather than two. **It deliberately does not try the payloads in
    /// turn**: `{"message":…}` deserializes happily under the `listening` name,
    /// so a guess would be ambiguous exactly where the protocol is trying to be
    /// precise. The name decides.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Event;
    ///
    /// let frame = "event: error\ndata: {\"message\":\"no profile\"}\n";
    /// assert_eq!(
    ///     Event::decode(frame),
    ///     Some(Event::Error {
    ///         message: "no profile".into()
    ///     })
    /// );
    /// assert!(Event::decode("data: not a frame").is_none());
    /// ```
    pub fn decode(frame: &str) -> Option<Self> {
        // The input is terminated rather than handed over as it stands: the
        // standard dispatches an event only at a blank line, and a caller that
        // reads the stream line by line holds a frame's two field lines without
        // one. Appending it is harmless for a frame that already ends in one —
        // extra blank lines dispatch nothing.
        let mut decoder = SseDecoder::with_limit(MAX_FIELD);
        let mut buffer = Bytes::from(format!("{frame}\n\n"));
        match decoder.next(&mut buffer) {
            Some(Ok(SseEvent::Message(message))) => {
                Self::decode_fields(&message.event, &message.data)
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::loopback;

    #[test]
    fn an_event_survives_a_round_trip() {
        for event in [
            Event::Listening {
                addr: loopback(9000),
            },
            Event::Error {
                message: "boom".into(),
            },
        ] {
            let frame = event.encode();
            assert!(frame.starts_with("event: "), "{frame}");
            assert!(frame.contains("\ndata: "), "{frame}");
            assert!(frame.ends_with("\n\n"), "{frame}");
            assert_eq!(Event::decode(&frame), Some(event));
        }
    }

    #[test]
    fn the_wire_shape_is_pinned() {
        // Pinned as a string, not only as a round trip: a consumer in another
        // language reads this, and a symmetric mistake would round-trip
        // perfectly against itself while breaking every such consumer.
        let frame = Event::Listening {
            addr: loopback(9000),
        }
        .encode();
        assert_eq!(
            frame,
            "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n"
        );
    }

    #[test]
    fn a_message_with_a_newline_stays_one_event() {
        // The reason the framing is SSE and not bare JSONL.
        let event = Event::Error {
            message: "line one\nline two".into(),
        };
        let frame = event.encode();
        assert_eq!(
            frame.matches("\n\n").count(),
            1,
            "one frame, one blank-line terminator"
        );
        assert_eq!(Event::decode(&frame), Some(event));
        // And the newline survived as data rather than splitting the payload:
        // the way through a `data:` value is an escape, not a second line.
        assert_eq!(frame.matches("data: ").count(), 1, "{frame}");
    }

    #[test]
    fn quotes_and_backslashes_survive() {
        let event = Event::Error {
            message: r#"a "quoted" \backslash\ and a \n literal"#.into(),
        };
        assert_eq!(Event::decode(&event.encode()), Some(event));
    }

    #[test]
    fn anything_that_is_not_a_frame_is_ignored() {
        for text in [
            "",
            "listening on 127.0.0.1:9000",
            "data: not json",
            "data: {\"addr\":\"127.0.0.1:9000\"}",
            ": a comment",
            // A frame whose name we do not define, and one whose payload does
            // not fit the name it claims.
            "event: unknown\ndata: {}\n",
            "event: listening\ndata: {\"message\":\"wrong shape\"}\n",
            "event: listening\n",
            "data: {\"addr\":\"127.0.0.1:9000\"}\n",
        ] {
            assert_eq!(Event::decode(text), None, "{text:?}");
        }
    }

    #[test]
    fn the_field_limit_decodes_just_under_and_refuses_just_over() {
        // **The public seam's share of `MAX_FIELD`, which the relay's tests
        // cannot cover.** `decode` builds its own decoder, so a limit pinned only
        // in `Observer::observe` left this the one path where the crate answered
        // "how large may one frame be" with the dependency's 512 KiB default. The
        // oversized frame below decoded then and is refused now.
        //
        // **Both sides, because a decoder that refused everything would pass a
        // refusal alone.** What the limit measures is the *data value*, not the
        // frame — `sse-core` compares the accumulated value's length — so the
        // JSON wrapper's bytes are inside what it bounds, and the two messages
        // below sit one byte either side of it.
        let wrapper = "{\"message\":\"\"}".len();
        assert_eq!(wrapper, 14, "the wrapper the sizes below are derived from");

        let under = Event::Error {
            message: "x".repeat(MAX_FIELD.get() - wrapper - 1),
        };
        assert_eq!(
            Event::decode(&under.encode()).map(|event| event.name()),
            Some("error"),
            "a frame one byte inside the limit must decode"
        );

        let over = Event::Error {
            message: "x".repeat(MAX_FIELD.get() - wrapper + 1),
        };
        assert!(
            Event::decode(&over.encode()).is_none(),
            "a frame one byte past the limit must not decode"
        );
    }
}
