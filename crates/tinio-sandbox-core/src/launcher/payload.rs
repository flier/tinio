//! The launch payload: how a confined child learns it is the child.
//!
//! It travels on the child's **standard input**, which the parent writes and
//! closes (design decision 17). The parent writes the configuration's **text**
//! rather than a parsed structure, so an unconfined launcher never holds
//! credentials it understands (design decision 18).
//!
//! **One line of JSON, and `serde` on both sides.** The earlier shape was a
//! hand-rolled binary frame — a magic constant, a nonce, a length prefix and the
//! raw bytes — and every part of it was a bespoke format that only this crate
//! could read. A one-field JSON object says the same thing and lets the
//! deserializer be the validator.
//!
//! `config` is a `String`, not bytes: `Config::load` reads the file with
//! `read_to_string` and `toml::from_str` takes a `&str` (`crates/tinio-config/src/schema/config.rs:129`),
//! so a configuration that is not valid UTF-8 already fails to load long before
//! a launch happens. Carrying bytes here would mean a base64 adapter hand-written
//! for an input the rest of the program cannot produce.
//!
//! The inherited descriptors are deliberately **not** in the payload: a socket
//! cannot travel on a stream, so they go in argv instead ([`LISTENER_FD_PREFIX`]
//! and [`CONTROL_FD_PREFIX`]), and carrying either in both places would be two
//! sources of truth for one fact.
//!
//! **The token is the payload's second field, and the stream is the only place
//! it may travel.** It guards `/status` and `/stop` on the control channel, so
//! anything another process can read hands it over: argv is visible to every
//! process on the machine (`CreateProcessW`'s command line is queried by
//! `NtQueryInformationProcess`, and `/proc/<pid>/cmdline` is world-readable), and
//! an environment variable is no better. stdin is neither — it is a pipe between
//! the two processes alone. The launcher needs it *here* rather than only in the
//! child because that same launcher is the process that publishes the child's
//! `state` file (the file's DACL is current-user-only, so a confined child cannot
//! write it), and the token it writes there and the token the child verifies
//! against have to be one value.
//!
//! **The fourth field is the launcher's *command* to a child that confines
//! itself** ([`SandboxSpec`]), and it rides here for the same reason every other
//! launcher-owned value does rather than on a side channel of its own: the frame
//! is already the one stream the two processes share, and a second channel would
//! be a second thing to keep in step. It is **not** configuration, which is why
//! it is not a field of the `config` text: the configuration is the operator's
//! document, forwarded unparsed, while this is an instruction the launcher
//! composes. `None` is every launch that confines by wrapping the child's `exec`
//! — the whole `sandbox-exec` platform, and Windows — so the field is absent on
//! the wire there and the frame's shape is unchanged.

use std::ffi::{OsStr, OsString};

use serde::{Deserialize, Serialize};

/// What the parent sends and the child validates.
///
/// **`decode` is `serde`'s, and `adopt` spells it** (`serde_json::from_str` over
/// the whole stream): a wrapper method would be a second spelling of "use serde"
/// with nothing of its own to add. **The two `encode`s are not**, and they are the
/// methods here that do something: a launcher holds the frame's three values as
/// borrows on the path that blocks until the child drains stdin, so the borrowed
/// spelling ([`Self::encode_frame`]) writes a *view* of this type rather than an
/// owned copy of it — see [`Self::encode`] for the owned spelling, and the
/// crate-internal `Wire` for the view both are written on.
///
/// The one thing a caller must not forget is that **the whole stream is the
/// payload**: the parent writes the object and closes stdin, and a child that
/// finds anything after it is reading something that was not meant for it.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::LaunchPayload;
///
/// let payload = LaunchPayload {
///     config: "port = 9000".into(),
///     token: Some("00".repeat(32)),
///     confinement: None,
/// };
/// let line = serde_json::to_string(&payload)?;
/// assert_eq!(serde_json::from_str::<LaunchPayload>(&line)?, payload);
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, derive_more::Debug)]
pub struct LaunchPayload {
    /// The configuration file's text, unparsed.
    ///
    /// **`#[debug(skip)]`, because the text is credential-bearing.** The
    /// launcher is *supposed* to hold these bytes without understanding them
    /// (design decision 18), and a derived `Debug` would print the whole
    /// document — secrets included — the first time anything logged a payload
    /// or an error carrying one. What the derived `Debug` still gives is the
    /// type name, which is what makes it useful in a failure message.
    #[debug(skip)]
    pub config: String,
    /// The token the child serves the management plane with, when the launcher
    /// generated one — the value the launcher also wrote into `state`, because
    /// it is the process that owns that file.
    ///
    /// **Absent for every launch that has no token to carry**, which is every
    /// unconfined run and the in-place platforms — and it must stay absent on the
    /// wire there: `#[serde(default)]` reads a field an older writer never wrote
    /// (a payload from a launcher that predates this field) and
    /// `skip_serializing_if` keeps a tokenless payload byte-for-byte what it was,
    /// so the one-field frame's shape is still the shape a reader tests for.
    ///
    /// **`#[debug(skip)]`, because it is a bearer credential**: a `Debug` that
    /// printed it would put the value that guards `/status` and `/stop` into any
    /// log line or `unwrap_err()` that happened to touch a payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[debug(skip)]
    pub token: Option<String>,
    /// What the child must do to confine *itself*, when the launcher's mechanism
    /// is one the child applies in-process rather than one that wraps its `exec`.
    ///
    /// **`None` is the ordinary frame, and the field is absent on the wire** —
    /// `#[serde(default)]` reads a payload an older launcher wrote and
    /// `skip_serializing_if` keeps a payload carrying none byte-for-byte what it
    /// was, so the shape every existing reader tests for is still the shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confinement: Option<SandboxSpec>,
}

impl LaunchPayload {
    /// This payload as the borrowed wire view of it, which is what a launcher that
    /// already holds the values serializes.
    ///
    /// **Crate-visible rather than public**, with the wire view it returns: a public
    /// name here would have to be published through this crate's root list and the
    /// `tinio-sandbox` facade's, and the view is the launcher's own encoding detail.
    /// [`Self::encode_frame`] is the borrowed spelling's public door.
    pub(crate) fn wire(&self) -> Wire<'_> {
        Wire {
            config: &self.config,
            token: self.token.as_deref(),
            confinement: self.confinement.as_ref(),
        }
    }

    /// The frame's bytes: one JSON object, exactly the shape a child decodes back
    /// into this type.
    ///
    /// **The same bytes [`Self::encode_frame`] writes**, and the two are one
    /// implementation rather than two: this is the owned payload's spelling, for a
    /// caller that has one to write.
    ///
    /// # Errors
    ///
    /// [`serde_json::Error`] for a value the serializer refuses, which no payload
    /// this crate builds can produce.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPayload;
    ///
    /// let payload = LaunchPayload {
    ///     config: "port = 9000".into(),
    ///     token: None,
    ///     confinement: None,
    /// };
    /// assert_eq!(payload.encode()?, "{\"config\":\"port = 9000\"}".as_bytes());
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        self.wire().encode()
    }

    /// The frame's bytes, **from the three values a launcher holds** — the borrowed
    /// spelling, and the one a launcher that never builds a payload writes.
    ///
    /// **Every argument is a borrow, and that is the point.** The frame goes into
    /// the child's stdin and the write **blocks until the child drains it** (design
    /// decision 17), so everything ahead of that write is startup latency — and
    /// building a [`Self`] first cost two deep copies of the largest strings in the
    /// process: the whole configuration text, and the whole rendered profile with
    /// its parameters. Here the serializer reads the caller's own bytes, and the
    /// only allocation left on the path is the frame itself.
    ///
    /// **The frame is the same frame, field for field.** The names, their order and
    /// their two `skip_serializing_if`s are the owned type's, which is what keeps the
    /// bytes a child decodes (`adopt`'s `serde_json::from_str::<LaunchPayload>`) and
    /// the bytes an earlier reader was written against unchanged — pinned as text by
    /// the tests below rather than left to two definitions agreeing by inspection.
    ///
    /// # Errors
    ///
    /// [`serde_json::Error`] for a value the serializer refuses, which no launch
    /// this crate builds can produce.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::LaunchPayload;
    ///
    /// // The three values as a launcher holds them: the plan's text, the token it
    /// // minted, and the confinement half when the mechanism asks for one.
    /// assert_eq!(
    ///     LaunchPayload::encode_frame("port = 9000", None, None)?,
    ///     "{\"config\":\"port = 9000\"}".as_bytes()
    /// );
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn encode_frame(
        config: &str,
        token: Option<&str>,
        confinement: Option<&SandboxSpec>,
    ) -> Result<Vec<u8>, serde_json::Error> {
        Wire::new(config, token, confinement).encode()
    }
}

/// The **borrowed** wire view of a payload: what a launcher that already holds the
/// three fields serializes.
///
/// **A second type rather than a change to [`LaunchPayload`], and what it buys is a
/// copy the launcher cannot afford.** The frame is written into the child's stdin
/// and the write **blocks until the child drains it** (design decision 17), so
/// everything ahead of the `write` is startup latency — and the owned payload the
/// launcher used to build cost two deep copies of the largest strings in the
/// process: the whole configuration text and the whole rendered profile, text and
/// parameters together. Here they are borrowed, and the only allocation left on the
/// path is the frame's own bytes.
///
/// **Crate-visible, and its public door is [`LaunchPayload::encode_frame`]** — a
/// public name would have to be published through this crate's root list and the
/// facade's, and how a launch is *encoded* is not part of the confinement contract a
/// consumer names.
///
/// **The frame is the same frame, field for field.** The names, their order and
/// their two `skip_serializing_if`s are [`LaunchPayload`]'s, which is what keeps the
/// bytes a child decodes (`adopt`'s `serde_json::from_str::<LaunchPayload>`) and the
/// bytes an earlier reader was written against unchanged — pinned as text by the
/// tests below rather than left to the two definitions agreeing by inspection.
#[derive(Serialize, derive_more::Debug)]
pub(crate) struct Wire<'a> {
    /// The configuration file's text, unparsed — [`LaunchPayload::config`]'s value,
    /// borrowed. **`#[debug(skip)]` for that field's own reason**: the text is
    /// credential-bearing, so a `Debug` may not print it.
    #[debug(skip)]
    pub config: &'a str,
    /// The launcher's token, when it minted one — [`LaunchPayload::token`]'s value,
    /// borrowed, and absent on the wire for the same reason. **`#[debug(skip)]` for
    /// that field's own reason too**: it is a bearer credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[debug(skip)]
    pub token: Option<&'a str>,
    /// The confinement the child must apply to itself, when the mechanism asks for
    /// one — [`LaunchPayload::confinement`]'s value, borrowed, and absent on the
    /// wire for the same reason.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confinement: Option<&'a SandboxSpec>,
}

impl<'a> Wire<'a> {
    /// The frame a launcher writes from the three values it holds — **the one
    /// constructor, and it takes borrows only**: a caller that owns any of the
    /// three has [`LaunchPayload::wire`] to turn it into this view.
    pub fn new(
        config: &'a str,
        token: Option<&'a str>,
        confinement: Option<&'a SandboxSpec>,
    ) -> Self {
        Self {
            config,
            token,
            confinement,
        }
    }

    /// The wire bytes: one JSON object, [`LaunchPayload`]'s shape exactly.
    ///
    /// # Errors
    ///
    /// [`serde_json::Error`] for a value the serializer refuses, which no payload a
    /// launcher builds can produce.
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// How a child confines itself, when its launcher hands it that job.
///
/// **The profile travels as data here, exactly as it travels to `sandbox-exec`**:
/// [`Self::profile`] is the rendered SBPL text and [`Self::params`] are the values
/// its `(param "…")` references resolve to. The text names no path (design decision
/// 24), so this is the same render-and-values pair the other mechanism spells as
/// `-p <text> -D NAME=VALUE …`, and the child is the one that turns it into
/// parameters instead of `sandbox-exec`.
///
/// **`mechanism` is the launcher's own word for what it resolved**, and that is the
/// whole reason it is on the frame: the child never reads the mechanism's
/// environment variable, so a variable that changed between the two processes
/// cannot make a child apply a confinement its launcher did not choose.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, derive_more::Debug)]
pub struct SandboxSpec {
    /// The mechanism's name, in the spelling its environment variable takes.
    pub mechanism: String,
    /// The rendered profile text.
    ///
    /// **Deliberately not `#[debug(skip)]`**: the text names no path and holds no
    /// secret — it is the same bytes `sandbox-exec` receives as an argv element,
    /// which are readable in the process list on that mechanism.
    pub profile: String,
    /// The `(param "…")` values the profile references, in the order it names
    /// them — one name and one value per pair, as the `-D` argv elements spell
    /// them.
    pub params: Vec<(String, String)>,
}

impl SandboxSpec {
    /// The value one parameter names, or `None` when no pair carries that name.
    ///
    /// **A lookup over the list rather than a map**, because the list is short,
    /// ordered, and rebuilt from a render every launch — the same list
    /// `sandbox-exec` receives as repeated `-D` elements.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::SandboxSpec;
    ///
    /// let confinement = SandboxSpec {
    ///     mechanism: "sandbox-init".into(),
    ///     profile: "(allow file-read* (subpath (param \"E0\")))".into(),
    ///     params: vec![(String::from("E0"), String::from("/srv/data"))],
    /// };
    /// assert_eq!(confinement.param("E0"), Some("/srv/data"));
    /// assert_eq!(confinement.param("E1"), None);
    /// ```
    pub fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The argv element that carries the inherited **listener** descriptor number.
///
/// Not the payload's: the payload travels on stdin (design decision 17), and a
/// socket cannot travel on a stream, so the descriptor handoff survives for the
/// listener alone. Both child-launching platforms spell it this way because the
/// core spells it, not because they agree.
pub const LISTENER_FD_PREFIX: &str = "tinio-sandbox-fd=";

/// The argv element carrying the listener descriptor `fd`.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::listener_fd_arg;
///
/// assert_eq!(listener_fd_arg(7), "tinio-sandbox-fd=7");
/// ```
pub fn listener_fd_arg(fd: i64) -> OsString {
    OsString::from(format!("{LISTENER_FD_PREFIX}{fd}"))
}

/// Whether `arg` is the listener-descriptor element.
///
/// # Examples
///
/// ```
/// use std::ffi::OsStr;
///
/// use tinio_sandbox_core::{is_listener_fd_arg, listener_fd_arg};
///
/// assert!(is_listener_fd_arg(&listener_fd_arg(7)));
/// assert!(!is_listener_fd_arg(OsStr::new("--port")));
/// ```
pub fn is_listener_fd_arg(arg: &OsStr) -> bool {
    listener_fd_from_arg(arg).is_some()
}

/// The descriptor number `arg` carries, when it is the listener element and the
/// number parses.
///
/// A malformed element — the right prefix, an unparseable tail — is treated as
/// *not* the element rather than as an error, so a caller filtering argv cannot
/// accidentally strip an argument it does not understand.
///
/// # Examples
///
/// ```
/// use std::ffi::OsStr;
///
/// use tinio_sandbox_core::listener_fd_from_arg;
///
/// assert_eq!(
///     listener_fd_from_arg(OsStr::new("tinio-sandbox-fd=9")),
///     Some(9)
/// );
/// assert_eq!(listener_fd_from_arg(OsStr::new("tinio-sandbox-fd=x")), None);
/// ```
pub fn listener_fd_from_arg(arg: &OsStr) -> Option<i64> {
    arg.to_str()?.strip_prefix(LISTENER_FD_PREFIX)?.parse().ok()
}

/// The argv element that carries the inherited **control-channel** descriptor number.
///
/// A second element beside the listener's, and a separate prefix rather than a second
/// field of one element: the two descriptors are handed over for different reasons. The
/// listener is a socket the launcher bound because the child may not bind one (design
/// decision 16); this is the control channel a launcher bound because **only the
/// unrestricted launcher can create it** — its pipe carries a current-user-only DACL,
/// and inside a container the creating process's identity is the package SID, not the
/// user, so the child's own `CreateNamedPipeW` is refused — or because the confinement's
/// profile grants no `bind()` at all, which is the macOS half. The launcher's channel
/// therefore travels the way the listener does, and the child serves on a channel it did
/// not create.
///
/// **Both launcher platforms set it**, one element per instance: Windows' pool of pipe
/// instances, and macOS' single unix socket.
pub const CONTROL_FD_PREFIX: &str = "tinio-sandbox-control-fd=";

/// The argv element carrying the control-channel descriptor `fd`.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::control_fd_arg;
///
/// assert_eq!(control_fd_arg(7), "tinio-sandbox-control-fd=7");
/// ```
pub fn control_fd_arg(fd: i64) -> OsString {
    OsString::from(format!("{CONTROL_FD_PREFIX}{fd}"))
}

/// Whether `arg` is the control-channel-descriptor element.
///
/// # Examples
///
/// ```
/// use std::ffi::OsStr;
///
/// use tinio_sandbox_core::{control_fd_arg, is_control_fd_arg};
///
/// assert!(is_control_fd_arg(&control_fd_arg(7)));
/// assert!(!is_control_fd_arg(OsStr::new("--port")));
/// ```
pub fn is_control_fd_arg(arg: &OsStr) -> bool {
    control_fd_from_arg(arg).is_some()
}

/// Whether `arg` is any descriptor element a launcher hands the child — the listener's or the
/// control channel's.
///
/// **One name for the pair, because the caller's question is one question.** The child's argv has
/// to lose *both* elements before clap parses it, so the two predicates were spelled together at
/// every call site; a third element kind would have to be added to each of those spellings, and
/// one that missed it makes clap refuse a confined start with a usage error — the failure the
/// filters exist to prevent.
pub fn is_launcher_fd_arg(arg: &OsStr) -> bool {
    is_listener_fd_arg(arg) || is_control_fd_arg(arg)
}

/// The descriptor number `arg` carries, when it is the control element and the number
/// parses.
///
/// The malformed-tail rule is [`listener_fd_from_arg`]'s, word for word: a right prefix
/// with an unparseable tail is *not* the element, so a caller filtering argv cannot
/// accidentally strip an argument it does not understand.
///
/// # Examples
///
/// ```
/// use std::ffi::OsStr;
///
/// use tinio_sandbox_core::control_fd_from_arg;
///
/// assert_eq!(
///     control_fd_from_arg(OsStr::new("tinio-sandbox-control-fd=9")),
///     Some(9)
/// );
/// assert_eq!(
///     control_fd_from_arg(OsStr::new("tinio-sandbox-control-fd=x")),
///     None
/// );
/// ```
pub fn control_fd_from_arg(arg: &OsStr) -> Option<i64> {
    arg.to_str()?.strip_prefix(CONTROL_FD_PREFIX)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use super::*;

    fn sample() -> LaunchPayload {
        LaunchPayload {
            config: "version = 1\n[server]\nport = 9000\n".into(),
            token: None,
            confinement: None,
        }
    }

    #[test]
    fn a_payload_survives_a_round_trip() {
        let payload = sample();
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            serde_json::from_str::<LaunchPayload>(&line).unwrap(),
            payload
        );
    }

    #[test]
    fn a_token_survives_a_round_trip() {
        // The second field travels the stream with the first: the child adopts the token the
        // launcher published `state` with, so a payload that lost it would leave the plane
        // verifying against a value no client holds.
        let payload = LaunchPayload {
            config: "port = 9000".into(),
            token: Some("00".repeat(32)),
            confinement: None,
        };
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            serde_json::from_str::<LaunchPayload>(&line).unwrap(),
            payload
        );
    }

    #[test]
    fn a_tokenless_payload_is_the_one_field_frame_it_always_was() {
        // **The back-compatibility rule, pinned as text.** A launch with no token writes the
        // frame every earlier reader was written against, which is what keeps the unconfined
        // payload byte-for-byte unchanged — `skip_serializing_if` on the field, not a second
        // frame shape.
        let payload = LaunchPayload {
            config: "port = 9000".into(),
            token: None,
            confinement: None,
        };
        assert_eq!(
            serde_json::to_string(&payload).unwrap(),
            "{\"config\":\"port = 9000\"}"
        );
    }

    #[test]
    fn a_frame_without_the_token_field_decodes_with_none() {
        // The other direction of the same rule: a payload a launcher that predates the field
        // wrote is still a payload, and the absent token is `None` rather than a decode error.
        let payload: LaunchPayload = serde_json::from_str("{\"config\":\"port = 9000\"}").unwrap();
        assert_eq!(payload.token, None);
    }

    #[test]
    fn a_confinement_survives_a_round_trip() {
        // The fourth field travels the same stream as the other three, and it is the one the
        // child acts on: a payload that lost it would leave a child under a mechanism that
        // confines in-process **serving unconfined**, which is the failure the whole feature
        // exists to prevent. The text, the parameter list and its order all cross.
        let payload = LaunchPayload {
            config: "port = 9000".into(),
            token: None,
            confinement: Some(SandboxSpec {
                mechanism: "sandbox-init".into(),
                profile: "(deny default)\n(allow file-read* (subpath (param \"E0\")))\n".into(),
                params: vec![
                    (String::from("E0"), String::from("/srv/data")),
                    (String::from("E1"), String::from("/srv/state")),
                ],
            }),
        };
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            serde_json::from_str::<LaunchPayload>(&line).unwrap(),
            payload
        );
    }

    #[test]
    fn a_confinement_less_payload_is_the_frame_it_always_was() {
        // **The back-compatibility rule, for the fourth field.** `sandbox-exec` confines a
        // child's `exec` and needs nothing on the frame, so the field is absent there and the
        // bytes are unchanged — which is what leaves Windows' launcher and every earlier reader
        // reading the shape they were written against rather than a frame with a `null` in it.
        let payload = LaunchPayload {
            config: "port = 9000".into(),
            token: None,
            confinement: None,
        };
        assert_eq!(
            serde_json::to_string(&payload).unwrap(),
            "{\"config\":\"port = 9000\"}"
        );
        // And the other direction: a frame with no such field decodes to `None` rather than
        // failing, so a payload written by a launcher that predates it is still a payload.
        let decoded: LaunchPayload = serde_json::from_str("{\"config\":\"port = 9000\"}").unwrap();
        assert_eq!(decoded.confinement, None);
    }

    #[test]
    fn the_borrowed_frame_is_the_owned_frame() {
        // **The property that makes `Wire` usable at all**: the launcher that serializes from
        // borrows (`hand_over`) writes the bytes a child decodes, which are the bytes the owned
        // payload writes (`LaunchPayload::encode`). Asserted for every field combination, so a
        // field added to one definition and not the other fails here rather than reaching a child
        // as a frame that disagrees with the launcher's own word about it.
        let confinement = SandboxSpec {
            mechanism: "sandbox-init".into(),
            profile: "(version 1)\n(deny default)\n(allow file-read* (subpath (param \"E0\")))\n"
                .into(),
            params: vec![
                (String::from("E0"), String::from("/srv/data")),
                (String::from("E1"), String::from("/srv/state")),
            ],
        };
        let token = "00".repeat(32);
        for payload in [
            LaunchPayload {
                config: "port = 9000".into(),
                token: None,
                confinement: None,
            },
            LaunchPayload {
                config: "port = 9000".into(),
                token: Some(token.clone()),
                confinement: None,
            },
            LaunchPayload {
                config: sample().config,
                token: Some(token),
                confinement: Some(confinement),
            },
        ] {
            let owned = serde_json::to_vec(&payload).unwrap();
            assert_eq!(payload.encode().unwrap(), owned, "{payload:?}");
            let borrowed = Wire::new(
                &payload.config,
                payload.token.as_deref(),
                payload.confinement.as_ref(),
            )
            .encode()
            .unwrap();
            assert_eq!(borrowed, owned, "{payload:?}");
            // And the borrowed frame decodes to the same payload, so the equality above is not
            // two spellings of one mistake: the child's own read of it is the launcher's value.
            assert_eq!(
                serde_json::from_slice::<LaunchPayload>(&borrowed).unwrap(),
                payload
            );
        }
    }

    #[test]
    fn a_tokenless_borrowed_frame_is_the_one_field_frame_it_always_was() {
        // The pinned text, for the borrowed writer: `skip_serializing_if` on a borrowed `Option`
        // is what keeps an absent field absent rather than a `null` — the same rule the owned
        // payload's tests assert, restated for the shape the launcher actually serializes.
        assert_eq!(
            Wire::new("port = 9000", None, None).encode().unwrap(),
            "{\"config\":\"port = 9000\"}".as_bytes()
        );
    }

    #[test]
    fn a_confinement_param_is_read_by_name() {
        // The lookup the child's own apply makes: the profile names a parameter and the child
        // resolves it from the list, exactly as `sandbox-exec` resolves a `-D` element. An
        // absent name is `None` rather than an empty value, so a profile that references a
        // parameter the launcher never wrote is a refusal at the call site rather than a grant
        // of the empty path.
        let confinement = SandboxSpec {
            mechanism: "sandbox-init".into(),
            profile: String::new(),
            params: vec![(String::from("E0"), String::from("/srv/data"))],
        };
        assert_eq!(confinement.param("E0"), Some("/srv/data"));
        assert_eq!(confinement.param("E1"), None);
    }

    #[test]
    fn an_empty_config_is_legal() {
        let payload = LaunchPayload {
            config: String::new(),
            token: None,
            confinement: None,
        };
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            serde_json::from_str::<LaunchPayload>(&line).unwrap(),
            payload
        );
    }

    #[test]
    fn the_wire_shape_is_one_json_object() {
        // Pinned as text, not only as a round trip: a symmetric mistake would
        // round-trip against itself while reading nothing else.
        let payload = LaunchPayload {
            config: "port = 9000".into(),
            token: None,
            confinement: None,
        };
        assert_eq!(
            serde_json::to_string(&payload).unwrap(),
            "{\"config\":\"port = 9000\"}"
        );
    }

    #[test]
    fn a_config_with_newlines_stays_one_object() {
        // The configuration is a TOML document, so it is full of newlines —
        // and `serde_json` escapes them, which is what keeps the payload one
        // object rather than several lines.
        let payload = sample();
        let line = serde_json::to_string(&payload).unwrap();
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(
            serde_json::from_str::<LaunchPayload>(&line).unwrap(),
            payload
        );
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        // A child must be certain the whole stream was meant for it.
        let line = serde_json::to_string(&sample()).unwrap();
        assert!(serde_json::from_str::<LaunchPayload>(&format!("{line}junk")).is_err());
    }

    #[test]
    fn foreign_text_is_rejected() {
        for text in [
            "not a payload at all, just text",
            // Well-formed JSON that is not our shape.
            "{\"port\":9000}",
            "[1, 2, 3]",
            "",
        ] {
            assert!(
                serde_json::from_str::<LaunchPayload>(text).is_err(),
                "{text:?}"
            );
        }
    }

    #[test]
    fn the_listener_element_round_trips() {
        assert_eq!(listener_fd_arg(7), OsString::from("tinio-sandbox-fd=7"));
        assert_eq!(listener_fd_from_arg(&listener_fd_arg(7)), Some(7));
        assert!(is_listener_fd_arg(&listener_fd_arg(7)));
    }

    #[test]
    fn a_negative_descriptor_is_carried_verbatim() {
        // Windows handles and unix descriptors share one spelling; the core
        // does not decide which of them is meaningful.
        assert_eq!(listener_fd_from_arg(&listener_fd_arg(-1)), Some(-1));
    }

    #[test]
    fn ordinary_arguments_are_not_the_listener_element() {
        for arg in [
            "--port",
            "serve",
            "tinio-sandbox-fd",
            "tinio-sandbox-fd=",
            "tinio-sandbox-fd=x",
        ] {
            assert!(!is_listener_fd_arg(OsStr::new(arg)), "{arg}");
        }
    }

    #[test]
    fn the_control_element_round_trips() {
        assert_eq!(
            control_fd_arg(7),
            OsString::from("tinio-sandbox-control-fd=7")
        );
        assert_eq!(control_fd_from_arg(&control_fd_arg(7)), Some(7));
        assert!(is_control_fd_arg(&control_fd_arg(7)));
    }

    #[test]
    fn a_negative_control_descriptor_is_carried_verbatim() {
        // The listener's rule, restated for the second element: Windows handles and
        // unix descriptors share one spelling, and the core does not decide which of
        // them is meaningful.
        assert_eq!(control_fd_from_arg(&control_fd_arg(-1)), Some(-1));
    }

    #[test]
    fn ordinary_arguments_are_not_the_control_element() {
        for arg in [
            "--port",
            "serve",
            "tinio-sandbox-control-fd",
            "tinio-sandbox-control-fd=",
            "tinio-sandbox-control-fd=x",
        ] {
            assert!(!is_control_fd_arg(OsStr::new(arg)), "{arg}");
        }
    }

    #[test]
    fn one_element_is_never_the_other() {
        // The two prefixes share a stem, so a predicate that stripped the wrong one —
        // or a filter that used `starts_with` on the stem — would take the control
        // element for the listener's. Each predicate answers for its own element only.
        let listener = listener_fd_arg(7);
        let control = control_fd_arg(7);
        assert!(is_listener_fd_arg(&listener) && !is_control_fd_arg(&listener));
        assert!(is_control_fd_arg(&control) && !is_listener_fd_arg(&control));
        assert_eq!(listener_fd_from_arg(&control), None);
        assert_eq!(control_fd_from_arg(&listener), None);
    }
}
