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

use std::ffi::{OsStr, OsString};

use serde::{Deserialize, Serialize};

/// What the parent sends and the child validates.
///
/// **No `encode` or `decode` method.** The type is `serde`'s on both sides, and
/// the two call sites use `serde_json` directly — the parent writes it with
/// `serde_json::to_writer` into the child's stdin, and `adopt` reads it back
/// with `serde_json::from_str`. A pair of wrapper methods would be a second
/// spelling of "use serde" with nothing of their own to add.
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
/// user, so the child's own `CreateNamedPipeW` is refused. The launcher's pipe therefore
/// travels the way the listener does, and the child serves on a handle it did not create.
///
/// Only the Windows launcher sets it in practice (the unix control channel is a socket
/// file the confined process binds itself), which is why [`LaunchPlan::control`] carries
/// the raw value and answers `None` everywhere else.
///
/// [`LaunchPlan::control`]: crate::LaunchPlan::control
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
    fn an_empty_config_is_legal() {
        let payload = LaunchPayload {
            config: String::new(),
            token: None,
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
