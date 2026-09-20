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
//! The listener descriptor is deliberately **not** in the payload: a socket
//! cannot travel on a stream, so it goes in argv instead
//! ([`LISTENER_FD_PREFIX`]), and carrying it in both places would be two
//! sources of truth for one fact.

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

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use super::*;

    fn sample() -> LaunchPayload {
        LaunchPayload {
            config: "version = 1\n[server]\nport = 9000\n".into(),
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
    fn an_empty_config_is_legal() {
        let payload = LaunchPayload {
            config: String::new(),
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
}
