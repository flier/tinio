//! What systemd is told, and the unit text it is told it by (design decisions 21 and 22).
//!
//! Every call here is conditional on the environment systemd provides, and the crate does
//! most of that itself: `sd_notify::notify` answers `Ok(())` **without sending anything**
//! when `NOTIFY_SOCKET` is absent (`sd-notify-0.5.0/src/lib.rs:179-181`), and
//! `sd_notify::watchdog_enabled` answers `None` without `WATCHDOG_USEC` — and it consults
//! `WATCHDOG_PID` too, so a child that inherited the two variables does not beat for a
//! watchdog that belongs to its parent (`:489-499`). Neither case is a failure and neither is
//! surfaced.
//!
//! **The whole module is `cfg(unix)`** (`lib.rs` gates it). `sd_notify` cannot even link
//! elsewhere — `sd-notify-0.5.0/src/lib.rs:30-34` imports `std::os::unix::*` — so there is no
//! Windows stub and no same-signature no-op: `serve` calls `ready`/`stopping`/`watchdog`
//! behind the same gate, and `unit_text` travels with them because the unit it renders is
//! systemd's.

use std::{
    env,
    ffi::{OsStr, OsString},
    iter,
    path::Path,
};

use sd_notify::{self, NotifyState, watchdog_enabled};
use tokio::time;

/// The `READY=1` that follows readiness.
pub fn ready() {
    let _ = notify(NotifyState::Ready);
}

/// The `STOPPING=1` that opens the drain window — called by the watcher `serve` spawns, so it
/// fires when the shutdown *begins* rather than when it has finished.
pub fn stopping() {
    let _ = notify(NotifyState::Stopping);
}

/// Sends one notification, answering whether a socket was there to send to.
///
/// The boolean is **this module's**, not the crate's: `sd_notify::notify`'s one-argument form
/// answers `Ok(())` both when the datagram was sent and when `NOTIFY_SOCKET` was simply
/// absent, so `Ok`-ness cannot distinguish "sent" from "nothing to send" — and the unit test
/// below is exactly the case that has to be assertable. `NOTIFY_SOCKET` is the check that can
/// tell them apart; the crate call is the send.
fn notify(state: NotifyState) -> bool {
    if env::var_os("NOTIFY_SOCKET").is_none() {
        return false;
    }
    match sd_notify::notify(&[state]) {
        Ok(()) => true,
        Err(err) => {
            // A socket that is there and still fails to take the datagram earns one line.
            tracing::debug!(error = %err, "the systemd notification could not be sent");
            false
        }
    }
}

/// Starts the watchdog task when the environment asks for one.
///
/// Cadence is half of what systemd asked for via `WATCHDOG_USEC`.
/// `sd_notify::watchdog_enabled()` already parses that and gates on `WATCHDOG_PID`;
/// hand-parsing the variable instead would silently drop the pid gate.
pub fn watchdog() {
    // Half the interval so the beat lands well inside systemd's timeout.
    let Some(interval) = watchdog_enabled().map(|interval| interval / 2) else {
        return;
    };
    tokio::spawn(async move {
        let mut ticker = time::interval(interval);
        loop {
            ticker.tick().await;
            let _ = notify(NotifyState::Watchdog);
        }
    });
}

/// The unit, rendered from the one template (decision 21).
///
/// Paths and argv are systemd-quoted so spaces / `#` / quotes cannot break the unit or inject
/// extra directives. `WatchdogSec=30` is a default the operator may edit; heartbeat cadence
/// still comes from `WATCHDOG_USEC`.
pub fn unit_text(program: &Path, args: &[OsString], state_dir: &Path) -> String {
    let exec = iter::once(systemd_quote(program.as_os_str()))
        .chain(args.iter().map(|a| systemd_quote(a)))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "[Unit]\n\
         Description=tinio S3-compatible local storage server\n\
         After=network.target\n\
         \n\
         [Service]\n\
         Type=notify\n\
         WatchdogSec=30\n\
         WorkingDirectory={work}\n\
         ExecStart={exec}\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=multi-user.target\n",
        work = systemd_quote(state_dir.as_os_str()),
        exec = exec,
    )
}

/// Quote a unit-file value the way `systemd-escape --path` is *not*: shell-like double quotes
/// when the value needs them (whitespace, `#`, quotes, backslash, control bytes).
fn systemd_quote(value: &OsStr) -> String {
    let raw = value.to_string_lossy();
    let needs = raw.is_empty()
        || raw.chars().any(|c| {
            c.is_whitespace()
                || c.is_control()
                || matches!(c, '"' | '\'' | '\\' | '#' | '$' | '`' | ';')
        });
    if !needs {
        return raw.into_owned();
    }
    let mut out = String::from('"');
    for c in raw.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use std::{env, fs, path::Path};

    use super::*;

    #[test]
    fn the_unit_notifies_and_watches() {
        let text = unit_text(
            Path::new("/usr/local/bin/tinio"),
            &["server".into(), "/srv/tinio".into()],
            Path::new("/srv/tinio/.tinio"),
        );
        assert!(
            text.contains("Type=notify"),
            "otherwise READY=1 is inert:\n{text}"
        );
        assert!(
            text.contains("WatchdogSec="),
            "otherwise WATCHDOG=1 is inert:\n{text}"
        );
        assert!(
            text.contains("ExecStart=/usr/local/bin/tinio server /srv/tinio"),
            "{text}"
        );
        assert!(text.contains("WantedBy=multi-user.target"), "{text}");
    }

    #[test]
    fn unit_text_quotes_paths_that_need_it() {
        let text = unit_text(
            Path::new("/usr/local/bin/tinio"),
            &["server".into(), "/srv/tinio data".into()],
            Path::new("/srv/tinio/.tinio"),
        );
        assert!(
            text.contains(r#"ExecStart=/usr/local/bin/tinio server "/srv/tinio data""#),
            "{text}"
        );
        let injected = unit_text(
            Path::new("/usr/bin/tinio"),
            &["server".into(), "/tmp/x\nEvil=1".into()],
            Path::new("/tmp/state"),
        );
        assert!(
            injected.contains(r#"\n"#) && !injected.contains("\nEvil=1"),
            "newlines in paths must not inject unit lines:\n{injected}"
        );
    }

    #[test]
    fn the_checked_in_unit_is_this_rendering() {
        let checked_in = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/tinio.service"
        ))
        .expect("packaging/tinio.service");
        let rendered = unit_text(
            Path::new("/usr/local/bin/tinio"),
            &["server".into(), "/srv/tinio".into()],
            Path::new("/srv/tinio/.tinio"),
        );
        assert_eq!(
            checked_in, rendered,
            "the file an operator reads and the file a machine installs must be the same text"
        );
    }

    /// The contract carries the same rendering in its `service` section, and nothing else pins
    /// it: `unit_text` is the author, `packaging/tinio.service` is checked by the leg above, and
    /// this is the third copy — the one an operator reads in the spec set. Rather than compare
    /// the block itself (fence, blank lines and all), the leg asserts the shipped file's text
    /// appears in the contract verbatim, which is the property that matters: an edit to
    /// `unit_text` that is not carried into `contracts/cli.md` fails here.
    #[test]
    fn the_contract_carries_the_same_unit() {
        // Normalised: this checkout is `core.autocrlf=true`, so the contract's working-tree copy
        // is CRLF while `packaging/tinio.service` is LF. The property under test is the text,
        // not the checkout's line endings.
        let contract = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../specs/001-s3-local-server/contracts/cli.md"
        ))
        .expect("contracts/cli.md")
        .replace("\r\n", "\n");
        let checked_in = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../packaging/tinio.service"
        ))
        .expect("packaging/tinio.service");
        assert!(
            contract.contains(checked_in.trim_end()),
            "contracts/cli.md's `service` section must carry the rendering verbatim, so the \
             document an operator reads and the file a machine installs cannot drift"
        );
    }

    #[test]
    fn notifications_are_inert_without_a_socket() {
        // No `NOTIFY_SOCKET` in this process, so nothing is sent and nothing is reported:
        // a run off systemd is the normal case and not a failure. The assertion is the
        // *outcome* — each call answers `false` (no socket, so nothing left the process)
        // rather than merely not panicking — which is why `notify` returns the boolean.
        //
        // The boolean is this module's, not the crate's: `sd_notify::notify` itself answers
        // `Ok(())` when `NOTIFY_SOCKET` is absent (`sd-notify-0.5.0/src/lib.rs:179-181`), so
        // a helper that returned `Ok`-ness would answer `true` here and the assertion below
        // would be testing nothing. The one-argument signature is that crate's real one.
        assert!(
            env::var_os("NOTIFY_SOCKET").is_none(),
            "the harness clears it"
        );
        assert!(!notify(NotifyState::Ready), "no socket, nothing sent");
        assert!(!notify(NotifyState::Stopping));
        assert!(!notify(NotifyState::Watchdog));
        // And the public entry points stay callable and silent on the same path.
        ready();
        stopping();
        assert!(
            watchdog_enabled().is_none(),
            "no WATCHDOG_USEC, no heartbeat"
        );
    }
}
