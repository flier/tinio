//! Shared runtime utilities for tinio: small backend-agnostic helpers
//! built on by both the storage backends and the server.
//!
//! - [`lockmap`] — the evicting per-key lock map (multipart part-write
//!   locks, per-object conditional-PUT locks, per-bucket directory
//!   mutation locks).
//! - [`pacing`] — [`sleep_checked`](pacing::sleep_checked), the chunked
//!   sleep that ends the moment a `watch` shutdown channel flips (the
//!   backends' scanner and sweeper pacing).
//! - [`path`] — the containment rule ("is this path inside that root?"),
//!   the one definer the filesystem backend's read-only guard and the
//!   server launcher's exposure set share.
//! - [`process`] — [`kill_and_reap`](process::kill_and_reap), the one
//!   silent kill-and-reap a spawned child's teardown paths share.
//! - `render` — the one Handlebars renderer (strict mode, no escaping)
//!   behind the `render` feature, for the status report, the systemd unit
//!   and the Seatbelt profile.
//! - [`secret`] — the zeroizing secret string and the box that redacts it,
//!   shared by the config's credentials and the management plane's tokens.
//! - [`switch`] — the tolerant `1`/`true`/`0`/`false` grammar, the one
//!   definer behind `TINIO_READ_ONLY`/`TINIO_SANDBOX` and the CLI's
//!   `--read-only`/`--sandbox` flags.
//! - `testing` — the conformance test harness (behind the `testing`
//!   feature: requires `tinio-core` types).

#[cfg(feature = "testing")]
#[doc(hidden)]
pub extern crate tinio_core as _core;

pub mod lockmap;
pub mod pacing;
pub mod path;
pub mod process;
#[cfg(feature = "render")]
pub mod render;
pub mod secret;
pub mod switch;

#[cfg(feature = "testing")]
pub mod testing;
