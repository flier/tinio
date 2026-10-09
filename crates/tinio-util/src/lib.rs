//! Shared runtime utilities for tinio: small backend-agnostic helpers
//! built on by both the storage backends and the server.
//!
//! - [`fsutil`] — the filesystem primitives the storage backends are written
//!   on, lifted here so there is one copy of each: the atomic commit protocol
//!   ([`commit`](fsutil::commit) — parent creation, rename, the EXDEV
//!   fallback, the D1 post-rename directory sync and the F03 ancestor chain),
//!   the staged-temp `Drop` discipline
//!   ([`remove_staged_temp`](fsutil::remove_staged_temp)), the bounded-chunk
//!   ranged body stream ([`file_stream`](fsutil::file_stream)), and the
//!   helper trio [`ensure_dir`](fsutil::ensure_dir) /
//!   [`ok_if_missing`](fsutil::ok_if_missing) /
//!   [`entries_of`](fsutil::entries_of). **The copies it replaced had already
//!   drifted** — the block store's publish had the parent-directory fsync but
//!   not the cross-volume fallback, its staged-temp `Drop` unlinked on the
//!   runtime worker, and its read loop allocated a fresh buffer per chunk.
//!   Nothing here names a backend: the module may use `tinio-core`, `tokio`,
//!   `std` and this crate, never `tinio-fs`.
//! - `job` — the guard that makes a test-spawned process die with the test
//!   process, and `job::spawn`, the one spelling a harness's spawn sites
//!   use for it. A job object on Windows (`GuardJob`, `guard`), the
//!   parent-death signal on Linux, nothing on macOS; reachable without the
//!   `testing` feature, so that its own tests run in the default graph;
//!   `testing` re-exports the module. **The gate `tests/spawn_guard.rs`
//!   holds that spelling up**: it fails on a raw `Command::spawn` under
//!   `crates/*/tests/**` that its allowlist does not name with a reason,
//!   and a `#[cfg(test)]` module inside `src/` is outside that scan.
//! - `job_object` — the Win32 job-object call sequence itself (create with
//!   its limits already set, assign, terminate, close on `Drop`), the one
//!   primitive `job`'s guard and `tinio-sandbox-windows`'s per-launch `Job`
//!   are both expressed on. `#[cfg(windows)]` and **not** feature-gated:
//!   the launcher creates its job in a production build, so this is the one
//!   part of the crate's Windows FFI that a shipped binary compiles.
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
//!   silent kill-and-reap a spawned child's teardown paths share, and
//!   [`poll_exit`](process::poll_exit), the bounded poll every wait on a
//!   child is written on.
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

#[doc(hidden)]
pub extern crate tinio_core as _core;

pub mod fsutil;
pub mod lockmap;
pub mod pacing;
pub mod path;
pub mod process;
#[cfg(feature = "render")]
pub mod render;
pub mod secret;
pub mod switch;

// **The job guard sits outside the `testing` feature on purpose.** The feature exists for the
// conformance suite and everything that pulls in `tinio-core` behind it; the guard needs `std`, a
// child handle and — on Windows — `GetCurrentProcess`, plus the job-object calls `job_object` owns
// — on Linux, one `prctl` — and its own tests have to run in the default graph (`cargo test -p
// tinio-util`, which is what each platform's leg runs) rather than only for a caller who asked for
// a feature. `testing` re-exports the module, so a harness still spells it `tinio_util::testing::job`.
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: behind `testing` (docs/style.md §Panics)"
)]
#[cfg(any(test, feature = "testing"))]
pub mod job;

// **`job_object` is the one part of this crate's Windows FFI that a production build compiles**,
// and the gate above is what keeps that sentence narrow: `tinio-sandbox-windows` creates its
// per-launch job inside the launcher's own build (design decision 20), so the call sequence the
// guard and the launcher used to spell twice is gated on the target alone. The guard stays out of
// those builds, and so does everything behind `testing` — nothing here needs a feature, a
// `tinio-core` type or `tracing` (both are plain dependencies now — the `fsutil`
// primitives need them — and neither guard reaches for either).
#[cfg(windows)]
pub mod job_object;

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: behind `testing` (docs/style.md §Panics)"
)]
#[cfg(feature = "testing")]
pub mod testing;
