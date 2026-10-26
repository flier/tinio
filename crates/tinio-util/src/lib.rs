//! Shared runtime utilities for tinio: small backend-agnostic helpers
//! built on by both the storage backends and the server.
//!
//! - `bench` — the benches' pinned data root and flush-cost control (behind
//!   the `bench` feature: the one copy every bench target in the workspace
//!   runs on, which is why it is a library module and not a bench-local
//!   include; see the module's docs for the false "2.7× regression" a
//!   `TMP`-following root produced).
//! - [`mod@env`] — the `TINIO_READ_ONLY`, `TINIO_SANDBOX` and `TINIO_STATE_DIR` names, one
//!   definition per variable for the CLI that parses them and the tests that set or clear
//!   them. No reading helpers: the reader is the CLI's.
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
//!   `testing` re-exports the module. **Every harness spawn site takes it**
//!   (docs/tests.md §Harness & guards): a raw `Command::spawn` is a review
//!   finding — recorded with its reason, not left unremarked.
//! - `job_object` — the Win32 job-object call sequence itself (create with
//!   its limits already set, assign, terminate, close on `Drop`), the one
//!   primitive `job`'s guard and `tinio-sandbox-windows`'s per-launch `Job`
//!   are both expressed on. `#[cfg(windows)]` and **not** feature-gated:
//!   the launcher creates its job in a production build, so this is the one
//!   part of the crate's Windows FFI that a shipped binary compiles.
//! - [`lockmap`] — the evicting per-key lock map (multipart part-write
//!   locks, per-object conditional-PUT locks, per-bucket directory
//!   mutation locks).
//! - [`metrics`] — the five cross-backend Prometheus families (write failures,
//!   pass duration/failures, read-only, corrupt rows): one declaration on the
//!   default registry, because a second backend registering a name is
//!   `AlreadyReg`. Backends call `record_*`; the module names no `tracing`,
//!   `tinio-core` or `tokio` type.
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
//! - `state` — state-directory resolution and hardening (FR-023) behind the `state` feature:
//!   the `<home>/.tinio/roots/<hash>/` derivation, the 0700/symlink rules and the pg run's
//!   synthetic root, with the error type the CLI wraps.
//! - [`switch`] — the tolerant `1`/`true`/`0`/`false` grammar, the one
//!   definer behind `TINIO_READ_ONLY`/`TINIO_SANDBOX` and the CLI's
//!   `--read-only`/`--sandbox` flags.
//! - [`time`] — [`civil_from_days`](time::civil_from_days), the one calendar conversion behind the tier verbs' RFC 3339 report timestamps and the test signer's SigV4 `x-amz-date`.
//! - `testing` — the conformance test harness (behind the `testing`
//!   feature: requires `tinio-core` types).
//! - `win32` — `error_message`, the one sentence a failed `*W` call is reported
//!   with (the management plane's pipe and the sandbox's ACE walk), and
//!   `system_directory`, the kernel's own answer where the system directory is
//!   (the sandbox's `icacls` oracle and the CLI config root's independent DACL test).
//!   `#[cfg(windows)]` and **not** feature-gated: every reader compiles them in a
//!   production build. The names are code spans rather than links because the
//!   module is absent on the unix host the doc gate runs on.
//! - `wide` — `to_wide`, the NUL-terminated UTF-16 form every
//!   `*W` call takes its name from, lifted here out of the dissolved
//!   `tinio-winsec` so the management plane's pipe name and the sandbox's own
//!   Win32 calls share one copy. `#[cfg(windows)]` and **not** feature-gated:
//!   both callers encode names in a production build. **This is the writer half
//!   of the rule and the reader half is `tinio-sandbox-windows`'s**
//!   (`from_wide_units` and `from_wide` at that crate's root), each naming the
//!   other.

#[doc(hidden)]
pub extern crate tinio_core as _core;

// **Behind the `bench` feature, the way `testing` is**: this module hands out
// directories and measures the host's flush latency, and `tinio-util` is a
// production dependency of several crates — bench-only code must not compile
// into a default build (the `expect`s below are fixture assertions on paths
// the bench itself named, `docs/style.md` §Panics).
#[expect(
    clippy::expect_used,
    reason = "test support: behind `bench` (docs/style.md §Panics)"
)]
#[cfg(feature = "bench")]
pub mod bench;

pub mod env;
pub mod fsutil;
pub mod lockmap;
pub mod metrics;
pub mod pacing;
pub mod path;
pub mod process;
#[cfg(feature = "render")]
pub mod render;
pub mod secret;
#[cfg(feature = "state")]
pub mod state;
pub mod switch;
pub mod time;

// **The job guard sits outside the `testing` feature on purpose.** The feature exists for the
// conformance suite and everything that pulls in `tinio-core` behind it; the guard needs `std`, a
// child handle and — on Windows — `GetCurrentProcess`, plus the job-object calls `job_object` owns
// — on Linux, one `prctl` — and its own tests have to run in the default graph (`cargo test -p
// tinio-util`, which is what each platform's leg runs) rather than only for a caller who asked for
// a feature. `testing` re-exports the module, so a harness still spells it `tinio_util::testing::job`.
#[allow(
    clippy::expect_used,
    clippy::allow_attributes,
    reason = "test support: the module's `expect`s sit in its `#[cfg(all(test, windows))]` tests — live in this crate's test build, cfg'd away when a dependent compiles it non-test — so one attribute cannot be `expect` in both"
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

// **The wide-string writer is gated on the target and not on a feature**, the way the
// job-object sequence is: the management plane's pipe name and the sandbox's own Win32
// calls both need it in a production build, and `tinio-util` is where the workspace keeps
// the one copy of a small runtime helper both crates already depend on.
#[cfg(windows)]
pub mod wide;

// **The failure sentence takes the same gate for the same reason**: both readers of it —
// the pipe's create and the ACE walk — report in a shipped build, so a feature would only
// make the one definer possible to compile out from under one of them.
#[cfg(windows)]
pub mod win32;

#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: behind `testing` (docs/style.md §Panics)"
)]
#[cfg(feature = "testing")]
pub mod testing;
