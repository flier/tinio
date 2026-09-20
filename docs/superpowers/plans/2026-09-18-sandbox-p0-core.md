# `--sandbox` P0 (core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the part of `--sandbox` that every platform shares — the `Backend` contract, its vocabulary, the launcher machinery both child-launching platforms use, the facade, and the three platform crates as compiling stubs.

**Architecture:** Five crates. `tinio-sandbox-core` owns the trait, the shared vocabulary and the launcher machinery. `tinio-sandbox-linux`, `-macos` and `-windows` implement the trait for their platform; macOS and Windows are `Unsupported` stubs here, present so the trait's shape is exercised from both an in-process backend and a launcher backend rather than guessed from one. `tinio-sandbox` is a thin facade that re-exports the core and, behind `[target.'cfg(…)'​.dependencies]`, the one platform implementation that applies. **This plan builds all five crates and fills none of the platform bodies.**

**Tech Stack:** Rust 2024, `thiserror` for error types, `derive_more` for `Display`, `serde` + `serde_json` for the event and payload wire types, `sse-core` for SSE framing, and `rustix` (in the platform crates the stubs declare but do not yet use).

**Spec:** `docs/superpowers/specs/2026-09-18-sandbox-design.md` — the design document and the authority for every decision cited below as `design §N` or `decision N`. Read it alongside this plan.

**This plan is the shared half, and P1, P2 and P3 all depend on it.** They are `2026-09-18-sandbox-p1-linux.md`, `-p2-windows.md` and `-p3-macos.md`, and each opens with a Prerequisite block naming the surface here that it consumes. **The rule that keeps the three of them from drifting apart is that the launcher-shared surface is defined here, once, and the platform plans consume it** — if a later phase needs something else shared, it comes back to this plan rather than growing a second copy. That rule is not a style preference: the three platform plans were written in parallel against an earlier single-document plan, each needed core items it did not have, each invented its own, and the drift was found by a cross-plan check rather than by any plan failing.

**What all four documents share, rather than what this one builds, has its own home: `2026-09-18-sandbox-shared.md`.** The conventions, the crate shapes, the list of what a launcher platform owns, the names two documents must spell alike and the three relays are stated there once, and this plan refers to them by section. What stays here is the surface itself — the names and signatures P0 Task 2 fixes — because that is what this plan builds.

## Global Constraints

**The conventions the four plans share are stated once, in `2026-09-18-sandbox-shared.md` (*shared* from here on): §1 owns test-first, the panic rule, the doc-example rule, imports, the manifest rules, the per-crate lint allocation, English only, attribution, hard-wrapping and git writes, each with the document it belongs to.** A copy of a rule here would be the second copy that document exists to prevent. What is left is what this plan adds:

- **No `unsafe` anywhere in this plan.** Every crate it creates is `unsafe_code = "forbid"`; the workspace's authorised `unsafe` sites belong to the platform backends — `tinio-sandbox-linux` (P1) and the Windows FFI (P2) — and design decision 14 enumerates them per capability.
- **This plan is where the crate topology and the two crate shapes are laid down**, as shared §2 states them: Task 3 creates the facade and the three platform stubs, and its Step 7 asserts the lint each one carries.
- **The launcher-shared surface is defined here, once, and the platform plans consume it** (shared §3). If a later phase needs something else shared, it comes back to this plan rather than growing a second copy — Task 2 Step 6 records what that rule cost the last time it was broken.

---

### Task 1: `tinio-sandbox-core` — vocabulary and the builder

**Files:**
- Create: `crates/tinio-sandbox-core/Cargo.toml`
- Create: `crates/tinio-sandbox-core/src/{lib.rs,error.rs,sandbox.rs}`
- Modify: `Cargo.toml` (workspace root — add the workspace dependency)
- Modify: `README.md` (crate table row)

**Interfaces:**
- Consumes: nothing.
- Produces: `Exposure`, `Sandbox` (`new`, `expose`, `device_files`, `exposures`, `device_files_enabled`, `from_exposures`), `Error`, `Result` — and, **re-exported from `tinio-core` rather than defined here**, `AccessMode`.

The builder is **pure data**. Validation that needs the filesystem belongs to the platform crate's `confine`, so the whole builder is testable with no privileges on any platform.

- [x] **Step 1: Write the failing tests**

`crates/tinio-sandbox-core/src/sandbox.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn expose_records_path_and_access() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        let exposures = sandbox.exposures();
        assert_eq!(exposures.len(), 1);
        assert_eq!(exposures[0].host, Path::new("/srv/data"));
        assert_eq!(exposures[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn duplicate_paths_keep_the_most_permissive_access() {
        // `serve` exposes the served root read-write and its state directory
        // read-write again; a later narrower entry must never downgrade it.
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadWrite)
            .expose("/srv/data", AccessMode::ReadOnly);
        assert_eq!(sandbox.exposures().len(), 1);
        assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn from_exposures_keeps_the_most_permissive_access_too() {
        // **The path a launcher platform takes, and the one the rule had to
        // move onto** (the cleanup that extracted `merge_exposure`). A
        // called-out set is assembled from somewhere else — a canonicalised
        // rebuild of an existing set, a set merged with another, the
        // executable's directory added — so the same path can arrive twice with
        // different access, and a vector stored as given handed the platform
        // both entries and left the choice of which to apply to it.
        //
        // Both orders, because the merge has to reach the same answer either
        // way: `data` is widened by its second entry, `logs` arrives narrower
        // after a wider one and must not lose it.
        let called_out = vec![
            Exposure { host: "/srv/data".into(), access: AccessMode::ReadWrite },
            Exposure { host: "/srv/data".into(), access: AccessMode::ReadOnly },
            Exposure { host: "/srv/logs".into(), access: AccessMode::ReadOnly },
            Exposure { host: "/srv/logs".into(), access: AccessMode::ReadWrite },
        ];
        let sandbox = Sandbox::from_exposures(called_out, false);
        assert_eq!(sandbox.exposures().len(), 2, "one entry per path");
        assert_eq!(sandbox.exposures()[0].host, Path::new("/srv/data"));
        assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadWrite);
        assert_eq!(sandbox.exposures()[1].host, Path::new("/srv/logs"));
        assert_eq!(sandbox.exposures()[1].access, AccessMode::ReadWrite);
    }

    #[test]
    fn the_builder_does_not_touch_the_filesystem() {
        // Recorded verbatim, even though the path does not exist — validation
        // is the platform's job, so the builder stays testable everywhere.
        let sandbox = Sandbox::new().expose("/no/such/path/anywhere", AccessMode::ReadOnly);
        assert_eq!(sandbox.exposures()[0].host, Path::new("/no/such/path/anywhere"));
    }

    #[test]
    fn device_files_are_opt_in() {
        assert!(!Sandbox::new().device_files_enabled());
        assert!(Sandbox::new().device_files().device_files_enabled());
    }
}
```

`crates/tinio-sandbox-core/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn a_step_error_names_the_operation() {
        let err = Error::Step {
            step: "mount",
            source: io::Error::from_raw_os_error(1),
        };
        assert!(err.to_string().contains("mount"), "{err}");
    }

    #[test]
    fn unsupported_carries_an_operator_facing_reason() {
        let err = Error::Unsupported { detail: "no user namespaces" };
        assert!(err.to_string().contains("no user namespaces"));
    }

    #[test]
    fn an_io_error_converts_so_a_wait_closure_can_use_the_question_mark() {
        // The one conversion the launcher platforms' wait closures are written
        // against: `Child::new(move || Ok(child.wait()?))`. Without it
        // there is no `?` that reaches this crate's `Result` from a platform's
        // `std::process` call.
        let err: Error = io::Error::other("boom").into();
        assert!(matches!(err, Error::Wait(_)), "{err}");
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-core`
Expected: FAIL — the package does not exist yet. Create `Cargo.toml` and an empty `src/lib.rs` first if the workspace refuses to load.

- [x] **Step 3: Write `Cargo.toml`**

```toml
[package]
name = "tinio-sandbox-core"
version.workspace = true
edition.workspace = true
description = "The confinement contract for tinio: the Backend trait, its vocabulary, and the launcher machinery"
publish = false

[dependencies]
bytes.workspace = true
derive_more = { workspace = true, features = ["debug"] }
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
sse-core.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["io-util"] }
tracing.workspace = true

tinio-core.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["io-util", "macros", "rt"] }
tracing-subscriber.workspace = true

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "forbid"
```

**`tokio` with `io-util` only, and the dev-dependency is where the runtime lives.** The relay moves bytes between a child's pipes, so its reads and writes are async — `tokio::io::{AsyncBufRead, AsyncWrite}` are the traits, and `io-util` supplies the `read_buf` / `write_all` convenience methods on them *and* the `Sink` impls that let a `Vec<u8>` be the writer in a test. The **production** feature list is one entry: no `rt`, no `macros`, no `io-std`, no `net`. The core never spawns, never drives a runtime, never touches a standard stream and never blocks — it is a set of async functions that whichever runtime the caller already has will poll, and keeping the list to one entry is what makes that claim checkable rather than aspirational.

**`io-std` is deliberately absent, and that is why the tests write into a `Vec<u8>`.** `tokio::io::stdout()` needs `io-std`, and the core has no business owning a standard stream: the launcher platforms pass one in. The crate that *does* pass one names the feature itself — P2 and P3 both build their relay thread with `tokio::io::stdout()`, and both declare `io-std` for it.

**The reader is the other half of that split, and this plan deliberately says nothing about it.** `Observer::observe` takes an `impl AsyncBufRead`; where a launcher gets one is per-platform and belongs to the platform plan, because a child's stdout is a pipe the platform created. On Windows that is `tinio-sandbox-windows`' business, written down in `2026-09-18-sandbox-p2-windows.md` (the dependency note in Task 1 and the relay thread in Task 9), including the tokio feature that makes it true. Naming the traits and not the types is what keeps this crate the shared half.

`dev-dependencies` is where `rt` and `macros` belong, because they exist for the tests: the async paths need *a* runtime to be *tested* in, and a current-thread one is enough. `io-util` is repeated there because the tests exercise the same writer impls the production code requires — Cargo unifies the features anyway, so the repetition documents the requirement rather than changing the build. The split is the whole point: a consumer of this crate gets the async functions and pays for no runtime it does not already have, while the tests can still `#[tokio::test]`.

The **parsing is still synchronous** and that is deliberate: `sse-core`'s `SseDecoder` is a plain state machine over a byte slice, so the frame-level logic is exercised with no runtime at all. Only the I/O around it is async.

`derive_more` needs **one** feature: `debug`, for the closure or credential-bearing field each of `Child`, `LaunchPayload` and `Adopted` skips. The `display` feature went with `access.rs` — the `Display` that needed it belonged to a type this crate no longer defines.

**`tinio-core` is a workspace crate and not a new dependency**, so the dependency ledger is unchanged; what changes is one edge inside the workspace, and it is the edge this crate's whole vocabulary rests on. It exists because `AccessMode` is already the type FR-023 resolves at the `serve` call site, and the alternative — a second enum here — put a conversion between two spellings of one fact exactly where the operator's read-only decision meets the exposure set. The core is still platform-free and FFI-free; `tinio-core` is the storage contract, which the consuming server already links. `serde` stays `workspace = true` and only the core asks for `derive` — it is the only crate here that has a wire type.

`bytes` is declared directly even though it arrives transitively, because the core names `BytesMut` in its own signatures and a crate that uses a type should depend on it.

**`sse-core`, not `sse-stream`, and the difference matters here.** `sse-core` is a zero-I/O state machine: `SseDecoder::next(&mut impl Buf) -> Option<Result<SseEvent, PayloadTooLargeError>>`, synchronous, `no_std`, and with the default features off its dependencies are `bytes`, `memchr` and `thiserror` — all already in the tree — where `sse-stream` lists `futures-util`, `http-body` and `http-body-util` as **non-optional** dependencies, and decodes from a `TryStream<Ok: Buf>`, which for a pipe means writing an adapter *either way*. Its `SseByteStream` also depends on `sse-core` itself, so choosing it would mean three new dependencies to replace three lines of encoding. There is nothing to configure away: those deps are unconditional there.

**One `SseDecoder` serves the whole stream, and that is not an optimisation.** The decoder is *resumable*: an event split across two reads is assembled because the bytes already parsed out of the first read are held **inside the decoder** — `consume_until_newline` appends a partial `data:`/`event:`/`id:` value to its own buffer and advances the input past it (`sse-core 0.2.3`, `src/decode.rs`). So a decoder built per call would consume those bytes and then be dropped, and every event whose chunk boundary landed mid-value would vanish — silently, and only on a real pipe. The doc comment on `next()` — "the decoder does not store unparsed bytes internally" — means *unparsed* ones: bytes that belong to a field value the decoder has already matched are its own state. [`Observer::observe`] therefore creates one decoder and passes it down, and the pinned test in Step 5 feeds three bytes at a time to make sure a split frame is assembled rather than dropped.

**There is no `getrandom` line, and no nonce.** An earlier draft carried a one-shot nonce in the payload as the child's discriminator; it was removed because it could not do that job — the parent writes the nonce to the same stdin it writes everything else to, so anything able to forge a payload already knows the value. What actually discriminates a confined child is the argv listener element plus a payload that decodes (design decision 17, amended). With the nonce gone, the core has no need for an entropy source at all.

- [x] **Step 4: Write `error.rs`**

```rust
//! The confinement error vocabulary, shared by every backend.

use std::io;
use std::path::PathBuf;

/// A confinement failure. Every variant is fatal to startup: design §11
/// requires the process to refuse to run rather than continue unconfined.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A syscall or platform call refused. `step` names the operation, never
    /// a path, so the message stays stable when a temporary jail path is
    /// involved.
    #[error("sandbox step `{step}` failed: {source}")]
    Step {
        /// A short operation name (`unshare`, `mount`, `CreateProcess`, …).
        step: &'static str,
        /// The underlying platform error.
        #[source]
        source: io::Error,
    },
    /// The platform cannot confine this process at all.
    ///
    /// Reserved for exactly that: no user namespaces and not root, a stub whose
    /// mechanism is not written yet, a kernel with no seccomp table. **A request
    /// this process cannot satisfy is not this variant** — an exposure set with
    /// nothing in it is reported as [`Self::Step`] with the rejected step named,
    /// so that "unsupported here" keeps pointing at the platform rather than at
    /// the operator's argument.
    #[error("sandbox unsupported here: {detail}")]
    Unsupported {
        /// Why, in operator-facing terms, with the remedy where one exists.
        detail: &'static str,
    },
    /// A path handed to [`crate::Sandbox::expose`] cannot be expressed inside
    /// the confinement — it is not absolute, it does not exist, or it is the
    /// confinement's own root.
    #[error("path `{path}` cannot be exposed: {reason}")]
    BadPath {
        /// The offending path.
        path: PathBuf,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// Something done *to a path* failed.
    ///
    /// Distinct from [`Self::Step`], which names an operation but no path, and
    /// from [`Self::BadPath`], which names a path but carries a static reason
    /// rather than a source. Design §11's Windows rows need both halves at
    /// once: "error naming the directory **and** the underlying failure", where
    /// that failure may be a Win32 code or a tool's exit status and text.
    #[error("sandbox could not {operation} `{path}`: {source}")]
    Launch {
        /// The operation, in operator-facing words (`grant ACL on`, `open`).
        operation: &'static str,
        /// The path it was done to.
        path: PathBuf,
        /// The underlying failure, whichever kind it was.
        #[source]
        source: io::Error,
    },
    /// Waiting for the confined child failed.
    ///
    /// **This is the `#[from]` target for [`std::io::Error`], and the only
    /// `From` impl on this type.** It exists so a launcher can write its wait
    /// closure the way the platform's own API reads —
    /// `Child::new(move || Ok(child.wait()?))` — without naming a step
    /// the core cannot know. A platform that *does* know the operation's name
    /// builds [`Self::Step`] instead, which is why the two never disagree: only
    /// one of them happens by accident.
    #[error("sandbox could not wait for the confined child: {0}")]
    Wait(#[from] io::Error),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

- [x] **Step 5: Write `sandbox.rs`**

**There is no `access.rs`, and `AccessMode` is not this crate's type.** `tinio-core` already owns the write policy of a served plane — `tinio_core::AccessMode`, with `ReadWrite` as its `Default`, `ReadOnly`, `is_read_only(self) -> bool` and `From<bool>` (`crates/tinio-core/src/mode.rs`) — and the sandbox needs exactly that vocabulary. FR-023's read-only resolution already produces an `AccessMode` in `serve`, so a second enum here would put a conversion between two spellings of one fact at the call site: before this correction, both P1's and P3's `serve` sections had to rebuild the sandbox's own enum out of the `AccessMode` they had already resolved, and that line is what disappears.

So the core **depends on `tinio-core` and re-exports the type**, `Exposure::access` is a `tinio_core::AccessMode`, and **nothing is added to it**: the core's own `is_writable`, `as_str` and `Display` do not exist. A backend that branches writes `if !access.is_read_only()`; a tracing field renders it with `?`/`{:?}`. The variant that used to be spelled first is not the default either — `AccessMode::default()` is `ReadWrite`, which is the right default for an exposure only because `expose` always states the access explicitly.

`sandbox.rs` carries `Exposure` and `Sandbox` exactly as tested in Step 1, with the doc examples from `crates/tinio-sandbox-core`:

```rust
//! The confinement request: which host paths must stay reachable, and how.

use std::path::PathBuf;

use crate::_core::AccessMode;

/// One host path the confined process must be able to reach.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Exposure};
///
/// let exposure = Exposure { host: Path::new("/srv/data").into(), access: AccessMode::ReadWrite };
/// assert!(!exposure.access.is_read_only());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exposure {
    /// The path as the host sees it. The confined process sees the same
    /// string — that is what makes the confinement invisible to the rest of
    /// the program (design decision 9).
    pub host: PathBuf,
    /// Whether the confined process may write there.
    pub access: AccessMode,
}

/// The set of paths a confined process may reach, plus the fixed device files.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Sandbox};
///
/// let sandbox = Sandbox::new()
///     .expose("/srv/tinio", AccessMode::ReadWrite)
///     .device_files();
/// assert!(sandbox.device_files_enabled());
/// ```
#[derive(Debug, Clone, Default)]
pub struct Sandbox {
    exposures: Vec<Exposure>,
    device_files: bool,
}

impl Sandbox {
    /// An empty request: nothing exposed, no device files.
    pub fn new() -> Self {
        Self::default()
    }

    /// Require `host` to remain reachable, with `access`.
    ///
    /// Exposing the same path twice keeps the **most permissive** access: a
    /// narrower entry can never silently downgrade a wider one.
    pub fn expose(mut self, host: impl Into<PathBuf>, access: AccessMode) -> Self {
        merge_exposure(&mut self.exposures, host.into(), access);
        self
    }

    /// Also bind the platform's device files, where the platform has the
    /// concept. Linux binds `/dev/{null,zero,random,urandom}`; they are
    /// defensive rather than load-bearing, because Rust's `getrandom` is a
    /// syscall and needs no `/dev` (design decision 12).
    pub fn device_files(mut self) -> Self {
        self.device_files = true;
        self
    }

    /// The exposures, in the order they were added.
    pub fn exposures(&self) -> &[Exposure] {
        &self.exposures
    }

    /// Whether [`Self::device_files`] was called.
    pub fn device_files_enabled(&self) -> bool {
        self.device_files
    }

    /// Take a set that was already built, with the device-file flag it was
    /// built with.
    ///
    /// **It applies [`Self::expose`]'s most-permissive rule to the vector it is
    /// given**, so a caller merging two sets — or rebuilding one it has just
    /// transformed — cannot leave a path carrying both a read-only entry and the
    /// read-write entry that was meant to widen it. Applying the rule to a set
    /// that already obeys it changes nothing: the first entry for a path stays
    /// where it was and a later, narrower duplicate merges into it.
    ///
    /// `expose` is the only way to *add*, so a caller that must transform the
    /// whole set — the launcher platforms canonicalise paths and add the
    /// executable's directory — would otherwise iterate [`Self::exposures`] and
    /// re-`expose` each one, rebuilding the set to change it. With a dozen
    /// exposures that is a dozen re-insertions, and a silent second application
    /// of a rule the caller has already applied.
    ///
    /// **The device-file flag is a parameter rather than something this
    /// remembers**, and that is the whole reason the signature has two
    /// arguments: a one-argument form drops the flag, so a caller rebuilding a
    /// set that had called [`Self::device_files`] would get a jail with no
    /// `/dev` and nothing to notice it. A caller transforming an existing set
    /// passes [`Self::device_files_enabled`]; a caller building one from
    /// scratch says `true` or `false` and means it.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{AccessMode, Sandbox};
    ///
    /// let built = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly).device_files();
    /// let taken = Sandbox::from_exposures(
    ///     built.exposures().to_vec(),
    ///     built.device_files_enabled(),
    /// );
    /// assert_eq!(taken.exposures(), built.exposures());
    /// assert!(taken.device_files_enabled(), "the flag travels with the set");
    /// ```
    pub fn from_exposures(exposures: Vec<Exposure>, device_files: bool) -> Self {
        let mut merged = Vec::with_capacity(exposures.len());
        for exposure in exposures {
            merge_exposure(&mut merged, exposure.host, exposure.access);
        }
        Self { exposures: merged, device_files }
    }
}

/// Insert `host` with `access`, or widen the entry already there for that path.
///
/// **The most-permissive rule lives here rather than on the `expose` path**, so
/// that a set rebuilt through [`Sandbox::from_exposures`] obeys it too: with the
/// rule inside `expose` alone, a vector carrying two entries for one path
/// arrived at the platform as it stood, and a path the caller meant to expose
/// read-only went in with a read-write entry beside it.
///
/// The incoming value rather than an `AccessMode::ReadWrite` literal: the two
/// agree while the enum has two variants, and a literal would silently coalesce
/// whatever variant it grows.
fn merge_exposure(exposures: &mut Vec<Exposure>, host: PathBuf, access: AccessMode) {
    match exposures.iter_mut().find(|e| e.host == host) {
        Some(existing) => {
            if !access.is_read_only() {
                existing.access = access;
            }
        }
        None => exposures.push(Exposure { host, access }),
    }
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

- [x] **Step 6: Write `lib.rs`**

```rust
//! The confinement contract for tinio.
//!
//! [`Sandbox`] collects the host paths a confined process must reach and the
//! access each gets. Turning that into a jail is a platform's job: this crate
//! holds only what more than one platform needs — the vocabulary, the
//! `Backend` trait, and the launcher machinery the two child-launching
//! platforms share. The platform implementations live in
//! `tinio-sandbox-linux`, `-macos` and `-windows`, and the `tinio-sandbox`
//! facade re-exports the one that applies.
//!
//! See `design §1` in `docs/superpowers/specs/2026-09-18-sandbox-design.md`.
//!
//! # Examples
//!
//! ```
//! use tinio_sandbox_core::{AccessMode, Sandbox};
//!
//! let sandbox = Sandbox::new()
//!     .expose("/srv/tinio", AccessMode::ReadWrite)
//!     .expose("/srv/tinio/static", AccessMode::ReadOnly);
//!
//! assert_eq!(sandbox.exposures().len(), 2);
//! ```

#[doc(hidden)]
pub extern crate tinio_core as _core;

mod error;
pub mod sandbox;

// **The whole public surface, grouped by where each name comes from.**
// Grouping is not decoration: a flat list is what let `AccessMode` drift into the
// `launcher` group in an earlier draft, where it does not exist — the failure
// reads as a missing name three phases later rather than as a wrong line here.
//
// `AccessMode` is the one name here that is not this crate's own: it is
// `tinio-core`'s write policy, re-exported so a consumer of the sandbox never
// has to know which crate the vocabulary came from, and so that a value the
// read-only work already produced is the value an exposure takes. It is
// therefore reached through the dependency's alias declared at the top of this
// file — `pub use crate::_core::AccessMode`, on its own line after the
// `self::{…}` group because `tinio-core` is a dependency and not one of this
// crate's modules.
//
// The facade re-exports exactly this list, name for name, so that a consumer
// naming `tinio_sandbox::` can reach everything the core publishes. Keeping
// that equality is a rule this file is edited under rather than something a
// test proves: `the_launcher_surface_is_re_exported` checks the names it lists,
// so a name added here still has to be added there by hand — see the comment on
// that test.
pub use self::{
    error::{Error, Result},
    sandbox::{Exposure, Sandbox},
};
pub use crate::_core::AccessMode;
```

**`tinio-core` is reached through the alias, not by its own name** (`docs/style.md` §Imports, whose facade precedent is `crates/tinio/src/lib.rs:25-45`): `#[doc(hidden)] pub extern crate tinio_core as _core;` at the crate root, then `pub use crate::_core::AccessMode;`. **The public path is unchanged** — a consumer still names `tinio_sandbox_core::AccessMode`, and only the path the re-export is written through differs.

**`Backend` is plain code text in the crate doc, not `[`Backend`]`, and that is a gate rather than a preference.** The trait does not exist until Task 2, and an intra-doc link to a name that is not in scope is an error under `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` — a leg of both the local Standard acceptance block (`docs/tests.md`) and CI's docs job (`.github/actions/run-test-leg/action.yml`). Rustdoc reports `unresolved link to `Backend`` and fails the crate, exit 101, on a tree whose tests, clippy and fmt are all green; the implementer's verification set does not run `cargo doc`, which is why this one is easy to miss. **Task 2's Step 7 restores the link** (`[`Backend`]`) in the same edit that adds the trait, so the crate ends the phase with the link it wanted and no intermediate state is red.

Add to the root `Cargo.toml` `[workspace.dependencies]` — each external line in its own alphabetical place, and the crate among the `tinio-*` paths:

```toml
bytes = "1"
sse-core = { version = "0.2", default-features = false }

tinio-sandbox-core = { path = "crates/tinio-sandbox-core" }
```

`bytes` is already declared in a workspace where several crates use it; if so, this line already exists and only the core's reference is new. `serde` and `serde_json` are likewise **already** in `[workspace.dependencies]`, and so is `tinio-core` — the core's `tinio-core.workspace = true` needs no root edit at all, because the pin was already there for the storage crates.

`sse-core` is the plan's one genuinely new third-party dependency, and it is the one the design's Bookkeeping entry has to gain — the earlier draft claimed "No new third-party dependency" on the strength of a hand-built seccomp filter, which was true of the Linux crate and silent about the core. Pin it to `"0.2"` (current `0.2.3`); the alternative `sse-stream` is explicitly rejected in the Cargo block above, for its unconditional async dependencies.

**`default-features = false`, and it is load-bearing rather than tidy.** `sse-core`'s defaults are `["fastrand", "std", "stream"]`, and `stream` pulls `futures-core` and `pin-project-lite` — the very async baggage this crate was chosen *over* `sse-stream` to avoid. What is left without them is `bytes`, `memchr` and `thiserror`, all of which are already in the workspace's tree. Nothing the design uses lives behind a default feature: [`SseDecoder`] is in the crate's unconditional core, and only `SseStream` is gated. So the smaller feature set is also the complete one, and the choice is stated here rather than discovered from a `cargo tree` three phases later.

**There is no `getrandom` line.** It was here for the nonce, and the nonce is gone (design decision 17, amended); the core needs no entropy source without it.

Add the README row after `tinio-server`'s:

```markdown
| `tinio-sandbox*` | Process confinement: the `Backend` contract, per-platform implementations, and a facade |
```

- [x] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-core`
Expected: PASS — 8 unit tests (five in `sandbox.rs`, three in `error.rs`), 4 doc tests (the crate-level `//!` example, `Exposure`, `Sandbox`, `Sandbox::from_exposures`). The fifth `sandbox.rs` case is the one the cleanup added: `from_exposures_keeps_the_most_permissive_access_too`, which pins the rule on the path a launcher platform takes, both orders of the duplicate pair — without it, a `from_exposures` that stored the vector as given passed every other test. `AccessMode` contributes neither count: it is `tinio-core`'s type, so its unit tests and its doc example run in that crate's own suite.

**Count them from the code blocks rather than from this line if they disagree**: a doc example on a `pub(crate)` item is not one of them (rustdoc would fail to compile it), and the crate-level `//!` example is.

- [x] **Step 8: Verify the workspace still lints**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean.

---

### Task 2: The launcher machinery

**Files:**
- Create: `crates/tinio-sandbox-core/src/{backend.rs,event.rs,launcher/mod.rs,launcher/payload.rs,launcher/relay.rs,launcher/supervise.rs}`
- Modify: `crates/tinio-sandbox-core/src/lib.rs`
- Create (later, in the cleanup that moved the test fixtures): `crates/tinio-sandbox-core/src/testing.rs`

**Interfaces:**
- Consumes: `Error`, `Result`, `Sandbox`.
- Produces: `Event` (`Listening`, `Error`) with `encode` / `decode` (its two-field decoder is `pub(crate)`); `Observer` with `Terminal { Listening, Error }`, `record` (pure) and `observe` (async) — **the relay's public half, because it is the one that answers "did the child become ready?"**; `Confinement { InPlace, Supervise(Child) }`; `Child` (`new`, `with_relay`, `wait`, `terminal`); `supervise(&mut Child, teardown) -> Result<ExitStatus>` — **by reference, because the caller reads `Child::terminal` after it returns**; `LaunchPlan` (`new`, `with_payload`, `payload`, `with_listener`, `listener`, `config`, `for_launch`) — **the listener's one home, and it owns it**, because the parent must keep the socket alive for the child's whole lifetime and a bare descriptor could outlive the thing it names; `LaunchPayload { config }` — one JSON object carrying the configuration text, serde on both sides and no wrapper methods; `Adopted` (`config`, `listener`) and `adopt(args, stdin)`; and the argv-element contract `LISTENER_FD_PREFIX` / `listener_fd_arg` / `is_listener_fd_arg` / `listener_fd_from_arg`.

**There is no stderr relay here, and that is a deliberate absence rather than an omission.** Both launcher platforms leave the child's stderr **inherited** (`Stdio::inherit()` / `h_std_error` set to the launcher's own), so there is no pipe to fill and no writer to block — the deadlock a pump would exist to prevent cannot arise. An earlier draft of this task carried a `relay_stderr` for that reason; it was removed once the platform plans were read closely enough to see that neither of them pipes stderr. A function with no caller is a promise the crate does not keep.

Three things are defined **here and only here**, for the same reason each time — both launcher platforms need them, the two never interoperate, and a per-platform copy would work perfectly while drifting unnoticed:

- **The payload frame**, which travels on the child's **stdin** (design decision 17). **It is still a pipe the parent creates** — a payload that flows parent → child on stdin needs one, and both platform plans set `Stdio::piped()` / `h_std_input` for it; what the stdin form removes is the payload's *descriptor argument* and the child's rebuild-from-raw-descriptor, not the pipe. The other two streams are inherited as they always were.
- **The argv element**, which now carries the inherited **listener** descriptor — a socket cannot ride on a stream, so this half of decision 16 is irreducible whatever the payload does.
- **The event stream** (design decision 19): an `event:` line and a `data:` line closed by a blank line, handed over as one `MessageEvent { event, data }` by `sse-core`. The blank line is what makes a message containing a newline unable to split one event into two. Together with `Observer`, this is what lets both launcher platforms present the same stdout to a consumer that Linux presents without any relay at all.

The platform plans consume all of it rather than defining their own.

This is the half of the launcher contract that macOS and Windows share: the stdin payload that identifies a confined child (design decision 17) and the supervision loop. Building it here — rather than inside either platform crate — is what makes the trait's two shapes concrete in P0 instead of guessed at. Neither platform crate's body exists yet, so everything in this task is pure or exercised against a plain child process.

The child discriminator is the payload itself, never an environment variable: an environment marker would be forgeable, and setting it would make a process believe it was already confined and skip the sandbox entirely.

**And this machinery logs what it decides**, because a launch that goes wrong has to be diagnosable from a log rather than only from a reproduction. shared §7 owns the rule, the levels and the sink; this task owns the core's share of it: `adopt`'s outcome **and why** (whether an element was present, and whether a payload decoded — never the payload's contents, which carry credentials), the relay's first terminal event, what `Child::wait` returned, and that the teardown ran. The crate installs no subscriber of its own; `serve`'s existing stderr subscriber is the sink, which is what keeps stdout free for the event protocol. One test runs an operation under a recording subscriber and asserts the event's `step` field, so "it logs" is a property a test can lose.

- [x] **Step 1: Write the failing tests**

`crates/tinio-sandbox-core/src/launcher/payload.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> LaunchPayload {
        LaunchPayload { config: "version = 1\n[server]\nport = 9000\n".into() }
    }

    #[test]
    fn a_payload_survives_a_round_trip() {
        let payload = sample();
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(serde_json::from_str::<LaunchPayload>(&line).unwrap(), payload);
    }

    #[test]
    fn an_empty_config_is_legal() {
        let payload = LaunchPayload { config: String::new() };
        let line = serde_json::to_string(&payload).unwrap();
        assert_eq!(serde_json::from_str::<LaunchPayload>(&line).unwrap(), payload);
    }

    #[test]
    fn the_wire_shape_is_one_json_object() {
        // Pinned as text, not only as a round trip: a symmetric mistake would
        // round-trip against itself while reading nothing else.
        let payload = LaunchPayload { config: "port = 9000".into() };
        assert_eq!(serde_json::to_string(&payload).unwrap(), "{\"config\":\"port = 9000\"}");
    }

    #[test]
    fn a_config_with_newlines_stays_one_object() {
        // The configuration is a TOML document, so it is full of newlines —
        // and `serde_json` escapes them, which is what keeps the payload one
        // object rather than several lines.
        let payload = sample();
        let line = serde_json::to_string(&payload).unwrap();
        assert!(!line.contains('\n'), "{line}");
        assert_eq!(serde_json::from_str::<LaunchPayload>(&line).unwrap(), payload);
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
            assert!(serde_json::from_str::<LaunchPayload>(text).is_err(), "{text:?}");
        }
    }
}
```

`crates/tinio-sandbox-core/src/launcher/supervise.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::io;
    use std::process::{Command, ExitStatus};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::testing::loopback;

    /// A child that exits with `code`, portable across the CI matrix.
    fn exiting(script: &str) -> Command {
        #[cfg(windows)]
        {
            let mut command = Command::new("cmd");
            command.args(["/c", script]);
            command
        }
        #[cfg(not(windows))]
        {
            let mut command = Command::new("sh");
            command.args(["-c", script]);
            command
        }
    }

    fn child_for(script: &str) -> Child {
        let mut process = exiting(script).spawn().expect("spawn");
        Child::new(move || Ok(process.wait()?))
    }

    /// The terminal event a relayed child would have reported.
    fn listening() -> Terminal {
        Terminal::Listening(loopback(9000))
    }

    #[test]
    fn supervise_returns_the_child_status() {
        let mut teardown_ran = false;
        let status = supervise(&mut child_for("exit 3"), || teardown_ran = true).unwrap();
        assert_eq!(status.code(), Some(3));
        assert!(teardown_ran, "teardown must run after the child exits");
    }

    #[test]
    fn teardown_runs_even_when_the_child_fails() {
        // A failed child is exactly when the ACLs, the profile and the jail
        // directory most need undoing.
        let mut runs = 0;
        let _ = supervise(&mut child_for("exit 1"), || runs += 1);
        assert_eq!(runs, 1);
    }

    #[test]
    fn a_wait_failure_still_runs_teardown() {
        let mut runs = 0;
        let result = supervise(
            &mut Child::new(|| Err(Error::Wait(io::Error::other("boom")))),
            || runs += 1,
        );
        assert!(result.is_err());
        assert_eq!(runs, 1, "teardown is not conditional on a clean wait");
    }

    #[test]
    fn the_status_is_the_childs_not_the_teardowns() {
        let status: ExitStatus = supervise(&mut child_for("exit 0"), || ()).unwrap();
        assert!(status.success());
    }

    #[test]
    fn wait_joins_the_relay_so_the_terminal_is_final() {
        // Reading the answer after the wait is only worth something if the relay
        // has finished, and the two events have no ordering of their own: the
        // child is reaped by the wait, while the relay notices EOF on its stdout
        // whenever the scheduler gets to it. The relay here sleeps well past the
        // child's exit, so **without the join the read below happens first** —
        // and a perfectly healthy child is reported as never having become
        // ready, which is the failure the join exists to prevent.
        let relay = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            Some(listening())
        });
        let mut child = child_for("exit 0").with_relay(relay);
        let status = supervise(&mut child, || ()).unwrap();
        assert!(status.success());
        assert_eq!(
            child.terminal(),
            Some(&listening()),
            "the relay's answer must be final once the wait has returned"
        );
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-core launcher::`
Expected: FAIL — `cannot find struct 'LaunchPayload'`.

- [x] **Step 3: Write `launcher/payload.rs`**

```rust
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
/// let payload = LaunchPayload { config: "port = 9000".into() };
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
/// assert_eq!(listener_fd_from_arg(OsStr::new("tinio-sandbox-fd=9")), Some(9));
/// assert_eq!(listener_fd_from_arg(OsStr::new("tinio-sandbox-fd=x")), None);
/// ```
pub fn listener_fd_from_arg(arg: &OsStr) -> Option<i64> {
    arg.to_str()?.strip_prefix(LISTENER_FD_PREFIX)?.parse().ok()
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

Add to Step 1's test module, so the contract is pinned rather than merely implemented:

```rust
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
        for arg in ["--port", "serve", "tinio-sandbox-fd", "tinio-sandbox-fd=", "tinio-sandbox-fd=x"] {
            assert!(!is_listener_fd_arg(OsStr::new(arg)), "{arg}");
        }
    }
```

with `use std::ffi::{OsStr, OsString};` added to the test module's imports.

- [x] **Step 4: Write `launcher/supervise.rs`**

```rust
//! Waiting for a confined child and undoing what its launch set up.

use std::process::ExitStatus;
use std::thread::JoinHandle;

use tracing::debug;

use crate::error::Result;
use crate::launcher::Terminal;

/// A confined child the platform started. The core only ever waits for it, so
/// the platform keeps its own handle — a `std::process::Child` for
/// `sandbox-exec`, a Win32 process handle for an AppContainer — behind the
/// closure.
///
/// It also carries **the terminal event the relay saw**, because the two facts
/// are needed together and only the platform can join them. The relay runs on
/// its own thread and owns the `Observer`; [`Self::with_relay`] hands that thread
/// over, [`Self::wait`] joins it, and **the thread's return value is the
/// channel** — the answer comes back through the join rather than through a
/// shared cell. Without both halves the "parent emits `Event::Error` when the
/// child never became ready" rule (design decision 19) would have nothing to
/// consult, and the parent would be guessing.
///
/// There is no slot to create first, and that is the simplification the join
/// bought: with the answer travelling through `JoinHandle`, nothing has to exist
/// before the relay thread starts, there is no lock to poison, no value to clone
/// on every read, and no ordering rule for a launcher to remember.
///
/// **`Child` is not `Send`, and that is a property rather than an oversight.**
/// The wait closure is a bare `Box<dyn FnMut() -> Result<ExitStatus>>`, and the
/// Windows launcher's closure carries a raw process handle besides; nothing here
/// crosses a thread boundary, because every call site confines *before* the
/// runtime exists (design §10) on the thread that already owns the child. A
/// future caller that needs to move one would add `+ Send` to that closure —
/// and the Windows crate would then have to decide what to do about its handle,
/// which is a decision worth making when something needs it rather than now.
///
/// # Examples
///
/// ```
/// use std::process::Command;
/// use std::thread;
///
/// use tinio_sandbox_core::Child;
///
/// # #[cfg(unix)] {
/// let mut process = Command::new("true").spawn().unwrap();
/// // A real relay returns what the child reported; this one reports nothing.
/// let relay = thread::spawn(|| None);
/// let mut child = Child::new(move || Ok(process.wait()?)).with_relay(relay);
/// assert!(child.terminal().is_none(), "nothing has relayed yet");
/// let _ = child.wait();
/// # }
/// ```
#[derive(derive_more::Debug)]
pub struct Child {
    /// `#[debug(skip)]` because the closure has no `Debug` impl and could not
    /// be printed if it did. `derive_more::Debug` rather than a hand-written
    /// `impl` so the skip is declared in the type rather than buried in a
    /// formatter — and because `Confinement` derives `Debug`, which needs this
    /// to exist: every `unwrap_err()` in the platform plans depends on it.
    #[debug(skip)]
    wait: Box<dyn FnMut() -> Result<ExitStatus>>,
    /// The launcher's relay thread — **and the channel its answer travels back
    /// through**: the thread returns what it recorded, and [`Self::wait`] takes
    /// that out of the join. `std::thread` rather than a runtime handle because
    /// the relay's own thread is a `std::thread` on both launcher platforms —
    /// the runtime lives *inside* it, not around it.
    relay: Option<JoinHandle<Option<Terminal>>>,
    /// Filled in by [`Self::wait`] from the join. `None` until then, which is
    /// also what a caller reads for a child that never reported.
    terminal: Option<Terminal>,
}

impl Child {
    /// Wrap a platform wait function.
    ///
    /// The relay goes in separately, with [`Self::with_relay`], because the two
    /// are not always both there: the core's own tests run a child with no relay
    /// at all, and a launcher never has one without a child.
    pub fn new(wait: impl FnMut() -> Result<ExitStatus> + 'static) -> Self {
        Self { wait: Box::new(wait), relay: None, terminal: None }
    }

    /// Hand over the relay thread, so [`Self::wait`] can join it — and keep the
    /// terminal event it returns.
    pub fn with_relay(mut self, relay: JoinHandle<Option<Terminal>>) -> Self {
        self.relay = Some(relay);
        self
    }

    /// Block until the child exits, then until its relay has finished.
    ///
    /// **The join is what makes [`Self::terminal`] final.** The child's exit is
    /// what closes its stdout, and the relay ends when it reads EOF — but the
    /// process being reaped and the other thread noticing are two different
    /// events, with no ordering between them. A caller that read the answer as
    /// soon as the wait returned would be reading a snapshot, and on a healthy
    /// child it would usually read `None`: the answer to "did this child ever
    /// become ready?" would be wrong on the runs that went *right*. So the wait
    /// does not return until the relay has.
    ///
    /// That makes it the wait's business that the child's stdout really does
    /// reach EOF: a descendant left holding the pipe would keep the relay — and
    /// so this call — waiting. `serve` spawns no children (design's evidence
    /// table), and a launcher that can leave one behind must deal with it before
    /// its wait closure returns.
    ///
    /// What the wait returned is traced (`step = "wait"`) before the join, so a
    /// launch that hangs is diagnosable from the log of what it had already
    /// reached rather than only from a reproduction.
    ///
    /// # Errors
    ///
    /// Whatever the platform's wait reports. A relay that panicked is not one of
    /// them: the panic is in this crate's own code, and [`Self::terminal`]
    /// reports a dead relay as "nothing was recorded", which is the same answer
    /// a caller gets for a child that never reported.
    pub fn wait(&mut self) -> Result<ExitStatus> {
        let status = (self.wait)();
        // The child's own answer, which is what a stuck launcher needs to see:
        // an exit code that arrived, or the platform error that stopped one.
        match &status {
            Ok(status) => debug!(
                step = "wait",
                code = status.code(),
                success = status.success()
            ),
            Err(error) => debug!(step = "wait", error = %error),
        }
        if let Some(relay) = self.relay.take() {
            // A relay that panicked recorded nothing, which is the same answer a
            // child that never reported gives — and the same one the earlier
            // shared-slot design produced for a poisoned lock. There is no lock
            // here to poison.
            self.terminal = relay.join().ok().flatten();
        }
        status
    }

    /// The terminal event the relay saw, if the child sent one.
    ///
    /// **Read it after [`Self::wait`] returns** — the wait joins the relay, so
    /// by then the answer is final rather than a snapshot. The caller's decision
    /// ("did this child ever become ready?") is only meaningful once the child
    /// is gone.
    ///
    /// `None` means neither `Listening` nor `Error` arrived, which a launcher
    /// turns into its own `Event::Error` carrying the exit status. A relay that
    /// panicked is reported the same way — a bug in our own code rather than a
    /// condition to propagate, and a child whose relay died has certainly not
    /// reported readiness.
    pub fn terminal(&self) -> Option<&Terminal> {
        self.terminal.as_ref()
    }
}

/// Wait for the confined child, then undo the launch.
///
/// Teardown runs on every path — a clean exit, a failed child and a failed
/// wait alike — because a failed launch is exactly when an ACL grant or a
/// temporary directory most needs reverting (design decision 15). The status
/// returned is always the **child's**; teardown cannot mask it.
///
/// **It borrows the child rather than consuming it, and that is the whole reason
/// the signature is `&mut Child`.** The caller's next move is to ask
/// [`Child::terminal`] whether the child ever became ready (design decision 19),
/// and a by-value `supervise` takes that away: the only place left to read the
/// read would be *before* the wait, while the relay is still running, which is
/// the snapshot this API exists to avoid. The wait joins the relay, so by the
/// time this returns the answer is final.
///
/// **Teardown cannot report a failure, and that is deliberate rather than a
/// gap.** A rollback that can fail belongs in the platform's wait closure, which
/// owns the platform's error type and can name the directory it could not fix
/// (design §11's Windows row); a `Drop` guard covers the `Child` that is never
/// waited on at all. What is left for this callback is the work that cannot fail
/// — disarming a signal handler, removing a path the launcher created — which is
/// what both launcher platforms pass it.
///
/// That the teardown ran is traced (`step = "teardown"`), because a rollback
/// that silently did not happen is indistinguishable from one that did.
///
/// # Errors
///
/// The child's wait failure, if it failed. A teardown that panics is not
/// caught: it is a bug in our own code, not a runtime condition.
///
/// # Examples
///
/// ```
/// use std::process::Command;
///
/// use tinio_sandbox_core::{Child, supervise};
///
/// # #[cfg(unix)] {
/// let mut process = Command::new("true").spawn().unwrap();
/// let mut child = Child::new(move || Ok(process.wait()?));
/// let mut undone = false;
/// let status = supervise(&mut child, || undone = true).unwrap();
/// assert!(status.success());
/// assert!(undone);
/// // The child is still ours afterwards — which is what the terminal read
/// // needs, and what a by-value `supervise` would have taken away.
/// assert!(child.terminal().is_none(), "no relay ran in this example");
/// # }
/// ```
pub fn supervise(child: &mut Child, teardown: impl FnOnce()) -> Result<ExitStatus> {
    let status = child.wait();
    teardown();
    debug!(step = "teardown");
    status
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

- [x] **Step 5: Write `event.rs` and `launcher/relay.rs`**

The event contract (design decision 19) and the relay that makes it trustworthy on the launcher platforms. Both are core, both are exercised on Linux even though Linux never relays: the relay is tested against a plain child process, which is the only way the launcher platforms' half gets measured before those platforms exist.

```rust
//! What the confined process reports on stdout.

use std::{borrow::Cow, net::SocketAddr, num::NonZeroUsize};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sse_core::{SseDecoder, SseEvent};

/// The SSE field prefixes. A frame is these two lines and a blank line.
const DATA: &str = "data: ";
const EVENT: &str = "event: ";

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
pub(crate) const MAX_FIELD: NonZeroUsize = NonZeroUsize::new(64 * 1024).unwrap();

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
/// assert_eq!(frame, "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n");
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
    /// assert_eq!(Event::decode(frame), Some(Event::Error { message: "no profile".into() }));
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
```

There is no hand-written reader here — the parsing half is `sse-core`'s, and the writing half is `serde`'s. What remains ours is the two framing lines and the meaning of the event names.

```rust
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
/// let observer = Observer::new(Vec::new()).observe(input.as_bytes()).await.unwrap();
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
        Self { terminal: None, inner }
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
        Some(Ok(SseEvent::Message(message))) => Step::Frame(message.event.into_owned(), message.data),
        None => Step::Incomplete,
    }
}
```

**`MAX_FIELD` is declared once, in `event.rs`, and that is the fix rather than a matter of taste.** It is the protocol's own bound — "how large may one frame be" — and the two places a decoder is built are `Event::decode` and `Observer::observe`; with a copy at either site the crate answered the question twice, and `decode` was building the dependency's 512 KiB default while this module pinned 64 KiB. So the constant lives beside the framing it bounds, the relay imports it (`use crate::{error::{Error, Result}, event::{Event, MAX_FIELD}}`), and both decoders take it.

**`MAX_FIELD` is a `NonZeroUsize`, and the `unwrap` is a compile-time one.** The dependency decides the type rather than a preference: `sse_core 0.2.3`'s `SseDecoder::with_limit` takes a `NonZeroUsize` (`src/decode.rs:200`), so an earlier draft's `const MAX_FIELD: usize = 64 * 1024;` does not compile — the limit is a `NonZeroUsize` at the call site or the frame limit cannot be stated at all. `NonZeroUsize::new(64 * 1024).unwrap()` is evaluated in the const initializer, so a zero would fail the build rather than panic a code path, and it is the same idiom `sse-core` uses for its own 512 KiB default (`SseDecoder::new`, `src/decode.rs:167`). The alternative — a `usize` constant plus a runtime `NonZeroUsize::new(...).unwrap()` where the decoder is built — puts a genuine unwrap on a library path, which is strictly worse.

These tests split across **two** test modules, and neither is `payload.rs`'s — "Step 1's test module" would send them there, where none of the names they use (`Event`, `Observer`, `reemit`, `Terminal`) exist:

- **`event.rs`** takes the pure frame cases: the round trip, the pinned wire shape, the newline case, quotes and backslashes, and `anything_that_is_not_a_frame_is_ignored`. Its header is `#[cfg(test)] mod tests { use super::*; use crate::testing::loopback; … }`.
- **`launcher/relay.rs`** takes the ones that run through the relay: the split frame, the incomplete trailing frame, several frames in one chunk, `retry:`, the three `Observer` cases, and `the_relay_drops_what_does_not_decode` — with `use super::*; use crate::{event::Event, testing::{blocking, loopback}};` and a three-byte `TRICKLE` beside them, the chunk the split-frame test passes to `blocking`.

`loopback` and the blocking-reader adapter the relay's frame test and the supervision test both need are **one `#[cfg(test)] pub(crate) mod testing`** — `crates/tinio-sandbox-core/src/testing.rs`, declared from `lib.rs` with the line `#[cfg(test)] pub(crate) mod testing;`, beside the crate's other module declarations — and holding the two helpers below: three test modules want the address and two want the adapter, and a copy per module was the price an earlier draft preferred to a crate-visible item — "a `#[cfg(test)]` helper in one module is invisible to another module's tests unless it is made a crate item, which is a price a test helper should not charge". The ruling went the other way, because the price is smaller than the sentence suggests: the module is `#[cfg(test)]`, so it exists in no build that ships, and its items are `pub(crate)`, so no consumer can reach them. One adapter parameterised by chunk size replaces the two copies (three bytes, forcing a frame to be assembled from many reads; 1024, reading a pipe the way a runtime would).

`crates/tinio-sandbox-core/src/testing.rs`:

```rust
//! Helpers shared by this crate's test modules: `loopback` (the address every
//! test's frames name) and `blocking` (a blocking reader presented as the async
//! reader the relay asks for). Crate items rather than a copy per module,
//! because three test modules want the address and two want the adapter.

use std::{
    io::{self, Read},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf};

/// `127.0.0.1:port`, the loopback address the tests' frames name.
pub(crate) fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// A blocking `std::io::Read` presented as the `AsyncBufRead` the relay takes,
/// handing over at most `chunk` bytes per read.
///
/// **Both readers a test drives are blocking ones** — a slice of frame bytes and
/// a real child's pipe — while `Observer::observe` takes an `AsyncBufRead`.
/// `tokio::fs::File::from_std` is how the launcher platforms bridge the two, and
/// it is tokio's `fs` feature, which this crate's dev-dependencies deliberately
/// stop short of; so the blocking read happens where the async one goes.
///
/// **Which is safe only because of where it runs**: a `poll_read` that blocks
/// its executor is a bug anywhere else, and every caller here drives it from a
/// thread that does nothing but wait — the relay thread a launcher spawns, whose
/// shape the supervision test reproduces.
///
/// `chunk` is the caller's because the two uses want opposite things: the
/// relay's frame tests force every frame to be assembled from many reads, while
/// the pipe wants whole chunks.
pub(crate) fn blocking(reader: impl Read + Unpin, chunk: usize) -> impl AsyncBufRead + Unpin {
    struct Blocking<R> {
        reader: R,
        chunk: Vec<u8>,
    }

    impl<R: Read + Unpin> AsyncRead for Blocking<R> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.as_mut().get_mut();
            let take = buf.remaining().min(this.chunk.len());
            let read = this.reader.read(&mut this.chunk[..take])?;
            buf.put_slice(&this.chunk[..read]);
            Poll::Ready(Ok(()))
        }
    }

    BufReader::new(Blocking {
        reader,
        chunk: vec![0; chunk],
    })
}
```

```rust
    // `loopback` is `crate::testing::loopback` for every module that uses it —
    // see the module above — and no test module defines a copy any more.

    #[test]
    fn an_event_survives_a_round_trip() {
        for event in [
            Event::Listening { addr: loopback(9000) },
            Event::Error { message: "boom".into() },
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
        let frame = Event::Listening { addr: loopback(9000) }.encode();
        assert_eq!(frame, "event: listening\ndata: {\"addr\":\"127.0.0.1:9000\"}\n\n");
    }

    #[test]
    fn a_message_with_a_newline_stays_one_event() {
        // The reason the framing is SSE and not bare JSONL.
        let event = Event::Error { message: "line one\nline two".into() };
        let frame = event.encode();
        assert_eq!(frame.matches("\n\n").count(), 1, "one frame, one blank-line terminator");
        assert_eq!(Event::decode(&frame), Some(event));
        // And the newline survived as data rather than splitting the payload:
        // the way through a `data:` value is an escape, not a second line.
        assert_eq!(frame.matches("data: ").count(), 1, "{frame}");
    }

    #[test]
    fn quotes_and_backslashes_survive() {
        let event = Event::Error { message: r#"a "quoted" \backslash\ and a \n literal"#.into() };
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
        let frames = Event::Listening { addr: loopback(9000) }.encode().repeat(2);
        let mut output = Vec::new();
        reemit(blocking(frames.as_bytes(), TRICKLE), &mut output).await.unwrap();
        assert_eq!(String::from_utf8(output).unwrap(), frames);
    }

    #[tokio::test]
    async fn an_incomplete_trailing_frame_is_discarded() {
        // A child killed mid-write leaves half a frame. The standard discards
        // it, and so does this: the sender is gone, so there is nothing to wait
        // for.
        let mut half = Event::Listening { addr: loopback(9000) }.encode().into_bytes();
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
        let input = Event::Error { message: "one".into() }.encode()
            + &Event::Error { message: "two".into() }.encode();
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
        reemit("retry: 1000\n\n".as_bytes(), &mut output).await.unwrap();
        assert!(output.is_empty(), "{output:?}");
    }

    #[tokio::test]
    async fn the_observer_records_the_listening_address() {
        let addr = loopback(9000);
        let input = format!("event: listening\ndata: {{\"addr\":\"{addr}\"}}\n\n");
        let observer = Observer::new(Vec::new()).observe(input.as_bytes()).await.unwrap();
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
        assert!(observer.into_inner().is_empty(), "noise must not be forwarded");
    }

    #[tokio::test]
    async fn the_observer_remembers_a_child_reported_error() {
        let input = "event: error\ndata: {\"message\":\"no such profile\"}\n\n";
        let observer = Observer::new(Vec::new()).observe(input.as_bytes()).await.unwrap();
        assert_eq!(observer.terminal(), Some(&Terminal::Error("no such profile".into())));
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
        let observer = Observer::new(Vec::new()).observe(input.as_bytes()).await.unwrap();
        assert_eq!(observer.terminal(), Some(&Terminal::Listening(loopback(9000))));
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
```

- [x] **Step 6: Write the reconciled launcher surface — `adopt`**

**Where this lives.** The code block below is additional content for `crates/tinio-sandbox-core/src/launcher/mod.rs`, beside `LaunchPlan` — not a new module and not `lib.rs`. That is what makes Step 7's `pub use self::launcher::{…, Adopted, adopt, …}` resolve: `lib.rs` re-exports them from `launcher`, so `launcher` must own them.

**This step exists because the two launcher plans each needed core items P1 did not have, and each invented its own.** The Windows and macOS plans were written in parallel against P1's Linux-only surface; both extended it, differently, and the drift was found by a cross-plan check rather than by either plan failing. The rule this step encodes: **the launcher-shared surface is defined here, once, and the platform plans consume it.** If a later phase needs something else shared, it comes back here rather than growing a second copy.

**Child-side adoption.** The two plans wrote `adopt` with different signatures *and* different return types: Windows took `&[OsString]` and rebuilt the socket, macOS took nothing and returned the descriptor. The parse and the stdin read are shared and hold no platform types; only the rebuild does. So the split is:

```rust
/// What a confined child recovers from its own launch (design decision 17).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Adopted;
///
/// let adopted = Adopted { config: "port = 9000".into(), listener_fd: Some(7) };
/// assert_eq!(adopted.listener_fd, Some(7));
/// ```
#[derive(Clone, PartialEq, Eq, derive_more::Debug)]
pub struct Adopted {
    /// The configuration text the parent forwarded, unparsed.
    ///
    /// `#[debug(skip)]` for the same reason [`LaunchPayload::config`] carries
    /// it: this text can hold credentials, and a `Debug` that prints them leaks
    /// them into the first log line, `dbg!` or `unwrap_err()` that happens to
    /// touch a value containing this one. A caller that needs to see the
    /// configuration prints it deliberately.
    #[debug(skip)]
    pub config: String,
    /// The inherited listener's descriptor, which only the platform can turn
    /// back into a socket.
    pub listener_fd: Option<i64>,
}

/// Recover a launch, or decide this process is the launcher.
///
/// **Reads `stdin` only when `args` carries the listener element.** A process
/// started from a shell owns its stdin — a terminal, a heredoc, another tool's
/// pipe — so a blind read would block a launcher that nothing launched, waiting
/// for input that is never coming, and present as a hang rather than a refusal.
///
/// Returns `Ok(None)` for "this is the launcher": no element, or an element
/// whose payload does not decode. Both are ordinary outcomes, and both fail
/// closed if the process turns out to be inside a confinement after all.
///
/// Arguments are taken rather than read from the environment so the whole rule
/// is unit-testable without a subprocess.
///
/// Which of those outcomes the call reached is traced (`step = "adopt"`): that
/// an element was present and whether a payload decoded, never the payload's
/// contents, which carry credentials.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
/// use std::io;
///
/// use tinio_sandbox_core::adopt;
///
/// // No element: the launcher, and stdin is never touched.
/// let none = adopt(Vec::<OsString>::new(), io::empty()).unwrap();
/// assert!(none.is_none());
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    mut stdin: impl Read,
) -> Result<Option<Adopted>> {
    let Some(listener_fd) = args.into_iter().find_map(|arg| listener_fd_from_arg(&arg)) else {
        debug!(step = "adopt", listener = false, payload = "absent");
        return Ok(None);
    };
    let mut text = String::new();
    // The parent writes the payload and closes; a read to EOF is the whole
    // protocol, and a failure to read is a payload failure rather than an IO
    // error the caller could act on differently.
    if stdin.read_to_string(&mut text).is_err() {
        debug!(step = "adopt", listener = true, payload = "unreadable");
        return Ok(None);
    }
    // `serde_json::from_str` already rejects trailing bytes after the object,
    // which is the property that matters: a child must be certain the whole
    // stream was meant for it.
    match serde_json::from_str::<LaunchPayload>(&text) {
        Ok(payload) => {
            debug!(step = "adopt", listener = true, payload = "decoded");
            Ok(Some(Adopted {
                config: payload.config,
                listener_fd: Some(listener_fd),
            }))
        }
        Err(_) => {
            debug!(step = "adopt", listener = true, payload = "rejected");
            Ok(None)
        }
    }
}

impl Adopted {
    /// The configuration text the parent forwarded, unparsed (design decision
    /// 18).
    ///
    /// Empty when the parent sent none, which is the ordinary case for a launch
    /// that took its configuration from somewhere else.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted { config: "port = 9000".into(), listener_fd: None };
    /// assert_eq!(adopted.config(), "port = 9000");
    /// ```
    pub fn config(&self) -> &str {
        &self.config
    }

    /// The inherited listener's descriptor, when the parent handed one over.
    ///
    /// The descriptor is still the platform's to turn into a socket: only
    /// `from_raw_socket` or an `OwnedFd` conversion knows what it names, which
    /// is why the conversion stays in the platform crate.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::Adopted;
    ///
    /// let adopted = Adopted { config: String::new(), listener_fd: Some(7) };
    /// assert_eq!(adopted.listener(), Some(7));
    /// ```
    pub fn listener(&self) -> Option<i64> {
        self.listener_fd
    }
}
```

**What stays in the platform crates, and why.** The listener rebuild (`Adopted::listener_fd` → a `std::net::TcpListener`) is `from_raw_socket` on Windows and an `OwnedFd` conversion on macOS — platform syscalls, and the reason this split is where it is. So are the pipes, the handle plumbing, the `CreateProcess`/`sandbox-exec` call, and the signal relay. The **signal relay is deliberately not a core trait**: Windows drives it from a console-control event and macOS from `signal-hook`, they share no code, and the only thing that must agree is the name — each crate exports `on_stop` and `disarm`, and the facade re-exports the selected one.

**`adopt`'s tests, and the one the earlier draft was missing.** The rule "reads `stdin` only when `args` carries the listener element" is the task's only fail-safe against a launcher blocking forever on a real terminal, and **a reader that returns EOF cannot test it**: `io::empty()` and a `Cursor` over an empty buffer behave identically whether or not they are read. The test therefore needs a reader that is *hostile to being read at all*:

```rust
    /// A reader that fails the test if anything reads from it.
    struct Untouchable;

    impl io::Read for Untouchable {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("`adopt` read stdin without a listener element in argv");
        }
    }

    #[test]
    fn adopt_does_not_touch_stdin_without_the_listener_element() {
        assert!(adopt(Vec::<OsString>::new(), Untouchable).unwrap().is_none());
        assert!(adopt([OsString::from("--port")], Untouchable).unwrap().is_none());
    }

    #[test]
    fn adopt_returns_the_configuration_text() {
        // The payload travels through `adopt` here rather than only through the
        // doc example, which never reaches it.
        let adopted = adopt([listener_fd_arg(7)], io::Cursor::new(br#"{"config":"port = 9000"}"#.to_vec()))
            .unwrap()
            .expect("a listener element and a decodable payload");
        assert_eq!(adopted.config(), "port = 9000");
        assert_eq!(adopted.listener(), Some(7));
    }
```

Both live in `launcher/mod.rs`'s test module. The second is not decoration: without it, the only evidence that a payload survives `adopt` is a doc example that constructs `Adopted` by hand.

**Two relay cases the earlier draft left unpinned, and the reason each is worth a test rather than a reading.** `Step::Skipped` and `Step::Incomplete` are different states with the same visible effect on a quiet stream, so a test that only asserts "nothing was written" cannot tell them apart — and the difference is load-bearing in both directions:

- **A `retry:` frame must not strand what follows it.** Treated as "need more data", the frame stays in the buffer and is dropped when the stream ends; treated as skipped, the decoder has already advanced past it. One chunk carrying a `retry:` line *and* a real event separates the two.
- **A frame over `MAX_FIELD` must be skipped, and the stream must continue.** This is the only test that exercises `Some(Err(_)) => Step::Skipped` and the only one that would notice `MAX_FIELD`'s pin being wrong; the decoder discards the offending event, resynchronises at the next boundary, and the good frame behind it must still arrive.

Write both, asserting the **bytes** the relay emitted rather than a count. If the observed behaviour contradicts either description, report it rather than weakening the assertion — the description comes from reading `sse-core 0.2.3`, not from running it.

- [x] **Step 7: Write `backend.rs`**

```rust
//! The trait every platform implements, and what it answers.

use crate::error::Result;
use crate::launcher::{Child, LaunchPlan};
use crate::sandbox::Sandbox;

/// What the call site must do once [`Backend::confine`] returns.
///
/// This is what keeps `#[cfg]` out of `main()`: Linux confines the running
/// process, macOS and Windows start a child, and the caller reacts to the
/// answer instead of branching on the platform (design decision 2).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Confinement;
///
/// let answer = Confinement::InPlace;
/// assert!(matches!(answer, Confinement::InPlace));
/// ```
#[derive(Debug)]
pub enum Confinement {
    /// The calling process is confined; carry on. Linux always answers this.
    InPlace,
    /// A confined child is running. Wait for it, forward its exit status, and
    /// undo whatever the launch set up — [`crate::supervise()`] does all three.
    Supervise(Child),
}

/// A platform's confinement mechanism.
///
/// **`confine` takes `&mut self`**, and the receiver is the honest one in both
/// directions: on Linux the call changes the calling process irreversibly
/// (design decision 26), while a launcher backend uses the receiver to record
/// what it set up — the state it reports afterwards. A backend that had to reach
/// for interior mutability to do that was expressing the contract wrongly.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Backend, Confinement, LaunchPlan, Result, Sandbox};
///
/// struct Noop;
///
/// impl Backend for Noop {
///     fn confine(&mut self, _sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
///         Ok(Confinement::InPlace)
///     }
/// }
/// ```
pub trait Backend {
    /// Confine the process, or start a confined child for it.
    ///
    /// # Errors
    ///
    /// Any failure is fatal: the caller must exit rather than continue
    /// unconfined (design §11).
    fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement>;
}
```

`LaunchPlan` is declared in `launcher/mod.rs`. Both fields are on it from the start — the payload and the owned listener — because the launcher plans consume them together and a plan that carried only half would push the other half into whichever platform crate noticed first:

```rust
//! The launcher contract shared by the child-launching platforms.

pub mod payload;
pub mod relay;
pub mod supervise;

pub use self::{
    payload::{
        LISTENER_FD_PREFIX, LaunchPayload, is_listener_fd_arg, listener_fd_arg,
        listener_fd_from_arg,
    },
    relay::{Observer, Terminal},
    supervise::{Child, supervise},
};

// `Adopted`, `LaunchPlan` and `adopt` are declared below in this module, not in
// a submodule, so `lib.rs` reaches them as `launcher::{Adopted, LaunchPlan,
// adopt}` without any re-export here — a `pub` item of a `pub` module is
// already public at that path.
//
// **There must be no `pub use self::{…}` for them, and the earlier draft had
// one.** An item declared in a module and then re-imported into that same
// module under the same name is `E0255`, "the name `Adopted` is defined
// multiple times" — verified with a bare `rustc` probe. The line was copied
// from the three re-exports above, which are `pub use self::<module>::…` and
// are correct precisely because those names live in submodules.

use std::ffi::OsString;
use std::io::Read;
use std::net::TcpListener;

use tracing::debug;

use crate::error::Result;

/// Everything a launcher needs that is not an exposed path.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::LaunchPlan;
///
/// let plan = LaunchPlan::default();
/// assert!(plan.payload().is_none());
/// assert!(plan.listener().is_none());
/// ```
#[derive(Debug, Default)]
pub struct LaunchPlan {
    payload: Option<LaunchPayload>,
    listener: Option<TcpListener>,
}

impl LaunchPlan {
    /// An empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Carry a payload.
    pub fn with_payload(mut self, payload: LaunchPayload) -> Self {
        self.payload = Some(payload);
        self
    }

    /// The payload, when one was set.
    pub fn payload(&self) -> Option<&LaunchPayload> {
        self.payload.as_ref()
    }

    /// **Own** the bound listener the child will serve on (design decision 16).
    ///
    /// Owned rather than named by descriptor: the parent must keep the socket
    /// alive for the child's whole lifetime, and a plan that carried a bare
    /// number while the socket lived somewhere else would let the two drift
    /// apart with nothing to notice it. The platform derives the descriptor
    /// when it builds the child's argv.
    ///
    /// The listener lives **here and not in the payload** (design §1: the plan
    /// holds what the launcher needs that is not a path) — carrying it in both
    /// would be two sources of truth for one fact.
    pub fn with_listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The owned listener, when the parent bound one.
    pub fn listener(&self) -> Option<&TcpListener> {
        self.listener.as_ref()
    }

    /// The configuration text the launcher forwards **unparsed** (design
    /// decision 18). Empty when no payload was set.
    pub fn config(&self) -> &str {
        self.payload.as_ref().map_or("", |payload| payload.config.as_str())
    }

    /// Build the plan a parent uses before launching a child.
    ///
    /// No listener parameter: [`Self::with_listener`] carries it, so a caller
    /// cannot set the same fact in two places.
    ///
    /// Infallible because the payload it builds is one string. The earlier shape
    /// had to generate a nonce here and was `Result` for it; with the nonce gone
    /// (design decision 17) there is nothing left to fail.
    pub fn for_launch(config: String) -> Self {
        Self::default().with_payload(LaunchPayload { config })
    }
}

```

Add to `lib.rs` — the module declarations, then one re-export group per module. **This block is where the earlier draft was wrong twice over**: it listed `AccessMode` among the `launcher` names, which do not include it, and it omitted `Error`, `Result`, `Exposure` and `Sandbox` — all four of which the facade re-exports, so the facade could not have compiled against it.

**Restore the crate-level doc's intra-doc link while you are in the file.** Step 6 wrote `Backend` as plain code text because the trait did not exist yet and an unresolvable link fails the `cargo doc` leg under `-D warnings`; this task is the one that adds the trait, so change that occurrence back to `` [`Backend`] `` in the same edit. The crate must not end the phase with the link still suppressed.

**`pub use crate::_core::AccessMode;` is already in the file — Step 6 wrote it — and it must not be written a second time.** It is repeated in the block below only because this block is the authoritative statement of the whole surface; merging it into the file means leaving the existing line where it is. A second `pub use` of the same item is not a harmless duplicate: it is `E0252`, "the name `AccessMode` is defined multiple times", which the lint step then reports as an error under `-D warnings` as well. The `#[doc(hidden)] pub extern crate tinio_core as _core;` that makes the path resolve is Step 6's too, and is not repeated here either.

```rust
pub mod backend;
pub mod event;
pub mod launcher;

pub use self::{
    backend::{Backend, Confinement},
    error::{Error, Result},
    event::Event,
    launcher::{
        Adopted, Child, LISTENER_FD_PREFIX, LaunchPayload, LaunchPlan, Observer, Terminal, adopt,
        is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg, supervise,
    },
    sandbox::{Exposure, Sandbox},
};

// `tinio-core`'s type rather than this crate's, reached through its alias — see
// Step 6. This block is the authoritative list.
pub use crate::_core::AccessMode;
```

**The order of the two groups is rustfmt's, not a preference**: `rustfmt` with the workspace's `reorder_imports` puts the `self::{…}` group first and the aliased dependency re-export after it. Writing them the other way round is a `cargo +nightly fmt --all -- --check` failure, not a compile error.

**Read the groups, not just the names.** `AccessMode` comes from `tinio-core` — the one entry that is a re-export of another crate rather than a module of this one, written through that crate's alias (`#[doc(hidden)] pub extern crate tinio_core as _core;`, so `pub use crate::_core::AccessMode;`) because `docs/style.md` §Imports reaches every workspace dependency that way — the four vocabulary items from `error` and `sandbox`, and the launcher machinery from `launcher`. The facade re-exports this same list name for name, which is what makes `tinio_sandbox::` a complete alias of `tinio_sandbox_core::` rather than a subset that drifts, and `AccessMode` travels through it the same way: `tinio_sandbox::AccessMode` resolves even though the facade has no `tinio-core` dependency of its own, because the type is reachable through the core's public surface.

- [x] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-core`
Expected: PASS — the payload round trips as one JSON line, a truncated or foreign line is rejected, the six supervision cases hold on both unix and Windows (including the one whose relay sleeps past the child's exit, which fails unless the wait joins it, and `a_real_childs_frame_reaches_the_terminal_through_the_relay`, the composed seam test: a real child's frame relayed by `Observer` on a `std::thread` — the thread both launcher platforms spawn, whose runtime is that thread's own — through `Child::new(…).with_relay(…)`, `supervise` and out as `Child::terminal()`. It is written as a `#[tokio::test]` with `async fn`, as `docs/tests.md` requires, and the one `Runtime::block_on` that drives the relay stands inside the relay's own thread, which is the production shape rather than a way of writing the test; removing `Child::wait`'s join fails it, `None` against the exact `Terminal::Listening(addr)` the child printed), every event round trips including the newline case, a frame split across reads is assembled whole rather than in part, the relay drops what does not decode, `adopt` returns `None` for empty argv without touching stdin, and the observer records the first terminal event the child reported. Plus the four cases added after the first review: `adopt` leaves stdin untouched when argv carries no listener element — pinned by a reader that panics on any read, because an EOF-returning reader proves nothing — `adopt` returns the configuration text it was sent, a `retry:` frame does not strand the event behind it, and a frame larger than `MAX_FIELD` is skipped with the good frame behind it still arriving — plus one case added by the code-review fix batch: `the_field_limit_decodes_just_under_and_refuses_just_over` pins `Event::decode`'s own share of `MAX_FIELD`, one byte either side, because the public seam had been left on the dependency's 512 KiB default while only the relay pinned 64 KiB — plus one case from the tracing batch, `adopt_logs_its_outcome_and_why`: `adopt` is the operation of the traced four that needs no child process, so its outcome is the one a test can record without one. It installs a **process-wide** recorder (`tracing` caches each callsite's interest process-wide, so a thread-local one loses events a concurrent test's rebuild never saw) and asserts the `step` and `payload` fields of one event per call — `absent`, `decoded`, `rejected` — rather than any rendered text, which is what makes "it logs" a property this suite can lose.

- [x] **Step 9: Confirm the core carries no FFI**

Run: `rg -n 'unsafe|allow\(unsafe_code\)' crates/tinio-sandbox-core/src`
Expected: no hits. The core is `forbid` and stays that way.

---

### Task 3: The facade and the three platform crates

**Files:**
- Create: `crates/tinio-sandbox/Cargo.toml`
- Create: `crates/tinio-sandbox/src/lib.rs`
- Create: `crates/tinio-sandbox/tests/facade.rs`
- Create: `crates/tinio-sandbox-linux/{Cargo.toml,src/lib.rs}`
- Create: `crates/tinio-sandbox-macos/{Cargo.toml,src/lib.rs}`
- Create: `crates/tinio-sandbox-windows/{Cargo.toml,src/lib.rs}`
- Modify: `.github/workflows/ci.yml` (the lint job's comment: the macOS crate's crate-root `#![cfg]` leaves its body unlinted on both clippy legs, and that window closes with P3)
- Modify: `Cargo.toml` (workspace root)
- Modify: `README.md`

**Interfaces:**
- Consumes: `Backend`, `Confinement`, `LaunchPlan`, `Sandbox`, `Error` from the core.
- Produces: `tinio_sandbox::{AccessMode, Sandbox, Backend, Confinement, LaunchPlan, platform, Platform}` — the facade's surface, which every consumer names and nothing else does.

The macOS and Windows bodies are `Unsupported` stubs. They exist now because the trait's shape must be exercised from both shapes — an in-process backend and a launcher backend — before either platform phase is written against it; a trait with one implementor is a guess (design decision 3, §3).

- [x] **Step 1: Write the failing test**

`crates/tinio-sandbox/tests/facade.rs`:

```rust
//! The facade is the only crate a consumer names, so it is the only place
//! that has to prove the platform selection actually resolves.

use std::ffi::{OsStr, OsString};
use std::io;
use std::process::ExitStatus;
use std::thread::JoinHandle;

use tinio_sandbox::{AccessMode, Backend, LaunchPlan, Sandbox, platform};

#[test]
fn the_facade_resolves_exactly_one_backend() {
    let mut backend = platform();
    // Confining an empty sandbox is allowed to fail for any platform-specific
    // reason; what this asserts is that the call reaches a real backend rather
    // than failing to compile or panicking on a missing selection.
    let result = backend.confine(&Sandbox::new(), &LaunchPlan::new());
    match result {
        Ok(_) => {}
        Err(err) => assert!(!err.to_string().is_empty()),
    }
}

#[test]
fn the_vocabulary_is_re_exported() {
    let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
    assert_eq!(sandbox.exposures().len(), 1);
    assert_eq!(sandbox.exposures()[0].access, AccessMode::ReadOnly);
}

/// Every item the facade publishes, named through `tinio_sandbox::`.
///
/// **This test enforces one direction, and the direction it cannot enforce is
/// worth naming rather than implying.** Every binding below names a
/// `tinio_sandbox::` item together with the shape the core gives it, so the
/// facade has to supply that name with that shape: a name the facade claims
/// while the core does not publish it — or publishes with a different
/// signature, which the `fn`-pointer bindings catch — fails to compile here,
/// and so does a facade that stops re-exporting one of the names listed.
/// An earlier draft was wrong in both of those ways at once: it named `Error`,
/// `Result`, `Exposure` and `Sandbox` that the core did not export, while
/// `Event`, `adopt` and `Adopted` were missing from it. **`reemit` is not one of
/// these either way**: it is `pub(crate)` and `#[cfg(test)]`, so it is a name
/// neither list has ever had to carry.
///
/// **What it cannot see is a name that is not in this list.** A core item added
/// later and never re-exported leaves the facade — and this test — compiling,
/// so "the two lists are equal" is the rule those additions are made under
/// rather than a property this test proves. The paths are not mirrored either:
/// the facade publishes names flat, so `tinio_sandbox::launcher::Observer` does
/// not exist even though `tinio_sandbox_core::launcher::Observer` does.
///
/// Written as a binding per item rather than a `use` list so the failure names
/// the missing symbol.
#[test]
fn the_launcher_surface_is_re_exported() {
    // The imports this test needs are at the file top, not here: `docs/style.md`
    // says "Top `use`; never inline".

    // The vocabulary — Task 1's half of the surface.
    let _: fn() -> tinio_sandbox::Sandbox = tinio_sandbox::Sandbox::new;
    let _: fn(&tinio_sandbox::Sandbox) -> &[tinio_sandbox::Exposure] =
        tinio_sandbox::Sandbox::exposures;
    let _: fn(Vec<tinio_sandbox::Exposure>, bool) -> tinio_sandbox::Sandbox =
        tinio_sandbox::Sandbox::from_exposures;
    let _: fn(tinio_sandbox::AccessMode) -> bool = tinio_sandbox::AccessMode::is_read_only;
    let _: fn(&tinio_sandbox::Error) -> String = |err| err.to_string();
    let _: tinio_sandbox::Result<()> = Ok(());

    // The launcher machinery — Task 2's.
    let _: fn(
        &mut tinio_sandbox::Platform,
        &tinio_sandbox::Sandbox,
        &tinio_sandbox::LaunchPlan,
    ) -> tinio_sandbox::Result<tinio_sandbox::Confinement> =
        <tinio_sandbox::Platform as tinio_sandbox::Backend>::confine;
    let _: fn(String) -> tinio_sandbox::LaunchPlan = tinio_sandbox::LaunchPlan::for_launch;
    let _: fn(&tinio_sandbox::LaunchPlan) -> &str = tinio_sandbox::LaunchPlan::config;
    let _: fn(&str) -> Option<tinio_sandbox::Event> = tinio_sandbox::Event::decode;
    let _: fn(&mut tinio_sandbox::Observer<Vec<u8>>, &str, &str) -> Option<tinio_sandbox::Event> =
        tinio_sandbox::Observer::record;
    let _: fn(&tinio_sandbox::Observer<Vec<u8>>) -> Option<&tinio_sandbox::Terminal> =
        tinio_sandbox::Observer::terminal;
    let _: fn(Vec<OsString>, io::Empty) -> tinio_sandbox::Result<Option<tinio_sandbox::Adopted>> =
        tinio_sandbox::adopt;
    let _: fn(i64) -> OsString = tinio_sandbox::listener_fd_arg;
    let _: fn(&OsStr) -> bool = tinio_sandbox::is_listener_fd_arg;
    let _: fn(&OsStr) -> Option<i64> = tinio_sandbox::listener_fd_from_arg;
    let _: &str = tinio_sandbox::LISTENER_FD_PREFIX;
    let _: tinio_sandbox::Child = tinio_sandbox::Child::new(|| unreachable!("not waited"));
    let _: fn(
        tinio_sandbox::Child,
        JoinHandle<Option<tinio_sandbox::Terminal>>,
    ) -> tinio_sandbox::Child = tinio_sandbox::Child::with_relay;
    let _: fn(&tinio_sandbox::Child) -> Option<&tinio_sandbox::Terminal> =
        tinio_sandbox::Child::terminal;
    // `&mut Child` rather than `Child`: the caller reads the terminal after this
    // returns, and a by-value signature would take the child away (decision 19).
    let _: fn(&mut tinio_sandbox::Child, fn()) -> tinio_sandbox::Result<ExitStatus> =
        tinio_sandbox::supervise;
    let _: fn(&mut tinio_sandbox::LaunchPayload) -> &mut String =
        |payload| &mut payload.config;
}

#[cfg(target_os = "linux")]
#[test]
fn linux_selects_the_in_process_backend() {
    // An empty sandbox has nothing to confine to, so Linux refuses — which is
    // itself the proof that the Linux backend is the one wired in.
    let mut backend = platform();
    let err = backend
        .confine(&Sandbox::new(), &LaunchPlan::new())
        .expect_err("an empty sandbox must be refused");
    assert!(err.to_string().contains("nothing exposed"), "{err}");
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
#[test]
fn the_launcher_platforms_answer_unimplemented_for_now() {
    let mut backend = platform();
    let err = backend
        .confine(&Sandbox::new(), &LaunchPlan::new())
        .expect_err("the stub backends are not implemented yet");
    assert!(err.to_string().contains("not implemented"), "{err}");
}
```

**This test is the stub's, and the phase that implements a backend is the one that changes it.** It asserts what the stub answers today, so replacing a stub with a real backend makes the assertion false on that platform — `2026-09-18-sandbox-p2-windows.md` narrows this test's `#[cfg]` to macOS as part of its Task 1 (the crate stops being a stub there), and P3 removes the test when its own backend lands. What P0 pins is that the *facade* reaches the platform's `Backend`; that any backend refuses forever was never the claim.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox`
Expected: FAIL — the package does not exist.

- [x] **Step 3: Write the facade**

`crates/tinio-sandbox/Cargo.toml`:

```toml
[package]
name = "tinio-sandbox"
version.workspace = true
edition.workspace = true
description = "Process confinement for tinio: the facade over the per-platform backends"
publish = false

[dependencies]
tinio-sandbox-core.workspace = true

[target.'cfg(target_os = "linux")'.dependencies]
tinio-sandbox-linux.workspace = true

[target.'cfg(target_os = "macos")'.dependencies]
tinio-sandbox-macos.workspace = true

[target.'cfg(target_os = "windows")'.dependencies]
tinio-sandbox-windows.workspace = true

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "forbid"
```

`crates/tinio-sandbox/src/lib.rs`:

```rust
//! Process confinement for tinio — the facade.
//!
//! Re-exports [`tinio_sandbox_core`] and, for the platform this build targets,
//! the one backend that implements it. Consumers name this crate and nothing
//! else; the platform split stays behind the selection below.
//!
//! The three platform crates depend on the core, so a facade that the core or
//! a platform crate depended on would be a cycle — which is why the layering
//! is three deep and not two (design decision 3).
//!
//! # Examples
//!
//! ```
//! use tinio_sandbox::{AccessMode, Sandbox};
//!
//! let sandbox = Sandbox::new().expose("/srv/tinio", AccessMode::ReadWrite);
//! assert_eq!(sandbox.exposures().len(), 1);
//! ```

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub extern crate tinio_sandbox_linux as _linux;
#[cfg(target_os = "macos")]
#[doc(hidden)]
pub extern crate tinio_sandbox_macos as _macos;
#[cfg(target_os = "windows")]
#[doc(hidden)]
pub extern crate tinio_sandbox_windows as _windows;

// **Exactly the core's public surface, name for name, and that equality is the
// rule rather than an accident.** A consumer of this facade is the `serve`
// example and the platform crates; none of them implements `Backend` or
// constructs a platform's `Options`, so "what the consumer needs" and "what the
// core publishes" are the same set. Anything narrower would have to be
// re-derived every time a consumer appears, and would drift silently until a
// platform crate failed to build.
//
// The list is kept in the core's grouping (vocabulary, then launcher) so the two
// can be compared by eye, and Task 3's `the_launcher_surface_is_re_exported`
// binds every name it lists — so a name that stops resolving, or that changes
// shape, is a compile error in that test rather than a discovery three phases
// later. A name *added* to the core is still this list's to gain by hand: see
// the comment on that test.
pub use crate::_core::{
    AccessMode, Adopted, Backend, Child, Confinement, Error, Event, Exposure, LISTENER_FD_PREFIX,
    LaunchPayload, LaunchPlan, Observer, Result, Sandbox, Terminal, adopt, is_listener_fd_arg,
    listener_fd_arg, listener_fd_from_arg, supervise,
};

/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "linux")]
pub use crate::_linux::{Backend as Platform, backend as platform};

/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "macos")]
pub use crate::_macos::{Backend as Platform, backend as platform};

/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "windows")]
pub use crate::_windows::{Backend as Platform, backend as platform};
```

**Each platform arm is one re-export of two names, and the doc comment is repeated per arm rather than written once.** The alias *is* the selection: a name the consumer uses must resolve on the target that selects it, and a doc comment only survives the build it is compiled in — so a comment on the Linux arm alone would leave `Platform` undocumented on the other two. `Backend as Platform` and `backend as platform` are the names the consumers already use, so there is no second name for either fact: an earlier shape re-exported `Backend as PlatformBackend` and `backend` and then re-bound them with `pub type Platform = PlatformBackend;` and `pub fn platform() -> Platform { backend() }`, which gave one item four names and a wrapper whose only job was to rename.

**Every `#[cfg]` above is written twice for one reason, and both copies are load-bearing.** The alias declarations put the dependencies behind `docs/style.md`'s rule (`crate::_core::{…}`, `crate::_linux::{…}` rather than each crate's own name), and a platform alias carries the *same* gate as the re-export it feeds, because the `extern crate` it declares names a crate that exists only on that target: an ungated `pub extern crate tinio_sandbox_linux as _linux;` is `E0463: can't find crate` on the other two hosts, from the declaration rather than from a use.

A platform other than the three has no selection and therefore no `platform()` — the compile error names the missing crate, which is the honest outcome for a platform the design does not support (design §2).

- [x] **Step 4: Write the three platform crates**

`crates/tinio-sandbox-linux/Cargo.toml` — the real one, whose body P1 fills. **`Mechanism` is not in this stub**: P1 Task 3 defines `pub enum Mechanism` in `mechanism.rs` and re-exports it from the crate root, so callers keep writing `tinio_sandbox_linux::Mechanism`. An earlier draft of this block spelled the enum here so P1 Task 2 could name the variants before Task 3 existed; that compile-order split is gone now that the type and its behaviour share a module.

```toml
[package]
name = "tinio-sandbox-linux"
version.workspace = true
edition.workspace = true
description = "Linux mount-namespace confinement for tinio"
publish = false

[dependencies]
derive_more = { workspace = true, features = ["display"] }
hex.workspace = true
rustix = { workspace = true, features = ["fs", "mount", "pipe", "process", "runtime", "thread"] }
sha2.workspace = true
thiserror.workspace = true

tinio-sandbox-core.workspace = true

[dev-dependencies]
tempfile.workspace = true

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "deny"
```

For this task the crate needs only enough to satisfy the facade test:

```rust
//! Linux confinement.
//!
//! The process is confined in place: `unshare` plus `pivot_root` apply to the
//! calling process, so there is no child, no payload and no exit-status
//! forwarding (design decision 6).
//!
//! **The crate is behind `#![cfg(target_os = "linux")]`** so the workspace
//! builds on every host. `rustix` itself compiles everywhere — it ships no
//! crate-level gate and a Windows build simply links a different backend — but
//! its `mount`/`fs`/`process`/`thread` modules are configured out on Windows,
//! so the first line of *this* crate's code that names one of them is an
//! `E0433: cannot find … in rustix`. The gate makes the crate an empty library
//! elsewhere, which is what the facade already assumes when it re-exports this
//! crate under `#[cfg(target_os = "linux")]`. **Every separate target in the
//! crate repeats the gate** (`src/bin/`, `tests/`, `examples/`): a crate-level
//! `#![cfg]` does not cover them.

#![cfg(target_os = "linux")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use std::io;

use crate::_core::{Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox};

/// The Linux backend.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Backend;
///
/// let backend = Backend::default();
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend;

impl BackendTrait for Backend {
    fn confine(&mut self, sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        if sandbox.exposures().is_empty() {
            // A request with nothing in it is the caller's bug, not the
            // platform's shortcoming: `Unsupported` would tell the operator to
            // go look at their kernel, which is the wrong place (design §11).
            return Err(Error::Step {
                step: "confine",
                source: io::Error::other("nothing exposed to confine to"),
            });
        }
        // Filled in by P1 (`2026-09-18-sandbox-p1-linux.md`), which replaces
        // this struct with one carrying `LinuxOptions`.
        Err(Error::Unsupported { detail: "the Linux jail is not built yet" })
    }
}

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_linux::backend();
/// let _ = backend;
/// ```
// **`Backend`, not `Backend::default()`** — verified on Linux, where this
// crate's body is actually linted: `Backend` is a unit struct, so
// `Backend::default()` trips `clippy::default_constructed_unit_structs` and
// `-D warnings` turns it into a build failure. The macOS and Windows stubs
// already spell it this way; this crate was the odd one out, and the
// discrepancy is invisible on any non-Linux host because the crate-root
// `#![cfg]` leaves their bodies unlinted there. The `# Examples` blocks above
// keep `Backend::default()`: doctests are not linted, and the example is there
// to show the derived `Default` exists.
pub fn backend() -> Backend {
    Backend
}
```

`crates/tinio-sandbox-macos/Cargo.toml` and `crates/tinio-sandbox-windows/Cargo.toml` are identical apart from name, description and the `Backend` doc path; both are:

```toml
[package]
name = "tinio-sandbox-macos"
version.workspace = true
edition.workspace = true
description = "Seatbelt-backed confinement for tinio"
publish = false

[dependencies]
tinio-sandbox-core.workspace = true

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "forbid"
```

with a body that exists to be filled in:

```rust
//! macOS confinement via Seatbelt. The stub exists so the launcher-shaped half
//! of the `Backend` contract is exercised from here onward; the implementation
//! lands with P3 (`docs/superpowers/plans/2026-09-18-sandbox-p3-macos.md`).
//!
//! Behind `#![cfg(target_os = "macos")]` so the workspace builds on every host:
//! `members = ["crates/*"]` has no per-target form, so this crate compiles — and
//! lints — on targets it does not serve. **P3 replaces this stub and may drop
//! the line**: that plan's shape is a crate which compiles everywhere with each
//! macOS-specific item gated internally, and both shapes satisfy the same rule.
//! Separate targets repeat whichever gate they need.

#![cfg(target_os = "macos")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use crate::_core::{Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox};

/// The macOS backend.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::Backend;
///
/// let backend = Backend::default();
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend;

impl BackendTrait for Backend {
    fn confine(&mut self, _sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        Err(Error::Unsupported { detail: "the macOS backend is not implemented yet" })
    }
}

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_macos::backend();
/// let _ = backend;
/// ```
pub fn backend() -> Backend {
    Backend
}
```

The Windows crate is the same with `windows` for `macos`, "AppContainer" for "Seatbelt", and `#![cfg(target_os = "windows")]` as its gate.

**Each of the three reaches the core through the alias, like every other crate in this workspace that depends on one** (`docs/style.md` §Imports): `#[doc(hidden)] pub extern crate tinio_sandbox_core as _core;` in the crate's own `lib.rs`, then `use crate::_core::{…}`. The alias sits **after** the crate-root `#![cfg]` — inner attributes must precede items — and it needs no gate of its own, because a false crate-root `#![cfg]` strips the whole body, this line included. **These three crates are the templates P1, P2 and P3 copy, so the form matters more here than anywhere else in the plan**: those phases' own code blocks predate it, and shared §1 says how they are corrected.

Add to the root `Cargo.toml`, alphabetically:

```toml
tinio-sandbox = { path = "crates/tinio-sandbox" }
tinio-sandbox-linux = { path = "crates/tinio-sandbox-linux" }
tinio-sandbox-macos = { path = "crates/tinio-sandbox-macos" }
tinio-sandbox-windows = { path = "crates/tinio-sandbox-windows" }
```

Update the README row added in Task 1 to name all five:

```markdown
| `tinio-sandbox` / `-core` / `-linux` / `-macos` / `-windows` | Process confinement: the `Backend` contract, per-platform implementations, and a facade |
```

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox`
Expected: PASS. On Linux the `linux_selects_the_in_process_backend` case passes; on macOS and Windows the launcher-stub case does.

- [x] **Step 6: Prove the layering has no cycle**

Run: `cargo tree -p tinio-sandbox-linux`
Expected: `tinio-sandbox-linux` → `tinio-sandbox-core` and nothing else from this workspace. If `tinio-sandbox` appears anywhere in that tree, a platform crate has picked up a facade dependency and the layering is broken.

- [x] **Step 7: Verify every crate's lint**

Run: `rg -n 'unsafe_code' crates/tinio-sandbox*/Cargo.toml`
Expected: `tinio-sandbox-linux` is `"deny"`; core, facade, macos and windows are `"forbid"`.

---

## Commit

**One commit, made by hand, at the end of the phase.** No task in this plan commits, so every change accumulates in the working tree and the phase is reviewed as one diff rather than a commit per task. `CLAUDE.md` owns the authorisation rule — git writes are asked for per operation, and the index is never written by an agent — so this section fixes the phase's **scope** and its message, and is not permission to run it.

Run the full Standard acceptance block (`docs/tests.md`) first, and commit only once the phase's own review is clean. The message is a conventional header plus single-line cause-and-effect bullets: what changed and why, never where it lives. It is a starting point rather than a description of finished work — where the phase's actual run makes a line of it false, that line changes.

```bash
git add Cargo.toml Cargo.lock README.md crates/tinio-sandbox-core crates/tinio-sandbox crates/tinio-sandbox-linux crates/tinio-sandbox-macos crates/tinio-sandbox-windows .github/workflows/ci.yml docs/superpowers/plans/2026-09-18-sandbox-p0-core.md docs/superpowers/plans/2026-09-18-sandbox-shared.md docs/superpowers/specs/2026-09-18-sandbox-design.md
git commit -F - <<'EOF'
feat(sandbox): add the confinement contract and the launcher machinery

- One trait, one method: Confinement keeps the cfg out of main
- Builder stays pure data; validation is the platform's job
- Payload on stdin, one JSON object: no nonce, nothing to forge
- Listener descriptor in argv: a socket cannot ride on a stream
- Stdout is a protocol: SSE framing survives a newline in a message
- One decoder per stream: a per-call one drops frames split mid-value
- Relay answer read after the join, or a healthy child reads as never ready
- Five crates, three stubs: the trait is exercised from both shapes
EOF
```

## Self-Review

**Spec coverage.** Walking `docs/superpowers/specs/2026-09-18-sandbox-design.md` against the tasks above. Only the rows this plan owns appear here; the platform-specific rows are in P1, P2 and P3, and each of those plans carries its own table.

| Spec item | Task |
|---|---|
| §1 five crates and the trait | 1, 2, 3 |
| §1 the launcher-shared surface, stated once | 2 — and this is the row this plan exists for |
| decision 2 one trait, one method, `Confinement` | 2, 3 |
| decision 3 crate boundary and the forced layering | 3, verified by its `cargo tree` step |
| decision 14 the lint follows the implementation | 3 Step 7 asserts core, facade and the macOS stub are `forbid`; the `deny` crates belong to P1 and P2 |
| decision 17 the payload on stdin, and no environment marker | 2 |
| decision 18 the parent never builds a `Config` | 2 — the payload carries the configuration **text**, unparsed; the parent side lands in P2 and P3 |
| decision 19 stdout is a protocol channel | 2 (the `Event` type, `Observer` and `Terminal`); the emitters land in P1, P2 and P3 |
| §12 the event contract, unit half | 2 |
| every other decision and section | **P1, P2 or P3** — this plan is the shared half only |

**Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N". One deliberate judgement call is flagged inline rather than hidden: the `ALL_CAPABILITIES`-style enumeration problems that the platform crates hit are theirs, not this plan's; the only enumeration here is the module and re-export lists, which are written out in full.

**Type consistency.** This is the plan where that check matters most, because three other plans consume what it defines. `AccessMode` (Task 1, and **spelled by `tinio-core`, not by this plan**: the two variants are `ReadWrite` and `ReadOnly`, the predicate is `is_read_only()`, and there is no `is_writable`), `Exposure`, `Sandbox` (`new`, `expose`, `device_files`, `exposures`, `device_files_enabled`, `from_exposures(exposures, device_files)`), `Error::{Step, Unsupported, BadPath, Launch, Wait}` (Task 1) are used verbatim by Tasks 2 and 3 and by all three platform plans. `Backend`, `Confinement`, `Child` (`new`, `with_relay`, `wait`, `terminal`), `supervise(&mut Child, teardown)`, `LaunchPlan`, `LaunchPayload`, `Adopted`, `adopt`, `Event`, `Observer`, `Terminal`, `LISTENER_FD_PREFIX` and the three `listener_fd_*` helpers (Tasks 2 and 3) are the surface P1, P2 and P3 were reconciled to; the reconciliation is recorded in Task 2 Step 6, including what each platform plan had invented for itself and why it is now here instead.

**What is deliberately *not* on that surface**, and the reason each one left it:

- **`Error::BadPayload`.** It existed to report a payload that failed its nonce check. With the nonce gone and the payload parsed by `serde_json`, nothing constructs it: `adopt` treats an undecodable payload as "this is the launcher" — `Ok(None)` — which is the documented behaviour and needs no error value. An enum variant no code path builds is a promise the type does not keep.
- **`MAGIC` and `NONCE_LEN`.** They belonged to the hand-rolled binary frame. `serde_json` rejects foreign text at the first byte and has no length field to validate.
- **`nonce()` and `LaunchPlan::with_nonce`.** The nonce they served could not discriminate anything the payload's shape did not (design decision 17, amended), and its removal is what let `for_launch` become infallible.
- **`LaunchPayload::encode` / `decode`.** Two wrappers whose whole body would be one `serde_json` call. The call sites use `serde_json` directly, which also keeps the "the whole stream is the payload" rule visible where it is enforced.

**What the grilling of 2026-09-19 changed, and why each item was a defect rather than a preference.**

1. **`Child` had no `Debug`, and `Confinement` derived it.** A compile error, not a nit: `Confinement::Supervise(Child)` is a field of a `#[derive(Debug)]` enum, and `Box<dyn FnMut…>` has no `Debug`. Every `unwrap_err()` in the three platform plans depends on it compiling. Fixed with `derive_more::Debug` and `#[debug(skip)]`.
2. **The module-level `//!` in `launcher/mod.rs` sat after a `pub use`.** Inner doc comments must precede the first item in a module; this one was inserted while moving the payload to stdin and would not have compiled.
3. **`Event` was hand-written JSON on both sides.** `push_json_string` wrote escapes that the reader could not parse back — it split on the first `"` — so a message containing a quote or a backslash would not round-trip, and the test that asserted it did would never have passed. Parsing is now `sse-core`'s resumable state machine (which is also what assembles an event split across two reads) and serialization is `serde_json`'s, with `addr: SocketAddr` so the port survives — the field had been a `String`, which is what `local_addr()` was being formatted into.
4. **`sse-stream` was the wrong crate to reach for.** It lists `futures-util`, `http-body` and `http-body-util` as *non-optional* dependencies, none of which a synchronous, thread-based core can use; `sse-core` is the same parsing lineage with `bytes` as its only dependency, and its synchronous `SseDecoder` is exactly what the relay needs.
5. **`reemit` returned nothing, so the parent could not tell whether the child ever became ready.** The "parent emits `Event::Error` on a child that never listened" rule and the "printed exactly once" guarantee from decision 19 are unsatisfiable without a record of what was relayed. `Observer` is that record — and the follow-through matters as much as the type: `Child` now carries the relay's `JoinHandle` and reads the answer out of the join after the wait, `reemit` is `pub(crate)` because its convenience is exactly the part that loses the answer, and both platform plans drive `Observer` rather than `reemit` for that reason.
6. **The relay was synchronous, and `relay_stderr` should never have existed.** The relay's streams are a launched child's pipes, which the runtime owns — so its reads and writes are async (`tokio::io`), and the core takes `io-util` and nothing else, with `rt`/`macros` confined to `dev-dependencies` because they exist to test what production does not need. Separately, a `relay_stderr` had been added to prevent a stderr-pipe deadlock that **neither launcher platform can suffer**: both leave stderr inherited, so there is no pipe. It was removed rather than kept as insurance, because a function with no caller is a promise the crate does not keep.
7. **`LaunchPayload` was a bespoke binary format with a nonce that could not do its job.** A magic constant, a 32-byte nonce, a length prefix and raw bytes — a format only this crate could read. The nonce was meant to identify a confined child, but the parent writes it to the same stdin it writes everything else to, so anything able to forge a payload already knows the value; what actually discriminates the child is the argv listener element plus a payload that decodes. The payload is now **one JSON object**, `{"config":"…"}`, with `serde` on both sides and `serde_json`'s own end-of-input check rejecting trailing bytes. `config` is a `String` because `Config::load` already reads it with `read_to_string` and `toml::from_str` takes a `&str` — the UTF-8 requirement is pre-existing, so carrying bytes would mean a base64 adapter for an input the program cannot produce. That removed `getrandom`, the `MAGIC` and `NONCE_LEN` constants, `nonce()` and `with_nonce`, and made `for_launch` infallible.
8. **`Event::decode` could not tell which variant it was reading.** Probing the payloads in turn was ambiguous by construction — `{"message":…}` deserializes under the `listening` name too — so the decoder is now given the frame (or its two field values) and the name decides.

**The second review pass of 2026-09-19 — six defects, each recorded because each was invisible until something was read or run.**

1. **A `SseDecoder` was built per call, which destroyed the resumability the framing was chosen for.** The decoder holds the field values of an event it has begun and not finished — `consume_until_newline` appends them to its own buffer and advances the input past them — so an instance built inside `decode` consumed those bytes and then dropped them: every frame whose chunk boundary landed mid-value vanished without a trace. The relay's own split-frame test, three bytes per read, could not have passed. The decoder now lives in `observe` and is passed down, and that test asserts whole frames rather than a count, because a relay that lost a payload would still emit one frame per surviving event.
2. **`supervise` consumed the `Child`, so the read decision 19 requires had nowhere to happen after the wait.** Both platform plans say "read the slot after the wait", and neither could: the only place left was before it, while the relay was still running. `supervise` borrows now — `&mut Child` — and the read happens after it returns.
3. **Nothing joined the relay thread, so "after the wait" was still a snapshot.** The child being reaped and the relay noticing EOF are unordered events, so a caller reading the slot as soon as `wait` returned would usually read `None` on a perfectly healthy child — and the launcher's never-became-ready rule would fire on the runs that went right. `Child::with_relay` takes the thread, `wait` joins it, and the test for it uses a relay that sleeps past the child's exit so the un-joined version fails.

    **The join then replaced the shared slot, which is the third pass's first change.** `TerminalSlot` — `Arc<Mutex<Option<Terminal>>>`, created by the launcher before the relay thread started and written by that thread — existed only to carry the answer across a join that was not happening. With the join in `wait`, the relay thread returns `Option<Terminal>` and `JoinHandle` *is* the channel: the alias, the lock, the poisoning branch, the clone on every read and the "create the slot before the thread" ordering rule all go with it, and `Child::new` loses a parameter. `Child::terminal` returns `Option<&Terminal>`, matching `Observer::terminal`.
4. **`Error` had no `From<io::Error>`, so this plan's own doc examples and tests — and P3's production code — could not be written with `?`.** `#[from]` needs a single-field variant and `Step { step, source }` is not one. `Error::Wait(#[from] io::Error)` is that variant and the only `From` impl on the type; a platform that knows the operation's name builds `Step` instead, so only one of the two can happen by accident.
5. **`Observer::record` let a later `Error` overwrite an earlier `Listening`.** The question the slot answers is "did this child ever become ready?", which a failure *after* a successful start cannot retract; the overwrite made a healthy launch look like a failed one to the very rule the type exists for. The first terminal event wins now, whichever it is.
6. **A `pub(crate)` item's doc example is a compile error rather than an untested example.** rustdoc collects doctests from private items, and a doctest is compiled against the crate's *public* surface — so `reemit`'s example, which named it, would have failed `cargo test` outright. It is prose now, and it says why, so nobody adds one back.

**Four decisions of the third pass that changed the surface rather than the prose.** (1) `TerminalSlot` is gone — see item 3 above. (2) **`Backend::confine` takes `&mut self`**, because the call changes the caller (irreversibly on Linux) and because a launcher backend records what it set up — which is why P1's `seccomp` field became a plain `Option<Seccomp>` instead of a `Cell`. (3) **`Child`'s `!Send`-ness is documented rather than assumed away**: the wait closure is a bare `Box<dyn FnMut …>` and the Windows closure carries a raw handle, so a `+ Send` bound would push an `unsafe impl` into a crate that is `deny(unsafe_code)` — for a property nothing needs today, since every call site confines before any thread exists. (4) **The relay flushes only when it wrote**: the protocol's events are sparse, and a flush per chunk on an idle stream was pure syscall noise.

**Four more that were factual rather than structural.** `sse-core`'s default features pull `futures-core` and `pin-project-lite` — the async baggage this crate was chosen over `sse-stream` to avoid — so it is declared `default-features = false`; and its dependencies are `bytes`, `memchr` and `thiserror`, not `bytes` alone. `Event::decode` was a second, hand-written SSE parser beside `sse-core`'s, and now runs the same state machine the relay does — one parser, one meaning for a frame. `Sandbox::from_exposures` silently dropped the device-file flag, so the flag is a parameter now rather than something a caller must remember to re-apply. And `LaunchPayload`/`Adopted` derived `Debug` over the configuration text — credentials included — so their `config` field carries `#[debug(skip)]`, the same treatment `Child`'s closure field already had.

**Three defects this plan's extraction was expected to fix, and did.** (1) The core surface and the Linux work lived in one document, so the two launcher plans read what they believed was a platform plan and found shared infrastructure inside it. (2) Both launcher plans wrote `adopt` with different signatures and different return types, because the parse-and-read half and the rebuild half were never separated. (3) The launcher-shared surface had no single home, which is what this plan is.
