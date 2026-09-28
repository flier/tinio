# `--sandbox` P3 (macOS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Progress, 2026-09-25**: the step boxes are ticked where the step's outcome is in the tree or recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 — the measurement record the follow-ups file's findings moved into; that file is now a settled-record pointer and holds no results. The one left unticked is Task 7 Step 3 (conditional, never triggered), carrying its own note; Task 9 Step 6's selection run was recorded on 2026-09-26 and its box is ticked. **The gate script is green as of 2026-09-26, both mechanisms**: `macos-sandbox-verify.sh` runs 1→4 to `rc=0`, its prune experiment now has its own log window (reported, never asserted on), `rg` is no longer required (`grep -oE` + `sed`), and two `EXPECTED` entries were corrected against measurement — the trace-device probe is `file-read-data` (the spelling the shipped profile produces; the write face would mean the profile had begun granting the read), and the six `sysctl-read` names the loader's startup probes are refused are enumerated in the rule rather than left to redden the gate. **The body below is the plan as written before Task 5's allowance audit**, so every `DEVICE_RULES` reference in it is the target it was written against: the table was deleted with the device literals, `HOME_GRANT` and the `H` parameter went with the home measurement, and the tree's `profile.rs` renders `RUNTIME_BASELINE` alone (design §5's audit and `home` paragraphs). **The body also predates the shared-type reconciliation, and the interface table above carries the landed spellings**: `Sandbox::device_files()`/`device_files_enabled()` and the `bool` second argument of `from_exposures` are gone (the flag is `tinio_sandbox_linux::Options`'s own knob), `Sandbox::served_root()` is the first exposure's one home, `LaunchPayload` derives `Deserialize` alone with `LaunchPayload::encode_frame` its only writer, and `LaunchPlan` carries `config` (`config()`, `has_config()`) rather than a whole payload — so every such call in a code block below is that block's record, not a shape to write.

**Goal:** Fill in `tinio-sandbox-macos` so `--sandbox` confines the tinio server on macOS with a Seatbelt (SBPL) profile: text generated as a pure function of the exposure set, applied by `sandbox-exec -p` to a child that inherits the parent's listener, with `(deny default)` supplying both the filesystem boundary and the network cut-off.

**Architecture:** The core owns the trait, the vocabulary and the launcher machinery (the payload type, the listener-descriptor argv element, the event stream and its relay, the supervision loop, exit-status forwarding). This crate owns the SBPL text (`profile.rs`, pure and unit-testable on any platform) and the launch (`launch.rs`: clear `FD_CLOEXEC` on the listener, pre-flight the profile with its `-D` parameters, spawn `sandbox-exec -p <profile> -D NAME=VALUE … <self> <argv…>`, write the payload to the child's stdin, relay its stdout events, return `Confinement::Supervise`). The consumer (`serve`) binds the listener, computes the exposure set, and in whichever process serves — the confined child here — adopts the payload, emits the `Listening` event and builds its runtime.

**Tech Stack:** Rust 2024, `tinio-sandbox-core` (trait, vocabulary, launcher machinery), `rustix` 1.1's `io` surface (`fcntl_setfd`, no feature flags) as a macOS-only target dependency, `tempfile` for tests, `sandbox-exec -p` (Seatbelt/SBPL) as the mechanism.

**Spec:** `docs/superpowers/specs/2026-09-18-sandbox-design.md` — the design document, which is the authority for every decision cited below as `design §N` or `decision N`. Read it alongside this plan: §5 is the profile, §3 the launch contract, §7 the network rules, §10 the entry-point sequence, §12 the testing requirements, and decisions 16, 18, 22 and 24 are cited repeatedly.

## Verification Posture (read this before Task 1)

**The mechanism and the profile are measured; the crate is not written.** The design's §5 profile is measured text from a macOS 15.8 VM, §7's macOS row is measured, every macOS row of the evidence table names its observation, and the Risks row about the profile reads **Closed**. What no command has touched is `crates/tinio-sandbox-macos`, which does not exist — so this plan is written as an **experiment with a fixed shape** for the crate, against answers the design already holds:

- Tasks 1, 2, 3 and 8's pure halves are unit-testable on any host, including the Windows host this plan was written on: the profile text, the launch argv, the relay's relay-or-not decision, and the read-only probe. Everything else runs only on a Mac.
- Task 4 exists to make **this crate's** profile start a confined process, and its first run is expected to fail for a reason the task explains. Task 1's allowance list is the design's measured working set rather than a fresh guess, but it is a starting point until Task 5's run reproduces it from this crate.
- Task 5 turns the working set into a **minimal recorded** set: every surviving rule must name the observation that keeps it, and every rule the run shows to be removable must be deleted.
- Task 7's `accept` question is **answered and not open**: a profile carrying zero network rules was measured taking a connection out of an inherited listener's backlog, and `accept` is gated by no rule at all (design §5 and §7). Task 7 confirms that answer against this crate instead of re-opening it.
- Nothing in Tasks 4–11 may be reported as working until a command's output on a real Mac has been pasted into the phase's evidence. A profile that merely fails to crash the process proves nothing (design §12).

## Global Constraints

**The conventions the four plans share are stated once, in `2026-09-18-sandbox-shared.md` — *shared* from here on — §1 of which owns test-first, the panic rule, the doc-example rule, imports, the manifest rules, the per-crate lint allocation, English only, attribution, hard-wrapping and git writes. Shared §2 owns the crate topology and the two shapes, §3 the list of what a launcher platform owns, §4 the names two documents must spell alike, and §5 the three relays. What is left here is this plan's own:

- **On a Mac the first run of an implementation task is expected to fail**, for a reason the task explains — shared §1's Red-Green-Refactor cycle with this phase's local consequence, which the Verification Posture above states in full.
- `unsafe_code = "deny"` in `tinio-sandbox-macos`, with **three** `#[allow(unsafe_code)]` sites, the first of which is `launch::inherited_fd` — the private helper that turns the descriptor number the launcher passed in argv into an `OwnedFd`, which its two safe callers `launch::listener` and `launch::control` build their sockets from, and whose invariant lives in its `# Safety` section (the descriptor is inherited from the launcher, which holds it open for the child's whole lifetime and never closes it). **The design's decision 14 enumerates them and is the only place the list lives** — every backend crate is `deny` with its sites enumerated, while `tinio-sandbox-core` and the facade keep `forbid` — and this bullet said "exactly one" until the in-process mechanism landed (2026-09-26): `apply::libsystem`'s `unsafe extern "C"` block, declaring the two `libSystem` symbols the apply needs (neither `rustix` nor `libc` 0.2.189 ships a `sandbox_*` binding) and `apply::validator`'s `fork` are the other two. **The descriptor site carries no FFI**: it is `std`'s `OwnedFd::from_raw_fd`, not an `extern` block — the FFI is `apply::libsystem`'s own — and every other macOS need is met by `std` or by `rustix`'s safe surface — `rustix::io::fcntl_setfd` is handed the socket's own `AsFd`, so the parent reconstructs no descriptor from an integer (Task 2 Step 5 says which functions, and why they are safe). The count is **per capability and established by the implementation**, never asserted ahead of it — the earlier claim that this crate needed no `unsafe` at all did not survive contact with `rustc` — so a further site arrives only with the observation that requires it and an entry in design decision 14, never to shorten a call.
- `tinio-sandbox-macos` may depend on `tinio-sandbox-core`. It must **never** depend on the facade `tinio-sandbox` — the facade selects a platform implementation, so that edge is the cycle shared §2 exists to prevent.
- **This crate takes the first of shared §2's two shapes, and the transition shared §2 left to P3 is settled the other way from this bullet's original wording**: `crates/tinio-sandbox-macos/src/lib.rs` carries `#![cfg(target_os = "macos")]` once at the crate root, so on Linux and Windows the member is an **empty library that still builds** (`cargo check --workspace` builds every member) and every macOS-only item is gated by that one attribute rather than by a per-item `#[cfg]` with a stub arm beside it. The operator's ruling and the sharp edge it leaves — an ungated item or an ungated `//!` doctest is a compile error off macOS rather than a silent omission — are design §5's last paragraph. Every separate target repeats the gate it needs, and this crate's `tests/` targets do: each opens with `#![cfg(target_os = "macos")]`.
- `sandbox-exec` is **deprecated by Apple and still shipped**. Every comment that names it says so; nothing in the code or docs may present it as a supported, versioned API. Its removal is the design's accepted degradation: "no sandbox available, fail closed", which the error table of design §11 already covers.
- The profile is passed **only** as a single argv element to `-p`, and the exposed paths travel beside it as `-D NAME=VALUE` parameters that the text refers to as `(param "E<i>")` (decision 24). No profile file may be created — not in `$TMPDIR`, not anywhere — and Task 2 has a test that fails if one is.
- The generated profile never contains `network-outbound`, and never contains `network-bind` (decision 16, design §7). Task 1's unit test asserts both, on the generated text.
- **Dated note, 2026-09-25: this constraint was contradicted by the revision that landed, and the fix restores it rather than relaxing it.** The generated profile emitted `(allow network-bind)` for the confined child's own local management control socket (`<state dir>/tinio-<16 hex>.sock`): its `bind()` is gated by the sandbox, and without the rule the child dies `could not bind …/.tinio/tinio-<hash>.sock: Operation not permitted (os error 1)`. The text therefore carried the operation this bullet forbids, and the record is this note rather than an amendment to the bullet — **the fix is the one the constraint always implied**: bind that socket outside the sandbox and hand it in, as the launcher already hands in the data plane's listener, which leaves the generated profile with no bind rule at all. Read the bullet above as the target, not as a description of the tree, until a real Mac's profile shows the rule gone. **That Mac's profile has since shown it gone, 2026-09-25**: the launcher binds the control socket and hands the descriptor over, so the generated text carries no bind rule — the bullet above is now a description of the tree (design §5's network paragraph).
- The profile **is** visible in the process list; its contents are paths, not credentials (decision 24). That is accepted, not mitigated.
- Exposures: `ReadOnly` is `(allow file-read* …)` and `ReadWrite` is `(allow file-read* file-write* …)` — the whole of decision 8's "read-only is the absence of a write permission" on this platform.
- **stdout is a protocol channel and carries `Event` frames and nothing else; every log goes to stderr** (decision 19), and the child's stderr is inherited untouched and unmediated (shared §3). The launcher never prints on its own behalf: it **decodes and re-emits** the child's frames through the core's `Observer`, so anything the child or a dependency writes that is not one of our events is dropped rather than passed to an operator who could not tell it apart from a real event. On this platform that includes `sandbox-exec`'s own diagnostics, which is where a refused profile shows up.
- **Signal handling stays async-signal-safe.** No code of ours runs in a signal handler: the handler is `signal-hook`'s (a self-pipe write, no allocation), and the relaying runs on a dedicated **normal** thread — not on the signal path — which does the child's pid (an `AtomicI32`), one `kill` syscall, and the `debug!` event shared §7 asks for. What may not allocate, lock or log is a handler, and none of ours is one (Task 3 states the boundary).
- **Three things in this phase are called a relay, and shared §5 defines the three names.** P3's own tasks are the *event relay* in Tasks 2 and 8, the *signal relay* in Task 3, and *exit-status forwarding* in Task 9; name them by those phrases, never by "the relay" alone.
- **Doc examples:** `no_run` is required wherever the example would exec a process.
- **This plan's imports backlog is about 80 sites**, mostly in test setup and FFI plumbing, where a bare `std::fs::write` or `std::mem::size_of` reads better than a wall of imports. Correct them in place as you write the code; do not relax the rule (shared §1).

## Depends on P0, P1 and P2

P3 consumes the core P0 builds; it does not rebuild it. The shared surface lives in its own document, `docs/superpowers/plans/2026-09-18-sandbox-p0-core.md` — P0 Task 1 is the vocabulary, Task 2 the launcher machinery (`adopt`, `Event`/`Observer`, `LaunchPlan`), Task 3 the facade and the three platform-crate stubs — and both other platform plans have landed before it. **The facts all four documents share rather than any one of them building are in `2026-09-18-sandbox-shared.md`** — the conventions (§1), the crate topology and its two shapes (§2), the list of what a launcher platform owns (§3), the names two documents must spell alike (§4), the three relays (§5) and the two verification conventions (§6) — and this plan refers to them by section.

**P3 depends on P1, and this plan never restates what P1 wrote.** P1 (`2026-09-18-sandbox-p1-linux.md`) owns, in the one `serve.rs` both plans edit: `parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Args, _>` and `struct Args { root, address, config_path, read_only, sandbox }` (its Task 7), the two-phase `main` — the argument parse, the configuration read, the read-only resolution and the guarded `create_dir_all` all happen before anything is confined (its Task 7) — the `sandbox` feature and the example's `required-features = ["sandbox"]` (its Task 7), the **`serve` entry point, whose signature `2026-09-18-sandbox-shared.md` §4 owns and this plan neither lists nor counts** (its Task 7), which P3 consumes unchanged, the readiness migration that turns `crates/tinio-server/tests/serve_read_only.rs`'s scraped line into a decoded `Event` frame (its Task 8), and the `sandbox_request(root, state_dir, access) -> Sandbox` helper (its Task 7) this plan reuses unchanged. **The file's two frame helpers are P1's as well** — `emit(event: &Event)` and `emit_error(message: &str)`, written in its Task 7 — because they live in `serve.rs`, which all three plans edit, and a second `fn emit` in one file is `E0428`: P2's and P3's arms call them and define neither, which is the same ownership rule the names above follow. P3 writes no argument parser, no second `main` and no helper of its own in that file: what it changes is the exposure set's construction, the launch, and the child arm.

**Decision 5 is P1's arm and this plan keeps it.** `main` refuses `--sandbox` in a build without the feature (exit 2) and, on a platform whose backend another plan owns, refuses it as well rather than accepting it and confining nothing (P1 Task 7 writes both; P2 and P3 replace the second with their own platform arm and keep the first). The arm is restored in Task 8 Step 2(e) below, because a build that silently ignores the flag is the failure the whole design exists to prevent.

P2 (`2026-09-18-sandbox-p2-windows.md`) is the other launcher, and it lands first; where it has already built a shared parent-side piece, this plan reuses it rather than rewriting it (the paragraph after the table below says where). The five-crate layout is settled — `tinio-sandbox-core`, `tinio-sandbox-{linux,macos,windows}`, and the `tinio-sandbox` facade (design decisions 3 and §1), and it is P0 Task 3 that lays it down: `tinio-sandbox-macos` ships `Backend` (`pub struct Backend`) plus `backend()`, whose `confine` currently returns `Error::Unsupported { detail: "the macOS backend is not implemented yet" }`. This plan fills that crate in; nothing in it depends on a different crate topology.

**The core's surface is complete and P3 is a pure consumer of it** — P0 defines all of it, this plan adds nothing to it, and every item below is called as listed. The names are fixed; Task 1 Step 1's and Task 2 Step 1's "reconcile" instructions exist only to catch a typo, not a missing piece.

| Consumed from `tinio-sandbox-core` | Called as | Used by |
|---|---|---|
| `AccessMode::{ReadOnly, ReadWrite}` + `is_read_only()` — **`tinio-core`'s type**, re-exported by the core rather than defined in it | `AccessMode::ReadWrite`, `!exposure.access.is_read_only()`, `AccessMode::from(read_only)` | Tasks 1, 2, 4, 6, 8 |
| `Exposure { host, access }` | field reads (`.host`, `.access`), construction in `with_canonical_paths` | Tasks 1, 2, 4 |
| `Sandbox::{new, expose, exposures, served_root, from_exposures}` | `Sandbox::from_exposures(exposures)` rebuilds — one argument, the device-file flag having left the shared type for the Linux backend's own `Options` — and `served_root()` answers the first exposure | Tasks 1, 2, 4, 6, 8 |
| `Error::{Step { step, source }, Unsupported { detail }, BadPath { path, reason }, Wait(io::Error), Launch { operation, path, source }}`, `Result<T>` | `Step` for a named operation, `BadPath` for an exposure, `Launch` for the failed spawn, `Unsupported` for `sandbox-exec` missing, `Wait` for the wait closure's `?`. `BadPayload` is never seen here: core's `adopt` turns a decode failure into `Ok(None)` | everywhere |
| `Confinement::{InPlace, Supervise(Child)}` | matched in `serve` and the harness | Tasks 2, 4, 8 |
| `Backend` (the trait) | imported as `Backend as _`; `platform().confine(sandbox, &plan)` | Task 8 |
| `LaunchPayload { config: String }` — **the pre-change record; the landed shape is `LaunchPayload { config: String, token: Option<String>, confinement: Option<SandboxSpec> }`** (`crates/tinio-sandbox-core/src/launcher/payload.rs`) — it derives `Deserialize` alone, the crate-private `Wire` view is the frame's one definition, and `LaunchPayload::encode_frame(config, token, confinement)` is its only writer; the owned `encode` is gone (shared §3 owns both the shape and the encoding) | `plan.config()` → `hand_over` writes the frame with `LaunchPayload::encode_frame` into the child's stdin; `adopt` deserializes with `serde_json` | Tasks 2, 4, 8 |
| `LaunchPlan::{new, config, has_config, with_listener, listener, for_launch}` | `LaunchPlan::for_launch(config).with_listener(listener)`; `plan.config()` / `plan.has_config()` — the plan carries the configuration text itself and no payload; `plan.listener().map(\|l\| i64::from(l.as_raw_fd()))` | Tasks 2, 4, 8 |
| `Adopted { config, listener_fd }` + `adopt(args, stdin)` | `adopt(env::args_os(), std::io::stdin().lock())` | Tasks 4, 8 |
| `Child::{new, with_relay, wait, terminal}`, `supervise(&mut Child, teardown)` | `Child::new(move \|\| Ok(child.wait()?)).with_relay(relay)` — the constructor takes the wait closure **only**, and the terminal record is the relay's return value that `Child::wait` keeps after joining it; `supervise(&mut child, teardown)`; `child.terminal()` read **after** the wait for the never-became-ready case | Tasks 2, 4, 8 |
| `Event::{Listening, Error}` + `encode()` / `decode()`, `Observer::new(w).observe(r)` (async) | `Event::Listening{..}.encode()` in `emit`; `Observer::new(tokio::io::stdout()).observe(reader).await` in the relay thread, whose **return value is the terminal** the `Child` keeps after the join; `Event::decode(&frame)` in the e2e | Tasks 2, 8, 9 |
| `listener_fd_arg`, `is_listener_fd_arg` — and, on the core's side of the same contract, `LISTENER_FD_PREFIX` and `listener_fd_from_arg` | `command_args` appends `listener_fd_arg(fd)`; `confine`, `serve`'s `own_args` and the harness filter with `is_launcher_fd_arg` — the one name for the listener element and the control element alike. `listener_fd_from_arg` is called inside core's `adopt`, not here; the prefix constant is referenced in docs only | Tasks 2, 4, 8 |
| `nonce()`, `MAGIC`, `with_nonce` | **do not exist**: the payload is one JSON object and the nonce is gone (decision 17, amended) | — |

**What this phase owns instead** — **the macOS deltas on `2026-09-18-sandbox-shared.md` §3's list**, plus the wiring only this launcher can have, all of it inside `crates/tinio-sandbox-macos` and none of it a core addition:

- the **profile**: its text, its empirical allowance set, and the pre-flight that proves `sandbox-exec` accepts it — in a debug build or under a `debug` log level; a release build proves it at the real spawn instead (design §5's operational-limits paragraph).
- the **descriptor work**: clearing `FD_CLOEXEC` on the listener and turning `plan.listener()`'s `&TcpListener` into the argv element's number.
- the argv filtering — `std::env::args_os().skip(1)`, with the listener element appended — and `hand_over`, the payload write to the child's stdin and its close (shared §3).
- the process handle, and the thread that drives the core's `Observer` over the child's stdout (shared §3), plus the signal relay this plan's Task 3 owns.

Task 8 (the `serve` wiring) is where these meet the call site. Where P2 has already landed shared parent-side pieces (the read-only probe, the supervision call), **reuse them unchanged** and let the corresponding step collapse to a verification that the call is present.

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` (workspace root) | one new workspace dependency, `signal-hook`, for the relay |
| `crates/tinio-sandbox-macos/Cargo.toml` | the crate's manifest; `tracing` is a workspace dependency every target takes, while `rustix` and `signal-hook` are macOS-only target dependencies |
| `crates/tinio-sandbox-macos/src/lib.rs` | module wiring, the `Backend` the facade selects and its `backend()` constructor, and the crate-level warning about `sandbox-exec`'s deprecation |
| `crates/tinio-sandbox-macos/src/profile.rs` | the SBPL text and the `-D` parameters its `(param …)` references resolve to, as a pure function of the exposure set, plus the empirical baseline allowance table |
| `crates/tinio-sandbox-macos/src/launch.rs` | `confine`: the profile pre-flight, the listener's close-on-exec flag, the `sandbox-exec -p` spawn, the payload on stdin, the stdout event relay, installing the stop-signal handling; the stub arm for other targets |
| `crates/tinio-sandbox-macos/src/relay.rs` | `on_stop` / `disarm` / the pure decision: termination-signal relaying, the handler install and the relaying thread |
| `crates/tinio-sandbox-macos/tests/relay.rs` | the relay measured: a signal to the launcher reaches the child, and a disarmed relay forwards nothing |
| `crates/tinio-sandbox-macos/tests/harness/mod.rs` | the re-exec harness shared by the tests (and the pruning experiment), including the launcher copy that gives a confined child an argv filter that matches |
| `crates/tinio-sandbox-macos/tests/startup.rs` | a confined Rust process starts, uses threads and can size its runtime |
| `crates/tinio-sandbox-macos/tests/exposures.rs` | the access rules bite: reads, writes, refusals, deep paths, a symlinked root, and a link out of the tree |
| `crates/tinio-sandbox-macos/tests/network.rs` | no outbound; inbound accept; the settled answer to decision 16's open question |
| `crates/tinio-sandbox-macos/scripts/prune-profile.sh` | the one-rule-at-a-time necessity run that settles the baseline |
| `crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` | the local macOS acceptance run |
| `crates/tinio-config/src/schema/server.rs` | `probe_read_only`: the targeted single-key lookup of decision 18 — **P2's edit**, verified here (Task 8 Step 1) |
| `crates/tinio-cli/src/commands/start/serve.rs` | the serving entry point the macOS launcher path lands in — bind, expose, confine and supervise live in `crates/tinio-cli/src/sandbox/confine.rs` and `crates/tinio-cli/src/commands/start/run.rs` (the `sandbox/macos.rs` this row landed with is gone — 2026-09-27, the seam's split), and the child's adoption with its `Listening` event is this file's. The retired `crates/tinio-server/examples/serve.rs` this row first named is gone |
| `crates/tinio-server/tests/sandbox_macos_e2e.rs` | the confined server serving a request, on the CI macOS leg |
| `docs/tests.md` | the macOS verification section |
| `docs/superpowers/specs/2026-09-18-sandbox-design.md` | the macOS evidence rows, flipped from `(unverified)` with the command that produced each |

---

### Task 1: The SBPL profile as a pure function of the exposure set

**Result, 2026-09-25: recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile"), in its audit and `home` paragraphs.** Do not read this plan's `RUNTIME_BASELINE`/`DEVICE_RULES`/`HOME_GRANT` passages as the result; the section named above is the record.

**Files:**
- Modify: `crates/tinio-sandbox-macos/Cargo.toml`
- Modify: `crates/tinio-sandbox-macos/src/lib.rs`
- Create: `crates/tinio-sandbox-macos/src/profile.rs`
- Read only: `docs/cargo.md` (Task 1 Step 9 verifies the rule bullet P1 already wrote; this plan does not edit it)

**Interfaces:**
- Consumes: `tinio_sandbox_core::{AccessMode, Exposure, Sandbox, Error, Result}` (P0 Task 1).
- Produces: `tinio_sandbox_macos::profile::{profile(&Sandbox) -> Result<Profile>, Profile { text: String, params: Vec<(String, OsString)> }, with_canonical_paths(&Sandbox) -> Sandbox, Allowance { id, rule, evidence }, RUNTIME_BASELINE: &[Allowance], DEVICE_RULES: &[Allowance]}`, and from the crate root the `Backend` the facade selects plus its `backend()` constructor. The `Profile` is one traversal's two halves: the text names each exposure as `(param "E<i>")` and `params` carries the matching values in the same order, because exposed paths travel as `-D` arguments rather than as profile text (decision 24).

This is the whole profile policy. It is pure, so it is testable on the machine this plan is written on — which matters, because everything else in P3 needs a Mac.

- [x] **Step 1: Read the core surface and fix the `use` block**

Run: `cargo doc -p tinio-sandbox-core --no-deps --open` (or read `crates/tinio-sandbox-core/src/{lib,sandbox,access,error}.rs`).

Confirm the names in the "Depends on P0" table above — `Sandbox::exposures`, `Exposure { host, access }`, `AccessMode::is_read_only()` (the type is `tinio-core`'s, re-exported by the core, so `tinio_sandbox_core::AccessMode` and `tinio_core::AccessMode` name the same type and either path works), `Error::BadPath { path, reason }`. Write the `use` block against what is actually there, and adjust the calls in Step 3 to match. Record any rename in the commit body.

- [x] **Step 2: Write the failing tests**

`crates/tinio-sandbox-macos/src/profile.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use tinio_sandbox_core::{AccessMode, Sandbox};

    use super::*;

    #[test]
    fn the_base_is_deny_default() {
        let rendered = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadWrite)).unwrap();
        let mut lines = rendered.text.lines();
        assert_eq!(lines.next(), Some("(version 1)"));
        assert_eq!(lines.next(), Some("(deny default)"));
    }

    #[test]
    fn read_write_grants_the_write_and_read_only_does_not() {
        let rw = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadWrite)).unwrap();
        assert!(rw.text.contains("(allow file-read* file-write* (subpath (param \"E0\")))"), "{}", rw.text);
        assert_eq!(rw.params, vec![(String::from("E0"), OsString::from("/srv/data"))]);

        let ro = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadOnly)).unwrap();
        assert!(ro.text.contains("(allow file-read* (subpath (param \"E0\")))"), "{}", ro.text);
        assert!(!ro.text.contains("file-write* (subpath (param \"E0\"))"), "{}", ro.text);
        // Read-only is the absence of the write keyword, not the absence of the
        // parameter: both access rules name the same exposure the same way.
        assert_eq!(ro.params, vec![(String::from("E0"), OsString::from("/srv/data"))]);
    }

    #[test]
    fn the_state_directory_stays_writable_under_a_read_only_root() {
        // Design §2: the state directory is read-write on every platform, even
        // when the served root is not. The index is exposure order — the served
        // root is `E0`, the state directory `E1` — so this also holds the two
        // halves in step: a text that referenced `E1` while `params[1]` was
        // something else would grant a path nobody asked for.
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadOnly)
            .expose("/home/u/.tinio/roots/h", AccessMode::ReadWrite);
        let rendered = profile(&sandbox).unwrap();
        assert!(rendered.text.contains("(allow file-read* file-write* (subpath (param \"E1\")))"), "{}", rendered.text);
        assert_eq!(
            rendered.params,
            vec![
                (String::from("E0"), OsString::from("/srv/data")),
                (String::from("E1"), OsString::from("/home/u/.tinio/roots/h")),
            ]
        );
    }

    #[test]
    fn sysctl_read_is_always_present() {
        // Load-bearing, not defensive: `std::thread::available_parallelism`
        // reads hw.ncpu through sysctl, and the multi-threaded tokio runtime
        // cannot size itself without it (design §5).
        let rendered = profile(&Sandbox::new()).unwrap();
        assert!(rendered.text.lines().any(|l| l.starts_with("(allow sysctl-read)")), "{}", rendered.text);
        assert!(rendered.params.is_empty(), "{:?}", rendered.params);
    }

    #[test]
    fn the_profile_grants_no_network_operation_at_all() {
        // Measured, not assumed: `accept` on a listener the parent bound and
        // handed in is not gated by the sandbox at all, so the profile needs no
        // `network-inbound` either (design §5, §7). A profile carrying zero
        // network rules was observed taking a real connection out of a
        // handed-in listener's backlog.
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadWrite)
            .device_files();
        let rendered = profile(&sandbox).unwrap();
        for op in ["network-outbound", "network-bind", "network-inbound", "network*"] {
            assert!(!rendered.text.contains(op), "{op} appears in {}", rendered.text);
        }
        // The parameters are separate argv data and are never parsed as rules,
        // so no exposure — however its path is spelled — can widen this surface;
        // `the_profile_text_carries_no_path` is the measurement of that.
    }

    #[test]
    fn the_profile_text_carries_no_path() {
        // Exposed paths travel as `-D` parameters (decision 24), so the text is
        // a constant and no path can reach it — which is what removes escaping
        // and injection from the builder entirely.
        //
        // Measured on macOS 15.8, 2026-09-19: a parameter value that closes the
        // literal and opens a rule — exactly the `hostile` path below, which is
        // rendered as a parameter rather than parsed as text — compiles and
        // grants nothing, and the operation it names stays denied. A newline, a
        // bare quote and a trailing backslash were measured the same way, and a
        // directory whose name genuinely contains `"` reads correctly through
        // `(param …)`. The failure mode is at the other end: a parameter the
        // profile references but the launch does not define is a compile error
        // (`invalid data type of path filter; expected pattern, got boolean`,
        // exit 65), and an empty value is one too (`empty subpath pattern`, also
        // exit 65) — so a missing or empty exposure fails loudly at startup
        // rather than silently granting nothing.
        let hostile = PathBuf::from("/srv/x\") (allow network-outbound) (subpath \"/");
        let sandbox = Sandbox::new()
            .expose(&hostile, AccessMode::ReadWrite)
            .expose("/home/u/data", AccessMode::ReadOnly);
        let rendered = profile(&sandbox).unwrap();
        assert!(!rendered.text.contains("/srv"), "{}", rendered.text);
        assert!(!rendered.text.contains("/home/u"), "{}", rendered.text);
        assert!(!rendered.text.contains("network-outbound"), "{}", rendered.text);
        assert!(rendered.text.contains("(param \"E0\")"), "{}", rendered.text);
        assert!(rendered.text.contains("(param \"E1\")"), "{}", rendered.text);
        // The hostile value is carried verbatim — it is data, and nothing here
        // needs to escape it, quote it or refuse it.
        assert_eq!(
            rendered.params,
            vec![
                (String::from("E0"), hostile.as_os_str().to_os_string()),
                (String::from("E1"), OsString::from("/home/u/data")),
            ]
        );
    }

    #[test]
    fn an_exposure_set_past_the_profile_limit_is_refused() {
        // The compiled profile is capped near 64 KiB: measured on macOS 15.8,
        // 10000 small `allow` operations compile and 12000 fail with
        // `data object length 72301 exceeds maximum (65535)`. Refuse rather
        // than truncate — a profile that silently dropped exposures would
        // grant less than the operator asked for, and the server would serve a
        // subset of its data with no explanation.
        let mut sandbox = Sandbox::new();
        for index in 0..=MAX_EXPOSURES {
            sandbox = sandbox.expose(format!("/srv/data/{index}"), AccessMode::ReadOnly);
        }

        let err = profile(&sandbox).unwrap_err();
        // The exposure the error names is the first one the profile cannot
        // express — the 4097th — not the first of the set.
        let Error::BadPath { path, reason } = err else { panic!("expected BadPath, got {err:?}") };
        assert_eq!(reason, "exposure set exceeds the SBPL profile limit");
        assert_eq!(path, PathBuf::from(format!("/srv/data/{MAX_EXPOSURES}")));
    }

    #[test]
    fn a_relative_path_is_refused() {
        let err = profile(&Sandbox::new().expose("relative/path", AccessMode::ReadOnly)).unwrap_err();
        assert!(matches!(err, Error::BadPath { reason: "not absolute", .. }), "{err:?}");
    }

    #[test]
    fn a_non_utf8_path_is_refused_rather_than_mangled() {
        // A lossy conversion would name a *different* path than the one the
        // operator exposed, which is worse than refusing to start.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let raw = std::ffi::OsStr::from_bytes(b"/srv/\xff\xfe");
            let err = profile(&Sandbox::new().expose(raw, AccessMode::ReadOnly)).unwrap_err();
            assert!(matches!(err, Error::BadPath { reason: "not valid UTF-8", .. }), "{err:?}");
        }
    }

    #[test]
    fn control_characters_are_refused() {
        let err = profile(&Sandbox::new().expose("/srv/a\nb", AccessMode::ReadOnly)).unwrap_err();
        assert!(matches!(err, Error::BadPath { reason: "control character", .. }), "{err:?}");
    }

    #[test]
    fn device_files_render_as_literal_rules() {
        // The device list is a constant, not an exposure: its paths are written
        // into the text as literals, and no parameter is emitted for them.
        let rendered = profile(&Sandbox::new().device_files()).unwrap();
        assert!(rendered.params.is_empty(), "{:?}", rendered.params);
        for device in ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"] {
            assert!(rendered.text.contains(&format!("(literal \"{device}\")")), "{device} missing from:\n{}", rendered.text);
        }
    }

    #[test]
    fn canonical_paths_are_added_when_they_differ() {
        // macOS resolves /tmp to /private/tmp and $TMPDIR to
        // /private/var/folders/…; a rule naming only the un-resolved path
        // would match nothing (the empirical check is Task 6's symlinked root).
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let real = dir.path().join("real");
            std::fs::create_dir(&real).unwrap();
            let link = dir.path().join("link");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            let canonical_real = std::fs::canonicalize(&real).unwrap();

            let sandbox = with_canonical_paths(&Sandbox::new().expose(&link, AccessMode::ReadWrite));
            let hosts: Vec<_> = sandbox.exposures().iter().map(|e| e.host.clone()).collect();
            assert!(hosts.contains(&link), "{hosts:?}");
            assert!(hosts.contains(&canonical_real), "{hosts:?}");

            // The rewrite is the module's whole point, so this test expects it
            // rather than working around it: on macOS `tempfile::tempdir()` lives
            // under `$TMPDIR` = `/var/folders/…`, which `canonicalize` rewrites to
            // `/private/var/folders/…`, so the canonical form of a tempdir path is
            // always a *second* path and the asserted length of this case is 2.
            // An earlier version asserted 1 and could only pass on a host without
            // that rewrite.
            let rewritten = with_canonical_paths(&Sandbox::new().expose(&real, AccessMode::ReadWrite));
            assert!(
                rewritten.exposures().iter().any(|e| e.host == canonical_real),
                "{:?}",
                rewritten.exposures()
            );
            #[cfg(target_os = "macos")]
            assert_eq!(rewritten.exposures().len(), 2, "{:?}", rewritten.exposures());

            // A path that already is its canonical form is not duplicated — which
            // is a claim only a path this test canonicalised itself can support.
            let plain = with_canonical_paths(&Sandbox::new().expose(&canonical_real, AccessMode::ReadWrite));
            assert_eq!(plain.exposures().len(), 1, "{:?}", plain.exposures());
        }
    }

    #[test]
    fn canonicalisation_leaves_access_and_device_files_alone() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly).device_files();
        let out = with_canonical_paths(&sandbox);
        assert!(out.device_files_enabled());
        assert!(out.exposures().iter().all(|e| e.access == AccessMode::ReadOnly));
    }
}
```

- [x] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-macos profile::`
Expected: FAIL — `cannot find function 'profile'` (compile error), because the module does not exist yet.

- [x] **Step 4: Write the crate manifest**

`crates/tinio-sandbox-macos/Cargo.toml` in full (P0 Task 3 creates a stub manifest; this is the shape it must end up with):

```toml
[package]
name = "tinio-sandbox-macos"
version.workspace = true
edition.workspace = true
description = "macOS confinement for tinio: a Seatbelt (SBPL) profile applied by sandbox-exec"
publish = false

[dependencies]
serde_json.workspace = true
tinio-sandbox-core.workspace = true
tracing.workspace = true

[dev-dependencies]
tempfile.workspace = true
tracing-subscriber.workspace = true

[target.'cfg(target_os = "macos")'.dependencies]
rustix = { workspace = true, features = ["process"] }
tokio = { workspace = true, features = ["fs", "io-std", "io-util", "rt"] }

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "deny"
```

**`tracing` is the one dependency every target of this crate takes, and `tracing-subscriber` is dev-only.** Shared §7 requires every core operation of this feature to emit a `tracing` event, with a level split that is not cosmetic, and these are this crate's operations:

| Operation | Level | Where |
|---|---|---|
| the rendered profile's rule and parameter counts | `info!` | `profile::profile`, and again in `confine` for the profile the child will actually run under |
| the pre-flight and its outcome | `info!` on acceptance, `info!` with `verdict="skipped"` when it does not run (a release build, or a level below `debug`), `error!` on refusal or when its deadline expires | `launch::preflight` |
| the spawn, with the child's pid and the listener descriptor | `info!` | `confine`, immediately after the spawn |
| the close-on-exec change | `debug!` | `launch::make_inheritable` |
| the payload write, with its size | `debug!` | `launch::hand_over` |
| the listener rebuild, with the descriptor number — the `unsafe` step is decision 14's `launch::inherited_fd`, which this and `launch::control` call | `debug!` | `launch::listener` |
| each relayed signal, with its number | `debug!` | `relay::start_relaying` (Task 3) |
| each arm and disarm of the relay | `debug!` | `relay::on_stop` / `relay::disarm` (Task 3) |
| the exit, with the status forwarded | `info!` | `serve`'s launcher arm (Task 8) |

The split follows what `serve` actually enables rather than taste: it builds its subscriber at a fixed `Verbosity::Info` with no environment filter (`crates/tinio-server/src/log.rs` — `build_subscriber` takes a `Verbosity`), so anything below `info!` is not printed today at all. The `info!` half is therefore the record an operator can read *now* — "was this process confined, and to what" — and the `debug!` half is the detail behind it, which becomes visible when the `[log]` wiring lands with the CLI. **One consequence has to be paid for here rather than noted**: the launcher's events are emitted in `main`, before the process that serves builds its subscriber, so Task 8's macOS arm installs a stderr subscriber at `Info` for the launch window and says why there.

**The crate itself installs no subscriber** (shared §7): the process that owns the runtime decides where events go, and in `serve` that is stderr — which decision 19 requires, since stdout carries the event protocol. Nothing here logs a formatted sentence either: every event carries its operation as a structured field.

`tracing` and `tracing-subscriber` are both already in `[workspace.dependencies]`, so the workspace gains no third-party crate; `tracing-subscriber` is a dev-dependency because Task 2's test asserts an event's `step` under a recording subscriber, and nothing in this crate's own code installs a subscriber.

**`tokio` is a macOS-target dependency, and all four of its features are load-bearing.** The core's `Observer` is async over `tokio::io` traits, so `io-util` is what this crate talks to it with; `io-std` is what makes `tokio::io::stdout()` an `AsyncWrite` at all (`Stdout` and `Stderr` need that feature); **`fs` is what turns the child's blocking stdout pipe into an `AsyncRead`** — Task 2's relay builds its reader with `File::from(OwnedFd::from(stdout))`, and without the feature `tokio::fs::File` does not exist (`crates/tinio-fs/Cargo.toml` names the same feature for the same reason); and `rt` is what lets the relay thread build a current-thread runtime to poll it — the launcher is `std::process`-based and has no runtime of its own, and growing a multi-threaded one for one relay would be the wrong size. Target-gated because `launch.rs` and `relay.rs` are the only users and both are macOS-only; the stub arm for other targets links neither.

`rustix` is target-gated because only `launch.rs` and `relay.rs` use it, and only on macOS. Its one feature is set here rather than at the workspace, as `docs/cargo.md` requires: `process`, for `kill_process` and the `Pid`/`Signal` types (Task 3). There is **no `rand` here**: the payload is one JSON object owned by the core, and the nonce that once wanted an entropy source is gone (design decision 17, amended). `rustix::io::fcntl_setfd` needs no feature — it ships in the crate's default surface, and it is `#[cfg(not(windows))]`, which is why every call site is macOS-gated. Task 3 adds one more line to the same block (`signal-hook`). No other dependency: the profile generator is text composition, and the launch is `std::process`.

- [x] **Step 5: Write `profile.rs`**

```rust
//! The Seatbelt (SBPL) profile, as a pure function of the exposure set.
//!
//! A profile is text: a `(version 1)` header, a `(deny default)` base, and one
//! `(allow …)` per path the confined process may reach. The text is handed to
//! `sandbox-exec -p` as a single argv element (design decision 24), so nothing
//! here touches the filesystem — the caller resolves paths first
//! ([`with_canonical_paths`]) and this module only renders.
//!
//! The paths themselves are **not** in that text: each exposure renders as a
//! `(param "E<i>")` reference and its value travels beside the profile as a `-D`
//! argv parameter, which is why [`profile`] returns a [`Profile`] rather than a
//! `String`. A parameter value is data — it is never parsed as SBPL — so there is
//! nothing to escape here and no path can open a rule of its own. The one path
//! that is not an exposure travels the same way: the `process-exec` rule's
//! `(literal (param "P"))` names the program the launch execs, and `command_args`
//! resolves `P` from the same `program` argument it writes as the argv's program
//! element.
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. The profile format
//! is undocumented and unversioned: every allowance in [`RUNTIME_BASELINE`]
//! carries the observation that keeps it here, and none of them may be kept
//! "just in case".

use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::Path;

use tinio_sandbox_core::{AccessMode, Error, Exposure, Result, Sandbox};
use tracing::{error, info};

/// One allowance in the profile, with the evidence that justifies it.
///
/// `id` is stable and unique: it appears in the generated text as a trailing
/// `; tinio:<id>` comment, so the pruning run (`scripts/prune-profile.sh`) can
/// remove exactly one rule at a time and the shipped profile says where each
/// rule came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Allowance {
    /// Stable identifier, unique across the profile.
    pub id: &'static str,
    /// The SBPL rule, without its provenance comment.
    pub rule: &'static str,
    /// What needs it. `"candidate"` means the design predicted it and no Mac
    /// has confirmed it yet — Task 5 replaces every one of those with the
    /// observation, or deletes the rule.
    pub evidence: &'static str,
}

/// The allowances a `(deny default)` profile needs before a Rust process can
/// start at all.
///
/// **Measured on macOS 15.8, not predicted.** This is the table the profile of
/// design §5 was built from, and the four entries marked `required` were each
/// established by removing the rule and watching what broke — none of them is
/// a guess, and none may be pruned by Task 5, because each fails in a way that
/// produces no usable diagnostic: a bare SIGABRT, an exit 71 before any rule is
/// consulted, a silent `-1` from `sysconf`, or a refusal to traverse a symlink.
/// **Task 5 prunes every other entry, and its run is required rather than a
/// follow-up**: an entry that survives without an observation naming what broke
/// without it is deleted, and the two entries that arrived from §5's measured
/// text without a removal measurement of their own say so in their `evidence`.
///
/// **The system read set is narrowed to what this crate needs, and the narrowing
/// is the point of the table.** The design's Goal is that the confined process
/// reaches "exactly the directories the operator exposed and nothing else"; §5's
/// measured text was the first version that ran, not the narrowest one that
/// could, and three of its rules are gone or split here:
///
/// - `(subpath "/private")` is replaced by the two subpaths below. The blanket
///   granted `/private/var`, which the server has no caller for, and it is the
///   one rule §5's own text flagged as reducible.
/// - `(subpath "/etc")` is removed outright. `/etc` is a symlink into `/private`,
///   and Seatbelt compares the rule's literal spelling against the kernel-resolved
///   path (design §5), so a rule spelled `/etc` matched nothing — the access it
///   was meant to grant came from the `/private` blanket, and it now comes from
///   `/private/etc`.
/// - `(subpath "/dev")` is removed and the four device literals carry the grant
///   instead: the blanket's read permission added nothing the literals do not
///   already say, and it reached every device node on the host.
///
/// Two allowances the design previously predicted are **absent**, because
/// measurement removed them: `mach-lookup` (with it gone every check passes
/// except `getpwuid`, whose failure names
/// `com.apple.system.opendirectoryd.libinfo`, and the serve path resolves no
/// users) and a `mach-lookup` for `com.apple.system.logger`, which was invented
/// rather than observed. Re-adding either requires an observation, not a
/// citation.
///
/// There is deliberately **no network allowance of any kind**, including no
/// `network-inbound`: `accept` on an inherited listener is not gated by the
/// sandbox at all, which was measured with a profile carrying zero network
/// rules taking a real connection out of a handed-in listener's backlog
/// (design §5, §7). See `tests/network.rs` for the assertion that holds this.
///
/// **`process-exec` is narrowed to the one exec that has to succeed.** The
/// measurement established that *an* exec must be permitted at all — without the
/// rule `sandbox-exec` exits 71 before any other rule is consulted — and it never
/// established that *every* binary must be. Unfiltered, and with `/bin`, `/usr`
/// and `/private` readable, the rule lets the confined child exec anything it can
/// read — `/bin/sh`, `/usr/bin/ssh`, `launchctl` — and that is the confined
/// process's own reach rather than a local actor's: the threat model rules out
/// the actor outside the sandbox, not what the process inside it may do, so the
/// rule is this table's to narrow.
/// The rule therefore carries one `(literal …)`: the child's own path, which is
/// the launcher's `current_exe()` — the same value its argv's program element
/// carries — travelling as the parameter `P` for the reason decision 24 gives, so
/// the path is data and the text stays a constant. **Being wrong costs a refusal
/// rather than a widening** — an exec the literal does not match fails as `rc=71`
/// with `execvp() of '<path>' failed: Operation not permitted` — and a *second*
/// exec the launch turns out to need is something no measurement covers: the
/// answer to that is the unfiltered rule **with the over-grant and its follow-up
/// written into the entry's `evidence`**, never the broad rule kept quietly.
pub const RUNTIME_BASELINE: &[Allowance] = &[
    Allowance {
        id: "process-exec",
        rule: "(allow process-exec (literal (param \"P\")))",
        evidence: "required: without it sandbox-exec cannot exec the target at all and exits 71 before any other rule is consulted. The code alone is not the discriminator — a missing target produces the same 71 — so the refusal is what stderr says: `execvp() of '<path>' failed: Operation not permitted`. The literal is the one exec that must succeed: `P` is the launcher's `current_exe()`, the same value `command_args` writes as the argv's program element, so the rule's filter and the exec `sandbox-exec` performs are one value and cannot disagree. The pre-flight binds the same parameter to `/bin/echo`, which is the exec that probe performs. If the literal is wrong the failure is the refusal above — loud, not a silent widening — and a second exec the launch needs is not covered by any measurement this phase has, so the answer is the unfiltered rule with the over-grant and its follow-up written here, never the broad rule kept quietly",
    },
    Allowance {
        id: "process-fork",
        rule: "(allow process-fork)",
        evidence: "carried by design §5's measured text (macOS 15.8, the profile that served a real request) and never measured on its own. Kept so a rule the measured text had cannot disappear silently, and Task 5 removes it unless the probe fails without it",
    },
    Allowance {
        id: "signal-self",
        rule: "(allow signal (target self))",
        evidence: "carried by design §5's measured text. The one measurement that touches the `signal` operation shows `kill(child, SIGTERM)` succeeding with no `signal` rule at all and under `(target self)`, so this rule's own necessity is unmeasured; Task 5 decides it by removal and rewrites this field with what the removal showed",
    },
    Allowance {
        id: "sysctl",
        rule: "(allow sysctl-read)",
        evidence: "required: without it sysconf(_SC_NPROCESSORS_ONLN) returns -1, so std::thread::available_parallelism fails and the tokio runtime cannot size its pool",
    },
    Allowance {
        id: "metadata",
        rule: "(allow file-read-metadata)",
        evidence: "required: /tmp, /var and /etc are symlinks at the root, and a grant on the target does not permit reaching it through the link — `(subpath \"/private/tmp\")` and even the literal full path are refused without this. Cost: existence and timestamps can be probed outside the exposures, contents cannot",
    },
    Allowance {
        id: "root-traversal",
        rule: "(allow file-read* (literal \"/\"))",
        evidence: "required: without it the process dies of SIGABRT and the only violation logged is `file-read-data /`. Grants on prefixes under the root do not substitute — the refused read is of the root directory itself",
    },
    Allowance {
        id: "system",
        rule: "(allow file-read* (subpath \"/System\") (subpath \"/usr\") (subpath \"/Library\"))",
        evidence: "measured working set: a dynamically-linked binary runs with exactly /usr, /System, /Library, /bin, /sbin and `(literal \"/\")` granted (design §12), and dyld's shared cache on macOS 15 lives under /System/Volumes/Preboot/Cryptexes, which /System already covers. Task 5 prunes each subpath against the real server",
    },
    Allowance {
        id: "bin",
        rule: "(allow file-read* (subpath \"/bin\") (subpath \"/sbin\"))",
        evidence: "measured working set: the binaries this launcher execs — `/bin/echo` in the pre-flight, and the process image the confined child re-execs. `/etc` is no longer listed here; it is a symlink into /private, and a rule spelled through a link matches nothing (design §5), so the access it meant to grant is `private-etc` below",
    },
    Allowance {
        id: "private-etc",
        rule: "(allow file-read* (subpath \"/private/etc\"))",
        evidence: "the passwd, group and hosts files libc reads on demand, reached through the /etc symlink and therefore written on the resolved path. §5's measured text covered it with the whole of /private and never named it; this is the narrower spelling of the same grant, and Task 5 keeps it only if the probe fails without it",
    },
    Allowance {
        id: "private-var-folders",
        rule: "(allow file-read* (subpath \"/private/var/folders\"))",
        evidence: "`$TMPDIR` resolves here, and it is the only part of /private/var the server plausibly needs — a temporary file, a lock, a runtime's spill. Note for Task 5: if the removal run shows a *write* under $TMPDIR being refused, the rule gains `file-write*` on the same subpath and the observation names the file that needed it; a rule that fails nothing without it is deleted",
    },
];

/// The device files, as literal rules, plus the two standard streams §5's
/// profile writes by path.
///
/// [`Sandbox::device_files`] means the same thing on every platform — "these
/// four must stay reachable" — and each platform says it in its own mechanism:
/// Linux binds them into the jail (design §4), macOS allows the host's copies.
/// **These literals are the whole `/dev` grant**: §5's measured text also
/// carried `(subpath "/dev")`, which under the read set would have made every
/// one of these four redundant and reached every device node on the host, so the
/// blanket is gone and the enumeration is what remains. `/dev/stdout` and
/// `/dev/stderr` are here because §5's measured profile grants the writes by
/// path; the descriptors themselves are inherited, so the grant is for a logger
/// that re-opens the name rather than for the stream it already holds.
pub const DEVICE_RULES: &[Allowance] = &[
    Allowance {
        id: "dev-null",
        rule: "(allow file-read* file-write* (literal \"/dev/null\"))",
        evidence: "design §4's device list, with the write grant design §5's measured profile carries — /dev/null is where a process sends what it discards",
    },
    Allowance {
        id: "dev-zero",
        rule: "(allow file-read* (literal \"/dev/zero\"))",
        evidence: "candidate: design §4's device list",
    },
    Allowance {
        id: "dev-random",
        rule: "(allow file-read* (literal \"/dev/random\"))",
        evidence: "candidate: design §4's device list",
    },
    Allowance {
        id: "dev-urandom",
        rule: "(allow file-read* (literal \"/dev/urandom\"))",
        evidence: "candidate: design §4's device list. Design decision 12 records that the four device files are defensive rather than load-bearing — Rust's getrandom is a syscall — so Task 5's removal run is expected to have something to say about all four",
    },
    Allowance {
        id: "dev-stdio",
        rule: "(allow file-write* (literal \"/dev/stdout\") (literal \"/dev/stderr\"))",
        evidence: "design §5's measured profile writes these two by path. Task 5 keeps them only if the probe needs neither; nothing in the crate re-opens either stream by name",
    },
];

/// The SBPL text and the `-D` parameters it references.
///
/// The text contains no path: every exposure is rendered as `(param "E<n>")`,
/// and `params` carries the matching values in the same order. Generating both
/// from one traversal of `sandbox.exposures()` is what keeps them in step — and
/// the one parameter that is **not** an exposure, `P`, the `process-exec` rule's
/// literal, is not generated here at all: it is the launcher's own program,
/// written by Task 2's `command_args` from the same argument it writes as the
/// argv's program element.
///
/// The pairs are `(format!("E{i}"), exposure.host.as_os_str().to_os_string())`
/// for exposure `i` in `sandbox.exposures()` order, and the launch turns each
/// into one `-D NAME=VALUE` argv pair (Task 2's `command_args`).
///
/// # Measured on macOS 15.8, 2026-09-19
///
/// `-D` works with `-f <file>` and with the inline `-p <text>`, and a `-D` may
/// appear on either side of the profile flag.
///
/// A parameter value is **data, not SBPL**. A value that closes the literal and
/// opens a rule — `/srv/x") (allow network-outbound) (subpath "/` — compiles and
/// grants nothing: the outbound `connect` it names stays denied. A newline, a
/// bare quote and a trailing backslash were measured the same way, and a
/// directory whose name genuinely contains `"` reads correctly through
/// `(param …)`. That is why nothing here escapes anything.
///
/// What a parameter cannot do is go missing or be empty, and both fail loudly at
/// startup rather than quietly granting nothing: an **undefined** parameter is a
/// compile error (`sandbox-exec` reports `invalid data type of path filter;
/// expected pattern, got boolean`, exit 65) and an **empty** value is one too
/// (`empty subpath pattern`, exit 65).
#[derive(Debug)]
pub struct Profile {
    /// The profile text.
    pub text: String,
    /// `-D` arguments, in exposure order.
    ///
    /// The text's one **other** `(param …)` reference is not here: `P`, the
    /// `process-exec` rule's literal, is the launcher's own value, and Task 2's
    /// `command_args` writes that pair from the program it is handed — the same
    /// one it writes as the argv's program element.
    pub params: Vec<(String, OsString)>,
}

/// The exposure-count guard, against a limit measured in **bytes**.
///
/// `sandbox-exec` compiles the profile into a data object capped near 64 KiB:
/// measured on macOS 15.8, 10000 small `allow` operations compile and 12000 fail
/// with `data object length 72301 exceeds maximum (65535)`, which is the
/// ~6-bytes-per-operation shape of the rules this generator writes. **The count
/// is a proxy for that byte cap, not the cap itself**, and it holds only while
/// the paths are short: every exposure's rule carries a `(param "E<i>")`
/// reference whose value is substituted at compile time, so the compiled object
/// grows with the *length* of the operator's paths as much as with their number —
/// 4096 exposures of `PATH_MAX`-sized paths are orders of magnitude past the cap,
/// while a few dozen ordinary ones are not. The assumption this number rests on
/// is therefore stated rather than implied: an operator's exposure set is a few
/// dozen paths of ordinary length, and 4096 sits 2.4× below the measured cliff.
/// The guard is deliberately not re-derived per path length, because it is not
/// the only one: a profile that overshoots the byte cap fails `sandbox-exec`'s own
/// compile step, and `preflight` (Task 2) turns that into a startup error carrying
/// the binary's diagnostic, so the count guard is what makes the common case cheap
/// and legible rather than the only thing between an operator and a silent grant.
const MAX_EXPOSURES: usize = 4096;

/// Render the profile for `sandbox`.
///
/// The order is stable — header, exposures, device files, runtime baseline —
/// so a diff of two profiles reads as a diff of two exposure sets. One traversal
/// produces both halves: exposure `i` renders as
/// `(allow file-read* (subpath (param "E<i>")))`, or with `file-write*` before
/// the `(subpath …)` when it is writable, and appends `("E<i>", host)` to
/// [`Profile::params`] as it goes.
///
/// # Errors
///
/// [`Error::BadPath`] for an exposure that cannot be a parameter: not absolute,
/// not valid UTF-8, or carrying a control character — and for an exposure set
/// past `MAX_EXPOSURES`, where the error names the first exposure the profile
/// cannot express.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::profile::profile;
///
/// let rendered = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadOnly)).unwrap();
/// assert!(rendered.text.starts_with("(version 1)\n(deny default)\n"));
/// assert!(rendered.text.contains("(allow file-read* (subpath (param \"E0\")))"));
/// assert!(!rendered.text.contains("file-write* (subpath (param \"E0\"))"));
/// assert_eq!(rendered.params[0], (String::from("E0"), OsString::from("/srv/data")));
/// ```
pub fn profile(sandbox: &Sandbox) -> Result<Profile> {
    if sandbox.exposures().len() > MAX_EXPOSURES {
        let over = &sandbox.exposures()[MAX_EXPOSURES];
        return Err(refuse(&over.host, "exposure set exceeds the SBPL profile limit"));
    }

    let mut out = String::from("(version 1)\n(deny default)\n");
    let mut params = Vec::with_capacity(sandbox.exposures().len());
    let mut rules = 0usize;

    for (index, exposure) in sandbox.exposures().iter().enumerate() {
        validate_path(&exposure.host)?;
        // The value never reaches `out`: the text names the parameter and the
        // value is carried beside it, so a path cannot close a literal or start
        // a rule whatever it contains.
        let param = format!("E{index}");
        let rule = match exposure.access {
            AccessMode::ReadOnly => format!("(allow file-read* (subpath (param \"{param}\")))"),
            AccessMode::ReadWrite => {
                format!("(allow file-read* file-write* (subpath (param \"{param}\")))")
            }
        };
        push(&mut out, "exposure", &rule, "the operator's exposure set (design §2)");
        params.push((param, exposure.host.as_os_str().to_os_string()));
        rules += 1;
    }

    if sandbox.device_files_enabled() {
        for allowance in DEVICE_RULES {
            push(&mut out, allowance.id, allowance.rule, allowance.evidence);
            rules += 1;
        }
    }

    for allowance in RUNTIME_BASELINE {
        push(&mut out, allowance.id, allowance.rule, allowance.evidence);
        rules += 1;
    }

    // No network rule is emitted here or anywhere: `accept` on the inherited
    // listener is not gated by the sandbox (design §5, §7), so the profile's
    // network surface is empty and there is no conditional to get wrong.

    // One event per operation, carrying the shape of the profile rather than
    // its text (shared §7): the counts are what an operator compares across two
    // runs, and the text is already in the process list and in the error
    // messages that carry it. `info!` and not `debug!` because this is the
    // answer to "what was this process confined to" — and because `serve`'s
    // subscriber is a fixed `Verbosity::Info`, so a `debug!` here would be a
    // record nobody could read.
    info!(step = "profile", rules, params = params.len(), "the SBPL profile is rendered");

    Ok(Profile { text: out, params })
}

/// Add the canonical form of every exposure whose path resolves elsewhere.
///
/// macOS resolves `/tmp` to `/private/tmp` and `$TMPDIR` to
/// `/private/var/folders/…`, and Seatbelt matches the path the kernel knows.
/// Emitting both forms is deliberate belt-and-braces: the given path is what
/// the operator wrote (and what the process will use), the canonical path is
/// what the kernel enforces against. Task 6's symlinked-root case measures
/// whether the given form is still needed, and Task 5 prunes it if it is not.
///
/// **Canonicalisation stops at the exposed path, and a link out of the tree is
/// not followed.** What is resolved is the path the operator named — including
/// the symlinked components of its own prefix, which is what makes `/tmp/x` and
/// `$TMPDIR/x` match — and nothing is added for a link that lives *inside* the
/// exposed tree: such a link resolves, at read time, to a path no rule names, so
/// the kernel refuses it. That is the settled behaviour rather than an omission:
/// the exposure set is exactly the set of paths the operator wrote, plus their
/// resolved spellings, and a link is never a way to widen it. Task 6 measures
/// both halves — the symlinked root reads, and a link out of the tree does not.
///
/// A path that cannot be canonicalised (it does not exist yet) is left alone —
/// the caller creates the mount sources before confining (design §10 step 5).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::profile::with_canonical_paths;
///
/// let out = with_canonical_paths(&Sandbox::new().expose("/srv/data", AccessMode::ReadOnly));
/// assert_eq!(out.exposures().len(), 1);
/// ```
pub fn with_canonical_paths(sandbox: &Sandbox) -> Sandbox {
    let mut exposures = sandbox.exposures().to_vec();
    for exposure in sandbox.exposures() {
        let Ok(canonical) = std::fs::canonicalize(&exposure.host) else { continue };
        // Deduplicated here rather than by `expose`: `from_exposures` takes the
        // set as given, so adding a canonical form that is already present (or
        // that another exposure canonicalises to) would otherwise double the
        // rule.
        let already = exposures.iter().any(|existing| existing.host == canonical);
        if canonical != exposure.host && !already {
            exposures.push(Exposure { host: canonical, access: exposure.access });
        }
    }

    // The flag travels as an argument rather than as state the rebuilt set
    // carries, so there is nothing to re-apply afterwards.
    Sandbox::from_exposures(exposures, sandbox.device_files_enabled())
}

/// One rule line, with its provenance marker.
fn push(out: &mut String, id: &str, rule: &str, evidence: &str) {
    let _ = writeln!(out, "{rule} ; tinio:{id} — {evidence}");
}

/// Refuse an exposure, logging where the `Error` is built.
///
/// Shared §7's rule for a failure: the event and the error carry the same
/// values, so a refusal that has no `step` to name is still diagnosable from the
/// log — and the reason string is written once, used by both, and cannot drift.
fn refuse(path: &Path, reason: &'static str) -> Error {
    error!(path = %path.display(), reason, "the exposure cannot be expressed in the profile");
    Error::BadPath { path: path.to_path_buf(), reason }
}

/// The validation a parameter value must pass, or an error if it cannot be one.
///
/// **This no longer escapes anything.** Escaping existed because a path was
/// written into the profile text; now the text carries `(param "E0")` and the
/// path travels as a `-D` value, so a quote, a backslash or a newline in a path
/// is data that reaches the kernel as the path the operator named (measured).
/// What is kept is the crate's pre-existing path policy — absolute, valid UTF-8,
/// no control character — because relaxing a check whose failure mode has not
/// been re-measured is a separate decision, not a consequence of this one.
fn validate_path(path: &Path) -> Result<()> {
    let Some(raw) = path.to_str() else {
        return Err(refuse(path, "not valid UTF-8"));
    };
    if !path.is_absolute() {
        return Err(refuse(path, "not absolute"));
    }
    if raw.chars().any(char::is_control) {
        return Err(refuse(path, "control character"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    // ... as written in Step 2
}
```

Note what `profile` does **not** do: it never emits `network-outbound`, `network-bind`, a `(deny …)` for anything, or a rule derived from a hostname, a user name or an environment variable. The only inputs are the exposure set and the two constant tables — and the exposure set reaches the text only as `(param "E<i>")` references, so no operator path is ever a literal in it.

**One guard belongs here, and its bound comes from a measurement rather than from taste.** `sandbox-exec` compiles the profile into a data object capped near 64 KiB, and the failure is not graceful: measured on macOS 15.8, 10000 small `allow` operations compile and **12000 fail** with `data object length 72301 exceeds maximum (65535)`, while a *single* operation carrying 9000 path literals fails earlier still with `profile compilation failed`. Design §5 therefore requires the generator to refuse rather than let `sandbox-exec` report it, so `profile` checks `sandbox.exposures().len()` before rendering and returns an error above **4096** exposures — 2.4× below the measured cliff, and far past any exposure set an operator would write. Refuse, do not truncate: a profile that silently drops exposures is a confinement that silently grants less than the operator asked for, and the server would then serve a subset of its data with no explanation.

**The error that says so is `Error::BadPath`, and it needs no core change.** An earlier draft of this plan argued that none of the core's four variants fitted and reached for `Error::Step` as the least-bad fit; that was wrong, and the variant's own doc comment is the argument — `BadPath` is "A path handed to [`Sandbox::expose`] cannot be expressed inside the confinement", and the 4097th exposure is exactly that: a path the operator exposed which the profile cannot carry. It already holds any `&'static str` reason, so this is a call site rather than an addition to the core's surface, and the dedicated `TooManyExposures { count, limit }` variant an earlier draft proposed would buy nothing this does not already say.

The guard itself is the one already written at the top of `profile` in Step 5 — it checks `sandbox.exposures().len()` before a single line is rendered, so the refusal costs nothing on the profiles that pass — and the byte cap it stands in for is stated there, in the `MAX_EXPOSURES` doc comment, along with the path-length assumption the number rests on and the pre-flight that catches what the count cannot. It is written once, in the code.

`an_exposure_set_past_the_profile_limit_is_refused` (Step 2) is the test that holds it, and it asserts both halves: the reason, and that the path named is the 4097th exposure — the first the profile cannot express — rather than the first of the set.

- [x] **Step 6: Write `lib.rs`**

```rust
//! macOS confinement for tinio: a Seatbelt (SBPL) profile applied by
//! `sandbox-exec -p`.
//!
//! `sandbox-exec` applies a profile to a process it **execs**, so macOS
//! confines a child: the parent keeps the listener, hands it over with the
//! launch payload and supervises (design §3, decision 6). The profile is
//! [`profile::profile`], a pure function of the exposure set: one
//! [`profile::Profile`] holding the text — which names every exposure as
//! `(param "E<i>")` — and the `-D` values those references resolve to.
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. It is not a
//! supported or versioned API, and nothing here treats it as one: when Apple
//! removes it the macOS path degrades to "no sandbox available, fail closed",
//! which is the error the backend returns when the binary is missing
//! (design, Risks).
//!
//! # Logging
//!
//! This crate emits `tracing` events and installs no subscriber (shared §7):
//! the process that owns the runtime decides where they go, and in `serve` that
//! is a stderr subscriber. stdout carries the event protocol and nothing else
//! (decision 19), so nothing in this crate writes a log line to it.
//!
//! The levels follow shared §7's split, which exists because `serve`'s
//! subscriber is a fixed `Verbosity::Info`: `info!` for what a confinement was —
//! the rendered profile's rule and parameter counts, the pre-flight and its
//! outcome, the spawn with the child's pid, and the exit — and `debug!` for the
//! detail behind it, which is not printed until the `[log]` wiring lands. A
//! failure is an `error!` emitted where its `Error` is built, so the log's
//! `step` and the error's are the same string.
//!
//! # Examples
//!
//! ```
//! use std::ffi::OsString;
//!
//! use tinio_sandbox_core::{AccessMode, Sandbox};
//! use tinio_sandbox_macos::profile::profile;
//!
//! let rendered = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadWrite)).unwrap();
//! assert!(rendered.text.contains("(allow file-read* file-write* (subpath (param \"E0\")))"));
//! assert_eq!(rendered.params[0], (String::from("E0"), OsString::from("/srv/data")));
//! ```

use tinio_sandbox_core::{Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox};

pub mod profile;

/// The macOS backend — the type the facade's `platform()` selects.
///
/// P0 ships this stub, and this task **keeps both halves of it**: the struct and
/// the `backend()` constructor the facade calls. Losing either would leave
/// `serve --sandbox` unable to leave `Unsupported` on macOS no matter what the
/// profile module does, because there would be no type for `platform()` to hand
/// back. The body is P0's text minus its crate-root `#![cfg]` (this crate takes
/// shared §2's second shape and compiles on every target), and **Task 2 replaces
/// the body** with the delegation to `launch::confine` — module declaration and
/// call site landing in the same task, so every intermediate state compiles.
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

**`Backend` and `backend()` are this crate's contract with the facade**, and both are in this step on purpose: `crates/tinio-sandbox/src/lib.rs`'s `platform()` calls `backend()`, and `tinio-sandbox`'s own test asserts that a launcher-shaped platform is selected on macOS — a `lib.rs` that kept only the module declarations would delete the type that test and the facade both name. The trait is imported as `Backend as BackendTrait` because the type and the trait share a name; that is P0's own spelling at the same site.

`pub mod profile;` is the declaration this task needs, and **`launch` and `relay` arrive with the tasks that write them** — Task 2 adds `pub mod launch;` and the `pub use self::launch::{SANDBOX_EXEC, command_args, confine};` line beside it, Task 3 adds `pub mod relay;`. Declaring a module whose file does not exist yet would leave this task's own verification step (Step 7) with a `cargo test` that cannot compile.

Both modules are public, and that is deliberate rather than incidental: `launch::listener` is called by the confined *child*, which is a test binary and can therefore only reach the public surface, so `mod launch` would make the harness of Task 4 uncompilable. `command_args` is re-exported at the root because the tests in Task 2 assert on the argv it builds without wanting to name the module; `listener` is not, because its only callers write the full path.

`SANDBOX_EXEC` is exported because the pruning experiment (`tests/harness/mod.rs`) launches `sandbox-exec` directly; it is a constant, not a second code path.

- [x] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-macos`
Expected: PASS — the whole `profile` module, 0 failures, on any host. No count is pinned here: the module gains a test whenever a rule gains a reason. Run it on the authoring host first; if the profile generator does not pass here, nothing downstream can.

- [x] **Step 8: Verify the workspace still builds and lints**

Run: `cargo check --workspace --all-targets && cargo clippy --workspace --all-targets -- -D warnings`
Expected: clean. `cargo check` builds this member on Linux and Windows too, which is why the crate takes shared §2's second shape: `lib.rs`'s `Backend` and `profile.rs` both compile everywhere, and the macOS-only code arrives in Task 2 behind its gates. The `tracing` dependency added in Step 4 is not a macOS-only one — and it could not be: `profile.rs` emits on every target, and the crate's other must be gated per import rather than by target-dependency, which Task 2 does.

- [x] **Step 9: Verify the lint rule was already updated, and do not rewrite it**

`docs/cargo.md` was edited by **P1**, not here: its Task 9 Step 4 writes the `unsafe_code` rule bullet once, naming all three backends and saying each becomes `"deny"` as its backend lands. `2026-09-18-sandbox-shared.md` §1 records why the line lives there rather than in each plan.

This plan used to carry its own version of the same bullet, worded differently, and this file's version and P1's disagreed about which backends were excepted.

What to do here is check, before the phase's commit (see [Commit](#commit)):

```bash
rg -n 'unsafe_code' docs/cargo.md
```

Expected: the rule bullet names the platform backends, says each becomes `deny` as its backend lands, and points at decision 14; the second occurrence — the `[lints.rust]` block near line 71, inside the worked `tinio-mem` example manifest — still reads `forbid`, because `tinio-mem` is not a platform backend, the exception does not apply to it, and an example showing `deny` would read as licence to relax the lint anywhere. If the rule bullet does **not** name the backends, P1 has not landed: stop and say so rather than writing the line here.

`docs/cargo.md` is therefore **not** in this step's `git add`.

---

### Task 2: The `sandbox-exec -p` launch

**Result, 2026-09-25: recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile"), in its `home` and `state` paragraphs.** This plan does not restate the run's findings.

**Files:**
- Create: `crates/tinio-sandbox-macos/src/launch.rs`
- Modify: `crates/tinio-sandbox-macos/src/lib.rs` (the `pub mod launch;` declaration, the `pub use` of this module's root names, and the body of `impl Backend`)
- Modify: `crates/tinio-sandbox-macos/Cargo.toml` (nothing new — Task 1's manifest already carries `tracing`; this task is where its events are written)

**Interfaces:**
- Consumes: `profile::{Profile, profile, with_canonical_paths}`; and from the core `Backend`, `Child`, `Confinement`, `Error`, `LaunchPayload`, `LaunchPlan`, `Result`, `Sandbox`. The signal relay is **not** consumed here: `crate::relay::on_stop` is armed in Task 3, which is the task that creates the module (Step 4 below says where the call goes).
- Produces the crate's public surface: `tinio_sandbox_macos::{Backend, backend, command_args, confine, SANDBOX_EXEC}` from the root — `confine` is this module's function and `impl Backend`'s body is the one-line delegation to it — plus `tinio_sandbox_macos::launch::{listener, PREFLIGHT_MARKER}`. **`hand_over`, `make_inheritable`, `preflight` and `spawn_error` are private**, and the `step` this file used to define was not the crate's after the phase-basis correction — it is the core's, imported: they are `fn`, not `pub fn`, because nothing outside the crate calls them, and the Self-Review lists them as internals rather than as surface. `with_canonical_paths` is **not** re-exported here — it stays in `profile`, where Task 1 defines it, and the crate root deliberately does not gain a second path to it. `confine(&Sandbox, &LaunchPlan) -> Result<Confinement>` is the trait-shaped entry — it is the body of `impl Backend` and the facade selects it through `platform()`; `command_args(&Path, &[OsString], &Profile, Option<i64>) -> Vec<OsString>` is the pure argv builder, and it emits one `-D NAME=VALUE` pair per `Profile::params` entry, in order and as separate argv elements, plus the one pair the `process-exec` literal resolves to — `P`, its value being that same `program` argument — all between the profile flag and the program path; `listener(&Adopted)` rebuilds the child's socket from the descriptor `adopt` handed back, because `Adopted` is the core's type and the rebuild is platform-specific — and it is **one of this crate's three `#[allow(unsafe_code)]` sites** — `apply::libsystem`'s `unsafe extern "C"` block and `apply::validator`'s `fork` are the other two, and decision 14 enumerates all three — since the descriptor arrives as an integer in argv and every `std` route from an integer (`BorrowedFd::borrow_raw`, `OwnedFd::from_raw_fd`) is an `unsafe fn` (decision 14). `adopt`, the descriptor predicates and the event relay are the core's, not this crate's.

Steps 1–4 are pure and run anywhere; steps 6–7 need a Mac.

- [x] **Step 1: Read P0's core surface and confirm the names**

Run: `cargo doc -p tinio-sandbox-core --no-deps --open` (or read `crates/tinio-sandbox-core/src/{backend.rs,launcher/mod.rs,launcher/adopt.rs,launcher/control.rs,launcher/plan.rs,launcher/payload.rs,launcher/supervise.rs}`).

The core owns the whole interface this task implements against: the `Backend` trait and `Confinement`, `Child`/`supervise`, `LaunchPayload { config, token, confinement }` (it derives `Deserialize` alone — the crate-private `Wire` is the frame's only writer, through `LaunchPayload::encode_frame`), `LaunchPlan` (`for_launch(config)` takes the configuration text, `with_listener(socket)` carries the port, `config()`/`has_config()` and `listener()` hand them back), `Adopted`/`adopt(args, stdin)` for the child side, the listener-descriptor argv element (`listener_fd_arg`, `is_listener_fd_arg`, `listener_fd_from_arg`), and the event stream and its relay (`Event`, `Observer`, `reemit` — the last two async, over the child's pipes). What is **not** the core's — and therefore what this task writes — is the macOS side of using it: the profile and its pre-flight, the child's program and argv on this platform, the descriptor work, the stdio setup, the thread that awaits the relay and the signal handling. Nothing here is an addition to the core's surface.

Three consequences to build against rather than discover. The child is **this binary re-invoked**, so `program` is `std::env::current_exe()` and `argv` is `std::env::args_os().skip(1)` — no `LaunchPlan` extension is needed. The listener's descriptor number has to be *converted into* the argv element the core names: the plan owns the socket (`for_launch(config)?` then `.with_listener(listener)`), `plan.listener()` lends it as `Option<&TcpListener>`, and this launcher derives the number with `i64::from(socket.as_raw_fd())`. The payload deliberately carries no descriptor — a socket cannot ride on the stdin stream, and holding the number in both places would be two sources of truth for one fact (§3 step 7). And the payload now goes on **stdin** (decision 17), which means this launcher writes a frame and closes a stream instead of creating a pipe and clearing a close-on-exec flag.

- [x] **Step 2: Write the failing test**

`crates/tinio-sandbox-macos/src/launch.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::path::Path;

    use tinio_sandbox_core::AccessMode;

    use super::*;

    fn own_argv() -> Vec<OsString> {
        vec![OsString::from("serve"), OsString::from("/srv/data")]
    }

    /// A rendered profile from the real generator, so the argv tests exercise
    /// the text and the parameters the launch will actually be handed: one
    /// read-only exposure, which is one `(param "E0")` and one `params` entry.
    fn rendered_profile() -> Profile {
        profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadOnly)).expect("profile")
    }

    #[test]
    fn the_profile_is_one_argv_element_and_no_file_is_named() {
        // Design decision 24: `-p <profile>` inline. `sandbox-exec` has no fd
        // or stdin form, and a profile file would need exit-path cleanup — so
        // the profile must appear as exactly one element, and nothing in the
        // argv may look like a profile file. The `-D` parameters ride beside it
        // as further elements (measured: they work with `-p` and with `-f`, on
        // either side of the profile flag; the plan puts them after it).
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        assert_eq!(args[0], OsString::from("-p"));
        assert_eq!(args[1], OsString::from(rendered.text.clone()));
        assert_eq!(args[2], OsString::from("-D"));
        assert_eq!(args[3], OsString::from("E0=/srv/data"));
        // The `process-exec` literal's parameter, whose value is the same path the
        // program element carries — the rule narrows to exactly the exec this
        // launch performs, and there is no second place to spell it.
        assert_eq!(args[4], OsString::from("-D"));
        assert_eq!(args[5], OsString::from("P=/srv/bin/serve"));
        assert_eq!(args[6], OsString::from("/srv/bin/serve"));
        assert_eq!(args[7], OsString::from("serve"));
        assert_eq!(args[8], OsString::from("/srv/data"));
        assert_eq!(
            args.iter().filter(|a| *a == OsStr::new("-p")).count(),
            1,
            "exactly one -p, carrying the whole profile"
        );
        assert!(
            args.iter().all(|a| !a.to_string_lossy().ends_with(".sb")),
            "no profile file may be named"
        );
    }

    #[test]
    fn every_parameter_is_emitted_after_the_profile_flag_in_exposure_order() {
        // The order is what makes the references resolve: `(param "E0")` in the
        // text is `params[0]`, and `params[0]` is the first `-D` here. Getting
        // it wrong is not silent — a parameter the profile references but the
        // launch never defines is a compile error, and `preflight` turns that
        // into a startup failure.
        let rendered = profile(
            &Sandbox::new()
                .expose("/srv/data", AccessMode::ReadOnly)
                .expose("/home/u/.tinio/roots/h", AccessMode::ReadWrite),
        )
        .expect("profile");
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        assert_eq!(args[0], OsString::from("-p"));
        assert_eq!(
            &args[2..6],
            &[
                OsString::from("-D"),
                OsString::from("E0=/srv/data"),
                OsString::from("-D"),
                OsString::from("E1=/home/u/.tinio/roots/h"),
            ]
        );
        // The program parameter comes after the exposure set rather than among it:
        // `Profile::params` is exposures in order, and `P` is the launcher's own
        // value, which `command_args` writes from the argument it already holds.
        assert_eq!(
            &args[6..8],
            &[OsString::from("-D"), OsString::from("P=/srv/bin/serve")]
        );
        assert_eq!(args[8], OsString::from("/srv/bin/serve"));
        assert_eq!(&args[9..11], &own_argv()[..]);
    }

    #[test]
    fn the_listener_descriptor_is_the_last_element() {
        // Design §3 step 7: the listener's descriptor number travels in argv,
        // "because a socket cannot ride on a stream". The payload does ride on
        // one — stdin — so this is the only descriptor element there is. What
        // this test asserts is the element's *identity*: the number is written
        // by `listener_fd_arg` and recognised by `is_listener_fd_arg`, both the
        // core's. **Decoding it back is not this crate's**: `listener_fd_from_arg`
        // is called on the core's side of the contract, inside `adopt`, so the
        // launcher never imports it and this test does not either — the round
        // trip is core's to test.
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered_profile(), Some(7));
        let last = args.last().expect("last element");
        assert_eq!(last, &listener_fd_arg(7));
        assert!(is_listener_fd_arg(last));
        assert!(
            !is_listener_fd_arg(OsStr::new("serve")),
            "a plain argument is not the launcher's descriptor element"
        );
    }

    #[test]
    fn a_launch_without_a_listener_carries_no_descriptor_element() {
        // Linux confines in place and its plan has no listener; a launcher that
        // bound nothing must not invent one.
        let rendered = rendered_profile();
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered, None);
        // `-p`, the text, the program parameter's two elements and the program,
        // plus two elements per parameter.
        assert_eq!(args.len(), own_argv().len() + 5 + 2 * rendered.params.len());
        assert!(args.iter().all(|arg| !is_listener_fd_arg(arg)));
    }

    #[test]
    fn the_child_is_this_binary_re_invoked() {
        // The launcher has no separate child program: it re-execs itself with
        // its own arguments (design §3 step 6, "`sandbox-exec -p … <self> <args>`").
        let args = command_args(Path::new("/srv/bin/serve"), &own_argv(), &rendered_profile(), Some(9));
        assert_eq!(args[6], OsString::from("/srv/bin/serve"));
        assert_eq!(&args[7..9], &own_argv()[..]);
    }

    /// Everything the closure logged, as the subscriber rendered it.
    ///
    /// **This installs a subscriber for the duration of one closure and no
    /// longer**, which is the whole reason it is a test helper: the crate
    /// installs none (shared §7), so a test that wants to read its own events
    /// has to be the one to capture them. `with_default` is thread-scoped, so
    /// the other tests in this binary are unaffected.
    fn record_events(emit: impl FnOnce()) -> String {
        use std::sync::{Arc, Mutex};

        use tracing_subscriber::fmt::MakeWriter;

        #[derive(Clone, Default)]
        struct Sink(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("sink").extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> MakeWriter<'a> for Sink {
            type Writer = Sink;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let sink = Sink::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(sink.clone())
            // **Load-bearing for a field assertion, and measured rather than
            // assumed**: the default subscriber wraps each field *name* and the
            // equals sign after it in ANSI escapes, so a captured line carries
            // the field only as coloured spans and never as `step=<value>`. The Linux
            // plan's own `traced` helper carries this same line for the same
            // reason, and so does the core's recorder test.
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        String::from_utf8_lossy(&sink.0.lock().expect("sink")).into_owned()
    }

    /// A failure logs the `step` it returns, under a recording subscriber.
    ///
    /// Shared §7's checking rule, and the one assertion that makes "it logs" a
    /// property this crate can lose: the closure runs real code and the test
    /// requires the event's `step` field and the `Error::Step`'s `step` to be the
    /// same string, so a step name changed in one place and not the other fails
    /// here rather than in a log nobody reads. **The builder it calls is the
    /// core's now** (the phase-basis correction), so what this test pins for this
    /// crate is the call this file actually makes — the curried `step` that the
    /// pre-flight and `make_inheritable` both reach for; the core's own
    /// recorder test covers `launch` and the property in general.
    #[cfg(target_os = "macos")]
    #[test]
    fn a_failure_logs_the_step_it_returns() {
        let mut named = None;
        let recorded = record_events(|| {
            let err = step("payload stdin")(std::io::Error::other("no piped stdin"));
            named = Some(match err {
                Error::Step { step, .. } => step,
                other => panic!("expected Step, got {other:?}"),
            });
        });
        assert_eq!(named, Some("payload stdin"));
        assert!(recorded.contains("step=\"payload stdin\""), "{recorded}");
    }

    /// The same property on every other target, through the stub arm of
    /// `confine` — which is the one failure this crate can produce without a
    /// Mac, and which logs the step it refuses with for exactly this reason.
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn a_failure_logs_the_step_it_returns() {
        let recorded = record_events(|| {
            let err = confine(&Sandbox::new(), &LaunchPlan::new()).expect_err("the stub refuses");
            assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        });
        assert!(recorded.contains("step=\"confine\""), "{recorded}");
    }
}
```

- [x] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-macos launch::`
Expected: FAIL — `cannot find function 'command_args'`.

- [x] **Step 4: Write `launch.rs`**

**The frame's spelling in the block below is the pre-change record too, and it stays as written**: `plan.payload()` and `serde_json::to_vec(payload)` are gone — `LaunchPlan` carries `config: Option<String>` (read through `config()` and `has_config()`) and the frame is written by `LaunchPayload::encode_frame(plan.config(), plan.token(), confinement)`, whose one definition is the crate-private `Wire`; what `hand_over` does (one JSON object into the child's stdin, the write end closed) is unchanged.

**Two comments in the block below count the crate's `unsafe` sites against a crate that had one, and they stay as written (a phase's record, `docs/superpowers/README.md`'s record rule)**: "the crate's single `unsafe` site stays in the child" and "this crate's single enumerated `#[allow(unsafe_code)]` site is the listener rebuild in `listener`" both predate the in-process mechanism, which added two more — `apply::libsystem`'s `unsafe extern "C"` block and `apply::validator`'s `fork` — so the crate carries three today (design decision 14 enumerates all three). The argument either sentence makes is unchanged: the descriptor rebuild is the launch path's own site, and one more `unsafe` block beside it would be one site more.

```rust
//! `sandbox-exec -p` — the launch, the profile pre-flight and the descriptor
//! handover.
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. This module is the
//! only place in the crate that depends on it, so the failure it represents —
//! "no sandbox available" — is one error, raised in one place, and it fails
//! closed (design §11).

#[cfg(target_os = "macos")]
use std::ffi::OsString;
#[cfg(target_os = "macos")]
use std::os::fd::{AsRawFd as _, RawFd};
#[cfg(target_os = "macos")]
use std::process::Command;

use std::path::Path;

use tinio_sandbox_core::{Confinement, Error, LaunchPlan, Result, Sandbox, listener_fd_arg};
// The core types only the macOS arm names. Ungated, they would be `use`d names
// the crate never resolves on a Linux or Windows build — an unused-import
// warning at best, and an `E0432` the moment one of them came from a
// macOS-only dependency. **`refused`, `step` and `launch` are the core's failure
// builders and the phase-basis correction is where they arrived**: this file
// defined its own `step` until then, and `spawn_error` is the call site that
// shows why the core publishes both shapes — a `NotFound` from this platform's
// spawn is a refusal the core cannot name (`refused`, with `Error::Unsupported`),
// while everything else is a failure about a path (`launch`).
#[cfg(target_os = "macos")]
use tinio_sandbox_core::{Adopted, Child, LaunchPayload, Observer, is_listener_fd_arg};
// **In the alias form, which is how every sandbox crate names the core**
// (`crate::_core`, `docs/style.md` §Imports; this phase's ledger states the
// alias wins over these blocks' spelling) — the `tinio_sandbox_core::` lines
// above are corrected to it in place, while these three arrive already
// corrected.
#[cfg(target_os = "macos")]
use crate::_core::{launch, refused, step};

use tracing::error;
// The other two levels are only ever emitted from macOS-gated code, so they are
// imported under the same gate; `error!` is not, because the stub arm below logs
// its refusal on every other target.
#[cfg(target_os = "macos")]
use tracing::{debug, info};

use crate::profile::{Profile, profile, with_canonical_paths};

/// Where macOS keeps `sandbox-exec`.
pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// What the pre-flight asks `/bin/echo` to print.
pub const PREFLIGHT_MARKER: &str = "tinio-sandbox-preflight";

/// How long the pre-flight may run before it is killed.
///
/// **The bound is in-process, because `timeout(1)` does not exist on macOS**:
/// there is no external program to lean on, so the wait polls the child's own
/// exit status against this deadline and kills the child when it expires. What is
/// being bounded is one profile compile and one `echo`, so the number is a bound
/// rather than a timeout to tune; what it buys is the difference between a
/// startup error and a launcher that emits no event, exits never and says nothing
/// on stderr while the operator waits for a server that is not coming.
///
/// Gated, because it is private and only the macOS arm reads it: ungated it would
/// be a `dead_code` warning on every other target, and the crate builds there too.
#[cfg(target_os = "macos")]
const PREFLIGHT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

// Note on the imports that are *absent*: this module names no `tokio::io` trait,
// and that is deliberate rather than an omission. `Observer::new` and `observe`
// are generic over `AsyncWrite` and `AsyncBufRead`, so the bounds are the
// callee's and the caller imports nothing to satisfy them; an earlier draft
// carried `use tokio::io::{AsyncBufRead, AsyncWrite};`, which was dead on every
// target and — because `tokio` is a macOS-only dependency — an `E0432` on every
// other one. Gating a dead import would satisfy neither the build nor the
// crate's no-warnings step, so it is gone rather than gated. The two `tokio`
// names this thread does need, `tokio::fs::File` and `tokio::io::BufReader`,
// are imported where they are used, inside the block that runs only on macOS.

/// Confine: render the profile, prove `sandbox-exec` accepts it, hand the child
/// its payload and its listener descriptor, launch it, and relay its events.
///
/// The calling process is **not** confined — it becomes the supervisor, which
/// is why this returns [`Confinement::Supervise`] and never
/// [`Confinement::InPlace`] (decision 6). **This function is the body of
/// `impl Backend for Backend`** (Task 1's `lib.rs` carries the impl; the trait's
/// method is the one-line delegation to this function), which is what lets
/// `serve` reach it through the facade's `platform()` without naming this crate.
///
/// # Errors
///
/// [`Error::BadPath`] for an exposure the profile cannot express;
/// [`Error::Unsupported`] when `sandbox-exec` is missing (the accepted
/// degradation of the design's Risks); [`Error::Step`] for a refused spawn or a
/// refused profile, carrying `sandbox-exec`'s own diagnostic.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::{AccessMode, LaunchPlan, LaunchPayload, Sandbox};
/// use tinio_sandbox_macos::confine;
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let plan = LaunchPlan::for_launch(String::new());
/// let _child = confine(&sandbox, &plan)?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn confine(sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement> {
    use std::process::Stdio;

    let program = std::env::current_exe().map_err(step("current_exe"))?;
    let argv: Vec<std::ffi::OsString> =
        std::env::args_os().skip(1).filter(|arg| !is_listener_fd_arg(arg)).collect();

    // 1. The profile: the exposure set, canonicalised for the kernel's view of
    //    the paths. Nothing is added for the executable's own directory:
    //    decision 22's grant is Windows's alone, and a macOS confined child
    //    runs with no grant of any kind on its own location (`process-exec` is
    //    the only gate), so macOS deliberately does not spend one. The paths
    //    come back as `-D` parameters rather than as text (decision 24), so the
    //    text is a constant with `(param "E<i>")` references in it and the
    //    values are data this launcher only forwards. The text's other reference
    //    is the one path the launcher supplies instead of forwarding:
    //    `process-exec`'s `(literal (param "P"))` is `program` above, resolved by
    //    `command_args` from the same binding it writes as the program element.
    let rendered = profile(&with_canonical_paths(sandbox))?;
    info!(
        step = "profile",
        rules = rendered.text.lines().count(),
        params = rendered.params.len(),
        "the profile the child will run under"
    );

    // Before the real child exists, so a profile `sandbox-exec` refuses is a
    // startup error and not a half-started server (design §11). The parameters
    // go with it: an undefined `(param …)` is a compile error, so a pre-flight
    // that dropped them would refuse every profile that has an exposure.
    //
    // It is also the second compile of the same text per startup: the profile is
    // proved here and then compiled again by the real launch, both times as one
    // argv element. No document measures what that costs, and it is not worth a
    // cache here — the alternative is not proving the profile at all, and the
    // price of that is the failure design §11 exists to prevent.
    preflight(&rendered)?;

    // 2. The payload, exactly as the plan carries it: the configuration **text**
    //    the parent forwarded, which this launcher neither parses nor rewrites
    //    (decisions 17 and 18). It is serialized to one JSON object and written
    //    to the child's **stdin**, and the stream is closed below; the standard
    //    streams are inherited anyway, so the payload costs no pipe and no
    //    descriptor.
    let payload = plan.payload().ok_or_else(|| {
        error!(step = "payload", "the launch plan carries no payload");
        Error::Unsupported { detail: "the launch plan carries no payload" }
    })?;

    // 3. The listener is the one descriptor that has to survive `sandbox-exec`,
    //    because a socket cannot ride on a stream: clear its close-on-exec flag
    //    so the child can rebuild a listener from the number in its argv. The
    //    plan owns the socket and the descriptor is derived from it here — argv
    //    is the single source of truth the child reads. Both halves go through
    //    the socket itself: `as_raw_fd` for the number, and `make_inheritable`
    //    for the flag, which takes the `TcpListener` because it implements
    //    `AsFd`. Nothing in this process reconstructs a descriptor from an
    //    integer, so the crate's single `unsafe` site stays in the child
    //    (decision 14).
    let listener_fd = plan.listener().map(|socket| i64::from(socket.as_raw_fd()));
    make_inheritable(plan.listener())?;

    // 4. stdout is the child's protocol channel and stderr is its log channel
    //    (decision 19): pipe the first so the parent can decode and re-emit, and
    //    let the second through untouched — `sandbox-exec`'s own diagnostics go
    //    there too, and the parent never mediates it.
    let mut child = Command::new(SANDBOX_EXEC)
        .args(command_args(&program, &argv, &rendered, listener_fd))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(spawn_error)?;
    info!(
        step = "spawn",
        pid = child.id(),
        listener_fd,
        "the confined child is spawned under sandbox-exec"
    );

    // 5. Write the frame, then close stdin so the child's read returns. **This
    //    write can block**, and the frame's size is not what bounds it: it is the
    //    whole configuration text, which nothing bounds, so the write returns as
    //    the child drains it — the product child's `main` drains it first thing
    //    (`adopt`), and that is the only reason this is bounded at all, because
    //    the relay thread below is not running yet: while this blocks, the child's
    //    stdout is unattended too. `hand_over`'s doc carries the whole statement.
    hand_over(&mut child, &payload)?;
    drop(child.stdin.take());

    // 6. The event relay, on a thread of its own. The parent waits for the child
    //    on the main thread, so draining its stdout has to happen here: one
    //    thread doing both would block on a full pipe while the child blocked
    //    writing into it.
    //
    //    **The thread's handle is kept rather than dropped, and its return value
    //    is the answer's channel**: the handle goes to the core through
    //    `Child::with_relay`, which joins it inside `Child::wait`, and the join's
    //    result becomes the child's terminal record. There is no slot for the
    //    parent to fill and no lock to share — the relay returns what it saw, and
    //    the caller reads it from the `Child` once the child is gone (decision
    //    19's "the parent owns the printed-exactly-once guarantee"). The child
    //    being reaped and the relay noticing end-of-stream are two events with no
    //    ordering of their own, so a terminal read before the join would be a
    //    snapshot — and on a healthy child it would usually read nothing at all.
    let mut relay = None;
    if let Some(stdout) = child.stdout.take() {
        let handle = std::thread::Builder::new()
            .name("tinio-sandbox-events".into())
            .spawn(move || {
                use std::os::fd::OwnedFd;

                use tokio::fs::File;
                use tokio::io::BufReader;

                // Decode and re-encode rather than forward bytes (decision 19):
                // anything the child or a dependency prints that is not one of
                // our events must not reach the operator, who could not tell it
                // apart from a real one.
                //
                // Two conversions stand between the child's stdout and the
                // core's relay, and both are load-bearing. The child came from
                // `std::process`, so its stdout is a **blocking** pipe —
                // `ChildStdout` implements `std::io::Read`, not any async
                // reader. `tokio::fs::File` is what turns a descriptor into an
                // `AsyncRead` correctly: it puts the descriptor in
                // non-blocking mode and parks the thread on an epoll/kqueue
                // readiness, which is precisely the work a hand-rolled wrapper
                // would get wrong.
                //
                // Both hops are safe, and neither needs `unsafe`. `std` gives
                // `impl From<ChildStdout> for OwnedFd`, and `tokio::fs::File`
                // accepts an `OwnedFd`; ownership moves into the `File` by value
                // with no second owner to reason about. The route through
                // `as_raw_fd` plus `mem::forget` would move ownership just as
                // well, but it needs an `unsafe` block where this needs none —
                // and this crate's single enumerated `#[allow(unsafe_code)]`
                // site is the listener rebuild in `listener` (decision 14), so
                // an `unsafe` here would be a second site with no invariant to
                // justify it.
                let file = File::from(OwnedFd::from(stdout));
                let reader = BufReader::new(file);
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
                    return None;
                };
                // `Observer` rather than `reemit`, and that is the whole point:
                // `reemit` discards what the child reported, and the caller needs
                // it to decide whether this child ever became ready.
                runtime
                    .block_on(Observer::new(tokio::io::stdout()).observe(reader))
                    .ok()
                    .and_then(|observer| observer.terminal().cloned())
            })
            .map_err(step("spawn event relay"))?;
        relay = Some(handle);
    }

    // 7. The signal relay (design §3 step 9, decision 20) — a different relay
    //    from the one above: that one moves the child's *bytes* out, this one
    //    moves the launcher's *signals* in. **It is armed at this point, with the
    //    child's pid, and Task 3 is the task that writes the two statements** —
    //    `i32::try_from(child.id())` into the `Error::Unsupported` above, and
    //    `crate::relay::on_stop(pid).map_err(step("install stop-signal handling"))?`
    //    after it. They are not here because `crate::relay` is the module Task 3
    //    creates: naming it before its file exists would leave this task's own
    //    verification step with a `cargo test` that cannot compile, which is the
    //    order Task 3 Step 4 carries.
    //
    //    Arming after the spawn is not incidental: before the child exists there
    //    is nothing to relay to, so dying on a signal that arrives then is the
    //    correct behaviour rather than a gap in the relay.

    // The relay handle goes in with the child, and `Child::wait` joins it: the
    // terminal read afterwards is a result only because of that join.
    let supervised = Child::new(move || Ok(child.wait()?));
    Ok(Confinement::Supervise(match relay {
        Some(relay) => supervised.with_relay(relay),
        // Not reachable: stdout was piped, so it is either taken above or the
        // child never started. Kept as a real branch rather than an `expect`
        // because this crate forbids panics in library code (constitution II) —
        // and a child with no relay is still a child to wait for.
        None => supervised,
    }))
}

/// Every other target: the same surface, refused. The crate is a workspace
/// member and is built on Linux and Windows too.
#[cfg(not(target_os = "macos"))]
pub fn confine(_sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
    let detail = "the macOS backend is only available on macOS";
    error!(step = "confine", detail, "this target has no macOS backend");
    Err(Error::Unsupported { detail })
}

// Both arms above are what `lib.rs`'s `impl Backend for Backend` delegates to,
// and this step is what changes that impl's body: Task 1 ships it with P0's stub
// text, and here it becomes the one-line forward
// `launch::confine(sandbox, plan)`. `confine` is also re-exported at the crate
// root (`pub use self::launch::{SANDBOX_EXEC, command_args, confine};`), which is
// what lets the facade and the tests name it without the module path.

/// The argv `sandbox-exec` is invoked with.
///
/// Pure, so the "inline, one element, no file" property of decision 24 is
/// testable on any host. Shape:
/// `-p <profile text> -D NAME=VALUE … <program> <argv…> <listener-fd>`, with one
/// `-D` flag and one `NAME=VALUE` assignment per [`Profile::params`] entry, in
/// that order, plus one more pair for the parameter the `process-exec` rule's
/// literal resolves to — `P`, whose value is this function's own `program`
/// argument, which is the path the kernel will exec — all of them between the
/// profile flag and the program path. Measured on macOS 15.8 (2026-09-19):
/// `-D` is accepted on either side of the profile flag, and with `-f <file>` as
/// well as `-p <text>` — the profile flag and the parameters belong to the same
/// invocation, and the assignments are where the text's `(param "E<i>")`
/// references are resolved from.
///
/// **`P` is written here rather than carried in [`Profile::params`]**, because it
/// is not an exposure: the value is one this function already holds, and taking
/// it from the same argument as the program element is what makes the rule's
/// filter and the exec `sandbox-exec` performs a single value rather than two
/// spellings that could drift.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::command_args;
/// use tinio_sandbox_macos::profile::profile;
///
/// let rendered = profile(&Sandbox::new().expose("/srv/data", AccessMode::ReadOnly)).unwrap();
/// let args = command_args(Path::new("/srv/bin/serve"), &[], &rendered, Some(7));
/// assert_eq!(args[0], OsString::from("-p"));
/// assert_eq!(args[1], OsString::from(rendered.text.clone()));
/// assert_eq!(args[2], OsString::from("-D"));
/// assert_eq!(args[3], OsString::from("E0=/srv/data"));
/// assert_eq!(args[4], OsString::from("-D"));
/// assert_eq!(args[5], OsString::from("P=/srv/bin/serve"));
/// assert_eq!(args[6], OsString::from("/srv/bin/serve"));
/// assert_eq!(args[7], OsString::from("tinio-sandbox-fd=7"));
/// ```
pub fn command_args(
    program: &Path,
    argv: &[std::ffi::OsString],
    profile: &Profile,
    listener_fd: Option<i64>,
) -> Vec<std::ffi::OsString> {
    let mut args = Vec::with_capacity(argv.len() + 7 + 2 * profile.params.len());
    args.push(std::ffi::OsString::from("-p"));
    args.push(std::ffi::OsString::from(&profile.text));
    // A parameter value is data, and it is why this function does no escaping:
    // the text refers to the value by name, so a quote, a newline or a whole
    // SBPL rule inside a path is passed through as the path it is (measured).
    for (name, value) in &profile.params {
        let mut assignment = std::ffi::OsString::from(name.as_str());
        assignment.push("=");
        assignment.push(value);
        args.push(std::ffi::OsString::from("-D"));
        args.push(assignment);
    }
    // The parameter the `process-exec` rule's literal resolves to. It is `program`
    // itself, so the one exec the rule permits is the one `sandbox-exec` performs
    // — a narrowing that cannot drift from the path it narrows to.
    let mut program_param = std::ffi::OsString::from("P=");
    program_param.push(program.as_os_str());
    args.push(std::ffi::OsString::from("-D"));
    args.push(program_param);
    args.push(program.as_os_str().to_os_string());
    args.extend(argv.iter().cloned());
    if let Some(fd) = listener_fd {
        args.push(listener_fd_arg(fd));
    }
    args
}

/// Rebuild the listener from the descriptor `adopt` handed back.
///
/// A free function rather than an inherent `Adopted::listener()`, because
/// `Adopted` is the core's type and an inherent impl may only live in its
/// defining crate. The rebuild is the platform's job — it is `fcntl`/`dup` on
/// Unix and handle plumbing on Windows — so core stops at the descriptor and
/// this crate does the rest.
///
/// **This is the crate's one `unsafe` site** (design decision 14, its Scope and
/// the Bookkeeping bullet), and the obvious-looking safe route is not one.
/// `BorrowedFd::try_clone_to_owned` is safe, and `From<OwnedFd> for TcpListener`
/// is safe — but each is safe only *after* a `BorrowedFd` or an `OwnedFd`
/// exists, and obtaining either from the integer that crossed argv is the unsafe
/// step: `BorrowedFd::borrow_raw` and `OwnedFd::from_raw_fd` are both `unsafe
/// fn`, and `rustc` answers `E0133` for each (measured with rustc 1.98.1 on
/// macOS 15.8, 2026-09-19). `rustix` is no escape: its `net::socket` module is
/// free functions over `OwnedFd` (`pub fn socket(...) -> io::Result<OwnedFd>`,
/// `pub fn accept<Fd: AsFd>(sockfd: Fd) -> io::Result<OwnedFd>`) with no
/// raw-descriptor constructor, so a `rustix` route would still begin at
/// `OwnedFd::from_raw_fd`. Duplicating rather than adopting does not help
/// either: the number is all that crossed the exec, so the duplicate would have
/// to be made *from* a raw descriptor, which is the same unsafe call.
///
/// # Safety
///
/// The descriptor is inherited from the launcher, which holds it open for the
/// child's whole lifetime and never closes it. This process is therefore the
/// descriptor's only owner: `OwnedFd::from_raw_fd` takes the only ownership
/// there is, and the `OwnedFd` — and so the `TcpListener` built from it —
/// closes it exactly once, when the listener is dropped at the end of the run.
/// The launcher's own copy stays open throughout, which is what makes the
/// number valid at the moment it is adopted.
///
/// **`from_raw_fd` may be called at most once per descriptor number**, which is
/// the part of this contract a caller can break without meaning to: the second
/// call claims a second `OwnedFd` over one descriptor, and the two drops close
/// it twice — a double close, or worse, a close of whatever the number was
/// reused for. This function is therefore **call-once per process**, and the
/// test harness — whose accessor is the one caller that could plausibly be
/// reached twice — takes the listener out of its `Adopted` rather than handing
/// out a fresh rebuild on every call (Task 4's `harness::Child::listener`).
///
/// # Errors
///
/// An I/O error when `adopt` carried no descriptor, or when the number does not
/// fit in a `RawFd` (the core carries it as an `i64`, the kernel wants an `i32`).
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_macos::launch::listener;
///
/// # let adopted = tinio_sandbox_core::adopt(std::env::args_os(), std::io::stdin()).unwrap().unwrap();
/// let socket = listener(&adopted)?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn listener(adopted: &Adopted) -> std::io::Result<std::net::TcpListener> {
    use std::os::fd::OwnedFd;

    let raw = adopted
        .listener_fd
        .ok_or_else(|| std::io::Error::other("no listener descriptor was handed over"))?;
    let fd = RawFd::try_from(raw)
        .map_err(|_| std::io::Error::other("the listener descriptor is out of range"))?;
    // SAFETY: the invariant in the `# Safety` section above — the descriptor is
    // the launcher's, inherited across the exec, held open there for this
    // process's whole lifetime, and owned by nothing else in this process; and
    // this call happens once, because the caller runs once.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    debug!(step = "listener", fd, "the child rebuilt its listener from the inherited descriptor");
    Ok(std::net::TcpListener::from(owned))
}

/// Write the payload frame to the child's standard input.
///
/// The caller closes the stdin once this returns, which is what makes the
/// child's `adopt` see the end of the stream.
///
/// **This is a blocking write, and what bounds it is the child rather than the
/// size of the frame.** The frame is the launcher's **whole configuration text**
/// (`LaunchPayload { config }`, decision 17), and nothing in the design or in this
/// plan bounds how large that is: a pipe's buffer is a few KiB, so a frame past it
/// returns only as the child drains it, and the figure is where the write stops
/// being a copy into the buffer rather than a bound on what may be sent. The child
/// does read: `adopt` is the first thing its `main` does, before the runtime or
/// the storage exists, and the write happens *after* the spawn — so the two are a
/// producer and a consumer that meet, and the write completes as long as the child
/// runs and reads. **What can stall is a child that neither reads nor exits**, and
/// it stalls here, before the launcher has reported anything. No caller imposes a
/// size limit today, and that is recorded rather than assumed away: a caller that
/// wants one has to add it here or where the configuration text is built.
///
/// **The order matters on this platform, and what keeps it safe is the child's
/// side of the arrangement.** The frame is written before the relay thread is
/// spawned (step 5 of `confine` above, then step 6), so while this write blocks
/// the child's stdout is unattended: a child that filled its stdout buffer would
/// leave the launcher blocked on stdin and itself blocked on stdout, with neither
/// side moving. What rules that out is that the child does not write enough on
/// stdout to fill the pipe before its stdin is drained — the product child
/// because `adopt` is the first statement of its `main` (design §10 step 1), and
/// the harness's child, a test binary, because what it opens with is libtest's own
/// preamble, a few hundred bytes against the pipe's buffer. Nothing in the
/// launcher enforces either: it is an invariant the child owns, and a child that
/// broke it would deadlock the launcher here rather than fail.
#[cfg(target_os = "macos")]
fn hand_over(child: &mut std::process::Child, payload: &LaunchPayload) -> Result<()> {
    use std::io::Write as _;

    let stdin = child
        .stdin
        .as_mut()
        .ok_or_else(|| step("payload stdin")(std::io::Error::other("the child has no piped stdin")))?;
    let frame = serde_json::to_vec(payload).map_err(step("serialize payload"))?;
    stdin.write_all(&frame).map_err(step("write payload"))?;
    stdin.flush().map_err(step("flush payload"))?;
    debug!(step = "payload", bytes = frame.len(), "the payload is written to the child's stdin");
    Ok(())
}

// Nothing here adds the executable's own directory to the exposure set, and
// that is deliberate: decision 22's grant on the program's own location is
// Windows's alone. A macOS confined child runs with no grant of any kind on its
// own location — `process-exec` is the only gate, `file-read-data` on its own
// image and directory is refused and logged, and the program proceeds
// regardless — so macOS deliberately does not spend one, and this is the note
// that stops a future reader adding one back.

/// Prove `sandbox-exec` accepts the profile before the real child is spawned.
///
/// Design §11 requires a refused profile to surface as "a startup error
/// carrying the binary's own diagnostic". The child's stderr is inherited (it
/// is the server's log), and capturing it would need a pump that deadlocks when
/// the pipe fills — so the profile is proved on a throwaway process instead.
/// The failure this catches is `sandbox-exec`'s own (a syntax error, a refusal),
/// not the confined program's.
///
/// **`/bin/echo` needs far fewer allowances than the server does, so passing
/// this proves the profile compiles and nothing more.** A profile missing
/// `sysctl-read` passes the pre-flight — `/bin/echo` never sizes a thread pool
/// — and then kills the real child, whose failure is a bare abort with no
/// diagnostic. The end-to-end leg (Task 9) is the check for that, not this one.
///
/// The argv comes from [`command_args`], so what is proved is the profile the
/// launch will hand over, parameters included — with the single exception the
/// next paragraph names. That is not a convenience: the text references
/// `(param "E0")` and an undefined parameter is a compile error, so a pre-flight
/// without the `-D` elements would refuse every profile that has an exposure —
/// and a pre-flight that escaped or rewrote a value would prove a profile the
/// launch never passes.
///
/// **One parameter differs from the launch's, and it has to.** `P` is the
/// parameter the `process-exec` rule's literal resolves to, and `command_args`
/// takes its value from the program it is handed — here `/bin/echo`, because that
/// is the exec this probe performs. Binding it to the launcher's own path instead
/// would refuse the probe's own exec, which is the `rc=71` that rule's `evidence`
/// names: correct behaviour, and a useless proof. Every other element is the
/// launch's, so a profile `sandbox-exec` cannot compile is still refused here.
///
/// **The marker below is not an event and is not framed like one.** Nothing but
/// a tinio process may write on the protocol channel (decision 19), and
/// `sandbox-exec` is not one — so this pre-flight does not use it. Its stdout is
/// captured and compared in-process: it is a private convention between this
/// function and `/bin/echo`, and it never reaches the launcher's stdout, which
/// continues to carry `Event` frames and nothing else.
///
/// **The wait is bounded, and that is the difference between a failure and a
/// hang.** This is the last thing `confine` does before the real spawn, so a
/// pre-flight that never returns is a launcher that emits no event, exits never
/// and writes nothing on stderr — which an operator cannot tell from "the server
/// never started". `Command::output()` is exactly that unbounded wait: both pipes
/// and the exit, with no deadline of its own. `timeout(1)` is not the fallback it
/// looks like either, because macOS does not ship one, so the bound is here, in
/// the wait ([`PREFLIGHT_TIMEOUT`]): it polls the child's own exit status and
/// kills the child when the deadline expires. **What the operator sees then** is
/// an `error!` naming the step and the deadline and a startup error carrying the
/// same sentence — `no exit within 10s; the pre-flight was killed` — instead of a
/// silence nothing distinguishes from a server that never came up. The pipes are
/// read *after* the exit, which cannot deadlock here: the only writer is
/// `/bin/echo` and its output is one short line.
#[cfg(target_os = "macos")]
fn preflight(profile: &Profile) -> Result<()> {
    use std::io::Read as _;
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let probe = command_args(
        Path::new("/bin/echo"),
        &[OsString::from(PREFLIGHT_MARKER)],
        profile,
        None,
    );

    // Spawned rather than `output()`-captured, because `output()` waits for both
    // pipes and the exit without a bound. `Stdio::null()` on stdin is what
    // `output()` did implicitly; neither `/bin/echo` nor `sandbox-exec` reads it.
    let mut child = Command::new(SANDBOX_EXEC)
        .args(probe)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(spawn_error)?;

    let deadline = Instant::now() + PREFLIGHT_TIMEOUT;
    let status = loop {
        match child.try_wait().map_err(step("sandbox-exec -p"))? {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                error!(
                    step = "sandbox-exec -p",
                    timeout = ?PREFLIGHT_TIMEOUT,
                    "the pre-flight outlived its deadline and was killed"
                );
                return Err(Error::Step {
                    step: "sandbox-exec -p",
                    source: std::io::Error::other(format!(
                        "no exit within {PREFLIGHT_TIMEOUT:?}; the pre-flight was killed"
                    )),
                });
            }
            // A poll rather than a blocking wait, because the wait is the thing
            // being bounded; the interval is smaller than anything this probe
            // spends doing its work.
            None => std::thread::sleep(Duration::from_millis(10)),
        }
    };

    // Read after the exit rather than beside the wait: a reader racing the wait
    // is the pipe-buffer deadlock shape, and there is nothing here for that
    // machinery to protect.
    let mut stdout = String::new();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_string(&mut stdout).map_err(step("sandbox-exec -p"))?;
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_string(&mut stderr).map_err(step("sandbox-exec -p"))?;
    }

    // `/bin/echo` ends its line with a newline, so the marker is compared with
    // that trimmed: a byte-exact comparison against `PREFLIGHT_MARKER` can never
    // match, and its failure would read as a refused profile.
    if stdout.trim_end_matches('\n') != PREFLIGHT_MARKER {
        // `rc=71` is not the discriminator it looks like: a missing exec target
        // produces the same code, so the stderr text is what says whether the
        // profile was refused or something else went wrong. Both are carried.
        let diagnostic = stderr.trim().to_string();
        error!(
            step = "sandbox-exec -p",
            exit = ?status.code(),
            diagnostic = %diagnostic,
            "sandbox-exec refused the profile"
        );
        return Err(Error::Step {
            step: "sandbox-exec -p",
            source: std::io::Error::other(format!("exit {:?}: {diagnostic}", status.code())),
        });
    }
    info!(
        step = "preflight",
        rules = profile.text.lines().count(),
        "sandbox-exec accepted the profile"
    );
    Ok(())
}

/// Clear `FD_CLOEXEC` on the listener, the one descriptor that must survive
/// `sandbox-exec`.
///
/// The payload no longer needs this: it travels on stdin, and the standard
/// streams are inherited anyway (decision 17). The listener does, because a
/// socket cannot ride on a stream and `sandbox-exec` sits between the launcher
/// and the child on the exec path — so the child can only rebuild its listener
/// from a descriptor that crossed that exec, and Rust creates sockets
/// close-on-exec. The child cannot clear the flag for itself: with `FD_CLOEXEC`
/// set there is no descriptor left on the other side for it to act on.
///
/// The wrapper is safe **because the socket itself is the argument**:
/// `TcpListener` implements `AsFd`, so `rustix::io::fcntl_setfd` is handed a
/// real borrowed descriptor and nothing here reconstructs one from an integer.
/// That is why this function needs no `#[allow(unsafe_code)]` and the crate's
/// single site stays in the child's rebuild ([`listener`], decision 14).
#[cfg(target_os = "macos")]
fn make_inheritable(listener: Option<&std::net::TcpListener>) -> Result<()> {
    use rustix::io::{FdFlags, fcntl_setfd};

    // `None` is legal: a caller that bound nothing is a caller whose child will
    // bind its own socket, and Linux has no listener to hand over at all.
    if let Some(socket) = listener {
        // `rustix` reports an `Errno` and the core's `step` takes what
        // converts into a `std::io::Error`, which is the whole of what
        // `into()` does here: handing the `Errno` straight to `step` does not
        // type-check without it — the bound is what lets one helper serve both
        // error types — and with it the step name, and the log line `step`
        // emits, are the ones the error carries.
        fcntl_setfd(socket, FdFlags::empty())
            .map_err(|err| step("fcntl(F_SETFD)")(err.into()))?;
        debug!(
            step = "fcntl(F_SETFD)",
            fd = socket.as_raw_fd(),
            "FD_CLOEXEC is cleared on the listener"
        );
    }
    Ok(())
}

/// Map a failed spawn: a missing `sandbox-exec` is the design's accepted
/// degradation, not a generic I/O error.
///
/// **The two halves are the two shapes the core publishes, and this plan
/// defines neither of them.** A `NotFound` is a *refusal* this platform can
/// name but the core cannot — `Error::Unsupported` with operator-facing words —
/// so it goes through [`refused`], which logs it where the `Error` is built;
/// everything else is something done *to a path*
/// (`spawn /usr/bin/sandbox-exec`), which is exactly what [`launch`] builds and
/// logs, with design §11's path and underlying failure named together. Both
/// come from `tinio-sandbox-core` (the phase-basis correction removed the
/// `step` this file used to define for itself).
#[cfg(target_os = "macos")]
fn spawn_error(source: std::io::Error) -> Error {
    if source.kind() == std::io::ErrorKind::NotFound {
        return refused(
            "spawn",
            Error::Unsupported {
                detail: "sandbox-exec is missing; the macOS sandbox cannot be applied",
            },
        );
    }
    launch("spawn", SANDBOX_EXEC, source)
}

#[cfg(test)]
mod tests {
    // ... as written in Step 2
}
```

Two details worth checking against the real API before you run anything: `std::net::TcpListener::bind` creates a close-on-exec socket, so `make_inheritable` *clears* a flag that is set rather than setting one that is not; and `Child::new` takes the wait closure **only** — the terminal record is not a slot the caller fills but the relay thread's return value, which `Child::with_relay` joins inside `Child::wait`. That is why `confine` moves its `std::process::Child` into the closure and attaches the handle the spawn returned (`Child::new(move || Ok(child.wait()?)).with_relay(relay)`), and why P0's constructor has no second parameter for a slot: the shared-slot design was removed before this plan was written.

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-macos && cargo clippy -p tinio-sandbox-macos --all-targets -- -D warnings`
Expected: PASS everywhere, including on the authoring host (every argv test here is pure — each one exercises the `command_args` builder; `confine` itself is called only by the stub-arm test on non-macOS targets). The tracing test runs on both: on macOS through the core's `step`, which this file calls rather than defines, and elsewhere through the stub arm's own refusal, and both require the event's `step` field to equal the error's. Confirm the lint stayed scoped to the three sites the design enumerates — a **fourth** attribute or block, and a block outside an enumerated module, is what this check is for, not a missing one: `rg -n '^\s*#!?\[allow\(unsafe_code\)\]' crates/tinio-sandbox-macos/src` prints exactly three lines, the attribute on `inherited_fd` in `launch.rs`, the one on `apply::validator`'s `fork` in `apply/mod.rs`, and `apply/libsystem.rs`'s module-level `#![allow(unsafe_code)]`; the words `unsafe_code` inside a doc comment are not sites, and `rg -n 'unsafe \{' crates/tinio-sandbox-macos/src/relay.rs crates/tinio-sandbox-macos/src/profile.rs` prints nothing; and `rg -c 'unsafe \{' crates/tinio-sandbox-macos/src/launch.rs` reports exactly one, in that same function. `cargo clippy` is green either way, because `deny` is what makes the allowance expressible at all.

- [x] **Step 6: Verify against a real `sandbox-exec` on a Mac**

On the Mac, run:

```bash
cargo test -p tinio-sandbox-macos
cargo clippy -p tinio-sandbox-macos --all-targets -- -D warnings
```

The unit tests must pass and clippy must be clean, under Apple's toolchain rather than the authoring host's — that combination is where a `cfg` mistake would first appear, and the `E0432` an ungated macOS-only import produces is exactly the kind of failure this step exists to catch. **There is no `--example probe` here**: `tests/startup.rs` is Task 4's probe and no task creates an example of that name, so a step that ran one would be discarding a failure it could not explain (`|| true` on a command that does not exist is not a check). The crate's own tests are the check this step can actually make.

---

### Task 3: Relaying termination signals to the confined child

**Files:**
- Create: `crates/tinio-sandbox-macos/src/relay.rs`
- Modify: `crates/tinio-sandbox-macos/src/lib.rs` (the `pub mod relay;` declaration)
- Modify: `crates/tinio-sandbox-macos/src/launch.rs` (the two statements that arm the relay in `confine`)
- Modify: `crates/tinio-sandbox-macos/Cargo.toml`
- Modify: `Cargo.toml` (workspace root — one new workspace dependency)

**Interfaces:**
- Consumes: `std::process::Child::id()` (the launcher's own handle, so the pid needs no new plumbing), `rustix::process::{getpid, kill_process, Pid, Signal}`. **`Signal` is `rustix::process::Signal` and not `rustix::signal::Signal`**: `rustix`'s `signal` module is private, and the type reaches the public API only through `process` (where `kill_process` takes it) and through the `runtime` module nobody here uses — so `rustix::signal::…` is an `E0603` (private module), which is why every site below imports it from `process`, on the same line as `Pid`.
- Produces: `tinio_sandbox_macos::relay::{on_stop, on_forwarded_stop, disarm, relay_decision, RELAYED_SIGNALS}` — `on_stop` and `disarm` are the two names the reconciliation fixes for a launcher platform (Windows exports the same pair, with no shared code), consumed by Task 2's `confine` (`on_stop`, right after the spawn — Step 4 below is where that call is added, because the module has to exist before anything names it) and by Task 8's `serve` (`disarm`, as the teardown closure `supervise` already takes). **`on_forwarded_stop` is the child-side half**, which no other phase has yet: the launcher's relay delivers a real signal into the confined child, so the child needs its own registration for the stop to land anywhere — Task 8's `confined` is its only caller, through the facade's `on_stop`.

Design §3 step 9 and decision 20's second obligation. **The grade is not uniform, and the plan should not imply that it is:**

| Platform | Grade | Why |
|---|---|---|
| Windows | **required** | A Windows container signals its main process and nothing else, so the launcher receives the stop and the confined server never does; the server is then terminated at the end of the grace period instead of draining, silently trading away the graceful shutdown the CLI contract promises. |
| macOS | **defensive** | There is no native container runtime; a container on a Mac is a Linux VM whose tinio takes the Linux path (in place, so the server is PID 1 and receives the signal directly), and `launchd` — the only supervisor that matters here — signals the job's whole process group by default. The hazard is not reachable through a standard macOS deployment. |

It is built anyway, for two reasons that are the design's own. The failure is **silent**: a launcher with no relay looks exactly like a server that ignores `SIGTERM`, so the defect gets diagnosed in the wrong process. And it keeps the launcher contract uniform, instead of making Windows's behaviour a special case someone has to remember. P2 owns the required half; this task owns the macOS half, and both are the same shape.

**Vocabulary, because three different things in this phase are called a relay.** The *event relay* (Task 2, `Observer`) moves the child's **bytes** out, child → parent, on stdout. The *signal relay* (this task) moves a **termination signal** in, parent → child. *Exit-status forwarding* (Task 9) carries the child's **exit code** back to the caller after the child has exited. They are independent: a launcher with no signal relay still passes every exit-status assertion in this plan, and one with no event relay still passes every signal assertion — which is exactly why each has its own test rather than an extra assertion in another task's.

- [x] **Step 1: Write the failing test**

`crates/tinio-sandbox-macos/src/relay.rs`, at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_live_child_is_the_relay_target() {
        // The set itself, not a copy of it: `RELAYED_SIGNALS` is what the
        // relaying thread hands to `relay_decision`, so a test that spelled
        // `[15, 2]` by hand would keep passing after the set changed — which is
        // exactly the drift this test exists to catch.
        for raw in RELAYED_SIGNALS {
            assert_eq!(relay_decision(4242, *raw, RELAYED_SIGNALS), Some(*raw));
        }
    }

    #[test]
    fn a_disarmed_relay_targets_nothing() {
        // `0` is the disarmed state, and a negative pid means "a process group"
        // to `kill(2)` — neither may ever be a relay target.
        for raw in RELAYED_SIGNALS {
            assert_eq!(relay_decision(0, *raw, RELAYED_SIGNALS), None);
            assert_eq!(relay_decision(-1, *raw, RELAYED_SIGNALS), None);
        }
    }

    #[test]
    fn only_the_relayed_set_is_forwarded() {
        // Both numbers are outside the set by construction, so a set that grew
        // to include one of them fails here instead of passing quietly:
        // `SIGKILL` cannot be caught or relayed, and `SIGHUP` is not a shutdown
        // request.
        assert!(!RELAYED_SIGNALS.contains(&9), "SIGKILL cannot be caught or relayed");
        assert!(!RELAYED_SIGNALS.contains(&1), "SIGHUP is not a shutdown request");
        assert_eq!(relay_decision(4242, 9, RELAYED_SIGNALS), None);
        assert_eq!(relay_decision(4242, 1, RELAYED_SIGNALS), None);
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-macos relay::`
Expected: FAIL — `cannot find function 'relay_decision'`. These three run on any host, which is deliberate: the relay's *policy* is testable everywhere, and only its mechanism needs a Mac.

- [x] **Step 3: Write `relay.rs`**

**Two comments in the block below count the crate's `unsafe` sites against a crate that had one, and they stay as written (a phase's record, `docs/superpowers/README.md`'s record rule)**: "the crate is `deny` with one enumerated `#[allow(unsafe_code)]` site" and "a safe crate still beats a **second** hand-written `unsafe` site under `deny`" both predate the in-process mechanism, which added two more — `apply::libsystem`'s `unsafe extern "C"` block and `apply::validator`'s `fork` — so the crate carries three today and any further hand-written site would be a **fourth** (design decision 14 enumerates all three, and this plan's Task 3 rationale further down states the corrected count). The argument survives the correction, because what it weighs is a further hand-written site rather than an exact number.

```rust
//! Relaying termination signals to the confined child while the launcher waits.
//!
//! Design §3 step 9 and decision 20. The obligation is **required on Windows**
//! (a container signals its main process and nothing else, so the launcher
//! receives the stop and the confined server never does) and **defensive on
//! macOS** (`launchd` signals the whole job, and a container on a Mac is a Linux
//! VM running the in-place Linux path). It is built on both because the failure
//! is silent — a missing relay looks exactly like a server that ignores
//! `SIGTERM` — and because it keeps the launcher contract uniform.
//!
//! # Mechanism
//!
//! `signal-hook` installs the handlers and writes one byte per signal into a
//! self-pipe, so **no code of ours runs in a signal handler at all**. A
//! dedicated thread drains that pipe and relays with one `kill` syscall per
//! signal. Everything that has to stay async-signal-safe is on the handler's
//! side of that boundary, and none of it is ours: what this module owns is a
//! normal thread, which may allocate, take a lock, and emit the `debug!` event
//! shared §7 asks for — each relayed signal with its number — after the `kill`
//! returns. An earlier version of this paragraph said the thread "touches
//! exactly two things … and does not log"; the event is the third, and the
//! distinction that matters is the one above rather than the count, because the
//! thread is not the signal path.
//!
//! # The alternative, and why it is not taken
//!
//! A hand-written `sigaction` through FFI is the only other correct option:
//! `rustix` deliberately does not wrap `sigaction` or `sigprocmask` (they sit in
//! its `not_implemented` list, exactly because libc may need to be involved in
//! signal handling).
//!
//! **The strongest argument this plan first made for `signal-hook` is void, and
//! it is better said plainly than left standing.** It argued that the crate
//! keeps `tinio-sandbox-macos` on `unsafe_code = "forbid"`. It does not, and it
//! cannot: the crate is `deny` with one enumerated `#[allow(unsafe_code)]` site
//! (the listener rebuild, Task 2; design decision 14), because no route to that
//! rebuild is safe. What remains is a smaller but real argument — a safe crate
//! still beats a **second** hand-written `unsafe` site under `deny`. A
//! hand-rolled handler is precisely where async-signal-safety bugs live, and
//! taking that route would mean adding the new site to decision 14's list — an
//! explicit, reviewable change that no `#[allow(unsafe_code)]` may make quietly. One small dependency is the cheaper
//! of the two, and its marginal cost is small: `signal-hook-registry`, the crate
//! that actually owns the handler and the self-pipe, is already in the tree
//! transitively. This is a preference between two working options rather than a
//! lint-forced choice, and a maintainer who rejects the dependency must take the
//! lint route explicitly — not read this plan as licence for a quiet
//! `#[allow(unsafe_code)]`.

// Every use below is macOS-only: the statics, their types, and `debug!` are
// named by `on_stop`/`disarm`/`start_relaying` and by nothing else. `relay_decision`
// — the one item this module exports on every target — is plain arithmetic.
#[cfg(target_os = "macos")]
use std::sync::OnceLock;
#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicI32, Ordering};

#[cfg(target_os = "macos")]
use tracing::debug;

/// The signals a launcher relays: the termination set a supervisor sends.
///
/// Anything outside this set is not a shutdown request, and forwarding
/// everything would make the launcher a signal-transparent proxy it is not.
///
/// **Written as raw numbers so the constant exists on every target**, which is
/// what lets the pure decision above be tested everywhere while the mechanism
/// stays macOS-only (`signal-hook` is a target dependency). The numbers are not
/// magic: the `const` assertion below fails at compile time on the one target
/// that links `signal-hook` if they ever stop being `SIGTERM` and `SIGINT`.
pub const RELAYED_SIGNALS: &[i32] = &[15, 2];

/// `SIGTERM` is 15 and `SIGINT` is 2 — checked against the names, not assumed.
///
/// Compiled only where `signal-hook` is, because that is where the names live;
/// elsewhere the numbers stand on their own, and both are POSIX constants that
/// do not vary by platform.
#[cfg(target_os = "macos")]
const _: () = {
    assert!(RELAYED_SIGNALS[0] == signal_hook::consts::signal::SIGTERM);
    assert!(RELAYED_SIGNALS[1] == signal_hook::consts::signal::SIGINT);
};

/// The child's pid while it lives; `0` once the relay is disarmed or the child
/// is gone.
///
/// **Gated, and it has to be**: the non-macOS arms below never touch either
/// static, so an ungated one would be a `dead_code` warning on every target that
/// is not a Mac — and the crate's own verification step demands a clean
/// `cargo clippy --all-targets -- -D warnings`.
#[cfg(target_os = "macos")]
static CHILD_PID: AtomicI32 = AtomicI32::new(0);

/// Whether the handlers and the relaying thread have been started.
#[cfg(target_os = "macos")]
static INSTALLED: OnceLock<std::io::Result<()>> = OnceLock::new();

/// What one received signal should do — plain data, so it is testable on any
/// host: the signal to relay, or `None` when the child is gone or the number is
/// not one this launcher relays.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::relay::relay_decision;
///
/// assert_eq!(relay_decision(4242, 15, &[15, 2]), Some(15));
/// assert_eq!(relay_decision(0, 15, &[15, 2]), None);
/// ```
pub fn relay_decision(child_pid: i32, raw: i32, relayed: &[i32]) -> Option<i32> {
    if child_pid <= 0 || !relayed.contains(&raw) {
        return None;
    }
    Some(raw)
}

/// Install the stop-signal handling for `child_pid`: from here until
/// [`disarm`], a termination
/// signal this process receives is forwarded to the child.
///
/// Called by the launcher **after** the child exists. Before that there is
/// nothing to relay to, and dying on a signal that arrives before the child is
/// the correct behaviour rather than a gap in the relay.
///
/// # Errors
///
/// Whatever installing the handlers or starting the thread reports; startup
/// fails closed on it (design §11). An operator who asked for a supervisor
/// should hear at startup that it cannot relay, rather than during a shutdown.
///
/// # Examples
///
/// ```no_run
/// tinio_sandbox_macos::relay::on_stop(4242)?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn on_stop(child_pid: i32) -> std::io::Result<()> {
    match INSTALLED.get_or_init(start_relaying) {
        Ok(()) => {
            CHILD_PID.store(child_pid, Ordering::SeqCst);
            debug!(step = "relay-arm", pid = child_pid, "termination signals are relayed to the confined child");
            Ok(())
        }
        Err(err) => Err(std::io::Error::new(err.kind(), err.to_string())),
    }
}

/// Disarm: a signal received after this is relayed nowhere.
///
/// The caller runs this in the teardown that `supervise` already takes, which
/// runs on every exit path (decision 15). It closes part of the pid-reuse
/// window: with no disarm, a signal arriving after the child has been reaped
/// would be forwarded to whatever owns that pid by then. **It cannot close the
/// whole of it, and this doc used to imply that it could** — the disarm runs in
/// teardown, after the wait has returned, so a signal that arrives between the
/// reap and the disarm is still forwarded to a pid the kernel may already have
/// reused. The window is a few instructions wide and no standard deployment
/// reaches it; what would close it is disarming before the reap, which would
/// cost the relay its last signal.
///
/// **`disarm` stops the relaying, and it does not restore `SIGTERM`/`SIGINT` to
/// their default disposition.** The handlers stay installed for the rest of the
/// process's life: this crate registers through `signal-hook`'s iterator API,
/// which owns the registration ids and does not hand them back, so there is no
/// `unregister` for it to call — and the relaying thread is left parked on a
/// self-pipe nobody writes to. The consequence is concrete and worth knowing:
/// after a teardown, this process **swallows** a later `SIGTERM` or `SIGINT`
/// instead of dying of it. `serve` never notices (its teardown is the last thing
/// before `std::process::exit`), and the relay tests rely on it deliberately —
/// the second test sends itself a `SIGTERM` and expects to survive. What it
/// means for a caller that keeps running is that a disarm is not a full
/// restoration; restoring the default would mean registering through
/// `signal_hook::low_level` directly, which is a different design and not this
/// one.
///
/// # Examples
///
/// ```
/// tinio_sandbox_macos::relay::disarm();
/// ```
#[cfg(target_os = "macos")]
pub fn disarm() {
    CHILD_PID.store(0, Ordering::SeqCst);
    debug!(step = "relay-disarm", "termination signals are no longer relayed");
}

/// Every other target: the same surface, so the workspace's whole CI matrix
/// builds this crate. Arming is refused rather than silently accepted, because
/// a caller that asks for a relay and gets none has a supervisor that cannot do
/// its job.
#[cfg(not(target_os = "macos"))]
pub fn on_stop(_child_pid: i32) -> std::io::Result<()> {
    Err(std::io::Error::other("the signal relay is only implemented for macOS"))
}

/// Every other target: the no-op counterpart of [`disarm`].
#[cfg(not(target_os = "macos"))]
pub fn disarm() {}

/// The **child-side** stop hook: run `hook` when the launcher's relay delivers a
/// termination signal to this process.
///
/// This is the other end of design §3 step 9, and it runs in a different process
/// from the one [`on_stop`] arms. The launcher forwards a real `SIGTERM` here, so
/// without a registration of its own the child dies of the default disposition —
/// a legitimate end for a run that asked to stop, but not the graceful shutdown
/// the CLI contract promises. What `hook` does is start that shutdown (Task 8's
/// `confined` hands it a `watch::Sender`), and it runs on the draining thread
/// rather than in a handler, which is the same boundary the mechanism section
/// above describes.
///
/// **The name is not `on_stop` because that one is the launcher's half.** The two
/// halves need two names in this crate; the facade presents this one under the
/// shared name `on_stop`, which is also what a Windows child calls with the same
/// closure shape (`2026-09-18-sandbox-p2-windows.md`).
///
/// # Errors
///
/// Whatever `signal-hook` reports while registering — the same refusal
/// [`on_stop`] propagates, and for the same reason: a child that cannot be told
/// to stop is one that gets killed at the end of a grace period instead of
/// draining.
///
/// # Examples
///
/// ```no_run
/// let (tx, _rx) = std::sync::mpsc::channel();
///
/// tinio_sandbox_macos::relay::on_forwarded_stop(move || {
///     let _ = tx.send(());
/// })?;
/// # Ok::<(), std::io::Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn on_forwarded_stop(hook: impl Fn() + Send + 'static) -> std::io::Result<()> {
    use signal_hook::iterator::Signals;

    let mut signals = Signals::new(RELAYED_SIGNALS)?;
    std::thread::Builder::new().name("tinio-sandbox-stop".into()).spawn(move || {
        for raw in signals.forever() {
            debug!(step = "stop-hook", signal = raw, "the confined child received a forwarded stop");
            hook();
        }
    })?;
    Ok(())
}

/// Every other target: the hook is never run, because nothing forwards a stop to
/// a process that is not a child.
#[cfg(not(target_os = "macos"))]
pub fn on_forwarded_stop(_hook: impl Fn() + Send + 'static) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "macos")]
fn start_relaying() -> std::io::Result<()> {
    use rustix::process::{Pid, Signal};
    use signal_hook::iterator::Signals;

    let mut signals = Signals::new(RELAYED_SIGNALS)?;
    std::thread::Builder::new().name("tinio-sandbox-signals".into()).spawn(move || {
        for raw in signals.forever() {
            let child = CHILD_PID.load(Ordering::SeqCst);
            let Some(raw) = relay_decision(child, raw, RELAYED_SIGNALS) else { continue };
            let (Some(pid), Some(signal)) =
                (Pid::from_raw(child), Signal::from_named_raw(raw))
            else {
                continue;
            };
            // Best effort: a child that has already gone is the normal case at
            // the end of a run.
            let _ = rustix::process::kill_process(pid, signal);
            // After the `kill`, on a normal thread — this is not a signal
            // handler, and `debug!` here is what shared §7 asks for: each
            // relayed signal with its number. The handler's side of the boundary
            // is still signal-hook's own self-pipe write and nothing of ours.
            debug!(step = "signal", pid = child, signal = raw, "relaying a termination signal");
        }
    })?;
    Ok(())
}
```

- [x] **Step 4: Wire it into the manifest, `lib.rs` and `launch.rs`**

`crates/tinio-sandbox-macos/Cargo.toml` — one dependency beside `rustix`, in the same target group:

```toml
[target.'cfg(target_os = "macos")'.dependencies]
rustix = { workspace = true, features = ["process"] }
signal-hook = { workspace = true, features = ["iterator"] }
```

Root `Cargo.toml`, in `[workspace.dependencies]`, keeping the block alphabetical (between `sha2` and `smart-default`):

```toml
signal-hook = "0.3"
```

This is the phase's one new third-party dependency, and the design's Bookkeeping now carries it explicitly ("One new third-party dependency: `signal-hook`, target-gated to `tinio-sandbox-macos`"), justified there against the same alternative this task weighs: a hand-written `sigaction`, which would add a **fourth** `unsafe` site to a crate that already carries three — the listener rebuild of Task 2, `apply::libsystem`'s `unsafe extern "C"` block and `apply::validator`'s `fork` — for a requirement no standard macOS deployment can reach. Note what that argument is *not*: it is no longer "the dependency keeps the crate on `forbid`", because the crate is not on `forbid` and could not be (decision 14 makes every backend `deny` with its sites enumerated); what is left is that a second hand-written `unsafe` site costs more justification than one small safe dependency. A purpose-built, safe crate whose entire job is a handler plus a self-pipe is the smaller change of the two, and `signal-hook-registry` — the crate that owns the handler — is already in the tree transitively. It is target-gated, so a Linux or Windows build of this workspace does not compile it for this crate.

`crates/tinio-sandbox-macos/src/lib.rs`:

```rust
pub mod relay;
```

**And `launch.rs` gains the arming call**, at the point Task 2 left for it — in `confine`, immediately after the spawn, where step 7's comment sits:

```rust
    // The signal relay (design §3 step 9, decision 20) — a different relay from
    // the event one above: that one moves the child's *bytes* out, this one
    // moves the launcher's *signals* in. Armed **after** the spawn because
    // before that there is nothing to relay to — dying on a signal that arrives
    // before the child exists is correct rather than a gap.
    let pid = i32::try_from(child.id()).map_err(|_| Error::Unsupported {
        detail: "the child's process id does not fit in an i32",
    })?;
    crate::relay::on_stop(pid).map_err(step("install stop-signal handling"))?;
```

This is why the module lands in this task rather than earlier: `pub mod relay;` and the call that names `crate::relay` are written together, so Task 2's own `cargo test` compiles and Task 3's does too — there is no intermediate state in which `confine` names a module that does not exist.

and the facade (`crates/tinio-sandbox/src/lib.rs`) gains the one relay symbol its call site needs. It is a **platform-neutral name with a `#[cfg]`-selected body**, not a re-export, because `serve`'s supervision is not itself behind a `#[cfg]`: the Linux backend returns `InPlace` and never reaches the teardown, but the code has to compile there.

```rust
/// Run `hook` when a stop forwarded by the launcher reaches this process.
///
/// The **child-side** half of the relay pair, and the shape both launcher
/// platforms call: `2026-09-18-sandbox-p2-windows.md`'s `confined` calls exactly
/// this name with exactly this closure. Real on macOS, where the launcher's
/// relay delivers a `SIGTERM` and the child needs a registration of its own to
/// give it somewhere to land; a no-op on Linux, which confines in place — the
/// process the supervisor signals *is* the server there, so nothing is forwarded
/// to it and there is nothing to hook.
pub fn on_stop(hook: impl Fn() + Send + 'static) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        tinio_sandbox_macos::relay::on_forwarded_stop(hook)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = hook;
        Ok(())
    }
}
```

```rust
/// Disarm the launcher's signal relay, if this platform has one.
///
/// A no-op where the process confines in place (Linux, decision 6) and where
/// the launcher does not relay yet (Windows, until P2); real on macOS. It is
/// the teardown `serve` hands to `supervise`, so it runs on every exit path
/// (decision 15) — including the one where the launch failed.
pub fn disarm_signal_relay() {
    #[cfg(target_os = "macos")]
    tinio_sandbox_macos::relay::disarm();
}
```

**The two relay names are the launcher's and the child's, in that order**, and that is why they are two wrappers rather than one: `on_stop` is called by the process that is *told* to stop, `disarm_signal_relay` by the process that *forwards* the stop, and each has a `None`-shaped arm on the platform that does neither. Windows supplies its own pair behind the same two names, which is what shared §4 means by the pair being the whole contract.

Everything else the launcher needs on the child side is named only inside `serve`'s macOS-gated arms, so the facade re-exports it under the same gate rather than wrapping it:

```rust
#[cfg(target_os = "macos")]
pub use tinio_sandbox_macos::launch::listener as adopted_listener;
```

`Adopted` and `adopt` need no re-export: they are the core's, and the facade already re-exports the core's surface in full. Only the platform-side rebuild is this crate's.

That split is deliberate: a name the call site uses on every platform gets a neutral wrapper, a name it uses only inside an already-branched arm gets a gated re-export. Either way `serve` names the facade and never a platform crate, which is what keeps the dependency graph acyclic (decision 3).

- [x] **Step 5: Run the pure tests**

Run: `cargo test -p tinio-sandbox-macos relay:: && cargo clippy -p tinio-sandbox-macos --all-targets -- -D warnings`
Expected: PASS on the authoring host — three tests, no warnings. Confirm this module added no `unsafe`: `rg -n 'unsafe' crates/tinio-sandbox-macos/src/relay.rs` must print nothing — the listener's is decision 14's `launch::inherited_fd`, which its two callers `launch::listener` and `launch::control` reach (Task 2), and a hit here would be a site design decision 14 does not enumerate.

- [x] **Step 6: Write the macOS integration test**

`crates/tinio-sandbox-macos/tests/relay.rs`:

```rust
//! The relay, measured: a signal sent to the launcher reaches the child, and
//! the launcher survives it (design §3 step 9, decision 20).
//!
//! This is deliberately *not* part of the confined-server test. A launcher with
//! no relay passes every exit-status assertion there; only a test that signals
//! the launcher itself can tell the relay apart from the forwarding.

#![cfg(target_os = "macos")]

use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The two tests below share this module's process-wide state — the armed pid
/// and the installed handlers — and **libtest runs tests in parallel by
/// default**, so without this lock they race: the disarmed test's `disarm` can
/// land between the first test's `on_stop` and its signal, and the first test
/// then fails for a reason that has nothing to do with the relay.
static SERIAL: Mutex<()> = Mutex::new(());

#[test]
fn a_signal_to_the_launcher_reaches_the_child_and_not_the_launcher() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    // A stand-in child that outlives the test unless it is signalled. The real
    // one is `sandbox-exec` and is covered by Task 9's end-to-end run.
    let mut child = Command::new("/bin/sh").args(["-c", "sleep 30"]).stdout(Stdio::null()).spawn().expect("spawn");
    let pid = child.id() as i32;

    tinio_sandbox_macos::relay::on_stop(pid).expect("install the stop-signal handling");

    // Signal *this* process, the way a supervisor signals the launcher. The
    // handler `on_stop` installed is what keeps this process alive: before the
    // implementation exists, this line kills the test binary, and that is the
    // failure this test is written to show.
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::TERM)
        .expect("signal the launcher");

    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => break status,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                panic!("the child outlived the relayed signal");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    };

    // It died of the relayed signal, not of something else.
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(status.signal(), Some(rustix::process::Signal::TERM.as_raw()), "{status:?}");

    tinio_sandbox_macos::relay::disarm();
}

#[test]
fn a_disarmed_relay_leaves_the_child_alone() {
    let _serial = SERIAL.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    let mut child = Command::new("/bin/sh").args(["-c", "sleep 30"]).stdout(Stdio::null()).spawn().expect("spawn");
    tinio_sandbox_macos::relay::on_stop(child.id() as i32).expect("install");
    tinio_sandbox_macos::relay::disarm();

    // The handler stays installed after the disarm (that is `disarm`'s documented
    // residue), so this line does not kill the test binary — which is what makes
    // the assertion below about the *relay* and not about this process surviving.
    rustix::process::kill_process(rustix::process::getpid(), rustix::process::Signal::TERM)
        .expect("signal the launcher");

    std::thread::sleep(Duration::from_millis(200));
    assert!(child.try_wait().expect("try_wait").is_none(), "a disarmed relay must not forward");
    let _ = child.kill();
    let _ = child.wait();
}
```

Both tests leave `SIGTERM` handled in the test binary for the rest of its run, which is `disarm`'s documented residue rather than an accident — the handler stays installed because this crate registers through `signal-hook`'s iterator API, which keeps its registration ids to itself. The second test depends on it: a restored default disposition would kill the process that sends itself the signal. What it means for the binary as a whole is worth knowing before a later test sends itself one: it will be swallowed, not fatal, and a test that wants to die has to use a signal outside `RELAYED_SIGNALS`.

- [x] **Step 7: Run the integration test to verify it fails, then passes**

Run: `cargo test -p tinio-sandbox-macos --test relay -- --nocapture`
Expected: FAIL before Step 3 — the test process dies on the signal it sends itself, and the harness reports a killed test binary rather than an assertion failure. Then, with the implementation in place, PASS both tests: the first asserts the child died of `SIGTERM` while the test process lived, the second asserts a disarmed relay forwards nothing.

---

### Task 4: A confined process starts at all

**Files:**
- Create: `crates/tinio-sandbox-macos/tests/harness/mod.rs`
- Create: `crates/tinio-sandbox-macos/tests/startup.rs`
- Modify: `crates/tinio-sandbox-macos/src/profile.rs` (the candidate set grows here)

**Interfaces:**
- Consumes: `tinio_sandbox_macos::{confine, launch::listener}`, `tinio_sandbox_core::{adopt, AccessMode, Adopted, Confinement, LaunchPlan, Result, Sandbox}`.
- Produces: `harness::{CHILD_ENV, LAUNCH_ENV, PARAMS_ENV, Report, Child, child, is_child_process, launch, line, finish, run_confined, run_pruned, remove_rule}` — Tasks 5, 6 and 7 all use this harness, and every child-side probe gets its parameters and its listener from `Child`. `launch` is the launcher-copy entry point each probe calls from its `None` arm (see the module docs for why the copy exists).

This is the phase's first real question: **does a `(deny default)` profile leave enough for a Rust process to start?** Design §5 says the exact set is empirical and the Risks table says "the first task of P3 is to make a confined server start and serve one request, not to refine the profile". A first run that fails is the expected outcome, not a mistake.

- [x] **Step 1: Write the failing test**

`crates/tinio-sandbox-macos/tests/startup.rs`:

```rust
//! The first question P3 asks: does a `(deny default)` profile leave enough for
//! a Rust process to start, size a multi-threaded runtime and read its data?
//!
//! The confined child is this same test binary, re-executed through the real
//! launcher — so the payload on stdin, the listener descriptor and the profile
//! are all exercised, not simulated. It is launched by a **launcher copy** of
//! this binary rather than by the test process, which is what puts this probe's
//! name in the child's argv: the harness module's docs have the whole argument.

#![cfg(target_os = "macos")]

mod harness;

use std::thread;

use tinio_sandbox_core::{AccessMode, Sandbox};

use harness::Report;

/// Runs inside the confinement and reports what a Rust process can still do.
#[test]
fn child_startup() {
    let Some(child) = harness::child() else {
        // Not the confined child: the test runner or the launcher copy, which
        // is the process this harness spawns to run `confine`. `launch` is a
        // no-op in the first and the whole of the launch in the second.
        harness::launch();
        return;
    };
    let mut log = String::new();
    log.push_str(&harness::line("payload", child.payload));
    log.push_str(&harness::line(
        "available_parallelism",
        match thread::available_parallelism() {
            Ok(n) => n.get().to_string(),
            Err(err) => format!("error({err})"),
        },
    ));
    let threads = thread::scope(|scope| {
        let handles: Vec<_> = (0..4).map(|_| scope.spawn(|| 1u32)).collect();
        handles.into_iter().map(|h| h.join().map(|v| v).unwrap_or(0)).sum::<u32>()
    });
    log.push_str(&harness::line("threads", threads.to_string()));
    log.push_str(&harness::line(
        "read_marker",
        match std::fs::read_to_string(child.param("marker")) {
            Ok(body) => body.trim().to_string(),
            Err(err) => format!("error({})", err.kind()),
        },
    ));
    harness::finish(&child, &log);
}

/// The parent half: the process starts, threads and the runtime sizing work.
#[test]
fn a_confined_process_starts() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_startup` and nothing else; the guard below is what keeps the parent
    // half from running inside it.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("create root");
    let marker = root.join("marker.txt");
    std::fs::write(&marker, "confined\n").expect("seed");

    // `.device_files()`, because `serve` asks for them (design §4) and the
    // pruning run measures the shipped profile: a sandbox without them would
    // leave the `DEVICE_RULES` out of the text the pruner renders, so those ids
    // could never get a verdict.
    let sandbox = Sandbox::new().expose(&root, AccessMode::ReadWrite).device_files();
    let params = [("marker", marker.display().to_string())];

    // Two modes, one probe. Normally the child is launched through `confine`,
    // the product path (by a launcher copy, so its argv carries the filter
    // below); under `scripts/prune-profile.sh` the harness launches
    // `sandbox-exec` itself so that a single baseline rule can be dropped, and
    // that child has no payload — which is why the expectation below differs.
    let pruning = std::env::var_os("TINIO_SANDBOX_MACOS_PRUNE").is_some();
    let report: Report = if pruning {
        harness::run_pruned(&sandbox, &params, "child_startup")
    } else {
        harness::run_confined(&sandbox, &params, "child_startup")
    };

    report.expect("payload", if pruning { "none" } else { "ok" });
    report.expect("threads", "4");
    report.expect("read_marker", "confined");
    let parallelism = report.value("available_parallelism").unwrap_or("missing");
    assert_ne!(parallelism, "missing", "{}", report.text());
    assert!(parallelism != "error(Operation not permitted)", "{}", report.text());
    assert_ne!(parallelism, "0", "{}", report.text());
}
```

- [x] **Step 2: Write the harness**

`crates/tinio-sandbox-macos/tests/harness/mod.rs`:

```rust
//! The re-exec harness shared by the macOS confinement tests.
//!
//! A confined child is a copy of this test binary, launched through the crate's
//! real `confine` — so the profile, the listener's descriptor handover and the
//! payload on stdin are all exercised. The child reports through a file inside a
//! writable exposure rather than on stdout, because stdout is the event channel
//! (decision 19) and the report is not an event.
//!
//! **There are three roles in one binary, and the argv filter is what separates
//! them.** `confine` re-invokes *its own* process (shared §3): it derives the
//! child's program from `current_exe()` and its argv from `args_os()`, so the
//! filter the confined child runs under is the filter the process that called
//! `confine` was started with — and libtest reads a positional argument as a
//! filter, which makes the listener-descriptor element the launcher appends one
//! too. The child therefore runs the tests *its launcher's* argv selects, and
//! the only way to select one exact probe — short of a test seam on the
//! production launcher, which the next paragraph rejects — is for the process
//! calling `confine` to be one whose argv already names it. **That is the design,
//! and it is why [`run_confined`] re-executes this binary as a launcher copy**
//! whose own argv names the probe: the copy calls `confine`, the child inherits
//! the filter and runs exactly the probe, and the original process reads the
//! report the probe wrote. Without the copy the child would carry the descriptor
//! element as its only filter, libtest would print `running 0 tests`, exit 0 and
//! write no report, and the parent would fail with a missing file that says
//! nothing about why.
//!
//! **The alternative is a seam this platform does not have.** A macOS analogue of
//! P2's `Options { program, args }` — the test-only override that lets a Windows
//! child be a role function of the test binary instead of the suite — is what
//! shared §3 grants Windows and not macOS, and adding it here would put a
//! test-only field on the production launcher to buy what one re-exec already
//! buys.
//!
//! The three roles, then: the **test runner** (plain `cargo test`), the
//! **launcher copy** (called by [`run_confined`], identified by [`LAUNCH_ENV`],
//! runs [`launch`]), and the **confined child** (identified by the payload on
//! stdin — decision 17 — or, in the pruning experiment, by [`CHILD_ENV`]).

use std::ffi::OsString;
use std::fs;
use std::io::Write as _;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tinio_sandbox_core::{AccessMode, Confinement, LaunchPlan, Sandbox};
use tinio_sandbox_macos::confine;

/// Selects the child role in the re-executed test binary.
pub const CHILD_ENV: &str = "TINIO_SANDBOX_MACOS_CHILD";

/// Carries the child's parameters to the launcher copy. Used by
/// [`run_confined`] (whose launcher copy has no payload yet) and by the pruning
/// experiment, which never has one; the product path passes the same text in the
/// payload's configuration bytes.
pub const PARAMS_ENV: &str = "TINIO_SANDBOX_MACOS_PARAMS";

/// Selects the **launcher copy** role — the process [`run_confined`] re-executes
/// so that the confined child inherits a filter.
pub const LAUNCH_ENV: &str = "TINIO_SANDBOX_MACOS_LAUNCH";

/// The confined child's report, as `key=value` lines.
pub struct Report(String);

impl Report {
    /// The report as the child wrote it.
    pub fn text(&self) -> &str {
        &self.0
    }

    /// The value of `key`, if the child reported it.
    pub fn value(&self, key: &str) -> Option<&str> {
        let prefix = format!("{key}=");
        self.0.lines().find_map(|line| line.strip_prefix(&prefix))
    }

    /// Assert the child reported exactly this value. The whole report is the
    /// failure message: on a macOS run it is the only evidence there is.
    pub fn expect(&self, key: &str, value: &str) {
        assert_eq!(self.value(key), Some(value), "report:\n{}", self.0);
    }
}

/// One `key=value` line.
pub fn line(key: &str, value: impl std::fmt::Display) -> String {
    format!("{key}={value}\n")
}

/// Everything a confined child needs, adopted **once**.
///
/// This is **not** `tinio_sandbox_core::Child` — that type is the launcher's
/// handle on a running child, and it is never in scope in this module.
///
/// The payload arrives once and only once (design §3 step 7), so `adopt`
/// must not be called twice in one process: stdin is consumed by the first read
/// and the second would see an empty stream. This type is how every child-side
/// probe gets its parameters and its listener without a second call.
pub struct Child {
    /// The payload, when the real launcher started this process. `None` in the
    /// pruning experiment, which has no payload.
    pub adopted: Option<tinio_sandbox_core::Adopted>,
    /// This run's parameters.
    pub params: Vec<(String, String)>,
    /// `ok` when the payload validated, `none` in the pruning experiment. A
    /// test asserting `payload=ok` is asserting the whole handover.
    pub payload: &'static str,
    /// The socket, rebuilt from the descriptor **on the first call and never
    /// again** — see [`Child::listener`].
    listener: std::sync::OnceLock<std::io::Result<std::net::TcpListener>>,
}

impl Child {
    /// One parameter, or a panic naming the whole set.
    pub fn param(&self, key: &str) -> String {
        self.params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("no `{key}` in {:?}", self.params))
    }

    /// The listener the launcher handed over, rebuilt from the descriptor —
    /// **once**, and the same socket on every call.
    ///
    /// The once is not tidiness. `listener` calls `OwnedFd::from_raw_fd`, whose
    /// contract is at most one call per descriptor number (Task 2's `# Safety`):
    /// a repeatable accessor is the easiest way to break it, and a probe that
    /// asked twice would take ownership of one descriptor twice and close it
    /// twice. `OnceLock` also hands a later caller the first caller's error
    /// rather than a second rebuild.
    pub fn listener(&self) -> std::io::Result<&std::net::TcpListener> {
        self.listener
            .get_or_init(|| {
                let adopted = self
                    .adopted
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("the pruning experiment has no payload"))?;
                tinio_sandbox_macos::launch::listener(adopted)
            })
            .as_ref()
            .map_err(|err| std::io::Error::new(err.kind(), err.to_string()))
    }
}

/// Whether this process is a confined child or the pruning experiment's,
/// **without consuming the payload**.
///
/// The launcher's descriptor element in argv is the marker on the product path;
/// the pruning experiment, which has no payload by construction, uses
/// [`CHILD_ENV`] instead. Every test — probe and parent alike — checks this
/// first, and the check is also what keeps the **launcher copy** out of the
/// probe: that process runs the probe's own test (it is the filter) but carries
/// no descriptor element, so it takes the other branch and calls
/// [`launch`]. A probe that adopted the payload twice would find the pipe
/// drained, which is why the payload is read in exactly one place — [`child`].
pub fn is_child_process() -> bool {
    std::env::args_os().any(|arg| tinio_sandbox_core::is_listener_fd_arg(&arg))
        || std::env::var_os(CHILD_ENV).is_some()
}

/// `Some` when this process is a confined child, `None` when it is the runner.
///
/// Both discriminators are the design's: the payload (decision 17 — never an
/// environment marker) for the product path, and the environmental mark of the
/// pruning experiment, whose child has no payload by construction. Calling this
/// reads the pipe, so it is called **exactly once per process**, by the probe.
pub fn child() -> Option<Child> {
    let adopted = tinio_sandbox_core::adopt(std::env::args_os(), std::io::stdin().lock())
        .ok()
        .flatten();
    if adopted.is_none() && std::env::var_os(CHILD_ENV).is_none() {
        return None;
    }
    let raw = match &adopted {
        Some(adopted) => adopted.config().to_vec(),
        None => std::env::var(PARAMS_ENV).unwrap_or_default().into_bytes(),
    };
    let payload = if adopted.is_some() { "ok" } else { "none" };
    Some(Child { adopted, params: parse_params(&raw), payload, listener: Default::default() })
}

fn parse_params(bytes: &[u8]) -> Vec<(String, String)> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Write the report where the parent will read it, then end the child process.
///
/// The path is always inside a writable exposure — writing it is itself a test
/// of the write rule. Ending the process rather than returning is deliberate:
/// the report is the whole of what this child is for, and exiting keeps it the
/// last thing the child does (the filter means libtest selected the probe and
/// nothing else, so nothing else was pending).
pub fn finish(child: &Child, log: &str) -> ! {
    let path = child.param("report");
    fs::write(&path, log).unwrap_or_else(|err| panic!("write {path}: {err}"));
    std::process::exit(0);
}

/// Run `test_name` confined through the crate's real launcher, and read back its
/// report.
///
/// **The confined child is launched by a launcher copy of this binary, and that
/// is the design rather than a detour** — the module docs carry the argument.
/// `confine` derives the child's argv from the calling process's own, so the
/// process that calls it has to be one whose argv already selects this probe, and
/// this function is that process's spawner: it re-executes `current_exe()` with
/// [`child_argv`] naming the probe and [`LAUNCH_ENV`] marking the launcher role,
/// so the child the copy confines inherits a filter that matches and runs exactly
/// the probe. The descriptor element the launcher appends is then a second,
/// harmless filter — the first one is what selected the test. Were the copy not
/// there, that element would be the child's *only* filter: libtest would run
/// nothing (`running 0 tests`, exit 0), no report would be written, and the
/// missing file would be all the parent could report.
pub fn run_confined(sandbox: &Sandbox, extra: &[(&str, String)], test_name: &str) -> Report {
    let report = report_path(sandbox);
    let params = launcher_parameters(sandbox, extra, &report);

    let status = Command::new(std::env::current_exe().expect("current exe"))
        .args(child_argv(test_name))
        .env(LAUNCH_ENV, "1")
        .env(PARAMS_ENV, &params)
        .stdout(Stdio::null())
        .status()
        .expect("spawn the launcher copy");
    assert!(status.success(), "the launcher copy exited with {status:?}");

    let text = fs::read_to_string(&report).unwrap_or_else(|err| {
        panic!("the confined child wrote no report ({err}); the launcher exited with {status:?}")
    });
    Report(text)
}

/// The launcher role: `confine`, run in the **launcher copy** of this binary.
///
/// A no-op in any process that [`run_confined`] did not spawn — the ordinary
/// test runner and the confined child alike — so every probe's `None` arm can
/// call it unconditionally, and the child (whose filter also selects the probe)
/// returns from it immediately.
pub fn launch() {
    if std::env::var_os(LAUNCH_ENV).is_none() {
        return;
    }
    let params_text = std::env::var(PARAMS_ENV).unwrap_or_default();
    let sandbox = sandbox_from_params(&parse_params(params_text.as_bytes()));

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");

    // The launcher owns the port (decision 16) and hands the descriptor over in
    // argv (design §3 step 7). The payload carries the configuration text and
    // nothing else; the launcher neither parses it nor rewrites it.
    let plan = LaunchPlan::for_launch(params_text).with_listener(listener);

    let mut child = match confine(&sandbox, &plan).expect("confine") {
        Confinement::Supervise(child) => child,
        Confinement::InPlace => panic!("macOS confines a child, never in place"),
    };

    // One inbound connection, written while the child is running, because the
    // child's `accept` probe would otherwise block until the harness gave up.
    // A child that never accepts pays one connection the backlog absorbs and
    // then closes; a child that does accept reads exactly this line, which is
    // how the network test proves it reached the inherited listener.
    if let Ok(mut stream) = TcpStream::connect(addr) {
        let _ = stream.write_all(b"ping\n");
        let _ = stream.flush();
    }

    // The wait blocks, because the core's `Child` hides the process handle
    // behind its wait closure: the harness cannot poll or kill it. That is why
    // every probe is written never to block indefinitely — the accept probe's
    // connection is the write above — and why Task 9's end-to-end test, which
    // owns a real `std::process::Child`, is where a timeout can actually kill.
    let status = child.wait().unwrap_or_else(|err| panic!("wait for the confined child: {err}"));
    assert!(status.success(), "the confined child exited with {status:?}");
}

/// The parameters the launcher copy needs, which is [`parameters`] plus the
/// **sandbox itself**: the copy is a different process, so the exposures the
/// test built have to travel with it.
///
/// The format is the same `key=value` text, and the reason a path can ride in it
/// at all is that this crate refuses an exposure carrying a control character.
/// **The refusal is a named place**: the `control character` arm of Task 1's
/// `validate_path` in `profile.rs` — the check `profile` runs over every exposure
/// before a rule is rendered for it, held by that task's
/// `control_characters_are_refused` — so no exposure that reaches a profile can
/// contain the newline this format separates its lines with.
///
/// **The refusal runs where the profile is rendered, which on this channel is one
/// step after this function.** That ordering is safe for a reason worth stating
/// rather than assuming: the exposures here are the test's own, built by the
/// process that reads the answer, and the product path never uses this format at
/// all — its configuration travels as the payload's JSON. The remaining values on
/// the channel are the harness's own too: the `report` path, the `listen`
/// address, and each probe's `extra`.
fn launcher_parameters(sandbox: &Sandbox, extra: &[(&str, String)], report: &Path) -> String {
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let mut text = parameters(extra, report, addr);
    for exposure in sandbox.exposures() {
        let access = if exposure.access.is_read_only() { "ro" } else { "rw" };
        text.push_str(&format!("expose={access}:{}\n", exposure.host.display()));
    }
    text.push_str(&format!("devices={}\n", u8::from(sandbox.device_files_enabled())));
    text
}

/// The launcher copy's [`Sandbox`], rebuilt from its parameters.
///
/// The same set the test built, in the same order, because the profile is
/// rendered from it: an exposure lost in the round trip would be a rule missing
/// from the profile under test, and the probe would be measuring something the
/// product never launches.
fn sandbox_from_params(params: &[(String, String)]) -> Sandbox {
    let mut sandbox = Sandbox::new();
    for (key, value) in params {
        if key == "expose" {
            let (access, host) = value.split_once(':').expect("access:path");
            let access = if access == "rw" { AccessMode::ReadWrite } else { AccessMode::ReadOnly };
            sandbox = sandbox.expose(host, access);
        }
    }
    if params.iter().any(|(key, value)| key == "devices" && value == "1") {
        sandbox = sandbox.device_files();
    }
    sandbox
}

/// Launch the profile **with one rule removed**, without the launcher.
///
/// This exists only for `scripts/prune-profile.sh`: the necessity of a baseline
/// allowance is measured by rerunning the probe without it, and `confine`
/// renders its own profile so it cannot express "one rule fewer". **The profile
/// rendered here is the one the launch renders** — `profile(&with_canonical_paths(…))`,
/// device rules included, because the caller's sandbox carries the flag — so the
/// removed rule is the only difference from a real launch, which is the property
/// the whole experiment rests on. The child gets no payload here, which is why
/// this mode passes its parameters through the environment. Never use this to
/// assert product behaviour — that is what [`run_confined`] is for.
pub fn run_pruned(sandbox: &Sandbox, extra: &[(&str, String)], test_name: &str) -> Report {
    let report = report_path(sandbox);
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("addr");
    let params = parameters(extra, &report, addr);
    let id = std::env::var("TINIO_SANDBOX_MACOS_PRUNE").unwrap_or_default();

    let rendered =
        tinio_sandbox_macos::profile::profile(&tinio_sandbox_macos::profile::with_canonical_paths(sandbox))
            .expect("profile");
    let pruned = match remove_rule(&rendered.text, &id) {
        Some(pruned) => pruned,
        // **Distinct from "required", and it has to stay distinct.** The id is
        // not in the rendered profile at all — a `DEVICE_RULES` id on a sandbox
        // that never called `device_files()`, for instance — so this run can say
        // nothing about necessity. The script greps for the line below; without
        // it, a bare failure is classified as "required" and a rule is then
        // documented as load-bearing on the strength of a run that never removed
        // it.
        None => {
            eprintln!("prune: rule-not-present id={id}");
            panic!("rule `{id}` is not in the rendered profile — nothing to remove");
        }
    };

    // The pruning experiment builds its own `sandbox-exec` invocation, so it also
    // has to build its own `-D` elements — the pruned text still refers to every
    // parameter, and a parameter the profile names but the launch does not define
    // is a compile error (exit 65), not a narrower profile. That includes the
    // program parameter the `process-exec` literal resolves to, which the launch
    // gets from `command_args`: here it is the same `current_exe()` the program
    // element below carries, so a pruned run measures the profile the launch
    // would render rather than one with the exec rule left unsatisfied.
    let program = std::env::current_exe().expect("current exe");
    let mut assignments = Vec::with_capacity(rendered.params.len() * 2 + 2);
    for (name, value) in &rendered.params {
        let mut assignment = OsString::from(name.as_str());
        assignment.push("=");
        assignment.push(value);
        assignments.push(OsString::from("-D"));
        assignments.push(assignment);
    }
    let mut program_param = OsString::from("P=");
    program_param.push(&program);
    assignments.push(OsString::from("-D"));
    assignments.push(program_param);

    let status = Command::new(tinio_sandbox_macos::SANDBOX_EXEC)
        .arg("-p")
        .arg(&pruned)
        .args(&assignments)
        .arg(&program)
        .args(child_argv(test_name))
        .env(CHILD_ENV, "1")
        .env(PARAMS_ENV, &params)
        .status()
        .expect("spawn sandbox-exec");

    let text = fs::read_to_string(&report).unwrap_or_else(|err| {
        panic!("the pruned child wrote no report ({err}); it exited with {status:?}\nprofile:\n{pruned}")
    });
    Report(text)
}

/// Where the child writes its report: inside the first **writable** exposure.
///
/// A confined process can only write where it was granted, so a report outside
/// the exposure set is refused by the very profile under test — and the
/// read-only run has no writable served root at all, which is why this picks
/// the first `ReadWrite` exposure rather than the first exposure. A stale report
/// from an earlier run is removed first, so a missing file always means "the
/// child could not write", never "an old file survived".
fn report_path(sandbox: &Sandbox) -> PathBuf {
    let host = sandbox
        .exposures()
        .iter()
        .find(|exposure| !exposure.access.is_read_only())
        .map(|exposure| exposure.host.clone())
        .expect("a confinement test needs at least one writable exposure");
    let path = host.join(".tinio-sandbox-test-report.txt");
    let _ = fs::remove_file(&path);
    path
}

/// Drop the line whose `; tinio:<id>` marker matches, if `id` is non-empty.
///
/// `None` means the id is **not in the text at all**, which is a different
/// answer from "the rule was removed and nothing broke" and must not be
/// reported as one: an empty `id` returns the text unchanged, and an id whose
/// rule is absent returns `None` so the caller can say so.
pub fn remove_rule(text: &str, id: &str) -> Option<String> {
    if id.is_empty() {
        return Some(text.to_string());
    }
    let marker = format!("; tinio:{id} ");
    let kept: Vec<&str> = text.lines().filter(|line| !line.contains(&marker)).collect();
    if kept.len() == text.lines().count() {
        return None;
    }
    let mut out = kept.join("\n");
    out.push('\n');
    Some(out)
}

fn child_argv(test_name: &str) -> Vec<OsString> {
    vec![
        OsString::from(test_name),
        OsString::from("--exact"),
        OsString::from("--nocapture"),
    ]
}

fn parameters(extra: &[(&str, String)], report: &Path, addr: SocketAddr) -> String {
    let mut all: Vec<(String, String)> = extra.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
    all.push(("report".to_string(), report.display().to_string()));
    all.push(("listen".to_string(), addr.to_string()));
    all.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("\n")
}

```

Three things about this module are worth stating outright, because each of them is a defect the naive version has:

- **`child` has two discriminators.** `run_confined` cannot set `CHILD_ENV` on the confined child, because that `Command` is built inside `confine` and a child only inherits what the launching process already has; the confined child is identified by the **payload** instead — the design's own discriminator (decision 17) — while the pruning experiment, which has no payload by construction, marks itself with the variable. `child()` also exists so that `adopt` happens exactly once per process: the pipe is read once, so a harness with a separate `is_child()` and `params()` would find it drained on the second call.
- **`run_confined` launches a launcher copy, and the probe's non-child arm calls [`launch`] — the two halves of one design.** The copy is what puts a filter the child can match into the child's argv (the module docs state why `confine`'s own re-invocation cannot supply one), and `launch()` is where the copy stops being a test runner and becomes the launcher: it is the one line each probe in Tasks 4, 6 and 7 carries in its `None` arm. Without the pair, the child's argv would carry the descriptor element as its only filter, libtest would run nothing (`running 0 tests … 3 filtered out`, exit 0), leaving the parent to report a missing report file.
- **`remove_rule` returns an `Option`, and the pruner reports "not present".** The `DEVICE_RULES` ids are in the source whether or not the sandbox asked for the device files (the table itself is the Progress note's record — deleted with the device literals — and the flag that asked for them is the Linux backend's own `Options` knob now), so an id can legitimately have no rule in the rendered text; the pruning script greps for the harness's `prune: rule-not-present id=…` line rather than mis-reading the failure as "required".

- [x] **Step 3: Run the test to verify it fails**

Run (on the Mac): `cargo test -p tinio-sandbox-macos --test startup -- --nocapture`
Expected: FAIL. Three failure modes arrive looking different, and telling them apart is most of this step's work:

1. **The report is not written at all, and the launcher's message carries `sandbox-exec`'s exit status.** `65` is a compile error in the profile (`invalid data type of path filter…`, `empty subpath pattern`); **`71` is an exec failure**, and it is *not* by itself evidence that the profile refused the exec — a target that could not be found or started produces the same code. The stderr text is the discriminator: `execvp() of '<path>' failed: Operation not permitted` is the profile's refusal, and anything else is a different problem wearing the same number.
2. **The report is written but a check failed**: `available_parallelism=error(Operation not permitted)` and `read_marker=error(PermissionDenied)` are the profile being too narrow, which is the interesting case — the rule that fixes each is what Task 5 will have to justify.
3. **The report is missing and the child's stderr is silent**, which is the launcher failing before `sandbox-exec` ran at all (a missing payload, a descriptor problem) rather than the profile.

**This failure is the deliverable of the step**: it is the empirical set's first iteration. Paste the full failure output into the task's notes.

- [x] **Step 4: Extend the candidate set until the process starts**

Work only through `RUNTIME_BASELINE` in `crates/tinio-sandbox-macos/src/profile.rs`. Each iteration: add one candidate rule, rerun, and record in the task notes which failure it removed. The refusals themselves come from one command, in a second terminal while the probe runs:

```bash
log stream --predicate 'sender == "Sandbox"' --style compact
```

**`log stream`, not `log show`, and that is measured rather than preferred**: on macOS 15.8 `log show` returns nothing for refusals even with `--info --debug` and a one-hour window, while the live stream reports them as they happen. Stop it with Ctrl-C. Two things to expect in what it prints, so neither is mistaken for a finding: **every confined process carries a noise floor of denials that have nothing to do with what it asked for** — `file-read-data` on its own executable image, `file-write-data /dev/dtracehelper`, and `network-outbound /private/var/run/syslog` — and a refusal of a read under a directory whose traversal was refused can surface as `ENOENT` rather than `EPERM`, which is indistinguishable from a missing file at the syscall boundary.

Expected candidates, in the order they usually matter: the exec itself — `process-exec` is already in the table, carrying `(literal (param "P"))`, so what a refusal there means is that the literal and the path `sandbox-exec` resolved are not the same string, and the diagnostic names which path it tried (`execvp() of '<path>' failed`) — then `(allow file-read* (subpath "/usr/lib"))` and `(subpath "/System")` if not already effective, `(allow file-read* (subpath "/private/var/db/dyld"))`, `(allow file-read-metadata)` for the ancestors of an exposure, and the device-file rules for `/dev/urandom` and `/dev/null`. **The `mach-lookup` candidates an earlier draft listed are not candidates at all**: measurement removed `mach-lookup` (with it gone every check passes except `getpwuid`, whose failure names `com.apple.system.opendirectoryd.libinfo`) and found the `com.apple.system.logger` allowance invented rather than observed. Re-adding either requires an observation from the log above, not a citation — and the whole point of this step is that the log is the source.

Two rules of engagement while doing this:

- **Never** add `(allow default)`, and never add `network-outbound`. A profile that starts by allowing everything proves nothing and cannot be pruned back honestly.
- Add the *narrowest* form that works: a named `global-name`, a `subpath` of the directory that was actually refused. Task 5 deletes whatever the pruning run says is unnecessary; adding a broad rule now and pruning it later is acceptable, adding `(allow default)` is not.

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-macos --test startup -- --nocapture`
Expected: PASS — `payload=ok`, `threads=4`, `read_marker=confined`, and a non-zero `available_parallelism`. The `payload=ok` assertion is what proves the payload-on-stdin handover of design §3 step 7 and the listener descriptor in argv both worked, so a pass here is the phase's first real progress.

---

### Task 5: Settle the baseline, and record why each rule survives

**Result, 2026-09-25: recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile"), in its audit, `sysctl` and `home` paragraphs.** This plan does not backfill the run's findings; the section named above is the record.

**Files:**
- Create: `crates/tinio-sandbox-macos/scripts/prune-profile.sh`
- Modify: `crates/tinio-sandbox-macos/src/profile.rs` (deletions, and every `evidence` field)

**Interfaces:**
- Consumes: `harness::run_pruned` and the `; tinio:<id>` markers from Task 1.
- Produces: a `RUNTIME_BASELINE` (and `DEVICE_RULES`) in which every surviving entry names the observation that keeps it, and no entry says `candidate`.

Task 4 found *a* working set. This task turns it into a *minimal, recorded* set, and **it is a required step rather than a follow-up**: design §5 calls the set empirical, and this is what makes "empirical" mean "measured" rather than "whatever worked the first time". Two things are settled before it starts and neither is re-opened here — the system read set is the narrowed one Task 1 ships (no `/private` blanket, no `/dev` blanket, no dead `/etc` rule, and `process-fork`/`signal (target self)` carried only until this run decides them), and every entry that is not one of the four `required` ones is subject to removal. A rule that survives without an observation saying what broke without it is deleted, not documented.

- [x] **Step 1: Write the pruning script**

`crates/tinio-sandbox-macos/scripts/prune-profile.sh`:

```bash
#!/usr/bin/env bash
# Establish which baseline allowances the profile actually needs.
#
# For each rule id, render the profile without that one rule and run the startup
# probe. A rule whose removal breaks the probe is load-bearing and must carry the
# observation in its `evidence` field; a rule whose removal changes nothing must
# be deleted from the source; an id that is not in the rendered profile at all
# gets no verdict from this run, and the harness says so. Run on a Mac, once per
# P3 landing.
#
# Usage: bash crates/tinio-sandbox-macos/scripts/prune-profile.sh
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "prune-profile.sh needs macOS (Seatbelt is a macOS facility)"; exit 1
fi

cd "$(dirname "$0")/../../.."
ids=$(rg -o --no-filename 'id: "[a-z0-9-]+"' crates/tinio-sandbox-macos/src/profile.rs \
  | cut -d'"' -f2 | sort -u)

echo "rule id            verdict"
echo "------------------ ---------------------------------------------"
for id in $ids; do
  # The output is kept rather than discarded: the third verdict — an id whose
  # rule is not in the rendered profile — is visible only in it, and reading that
  # run as "required" would document a rule as load-bearing on the strength of a
  # run that never removed anything.
  out=$(TINIO_SANDBOX_MACOS_PRUNE="$id" cargo test -q -p tinio-sandbox-macos --test startup \
      -- --nocapture a_confined_process_starts 2>&1) && status=0 || status=$?
  if grep -q "rule-not-present id=$id" <<<"$out"; then
    printf '%-18s NOT PRESENT — the id is in the source but not in the rendered profile;\n' "$id"
    printf '%-18s check that the probe asks for it (device files) before reading this as a verdict\n' ""
  elif [[ $status -eq 0 ]]; then
    printf '%-18s NOT NEEDED — delete the rule from the source\n' "$id"
  else
    printf '%-18s required — keep it, with the observation in `evidence`\n' "$id"
  fi
done
echo
echo "Paste this table into the design's evidence table (Task 11) and rewrite each"
echo "surviving entry's `evidence` field with what the run showed."
```

Make it executable: `chmod +x crates/tinio-sandbox-macos/scripts/prune-profile.sh`.

- [x] **Step 2: Run it**

Run: `bash crates/tinio-sandbox-macos/scripts/prune-profile.sh`
Expected: a table, one line per rule id. Every `NOT NEEDED` line is a rule to delete; every `required` line is a rule to keep and to justify; a `NOT PRESENT` line is a rule the probe's own sandbox does not ask for — fix the probe (the device-file request, which is `tinio_sandbox_linux::Options`'s knob now rather than the shared builder's) rather than treating it as a verdict. Save the table verbatim in the task notes — it is the phase's evidence.

- [x] **Step 3: Delete what is not needed**

For each rule the run marked `NOT NEEDED`, delete its `Allowance` from `RUNTIME_BASELINE` (or `DEVICE_RULES`), then rerun the whole suite to confirm nothing depended on it for a different reason:

Run: `cargo test -p tinio-sandbox-macos && bash crates/tinio-sandbox-macos/scripts/prune-profile.sh`
Expected: the suite passes and the deleted ids no longer appear in the table. If deleting a rule makes the pruning table unstable (a different rule now reports `NOT NEEDED`), the two rules overlap — keep the narrower one and say so in `evidence`.

- [x] **Step 4: Rewrite every surviving `evidence` field**

Each surviving entry's `evidence` names the observation, in the shape:

```rust
    Allowance {
        id: "sysctl",
        rule: "(allow sysctl-read)",
        evidence: "measured 2026-09-18, macOS 15.6: without it `available_parallelism` fails with EPERM and the startup probe reports error(Operation not permitted)",
    },
```

Write the machine, the macOS version and the symptom. `"candidate"` may not survive this task, and neither may a restatement of what the rule obviously does — the field exists to say *what broke without it*. **Two shapes need a sentence more than the others**: an entry whose removal changed nothing is *deleted*, not excused; and an entry whose evidence still reads "carried by design §5's measured text" (`process-fork` and `signal-self` arrive that way) has to have been decided by this run — kept with the failure its removal produced, or deleted.

- [x] **Step 5: Record the network question's state**

There is no network rule to leave alone and none to prune: the `accept` question was settled on macOS 15.8 before this plan was revised (design §5), and Task 7 asserts the empty network surface rather than discovering it. Add nothing network-side here; a rule added on a hunch would later read as a measurement.

- [x] **Step 6: Run the full crate test suite**

Run: `cargo test -p tinio-sandbox-macos`
Expected: PASS — `startup` green with the reduced baseline, and every pure test from Tasks 1 and 2 still green.

---

### Task 6: The exposure rules bite

**Files:**
- Create: `crates/tinio-sandbox-macos/tests/exposures.rs`

**Interfaces:**
- Consumes: the harness, `AccessMode`, `Sandbox`.
- Produces: nothing the library uses; this is the proof that `expose(path, access)` means what design §2's table says on this platform.

- [x] **Step 1: Write the failing test**

`crates/tinio-sandbox-macos/tests/exposures.rs`:

```rust
//! Design §2's macOS column, measured: the exposed tree is readable, the write
//! grant is exactly the difference between `ReadWrite` and `ReadOnly`, a
//! non-exposed path is refused, deep paths behave, a root exposed through a
//! symlink reads through both its spellings, and a link out of the tree does not.

#![cfg(target_os = "macos")]

mod harness;

use tinio_sandbox_core::{AccessMode, Sandbox};

/// Runs inside the confinement and reports what each path allows.
#[test]
fn child_exposures() {
    if !harness::is_child_process() {
        harness::launch();
        return;
    }
    let Some(child) = harness::child() else {
        return;
    };
    let mut log = String::new();

    // A path several directories deep, inside the served root, spelled the way
    // the operator spelled the root: proves no ancestor rule is needed for
    // traversal (Seatbelt matches the resolved path, not each component).
    log.push_str(&harness::line("deep_read", read(&child.param("deep"))));
    // The state directory, exposed read-write even alongside a read-only root.
    log.push_str(&harness::line("state_write", write(&child.param("state_write"))));
    // The served root: allowed here, denied in the read-only run.
    log.push_str(&harness::line("root_write", write(&child.param("root_write"))));
    // A directory the profile does not name, holding a file that does exist —
    // so what is measured is a refusal rather than a missing file.
    log.push_str(&harness::line("sibling_read", read(&child.param("sibling"))));
    // The same tree through the root's **canonical** spelling: granted only by
    // the rule `with_canonical_paths` adds, which is the measurement that keeps
    // both spellings in the profile.
    log.push_str(&harness::line("canonical_read", read(&child.param("canonical"))));
    // A link inside the exposed tree that points out of it: the kernel resolves
    // it to a path no rule names, so the read is refused.
    log.push_str(&harness::line("escape_read", read(&child.param("escape"))));
    harness::finish(&child, &log);
}

fn read(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(body) => format!("ok:{}", body.trim()),
        Err(err) => format!("error({})", err.kind()),
    }
}

fn write(path: &str) -> String {
    match std::fs::write(path, b"written by the confined server\n") {
        Ok(()) => "ok".to_string(),
        Err(err) => format!("error({})", err.kind()),
    }
}

#[test]
fn a_read_write_exposure_is_readable_and_writable() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_exposures` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");

    // **The served root is exposed through a symlink the operator's path goes
    // through**: `link` -> `real`, and the exposure names `link`. That is the
    // case `with_canonical_paths` exists for — Seatbelt compares a rule's
    // spelling against the kernel-resolved path, so `real`'s spelling is what
    // matches — and it is the one measurement that justifies keeping *both*
    // spellings in the profile: `deep` reads through the given one and
    // `canonical` through the resolved one, and the second is granted only by
    // the rule the canonicalisation adds.
    let real = dir.path().join("real");
    let deep = real.join("a/b/c");
    std::fs::create_dir_all(&deep).expect("create root");
    std::fs::write(deep.join("obj.txt"), "deep object\n").expect("seed");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).expect("symlink the served root");

    // A link *inside* the exposed tree, pointing out of it.
    let outside = dir.path().join("outside");
    std::fs::create_dir_all(&outside).expect("create outside");
    std::fs::write(outside.join("secret.txt"), "out of the tree\n").expect("seed");
    std::os::unix::fs::symlink(&outside, real.join("escape")).expect("symlink out of the tree");

    let sibling = dir.path().join("sibling");
    std::fs::create_dir_all(&sibling).expect("create sibling");
    std::fs::write(sibling.join("secret.txt"), "not exposed\n").expect("seed");
    let state = link.join(".tinio");
    std::fs::create_dir_all(&state).expect("create state");

    let sandbox = Sandbox::new().expose(&link, AccessMode::ReadWrite);
    let report = harness::run_confined(
        &sandbox,
        &[
            ("deep", link.join("a/b/c/obj.txt").display().to_string()),
            ("state_write", state.join("log.txt").display().to_string()),
            ("root_write", link.join("written.txt").display().to_string()),
            ("sibling", sibling.join("secret.txt").display().to_string()),
            ("canonical", real.join("a/b/c/obj.txt").display().to_string()),
            ("escape", link.join("escape/secret.txt").display().to_string()),
        ],
        "child_exposures",
    );

    report.expect("payload", "ok");
    report.expect("deep_read", "ok:deep object");
    report.expect("state_write", "ok");
    report.expect("root_write", "ok");
    // The two refusals below are asserted as *refusals*, and the expected error
    // is written as a pair on purpose: a read under a directory whose traversal
    // was refused can come back `no such file or directory` rather than
    // `operation not permitted`, and the two are indistinguishable at the
    // syscall boundary. What the test can hold — and what it does hold, with the
    // fixtures on disk — is that the read did not succeed and that the file was
    // there to be read.
    assert!(sibling.join("secret.txt").exists(), "the fixture must exist");
    assert!(
        matches!(report.value("sibling_read"), Some("error(permission denied)" | "error(not found)")),
        "{}",
        report.text()
    );
    report.expect("canonical_read", "ok:deep object");
    // The link out of the tree: the tree's rule does not follow it, and the
    // target is named by no rule at all.
    assert!(outside.join("secret.txt").exists(), "the fixture must exist");
    assert!(
        matches!(report.value("escape_read"), Some("error(permission denied)" | "error(not found)")),
        "{}",
        report.text()
    );
}

#[test]
fn a_read_only_exposure_refuses_writes_and_still_reads() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_exposures` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    let state = dir.path().join("state");
    let private = dir.path().join("private");
    std::fs::create_dir_all(&root).expect("create root");
    std::fs::create_dir_all(&state).expect("create state");
    std::fs::create_dir_all(&private).expect("create private");
    std::fs::write(root.join("obj.txt"), "read only\n").expect("seed");
    std::fs::write(private.join("notes.txt"), "never exposed\n").expect("seed");
    std::os::unix::fs::symlink(&private, root.join("escape")).expect("symlink out of the tree");

    // Design §2: the state directory is read-write on every platform, so the
    // report path lives there and the served root stays unwritable.
    let sandbox = Sandbox::new()
        .expose(&root, AccessMode::ReadOnly)
        .expose(&state, AccessMode::ReadWrite);
    let report = harness::run_confined(
        &sandbox,
        &[
            ("deep", root.join("obj.txt").display().to_string()),
            ("state_write", state.join("log.txt").display().to_string()),
            ("root_write", root.join("written.txt").display().to_string()),
            ("sibling", private.join("notes.txt").display().to_string()),
            ("canonical", std::fs::canonicalize(root.join("obj.txt")).unwrap().display().to_string()),
            ("escape", root.join("escape/notes.txt").display().to_string()),
        ],
        "child_exposures",
    );

    report.expect("payload", "ok");
    report.expect("deep_read", "ok:read only");
    report.expect("state_write", "ok");
    // A write *inside* an exposed directory is refused with `permission denied`
    // proper — the traversal is granted, so there is no `not found` to confuse
    // it with, which is why this one stays strict.
    assert_eq!(report.value("root_write"), Some("error(permission denied)"), "{}", report.text());
    // A read-only root does not make the confinement narrower anywhere else:
    // the unexposed tree is still refused, and the link out of the tree with it.
    assert!(
        matches!(report.value("sibling_read"), Some("error(permission denied)" | "error(not found)")),
        "{}",
        report.text()
    );
    assert!(
        matches!(report.value("escape_read"), Some("error(permission denied)" | "error(not found)")),
        "{}",
        report.text()
    );
    report.expect("canonical_read", "ok:read only");
    // The host's copy is untouched too: this is decision 8's "the absence of a
    // write permission", not an application-level check.
    assert!(!root.join("written.txt").exists());
}
```

The single `child_exposures` probe serves both parents by reading its paths from the parameters, so the read-only run swaps `root_write` for a denial without a second probe, and the symlinked-root fixtures appear in both — the read-only run canonicalises its own root, which on macOS is a second spelling for the same reason.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-macos --test exposures -- --nocapture`
Expected: FAIL. Likely first failures, in order: `canonical_read` refused (canonicalisation missing or wrong — the profile then spells only the path the operator wrote, and Seatbelt matches the resolved one), `deep_read` refused for the same reason when the root's own prefix is a symlink, or `state_write` denied when the state directory sits outside the served root and was not exposed.

- [x] **Step 3: Make it pass**

Fix in this order, then rerun after each fix:

1. If an exposure is denied although the profile names it, check the `-D` value `profile.rs` rendered for that exposure for the path the kernel sees (`std::fs::canonicalize` and compare the `params` entry) — on macOS `/tmp` and `$TMPDIR` are symlinks, and this is what `with_canonical_paths` is for.
2. If reads under an exposure are denied but the exposure itself resolves, the ancestor traversal is being checked after all: add `(allow file-read-metadata)` (or the narrowest form that works) to `RUNTIME_BASELINE` with its observation, and rerun Task 5's pruning script so it is pruned like everything else.
3. If a write into a read-only exposure *succeeds*, the rule is wrong, not the test: `AccessMode::ReadOnly` must emit no `file-write*` for that path. Fix `profile` before touching anything else.

- [x] **Step 4: Run the test to verify it passes**

Run: `cargo test -p tinio-sandbox-macos --test exposures -- --nocapture`
Expected: PASS — both tests, `payload=ok`, every `ok:` assertion exact, the read-only `root_write` reporting `permission denied`, and the refusals reporting an error of either kind (the test's pair of accepted strings is deliberate and Step 1 explains why). If `escape_read` reads successfully, the confinement is wider than the exposure set says and that is a stop-and-investigate, not a test to relax.

- [x] **Step 5: Rerun the pruning script**

Run: `bash crates/tinio-sandbox-macos/scripts/prune-profile.sh`
Expected: the same table as Task 5, modulo any rule added in Step 3. A rule whose necessity changes here was only ever needed by a path this test exercises; update its `evidence`.

---

### Task 7: The network rules — the `accept` question, already answered

**Result, 2026-09-25: recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile"), in its network paragraph.**

**Files:**
- Create: `crates/tinio-sandbox-macos/tests/network.rs`

**Interfaces:**
- Consumes: the harness and the listener it binds into the payload.
- Produces: the assertion that holds design §7's macOS row and decision 16 in place.

Design §5 used to leave one question to a real Mac: "whether `accept` on an already-bound inherited socket still needs `network-inbound`". It was settled on macOS 15.8 on 2026-09-19, and the answer is **no** — `accept` on an inherited listener is not gated by the sandbox at all. Only `bind()` and `listen()` are, and the parent performs both before the child exists. So the profile's entire network surface is **empty**, there is no inbound rule to add, and this task is an assertion rather than an experiment. (Measured alongside it, and not needed here because nothing inside the profile binds: `bind()` is satisfied by *either* `network-bind` *or* `network-inbound`.)

- [x] **Step 1: Write the failing test**

`crates/tinio-sandbox-macos/tests/network.rs`:

```rust
//! Design §7's uniform rule on macOS: the confined process cannot initiate an
//! outbound connection, and the profile never grants one. Inbound goes through
//! the listener the parent bound (decision 16) — and whether `accept` needs a
//! rule of its own is what this file settles.

#![cfg(target_os = "macos")]

mod harness;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use tinio_sandbox_core::{AccessMode, Sandbox};

/// Runs inside the confinement: accept on the inherited listener, and try to
/// reach out.
#[test]
fn child_network() {
    if !harness::is_child_process() {
        harness::launch();
        return;
    }
    let Some(child) = harness::child() else {
        return;
    };
    let mut log = String::new();

    // The listener arrived in the payload (design §3 step 7). Accepting on it is
    // the whole of the child's inbound surface — no bind, and therefore no
    // `network-bind` rule (decision 16).
    match child.listener() {
        Ok(listener) => match listener.accept() {
            Ok((mut stream, _)) => {
                let mut line = String::new();
                let read = BufReader::new(stream.try_clone().expect("clone")).read_line(&mut line);
                let _ = stream.write_all(b"pong-from-inside-the-profile\n");
                let _ = stream.flush();
                log.push_str(&harness::line(
                    "accept",
                    match read {
                        Ok(_) => format!("ok:{}", line.trim()),
                        Err(err) => format!("error({})", err.kind()),
                    },
                ));
            }
            Err(err) => log.push_str(&harness::line("accept", format!("error({})", err.kind()))),
        },
        Err(err) => log.push_str(&harness::line("accept", format!("no-listener({err})"))),
    }

    // Outbound. The parent proved this exact address is reachable from outside
    // the confinement first, so a failure here is the confinement and not a
    // machine with no route (design §12).
    let target = child.param("outbound");
    let outbound = match target.to_socket_addrs().ok().and_then(|mut a| a.next()) {
        Some(addr) => match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
            Ok(_) => "CONNECTED".to_string(),
            Err(err) => format!("refused({})", err.kind()),
        },
        None => "bad-address".to_string(),
    };
    log.push_str(&harness::line("outbound", outbound));
    harness::finish(&child, &log);
}

#[test]
fn the_confined_process_cannot_reach_out() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_network` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("create root");

    // The baseline first: the target is reachable from *outside* the
    // confinement, so the assertion below cannot pass vacuously on a machine
    // with no route or no network (design §12). The listener stays alive for
    // the whole test; on loopback a completed `connect` is a completed
    // handshake, so no `accept` is needed to prove reachability.
    let target = std::net::TcpListener::bind("127.0.0.1:0").expect("bind target");
    let addr = target.local_addr().expect("local addr");
    TcpStream::connect(addr).expect("an unconfined process must reach the target");

    let sandbox = Sandbox::new().expose(&root, AccessMode::ReadWrite);
    let report = harness::run_confined(&sandbox, &[("outbound", addr.to_string())], "child_network");

    report.expect("payload", "ok");
    // The harness's own connection to the inherited listener is what the
    // child's `accept` probe reads, so this line proves inbound works *and*
    // that the descriptor handover of design §3 step 7 reached the child.
    report.expect("accept", "ok:ping");
    assert_eq!(report.value("outbound"), Some("refused(permission denied)"), "{}", report.text());
}

#[test]
fn the_profile_grants_no_network_operation_at_all() {
    // Design §7's macOS row is not "no outbound" — it is no network capability
    // of any kind. Measured on macOS 15.8: a profile carrying zero network
    // rules accepted a real connection from a listener the parent had handed
    // in, and the kernel logged no `accept` violation.
    let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
    let rendered = tinio_sandbox_macos::profile::profile(&sandbox).expect("profile");
    for op in ["network-outbound", "network-bind", "network-inbound", "network*"] {
        assert!(!rendered.text.contains(op), "{op} appears in {}", rendered.text);
    }
}
```

- [x] **Step 2: Run the test**

Run: `cargo test -p tinio-sandbox-macos --test network -- --nocapture`
Expected: PASS on all three tests. `accept=ok:ping` proves both that the descriptor handover of design §3 step 7 reached the child and that the sandbox does not gate `accept`; `outbound=refused(permission denied)` proves the outbound refusal; the profile assertion proves the row is empty.

- [ ] **Step 3: If `accept` is refused, stop and re-measure rather than adding a rule**
  *(Not ticked: conditional and never triggered — Step 2's run is green, so the premise it guards did not occur; the three network tests pass in the tree, and design §5's network paragraph records the control-socket handoff they rest on.)*

A denial here contradicts a measurement taken on macOS 15.8, and none of the three things it can mean is "add `network-inbound`": the listener is not actually being inherited (check the payload's fd number against design §3 step 7), the descriptor lost its listening state between parent and child, or this macOS version gates `accept` where 15.8 did not. The third is a real possibility — the tool that separates them is `log stream --predicate 'sender == "Sandbox"'`, which names the refused operation as it happens and is how every macOS row of design §5 was established (`log show` reports nothing for refusals on 15.8, even with `--info --debug`). Record what it says; do not add a rule to turn a red test green.

---

### Task 8: `serve` on macOS — the launcher path

**Result, 2026-09-25: recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile"), in its `state` paragraph.** This plan does not restate the run's findings.

**Files:**
- Verify, do not edit: `crates/tinio-config/src/schema/server.rs` — P2 Task 11 Step 1 writes `probe_read_only` there (shared §4)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` — the serving entry point; the launcher's own half is `crates/tinio-cli/src/sandbox/confine.rs` and `crates/tinio-cli/src/commands/start/run.rs` (`crates/tinio-cli/src/sandbox/macos.rs` is the path this line landed with and no longer exists — the CLI's sandbox split is `sandbox/{mod,plan,confine,product,child}.rs` since 2026-09-27 — and the `crates/tinio-server/examples/serve.rs` this plan was written against is retired and gone)

**Interfaces:**
- Consumes: `tinio_sandbox::{adopt, is_listener_fd_arg, on_stop, platform, supervise, AccessMode, Backend as _, Confinement, Event, LaunchPlan, Sandbox}` (the facade re-exports core and the selected platform crate) plus the file's existing `FsStorage`/`FsOptions`, `state`, `sources` and `watch` imports, which the child's storage construction and stop channel use unchanged.
- Produces: `serve --sandbox` working on macOS; the confined child serving on the parent's listener, owning the state handle and reacting to a forwarded stop.

This is design §10's sequence for a child-launching platform: steps 1 (discriminator), 2 (targeted read-only lookup), 3 (bytes only), 5 (sources exist), 6 (bind, expose, launch), 7 (print, supervise, forward), 8 (runtime).

- [x] **Step 1: Verify the read-only probe is present, and do not write it again**

`tinio_config::server::probe_read_only` is **P2's edit and not this plan's**: P2 Task 11 Step 1 writes the function and its test into `crates/tinio-config/src/schema/server.rs`, because P2 is the earlier of the two phases that need it (`2026-09-18-sandbox-shared.md` §4). This plan used to write both as well, which made one file the subject of two tasks' edits — the defect that section exists to remove.

What to do here, before Step 2 wires `serve`:

```bash
rg -n 'fn probe_read_only|fn probe_reads_only_the_read_only_key' crates/tinio-config/src/schema/server.rs
cargo test -p tinio-config probe_reads_only
```

Expected: both names are found in that file, and the test passes. If the function is **not** there, P2 has not landed — stop and say so rather than writing it here. The one thing this step checks beyond its presence is the shape Step 2's call depends on: it takes the configuration bytes as `&[u8]` and returns a `Result` that `?` can carry into `main`, and it reads nothing but `[server] read_only` (design decision 18).

- [x] **Step 2: Wire `serve`**

In `crates/tinio-cli/src/commands/start/serve.rs` (the launcher's own half is `crates/tinio-cli/src/sandbox/confine.rs` and `crates/tinio-cli/src/commands/start/run.rs`; the `sandbox/macos.rs` this task landed in was deleted on 2026-09-27):

(a) imports, beside the existing ones:

```rust
#[cfg(feature = "sandbox")]
use std::ffi::OsString;

#[cfg(feature = "sandbox")]
use tinio_sandbox::{AccessMode, Backend as _, Confinement, Exposure, LaunchPlan, Sandbox};
```

`Backend as _` is what brings `platform()`'s `confine` into scope without naming the trait anywhere else — the Linux plan's call site already does exactly this.

(b) the exposures helper. **It is P1's and P3 defines none of it**: `sandbox_exposures` lives in the one `serve.rs` all three plans edit, `2026-09-18-sandbox-shared.md` §4 owns it along with that file's other helpers, and P1 Task 7 is the task that writes it — same name, same signature, same return type, none of which this plan restates. **The landed name is `sandbox_request`, which inlines the exposure rule and returns a `Sandbox`; `sandbox_exposures` has zero hits under `crates/**`, and `2026-09-18-sandbox-shared.md` §4 forbids adding it.** The name below is what this plan was written against and is left as written — P3 is unimplemented, so correcting the code shape is P3's own work when it lands.

**P3 does not change this helper, and the earlier draft of this step was wrong to try.** It proposed a macOS-flavoured version returning a `Sandbox` with the device files already applied, and justified it as "replacing the Linux plan's Linux-only shape" — while the same step's prose claimed the Linux behaviour does not change at all. Both cannot be true, and the one-definer rule settles it: the helper is P1's Task 7, macOS calls the same function with the same arguments, and the device-file flag is no longer a parameter of anything here — the shared `Sandbox` carries no device knob and `from_exposures` takes the exposure vector alone, the flag being `tinio_sandbox_linux::Options`'s own (`with_device_files`/`device_files`), so the snippet below records the pre-change shape:

```rust
        let sandbox = Sandbox::from_exposures(sandbox_exposures(root, state, root_access), true);
```

That is the platform-neutral set, and it is the same set on both launcher platforms: no backend adds the executable's own directory (decision 22's grant is Windows's, and macOS deliberately spends none), and the exposure the state directory gets is `ReadWrite` on both.

(c) the macOS step of design §10 step 6, on its own:

```rust
/// Design §10 step 6 on macOS: bind the port, apply the profile, launch the
/// confined child, supervise it, forward its status and exit. It returns no
/// child — `supervise` owns the wait, the teardown and the exit-status
/// forwarding (design §3 step 8) — and it does not return at all.
///
/// **macOS-only, and the `#[cfg]` is load-bearing.** The platform that confines
/// in place does it in its own `main`, where it can bind *after* the confinement
/// and emit its own `Listening` event (design decisions 6 and 19), and that block
/// is `2026-09-18-sandbox-linux.md` Task 7's. An earlier draft made this helper
/// cross-platform and gave it a `#[cfg(not(target_os = "macos"))]` arm that called
/// `sandbox.enter(&EnterOptions::default())` — neither name exists in any phase
/// (P1's backend answers `Confinement::InPlace` from `confine`) — so the arm is
/// gone and the helper is macOS's alone. Windows takes the same shape in
/// `2026-09-18-sandbox-p2-windows.md` Task 11, for the same reason.
#[cfg(all(feature = "sandbox", target_os = "macos"))]
fn confine_or_launch(
    sandbox: &Sandbox,
    address: SocketAddr,
    config_bytes: &[u8],
    argv: &[OsString],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(target_os = "macos")]
    {
        // **The launcher installs its own subscriber, and this is the one place
        // shared §7's level split has to be paid for rather than noted.** `serve`
        // builds its subscriber inside the process that *serves* — the child —
        // so without this every `info!` the launch emits would be dropped, and
        // the record an operator needs most (what was confined, to what, with
        // which pid) would be exactly the record missing. The level matches what
        // `serve` builds; the `[log]` wiring that would let an operator raise it
        // lands with the CLI, and `debug!` events stay invisible in `serve`
        // until then. `set_global_default` can fail only if a subscriber is
        // already installed, which cannot happen here: the confined child takes
        // the `adopt` branch in `main` and never reaches this function.
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_writer(std::io::stderr)
            // **Load-bearing for the one test that reads this stream back, and
            // measured rather than assumed**: the default subscriber wraps each
            // field *name* and the equals sign after it in ANSI escapes, so a
            // drained line carries `step` as coloured spans and never as the
            // `step="spawn"` the launch e2e asserts on. ANSI is on whenever the
            // `ansi` feature is enabled and `NO_COLOR` is unset, which makes that
            // assertion turn on the operator's environment rather than on the
            // event. The Linux plan's `traced` helper and this plan's recorder
            // test carry the same line for the same reason.
            .with_ansi(false)
            .try_init();

        // The parent owns the port (decision 16): it binds here and the
        // descriptor travels to the child in its argv (design §3 steps 3 and 7).
        // The parent does **not** announce the endpoint — the child emits the
        // `Listening` event and the launcher relays it (decision 19), so nothing
        // but a decoded event ever reaches stdout.
        let listener = std::net::TcpListener::bind(address)?;
        // `with_listener` hands the bound socket over — **the plan owns it from
        // here on**, and it must outlive the plan it was handed to: the
        // launcher's copy of the descriptor stays open for the child's whole
        // life, which is what makes the number in the child's argv valid. The
        // backend turns it into that argv element (design §3 steps 3 and 7).
        // The configuration travels as the text it already is — the payload is
        // one JSON object, and its `String` field is what makes that a
        // compile-time fact rather than a convention.
        let plan = LaunchPlan::for_launch(String::from_utf8_lossy(config_bytes).into_owned())
            .with_listener(listener);
        let outcome = tinio_sandbox::platform().confine(sandbox, &plan);
        let child = match outcome {
            Ok(Confinement::Supervise(child)) => child,
            Ok(Confinement::InPlace) => {
                return Err("the macOS backend must supervise its child".into());
            }
            Err(err) => {
                // A launch failure is an event, not a silent absence (decision
                // 19): the consumer reads stdout and would otherwise wait for a
                // `Listening` that can never come.
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };

        // The teardown of decision 15's every-exit-path rule: it runs after the
        // child, whatever the child's status, and it is what disarms the signal
        // relay (Task 3) so a late signal cannot be forwarded to a recycled pid.
        //
        // `supervise` **borrows** the child, which is what lets the terminal
        // record be read after it returns — once the wait has joined the relay
        // and the answer is final. A child that exited without ever reporting is
        // the one thing only the parent can witness (decision 19), and it is the
        // only frame the parent emits on its own behalf: `Listening` is the
        // child's, because only the process serving the data plane can say it is
        // serving.
        let mut child = child;
        let status = tinio_sandbox::supervise(&mut child, tinio_sandbox::disarm_signal_relay)?;
        if child.terminal().is_none() {
            emit_error(&format!(
                "the confined child exited ({status}) without reporting readiness"
            ));
        }
        // The exit, with the status this launcher forwards: the last of shared
        // §7's `info!` events, and the one that pairs with the spawn's pid when
        // an operator is reconstructing a run that ended in a way nobody
        // explained.
        tracing::info!(step = "exit", status = %status, "the confined child exited; its status is forwarded");
        // No `drop(listener)`: the socket belongs to the plan from
        // `with_listener` on, it is dropped with the plan at the end of this
        // scope, and what matters is that it is dropped *after* the child — the
        // launcher's copy is what keeps the child's descriptor number valid for
        // its whole life. Naming the drop would also not compile: the value was
        // moved.
        std::process::exit(status.code().unwrap_or(1));
    }
}
```

**There is no Linux arm here, and the Linux behaviour does not change in this task at all** — which is the whole reason the helper is macOS-gated. P1's arm stays inline in `main`, in `2026-09-18-sandbox-linux.md` Task 7; this plan neither restates it nor wraps it (the one-definer rule in `docs/superpowers/README.md`). Note what the macOS arm does **not** do with the child it gets back: it never waits on it directly — `supervise` owns the wait, the teardown and the exit-status forwarding (design §3 step 8) — which is why it exits rather than returning a child to `main`.

(d) this process's own arguments, and the child's arm (design §10 steps 1 and 8):

**`own_args` is P2 Task 11's, and P3 defines none of it** — the rule `2026-09-18-sandbox-shared.md` §4 states for every helper that lands in this one file, applied between the two launcher plans exactly as it is applied between P1 and them: `2026-09-18-sandbox-p2-windows.md` Task 11 writes the file's single `fn own_args() -> Vec<OsString>`, and both of this plan's uses of it (the child's arm below and `main`) are calls into that one definition. Two definitions of one function in one module is `E0428` — the same failure a second `fn emit` would be — and the plan that lands first is what owns the helper. **What stays here is the macOS fact the filter exists for**: the element every child inherits is the core's `listener_fd_arg` spelling (`tinio-sandbox-fd=<n>`, decision 17), it is dropped from the `argv[0]`-less `args_os()` before any parser downstream sees it (design §3: the child's argv carries the payload descriptor and nothing else that is security-relevant), and the predicate that recognises it — `is_listener_fd_arg` — lives in the crate that writes the element rather than in this file.

The child's arm:

```rust
/// The confined child: design §10 step 8 and nothing else. The configuration
/// comes from the payload's **bytes**, never from re-reading the path in `argv`
/// — the file it names is outside every exposure, and re-reading it would be
/// denied by the profile this process is running under (design §3 step 2).
///
/// **This is the process that serves, so this is the process that builds the
/// `FsStorage`** (and P1's signature note states the contract this half of it
/// follows): the launcher built none, because opening the state database twice —
/// once there and once here — is not something the storage layer is written for.
/// The launcher resolves the state directory as a *path*, which is all the
/// exposure set and the profile's `-D` parameters need; this function owns the
/// only handle, and everything it derives from a host path is derived from the
/// same three sources in the same order as the launcher did (design §10).
#[cfg(feature = "sandbox")]
#[cfg(target_os = "macos")]
fn confined(
    adopted: tinio_sandbox::Adopted,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = parse_args(own_args())?;
    // `Adopted::config()` is a `&str` and not bytes (P0): `Config::load` already
    // requires UTF-8, so the payload carries text and there is no second
    // decode here.
    let config = match adopted.config() {
        "" => None,
        text => Some(Config::parse(text)?),
    };
    // The mode, re-resolved here exactly as the launcher resolved it: the file's
    // `[server] read_only` out of the payload's text — through the same targeted
    // probe, because this process keeps no parsed `Config` (decision 18) —
    // overlaid by `TINIO_READ_ONLY` and then by the flag.
    let read_only = sources::env_read_only()?
        .unwrap_or(tinio_config::server::probe_read_only(adopted.config().as_bytes())?)
        || args.read_only;
    let access = AccessMode::from(read_only);
    let state_dir = if read_only {
        Some(match state::env_state_dir()? {
            Some(dir) => dir,
            None => state::default_home_state_dir(Path::new(&args.root))?,
        })
    } else {
        None
    };
    // The lanes `serve` is handed beside the storage — `2026-09-18-sandbox-shared.md`
    // §4 says why the value has to travel rather than be rebuilt inside the body.
    let pipelines = /* the lanes the options literal below names */;
    // The `FsStorage::new(root, FsOptions { … })` call the body makes today,
    // verbatim — the same literal the unconfined path builds, with this
    // process's `state_dir` and `access` — and **the two call sites have to be
    // kept in step**: the state directory is a path the exposure set names, so a
    // disagreement between them would grant the child a directory it does not
    // write to.
    let storage = FsStorage::new(
        Path::new(&args.root),
        /* the options literal, verbatim from the body */
    )?;
    // Through the facade's platform-neutral name, not a method on `Adopted`:
    // the rebuild is each platform's own (it turns an argv descriptor number
    // back into a listening socket), and this function is the child-side entry
    // that both launcher platforms write. One name for it keeps the two arms
    // recognisably the same shape — `2026-09-18-sandbox-p2-windows.md`'s
    // `confined` calls exactly this, and core's `Adopted` carries the number
    // rather than a socket so that it can.
    let listener = tinio_sandbox::adopted_listener(&adopted)?;
    // A forwarded stop lands here (design §3 step 9, decision 20's second half).
    // **The channel is created in this process, not carried across the launch**:
    // the child is what reacts to a stop, and a `watch` receiver that crossed an
    // `execve` would be a descriptor and a rebuild on arrival, which is the
    // machinery this shape exists to avoid. What the hook does is start the
    // shutdown the runtime already knows — the plane is the subscriber that
    // matters, `2026-09-18-sandbox-p2-windows.md` Task 11 states why the
    // substitution belongs at the plane's call site and nowhere else — and the
    // bounded version of it (≤10 s, state file removed) is the CLI contract's
    // (T066/T067) and lands on this hook.
    let (stop_tx, stop_rx) = watch::channel(false);
    tinio_sandbox::on_stop(move || {
        let _ = stop_tx.send(true);
    })?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(args, config, storage, access, pipelines, Some(listener), Some(stop_rx)))
}
```

(e) `main`, in the design's order:

```rust
fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Design §10 step 1, before anything else, so a confined child does no
    // privileged work on its way in. The payload is the only discriminator: an
    // environment marker would be forgeable, and a forged marker is a bypass of
    // the whole feature (decision 17).
    #[cfg(all(feature = "sandbox", target_os = "macos"))]
    if let Some(adopted) = tinio_sandbox::adopt(env::args_os(), std::io::stdin().lock())? {
        return confined(adopted);
    }

    let args = parse_args(own_args())?;

    // **P1's decision-5 arms, kept verbatim** — this plan does not replace them,
    // and dropping them was a defect in its earlier draft: a build or a platform
    // that accepts `--sandbox` and confines nothing is the failure the whole
    // design exists to prevent. The first arm is about the build: a binary
    // without the feature refuses the request rather than ignoring it. `serve`
    // is declared with `required-features = ["sandbox"]`, so *this* example
    // cannot be produced without the feature — the arm is here because the same
    // `main` is the one the CLI will carry, where the flag and the build do
    // collide.
    #[cfg(not(feature = "sandbox"))]
    if args.sandbox {
        eprintln!(
            "error: this build was compiled without the `sandbox` feature, so --sandbox \
             cannot be honoured; rebuild with the feature or drop the flag"
        );
        process::exit(2);
    }

    // The other half of decision 5: a platform whose backend another phase owns
    // refuses the flag rather than accepting it and confining nothing. P1 wrote
    // this arm as `not(target_os = "linux")`; P3 replaces the macOS case with
    // its own arm below, and P2 narrows the Windows one the same way — so what
    // is left here is a platform no phase serves, the other-unix `chroot`
    // fallback being P4's, and it still must not accept the flag.
    #[cfg(all(
        feature = "sandbox",
        not(any(target_os = "linux", target_os = "macos", target_os = "windows"))
    ))]
    if args.sandbox {
        eprintln!(
            "error: --sandbox has no backend on this platform yet; the Linux, macOS and \
             Windows ones are the ones that have landed"
        );
        process::exit(2);
    }

    // Step 3 reads the bytes; the launcher never builds a `Config` (decision
    // 18), so the only thing taken from the document here is one boolean.
    let config_bytes = match &args.config_path {
        Some(path) => fs::read(path)?,
        None => Vec::new(),
    };

    // FR-023 has landed, and the mode is resolved here exactly as `serve`
    // resolves it: the file's `[server] read_only` — through the targeted probe,
    // because this process keeps no parsed `Config` (decision 18) — overlaid by
    // `TINIO_READ_ONLY` and then by the flag.
    let read_only = sources::env_read_only()?
        .unwrap_or(tinio_config::server::probe_read_only(&config_bytes)?)
        || args.read_only;
    let access = AccessMode::from(read_only);

    // Read-only mode serves a root that must already exist: creating it would
    // write the one thing the mode promises to leave alone.
    if !read_only {
        fs::create_dir_all(&args.root)?;
    }
    // `None` in normal mode: the backend keeps state at `<root>/.tinio/`, which
    // the root's own exposure already covers. `Some(…)` under FR-023, where the
    // relocation puts it outside the root and the profile has to grant it.
    let state_dir = if read_only {
        Some(match state::env_state_dir()? {
            Some(dir) => dir,
            None => state::default_home_state_dir(Path::new(&args.root))?,
        })
    } else {
        None
    };

    #[cfg(all(feature = "sandbox", target_os = "macos"))]
    if args.sandbox {
        let root_access = AccessMode::from(read_only);
        // The exposed state directory is the resolved one rather than a guess.
        // With `None` that is the backend's own `<root>/.tinio`, which the
        // root's exposure already covers, so `sandbox_exposures` returns it
        // without a second entry.
        let state = state_dir
            .clone()
            .unwrap_or_else(|| Path::new(&args.root).join(".tinio"));
        // Design §9's refused combination, refused here rather than inside the
        // confinement: a read-only root with a state directory under it cannot
        // be served, and `FsStorage` would reject it a process too late.
        if read_only && state.starts_with(Path::new(&args.root)) {
            return Err("read-only mode cannot serve a root that holds the state directory; \
                 set TINIO_STATE_DIR outside the served root"
                .into());
        }
        // One call decides which paths, and the device-file flag is an argument
        // rather than something the rebuilt set remembers (P1's shape, and the
        // reason `sandbox_exposures` returns exposures rather than a `Sandbox`):
        // macOS expresses `device_files()` as literal rules — the four device
        // files plus the two standard streams §5's profile writes by path — where
        // Linux binds them into the jail (design §4, Task 1).
        let sandbox = Sandbox::from_exposures(
            sandbox_exposures(Path::new(&args.root), &state, root_access),
            true,
        );
        let argv: Vec<OsString> = args.child_argv();
        // This call does not return: it supervises the confined child, forwards
        // its exit status and exits (design §3 step 8).
        confine_or_launch(&sandbox, args.address, &config_bytes, &argv)?;
    }

    // Linux: P1's arm stays where P1 wrote it — inline in `main`, in
    // `2026-09-18-sandbox-linux.md` Task 7 — and it returns, because there the
    // process confines *itself*, binds its own listener afterwards and emits its
    // own `Listening` event (design decisions 6 and 19). This plan neither
    // restates it nor wraps it.

    // Not launched as a child, and not confined in place: this process serves.
    let config = match &args.config_path {
        Some(path) => Some(Config::load(path)?),
        None => None,
    };
    // The probe writes `<root>/.tinio-chown-probe-<nonce>/probe` into the served
    // root, so read-only mode skips it for the same reason it skips the
    // `create_dir_all` above (FR-023).
    #[cfg(unix)]
    if !read_only {
        identity::ensure_chown_privilege(
            config.as_ref().unwrap_or(&Config::default()),
            Path::new(&args.root),
        )?;
    }
    // **This path serves, so this path builds the storage — and it is built
    // here, below the sandbox arm, rather than beside the state directory it
    // names.** That placement is the whole of how two true statements stay
    // true at once: a *launcher* must not open the state database, because its
    // child will and two handles on one state directory is not a shape the
    // storage layer is written for (the paragraph after this one), while a
    // `main` that goes on to serve is not a launcher and needs the handle. The
    // sandbox arm never returns — `confine_or_launch` exits — so the launcher
    // path cannot reach this line, and moving it up would open the database in
    // the one process that must not.
    //
    // The call the body makes today, verbatim, and the same one the child's
    // `confined` makes (Step 2(d)): the two call sites are kept in step by the
    // state directory they both name, which is also an exposure.
    let access = AccessMode::from(read_only);
    let pipelines = /* the lanes the options literal below names */;
    let storage = FsStorage::new(
        Path::new(&args.root),
        /* the options literal, verbatim from the body */
    )?;
    // ... and then this process serves. What it passes beside the storage is its
    // own platform fact: it bound its own listener rather than adopting a
    // child's, and nothing forwards a stop to a process that is not a child
    // (design decision 6). What the call takes, and in what order, is
    // `2026-09-18-sandbox-shared.md` §4's and is not stated here.
    serve(args, config, storage, access, pipelines, None, None)
}
```

Two signature changes make the above work, and **which plan holds the pen is decided by who needs the thing, not by which plan the rule happens to name**: `Args` is P1 Task 7's type and `2026-09-18-sandbox-shared.md` §4 owns it, so what this plan states is the **macOS requirement** — and then carries the half of it that no one else needs. `parse_args` takes the argument iterator already, landed with P1's task, so that half is consumed as it stands and the launcher's descriptor element is filtered out of the iterator before the parser is handed it, in exactly one place. **`argv` and `child_argv()` are this plan's addition to P1's type**: only P3 needs them, and a helper belongs to whoever needs it first — the same reading that gives P2 the file's `own_args` (both launcher plans need it, P2 lands earlier, so P2 owns it and this plan calls it). The parsed arguments keep the filtered `argv: Vec<OsString>` (`argv[0]` dropped, the descriptor element already removed by `own_args`) with a `child_argv()` that clones it, because the child must be re-invoked with the same arguments this process was started with, minus nothing else and with nothing added except the descriptor element the launcher appends. The field and the method are added **here rather than in P1's task**, which the rule's "added to P1's task, not copied beside it" would otherwise ask for: that leg presumes a plan still to be executed, and P1's has landed — it restructured the entry point for a platform that confines in place and never re-invokes itself, so it has no use for the arguments it parsed, while this is the plan whose arm re-issues them. Every other member of the type is P1's and landed, and none of it is restated here: `root`, `address`, `config_path`, `read_only` and `sandbox` are the five fields that task writes — `sandbox` among them, set by the parser from `--sandbox` (design decision 5) — so `argv` is the one field this plan adds.

**The launcher's side reaches `serve` never, and builds no storage** — which is a different statement from the one the paragraph above makes about the serving path. `main`'s macOS arm resolves the state directory as a `PathBuf`, for the exposure set's second entry and for the profile's `-D` parameters, and hands the plan to `confine_or_launch`, which exits: constructing an `FsStorage` there would *open* the state database in the process whose child opens the same one, and two handles on one state directory is not something the storage layer is written for. The child's `confined` is where that handle is taken, and the plain run's path above is where the other one is; P1's signature note states the same rule for its own platform.

(f) `serve` itself — **P1's, whose signature `2026-09-18-sandbox-shared.md` §4 owns and this plan does not write out** — its listener, and the one place an endpoint is announced. The two frame helpers it and the macOS arm's refusals call, `emit(event: &Event)` and `emit_error(message: &str)`, are **P1's too**, defined once in this same file beside its arm: a second `fn emit` here would be `E0428`, and the rule that gives `serve.rs`'s helpers one owner is the one that already gives `parse_args`, `Args`, `sandbox_exposures` and the `serve` signature theirs. P3 calls them and defines neither. What follows is only what the macOS arm adds inside the body: **`serve`'s own declaration is not repeated here**, and neither is the storage's construction:

```rust
    // ... `storage.set_max_concurrent_uploads(…)` onward, unchanged ...
    // The listener is not this plan's line either: `inherited` is
    // `Option<std::net::TcpListener>` and the landed body converts it
    // (`Some(inherited) => TcpListener::from_std(inherited)?`, beside an
    // untouched `None` arm) — `2026-09-18-sandbox-shared.md` §4 owns that, and
    // the conversion cannot happen where this plan's child rebuilds its socket,
    // which is before any runtime exists. The launcher platforms hand the bound
    // socket over (decision 16); the child does not bind and does not need to
    // know how it got here.
    // The address is the one actually bound, not the one requested — with
    // `--port 0` they differ, and this event is how a consumer learns the port.
    // Emitted by the child whether or not it bound the socket itself: the event
    // says the data plane is serving, and the launcher — outside the sandbox,
    // doing no request work — is not the process that knows (decision 19).
    emit(&tinio_sandbox::Event::Listening { addr: listener.local_addr()? });
    // ... unchanged from here on, except that the plane's shutdown receiver is
    //     the caller's when one was handed in ...
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // ... the scanner takes `shutdown_rx`, the sweeper its own subscription ...
    plane.serve(listener, stop.unwrap_or_else(|| shutdown_tx.subscribe())).await?;
    // ... the rest, with `storage` in place of the local it replaced ...
```

**The signature is P1's, not this plan's, and `2026-09-18-sandbox-shared.md` §4 is where it is written down** — what it takes, in what order, and which of its parameters are optional are that row's, and this plan neither lists them nor counts them. What is this plan's is that `confined` is the launcher-side caller and the plain serving path is the other, that P3 adds no parameter and changes none, and that the two substitutions above are the ones a launcher platform needs and P1 left in place for it: `inherited` replaces the bind, and `stop` replaces the plane's own receiver — **at the plane's call site, not on the scanner's**, because the scanner and the plane are independent subscribers and a stop delivered only to the scanner would leave the server accepting (P2 Task 11 states that, and this plan applies it unchanged rather than restating it).

Delete the `#[tokio::main]` attribute (the Linux plan already replaced it with a plain `fn main`), delete the old `println!("listening on …")`, and make sure nothing else in the file reads `Config` before step 7 above. Every remaining `println!` and `eprintln!` in this file is a bug: stdout is the protocol channel and the logs belong on stderr.

- [x] **Step 3: Run the tests and the example**

Run:

```bash
cargo test -p tinio-server --example serve
cargo clippy -p tinio-server --all-targets -- -D warnings
cargo check -p tinio-server --no-default-features
```

Expected: the example's unit tests pass, clippy is clean, and the feature-off build still compiles (the example is skipped by `required-features`).

- [x] **Step 4: Run it by hand on a Mac**

Run:

```bash
cargo run -p tinio-server --example serve -- /tmp/tinio-jailed --port 0 --sandbox
```

Expected: one SSE frame on stdout — the two lines `event: listening` and `data: {"addr":"127.0.0.1:<port>"}`, closed by a blank line (that is what `Event::encode` produces: the name in the `event:` field, the payload carrying only what the name does not already say) — relayed from the confined child, the process stays up, and `curl -sS -o /dev/null -w '%{http_code}\n' http://127.0.0.1:<port>/` prints an HTTP status (403 for an unsigned request is fine — it is the S3 plane answering). If the frame is missing but the server is up, the event relay is not running; if the port in the frame is not the one curl reached, the child rebuilt the wrong descriptor. On stderr, in the same run, the launcher's own `info!` events must appear — the profile's rule and parameter counts, the pre-flight's acceptance, the spawn with its pid, and the exit — which is what makes a failed launch diagnosable from a terminal rather than only from a reproduction (shared §7). Then in another terminal:

```bash
pgrep -fl 'sandbox-exec|serve'   # the confined child must appear under sandbox-exec
ls /tmp/tinio-jailed/.tinio      # the confined child wrote its state here, through the profile
```

Expected: the child runs under `sandbox-exec`, and the state directory contains the access log. Stop the parent — the child exits with it in this flow only because the parent is waiting on it; killing the parent hard leaves the child behind (recorded in the Self-Review as a known gap, not a bug).

---

### Task 9: The confined server, end to end, on the CI macOS leg

**Result, 2026-09-25: the arm this leg owes is recorded in `docs/tests.md`'s CLI section and `.github/workflows/ci.yml`'s cucumber comment; the profile and launch measurements behind it are in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile").**

**Files:**
- Create: `crates/tinio-server/tests/sandbox_macos_e2e.rs`
- Modify: `crates/tinio-server/Cargo.toml` (the `[[test]]` target)
- Modify: `.github/workflows/ci.yml` (a clippy step on the `macos-latest` leg of `test-port`, and the `lint` job's false comment)

**Interfaces:**
- Consumes: the built `tinio` binary, located by `crates/tinio-server/tests/common/mod.rs`'s `tinio_bin` (the retired example's `serve_bin` under its current name — the helper is `common::tinio_bin` today), which this tree's spawning targets share (this file among them, since the spawn/readiness hoist); `crates/tinio-e2e`'s harness resolves the same binary its own way, `cargo_bin("tinio")`, because a different crate's test tree cannot import this one's support.
- Produces: the macOS leg's acceptance — "a confined server serves a request" (design §12), and the exit-status forwarding of design §3 step 8.

- [x] **Step 1: Write the failing test**

`crates/tinio-server/tests/sandbox_macos_e2e.rs`:

```rust
//! The confined server, driven the way the interop harness drives it: spawn the
//! example, read the endpoint the confined child reports, and talk to it.
//!
//! This is the CI macOS leg's acceptance (design §12). It fails rather than
//! skips on macOS: a leg that quietly passes without confining anything is the
//! failure mode the design refuses everywhere else.

#![cfg(target_os = "macos")]
#![cfg(feature = "sandbox")]

mod common;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// The location rule is `common::tinio_bin`'s: this test tree shares it
// — this file, the read-only suite and the two e2e suites beside it all take it
// from `tests/common/mod.rs`, next to their HTTP helpers. `crates/tinio-e2e`'s
// harness resolves the same binary its own way, because a test crate cannot
// import another test crate's support: `cargo_bin("tinio")`. `tinio_bin` is not
// `assert_cmd`'s helper and not `env!("CARGO_BIN_EXE_tinio")`, which this
// package does not export (`CARGO_BIN_EXE_*` is set for a package's own
// integration tests, and the e2e suites are a different crate's).
use common::tinio_bin;

#[test]
fn a_confined_server_serves_a_request_and_forwards_its_child_status() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    std::fs::create_dir_all(&root).expect("create root");

    let mut child = Command::new(tinio_bin())
        .args([root.to_str().expect("utf-8 root"), "--port", "0", "--sandbox"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn serve");

    // The launcher's own record travels on stderr, which the confined child
    // inherits, and shared §7's checking rule is that the `info!` half is
    // asserted against it as it stands: the profile, the pre-flight, the spawn
    // with its pid, and the exit. The drain runs on a thread of its own so a
    // full pipe can never block the process under test.
    let mut stderr = child.stderr.take().expect("stderr");
    let logged = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });

    // The endpoint arrives as an event, not as a line to scrape (decision 19):
    // the confined child emits it and the launcher relays it, so a consumer
    // never has to care which process wrote it.
    let stdout = child.stdout.take().expect("stdout");
    let mut lines = BufReader::new(stdout).lines();
    let addr = read_listening(&mut lines).expect("a Listening event from the confined server");

    // Serving: any HTTP response proves the confined process reached `accept`
    // and answered; an unsigned S3 request is refused with 403, which is still
    // the plane answering.
    let response = request(&addr);
    assert!(response.starts_with("HTTP/1.1 "), "{response}");

    // A write through the profile: the access log lives in the state directory,
    // which only the confined child's own grants allow.
    assert!(
        root.join(".tinio/access.log").exists(),
        "the confined child wrote no access log under {}",
        root.join(".tinio").display()
    );

    // The relay (design §3 step 9, decision 20 — *not* exit-status forwarding):
    // the signal goes to the **launcher**, which must pass it on to the confined
    // child rather than dying and leaving the child to be killed at a grace
    // period. The child is what we then watch exit; this assertion is the only
    // one in the plan that can tell a relay apart from no relay at all.
    let child_pid = wait_for_child_pid(child.id());
    Command::new("kill").args(["-TERM", &child.id().to_string()]).status().expect("kill");
    assert!(
        waits_for_exit(child_pid),
        "the confined child outlived a signal sent to the launcher — no relay, or a dead launcher"
    );

    // Exit-status forwarding (design §3 step 8): the launcher outlived the
    // signal and carried the **child's** status back, which in this flow is a
    // clean exit — the child's own stop hook catches the forwarded `SIGTERM`, the
    // plane drains, `serve` returns `Ok`, and the code is 0. `Some(143)` — the
    // shell's 128 + `SIGTERM` for a signal death — can never appear here, because
    // that rendering belongs to a shell and `ExitStatus::code()` refuses to
    // invent one. A `None` would mean the child died of the default disposition
    // instead, i.e. that the stop hook never registered: that is a defect in
    // Task 8's `confined`, not an assertion to relax.
    let status = wait_for_exit(&mut child);
    assert_eq!(status.code(), Some(0), "the launcher forwards the child's status; a clean stop is 0");

    // Shared §7's `info!` half, read off the stderr the child inherited: the
    // events that answer "was this process confined, and to what". A launch that
    // leaves no such record is the failure the requirement exists to prevent,
    // and this is the assertion that keeps it from coming back.
    let logged = logged.join().expect("the stderr drain");
    for step in ["profile", "preflight", "spawn", "exit"] {
        assert!(
            logged.contains(&format!("step=\"{step}\"")),
            "the launcher logged no `{step}` event:\n{logged}"
        );
    }

    // The event relay's whole job (design §12): what reaches the consumer is
    // frames, and only frames. The child has exited, so this drains to EOF.
    // (The positive half — a stray line the child prints being *dropped* rather
    // than forwarded — is the relay's own unit test in the core; duplicating it
    // here would test the decoder, not the launcher.)
    let mut events = 0usize;
    for line in lines {
        let line = line.expect("read the launcher's stdout");
        if line.is_empty() {
            continue;
        }
        if line.starts_with("event: ") {
            events += 1;
        } else if !line.starts_with("data: ") {
            panic!("a non-frame line reached the consumer: {line:?}");
        }
    }
    // A frame is an `event:` line, a `data:` line and a blank line — so a
    // *second* legitimate frame's `event:` line is part of the protocol, and
    // counting it as a stray write would be the test failing on correct output.
    // What the count asserts is the number of frames, not of lines.
    assert!(
        events <= 1,
        "a confined server emitted {events} frames; only `Listening` was expected"
    );
}

/// Read lines until a `Listening` frame arrives, and return its address.
fn read_listening(
    lines: &mut impl Iterator<Item = std::io::Result<String>>,
) -> Option<std::net::SocketAddr> {
    // A frame is the `event:` line and the `data:` line together, so they are
    // read in pairs — decoding one line at a time would see neither whole.
    while let (Some(first), Some(second)) = (lines.next(), lines.next()) {
        let (Ok(first), Ok(second)) = (first, second) else { return None };
        // The core decodes the frame; the test does not re-implement SSE or the
        // JSON body, which is exactly the drift a hand parser invites.
        if let Some(tinio_sandbox::Event::Listening { addr }) =
            tinio_sandbox::Event::decode(&format!("{first}\n{second}"))
        {
            return Some(addr);
        }
    }
    None
}

/// Whether `pid` is gone within the test timeout.
fn waits_for_exit(pid: u32) -> bool {
    let deadline = Instant::now() + Duration::from_secs(timeout());
    while Instant::now() < deadline {
        let alive = Command::new("kill")
            .args(["-0", &pid.to_string()])
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if !alive {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn request(addr: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to the confined server");
    stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    response
}

/// `pgrep -P` for the confined child; `sandbox-exec` execs in place, so the
/// pid it reports is the one the test signals.
fn wait_for_child_pid(parent: u32) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(timeout());
    while Instant::now() < deadline {
        if let Ok(out) = Command::new("pgrep").args(["-P", &parent.to_string()]).output() {
            if let Some(pid) = String::from_utf8_lossy(&out.stdout).split_whitespace().next() {
                if let Ok(pid) = pid.parse() {
                    return pid;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("the launcher never showed a child process");
}

fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(timeout());
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the launcher did not exit after its child was signalled");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn timeout() -> u64 {
    std::env::var("TINIO_TEST_WAIT_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(60)
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-server --test sandbox_macos_e2e -- --nocapture`
Expected: FAIL before Step 3 with "no test target named `sandbox_macos_e2e`"; after Step 3, the first real failures are the interesting ones — no `Listening` frame (the launcher failed, or the event relay is not running), a refused connection (the child never reached `accept`), or a missing access log (the write grant is wrong).

- [x] **Step 3: Wire the test target**

`crates/tinio-server/Cargo.toml`:

```toml
[[test]]
name = "sandbox_macos_e2e"
required-features = ["sandbox"]
```

- [x] **Step 4: Make it pass**

The three likely causes, in order:

1. `no Listening frame`: either the launcher died before the child started (run the example by hand, Task 8 Step 4, and read `sandbox-exec`'s diagnostic on stderr — that is what `preflight` exists to surface), or the child is serving but its stdout is not being relayed (the relay thread in `confine`).
2. `connection refused`: the child is running but never accepts. Check Task 7's `accept=` result — this is the same question, now on the inherited descriptor through `tokio::net::TcpListener::from_std`.
3. `no access log`: the state directory is not inside a writable exposure. Check the exposure set actually passed (Task 8's `sandbox_exposures`) and whether `with_canonical_paths` produced the rule the kernel matches.

- [x] **Step 5: Run the test to verify it passes**

Run: `cargo test -p tinio-server --test sandbox_macos_e2e -- --nocapture`
Expected: PASS. Confirm the leg's claims in the output: an HTTP response, an access log on disk, the launcher's own `info!` events on stderr (`profile`, `preflight`, `spawn`, `exit`), a forwarded exit status of `Some(0)` — the child's own graceful stop, carried back by the launcher — and frames and nothing else on stdout.

- [x] **Step 6: Confirm the CI leg picks it up**
  *(Ticked on a **measured** selection run, 2026-09-26: `cargo-nextest 0.9.146` — the version `.config/nextest.toml:25` says its filter was verified against — on the macOS guest, where `cargo nextest list --cargo-profile ci --profile ci --workspace --exclude tinio-e2e` reported **1719 tests across 36 binaries**. `tinio-server::sandbox_macos_e2e` is present with its **3** tests and the six `tinio-sandbox-macos` suite binaries with **13** (`exposures` 3, `handover` 1, `injection` 2, `network` 3, `relay` 2, `startup` 2) beside that crate's 26 lib tests. That command is `action.yml:62`'s verbatim, with no `-E`.)*

**This step's original command was wrong, and the run showed it rather than the docs arguing it.** `-E 'test(sandbox)'` selects **21 tests across 4 binaries** — 12 `tinio-cli sandbox::tests::*`, one `tinio-config sources::tests::sandbox_flag_parses_strictly`, 5 `tinio-sandbox-core sandbox::tests::*`, 3 `tinio-server::serve_sandbox_channels *` — and **not one** from `tinio-sandbox-macos` or `sandbox_macos_e2e`. `test()` matches the **test name**, not the binary id, and no test name in either target contains `sandbox`; only module paths and an unrelated binary name do. The predicate for a target is `binary_id`, and it needs the **qualified** form: `-E 'binary_id(sandbox_macos_e2e)'` fails to parse (`operator didn't match any binary IDs`, exit 94) while `-E 'binary_id(tinio-server::sandbox_macos_e2e)'` yields the 3 and `-E 'binary_id(/^tinio-sandbox-macos::./)'` the 13.

**One correction to this step's own mechanism claim.** `jail` and `signals` are absent from the macOS leg **not** because `profile.ci.default-filter` removed them: `--ignore-default-filter` returned a byte-identical 1719/36 and nextest printed no "skipped via profile.ci.default-filter" note. Both binaries *are* built for the leg and each answers `--list` with **0 tests**, because `crates/tinio-sandbox-linux/tests/jail.rs` and `crates/tinio-sandbox-windows/tests/signals.rs` are `#![cfg(target_os = …)]`-emptied on macOS. That filter is inert on this leg and bites on the linux/windows ones.

**Listing is not a hermetic dry run.** `cargo nextest list` executed `#[test]` bodies during discovery — macOS kernel `Sandbox` entries appeared while listing — so a list run is evidence about *selection* and nothing about pass/fail; Step 5 owns that.

Run: `cargo nextest list --cargo-profile ci --profile ci --workspace --exclude tinio-e2e` (no `-E`; add `--ignore-default-filter` to see that the floor does not move on this leg)
Expected: **1719 tests across 36 binaries**; `tinio-server::sandbox_macos_e2e` with its three tests, the six `tinio-sandbox-macos` suite binaries with thirteen, plus the Linux confinement tests (which skip by `cfg`). The CI macOS leg (`test-port`, `macos-latest`, `default` mode) runs that `-E`-free command set through `./.github/actions/run-test-leg`; Step 7 adds the linting that leg needs on top; the `no-default` leg skips `sandbox_macos_e2e` through `required-features`, and `tinio-sandbox-macos`'s own tests run in both.

The macOS leg must prove, and this task makes it prove: a confined process starts and reads its data; the exposure rules bite (a refused read and a refused write, either of the two errors a refusal can wear); outbound is refused after a proven baseline; the profile grants no `network-outbound`; a signal to the launcher reaches the confined child (Task 3's signal relay, re-asserted here against the real `sandbox-exec` process tree); the confined server emits a `Listening` event that the launcher relays, and nothing but frames reaches the consumer; the launcher's `info!` events are on stderr; and the confined *server* serves a request and has its exit status forwarded. A profile that merely fails to crash the process passes none of them.

**The leg records refusals; it does not assert on them, and the asymmetry with Task 10 is deliberate.** On a shared runner the `Sandbox` log is every process on the machine and the leg runs concurrently with the rest of the matrix, so a denial line there is evidence to read rather than a gate — and attributing one at all needs the process name in the record, which is the trap Task 10's script comments describe. The exact-zero assertion lives in that script (Task 10), where the machine is quiet and the profile under test is the only thing running.

- [x] **Step 7: Make the macOS leg lint the workspace, and correct the lint job's comment**

`.github/workflows/ci.yml`. The `lint` job lints only on ubuntu, and macOS-only code is invisible to it — the comment in that job claims the opposite and is already false (the workspace already carries macOS-conditional code: `#[cfg(all(unix, not(target_os = "macos")))]` in `crates/tinio-fs/src/fsutil.rs` and `crates/tinio-fs/src/backend/tests.rs`, whose macOS side ubuntu never compiles, and this phase adds a whole backend of it), so it is corrected here rather than left to mislead the next reader. Add the step below to the `test-port` job, after `*sccache` and before `run-test-leg`, so the leg's lint is its gate and its failure is reported before the leg spends its test time:

```yaml
      # macOS-only code is invisible to the ubuntu clippy job (see the lint
      # job's own comment), so this leg lints the workspace as well as testing
      # it. Native, not a cross-lint: the runner is already a Mac, so no SDK
      # and no cross toolchain is involved. Gated to the default leg so it
      # does not run twice, and the toolchain anchor installs without clippy,
      # hence the component add.
      - name: clippy (-D warnings, macOS-only code)
        if: matrix.os == 'macos-latest' && matrix.target == 'default'
        run: |
          rustup component add clippy
          cargo clippy --profile ci --workspace --all-targets -- -D warnings
```

And replace the `lint` job's matrix comment (its first three lines are the ones that are false):

```yaml
        # 2 clippy runs, both on ubuntu. The unix leg lints linux, and that
        # does NOT cover macos: the workspace carries macOS-only code behind
        # cfg(target_os = "macos") — crates/tinio-fs/src/fsutil.rs today, and
        # the whole tinio-sandbox-macos backend after P3 — which a linux
        # compile never builds, so the macos-latest leg of test-port lints it
        # natively instead. The windows-gnu leg cross-lints the cfg(windows)
        # code; the msvc-native clippy legs are dropped (their compile
        # coverage already comes from the test-port matrix).
```

**Linting the workspace once, from ubuntu, against Apple's target was measured and rejected — the negative result is worth keeping.** `cargo clippy --target x86_64-apple-darwin --profile ci --workspace --all-targets -- -D warnings` fails in a *dependency's* build script before it reaches any of this workspace's code: `alloca v0.4.0`, pulled in through `criterion`, has cc-rs invoke the host gcc with Darwin-only flags. Dropping `--all-targets` does not rescue it; it only moves the failure to `psm v0.1.32`, a non-dev dependency of `tinio-select`. The route becomes passable only with `CC_x86_64_apple_darwin=clang` **and** `CFLAGS_x86_64_apple_darwin=-ffreestanding`, and the second of those works only by accident: the sole C file in the graph needs nothing but `stddef.h` and `stdint.h`, so a freestanding compile of it happens to succeed — any dependency whose C code wants real libc headers would break it, and there is no apt-installable macOS SDK to fall back on. A lint gate that depends on the C files of other people's crates is a gate the next dependency breaks, so the leg lints natively.

---

### Task 10: The macOS verification script and its documentation

**Files:**
- Create: `crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh`
- Modify: `docs/tests.md`

**Interfaces:**
- Consumes: nothing.
- Produces: the one command that reproduces the phase's macOS acceptance, and its record in the documentation the workspace treats as the source of truth for test runs.

- [x] **Step 1: Write the script**

`crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh`:

```bash
#!/usr/bin/env bash
# Verify the macOS sandbox on a real Mac: the crate's confinement tests, the
# confined server end to end, and the profile's necessity table — and fail if
# the server's own process was refused anything the profile was not expected to
# refuse.
#
# The check asserts exactly zero *unexpected* refusals, on purpose: a refusal
# outside the allow-list below is information about the profile rather than a
# reason to relax the check. Investigate the profile, never this script.
#
# **Every confined process carries a noise floor**, measured on macOS 15.8: a
# read of its own executable image, a write to `/dev/dtracehelper`, and an
# outbound connection to the syslog socket. None of the three is something the
# server asked for, and none can be granted away without granting a great deal
# more than the profile should — the executable's own directory is deliberately
# not exposed (decision 22 is Windows's) and `/private/var/run/syslog` is a
# network path. They are therefore named as expected rather than granted, and
# anything else is the finding.
#
# The log is system-wide and asynchronous, so two things are load-bearing.
# Stream it into a file for the whole run and read the file only once the stream
# has stopped, or a record the run already produced can be missed. And
# **attribute by process name**: the record is what makes a denial ours — a run
# that looked like it had one refusal in fact had zero, because the line came
# from `mobileassetd`, a system daemon. Matching "Sandbox: serve(" is what
# separates the confined server from everything else on the machine.
#
# **`log stream`, not `log show`.** On macOS 15.8 `log show` returns nothing for
# refusals even with `--info --debug` and an hour-wide window; only the live
# stream reports them.
#
# CI does not use this script and does not assert on refusals: there the log is
# shared with everything else on the runner and the leg runs concurrently with
# the rest of the matrix, so a refusal is evidence to read rather than a gate
# (design §12). The exact-zero assertion belongs here, on a quiet machine.
#
# Usage: bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "macos-sandbox-verify.sh needs macOS (Seatbelt is a macOS facility)"; exit 1
fi

if ! command -v sandbox-exec >/dev/null; then
  echo "sandbox-exec is missing. It is deprecated by Apple and still shipped;"
  echo "without it --sandbox has no mechanism on macOS and fails closed."
  exit 1
fi

LOG="$(mktemp -t tinio-sandbox-refusals)"
log stream --predicate 'sender == "Sandbox"' --style compact >"$LOG" 2>/dev/null &
LOG_PID=$!
trap 'kill "$LOG_PID" 2>/dev/null || true; rm -f "$LOG"' EXIT

echo "== the confinement tests (startup, exposures, network, relay) =="
cargo test -p tinio-sandbox-macos -- --nocapture

echo
echo "== the confined server, end to end =="
cargo test -p tinio-server --test sandbox_macos_e2e -- --nocapture

echo
echo "== the baseline's necessity table =="
bash crates/tinio-sandbox-macos/scripts/prune-profile.sh

echo
echo "== the server's own refusals =="
kill "$LOG_PID" 2>/dev/null || true
wait "$LOG_PID" 2>/dev/null || true
# The noise floor, named so that the check asserts on the rest. Each entry is
# spelled as the log spells it, and the executable's is a suffix match because
# the path depends on the profile directory the run was built in.
EXPECTED='file-read-data .*/(serve|sandbox-exec)$|file-write-data /dev/dtracehelper|network-outbound /private/var/run/syslog'
refusals=$(grep "Sandbox: serve(" "$LOG" | grep -Ev "$EXPECTED" || true)
if [[ -n "$refusals" ]]; then
  echo "the profile no longer covers the server:"
  echo "$refusals"
  echo "fix the profile, not this check"
  exit 1
fi
echo "no unexpected refusal for the server's own process"
```

Make it executable: `chmod +x crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh`.

- [x] **Step 2: Run it**

Run: `bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh`
Expected: the three sections pass, the necessity table prints, and the refusal check reports no unexpected refusal for the server's own process — the three expected operations are named in the script and anything else is a failure. This is the P3 acceptance run (design §13's "the operator verifies on their own machine").

- [x] **Step 3: Record the commands in `docs/tests.md`**

Add a section after `## Platform-gated verification (WSL2)`, in the file's existing compressed style:

```markdown
## Platform-gated verification (macOS)

- Confinement (`--sandbox`, SBPL via `sandbox-exec -p`): `bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` — the confinement tests, the confined server, the profile's necessity table, and an assertion that the server's own process logged no `Sandbox` refusal outside the three the noise floor produces (its own executable's image, `/dev/dtracehelper`, and the syslog socket). Needs a real Mac; the CI macOS leg (`test-port`) runs the first two and records refusals rather than asserting on them, its log being shared and concurrent. Refusals are read with `log stream --predicate 'sender == "Sandbox"'`: on macOS 15.8 `log show` reports none even with `--info --debug`.
- `sandbox-exec` is deprecated by Apple and still shipped. That script checks for it and fails closed without it.
- The confined server's endpoint is an `Event` frame — `event: listening`, then `data: {"addr":"127.0.0.1:<port>"}`, then a blank line (the name travels in the SSE field and the payload carries only what the name does not say) — and not a line to scrape. Parsing lives with the consumer; the harness's `wait_for_ready` change is the Linux plan's (it emits the first events), not this phase's. The launcher's own record is on stderr: shared §7's `info!` half, at `Verbosity::Info`, without an environment filter.
```

- [x] **Step 4: Run the documentation gate**

Run:

```bash
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --doc --workspace --exclude tinio-e2e
```

Expected: both clean. The macOS-only items' doctests are compiled only on macOS, so run this the same way on the Mac when the phase lands.

---

### Task 11: Check the macOS evidence rows against the run, and run the whole gate

**Result, 2026-09-25: the profile's measurements are recorded in `docs/superpowers/specs/2026-09-18-sandbox-design.md` §5 ("The macOS SBPL profile").** Read that section, not this task's checklist, for what each rule's failure mode is.

**Files:**
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (§5's profile block and the paragraphs around it, §12's CI bullet, and the Risks row only if the run contradicts it)

**Interfaces:**
- Consumes: the outputs of Tasks 4–6 (the probe reports, the necessity table, the network answer).
- Produces: a design document that still says exactly what was measured, and the phase's own gate run.

**There is nothing left to flip, and that is what this task records.** The design was written while macOS was unmeasured, and P3 originally carried two steps that replaced its `(unverified)` rows with results. The VM run of 2026-09-19 was folded into the design first, so the macOS rows are measured, §5 is measured text, and the only `(unverified)` rows remaining are Windows's. What this task does is the opposite of a flip: it reads the document against the run and corrects only what the run contradicts.

- [x] **Step 1: Read the design's macOS rows against your run, and do not re-derive them**

The doc was updated when the VM run was folded in, so §5 already holds the measured profile, the four load-bearing allowances with the failure mode of each, and the symlink/`file-read-metadata` rule; §7's macOS row already reads "no network capability of any kind"; decisions 16 and 24 already cite the `accept` and `-D` results; and the evidence table already lists every macOS row with its observation. Check each against your own run and correct only what your run contradicts. Those paragraphs were written from a probe on macOS 15.8 with a listener handover, which is exactly what Task 7 exercises — so a disagreement means something in the crate, not in the doc, and it is worth stopping over.

That the only `(unverified)` rows left are Windows's is itself worth checking rather than assuming: `rg -n 'unverified' docs/superpowers/specs/2026-09-18-sandbox-design.md` should return two rows, and neither is about this platform. A macOS row you cannot match to a pasted measurement is something to report, never a row to edit into shape — and a row that claims a result nobody observed is worse than a row that stays open.

**One claim is known to be wrong before you start, and this is where it gets corrected.** The evidence table's row about the server running under §5's profile says the log returns **zero** violations for the server's own process. That is a rendering of a measurement taken with `log show`, which on 15.8 reports no refusals at all — and every confined process in fact carries the noise floor Task 4 and Task 10 name (its own executable's `file-read-data`, `file-write-data /dev/dtracehelper`, `network-outbound /private/var/run/syslog`). The row is corrected here to say what was observed: no refusal *outside that floor*, read with `log stream`.

- [x] **Step 2: Correct §5's profile text where this phase's run contradicts it — which it does**

Design §5 holds the measured profile, and **this phase's Task 1 already ships a *narrower* one**, deliberately: `/private` is split into `/private/etc` and `/private/var/folders`, the `(subpath "/etc")` rule is deleted (a symlink into `/private`, matching nothing), the `(subpath "/dev")` blanket is replaced by the device literals, `(allow process-exec)` becomes `(allow process-exec (literal (param "P")))` — the one exec the launch performs, rather than whatever the child can read — and `(allow process-fork)`/`(allow signal (target self))` are carried until Task 5's run decides them. So the correction here is not conditional on Task 5 finding something: the two texts disagree, and the run is what justifies the narrower one. Update §5's profile block and the paragraphs around it from the necessity table:

- each surviving rule gets the observation that keeps it, in the design's own voice;
- each deleted rule gets one sentence saying it was removed by measurement, so a reader does not re-add it from the earlier text;
- the device grant is brought in line with what Task 1 renders (`file-write*` on `/dev/null`, `/dev/stdout`, `/dev/stderr`, and the read grants on the other three), and the `(subpath "/dev")` blanket is gone;
- the `process-exec` entry becomes the narrowed rule, with the `P` parameter and what being wrong costs (a refused exec, `rc=71` with the `execvp()` diagnostic) stated beside it, so the design does not read as having intended an unfiltered grant — or as having measured the narrowing, which it did not.

The Risks row is the other thing to touch, and only if the run contradicts it: it says the remaining macOS work is a smoke test rather than a research question, and if Task 5 pruned an allowance §5 still lists, or if Task 4 needed one the doc never mentions, both are wrong and get corrected with the observation that justifies it. An edit for its own sake is how a measured statement turns back into a guess — but this phase's narrowing is not an edit for its own sake, it is the operator's decision and Task 5's table is its evidence.

- [x] **Step 3: Record that the macOS leg lints the workspace, not only tests it**

§12's **CI** bullet describes which leg does what, and P3 changed one of those facts: the `macos-latest` leg of `test-port` now runs `cargo clippy --profile ci --workspace --all-targets -- -D warnings` as well as the test suite (Task 9 Step 7), because macOS-only code never compiles in the ubuntu clippy job. Add that to the existing bullet — it is one sentence, and it goes inside the bullet, not beside it:

```markdown
The `macos-latest` leg lints the workspace as well as testing it, since macOS-only code is invisible to the ubuntu clippy job.
```

Do not add a second bullet: §12's CI bullet is the one that already says which leg does what, and a new bullet would be a second place to keep in step.

- [x] **Step 4: Run the standard acceptance gate**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo check --workspace --all-targets
cargo test --workspace --exclude tinio-e2e
cargo test --doc --workspace --exclude tinio-e2e
cargo test --workspace --exclude tinio-e2e --no-default-features
cargo check -p tinio-server --no-default-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

Expected: all clean. On the authoring host the macOS tests compile to empty binaries and the Linux ones are unaffected; on the Mac, `bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` must be green in the same session.

- [x] **Step 5: Confirm the crate's lint boundary stayed where the design put it**

Run: `rg -n 'allow\(unsafe_code\)' crates/tinio-sandbox-macos/src crates/tinio-sandbox-linux/src crates/tinio-sandbox-windows/src crates/tinio-sandbox-core/src crates/tinio/src`
Expected: **one** line from `tinio-sandbox-macos` — the attribute on `launch::inherited_fd`, decision 14's name for the site, which its two safe callers `launch::listener` and `launch::control` reach for the child's rebuild from the inherited descriptor, whose `# Safety` section states the invariant (the descriptor is the launcher's, held open for the child's whole lifetime and never closed) — and no second one **as this task ran (the count is three since the in-process mechanism landed; decision 14 owns the list)** — which is the boundary this step enforces: the crate is `deny` with that site alone, because `BorrowedFd::borrow_raw` and `OwnedFd::from_raw_fd` are both `unsafe fn` and no safe route to the rebuild exists. Then three from `tinio-sandbox-linux` (`kernel_fork` in the user-namespace probe, the `unshare` call, and the seccomp `prctl`), **nothing** from `tinio-sandbox-core` and the facade (both stay `forbid`), and whatever `tinio-sandbox-windows` establishes if P2 has landed — one module per Win32 capability, its count taken from its implementation. Every one of them is listed in design decision 14 and its Bookkeeping; a hit outside that list is the failure this step is looking for, and a macOS hit count other than one is the same failure.

---

## Commit

**One commit, made by hand, at the end of the phase.** No task in this plan commits, so every change accumulates in the working tree and the phase is reviewed as one diff rather than a commit per task. `CLAUDE.md` owns the authorisation rule — git writes are asked for per operation, and the index is never written by an agent — so this section fixes the phase's **scope** and its message, and is not permission to run it.

Run the full Standard acceptance block (`docs/tests.md`) first, and commit only once the phase's own review is clean. The message is a conventional header plus single-line cause-and-effect bullets: what changed and why, never where it lives. It is a starting point rather than a description of finished work — where the phase's actual run makes a line of it false, that line changes.

```bash
git add crates/tinio-sandbox-macos crates/tinio-server .github/workflows/ci.yml Cargo.toml docs/tests.md docs/superpowers/specs/2026-09-18-sandbox-design.md
git commit -F - <<'EOF'
feat(sandbox-macos): confine serve with a Seatbelt profile

- sandbox-exec applies a profile only to a process it execs, so parent stays outside
- Sandboxed process cannot apply a second sandbox: the launcher must stay out
- Profile rides inline in argv: no profile file to create or clean up
- Exposed paths passed as parameters, not interpolated: no profile injection
- Listener bound outside and handed in, so no network rule is needed to serve
- Allowance set pruned rule by rule: each survivor is one the server needs
- Parent stays outside but holds the listener for the child's whole life
EOF
```

## Self-Review

**Spec coverage.** Walking `docs/superpowers/specs/2026-09-18-sandbox-design.md` against the tasks above:

| Spec item | Task |
|---|---|
| §1 the crate's place in the layering, core consumed not rebuilt | Depends-on-P0/P1/P2 table, 1, 2 |
| §2 macOS column of the exposure table (`file-read*` vs `file-read* file-write*`, state dir always rw, "visible, access denied") | 1, 6 |
| §3 launch contract, steps 1–8 | 8 (steps 1–5 and 8), 2 (steps 6–7: the profile, `sandbox-exec`, the payload on stdin, the listener descriptor in argv, the stdout event relay); the supervision loop itself is core's, consumed unchanged |
| §3 step 7 and decision 17: the payload on the child's stdin, with no descriptor element of its own | 2 (the write and the close), 4 (the harness builds the same plan), 8 (the child reads it back with `adopt`) |
| §3 step 8 and decision 19: stdout is an SSE-framed event stream, the launcher re-emits and never forwards bytes, stderr is untouched | 2 (the relay thread over the core's `Observer`, stderr inherited), 8 (`Listening` from the server, `Error` from the launcher), 9 (the consumer sees frames only) |
| §3 step 9 termination-signal forwarding (decision 20) | 3 (the relay: mechanism, async-signal-safety, the launcher's `on_stop`, the child's `on_forwarded_stop` behind the facade, and its own tests), 2 (the place the launcher's arming call goes), 8 (`confined` creates the stop channel and registers the hook; `disarm` is the teardown `supervise` takes), 9 (the launcher-signal assertion, kept distinct from exit-status forwarding) |
| §5 the SBPL profile, `(deny default)`, the four load-bearing allowances and the failure mode of each, the empty network surface, paths as `-D` parameters | 1 (the text and the tables), 2 (the argv), 4 (a working set), 5 (the settled, pruned one), 6 (the exposure rules bite) |
| §7 the uniform network rule, macOS row ("never grants `network-outbound`") | 7 |
| §8 capabilities | **not applicable on macOS** — Seatbelt has no capability model; nothing to drop, and `UID` is unchanged (decision 11 is Linux's) |
| §9 read-only as a value, the state directory's exposure when it moves outside the root | 1 (the rule), 8 (the exposure set), 6 (the denial) |
| §10 the entry-point sequence | 8 |
| §11 errors: refused profile carrying the binary's diagnostic; `sandbox-exec` missing; payload absent → treated as launcher | 2 (`preflight`, `spawn_error`, and the `BadPath` refusals), 8 (Step 1's verification of the read-only probe), 9 (the failure causes named in its Step 4) |
| §12 unit tests (profile text), integration (confined server), network (baseline then denial), CI macOS leg | 1 (the profile), 2 (the argv and the tracing test), 4 (startup), 6 (exposures), 7 (network), 9 (the e2e and the leg) |
| §13 P3's row | all |
| decision 2 `Confinement`, no `#[cfg]` at the call site | 8 (one `#[cfg]` for who binds, which decision 16 makes platform-dependent; the confinement *shape* stays cfg-free) |
| decision 6 macOS launches a child | 2, 8 |
| decision 8 read-only is the absence of a write permission | 1, 6 |
| decision 14 `deny` plus enumerated `#[allow(unsafe_code)]` sites per backend | 1 (the manifest's `unsafe_code = "deny"`), 2 (`listener`: the rebuild from the inherited descriptor, the attribute, and the `# Safety` invariant — the crate's first site; it had exactly one when this plan was written and has three since 2026-09-26, when the in-process mechanism added `apply::libsystem` and `apply::validator`'s `fork`) |
| decision 16 the parent binds the port; no `network-bind`; `accept` ungated on the inherited listener | 7, 8 |
| decision 17 the payload is the only discriminator, and it travels on stdin | 2 (write and close), 4 (`payload=ok` in the harness), 8 (`adopt`) |
| decision 18 no retained parse in the parent; one targeted lookup | 8 (`probe_read_only`, verified not written) |
| decision 19 the child owns the endpoint event, and the launcher relays it — the parent knows the port but never prints it | 8 (the child emits `Listening`, the launcher relays it and emits only `Error`), 9 (frames and nothing else reach the consumer) |
| decision 22 the executable's directory is exposed read-only — **Windows's rule, not macOS's**; macOS deliberately spends no grant on its own location | 8 (the exposure set is P1's `sandbox_exposures`, which carries no executable-directory rule, and the macOS backend adds none — Task 2 records why where the helper used to be, so it is not re-added) |
| decision 24 `-p` inline, no profile file, exposed paths as `-D` parameters | 1 (the `(param "E<i>")` rendering and its `params`), 2 (the argv tests) |
| decision 25 the `otel` conflict | **not in P3** — `serve` never passes an OTLP endpoint and `otel` is not a default feature; the check belongs with the CLI, exactly as the Linux plan's review recorded |
| decision 26 confinement is one-way and before the runtime | 8 (the child builds its runtime only after adopting the payload) |
| decision 20 the launcher relays termination signals | 3 (macOS's defensive half; P2 owns the required one), 8 (the teardown that disarms it) |
| shared §7 the level split (`info!` for what a confinement was, `debug!` for the detail), failures logged where the `Error` is built | 1 (the profile's counts, and the crate-level statement), 2 (the pre-flight, the spawn, the payload, the close-on-exec, the listener rebuild, and the recording-subscriber test), 3 (each relayed signal, each arm and disarm), 8 (the launcher's subscriber and the exit), 9 (the `info!` half asserted against stderr) |

**Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N". Two places carry an explicit *procedure* instead of a value, because the value cannot be known before a Mac runs it — and both say so in the step: the baseline allowance set (Tasks 4 and 5, which measure and then require the pruning run) and the `accept` rule (Task 7, whose outcome is already measured and whose step says what to do if the machine disagrees). Task 11 forbids writing an evidence row without a pasted measurement rather than templating one.

**Type consistency.** Every core item is called exactly as P0 Task 2 fixes it — that surface, and the ownership of it, is `2026-09-18-sandbox-shared.md` §3's and is not repeated here. There is no `nonce()`, no `MAGIC` and no `with_nonce` on it: the payload is one JSON object. **The crate's public surface, and nothing else**: `Backend` and `backend()` at the root (the type the facade's `platform()` selects — Task 1), `confine`, `command_args(&Path, &[OsString], &Profile, Option<i64>) -> Vec<OsString>` and `SANDBOX_EXEC` (Task 2), `launch::listener` and `launch::PREFLIGHT_MARKER`, `profile::{profile(&Sandbox) -> Result<Profile>, Profile { text: String, params: Vec<(String, OsString)>, rules: usize }, rule_count, with_canonical_paths, Allowance, RUNTIME_BASELINE, DEVICE_RULES}` (Task 1), and `relay::{on_stop, on_forwarded_stop, disarm, relay_decision, RELAYED_SIGNALS}` (Task 3) — the launcher's half and the child's half of the relay, in that order, with the facade presenting the child's as `on_stop` (Task 3 Step 4) so both launcher platforms call one name. **Everything else in those modules is private and stays private** — `validate_path`, `refuse` and `push` in `profile.rs`; `hand_over`, `make_inheritable`, `preflight`, `spawn_error` and `PREFLIGHT_TIMEOUT` in `launch.rs` — and the Self-Review says so rather than listing them as surface, because none of them is called from outside the crate. **`serve`'s signature is P1's, `2026-09-18-sandbox-shared.md` §4 owns it, and P3 changes none of it**: `confined` passes the storage it built, its listener and its stop receiver; the plain serving path passes its own storage and no platform values at all; and the launcher passes none of them because it never reaches `serve` — the placement of the two storage constructions (Step 2(e)) is what keeps the launcher from building one. `harness::{Report, Child, child, is_child_process, launch, line, finish, run_confined, run_pruned, remove_rule, CHILD_ENV, PARAMS_ENV, LAUNCH_ENV}` is created in Task 4 and used by Tasks 5, 6 and 7. No name is introduced twice with different meanings; the test harness's `Child` is its own type and is always written `harness::Child`. **The four failure builders are the core's and this crate defines none of them** — `refused`, `step` and `launch` are imported by `launch.rs` and `refused` is what `spawn_error`'s `NotFound` arm refuses with (P0 Task 1 landed them; the phase-basis correction is where this crate stopped defining its own `step`). The crate's lint is `unsafe_code = "deny"` with its sites enumerated by decision 14 — the inherited-descriptor rebuild in `launch::inherited_fd` (the helper both safe rebuilds, `launch::listener` and `launch::control`, call), the libSystem FFI in `apply`, and `apply`'s validator `fork` — and `inherited_fd`'s `# Safety` section carries the invariant that the descriptor is the launcher's, held open for the child's whole lifetime and never closed, **and that `from_raw_fd` is called at most once per descriptor number**. `make_inheritable` is deliberately written to need none: it clears `FD_CLOEXEC` through the socket's own `AsFd` rather than through a descriptor reconstructed from an integer.

**Deliberately out of P3's scope.**

- The CLI flag, the `[server] sandbox` key and `TINIO_SANDBOX` (design Non-goals: they land with T066/T067). P3 reaches operators through `serve` only, exactly as P1 does on Linux (where the phase is Linux-only by construction now).
- `TINIO_SANDBOX_MODEL` and the model selection of decision 7: Linux only. macOS had one mechanism and no fallback ladder when this plan was written; **2026-09-26 added the second** (`TINIO_SANDBOX_MACOS_MECHANISM`, `sandbox-exec` default and `sandbox-init` the alternative, no ladder between them — an unknown value is a usage error), which is decision 6's and not a P3 task.
- Windows (P2) and the other-unix `chroot` fallback (P4).
- Read-only *mode* (FR-023) as a feature: it has **landed**. `serve` resolves the mode from `[server] read_only`, the `TINIO_READ_ONLY` overlay and the `--read-only` flag, and relocates the state directory to `TINIO_STATE_DIR` or `<home>/.tinio/roots/<hash>/` (`crates/tinio-config/src/state.rs`). What is P3's is the *confinement* of it — rendering `AccessMode::ReadOnly` on the served root and exposing the relocated state directory writable — which is why Task 8's `main` takes the state directory from the storage instead of hand-building `<root>/.tinio`.
- The `otel`/`--sandbox` startup conflict (decision 25): `serve` never passes an endpoint.
- **Tying the confined child's *lifetime* to the launcher's.** Decision 20's first obligation is a Windows Job object (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`) and has no macOS analogue: a launcher killed with `SIGKILL` leaves a confined server running. Its *second* obligation — relaying termination signals — is in scope and is Task 3. The distinction matters at test time: Task 9 signals the child directly rather than killing the launcher, so the test does not leak a confined server into the CI leg.
  - **Correction, 2026-09-26; revised 2026-09-28: the gap is a hard-killed launcher, and only that.** The bullet above still holds for the launcher that never runs again — no mechanism here can end a child after its supervisor is gone — but it is **not** a statement that the launcher cannot end the child it started. The `state` publish failure that Task 8's launcher hits after the child reports `listening` used to return through `?` and leave a Seatbelt-confined server serving with no `state` file to find it; `tinio-sandbox-core`'s `Child` now carries an optional platform killer (`with_kill`/`kill`, `crates/tinio-sandbox-core/src/launcher/supervise.rs`), Task 2's `confine` supplies a `SIGKILL` to the pid it spawned, and `Child::abort` kills and reaps that child in one call. **The ending is the core's and no longer a platform fact**: the call site (`crates/tinio-cli/src/sandbox/confine.rs`, which absorbed the deleted `sandbox/macos.rs`) calls `child.abort()` unconditionally on the publish failure, and `retract_on_publish_failure` — the last fact consulted about it — was deleted on 2026-09-28 with no readers left in the workspace. That narrows one path — a startup failure leaves no serving process — and creates no lifetime tie: a launcher killed outright still leaves the child, which is what Task 9's signalling decision and this bullet's accepted gap continue to describe.
- **A guard on `argv` length**, and the earlier draft of this bullet counted the wrong quantity. The argv carries two things that grow with the exposure set: the profile **text**, which is one argv element and is not small — every rule is written with a `; tinio:<id> — <evidence>` provenance comment and the baseline's entries carry sentences — and the `-D NAME=VALUE` elements, one pair per exposure, whose value is the operator's path. Decision 24's "argv is bounded by `ARG_MAX`, an order of magnitude larger than the compiled cap" compares the text against the *compiled* object's ~6 bytes per operation, and those are not the two things to compare: the text is kilobytes before a single exposure is added and can approach `ARG_MAX` at the 4096 exposures Task 1's count guard allows. No guard is written here, deliberately and for the reason that guard's own doc comment gives — the exposure set is a handful of paths in practice, and at a size where the text did not fit, the failure is the platform's own — `Command::spawn` answers `ArgumentListTooLong`, and the pre-flight, when it runs, refuses it as a compile failure first — rather than anything silently truncating. A byte budget for the rendered text (comments included) is a follow-up measurement, not a guess to write in now.
- The fallback to `sandbox_init` FFI if Apple removes `sandbox-exec`. The design authorized it as a later fallback and P3's failure mode is "fail closed". **It landed as a selectable mechanism on 2026-09-26** (`TINIO_SANDBOX_MACOS_MECHANISM=sandbox-init`, decision 6 and the Bookkeeping ledger): nothing in P3 depends on it, and this bullet stays as the record of what the phase deliberately left to a later change.

**Decisions taken here that the design leaves open.** These are the places where the plan had to choose:

1. **One `#[cfg(target_os = "macos")]` helper in `serve`** (Task 8), for *who binds the listener*. Decision 16 makes that platform-dependent; decision 2's `Confinement` keeps the confinement *shape* out of the call site, which it still does.
2. **The listener descriptor travels as an argv element spelled `tinio-sandbox-fd=<n>`** (the core's `listener_fd_arg`), and the payload does not: it goes on stdin (decision 17). Both `serve` and the test harness filter it out of their own arguments through the core's `is_launcher_fd_arg` (Tasks 2, 4, 8). **The element has no leading dashes, and that is a hazard rather than a convenience**: libtest reads a positional argument as a test filter, so `sandbox-exec`'s child — which is the same test binary — would have this element as its filter. If it were the only one, the child would select no test at all, run nothing, exit 0 and write no report. `run_confined` therefore launches a **launcher copy** of the test binary with the probe's own name as its filter (Task 4), so the child inherits a filter that matches and this element is a second, harmless one. The earlier version of this decision called it "a harmless extra filter" without arranging that; the reproduction is in Task 4's harness docs (`running 0 tests … 3 filtered out`, exit 0).
3. **Canonical paths are emitted alongside the caller's paths, and a link out of the tree is not followed** (Task 1): macOS resolves `/tmp` to `/private/tmp` and `$TMPDIR` under `/private/var/folders`, and Seatbelt matches what the kernel knows — so the exposed path's own resolved spelling is added, because a rule on the given spelling alone matches nothing. What is *not* added is anything for a link that lives inside the exposed tree: such a link resolves at read time to a path no rule names, and the read is refused. Task 6 measures both halves — a root exposed through a symlink reads through either spelling, and `escape` does not read — and the exposure rules themselves are not separately prunable (they share one id), which is why the measurement is the test rather than a pruning run.
4. **Non-UTF-8 and control-character paths are refused**, not escaped or lossily converted (Task 1): a lossy conversion would name a different path than the operator exposed.
5. **The executable's directory is deliberately *not* exposed on macOS** (Task 2, Task 8). An earlier draft of this plan had the backend add it, by reading decision 22 as "launcher platforms"; the design now scopes that grant to Windows, where a confined child genuinely cannot load without an ACE on the binary's directory. On macOS the measurement is the opposite: `process-exec` is the only gate, `file-read-data` on the child's own image and on its directory is refused and logged, and the program runs regardless — true of an 81 MB Rust server binary as much as of a small C tool. §5's profile never carried the grant, so nothing is spent and nothing is lost, and `launch.rs` carries the note where the helper used to be so a future reader does not re-add it. (The launcher re-invokes itself, so the program path is the running process's own; nothing in `LaunchPlan` has to carry it.)
6. **`Sandbox::device_files()` on macOS meant "these literal paths, and the two standard streams"** (Task 1) — `/dev/null` read and write, `/dev/zero`, `/dev/random` and `/dev/urandom` read, and `file-write*` on `/dev/stdout` and `/dev/stderr` because design §5's measured profile carries them — **and both the builder and the table behind it are gone**: the shared `Sandbox` carries no device knob (the flag is `tinio_sandbox_linux::Options`'s now), Task 5's audit deleted `DEVICE_RULES` with the device literals, and the emitted profile is `RUNTIME_BASELINE` alone (the Progress note above). The blanket `(subpath "/dev")` that profile also carried is **gone**: it would have made the literals redundant and reached every device node on the host, which is the opposite of what the narrowed read set is for. Linux expresses the same request as bind mounts into the jail.
7. **A pre-flight `sandbox-exec -p <profile> -D NAME=VALUE … /bin/echo <marker>`** (Task 2), built by the same `command_args` the launch uses so the parameters are included — the program parameter among them, bound to `/bin/echo`, which is the exec that probe performs — because design §11 wants the binary's own diagnostic and the child's stderr cannot be captured without a pump that deadlocks when its pipe fills.
8. **`fcntl(F_SETFD, 0)` through `rustix` for the inherited listener** (Task 2), safe on the parent's side and therefore needing no `unsafe` there: `make_inheritable` takes the `TcpListener` itself, which implements `AsFd`, so `fcntl_setfd` is handed a real borrowed descriptor and nothing in the parent reconstructs one from an integer. The child's rebuild is **one of the crate's three `#[allow(unsafe_code)]` sites** — `apply::libsystem`'s FFI block and `apply::validator`'s `fork` are the others — and it has to be: `BorrowedFd::borrow_raw` and `OwnedFd::from_raw_fd` are both `unsafe fn` (`E0133` from `rustc` 1.98.1 on macOS 15.8, 2026-09-19), and `rustix` exposes no raw-descriptor constructor for a socket. The earlier claim that this crate could avoid `unsafe` altogether — via `BorrowedFd::try_clone_to_owned` plus `From<OwnedFd>` — did not survive contact with the compiler, because both of those are safe only once the borrowed or owned descriptor already exists (decision 14, Scope).
9. **`tinio-config` gains `server::probe_read_only(&[u8])`**: decision 18's targeted single-key lookup has to live in the crate that owns the schema. **P2 Task 11 Step 1 is the edit** — P2 is the earlier of the two phases that need it — and this plan verifies it in Task 8 Step 1 instead of writing it a second time, which is what `2026-09-18-sandbox-shared.md` §4 records.
10. **The outbound test's baseline is the parent's own loopback listener** (Task 7), so the "prove it was reachable first" requirement of design §12 is satisfied without the CI runner's internet connectivity, which would otherwise make the assertion vacuous.
11. **The launcher's forwarded exit status is asserted as `Some(0)`** (Task 9), and that is the shape of the flow rather than a convenience. The signal goes to the launcher, the relayed `SIGTERM` reaches the child, and the child's **own stop hook** (Task 3's `on_forwarded_stop`, through the facade's `on_stop`) catches it: the plane drains, `serve` returns `Ok` and the child exits 0, which the launcher forwards. The `128 + SIGTERM` convention (143) belongs to a shell reporting a signal death, and `ExitStatus::code()` refuses to invent one, so `Some(143)` could never pass. A `None` here means the child died of the default disposition instead — the stop hook did not register — which is a defect in Task 8's `confined` rather than a value to record.
12. **The signal relay uses a safe signal crate, not an FFI handler.** `signal-hook` is one new declared dependency, and the reason is no longer that it preserves `unsafe_code = "forbid"` — this crate cannot keep that lint, because its listener rebuild is irreducibly `unsafe` (Task 2, decision 14). The reason is the smaller one that survives, and Task 3 states it as such: a safe crate whose entire job is a handler plus a self-pipe beats a **fourth** hand-written `unsafe` site under `deny` (the crate carries three already: the listener rebuild, `apply::libsystem`'s FFI block and `apply::validator`'s `fork`), and a hand-rolled handler is precisely where async-signal-safety bugs live. The alternative is the only other correct option — a hand-written `sigaction`, which `rustix` deliberately does not wrap — and it would require adding that site to design decision 14's list. A maintainer who rejects the dependency must take the lint route *explicitly*, and must not read this plan as licence for a quiet `#[allow(unsafe_code)]`.
13. **The relay is armed after the spawn and disarmed in the teardown, and the disarm is a partial undo.** Before the child exists there is nothing to relay to, so dying on a signal that arrives then is correct rather than a gap; and the disarm, driven from `supervise`'s teardown (every exit path, decision 15), closes the window in which a late signal could be forwarded to a recycled pid. **Two limits are stated rather than implied.** It cannot close the whole window — the disarm runs after the wait has returned, so a signal arriving between the reap and the disarm still reaches a pid the kernel may have reused, a window of a few instructions that no standard deployment opens. And it does not restore the default disposition: the handlers stay installed, because `signal-hook`'s iterator API keeps its registration ids to itself, so a process that keeps running after a disarm swallows a later `SIGTERM` rather than dying of it (Task 3's `disarm` doc says so, and the relay tests depend on it).
14. **The armed pid is module state, and the disarm is an explicit function.** `Confinement::Supervise(Child)` carries the child, not a launcher handle, so there is nowhere for `confine` to return a guard through; the state is an `AtomicI32` and the teardown names `disarm`. If the core's enum ever grows a launcher handle, this collapses back into a `Drop`.
15. **The facade splits its exports by how the call site uses them**: a platform-neutral `disarm_signal_relay()` (the supervision path is not `#[cfg]`-branched, so the name must exist everywhere) and `#[cfg(target_os = "macos")]` re-exports of the child-side launcher surface (used only inside already-branched arms). `serve` therefore still names only the facade, which is what keeps decision 3's layering intact.
16. **The payload is the core's, and this launcher only serializes it.** `LaunchPlan::for_launch(config)` takes the configuration text; `confine` writes the frame from `plan.config()` through `LaunchPayload::encode_frame`, as one JSON object, and does not parse it (decision 18) or add anything to it — `plan.payload()` is the pre-change spelling the core removed. An earlier draft had the launcher overwrite a nonce placeholder the caller passed, which was incoherent — exactly the two-owners-for-one-fact shape the reconciled surface removed, and the nonce itself is gone (design decision 17, amended).
17. **This plan names the core's `adopt(args, stdin)` contract, and relies on its stdin rule.** `adopt` reads its reader only when the arguments carry the listener descriptor — stdin belongs to the operator otherwise (a terminal, a heredoc, another tool's pipe), and a blind `read_to_end` would hang a launcher nothing launched. That rule is core's now, unit-testable there without a subprocess; this crate's side of it is the rebuild, `listener(&Adopted)`.
18. **The pre-flight keeps a private stdout convention, does not use the protocol, and is bounded in time.** `/bin/echo`'s marker is captured and compared in-process, never inherited: it is a convention between this launcher and `sandbox-exec`, and `sandbox-exec` is not a tinio process, so it must not write on the event channel. Two stdout conventions therefore exist, and the distinction is stated at both sites rather than left to be inferred. **The capture is a spawn, a deadline and a kill rather than `Command::output()`**: that call waits for both pipes and the exit with no bound, and it is the last thing before the real spawn, so a stall there emits no event, exits never and writes nothing on stderr — indistinguishable, to an operator, from a server that never started. `timeout(1)` is not an alternative on this platform, so the wait is in-process (`PREFLIGHT_TIMEOUT`, 10 s): expiry kills the child and the operator gets an `error!` naming the step and the deadline plus a startup error carrying the same sentence.
19. **The event relay runs on its own thread, and the wait stays on the main one.** Draining the child's stdout in the thread that also waits on it is the classic pipe-buffer deadlock: the child blocks writing, the parent blocks waiting, and neither moves. The relay thread ends when the child closes stdout, which happens at its exit. The core's `Observer` is async, so that thread owns a **current-thread** runtime — the smallest thing that can poll it — rather than the launcher growing one it does not otherwise need.
20. **The listener descriptor has exactly one home: the argv element.** `LaunchPayload` carries `config` (with the `token` and `confinement` halves the launchers added) and no descriptor, and this plan never asks the payload for one — `Adopted::listener()` reads argv, and the launcher writes argv from what the plan was handed. A payload field alongside it would be two sources of truth for one fact, which is the duplication the stdin move removed elsewhere.
21. **The system read set is narrowed to what this crate needs, and the narrowing is part of the deliverable rather than a follow-up.** §5's measured text was the first profile that ran, not the narrowest one that could: `(subpath "/private")` is replaced by `/private/etc` and `/private/var/folders`, the `(subpath "/etc")` rule is deleted outright (it is a symlink into `/private`, so a rule spelled through it matches nothing), and the `(subpath "/dev")` blanket is replaced by the device literals. Every surviving entry names the observation that keeps it, Task 5's pruning run is required, and an entry that survives without one is deleted. The Goal is that the confined process reaches exactly what the operator exposed; this is the plan's half of it.
22. **`process-exec` is narrowed to the child's own path, and the narrowing is derivable rather than a measurement still owed.** The measurement established that *an* exec must be permitted — without the rule `sandbox-exec` exits 71 before consulting anything else — and never that *every* binary must be: unfiltered, and with `/bin`, `/usr` and `/private` readable, it lets the confined child exec anything it can read, which is the confined process's own reach and so is not the local-actor half the threat model excludes. What the launch needs is not a question the log has to answer: `confine` execs exactly one program, so the rule carries one `(literal …)` — `(allow process-exec (literal (param "P")))`, where `P` is the launcher's `current_exe()`, the same value `command_args` writes as the argv's program element, so the filter and the exec cannot drift. The pre-flight binds `P` to `/bin/echo`, the exec it performs. **What is still uncovered is a second exec**, if the confined server ever needs one and no measurement says so — and that is answered by going back to the unfiltered rule with the over-grant written down, never by keeping it quietly. Task 1's table says both halves in the entry's own `evidence`; Task 10 Step 2 carries the same correction into the design's §5 text.
23. **The events follow shared §7's split, and the launcher installs its own subscriber for the launch window.** `info!` for what a confinement was — the profile's rule and parameter counts, the pre-flight and its outcome, the spawn with its pid, the exit — and `debug!` for the detail behind it (the close-on-exec change, the payload write, the listener rebuild, each relayed signal, each arm and disarm). The reason is concrete: `serve` builds its subscriber at a fixed `Verbosity::Info` with no environment filter, so a `debug!`-only rule would leave no visible record at all. And because the launcher's events are emitted in `main` — before the process that serves builds its subscriber — Task 8's macOS arm installs a stderr subscriber for the launch window, which is the "say so in its own task" that rule asks for. **A relayed signal is logged by the relaying thread, not from a signal handler**: that thread is a normal one, and the async-signal-safety boundary is the handler, which stays `signal-hook`'s self-pipe write and nothing of ours.
24. **The pruning harness reports three verdicts, not two.** `remove_rule` returns `Option`, and an id whose rule is not in the rendered profile is reported as `prune: rule-not-present id=…` rather than as a failure: the `DEVICE_RULES` ids are in the source whether or not the sandbox asked for the device files, and a bare panic would be classified as "required" — documenting a rule as load-bearing on the strength of a run that never removed it. The startup probe asks for the device files for the same reason (the flag being the Linux backend's `Options` knob now, not the shared builder's), and `run_pruned` renders the same profile the launch renders (canonicalisation included), so the removed rule really is the only difference.
25. **The child builds the `FsStorage`, the plain serving path builds its own, and the launcher builds none** (Task 8). Those three are one rule read three ways, and the placement is what keeps them from contradicting: constructing an `FsStorage` *opens* the state database, so the launcher — whose child opens the same one in the same directory — must not build one (two handles on one state directory is not a shape the storage layer's exclusive-handle and compaction assumptions are written for), while a `main` that goes on to serve is not a launcher and needs the handle. The launcher therefore resolves the state directory as a `PathBuf`, which is all the exposure set and the profile's `-D` parameters need, and the plain path's construction sits **below** the sandbox arm, which never returns, so the launcher cannot reach it. The `FsStorage::new(root, FsOptions { … })` literal is not restated here: it is the call the body makes today, verbatim, and the two call sites have to be kept in step, because the state directory is a path the exposure set names.
26. **The stop channel is created in the child and never crosses the process boundary** (Task 8). The child is what reacts to a forwarded stop, so `confined` makes the `watch` channel there, hands the sender to the facade's `on_stop` (Task 3's `relay::on_forwarded_stop` behind it) and passes `Some(rx)` to `serve`. A receiver that had to be carried across the `execve` would be a descriptor and a rebuild on arrival — the machinery this shape exists to avoid — and the plane, not the scanner, is the subscriber it reaches (P2 Task 11 states why, and this plan applies it unchanged).
