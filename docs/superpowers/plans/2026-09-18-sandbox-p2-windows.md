# `--sandbox` P2 (Windows) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Confine the tinio server to the paths the operator exposes, on Windows, by launching it inside an AppContainer whose package SID holds an ACE on exactly those paths and nothing else, tied to the launcher by a Job object, relaying the termination signal the container sends the launcher, and cut off from outbound network access.

**Architecture:** P0 left `crates/tinio-sandbox-windows` as an `Unsupported` stub: `pub struct Backend` plus `backend()`, whose `confine` returns `Error::Unsupported`. P2 fills that crate in. Create the AppContainer **profile** whose package SID is derived from the served root — a merely derived SID cannot launch at all, which is the measurement Task 2 is written against — canonicalise the exposure set and refuse a reparse point in it (Task 3), grant the profile's SID `(OI)(CI)RX` or `(OI)(CI)M` on each exposed path through `icacls`, create the two pipes the child's stdin and stdout are wired to, hand the parent's listener (by handle list and argv element) and the two pipe ends to a child created with `CreateProcessW` + `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES` + `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` + `CREATE_NEW_PROCESS_GROUP`, assign that child to a `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` job, arm the termination relay (design §3 step 9) and the stdout event relay (decision 19), write the payload into its stdin, and return `Confinement::Supervise` with a wait closure and a teardown that removes exactly the ACEs it added. The payload frame, the event type, the event stream's decode-and-re-emit (`reemit`), the supervision loop and the exit-status forwarding are `tinio-sandbox-core`'s from P0 and are consumed, not rebuilt; what this crate drives is a thread over the child's stdout.

**Tech Stack:** Rust 2024; `tinio-sandbox-core` for the trait, the vocabulary and the launcher machinery; `sha2` + `hex` for the package name; `icacls.exe` for the DACL edits; the AppContainer profile API in `userenv.dll` (Task 2); hand-declared `unsafe extern "system"` Win32 bindings (no `windows-sys`, no new dependency — design Bookkeeping); `tracing` for the events shared §7 requires, with `tracing-subscriber` in dev-dependencies so a test can assert one; `tempfile` in tests, which spawn their processes with `std::process::Command`. **`assert_cmd` is not among them**: it cannot resolve an example binary, and Task 11's `serve_bin` is what the suites use instead, for the reason that function states.

**Spec:** `docs/superpowers/specs/2026-09-18-sandbox-design.md` — the design document, which is the authority for every decision cited below as `design §N` or `decision N`. Read it alongside this plan.

**Sibling plans:** `2026-09-18-sandbox-p0-core.md` builds `tinio-sandbox-core`, the facade and the three platform-crate stubs — every shared item this plan consumes is defined there and nowhere else; `2026-09-18-sandbox-p1-linux.md` fills the Linux backend and restructures `serve`; `2026-09-18-sandbox-p3-macos.md` fills the other launcher backend. The two launcher plans must agree on the two things the child parses — the payload frame and the one argv element that names the listener. **What all four documents share rather than any one of them building — the conventions, the crate topology and its two shapes, the list of what a launcher platform owns, the names two documents must spell alike, and the three relays — is `2026-09-18-sandbox-shared.md`**, and each plan refers to it by section rather than writing it out again.

## The core surface, consumed in full

**Prerequisite: `2026-09-18-sandbox-p0-core.md`.** That plan builds `tinio-sandbox-core`, the facade and the three platform-crate stubs, and it is where the launcher-shared surface is defined: its Task 1 is the vocabulary and the builder, its Task 2 is the launcher machinery (`LaunchPlan`, `LaunchPayload`, `Adopted`/`adopt`, `Event`/`Observer`/`reemit`, `Child`/`supervise`, the argv-element helpers), and its Task 3 is the facade and the stubs this plan fills in — including the `tinio_sandbox_windows::Backend` stub whose `confine` currently returns `Error::Unsupported`. P0 defines that surface completely; this plan consumes it and adds nothing to it — the two items that were additive while P2 was being written (`Error::Launch` and `LaunchPlan`'s accessors) are now P0's, and their signatures are unchanged from what P2 argued for. If this plan needs something else shared, it goes into P0 rather than into a platform crate.

| Item | Used by |
|---|---|
| `AccessMode::{ReadOnly, ReadWrite}` (with `is_read_only()`; **`tinio-core`'s type**, re-exported by the core), `Exposure { host, access }`, `Sandbox::{new, expose, device_files, exposures, device_files_enabled, from_exposures(exposures, device_files)}` | the exposure set (Task 3), the launch (Task 9), the `serve` call site (Task 11) |
| `Error::{Step, Unsupported, BadPath, Wait, Launch { operation, path, source }}`, `Result<T>` | everywhere; `Launch` is the one every launcher step reports through, and `Wait` is the one a wait closure reaches through `?` |
| `Backend` (the trait), `Confinement::{InPlace, Supervise(Child)}` | `confine`'s return (Task 9) |
| `Child::new(wait_closure)` + `Child::with_relay(JoinHandle<Option<Terminal>>)`, `Child::{wait, terminal}` | the platform keeps its own handle behind the closure, the relay thread goes in with `with_relay` and **returns its answer through the join**, and `wait` joins it — which is what the terminal read afterwards depends on (Task 9) |
| `supervise(&mut child, teardown) -> Result<ExitStatus>` | the call site's wait and exit-status forwarding; **by reference**, so the terminal read can follow it (design §3 step 8, Task 11) |
| `LaunchPayload { config: String }`, serde on both sides and no wrapper methods | the object the launcher serializes into the child's **stdin** and the child deserializes (decision 17; Tasks 7, 9, 11) |
| `Event::{Listening { addr }, Error { message }}` with `encode() -> String` / `decode(&str) -> Option<Self>` | the SSE-framed JSONL protocol on stdout (decision 19; Tasks 9, 11) |
| `Observer::new(writer).observe(reader)` — async; reads chunks, decodes each frame through `sse-core`, writes on only what parses, and **records the terminal event**; `.terminal()` reads it back | the parent's re-emission of the child's events, and the answer to "did it ever become ready?" (decision 19; Task 9 drives it on its own thread, over a current-thread runtime, writing the terminal into the `Child`'s slot) |
| the argv element helpers — `LISTENER_FD_PREFIX` (`"tinio-sandbox-fd="`), `listener_fd_arg(fd) -> OsString`, `is_listener_fd_arg(&OsStr) -> bool`, `listener_fd_from_arg(&OsStr) -> Option<i64>` | naming the inherited **listener** descriptor (decision 17; Tasks 7, 8, 9, 11) |
| `LaunchPlan::{new, with_payload, payload, config, with_listener, listener}`, and `for_launch(config: String) -> Self` | the launcher's own plan (Tasks 9, 11) |
| `Adopted { config, listener_fd: Option<i64> }` and `adopt(args: impl IntoIterator<Item = OsString>, stdin: impl Read) -> Result<Option<Adopted>>` | the child's identity, the configuration text and the listener descriptor (Task 7) |

**What the core does not do, and this phase therefore owns.** The list is `2026-09-18-sandbox-shared.md` §3's — one list with per-platform deltas, carried by both launcher plans, rather than a list each of them writes — so what is here is P2's deltas and the wiring only this launcher can have. The stdout relay is explicitly **not** on that list: P0 builds its decode-and-re-emit as the core's `Observer`, deliberately, because that half is shared by both launcher platforms and is tested where it lives rather than once per platform. What this phase owns over and above shared §3 is the thread that drives it, and the pipe and handle wiring that connects the child to it:

- the listener travels by **`PROC_THREAD_ATTRIBUTE_HANDLE_LIST` and argv**, never on a stream — a socket cannot ride on one, and that is the irreducible half of decision 16.
- the payload write is `serde_json::to_vec(&payload)` into the child's stdin, then the write end closed (shared §3; design §3 step 7).
- **`Options:{program, args}`**, the two test seams a launcher needs and a plan must not carry: a launch that fails *after* the ACLs are on is the only way to exercise §11's failed-launch rollback, and a test binary's argv belongs to libtest.
- the child-side stop hook that gives a forwarded stop somewhere to land (Task 10).

**Program and argv are not carried by `LaunchPlan`** (shared §3): `confine` derives them from `std::env::current_exe()` and `std::env::args_os()`, and `Options` overrides them for tests only.

## Global Constraints

**The conventions the four plans share are stated once, in `2026-09-18-sandbox-shared.md` — *shared* from here on — §1 of which owns test-first, the panic rule, the doc-example rule, imports, the manifest rules, the per-crate lint allocation, English only, attribution, hard-wrapping and git writes. What is left here is this plan's own:

- **`unsafe` only in the modules Task 1 enumerates** — one file-level `#[allow(unsafe_code)]` per module, a `// SAFETY:` comment on every unsafe block, and the module list kept current in **decision 14 of the sandbox design**, which is its only home; `docs/cargo.md` states the rule and points there. The scope is enumerated per capability and the count is established by the implementation, not promised ahead of it.
- **Doc examples:** runnable on the pure items, `no_run` on anything that would launch a process or touch a DACL.
- **This plan's imports backlog is about 125 sites**, mostly in test setup and FFI plumbing, where a bare `std::fs::write` or `std::mem::size_of` reads better than a wall of imports. Correct them in place as you write the code; do not relax the rule (shared §1).
- **The crate topology is shared §2's**, and this crate is the edge that section warns about: `tinio-sandbox-windows` must never depend on the facade.
- The confined child never runs `icacls` and never grants anything: the launcher is the only process that edits a DACL (design decisions 15, 17).
- **Every core operation of this crate is traced, and the split is shared §7's.** The levels — `info!` for what answers "was this process confined, and to what", `debug!` for the per-item detail behind it, `error!` where a returned `Error` is built — the structured fields, and the reason the split exists (a `debug!`-only rule would emit nothing an operator can see, because `serve` builds its subscriber at a fixed `Verbosity::Info` with no environment filter) are stated there once and are not restated here. **What this plan's own is which operation falls on which side:** `info!` for the package name and the derived SID, the profile's creation and deletion, the exposure set the launch actually resolves, `CreateProcessW` with its pid, the Job object and its assignment, and the child's exit; `debug!` for each ACE grant and revoke, each pipe, the payload write, and each armed, disarmed or forwarded stop. The crate installs **no subscriber of its own** — the process that owns the runtime decides where the events go (shared §7) — and because `serve` builds its subscriber *inside* `serve`, the launcher arm installs one before it confines anything: a run that never reaches `serve` would otherwise leave no record at all (Task 11).
- **A `tracing` call is not available to the console control handler.** The handler runs on a thread the system creates and must do nothing that allocates or locks (design §3 step 9), and `tracing` does both — so what carries the relay's events is the arming, the child id addressed, the delivery path and the child's own hook thread, never the handler (Task 10).

## Three decisions this plan records

1. **The DACL is edited through `icacls.exe`, not through security-descriptor FFI.** `/grant:r *<sid>:(OI)(CI)M` replaces this SID's entry and touches no other ACE (decision 21's takeover, decision 15's grant-only), `/remove:g *<sid>` removes exactly those entries, and every ACL row of the design's evidence table was produced with this tool. §11 now states the consequence plainly: where a grant goes through a tool rather than a direct API, what arrives is that tool's exit status and text, and `Error::Launch` carries both forms.
2. **The AppContainer package name is derived from the served root, not a constant, and the profile behind it is created and taken back with the ACEs.** `CreateAppContainerProfile("tinio.sandbox.<digest>", …)` where the digest covers the served root's normalised path — the same SID `DeriveAppContainerSidFromAppContainerName` computes, which is what makes the two interchangeable names for one object — so two servers serving different roots do not share a package SID (a shared SID would let one read the other's granted tree), a crashed run's residue is still identifiable and reusable for that root (decision 21), and the residue stays bounded to one directory per root — the same reasoning as decision 10's jail name. **`CreateAppContainerProfile` rather than the derivation alone is forced rather than preferred**: a merely derived SID cannot launch (Task 2's measurement), so the profile is the object the launch actually needs, it is created without elevation, and `DeleteAppContainerProfile` joins the rollback path so a clean exit leaves neither an ACE nor a profile behind.
3. **The stop relay is a console control event, and the child is created with `CREATE_NEW_PROCESS_GROUP` so it can be addressed.** Windows has no `SIGTERM`; what a container delivers to its main process, and what a launcher can deliver onward, is established by Task 10's experiment before any of it is coded against — and if the experiment shows the child is reached anyway, the relay collapses to a comment rather than shipping as decoration.

## How the integration tests run a confined child

Every launcher-driven test in this plan needs a confined child process, and Windows has no `fork`: the child is a fresh process, so it is a re-execution of the test binary itself, running one role function. Windows gives a test binary no safe way to receive custom environment from its launcher — `std::env::set_var` is `unsafe` in edition 2024, and libtest rejects argv it does not recognise — so every launcher-driven test uses the same two channels instead, both of which are production channels:

- **Identity** comes from the payload: the child role begins with `launch::adopt(...)` and returns immediately when there is none, so the same function body is inert in the launcher process (design decision 17, exercised as written). The frame arrives on the child's **stdin** — the launcher creates that pipe, so a launcher-driven test gets it for free, and no test needs an environment channel to fake a payload.
- **Inputs** come from the payload's `config` bytes, which carry the path of a `probe.txt` the harness wrote **into the report directory — a path the confinement granted read-write**; the child reads that file for the absolute paths it needs. The location is load-bearing rather than tidy: an AppContainer's reads are checked against its package SID and the operator's own ACEs do not help it, so a probe left beside the exposed tree is a file the child cannot open, and the role panics before doing anything a test could assert on. Production puts configuration bytes there; the payload is a byte channel, and the test harness is what reads them differently.

Task 8's `tests/launch.rs` carries its own small copy of the probe writer because it predates the shared harness; Task 9 moves the role and the probe file into `tests/common/mod.rs`, where Tasks 10 and 12 reuse them (`mod common;` plus a one-line `child_role` test in each binary).

---

### Task 1: The crate's FFI surface and its unsafe boundary

**Files:**
- Create: `crates/tinio-sandbox-windows/src/ffi.rs`
- Create: `crates/tinio-sandbox-windows/src/handles.rs`
- Modify: `crates/tinio-sandbox-windows/Cargo.toml`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`
- Modify: `crates/tinio-sandbox/tests/facade.rs` (the stub-era assertion this phase invalidates)
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (decision 14's list of `unsafe` sites)

**Interfaces:**
- Consumes: P0's `tinio-sandbox-core` in full — `Error::Launch { operation, path, source }` (constructed by `handles::step`) and `LaunchPlan`'s accessors among it.
- Produces: `tinio_sandbox_windows::ffi` (every Win32 declaration, constant and `repr(C)` struct); `handles::{Handle, system_directory, last_error, step, mark_inheritable, inherited_stderr, Pipe, pipe, parent_end, close, write_payload}`.

Every Win32 call the crate will ever make is declared in this one module, so a reviewer reads the crate's whole ABI surface in one place, and the unsafe *calls* live only in the modules whose files carry `#[allow(unsafe_code)]`.

- [ ] **Step 1: Write the failing layout tests**

`crates/tinio-sandbox-windows/src/ffi.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::mem::{offset_of, size_of};

    use super::*;

    #[test]
    fn sid_and_attributes_is_sixteen_bytes_on_x64() {
        // Verified: a stride of 8 makes CreateProcessW return
        // ERROR_INVALID_PARAMETER (87); 16 works (design evidence table).
        assert_eq!(size_of::<SidAndAttributes>(), 16);
    }

    #[test]
    fn security_capabilities_matches_winnt_h() {
        // PSID + PSID_AND_ATTRIBUTES + DWORD + DWORD.
        assert_eq!(size_of::<SecurityCapabilities>(), 24);
        assert_eq!(offset_of!(SecurityCapabilities, capability_count), 16);
    }

    #[test]
    fn startup_info_has_the_layout_the_kernel_expects() {
        assert_eq!(size_of::<StartupInfoW>(), 104);
        assert_eq!(size_of::<StartupInfoExW>(), 112);
        assert_eq!(offset_of!(StartupInfoExW, attribute_list), 104);
    }

    #[test]
    fn process_information_and_job_limits_match() {
        assert_eq!(size_of::<ProcessInformation>(), 24);
        assert_eq!(size_of::<JobObjectBasicLimitInformation>(), 64);
        assert_eq!(size_of::<JobObjectExtendedLimitInformation>(), 144);
        assert_eq!(
            offset_of!(JobObjectExtendedLimitInformation, process_memory_limit),
            112
        );
    }

    #[test]
    fn security_attributes_is_twenty_four_bytes_on_x64() {
        assert_eq!(size_of::<SecurityAttributes>(), 24);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows`
Expected: FAIL — there is no `ffi` module yet, so `SidAndAttributes` and the rest are unresolved. The crate is `#![cfg(windows)]` from Step 4, so on a non-Windows host this suite does not exist.

- [ ] **Step 3: Write `ffi.rs`**

```rust
//! The raw Win32 surface, in one module.
//!
//! Every declaration, constant and `repr(C)` struct the crate uses lives here,
//! so the ABI surface is reviewable in one place and the unsafe *calls* are the
//! only thing the FFI modules (`control`, `handles`, `job`, `process`, `sid`,
//! `socket`) contain (design decision 14: the scope is enumerated per
//! capability rather than promised as a count).
//!
//! Layout notes carry the evidence that fixed them: the design's table records
//! that a `SID_AND_ATTRIBUTES` stride of 8 returns `ERROR_INVALID_PARAMETER`
//! (87) from `CreateProcessW`, which is what makes the sizes below load-bearing
//! rather than incidental.

use std::ffi::c_void;

/// `HANDLE` — an opaque kernel object handle.
pub type Handle = *mut c_void;
/// `PSID` — a security identifier.
pub type Psid = *mut c_void;
/// `LPCWSTR` — a NUL-terminated UTF-16 string.
pub type WideStr = *const u16;

/// `INVALID_HANDLE_VALUE`.
pub const INVALID_HANDLE_VALUE: Handle = usize::MAX as Handle;

/// `WAIT_OBJECT_0` — the only return value of `WaitForSingleObject` that means
/// the wait succeeded.
pub const WAIT_OBJECT_0: u32 = 0;
/// `WAIT_OBJECT_0 + 1` — the wait that `SetEvent` releases.
pub const WAIT_OBJECT_1: u32 = 1;
/// `INFINITE`.
pub const INFINITE: u32 = 0xFFFF_FFFF;
/// `WAIT_TIMEOUT`.
pub const WAIT_TIMEOUT: u32 = 0x0000_0102;

/// `EXTENDED_STARTUPINFO_PRESENT` — required for `STARTUPINFOEXW`.
pub const EXTENDED_STARTUPINFO_PRESENT: u32 = 0x0008_0000;
/// `CREATE_UNICODE_ENVIRONMENT`.
pub const CREATE_UNICODE_ENVIRONMENT: u32 = 0x0000_0400;
/// `CREATE_NEW_PROCESS_GROUP` — the child becomes the leader of a new process
/// group, which is the only way a console control event can be addressed to it
/// alone (design §3 step 9).
pub const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
/// `STARTF_USESTDHANDLES` — the child's std handles are the ones we name.
pub const STARTF_USESTDHANDLES: u32 = 0x0000_0100;

/// `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES` — the attribute that makes the
/// child an AppContainer process.
pub const PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES: usize = 0x0002_0009;
/// `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` — the attribute that restricts
/// inheritance to exactly the handles we name.
pub const PROC_THREAD_ATTRIBUTE_HANDLE_LIST: usize = 0x0002_0002;

/// `HANDLE_FLAG_INHERIT` — the flag `SetHandleInformation` sets or clears.
pub const HANDLE_FLAG_INHERIT: u32 = 0x0000_0001;

/// `FILE_ATTRIBUTE_REPARSE_POINT` — a junction, a mount point or a symlink. An
/// exposed path carrying it is refused rather than followed, because the grant
/// would land on the link's target (Task 3, design §6).
pub const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;

/// `STD_INPUT_HANDLE`.
pub const STD_INPUT_HANDLE: u32 = -10i32 as u32;
/// `STD_OUTPUT_HANDLE`.
pub const STD_OUTPUT_HANDLE: u32 = -11i32 as u32;
/// `STD_ERROR_HANDLE`.
pub const STD_ERROR_HANDLE: u32 = -12i32 as u32;

/// `CTRL_C_EVENT`.
pub const CTRL_C_EVENT: u32 = 0;
/// `CTRL_BREAK_EVENT` — the only console control event that can be addressed
/// to a process group, which is what makes the relay possible.
pub const CTRL_BREAK_EVENT: u32 = 1;
/// `CTRL_CLOSE_EVENT` — the console window is closing.
pub const CTRL_CLOSE_EVENT: u32 = 2;
/// `CTRL_LOGOFF_EVENT`.
pub const CTRL_LOGOFF_EVENT: u32 = 5;
/// `CTRL_SHUTDOWN_EVENT`.
pub const CTRL_SHUTDOWN_EVENT: u32 = 6;

/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` — killing the launcher takes the
/// confined child with it (design decision 20).
pub const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;
/// `JobObjectExtendedLimitInformation`.
pub const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: i32 = 9;

/// `ERROR_INSUFFICIENT_BUFFER` — what the attribute-list size query returns.
pub const ERROR_INSUFFICIENT_BUFFER: i32 = 122;

/// `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` — what
/// `CreateAppContainerProfile` returns for a profile that is already there, with
/// the SID still filled in. A residue from a crashed run is success here rather
/// than an error (design decision 21).
pub const HRESULT_ALREADY_EXISTS: i32 = 0x8007_00B7u32 as i32;

/// `SECURITY_ATTRIBUTES`.
#[repr(C)]
pub struct SecurityAttributes {
    /// `sizeof(SECURITY_ATTRIBUTES)`.
    pub length: u32,
    /// Always null here: no security descriptor is attached to our objects.
    pub descriptor: *mut c_void,
    /// Non-zero when the handle is to be inherited.
    pub inherit: i32,
}

/// `SID_AND_ATTRIBUTES` — 16 bytes on x64, not 8.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SidAndAttributes {
    /// The capability SID.
    pub sid: Psid,
    /// `SE_GROUP_ENABLED` (0) is the value the probe used and the only one a
    /// capability entry needs.
    pub attributes: u32,
}

/// `SECURITY_CAPABILITIES` — the value behind
/// `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES`. Setting this attribute is what
/// creates the child as an AppContainer process.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SecurityCapabilities {
    /// The package SID, derived rather than generated (design decision 21).
    pub app_container_sid: Psid,
    /// A contiguous array of `capability_count` `SID_AND_ATTRIBUTES`.
    pub capabilities: *const SidAndAttributes,
    /// How many capabilities; zero by default (design §7).
    pub capability_count: u32,
    /// Reserved; must be zero.
    pub reserved: u32,
}

/// `STARTUPINFOW` — 104 bytes on x64.
#[repr(C)]
pub struct StartupInfoW {
    /// `sizeof(STARTUPINFOEXW)` on the `STARTUPINFOEXW` path.
    pub cb: u32,
    /// Reserved.
    pub lp_reserved: *mut u16,
    /// The desktop, or null.
    pub lp_desktop: *mut u16,
    /// The title, or null.
    pub lp_title: *mut u16,
    /// Window position and size fields, all zero here.
    pub dw_x: u32,
    /// See `dw_x`.
    pub dw_y: u32,
    /// See `dw_x`.
    pub dw_x_size: u32,
    /// See `dw_x`.
    pub dw_y_size: u32,
    /// See `dw_x`.
    pub dw_x_count_chars: u32,
    /// See `dw_x`.
    pub dw_y_count_chars: u32,
    /// See `dw_x`.
    pub dw_fill_attribute: u32,
    /// `STARTF_USESTDHANDLES` when the standard handles are supplied.
    pub dw_flags: u32,
    /// Zero.
    pub w_show_window: u16,
    /// Zero.
    pub cb_reserved2: u16,
    /// Null.
    pub lp_reserved2: *mut u8,
    /// The handle the child reads as standard input.
    pub h_std_input: Handle,
    /// The handle the child writes as standard output.
    pub h_std_output: Handle,
    /// The handle the child writes as standard error.
    pub h_std_error: Handle,
}

/// `STARTUPINFOEXW` — the `EXTENDED_STARTUPINFO_PRESENT` form.
#[repr(C)]
pub struct StartupInfoExW {
    /// The plain `STARTUPINFOW`, `cb` set to the whole structure's size.
    pub startup_info: StartupInfoW,
    /// The attribute list from `InitializeProcThreadAttributeList`.
    pub attribute_list: *mut c_void,
}

/// `PROCESS_INFORMATION`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ProcessInformation {
    /// The new process; the caller closes it.
    pub process: Handle,
    /// The new thread; the caller closes it.
    pub thread: Handle,
    /// The new process id. Also the child's process-group id, because the child
    /// is created with `CREATE_NEW_PROCESS_GROUP`.
    pub process_id: u32,
    /// The new thread id.
    pub thread_id: u32,
}

/// `JOBOBJECT_BASIC_LIMIT_INFORMATION` — 64 bytes on x64.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct JobObjectBasicLimitInformation {
    /// `LARGE_INTEGER`.
    pub per_process_user_time_limit: i64,
    /// `LARGE_INTEGER`.
    pub per_job_user_time_limit: i64,
    /// The `JOB_OBJECT_LIMIT_*` bits; only one is set here.
    pub limit_flags: u32,
    /// Unused.
    pub minimum_working_set_size: usize,
    /// Unused.
    pub maximum_working_set_size: usize,
    /// Unused.
    pub active_process_limit: u32,
    /// Unused.
    pub affinity: usize,
    /// Unused.
    pub priority_class: u32,
    /// Unused.
    pub scheduling_class: u32,
}

/// `IO_COUNTERS`.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct IoCounters {
    /// Unused.
    pub read_operation_count: u64,
    /// Unused.
    pub write_operation_count: u64,
    /// Unused.
    pub other_operation_count: u64,
    /// Unused.
    pub read_transfer_count: u64,
    /// Unused.
    pub write_transfer_count: u64,
    /// Unused.
    pub other_transfer_count: u64,
}

/// `JOBOBJECT_EXTENDED_LIMIT_INFORMATION` — what `SetInformationJobObject`
/// takes for `KILL_ON_JOB_CLOSE`.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct JobObjectExtendedLimitInformation {
    /// The basic limits, where the kill-on-close bit lives.
    pub basic_limit_information: JobObjectBasicLimitInformation,
    /// Unused.
    pub io_info: IoCounters,
    /// Unused.
    pub process_memory_limit: usize,
    /// Unused.
    pub job_memory_limit: usize,
    /// Unused.
    pub peak_process_memory_used: usize,
    /// Unused.
    pub peak_job_memory_used: usize,
}

unsafe extern "system" {
    /// `GetLastError`.
    pub fn GetLastError() -> u32;
    /// `CloseHandle`.
    pub fn CloseHandle(handle: Handle) -> i32;
    /// `SetHandleInformation`.
    pub fn SetHandleInformation(handle: Handle, mask: u32, flags: u32) -> i32;
    /// `GetStdHandle`.
    pub fn GetStdHandle(std: u32) -> Handle;
    /// `CreatePipe`.
    pub fn CreatePipe(
        read: *mut Handle,
        write: *mut Handle,
        attributes: *const SecurityAttributes,
        size: u32,
    ) -> i32;
    /// `FreeSid`.
    pub fn FreeSid(sid: Psid) -> Psid;
    /// `LocalFree`.
    pub fn LocalFree(memory: *mut c_void) -> *mut c_void;
    /// `DeriveAppContainerSidFromAppContainerName`. Returns `S_OK` (0) or a
    /// failure `HRESULT`; the returned SID is freed with [`FreeSid`].
    pub fn DeriveAppContainerSidFromAppContainerName(name: WideStr, sid: *mut Psid) -> i32;
    /// `CreateAppContainerProfile` — the profile *behind* the derived SID, and
    /// the call that makes a launch possible at all. Needs no elevation, is
    /// idempotent (`HRESULT_ALREADY_EXISTS` still fills `sid` in), and returns
    /// the same SID the derivation does; the SID is freed with [`FreeSid`].
    pub fn CreateAppContainerProfile(
        name: WideStr,
        display_name: WideStr,
        description: WideStr,
        capabilities: *const SidAndAttributes,
        capability_count: u32,
        sid: *mut Psid,
    ) -> i32;
    /// `DeleteAppContainerProfile` — removes the profile, so a clean exit leaves
    /// neither an ACE nor a profile behind (design decisions 15, 21).
    pub fn DeleteAppContainerProfile(name: WideStr) -> i32;
    /// `ConvertSidToStringSidW`. The returned string is freed with
    /// [`LocalFree`].
    pub fn ConvertSidToStringSidW(sid: Psid, string: *mut *mut u16) -> i32;
    /// `ConvertStringSidToSidW`. The returned SID is freed with [`LocalFree`].
    pub fn ConvertStringSidToSidW(string: WideStr, sid: *mut Psid) -> i32;
    /// `CommandLineToArgvW`, used by the command-line quoting test as the
    /// independent oracle for the quoting rules.
    pub fn CommandLineToArgvW(command_line: WideStr, argc: *mut i32) -> *mut *mut u16;
    /// `CreateJobObjectW`.
    pub fn CreateJobObjectW(attributes: *const SecurityAttributes, name: WideStr) -> Handle;
    /// `SetInformationJobObject`.
    pub fn SetInformationJobObject(job: Handle, class: i32, info: *const c_void, len: u32) -> i32;
    /// `AssignProcessToJobObject`.
    pub fn AssignProcessToJobObject(job: Handle, process: Handle) -> i32;
    /// `TerminateJobObject`.
    pub fn TerminateJobObject(job: Handle, code: u32) -> i32;
    /// `TerminateProcess` — how a child that never reached the job is ended
    /// (Task 9's two job failures).
    pub fn TerminateProcess(process: Handle, code: u32) -> i32;
    /// `InitializeProcThreadAttributeList`.
    pub fn InitializeProcThreadAttributeList(
        list: *mut c_void,
        count: u32,
        flags: u32,
        size: *mut usize,
    ) -> i32;
    /// `UpdateProcThreadAttribute`.
    pub fn UpdateProcThreadAttribute(
        list: *mut c_void,
        flags: u32,
        attribute: usize,
        value: *mut c_void,
        size: usize,
        previous: *mut c_void,
        returned: *mut usize,
    ) -> i32;
    /// `DeleteProcThreadAttributeList`.
    pub fn DeleteProcThreadAttributeList(list: *mut c_void);
    /// `CreateProcessW`.
    pub fn CreateProcessW(
        application: WideStr,
        command_line: *mut u16,
        process_attributes: *const SecurityAttributes,
        thread_attributes: *const SecurityAttributes,
        inherit_handles: i32,
        flags: u32,
        environment: *mut c_void,
        current_directory: WideStr,
        startup_info: *const StartupInfoExW,
        process_information: *mut ProcessInformation,
    ) -> i32;
    /// `WaitForSingleObject`.
    pub fn WaitForSingleObject(handle: Handle, milliseconds: u32) -> u32;
    /// `GetExitCodeProcess`.
    pub fn GetExitCodeProcess(process: Handle, code: *mut u32) -> i32;
    /// `GetSystemDirectoryW` — where the loader says the system directory is,
    /// which is what keeps `icacls.exe`'s location out of the environment.
    /// Returns the length copied, or the length required when the buffer is too
    /// small ([`crate::handles::system_directory`]).
    pub fn GetSystemDirectoryW(buffer: *mut u16, len: u32) -> u32;
    /// `SetConsoleCtrlHandler`. The handler runs on a thread the system
    /// creates, and must do nothing but the one operation it exists for.
    pub fn SetConsoleCtrlHandler(handler: Option<CtrlHandler>, add: i32) -> i32;
    /// `GenerateConsoleCtrlEvent`.
    pub fn GenerateConsoleCtrlEvent(event: u32, group: u32) -> i32;
    /// `CreateEventW`. A manual-reset event the console handler can signal,
    /// which is the only work the handler does.
    pub fn CreateEventW(
        attributes: *const SecurityAttributes,
        manual_reset: i32,
        initial_state: i32,
        name: WideStr,
    ) -> Handle;
    /// `SetEvent`.
    pub fn SetEvent(event: Handle) -> i32;
}

/// The console control handler's shape: it receives the event and returns
/// non-zero when it handled it.
pub type CtrlHandler = unsafe extern "system" fn(event: u32) -> i32;
```

- [ ] **Step 4: Make the crate Windows-only and rewrite `lib.rs`**

P0 leaves `crates/tinio-sandbox-windows/src/lib.rs` as the stub with `pub struct Backend` and `backend()`. The crate-level `cfg` is what lets every module below use `std::os::windows` without a per-module guard, and it leaves the crate empty on Linux and macOS, where the facade never names it.

**This is also where P0's stub-era facade assertion stops being true, and this phase is the one that says so.** `crates/tinio-sandbox/tests/facade.rs` carries `the_launcher_platforms_answer_unimplemented_for_now`, which asserts `err.to_string().contains("not implemented")` under `#[cfg(any(target_os = "macos", target_os = "windows"))]` — true while both backends are stubs, false on Windows the moment this crate answers `Confinement::Supervise` instead. Narrow that test's `#[cfg]` to `target_os = "macos"` (P3 removes it entirely when its own backend lands, as P0's note beside the test says), and keep the rest of the file untouched: it binds the facade's whole re-export list by signature, and nothing this phase adds to the facade may change a pinned signature.

```rust
//! Windows confinement for tinio: an AppContainer whose package SID holds an
//! ACE on exactly the exposed paths, launched by a supervising parent.
//!
//! [`Backend`] implements [`tinio_sandbox_core::Backend`]. It can only confine a
//! process it creates — AppContainer membership is fixed at process creation —
//! so it binds nothing itself, grants the ACLs, launches a confined child and
//! supervises it through [`Confinement::Supervise`] (design decisions 2, 6).
//!
//! The confinement is three mechanisms, not one: the DACL whitelist decides
//! which paths exist for the confined process (design §6), the AppContainer's
//! capability set decides what it can reach over the network (design §7), and
//! the relay in [`control`] forwards the stop the launcher receives, because a
//! Windows container signals its main process and nothing else (design §3 step
//! 9). The parent hands the child a listener it never created, which is a
//! convenience for the port, **not** a network-isolation mechanism (design
//! decision 16).
//!
//! Three consequences are stated rather than hidden: the launcher writes ACEs
//! onto the operator's directories and a hard kill between grant and rollback
//! leaves them (design §6); the confined process can read its own install
//! directory, which is exposed read-only because it cannot load without it
//! (design decision 22); and the relay delivers a stop the child must still
//! know what to do with — the drain that follows is the CLI contract's
//! (T066/T067), and `launch::on_stop` is the hook it lands on.

#![cfg(windows)]

pub mod acl;
pub mod backend;
pub mod control;
pub mod ffi;
pub mod handles;
pub mod job;
pub mod launch;
pub mod process;
pub mod sid;
pub mod socket;

pub use self::{
    backend::{Backend, Capability, Options},
    launch::{adopt, listener},
};

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_windows::backend();
/// let _ = backend;
/// ```
pub fn backend() -> Backend {
    Backend::default()
}

/// Confine through this platform's backend, with its defaults.
///
/// The trait-shaped entry for a caller that is content with the backend's
/// defaults, so the call site does not name the `Backend` type (design decision 2).
///
/// # Errors
///
/// Whatever [`Backend::confine`](tinio_sandbox_core::Backend::confine) reports.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::{LaunchPlan, Sandbox};
///
/// let confined = tinio_sandbox_windows::confine(&Sandbox::new(), &LaunchPlan::new());
/// assert!(confined.is_err(), "nothing was exposed");
/// ```
pub fn confine(
    sandbox: &tinio_sandbox_core::Sandbox,
    plan: &tinio_sandbox_core::LaunchPlan,
) -> tinio_sandbox_core::Result<tinio_sandbox_core::Confinement> {
    use tinio_sandbox_core::Backend as BackendTrait;

    backend().confine(sandbox, plan)
}
```

- [ ] **Step 5: Write `handles.rs`**

```rust
//! The raw handle plumbing: which handles a child inherits, and how the
//! parent's own handles are made inheritable at all.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design
//! decision 14): it exists so that every other module can name a handle, make it
//! inheritable, create a pipe, report a Win32 error and take the system
//! directory from the kernel without an unsafe call of its own. Every unsafe
//! block below carries its safety argument.

#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::windows::io::FromRawHandle;
use std::path::PathBuf;

use tinio_sandbox_core::{Error, Result};

use crate::ffi;

/// The handle type the crate's public signatures name.
///
/// **Re-exported rather than imported, and that is not a style choice.**
/// [`crate::job::Job::assign`], [`crate::process::LaunchSpec`] and
/// [`crate::socket::handle`] are `pub`, so a private `use crate::ffi::Handle;`
/// here would leave the type those signatures name unreachable from outside the
/// crate — the private-in-public rule the compiler reports as `E0446` — and the
/// integration tests, which are a separate crate, could not name it at all.
pub use crate::ffi::Handle;

/// The Win32 error the last failing call set.
///
/// This is the crate's only `GetLastError` call site, kept here so that the
/// other modules report a Win32 error without their own unsafe call.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::handles;
///
/// let err = handles::last_error();
/// assert!(err.raw_os_error().is_some());
/// ```
pub fn last_error() -> io::Error {
    // SAFETY: GetLastError reads the calling thread's last-error value; it takes
    // no arguments and has no preconditions.
    unsafe { io::Error::from_raw_os_error(ffi::GetLastError() as i32) }
}

/// The system directory, as the kernel reports it rather than as an environment
/// variable spells it.
///
/// `GetSystemDirectoryW` answers with the directory the loader itself searches —
/// `…\System32` under the installation wherever it is — so a tool the crate runs
/// is not selectable through the environment. **That is robustness rather than
/// defence**: an actor who can set this launcher's environment is a local actor,
/// and local actors are outside this feature's threat model, so what the rule
/// buys is that the path cannot be wrong for an environment reason. The one
/// caller is [`crate::acl::icacls_binary`].
///
/// **Infallible on purpose.** A system directory is far shorter than the buffer
/// below, so the call cannot fail here, and every caller wants a path rather than
/// an error it would have to invent a fallback for — the documented default
/// location answers in the branch nothing reaches, which is why the return type
/// is not a `Result`.
///
/// # Examples
///
/// ```no_run
/// let system = tinio_sandbox_windows::handles::system_directory();
/// assert!(system.is_absolute(), "{}", system.display());
/// ```
pub fn system_directory() -> PathBuf {
    // `MAX_PATH` is 260 and a system directory is a fraction of it, so this
    // buffer cannot be too small; the call reports the length it needs instead of
    // overrunning if that ever stopped being true.
    let mut buffer = [0u16; 260];
    // SAFETY: `buffer` is a valid, writable array of `buffer.len()` UTF-16 units
    // and stays alive for the call; the call writes at most that many and touches
    // nothing else.
    let len = unsafe { ffi::GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) };
    if len == 0 || len as usize >= buffer.len() {
        return PathBuf::from(r"C:\Windows\System32");
    }
    // SAFETY: the call above copied `len` UTF-16 units into `buffer`, which is
    // alive and unmodified here, and `len` is the count it reported — bounded by
    // the buffer's length by the branch above.
    //
    // Decoded here rather than through `crate::from_wide`, which lands with
    // `lib.rs` in Task 2: this module is written before it, and the length the
    // call already returned is all the conversion needs.
    let text = unsafe { std::slice::from_raw_parts(buffer.as_ptr(), len as usize) };
    PathBuf::from(String::from_utf16_lossy(text))
}

/// A launcher-step failure about `path`, logged as it is built.
///
/// **The `error!` is emitted here rather than at each call site, and that is
/// shared §7's rule made structural**: the log's `step` and the `Error::Launch`
/// it accompanies carry the same string because one function produces both, so
/// they cannot drift apart as call sites are added. It is also this crate's only
/// failure-logging site, which is why `ffi.rs`, `job.rs`, `process.rs` and the
/// rest report a Win32 error without a `tracing` call of their own.
pub fn step(operation: &'static str, path: impl Into<PathBuf>, source: io::Error) -> Error {
    let path = path.into();
    tracing::error!(step = operation, path = %path.display(), err = %source);
    Error::Launch {
        operation,
        path,
        source,
    }
}

/// Make `handle` inheritable, so a `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` may name
/// it.
///
/// Windows 8 and later create sockets non-inheritable, so this call is required
/// rather than defensive (design evidence table).
///
/// # Errors
///
/// The Win32 error when `SetHandleInformation` refuses.
pub fn mark_inheritable(handle: Handle) -> io::Result<()> {
    // SAFETY: `handle` is a live handle the caller owns for the duration of the
    // call, and SetHandleInformation with HANDLE_FLAG_INHERIT only sets the
    // inherit bit on that handle.
    let ok = unsafe {
        ffi::SetHandleInformation(handle, ffi::HANDLE_FLAG_INHERIT, ffi::HANDLE_FLAG_INHERIT)
    };
    if ok == 0 {
        return Err(last_error());
    }
    Ok(())
}

/// The parent's standard error, marked inheritable.
///
/// The confined child inherits it untouched and unmediated, which is where every
/// log goes (decision 19). An inherited handle needs no DACL check on use, which
/// is why the child can write to the operator's console or to a redirect the
/// shell set up.
///
/// The child's **stdin and stdout** are the launcher's pipes, not the launcher's
/// own handles: stdin carries the payload, stdout carries the event stream the
/// parent relays. Taking them from this process would mean writing into the
/// operator's console and reading whatever it types.
///
/// The inherit bit is left set on the launcher's own handle: it is a
/// process-wide flag and putting it back would be tidiness nothing depends on —
/// the launcher lives exactly as long as the child it supervises.
///
/// # Errors
///
/// [`Error::Launch`] when there is no standard error at all: a process with
/// nowhere to log cannot host a confined server, and the remedy belongs in the
/// message rather than in a child that dies on its first log write.
pub fn inherited_stderr() -> Result<Handle> {
    // SAFETY: GetStdHandle takes an index and returns the process's handle for
    // it; it has no other precondition.
    let handle = unsafe { ffi::GetStdHandle(ffi::STD_ERROR_HANDLE) };
    if handle.is_null() || handle == ffi::INVALID_HANDLE_VALUE {
        return Err(step(
            "read the standard handles",
            PathBuf::from("stderr"),
            io::Error::other(
                "no standard error: run the server from a console, or redirect its output to a file",
            ),
        ));
    }
    mark_inheritable(handle)
        .map_err(|source| step("mark a standard handle inheritable", "stderr", source))?;
    Ok(handle)
}

/// An anonymous pipe between the launcher and the child.
///
/// The launcher creates two of these and each one carries a std stream of the
/// child: for the child's **stdin** the child takes `read` and the parent keeps
/// `write`, and for the child's **stdout** the child takes `write` and the parent
/// keeps `read` — the payload flows one way and the events the other.
///
/// Both ends are created inheritable, and the handle list is what decides which
/// one the child actually gets: clearing the other's inherit bit would be a
/// second mechanism for a guarantee `CreateProcessW` already applies.
pub struct Pipe {
    /// The end the child reads: the payload pipe's child side (its stdin).
    pub read: Handle,
    /// The end the child writes: the event pipe's child side.
    pub write: Handle,
}

/// Create a pipe with both ends inheritable.
///
/// # Errors
///
/// The Win32 error from `CreatePipe`.
pub fn pipe() -> io::Result<Pipe> {
    let attributes = ffi::SecurityAttributes {
        length: std::mem::size_of::<ffi::SecurityAttributes>() as u32,
        descriptor: std::ptr::null_mut(),
        inherit: 1,
    };
    let mut read: Handle = std::ptr::null_mut();
    let mut write: Handle = std::ptr::null_mut();
    // SAFETY: both out-parameters are valid, aligned and writable for the call,
    // and `attributes` is a correctly sized SECURITY_ATTRIBUTES that lives until
    // the call returns.
    let ok = unsafe { ffi::CreatePipe(&mut read, &mut write, &attributes, 0) };
    if ok == 0 {
        return Err(last_error());
    }
    // The caller names which std stream this pipe carries when it maps the
    // failure above (`step("CreatePipe", "stdin"/"stdout", …)`); the event has
    // the two handle values, which is what identifies a pipe in a log.
    tracing::debug!(step = "CreatePipe", read = ?read, write = ?write);
    Ok(Pipe { read, write })
}

/// The launcher's end of a pipe it just created, as a [`File`].
///
/// The returned `File` closes the handle when it drops, which is what the
/// launcher wants at both ends of the child's std streams: it writes the payload
/// and lets go, and it reads the event stream until the child's stdout closes.
///
/// Keeping this safe is deliberate: the contract — one `File` per handle, on an
/// end `Self::pipe` just returned — is proven by the call site, so the crate's
/// other modules never carry an `unsafe` block of their own.
///
/// **The other end of the same pipe is the caller's to close, and forgetting it
/// is how a launcher hangs.** `CreateProcessW` duplicates the child's ends into
/// the child's handle table, so the launcher's copies are dead weight the moment
/// it returns — and one of them is not merely dead: with `event_pipe.write` still
/// open, the child's stdout never reaches end-of-stream, the relay's read never
/// returns, and `Child::wait`'s join with it never finishes. [`close`] is the
/// call, and the launch site makes it immediately after `CreateProcessW`.
pub fn parent_end(handle: Handle) -> File {
    // SAFETY: `handle` is one end of a pipe this process created and handed
    // over; no other `File` owns it, and nothing else closes it.
    unsafe { File::from_raw_handle(handle.cast()) }
}

/// Close a handle the launcher no longer needs.
///
/// Used for the child's own ends of the two pipes once `CreateProcessW` has
/// duplicated them into the child: see [`parent_end`] for why leaving the event
/// pipe's write end open is a hang rather than a leak.
pub fn close(handle: Handle) {
    // SAFETY: `handle` is a live handle this process owns and is giving up; it
    // is closed exactly once, here, and nothing else holds a copy of it.
    unsafe { ffi::CloseHandle(handle) };
}

/// Write the launch frame into the child's stdin and close it (design §3 step 7).
///
/// The close is what makes the child's read terminate: dropping the `File` closes
/// this process's copy of the write end, so the child's read of stdin reaches EOF
/// after the frame.
///
/// **This is a blocking write, and what bounds it is the child rather than the
/// size of the frame.** The frame is the launcher's **whole configuration text**
/// (`LaunchPayload { config }`, decision 17), and no document bounds how large
/// that is; a Win32 anonymous pipe created without a size buffers 4 KiB
/// (`CreatePipe`'s default, and [`pipe`] passes 0), so a frame larger than that
/// returns only as the child drains it. The child does read: its `adopt` reads
/// the frame before it does anything else, and the launch site writes *after*
/// `CreateProcessW`, so the two are a producer and a consumer that meet — the
/// write completes as long as the child runs and reads. What can stall is a child
/// that neither reads nor exits, and it stalls here, before the launcher has
/// reported anything: the 4 KiB figure is the point at which the write stops
/// being a memcpy into the pipe buffer, not a bound on what may be sent. No
/// caller imposes a size limit today, and a caller that wants one has to add it
/// either here or at the plan that builds the configuration text.
///
/// # Errors
///
/// [`Error::Launch`] when the write or the flush fails — including a child that
/// died before reading, which a pipe write reports as a broken pipe.
pub fn write_payload(writer: Handle, bytes: &[u8]) -> Result<()> {
    use std::io::Write;

    let mut file = parent_end(writer);
    file.write_all(bytes)
        .map_err(|source| step("write the launch payload", "stdin", source))?;
    file.flush()
        .map_err(|source| step("flush the launch payload", "stdin", source))?;
    tracing::debug!(step = "write the launch payload", bytes = bytes.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    use tracing::subscriber::with_default;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// A subscriber whose writer keeps what it is given.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("the capture mutex").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Shared §7's "the log's `step` and the error's `step` are the same string",
    /// made a property a test can lose: `step` both builds the `Error` and emits
    /// the event, so removing one of the two breaks this test and nothing else.
    ///
    /// It is deliberately one test and not one per operation: the other events
    /// this crate emits are read where they land (shared §7), while this one pins
    /// the rule that the failure path cannot drift from the value the caller sees.
    ///
    /// The level is the one `serve`'s subscriber enables (`Verbosity::Info`) and
    /// not `TRACE`, so a failure that was moved to `debug!` would stop being
    /// recorded and this test would lose the property rather than keep it.
    #[test]
    fn a_step_failure_logs_the_step_it_reports() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(capture.clone())
            .with_max_level(tracing::Level::INFO)
            .finish();

        let error = with_default(subscriber, || {
            step("CreateProcessW", "program", io::Error::other("boom"))
        });

        let Error::Launch { operation, .. } = &error else {
            panic!("step builds Error::Launch: {error:?}");
        };
        assert_eq!(*operation, "CreateProcessW");
        let logged = String::from_utf8(capture.0.lock().expect("the capture mutex").clone())
            .expect("the capture is UTF-8");
        assert!(logged.contains("CreateProcessW"), "{logged}");
        assert!(logged.contains("program"), "{logged}");
    }
}
```

- [ ] **Step 6: Write the manifest and the lint exception**

`crates/tinio-sandbox-windows/Cargo.toml` — P0's stub becomes the real manifest:

```toml
[package]
name = "tinio-sandbox-windows"
version.workspace = true
edition.workspace = true
description = "Windows AppContainer confinement for tinio: DACL whitelist, container launch, signal relay"
publish = false

[dependencies]
hex.workspace = true
serde_json.workspace = true
sha2.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["fs", "io-std", "io-util", "rt"] }
tracing.workspace = true

tinio-sandbox-core.workspace = true

[dev-dependencies]
tempfile.workspace = true
tracing-subscriber.workspace = true

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "deny"
```

**`serde_json` and `tokio`, and each is there for a named reason.** `serde_json` serializes the payload at the call site — the core has no `encode` method, because the payload is one `serde` object (decision 17). `tokio`'s `io-util` is what the core's async `reemit` is written against, `io-std` is what makes `tokio::io::stdout()` an `AsyncWrite` at all (`Stdout` and `Stderr` need that feature; neither has a `futures::io` bridge), and `rt` is what lets the relay thread build a **current-thread** runtime to poll it: this launcher is built on blocking Win32 calls and has no runtime of its own, so the relay gets the smallest one that works rather than a multi-threaded runtime nothing else would use. **`fs` is the fourth and it is the least obvious**: the core's `Observer` wants an `AsyncBufRead`, and on Windows a child's stdout is a `CreatePipe` handle with no async counterpart in tokio — `tokio::fs::File::from_std` is what reads one, and it is the same class of implementation tokio itself uses for `ChildStdout` on this platform (a blocking read on the blocking pool, not an IOCP registration). P0 leaves that choice to the platform crate by design ("the child's stdout *is* an `AsyncBufRead`"), so this crate names the feature that makes it true. All four are unconditional here rather than macOS-style target-gated, because this crate only ever compiles on Windows. **`tracing` is the fifth line and `tracing-subscriber` is the dev-dependency**, for the reason shared §7 gives: every core operation this crate performs emits an event, the crate installs no subscriber of its own, and the one subscriber it ever builds is `handles`' test recorder. `tracing` is already in `[workspace.dependencies]`, so the workspace gains no new third-party dependency from either line.

**`docs/cargo.md` is not edited here, and this phase must not edit it.** The lint bullet is written once, by P1 Task 9 Step 4, and `2026-09-18-sandbox-shared.md` §1 records why the line lives there and what a second copy of it costs. This plan used to carry its own version of the same bullet, worded differently and enumerating this crate's FFI modules; it was removed for that reason, and an enumeration written here would have had to list modules this phase had not yet designed.

What this phase does instead is **verify the line before it starts**: `docs/cargo.md` must already carry the rule for the platform backends — each becomes `deny` as its backend lands — with the sites deferred to decision 14. If it does not, P1 has not landed and this phase stops rather than restating the rule. Where this crate's own sites are recorded is the design's decision 14 and this plan's Task 1 — not `docs/cargo.md`, which states the rule and points there.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows` and `cargo test -p tinio-sandbox-core`
Expected: PASS — the layout tests and the core's error test. The crate's other modules do not exist yet, so trim `lib.rs`'s `pub mod` list to what exists and add each module back in the task that creates it.

- [ ] **Step 8: Verify the boundary is what the docs say**

Run: `rg -c 'allow\(unsafe_code\)' crates/tinio-sandbox-windows/src` and `cargo clippy -p tinio-sandbox-windows --all-targets -- -D warnings`
Expected: clippy clean; the grep prints the number of files carrying an allow — at this point `handles.rs`, plus `ffi.rs` if the mandatory `unsafe extern` keyword trips the lint in edition 2024. `lib.rs` joins them in Task 2, and it does so **without a file-level attribute**: `from_wide` is the crate root's single site and carries `#[allow(unsafe_code)]` on the function, which is the smallest scope the constitution asks for and the reason the crate's `deny` is not relaxed to cover the whole root. Keep decision 14's list of sites equal to what exists, and let it grow as the later tasks add modules: decision 14 asks for an enumerated scope, not a number fixed in advance.

---

### Task 2: The package SID and the capability SIDs

**Files:**
- Create: `crates/tinio-sandbox-windows/src/sid.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (§6's derivation sentence and decision 21: the profile is created, reused and deleted with the SID)

**Interfaces:**
- Consumes: `handles::{last_error, step}`, `ffi`.
- Produces: `sid::package_name(root: &Path) -> String`, `sid::PackageSid` with `derive(name)`, `as_str()`, `raw()` and `Drop` — the `Drop` removes the profile as well as freeing the SID; `sid::CapabilitySids` with `resolve(&[&str])`, `as_ptr()`, `len()`, `is_empty()` and `Drop`. **Nothing about the launch payload belongs here**: the payload is one JSON object owned by the core, and this module's job is the SID the confinement is built around.

The SID is derived, never generated, and the name it is derived from is a function of the served root (decision 2 of this plan): a crash leaves *identifiable, reusable* ACEs (design decision 21), and two servers on different roots cannot read each other's tree through a shared package SID.

**The derived SID is not enough on its own, and this is measured rather than assumed.** A SID that only `DeriveAppContainerSidFromAppContainerName` produced cannot launch anything: handing it to `PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES` makes `CreateProcessW` fail with `err=2 ERROR_FILE_NOT_FOUND` — for this crate's own binary and for `cmd.exe` alike, and that failure is unchanged by granting the SID an ACE on the directory holding the program. Calling `CreateAppContainerProfile` first makes the identical call succeed, needs no elevation, and returns the *same* SID the derivation does; the profile is the object the kernel checks for, and the derivation is the way to compute its name. **So the profile joins the residue story rather than sitting beside it.** It is a second artifact this run creates and must take back, and it follows exactly the rules the ACEs do: it is named from the served root, so a hard kill's profile is the next run's to take over (`CreateAppContainerProfile` reports `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` and still fills the SID in, which this module treats as success), and a clean exit removes it with `DeleteAppContainerProfile` on the same path that removes the ACEs (design decisions 15 and 21 — the ACE half is Task 4's and Task 9's, this half is the `Drop` below).

- [ ] **Step 1: Write the failing tests**

`crates/tinio-sandbox-windows/src/sid.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn the_package_name_is_root_derived_and_stable() {
        let a = package_name(Path::new(r"E:\data"));
        assert_eq!(a, package_name(Path::new(r"E:\data")));
        assert_ne!(a, package_name(Path::new(r"E:\other")));
        assert!(a.starts_with("tinio.sandbox."), "{a}");
        assert_eq!(a.len(), "tinio.sandbox.".len() + 16, "{a}");
        // AppContainer profile names are limited in length; the digest keeps
        // this short.
        assert!(a.len() < 64, "{a}");
    }

    #[test]
    fn one_directory_has_one_package_name_however_it_is_spelled() {
        // A second name for the same directory would mean a second SID, a second
        // ACE on the same tree, and a residue neither run would ever take over
        // (decision 21). Case, separator, a trailing separator and a `.` are all
        // the same directory.
        let expected = package_name(Path::new(r"E:\Data\Root"));
        assert_eq!(expected, package_name(Path::new(r"e:/data/root")));
        assert_eq!(expected, package_name(Path::new("E:\\Data\\Root\\")));
        assert_eq!(expected, package_name(Path::new(r"E:\Data\.\Root")));
    }

    #[test]
    fn creating_the_profile_yields_the_derived_sid_and_a_second_run_takes_it_over() {
        // Measured on 2026-09-19: a SID that is *only* derived cannot launch —
        // handing it to PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES makes
        // CreateProcessW fail with err=2 ERROR_FILE_NOT_FOUND for our own binary
        // and for cmd.exe alike, ACE already granted or not — and the same call
        // succeeds once `CreateAppContainerProfile` has run. Task 8's launch test
        // is where that is asserted end to end; this one pins the two facts the
        // rest of the plan rests on: the profile's SID is the derivation's SID,
        // and creating an existing profile is a takeover rather than an error.
        let name = package_name(Path::new(r"E:\data"));
        let sid = PackageSid::derive(&name).unwrap();
        assert!(sid.as_str().starts_with("S-1-15-2-"), "{}", sid.as_str());

        let wide = crate::wide(&name);
        let mut derived: Psid = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `derived`
        // is a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe {
            ffi::DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut derived)
        };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: `derived` is the SID just returned and `text` is a valid
        // out-parameter; the string is freed below with LocalFree.
        let ok = unsafe { ffi::ConvertSidToStringSidW(derived, &mut text) };
        assert_ne!(ok, 0);
        // SAFETY: `text` is a NUL-terminated UTF-16 string the conversion just
        // allocated, and it is the last use of the buffer.
        let rendered = unsafe { crate::from_wide(text) };
        // SAFETY: `text` came from the conversion, whose documented release is
        // LocalFree, and `derived` from the derivation, whose release is FreeSid.
        unsafe {
            ffi::LocalFree(text.cast());
            ffi::FreeSid(derived);
        }
        assert_eq!(rendered, sid.as_str(), "the profile's SID is the derived SID");

        // The second creation is what a run after a hard kill does, and it is
        // success with the SID filled in (decision 21's takeover).
        let mut again: Psid = ptr::null_mut();
        // SAFETY: as above: a null capability array with a count of zero is the
        // documented "no capabilities", and the SID is freed below.
        let hr = unsafe {
            ffi::CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut again,
            )
        };
        assert_eq!(hr, ffi::HRESULT_ALREADY_EXISTS, "hr=0x{hr:08X}");
        assert!(!again.is_null(), "ERROR_ALREADY_EXISTS still fills the SID in");
        // SAFETY: `again` is this call's own allocation, freed exactly once.
        unsafe { ffi::FreeSid(again) };
    }

    #[test]
    fn no_capabilities_is_the_default_and_is_free() {
        let caps = CapabilitySids::resolve(&[]).unwrap();
        assert!(caps.is_empty());
        assert_eq!(caps.len(), 0);
    }

    #[test]
    fn a_capability_string_that_is_not_a_sid_is_an_error() {
        assert!(CapabilitySids::resolve(&["not-a-sid"]).is_err());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows sid::`
Expected: FAIL — `cannot find function 'package_name'`.

- [ ] **Step 3: Write `sid.rs`**

```rust
//! The AppContainer package SID, and the capability SIDs a container is created
//! with.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14): the SID derivation and the two SID/string conversions are the crate's
//! only security-identifier FFI.

#![allow(unsafe_code)]

use std::path::{Path, PathBuf};
use std::ptr;

use sha2::{Digest, Sha256};

use tinio_sandbox_core::Result;

use crate::ffi::{self, Psid, SidAndAttributes};
use crate::handles::{last_error, step};

/// The AppContainer package name for `root`.
///
/// Derived, so a hard kill's residue is the *same* SID on the next run and the
/// launcher takes its own residue over instead of accumulating new ACEs (design
/// decision 21). Root-derived rather than a constant so that two servers serving
/// different roots do not share a package SID — a shared SID would let either
/// one read the other's granted tree — and bounded so that the residue stays one
/// directory per root, the same reasoning as decision 10's jail name.
///
/// **The normalisation is what makes the takeover work**, and it is lexical
/// rather than a filesystem call. `Path::components` drops a trailing separator,
/// drops `.` components and collapses repeated separators, so `E:\data`,
/// `E:\data\` and `E:\data\.` are one name: three spellings of one directory
/// that would otherwise derive three SIDs, leave three ACEs and take none of
/// them over — the exact failure decision 21 exists to prevent. Case is folded
/// for the same reason (`E:\Data` is that directory on Windows) and separators
/// are normalised because `/` and `\` are interchangeable to the filesystem and
/// to nothing else. `..` is deliberately left alone: resolving it lexically
/// would let one spelling derive another directory's name without that directory
/// being the one exposed.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::package_name;
///
/// let name = package_name(Path::new(r"E:\srv\tinio"));
/// assert!(name.starts_with("tinio.sandbox."));
/// // One directory, three spellings, one name — and so one SID.
/// assert_eq!(name, package_name(Path::new(r"e:/srv/tinio/.")));
/// ```
pub fn package_name(root: &Path) -> String {
    let normalised = root
        .components()
        .collect::<PathBuf>()
        .to_string_lossy()
        .replace('/', r"\")
        .to_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(normalised.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("tinio.sandbox.{}", &digest[..16])
}

/// A created package SID **and the profile behind it**, alive for as long as
/// anything needs them.
///
/// The two are one value deliberately. A SID whose profile does not exist cannot
/// launch, so there is no state in which this crate holds one without the other;
/// the raw SID is freed with `FreeSid` when this value drops, the string form
/// with `LocalFree`, and the profile with `DeleteAppContainerProfile` — so the
/// profile's lifetime is the value's, which is the launcher's, which is the
/// child's.
pub struct PackageSid {
    raw: Psid,
    text: String,
    name: String,
}

impl PackageSid {
    /// Create the profile for `name` and take its package SID.
    ///
    /// The name is both the profile's identity and its display name, so a
    /// profile left behind by a hard kill is recognisable in the shell's own
    /// list of packages as well as by a derivation (decision 21).
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the profile
    /// cannot be created or the SID cannot be rendered — a name that is too long
    /// or malformed surfaces here, before anything has been granted.
    pub fn derive(name: &str) -> Result<Self> {
        let wide = crate::wide(name);
        let mut raw: Psid = ptr::null_mut();
        // SAFETY: `wide` is a NUL-terminated UTF-16 buffer alive for the call and
        // `raw` is a valid out-parameter; the display name and the description
        // are the same buffer so the profile is identifiable, and a null
        // capability array with a count of zero is the documented "no
        // capabilities". The returned SID is freed in `Drop` with FreeSid, as the
        // API documents.
        let hr = unsafe {
            ffi::CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut raw,
            )
        };
        // `ERROR_ALREADY_EXISTS` is success and not a special case: it is exactly
        // what the run after a hard kill sees, `raw` is filled either way, and
        // treating it as a failure would make the residue unrecoverable rather
        // than reusable (design decision 21).
        if hr != 0 && hr != ffi::HRESULT_ALREADY_EXISTS {
            return Err(step(
                "CreateAppContainerProfile",
                name,
                std::io::Error::other(format!("CreateAppContainerProfile: hr=0x{hr:08X}")),
            ));
        }
        if raw.is_null() {
            return Err(step(
                "CreateAppContainerProfile",
                name,
                std::io::Error::other("CreateAppContainerProfile returned no SID"),
            ));
        }
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: `raw` is the SID `CreateAppContainerProfile` just returned, and
        // `text` is a valid out-parameter; the string is freed below.
        let ok = unsafe { ffi::ConvertSidToStringSidW(raw, &mut text) };
        if ok == 0 || text.is_null() {
            // SAFETY: `raw` is this call's own allocation and nothing else has
            // taken it.
            unsafe { ffi::FreeSid(raw) };
            return Err(step("render the package SID", name, last_error()));
        }
        // SAFETY: `text` is a NUL-terminated UTF-16 string the conversion just
        // allocated.
        let rendered = unsafe { crate::from_wide(text) };
        // SAFETY: `text` is the buffer the conversion allocated for us, and the
        // copy above was its last use.
        unsafe { ffi::LocalFree(text.cast()) };
        // `info!`, and it is the event an operator's "was this process confined,
        // and to what" question is answered from: the name the profile was created
        // under and the SID it returned, as fields rather than a sentence. When a
        // run left ACEs behind, this is the line that says whose they are (shared
        // §7's split, and decision 21).
        tracing::info!(step = "CreateAppContainerProfile", name = name, sid = %rendered);
        Ok(Self {
            raw,
            text: rendered,
            name: name.to_owned(),
        })
    }

    /// The `S-1-15-2-…` form: what `icacls` is given, what a log line prints,
    /// and what a test compares against. One accessor, not two — an earlier
    /// draft carried `sid_text` as a second name for exactly this string.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The raw SID, for [`ffi::SecurityCapabilities`].
    pub fn raw(&self) -> Psid {
        self.raw
    }
}

impl Drop for PackageSid {
    fn drop(&mut self) {
        // SAFETY: `raw` is this value's own allocation, produced by the profile
        // creation and freed exactly once here.
        unsafe { ffi::FreeSid(self.raw) };
        let wide = crate::wide(&self.name);
        // SAFETY: `wide` is NUL-terminated and alive for the call, and the name
        // is the one this value created the profile under.
        let hr = unsafe { ffi::DeleteAppContainerProfile(wide.as_ptr()) };
        if hr != 0 {
            // A residue rather than a failure, and `Drop` has no caller to report
            // one to: the next run derives the same name and takes this profile
            // over (decision 21). Warned, because shared §7's "a documented
            // degradation is logged" is what makes an undeleted profile visible
            // in a log rather than inferable from a package listing.
            tracing::warn!(step = "DeleteAppContainerProfile", name = %self.name, hr = hr);
        }
    }
}

/// The capability SIDs a container is created with.
///
/// Kept alive until the attribute list is deleted, because
/// `UpdateProcThreadAttribute` requires every value it is given to outlive the
/// list, and the kernel reads it again at `CreateProcessW` time rather than
/// copying it.
pub struct CapabilitySids {
    entries: Vec<SidAndAttributes>,
    owned: Vec<Psid>,
}

impl CapabilitySids {
    /// Resolve capability SID strings (`S-1-15-3-3`, …) into entries.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) for a string that is
    /// not a SID.
    pub fn resolve(sids: &[&str]) -> Result<Self> {
        let mut entries = Vec::with_capacity(sids.len());
        let mut owned = Vec::with_capacity(sids.len());
        for text in sids {
            let wide = crate::wide(text);
            let mut raw: Psid = ptr::null_mut();
            // SAFETY: `wide` is NUL-terminated and alive for the call; the
            // returned SID is freed in `Drop` with LocalFree, as documented.
            let ok = unsafe { ffi::ConvertStringSidToSidW(wide.as_ptr(), &mut raw) };
            if ok == 0 || raw.is_null() {
                return Err(step("resolve a capability SID", *text, last_error()));
            }
            owned.push(raw);
            entries.push(SidAndAttributes {
                sid: raw,
                attributes: 0,
            });
        }
        Ok(Self { entries, owned })
    }

    /// The contiguous array the `SECURITY_CAPABILITIES` value points at.
    pub fn as_ptr(&self) -> *const SidAndAttributes {
        self.entries.as_ptr()
    }

    /// How many capabilities are present.
    pub fn len(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Whether the container gets no capability at all — the default, and the
    /// configuration the outbound guarantee rests on (design §7).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for CapabilitySids {
    fn drop(&mut self) {
        for sid in self.owned.drain(..) {
            // SAFETY: each `sid` came from ConvertStringSidToSidW, which
            // documents LocalFree as the release, and each is freed once.
            unsafe { ffi::LocalFree(sid.cast()) };
        }
    }
}
```

Add the two shared helpers to `lib.rs`, since three modules need them:

```rust
/// A NUL-terminated UTF-16 copy of `text`.
pub(crate) fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Read a NUL-terminated UTF-16 string into a `String`.
///
/// # Safety
///
/// `text` must point at a NUL-terminated UTF-16 buffer that stays valid for the
/// call.
#[allow(unsafe_code)]
pub(crate) unsafe fn from_wide(text: *const u16) -> String {
    let end = unsafe { (0..).position(|offset| *text.add(offset) == 0).unwrap_or(0) };
    let slice = unsafe { std::slice::from_raw_parts(text, end) };
    String::from_utf16_lossy(slice)
}
```

`wide` is safe and `from_wide` is not. **The attribute goes on `from_wide` and not on the file**, and that is the point rather than a detail of placement: the manifest is `unsafe_code = "deny"`, a crate-level `#![allow(unsafe_code)]` would lift that for every line of `lib.rs` — including the modules it declares and every item added to it afterwards — and the constitution asks for the smallest scope in which the exception is expressible. `lib.rs` therefore joins decision 14's list as the crate root's single site, with the attribute on the one function that needs it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows`
Expected: PASS — the layout tests, `handles`' recording-subscriber test, and the five SID tests. **The profile one creates a real package on the machine and needs no elevation to do it**, which is the same property the derivation had (design §6) — and it leaves nothing behind, because the last `PackageSid` of the test drops and takes the profile with it. A refused delete is a residue rather than a test failure — the next run derives the same name and takes it over (decision 21) — and the `warn!` that reports it needs a subscriber, which `cargo test` installs none of (shared §7): the residue shows up in the next run's `CreateAppContainerProfile`, not in this run's output.

Design §6 and decision 21 carry the change the measurement forces: the section states the mechanism, and the mechanism is "create the profile whose SID the served root derives", not "derive a SID and launch with it". Decision 21's residue story gains the second artifact while keeping its rule — identifiable, reusable, and taken back on every exit path the launcher survives.

---

### Task 3: The exposure set and the container capability list

**Files:**
- Create: `crates/tinio-sandbox-windows/src/backend.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`

**Interfaces:**
- Consumes: `AccessMode`, `Exposure`, `Sandbox`, `Error` from the core.
- Produces: `backend::{Capability, Options, exposures, canonical_exposures, merge, describe, served_root}`; `Backend` (`Default`, `with_options`, `options`).

This is the half of the backend that runs before anything privileged: which paths the whitelist covers, which capabilities the container gets, and what the volume says those paths really are. The first two are functions of builder state and are testable without a filesystem and without a launch (design §12's "Unit, every platform"); the third is the one step here that has to ask the volume, so it is a function of its own — and the reason it is not folded into `exposures` is that folding it in would make the arithmetic untestable without a disk and a junction.

- [ ] **Step 1: Write the failing tests**

`crates/tinio-sandbox-windows/src/backend.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::path::Path;

    use tinio_sandbox_core::{AccessMode, Exposure, Sandbox};

    use super::*;

    #[test]
    fn the_executables_directory_is_exposed_read_only() {
        // Measured: without an ACE on the directory holding the binary the
        // confined child cannot load at all (design decision 22).
        let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
        let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
        let exe_dir = all
            .iter()
            .find(|e| e.host == Path::new(r"E:\build"))
            .expect("the executable's directory is exposed");
        assert_eq!(exe_dir.access, AccessMode::ReadOnly);
    }

    #[test]
    fn the_callers_access_survives_the_executable_exposure() {
        // The whole-directory exposure is read-only and must never widen a path
        // the caller exposed read-write.
        let sandbox = Sandbox::new().expose(r"E:\build", AccessMode::ReadWrite);
        let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn a_duplicate_host_keeps_the_widest_access() {
        let merged = merge(vec![
            Exposure {
                host: r"E:\data".into(),
                access: AccessMode::ReadWrite,
            },
            Exposure {
                host: r"E:\data".into(),
                access: AccessMode::ReadOnly,
            },
        ]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].access, AccessMode::ReadWrite);
    }

    #[test]
    fn nothing_outside_the_exposures_is_exposed() {
        let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadOnly);
        let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
        assert!(!all.iter().any(|e| e.host == Path::new(r"E:\srv\other")));
        assert!(!all.iter().any(|e| e.host == Path::new(r"E:\")));
    }

    #[test]
    fn capabilities_parse_by_name_and_by_sid() {
        assert_eq!(
            Capability::from_name("internetClient").unwrap(),
            Capability::InternetClient
        );
        assert_eq!(
            Capability::from_name("privateNetworkClientServer").unwrap(),
            Capability::PrivateNetworkClientServer
        );
        assert_eq!(
            Capability::from_sid("S-1-15-3-1").unwrap(),
            Capability::InternetClient
        );
        assert!(Capability::from_name("bogus").is_err());
    }

    #[test]
    fn capability_names_round_trip_through_their_sids() {
        for cap in [
            Capability::InternetClient,
            Capability::InternetClientServer,
            Capability::PrivateNetworkClientServer,
        ] {
            assert_eq!(Capability::from_sid(cap.sid()).unwrap(), cap);
            assert_eq!(Capability::from_name(cap.name()).unwrap(), cap);
        }
    }

    #[test]
    fn the_default_container_has_no_capability() {
        // Design §7: the outbound guarantee is the *absence* of a network
        // capability, so an empty list is the default and stays empty unless an
        // operator asks otherwise.
        assert!(Options::default().capabilities.is_empty());
        assert!(Capability::parse_list(None).unwrap().is_empty());
        assert!(Capability::parse_list(Some("")).unwrap().is_empty());
        assert_eq!(
            Capability::parse_list(Some("privateNetworkClientServer")).unwrap(),
            vec![Capability::PrivateNetworkClientServer]
        );
        assert!(Capability::parse_list(Some("bogus")).is_err());
    }

    #[test]
    fn the_served_root_is_the_first_exposure() {
        let sandbox = Sandbox::new()
            .expose(r"E:\srv\tinio", AccessMode::ReadOnly)
            .expose(r"E:\state", AccessMode::ReadWrite);
        assert_eq!(served_root(&sandbox).unwrap(), Path::new(r"E:\srv\tinio"));
        assert!(served_root(&Sandbox::new()).is_err());
    }

    #[test]
    fn a_path_spelled_through_a_junction_is_refused_rather_than_followed() {
        // Measured on this host, 2026-09-19: `icacls` resolves the path it is
        // given, and `(OI)(CI)` is inheritable — so granting through a junction
        // puts the ACE on the junction's *target*, outside the exposure, and that
        // target's own children inherit it. A junction rather than a symlink
        // because `mklink /J` needs no elevation and a symlink does.
        let dir = tempfile::tempdir().expect("temp dir");
        let real = dir.path().join("real");
        std::fs::create_dir_all(&real).expect("create the target");
        let link = dir.path().join("link");
        let made = std::process::Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&link)
            .arg(&real)
            .output()
            .expect("run mklink");
        assert!(
            made.status.success(),
            "{}",
            String::from_utf8_lossy(&made.stderr)
        );

        let exposure = |host: &Path| Exposure {
            host: host.to_path_buf(),
            access: AccessMode::ReadOnly,
        };
        let err = canonical_exposures(vec![exposure(&link)]).expect_err("a junction is refused");
        assert!(err.to_string().contains("reparse point"), "{err}");
        // The target it points at is a directory like any other, and is granted:
        // what is refused is the spelling that would have granted somewhere else.
        let all = canonical_exposures(vec![exposure(&real)]).expect("a real directory resolves");
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn two_spellings_of_one_directory_collapse_to_one_exposure() {
        // The merge *after* the resolution is the one that matters: two spellings
        // that survive the first merge as two entries are one directory once they
        // are resolved, and two ACEs on one directory is what rollback would have
        // to remove twice.
        let dir = tempfile::tempdir().expect("temp dir");
        let real = dir.path().join("data");
        std::fs::create_dir_all(&real).expect("create the directory");
        let all = canonical_exposures(vec![
            Exposure {
                host: real.clone(),
                access: AccessMode::ReadOnly,
            },
            Exposure {
                host: real.join("."),
                access: AccessMode::ReadWrite,
            },
        ])
        .expect("both spellings resolve");
        assert_eq!(all.len(), 1, "{}", describe(&all));
        assert_eq!(all[0].access, AccessMode::ReadWrite, "the widest access survives");
        assert_eq!(all[0].host, std::fs::canonicalize(&real).expect("canonical"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows backend::`
Expected: FAIL — `cannot find function 'exposures'`.

- [ ] **Step 3: Write the implementation**

```rust
//! The Windows backend: the whitelist it grants, the container it launches, the
//! relay it arms, and the supervision it returns to the caller.
//!
//! The composition lives in `Backend::confine` (Task 9); this module opens with
//! the parts that are pure data, so the whitelist and the capability list are
//! testable without a launch.

use std::io;
use std::os::windows::fs::MetadataExt;
use std::path::{Path, PathBuf};

use tinio_sandbox_core::{AccessMode, Error, Exposure, Sandbox};

/// A capability the container may hold.
///
/// Empty by default, on purpose: the whole of the Windows network guarantee is
/// that a container with no network capability cannot open an outbound
/// connection (design §7). Capabilities are additive, so an operator who needs a
/// non-loopback listener adds
/// [`PrivateNetworkClientServer`](Self::PrivateNetworkClientServer) and accepts
/// that the container may then reach the private network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// `internetClient` — outbound to the internet.
    InternetClient,
    /// `internetClientServer` — outbound, and inbound from the internet.
    InternetClientServer,
    /// `privateNetworkClientServer` — inbound from the private network, which is
    /// what a non-loopback bind needs (design §7).
    PrivateNetworkClientServer,
}

impl Capability {
    /// The capability's SID.
    pub const fn sid(self) -> &'static str {
        match self {
            Self::InternetClient => "S-1-15-3-1",
            Self::InternetClientServer => "S-1-15-3-2",
            Self::PrivateNetworkClientServer => "S-1-15-3-3",
        }
    }

    /// The name used in configuration and in `TINIO_SANDBOX_CAPABILITIES`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::InternetClient => "internetClient",
            Self::InternetClientServer => "internetClientServer",
            Self::PrivateNetworkClientServer => "privateNetworkClientServer",
        }
    }

    /// Parse a capability by name.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for an unknown name, so a typo cannot silently
    /// leave the container with fewer capabilities than the operator asked for.
    pub fn from_name(name: &str) -> Result<Self, Error> {
        match name {
            "internetClient" => Ok(Self::InternetClient),
            "internetClientServer" => Ok(Self::InternetClientServer),
            "privateNetworkClientServer" => Ok(Self::PrivateNetworkClientServer),
            _ => Err(Error::Unsupported {
                detail: "unknown AppContainer capability (expected internetClient, internetClientServer or privateNetworkClientServer)",
            }),
        }
    }

    /// Parse a capability by SID.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for a SID this crate does not name.
    pub fn from_sid(sid: &str) -> Result<Self, Error> {
        match sid {
            "S-1-15-3-1" => Ok(Self::InternetClient),
            "S-1-15-3-2" => Ok(Self::InternetClientServer),
            "S-1-15-3-3" => Ok(Self::PrivateNetworkClientServer),
            _ => Err(Error::Unsupported {
                detail: "unknown AppContainer capability SID",
            }),
        }
    }

    /// Parse a comma-separated list, the form the environment channel uses.
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for any unknown entry.
    pub fn parse_list(raw: Option<&str>) -> Result<Vec<Self>, Error> {
        let Some(raw) = raw else {
            return Ok(Vec::new());
        };
        raw.split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(Self::from_name)
            .collect()
    }
}

/// What the launcher needs beyond the [`Sandbox`] and the [`LaunchPlan`].
///
/// [`LaunchPlan`]: tinio_sandbox_core::LaunchPlan
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The container's capabilities; empty by default (design §7).
    pub capabilities: Vec<Capability>,
    /// Override the derived package name. Tests use it; production always
    /// derives, so a crashed run's residue is recognizable (decision 21).
    pub package_name: Option<String>,
    /// Override the program the launcher re-invokes. `None` means this process's
    /// own executable, which is what production uses (design §10 step 6).
    ///
    /// A test seam with one job: a launch that fails *after* the ACLs are on is
    /// the only way to exercise design §11's failed-launch rollback.
    pub program: Option<PathBuf>,
    /// Override the child's arguments. `None` means the launcher's own, minus
    /// `argv[0]`, with the payload element appended (design §3 step 1).
    ///
    /// A test seam: a test binary's own argv belongs to libtest, so the harness
    /// points its child at one role function instead of at the suite.
    pub args: Option<Vec<std::ffi::OsString>>,
}

/// The Windows backend.
///
/// P0 shipped this type as a stub whose `confine` returned
/// [`Error::Unsupported`]; P2 fills it in.
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: Options,
}

impl Backend {
    /// A backend with the given options.
    pub fn with_options(options: Options) -> Self {
        Self { options }
    }

    /// The options it was built with.
    pub fn options(&self) -> &Options {
        &self.options
    }
}

/// The full whitelist: what the caller exposed, plus the directory holding the
/// executable.
///
/// The executable's directory is exposed **read-only** and whole-directory,
/// because a Rust binary may load siblings and the alternative is a
/// hand-maintained list of shared-library paths (design decision 22, and its
/// accepted consequence: the confined process can read its own install
/// directory).
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_windows::backend::exposures;
///
/// let sandbox = Sandbox::new().expose(r"E:\srv\tinio", AccessMode::ReadWrite);
/// let all = exposures(&sandbox, Path::new(r"E:\build\serve.exe"));
/// assert_eq!(all.len(), 2);
/// ```
pub fn exposures(sandbox: &Sandbox, program: &Path) -> Vec<Exposure> {
    let mut all = sandbox.exposures().to_vec();
    if let Some(dir) = program.parent() {
        all.push(Exposure {
            host: dir.to_path_buf(),
            access: AccessMode::ReadOnly,
        });
    }
    merge(all)
}

/// Collapse duplicate hosts, keeping the widest access.
///
/// A path may be exposed twice — the caller's list, the state directory, the
/// executable's directory — and granting the same SID twice on the same
/// directory would leave an entry rollback has to remove twice. The rule matches
/// the core's `Sandbox::expose`: a narrower entry never silently downgrades a
/// wider one.
pub fn merge(exposures: Vec<Exposure>) -> Vec<Exposure> {
    let mut merged: Vec<Exposure> = Vec::with_capacity(exposures.len());
    for exposure in exposures {
        match merged
            .iter_mut()
            .find(|existing| existing.host == exposure.host)
        {
            Some(existing) => {
                if !exposure.access.is_read_only() {
                    existing.access = AccessMode::ReadWrite;
                }
            }
            None => merged.push(exposure),
        }
    }
    merged
}

/// The exposure set as the volume names it, with every reparse point refused.
///
/// **Why this is a step of its own rather than part of `exposures`.** `icacls`
/// resolves the path it is given and `(OI)(CI)` is inheritable, so a junction or
/// a directory symlink anywhere in an exposed path puts the ACE on whatever the
/// link resolves to — a directory outside the exposure — and inheritance then
/// carries the same ACE into that directory's own children. Following the link
/// *is* the grant the whitelist exists to prevent, and `std::fs::canonicalize`
/// by itself is the following: it resolves the link and returns a path that is no
/// longer a reparse point. The refusal therefore has to be checked on the path as
/// it is spelled, before the resolution, and the resolution has to happen as well
/// — a grant written on a spelling the volume does not use is a grant the child's
/// own view of the path may not match.
///
/// **This is the Windows half of the requirement macOS meets with
/// `with_canonical_paths`** (P3), and the two are separate functions because the
/// failure they prevent is not the same shape: there a profile naming an
/// unresolved spelling simply matches nothing, so macOS *adds* the canonical form
/// beside the original; here the resolved spelling is what the grant lands on, so
/// Windows *requires* the canonical form and refuses the link that produced it.
/// What the two share is the rule, not the remedy: a path is granted in the form
/// the kernel resolves, never in the form it happened to be written in.
///
/// # Errors
///
/// [`Error::BadPath`] for a path that is, or passes through, a reparse point —
/// the variant whose own definition is "a path handed to `Sandbox::expose`
/// cannot be expressed inside the confinement", which is exactly what a link
/// whose target lies outside the exposure is. [`Error::Launch`] when a path
/// cannot be resolved at all, carrying the OS error.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Exposure};
/// use tinio_sandbox_windows::backend::canonical_exposures;
///
/// let all = canonical_exposures(vec![Exposure {
///     host: Path::new(r"E:\srv\tinio").to_path_buf(),
///     access: AccessMode::ReadWrite,
/// }])?;
/// assert_eq!(all.len(), 1);
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn canonical_exposures(exposures: Vec<Exposure>) -> Result<Vec<Exposure>, Error> {
    let mut canonical = Vec::with_capacity(exposures.len());
    for exposure in exposures {
        refuse_reparse_points(&exposure.host)?;
        // The `\\?\`-prefixed verbatim form is what `std::fs::canonicalize`
        // returns on Windows, and `icacls` accepts it (measured on this host,
        // 2026-09-19) — which is what lets the granted spelling *be* the resolved
        // one instead of a second spelling of it.
        let host = std::fs::canonicalize(&exposure.host)
            .map_err(|source| crate::handles::step("resolve an exposed path", &exposure.host, source))?;
        // `info!`: this set is "to what" half of shared §7's question. An operator
        // whose server cannot reach a directory reads those lines to see which
        // paths the run was actually about to grant, in the spelling the volume
        // resolves them to.
        tracing::info!(
            step = "resolve an exposed path",
            path = %host.display(),
            access = ?exposure.access
        );
        canonical.push(Exposure {
            host,
            access: exposure.access,
        });
    }
    // Merged again, and this is the merge that matters: two spellings of one
    // directory are two entries until they are resolved, and one directory with
    // two ACEs is a rollback that has to remove the same grant twice.
    Ok(merge(canonical))
}

/// Refuse `path` when it, or any directory above it, is a reparse point.
///
/// The whole chain rather than the last component alone: `E:\link\tinio` names a
/// directory whose *parent* is a junction, and a grant on it lands wherever that
/// junction points. `symlink_metadata` is the call that reports on the link
/// itself rather than on its target, which is the property this check needs and
/// the reason a plain `metadata` would not do.
///
/// # Errors
///
/// [`Error::BadPath`] naming the component that is a reparse point.
fn refuse_reparse_points(path: &Path) -> Result<(), Error> {
    for component in path.ancestors() {
        // A component that cannot be read is not this check's business: the
        // resolution that follows reports it, with the OS error attached.
        let Ok(metadata) = std::fs::symlink_metadata(component) else {
            continue;
        };
        if metadata.file_attributes() & crate::ffi::FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(Error::BadPath {
                path: component.to_path_buf(),
                reason: "is a reparse point (a junction or a symlink), and a grant through it would land on its target rather than inside the exposed tree",
            });
        }
    }
    Ok(())
}

/// The whitelist as one line, for the launcher's own log.
///
/// The launcher prints what it is about to grant: an operator whose server
/// cannot read a path needs to see which paths were on the list, and the
/// design's residue story needs the package SID printed next to it.
pub fn describe(exposures: &[Exposure]) -> String {
    exposures
        .iter()
        .map(|exposure| format!("{} ({})", exposure.host.display(), exposure.access))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The served root: the first exposure the caller gave.
///
/// The core's builder keeps the caller's order and `serve` exposes the root
/// before anything else, so the first entry is the root by construction.
pub fn served_root(sandbox: &Sandbox) -> Result<PathBuf, Error> {
    sandbox
        .exposures()
        .first()
        .map(|exposure| exposure.host.clone())
        .ok_or(Error::Step {
            // The caller's bug rather than the platform's shortcoming, so it is
            // a named step and not `Unsupported` (design §11).
            step: "confine",
            source: io::Error::other("nothing exposed to confine to"),
        })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows backend::`
Expected: PASS — ten tests. The two that touch the volume create their own temporary tree and a junction under it; a host that refuses `mklink /J` refuses the whole suite rather than skipping quietly, because a junction is what a grant through a link would follow on every host that has one (design §6, and the test's own message says so).

---

### Task 4: The DACL whitelist, its rollback, and the inheritance question

**Files:**
- Create: `crates/tinio-sandbox-windows/src/acl.rs`
- Create: `crates/tinio-sandbox-windows/tests/acl.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (the inheritance row)

**Interfaces:**
- Consumes: `sid::PackageSid`, `AccessMode`.
- Produces: `acl::{ace_spec, grant_spec, icacls_binary, grant, revoke, has_ace, Grants}`, with `Grants::{new, apply, rollback, is_empty, sid_text}` and `Drop`.

Grant-only, never revoke-then-grant (design decision 15): `/grant:r` replaces this SID's entry and nothing else, `/remove:g` removes exactly this SID's entries, and neither touches a foreign ACE, the permission bits or the owner. Idempotence is what makes decision 21's takeover work: a residue from a crashed run is replaced rather than duplicated, and removed by the next clean exit.

**§6's open question is settled in this task's second test.** The design's evidence table admits that every ACL probe used `icacls … /t`, which applies the ACE to each child explicitly as well as to the directory, so "an inheritable ACE on the exposed directory reaches its pre-existing children" was never isolated — and §6 says P2 must settle it before the grant list is treated as final. The two outcomes are not cosmetic: **inheritance alone** keeps one ACE per exposed directory and a rollback of one `icacls /remove:g` per directory; **a walk** (`/t`) puts an ACE on every file in the operator's tree and makes the rollback a per-file undo, which is a different residue story on a hard kill (one directory versus every file).

**What the tool counts and what the filesystem does are two different counts, and only the first is small.** `icacls` answers `Successfully processed 1 files` for a directory-only `/grant:r` because one path was named, and neither the launcher nor the tool walks the operator's tree — that half of §6 is true and stays true. The ACE it writes is `(OI)(CI)`, though, and the propagation is the *filesystem*'s: every descendant the directory already holds picks up an inherited copy, which is precisely what this task's second test demonstrates and why that measurement is the evidence rather than a formality. The grant pass (before `CreateProcessW`) and the rollback pass (`/remove:g`, after the child exits) each touch as many objects as the served tree contains — the whole corpus, twice per run — and **no document gives that cost a number**: an `icacls` timing, a per-object cost and a tree-size assumption are all missing, while the readiness budget this feature has to fit inside is 30 s (`crates/tinio-e2e/tests/steps/clients.rs`'s `START_TIMEOUT`). Task 13 establishes the number instead of assuming one, and states what an operator with a very large tree should do about it.

- [ ] **Step 1: Write the failing unit tests**

`crates/tinio-sandbox-windows/src/acl.rs`:

```rust
#[cfg(test)]
mod tests {
    use tinio_sandbox_core::AccessMode;

    use super::*;

    #[test]
    fn the_permission_spec_is_inherited_by_children() {
        // `(OI)(CI)` is the whole reason nothing has to be granted per file
        // (design §6: ReadWrite → (OI)(CI)M, ReadOnly → (OI)(CI)RX) — subject to
        // the pre-existing-child question this task settles.
        assert_eq!(ace_spec(AccessMode::ReadOnly), "(OI)(CI)RX");
        assert_eq!(ace_spec(AccessMode::ReadWrite), "(OI)(CI)M");
    }

    #[test]
    fn the_icacls_binary_comes_from_the_system_directory_not_the_environment() {
        // The process that decides this run's DACLs must not be selectable
        // through the environment. Robustness rather than defence — a local
        // actor is out of the threat model — but the rule in `icacls_binary`'s
        // doc is only true, and this test only meaningful, if nothing on the
        // path is read from the environment.
        let binary = icacls_binary();
        assert!(binary.is_absolute(), "{}", binary.display());
        assert_eq!(
            binary.file_name().and_then(|name| name.to_str()),
            Some("icacls.exe")
        );
        assert!(binary.to_string_lossy().to_lowercase().contains("system32"));
    }

    #[test]
    fn the_grant_argument_names_the_sid_literally() {
        // The `*` prefix is what stops icacls resolving the SID as a name.
        assert_eq!(
            grant_spec("S-1-15-2-1-2-3", AccessMode::ReadOnly),
            "*S-1-15-2-1-2-3:(OI)(CI)RX"
        );
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows acl::`
Expected: FAIL — `cannot find function 'ace_spec'`.

- [ ] **Step 3: Write `acl.rs`**

```rust
//! The ACL whitelist: grant the package SID an access mode on each exposed path,
//! and take exactly those grants back on the way out (design decisions 15, 21
//! and §6).
//!
//! The DACL is edited through `icacls.exe` rather than through
//! `SetNamedSecurityInfo` FFI, for three reasons: `/grant:r` *replaces* this
//! SID's entry and touches no other ACE, which is exactly the semantics decision
//! 21's takeover and decision 15's grant-only rule ask for; `/remove:g *<sid>`
//! removes exactly our entries; and every ACL row of the design's evidence table
//! was produced with this tool. §11 accepts the consequence: where a grant goes
//! through a tool, the failure that arrives is the tool's exit status and text,
//! and [`Error::Launch`] carries both forms.
//!
//! `icacls` is resolved from the system directory the kernel reports rather than
//! from `PATH` or an environment variable: the process that decides this run's
//! DACLs must not be selectable through the environment. That is robustness
//! rather than defence — whoever can set this launcher's environment is a local
//! actor, and local actors are outside this feature's threat model — and it is
//! what makes the rule in the previous sentence a fact rather than an intention.
//! This module contains no `unsafe` code of its own: it is a subprocess, not an
//! FFI call, and the one kernel call it needs goes through [`crate::handles`].
//!
//! **What a grant costs is not what the tool reports.** `icacls` answers
//! `Successfully processed 1 files` because it was handed one path, and neither
//! it nor this module walks anything; the `(OI)(CI)` ACE is propagated by the
//! filesystem to every descendant that path already holds. The grant pass and its
//! rollback therefore each touch as many objects as the served tree contains —
//! the whole corpus, twice per run — and nothing measures that yet (Task 13
//! establishes the number; the readiness budget it competes with is 30 s).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;

use tinio_sandbox_core::{AccessMode, Error, Result};

use crate::handles::step;
use crate::sid::PackageSid;

/// The permission specification `icacls` grants for an access mode.
///
/// `M` is `FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE |
/// DELETE` (`0x001301BF`), `RX` is the read and execute halves (`0x001200A9`),
/// and both are inherited by children through `(OI)(CI)` — which is what makes
/// one ACE per directory enough, as this task's second test confirms. One ACE
/// *written* per directory, that is: how many objects the filesystem then carries
/// it to is the module doc's subject, and it is not a count this specification
/// can state.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::ace_spec;
///
/// assert_eq!(ace_spec(AccessMode::ReadWrite), "(OI)(CI)M");
/// ```
pub const fn ace_spec(access: AccessMode) -> &'static str {
    match access {
        AccessMode::ReadOnly => "(OI)(CI)RX",
        AccessMode::ReadWrite => "(OI)(CI)M",
    }
}

/// The `*<sid>:<perm>` argument `icacls` is given.
pub fn grant_spec(sid: &str, access: AccessMode) -> String {
    format!("*{sid}:{}", ace_spec(access))
}

/// `<system directory>\icacls.exe` — the kernel's own answer to where the system
/// directory is, so no part of this path is read from the environment.
///
/// **Robustness rather than defence, and the distinction is deliberate**: an
/// actor who can set this launcher's environment is a local actor, and local
/// actors are outside this feature's threat model, so nothing here is a defence
/// being claimed. What the rule buys is that the tool which edits the DACL is the
/// operating system's own even when `%SystemRoot%` is unset, stale or wrong. The
/// module's rule — the process that decides this run's DACLs is not selectable
/// through the environment — is what this function makes true of the code rather
/// than an intention stated beside it.
pub fn icacls_binary() -> PathBuf {
    crate::handles::system_directory().join("icacls.exe")
}

/// Grant `access` to `sid` on `path`, replacing any entry this SID already has
/// there and touching nothing else.
///
/// # Errors
///
/// [`Error::Launch`] naming the path and carrying `icacls`'s own diagnostic when
/// it exits non-zero.
pub fn grant(path: &Path, access: AccessMode, sid: &PackageSid) -> Result<()> {
    let spec = grant_spec(sid.as_str(), access);
    run(
        "icacls /grant:r",
        path,
        &[OsStr::new("/grant:r"), OsStr::new(&spec), OsStr::new("/q")],
    )?;
    // Shared §7's per-operation event, with the three fields that matter here:
    // which path, which access, which SID.
    tracing::debug!(
        step = "icacls /grant:r",
        path = %path.display(),
        access = ?access,
        sid = sid.as_str()
    );
    Ok(())
}

/// Remove this SID's entries from `path`, leaving every other ACE alone.
///
/// Idempotent: a path this SID has no entry on is success, which is what lets
/// rollback run over paths whose grant never landed.
///
/// # Errors
///
/// [`Error::Launch`], as [`grant`].
pub fn revoke(path: &Path, sid: &PackageSid) -> Result<()> {
    let spec = format!("*{}", sid.as_str());
    run(
        "icacls /remove:g",
        path,
        &[OsStr::new("/remove:g"), OsStr::new(&spec), OsStr::new("/q")],
    )?;
    tracing::debug!(step = "icacls /remove:g", path = %path.display(), sid = sid.as_str());
    Ok(())
}

/// Whether `path`'s DACL carries an entry for `sid`.
///
/// The oracle is `icacls`'s own rendering — the same tool the design's evidence
/// table was produced with. An AppContainer package SID does not resolve to an
/// account name, so the rendered form is the `S-1-15-2-…` string itself.
///
/// **A non-zero exit is an error and not `Ok(false)`**, which is the difference
/// between an oracle and a guess: `invoke` reports a failed *spawn* and nothing
/// else, so a call that exited non-zero would hand back its empty stdout as "no
/// entry for this SID" — and every assertion that a rolled-back directory
/// carries no ACE would then pass because `icacls` refused to read the path
/// rather than because the ACE was gone.
///
/// # Errors
///
/// [`Error::Launch`] when `icacls` cannot read the DACL at all, carrying its own
/// exit status and text.
pub fn has_ace(path: &Path, sid: &PackageSid) -> Result<bool> {
    let output = checked("icacls (read the DACL)", path, &[])?;
    Ok(String::from_utf8_lossy(&output.stdout).contains(sid.as_str()))
}

/// The grants this run applied, so that every exit path can take exactly those
/// back (design decision 15 — including the path where the launch itself failed,
/// not only the clean one).
pub struct Grants {
    sid: Arc<PackageSid>,
    applied: Vec<PathBuf>,
}

impl Grants {
    /// An empty grant set for `sid`.
    pub fn new(sid: Arc<PackageSid>) -> Self {
        Self {
            sid,
            applied: Vec::new(),
        }
    }

    /// Grant `access` on `path` and remember it.
    ///
    /// The record happens *after* the grant succeeds, so a failure leaves the
    /// path recorded only if it may already carry the entry.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] from [`grant`].
    pub fn apply(&mut self, path: &Path, access: AccessMode) -> Result<()> {
        grant(path, access, &self.sid)?;
        self.applied.push(path.to_path_buf());
        Ok(())
    }

    /// Whether anything is still granted.
    pub fn is_empty(&self) -> bool {
        self.applied.is_empty()
    }

    /// The SID this set is about, for logs and for tests.
    pub fn sid_text(&self) -> &str {
        self.sid.as_str()
    }

    /// Remove every grant this run applied, in reverse order, and forget them.
    ///
    /// A failure does not stop the loop: it is kept and returned after every
    /// other path has been attempted, because a rollback that stopped at the
    /// first error would leave the rest of the residue behind. It is not printed
    /// here either — [`step`] logged it where the `Error` was built, with the
    /// same `step` this method's caller will see (shared §7).
    ///
    /// # Errors
    ///
    /// The first failure, after attempting every other path.
    pub fn rollback(&mut self) -> Result<()> {
        let mut first: Option<Error> = None;
        while let Some(path) = self.applied.pop() {
            if let Err(err) = revoke(&path, &self.sid) {
                first.get_or_insert(err);
            }
        }
        match first {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

impl Drop for Grants {
    fn drop(&mut self) {
        // Every exit path, including a panic: design §11 requires the rollback
        // after a failed launch too, and a guard is the only shape that cannot be
        // forgotten on one of them.
        if !self.applied.is_empty() {
            let _ = self.rollback();
        }
    }
}

/// Run `icacls` on `path` with `flags`, refusing a non-zero exit.
fn run(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<()> {
    checked(operation, path, flags).map(drop)
}

/// Run `icacls` on `path` with `flags` and refuse a non-zero exit, carrying the
/// tool's own diagnostic — §11's Windows row in the form this crate can honour:
/// the failure that arrives is the tool's exit status and text.
///
/// The one helper `run` and [`has_ace`] share, so that a write and a read of the
/// DACL cannot disagree about what "the call failed" means.
fn checked(operation: &'static str, path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let output = invoke(path, flags)?;
    if output.status.success() {
        return Ok(output);
    }
    let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let status = output.status.code().unwrap_or(-1);
    Err(step(
        operation,
        path.to_path_buf(),
        std::io::Error::other(format!("icacls exited {status}: {diagnostic}")),
    ))
}

/// Run `icacls` on `path`, with the path first as icacls' own syntax requires,
/// and no shell in between.
fn invoke(path: &Path, flags: &[&OsStr]) -> Result<Output> {
    let mut command = Command::new(icacls_binary());
    command.arg(path).args(flags);
    command
        .output()
        .map_err(|source| step("run icacls", path.to_path_buf(), source))
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows acl::`
Expected: PASS — three tests, none of them touching a DACL.

- [ ] **Step 5: Write the failing integration test, inheritance question first**

`crates/tinio-sandbox-windows/tests/acl.rs`:

```rust
//! The DACL whitelist against a real filesystem, with `icacls` as the
//! independent oracle for the grant, the rollback and — first of all — the
//! question design §6 leaves open: whether the directory's inheritable ACE
//! reaches files that were already there.

#![cfg(windows)]

use std::path::Path;
use std::sync::Arc;

use tinio_sandbox_core::AccessMode;
use tinio_sandbox_windows::{
    acl,
    sid::{PackageSid, package_name},
};

/// The SID this run derives for `root` — the same question the launcher asks.
fn sid_for(root: &Path) -> PackageSid {
    PackageSid::derive(&package_name(root)).expect("derive the package SID")
}

/// Run `icacls` the way a human would, for the oracle assertions below.
///
/// **The exit status is checked, and that is what makes this an oracle.** A
/// helper that returned its stdout whatever the exit status was would hand an
/// empty string back for a path `icacls` refused, and every "the ACE is gone"
/// assertion built on it would pass for the wrong reason — the same failure the
/// crate's own `has_ace` refuses one layer down (Task 4).
fn icacls(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new(acl::icacls_binary())
        .arg(path)
        .args(args)
        .output()
        .expect("run icacls");
    assert!(
        output.status.success(),
        "icacls {args:?} on {} exited {}: {}",
        path.display(),
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// The design's open row: an inheritable ACE on a directory, and a file that was
/// already inside it.
///
/// Every probe behind §6 used `icacls … /t`, which applies the ACE to each child
/// explicitly, so inheritance alone was never isolated. This test makes the
/// distinguishing run: the file is created *before* the grant, the grant is
/// made **without** `/t`, and the oracle is asked whether the file's DACL
/// carries the package SID.
#[test]
fn the_directory_ace_alone_reaches_a_file_that_was_already_there() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    let existing = root.join("bucket").join("before.txt");
    let created_later = root.join("bucket").join("after.txt");
    std::fs::create_dir_all(existing.parent().unwrap()).expect("create the tree");
    std::fs::write(&existing, b"before the grant").expect("seed");
    let sid = sid_for(&root);
    let sid_text = sid.as_str().to_string();

    assert!(!acl::has_ace(&existing, &sid).unwrap(), "no package ACE yet");

    let mut grants = acl::Grants::new(Arc::new(sid_for(&root)));
    grants.apply(&root, AccessMode::ReadWrite).expect("grant");

    let before = icacls(&existing, &[]);
    let inherited = before.contains(&sid_text);
    // Recorded in the design's evidence table by this task's last step, whichever
    // way it came out: the answer decides whether the launcher walks the tree.
    assert!(
        inherited,
        "an inheritable ACE on the directory did not reach a pre-existing file, so the grant list needs\n\
         the `/t` walk of design §6 and the per-file rollback that comes with it. File DACL:\n{before}"
    );
    assert!(
        before.to_lowercase().contains("inherited") || before.contains("(I)"),
        "the entry on the pre-existing file should be the *inherited* one, not a second explicit ACE:\n{before}"
    );

    // A file created afterwards is the case the design's rows did cover; it must
    // keep working either way.
    std::fs::write(&created_later, b"after the grant").expect("write");
    assert!(acl::has_ace(&created_later, &sid).unwrap());

    grants.rollback().expect("roll back");
    // Read the DACL text itself rather than asking our own predicate: an
    // assertion that rests on `has_ace` alone would also pass for a path whose
    // DACL could not be read, and the point of the rollback is the operator's
    // directory, not this crate's view of it.
    assert!(
        !icacls(&root, &[]).contains(&sid_text),
        "the package ACE is gone"
    );
    assert!(
        !icacls(&existing, &[]).contains(&sid_text),
        "the pre-existing file carries no residue either"
    );
    // And the predicate agrees with the oracle, which is what lets the suites
    // that have no oracle of their own rely on it.
    assert!(!acl::has_ace(&root, &sid).unwrap());
}

#[test]
fn a_grant_leaves_a_foreign_ace_alone_and_the_rollback_removes_exactly_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("create the root");
    // A foreign ACE the run must not touch, in the shape the verified row
    // describes: `Users` present, package SID absent and therefore denied.
    let granted = std::process::Command::new(acl::icacls_binary())
        .arg(&root)
        .args(["/grant", r"*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users");
    assert!(granted.status.success(), "{}", String::from_utf8_lossy(&granted.stderr));

    let sid = sid_for(&root);
    let mut grants = acl::Grants::new(Arc::new(sid_for(&root)));
    grants.apply(&root, AccessMode::ReadWrite).expect("grant");
    assert!(acl::has_ace(&root, &sid).unwrap(), "the grant landed");

    grants.rollback().expect("roll back");
    let after = icacls(&root, &[]);
    assert!(!after.contains(&sid.as_str().to_string()), "the package ACE is gone");
    assert!(
        after.contains("S-1-5-32-545"),
        "the Users ACE survived the rollback"
    );
}

#[test]
fn a_rollback_with_nothing_granted_is_a_no_op() {
    let dir = tempfile::tempdir().expect("temp dir");
    let mut grants = acl::Grants::new(Arc::new(sid_for(dir.path())));
    assert!(grants.is_empty());
    grants.rollback().expect("an empty rollback succeeds");
}

#[test]
fn revoking_an_absent_ace_succeeds() {
    // Idempotence is what lets the rollback run over paths whose grant never
    // landed, and what makes a crashed run's residue removable twice.
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = sid_for(dir.path());
    acl::revoke(dir.path(), &sid).expect("first revoke");
    acl::revoke(dir.path(), &sid).expect("second revoke");
}

#[test]
fn dropping_the_grants_removes_them() {
    // The guard is the path a panic takes; a leaked ACE there would outlive the
    // process.
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = Arc::new(sid_for(dir.path()));
    {
        let mut grants = acl::Grants::new(Arc::clone(&sid));
        grants.apply(dir.path(), AccessMode::ReadOnly).expect("grant");
        assert!(acl::has_ace(dir.path(), &sid).unwrap());
    }
    // The oracle, not the predicate: this assertion is about the operator's
    // directory, which is the thing the guard is supposed to have restored.
    assert!(!icacls(dir.path(), &[]).contains(sid.as_str()));
}
```

- [ ] **Step 6: Run the test to verify it fails, then make it pass**

Run: `cargo test -p tinio-sandbox-windows --test acl`
Expected: FAIL first (the module is there but the helper functions are not yet compiled in), then PASS — five tests. **The inheritance question this test exists for is already answered.** It was measured on 2026-09-19, on NTFS (`C:`) and ReFS (`E:`) alike, before this plan was executed: a directory-only `icacls /grant:r "*<sid>:(OI)(CI)M"` reaches a pre-existing child at depth 1 *and* at depth 2, as an inherited `(I)(M)` entry, and a directory-only `/remove:g` takes every inherited copy back. So the assertions below are a **regression test for a settled answer**, not a branch — there is no `/t` walk to switch to and no per-file rollback to design, and §6 of the design and its evidence table carry the measurement. **The same measurement is also the cost**, which the test makes visible rather than merely records: "a directory-only `/remove:g` takes every inherited copy back" is one `icacls` call walking every object under the directory, and the grant did the same on the way in. Neither the tool nor this crate walks, the filesystem does, and the bill is proportional to the served tree — Task 13 Step 3 measures it, and the module doc above states it as what it is. One outcome is still worth recording in the commit body if it happens: if `has_ace`'s oracle fails because this host's `icacls` renders an AppContainer SID as a name rather than the `S-1-15-2-…` string, move the assertion to a before/after ACL comparison and say so.

- [ ] **Step 7: Check the test against the answer the design already records**

`docs/superpowers/specs/2026-09-18-sandbox-design.md` already carries the measurement — the evidence table's inheritance rows and §6's settled paragraph were written on 2026-09-19 from the same experiment this task's test reproduces. What is left here is to confirm the test asserts what those rows claim: the pre-existing file at depth 1, the inherited `(I)` form, a file created *after* the grant, and a directory-only rollback that leaves no residue. If this run disagrees with the design, the design is what changes, in this commit.

---

### Task 5: The Job object

**Files:**
- Create: `crates/tinio-sandbox-windows/src/job.rs`
- Create: `crates/tinio-sandbox-windows/tests/job.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`

**Interfaces:**
- Consumes: `handles::{last_error, step}`, `ffi`.
- Produces: `job::Job` with `create() -> Result<Job>`, `assign(&self, process: Handle) -> Result<()>`, `terminate(&self, code: u32) -> Result<()>` and `Drop`.

`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` is decision 20's first half: killing the launcher takes the confined server with it instead of leaving an orphan holding a listener. Its second half — the stop relay — is Task 10.

**What the job does not cover is the window between the two calls that create it, and that window is recorded here rather than claimed away.** The child is created running (`CreateProcessW` without `CREATE_SUSPENDED`) and only joins the job when `AssignProcessToJobObject` returns, so a launcher that dies inside that window leaves a child the job never owned — and the same is true, for the same reason, of a failure in either call, which Task 9 handles by terminating the process on both. The window could be closed with `CREATE_SUSPENDED` plus a `ResumeThread` on the thread handle the launch closes today; it is not closed here because what the child does with the window is bounded and already designed: it has inherited the listener and the two pipe ends but **no payload**, so its stdin read reaches EOF, `adopt` reports the launcher case, and it becomes a launcher inside its own confinement — which fails closed at its first grant or bind (design §11's "payload absent or malformed" row, decision 17). What a caller must not conclude from the job is that the child is never unowned; what they can conclude is that an unowned child is a child that cannot serve.

- [ ] **Step 1: Write the failing test**

`crates/tinio-sandbox-windows/tests/job.rs`:

```rust
//! The Job object, tested against an unconfined child: the primitive is
//! independent of the AppContainer, and a failure here is then unambiguous.

#![cfg(windows)]

use std::os::windows::io::AsRawHandle;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tinio_sandbox_windows::{handles::Handle, job::Job};

/// The raw process handle behind a `std::process::Child`.
fn raw_handle(child: &Child) -> Handle {
    child.as_raw_handle().cast()
}

/// A child that comfortably outlives the test.
fn sleeper() -> Child {
    Command::new("cmd.exe")
        .args(["/c", "ping -n 60 127.0.0.1 > nul"])
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn a child that outlives the test")
}

/// Wait for `child` to be gone, or fail loudly.
fn expect_death(child: &mut Child, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match child.try_wait().expect("poll the child") {
            Some(_) => return,
            None if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(50)),
            None => panic!("the child outlived {what}"),
        }
    }
}

#[test]
fn dropping_the_job_kills_the_process_it_owns() {
    let mut child = sleeper();
    let job = Job::create().expect("create the job");
    job.assign(raw_handle(&child)).expect("assign the child");
    // The job closes here; KILL_ON_JOB_CLOSE takes the child with it.
    drop(job);
    expect_death(&mut child, "the job's close");
}

#[test]
fn terminating_the_job_kills_the_process() {
    let mut child = sleeper();
    let job = Job::create().expect("create the job");
    job.assign(raw_handle(&child)).expect("assign the child");
    job.terminate(1).expect("terminate the job");
    expect_death(&mut child, "TerminateJobObject");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-windows --test job`
Expected: FAIL — `cannot find struct 'Job'`.

- [ ] **Step 3: Write the implementation**

```rust
//! The Job object that ties the confined child's lifetime to the launcher's
//! (design decision 20, first half).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14).

#![allow(unsafe_code)]

use std::ffi::c_void;
use std::ptr;

use tinio_sandbox_core::Result;

use crate::ffi::{self, Handle, JobObjectBasicLimitInformation, JobObjectExtendedLimitInformation};
use crate::handles::{last_error, step};

/// A job object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` set.
///
/// Holding the job for the child's whole lifetime is the point: when the
/// launcher exits — cleanly, by panic, or by `TerminateProcess` — the kernel
/// closes this handle, the last handle to the job goes with it, and the confined
/// server dies rather than surviving as an orphan holding a listener.
pub struct Job {
    handle: Handle,
}

impl Job {
    /// Create a job with kill-on-close set.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) naming the failing
    /// call.
    pub fn create() -> Result<Self> {
        // SAFETY: a null security descriptor and a null name are both documented
        // as valid, and mean "unnamed, default security".
        let handle = unsafe { ffi::CreateJobObjectW(ptr::null(), ptr::null()) };
        if handle.is_null() {
            return Err(step("CreateJobObjectW", "job", last_error()));
        }
        let job = Self { handle };
        let limits = JobObjectExtendedLimitInformation {
            basic_limit_information: JobObjectBasicLimitInformation {
                limit_flags: ffi::JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                ..Default::default()
            },
            ..Default::default()
        };
        // SAFETY: `handle` is the job just created, `limits` is a correctly
        // sized JOBOBJECT_EXTENDED_LIMIT_INFORMATION that outlives the call, and
        // the class matches the structure passed.
        let ok = unsafe {
            ffi::SetInformationJobObject(
                handle,
                ffi::JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                (&limits as *const JobObjectExtendedLimitInformation).cast::<c_void>(),
                std::mem::size_of::<JobObjectExtendedLimitInformation>() as u32,
            )
        };
        if ok == 0 {
            return Err(step("SetInformationJobObject", "job", last_error()));
        }
        // `info!`: the job is one of the two mechanisms that make the confinement
        // a *lifetime* rather than a permission, so it belongs on the "to what"
        // side of shared §7's split.
        tracing::info!(step = "CreateJobObjectW", limit = "JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE");
        Ok(job)
    }

    /// Assign `process` to the job.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the assignment
    /// is refused. **The caller must terminate the process in that case** — a
    /// child outside the job is one the launcher cannot guarantee to take with
    /// it — and Task 9's composition is the only caller: it calls
    /// [`crate::process::LaunchedProcess::terminate`] on this error and on a
    /// failure to create the job at all, before it reports either.
    pub fn assign(&self, process: Handle) -> Result<()> {
        // SAFETY: both handles are live and owned by the caller for the duration
        // of the call; the job takes no ownership of the process handle.
        let ok = unsafe { ffi::AssignProcessToJobObject(self.handle, process) };
        if ok == 0 {
            return Err(step("AssignProcessToJobObject", "job", last_error()));
        }
        tracing::info!(step = "AssignProcessToJobObject", process = ?process);
        Ok(())
    }

    /// Terminate every process in the job.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the call is
    /// refused.
    pub fn terminate(&self, code: u32) -> Result<()> {
        // SAFETY: `self.handle` is the job this value owns.
        let ok = unsafe { ffi::TerminateJobObject(self.handle, code) };
        if ok == 0 {
            return Err(step("TerminateJobObject", "job", last_error()));
        }
        Ok(())
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: `self.handle` is this value's own job handle, closed once.
        unsafe { ffi::CloseHandle(self.handle) };
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p tinio-sandbox-windows --test job`
Expected: PASS — two tests, each within a few seconds.

---

### Task 6: The listener handoff

**Files:**
- Create: `crates/tinio-sandbox-windows/src/socket.rs`
- Create: `crates/tinio-sandbox-windows/tests/listener.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`

**Interfaces:**
- Consumes: `handles::{mark_inheritable as mark_handle_inheritable, step}`, `ffi`.
- Produces: `socket::{value, handle, mark_inheritable, from_value}`.

The parent binds, the child serves on a socket it never created (decision 16). This is a convenience for the port, not an isolation mechanism: the AppContainer's network policy is enforced against the process's package SID, not against whoever created the socket. Nothing in this plan depends on the handoff for security, and the LAN question the design leaves open — whether such a listener reaches the network — is recorded by Task 12, never assumed.

- [ ] **Step 1: Write the failing test**

`crates/tinio-sandbox-windows/tests/listener.rs`:

```rust
//! The inherited listener: a child rebuilds the parent's socket from a handle
//! value and serves on it. No AppContainer here — this test pins the handle
//! plumbing on its own so a failure is unambiguous; the confined variant is in
//! `tests/confine.rs`.

#![cfg(windows)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;

#[test]
fn child_role() {
    if std::env::var_os("TINIO_SANDBOX_TEST_LISTENER_CHILD").is_none() {
        return;
    }
    let raw: u64 = std::env::var("TINIO_SANDBOX_TEST_LISTENER")
        .expect("handle value")
        .parse()
        .expect("a handle value");
    let listener = tinio_sandbox_windows::socket::from_value(raw).expect("rebuild the listener");
    let (mut stream, _) = listener.accept().expect("accept");
    let mut buffer = [0u8; 4];
    stream.read_exact(&mut buffer).expect("read the ping");
    assert_eq!(&buffer, b"ping");
    stream.write_all(b"pong").expect("write the pong");
}

#[test]
fn an_inherited_handle_is_the_same_socket_in_the_child() {
    if std::env::var_os("TINIO_SANDBOX_TEST_LISTENER_CHILD").is_some() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("address");
    tinio_sandbox_windows::socket::mark_inheritable(&listener).expect("mark inheritable");

    let child = Command::new(std::env::current_exe().expect("current exe"))
        .args(["child_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_LISTENER_CHILD", "1")
        .env(
            "TINIO_SANDBOX_TEST_LISTENER",
            tinio_sandbox_windows::socket::value(&listener).to_string(),
        )
        .spawn()
        .expect("spawn the child");

    let mut stream = TcpStream::connect(address).expect("connect through the parent's socket");
    stream.write_all(b"ping").expect("write");
    let mut buffer = [0u8; 4];
    stream.read_exact(&mut buffer).expect("read");
    assert_eq!(&buffer, b"pong");

    let status = child.wait_with_output().expect("wait");
    assert!(status.status.success(), "the child role failed: {status:?}");
}
```

A `Command`-spawned child inherits all inheritable handles (std sets `bInheritHandles`), which is why this test needs no handle list — the list belongs to the confined path, and Task 8 covers it.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-windows --test listener`
Expected: FAIL — `cannot find function 'from_value'`.

- [ ] **Step 3: Write the implementation**

```rust
//! The inherited listener: the parent binds it, the child serves on it (design
//! decision 16).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14). A socket that crosses a process boundary is a raw handle, and rebuilding
//! a `TcpListener` from one is unsafe by construction.

#![allow(unsafe_code)]

use std::net::TcpListener;
use std::os::windows::io::{AsRawSocket, FromRawSocket, RawSocket};

use tinio_sandbox_core::Result;

use crate::ffi::Handle;
use crate::handles::{mark_inheritable as mark_handle_inheritable, step};

/// The handle value the launch payload carries (design decision 17).
///
/// Handle values survive inheritance unchanged — the probe's parent had 1040 and
/// the child saw 1040 — which is why the payload can name one at all.
///
/// # Examples
///
/// ```
/// use std::net::TcpListener;
///
/// use tinio_sandbox_windows::socket;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// assert!(socket::value(&listener) > 0);
/// ```
pub fn value(listener: &TcpListener) -> u64 {
    listener.as_raw_socket() as u64
}

/// The listener as a raw handle, for the handle list the child inherits.
///
/// The same value as [`value`], typed the way `CreateProcessW` and
/// `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` want it: a socket *is* a handle on
/// Windows, which is why the listener can be passed at all (decision 16).
///
/// # Examples
///
/// ```
/// use std::net::TcpListener;
///
/// use tinio_sandbox_windows::socket;
///
/// let listener = TcpListener::bind("127.0.0.1:0").unwrap();
/// assert_eq!(socket::handle(&listener) as u64, socket::value(&listener));
/// ```
pub fn handle(listener: &TcpListener) -> Handle {
    listener.as_raw_socket() as Handle
}

/// Mark the listener inheritable.
///
/// Required, not defensive: Windows 8 and later create sockets non-inheritable,
/// so a handle list cannot name one without this call (design evidence table).
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when
/// `SetHandleInformation` refuses.
pub fn mark_inheritable(listener: &TcpListener) -> Result<()> {
    mark_handle_inheritable(listener.as_raw_socket() as Handle)
        .map_err(|source| step("mark the listener inheritable", "listener", source))
}

/// Rebuild the listener from the handle value a launcher passed.
///
/// The handle is trusted rather than validated: only a launcher can write the
/// payload that names it (design decision 17), and the child that calls this is
/// the process that inherited it.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when Winsock cannot be
/// initialised — the only thing here that can fail.
pub fn from_value(raw: u64) -> Result<TcpListener> {
    initialize_winsock()?;
    // SAFETY: this is the confined child's path. The handle came from the launch
    // payload the launcher wrote, this process inherited it, and nothing else
    // here has touched it, so the value is a live listening socket no other
    // value in this process owns.
    Ok(unsafe { TcpListener::from_raw_socket(raw as RawSocket) })
}

/// Trigger Rust's lazy Winsock initialisation.
///
/// `std` initialises Winsock on the first socket it *creates*, and a rebuilt
/// socket is not created by `std`: without this, the first Winsock call on it
/// fails with `WSANOTINITIALISED`. One loopback bind-and-drop is enough, and a
/// loopback bind needs no capability inside the container (design evidence
/// table).
fn initialize_winsock() -> Result<()> {
    TcpListener::bind(("127.0.0.1", 0))
        .map(drop)
        .map_err(|source| step("initialise Winsock", "127.0.0.1:0", source))
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p tinio-sandbox-windows --test listener`
Expected: PASS — one round trip through a socket the parent created and the child serves on.

---

### Task 7: The child's adoption, the listener rebuild and the stop hook

**Files:**
- Create: `crates/tinio-sandbox-windows/src/launch.rs`
- Create: `crates/tinio-sandbox-windows/tests/payload.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`

**Interfaces:**
- Consumes: core's `adopt(args: impl IntoIterator<Item = OsString>, stdin: impl Read) -> Result<Option<Adopted>>`, `Adopted { config, listener_fd: Option<i64> }`, and the element helpers `LISTENER_FD_PREFIX`, `listener_fd_arg`, `is_listener_fd_arg`, `listener_fd_from_arg`. The payload's serialization is **not** adopted here: the core's `adopt` does it with `serde_json`, and this crate never sees the bytes.
- Produces: `launch::{adopt, listener, command_args, on_stop, disarm}`. The payload write is **not** here: it is `handles::write_payload(writer, bytes)`, and the serialization is `serde_json`'s at the call site — a wrapper that only re-spelled those two would be a name without a job.

The payload arrives on the child's **stdin** (decision 17): the standard streams are inherited already, so the frame needs no descriptor of its own, no entry in the handle list, and no rebuild-from-raw-handle on the child's side. The child consumes stdin once at startup and never needs it again, which is exactly what `serve` gives it.

**The adoption rule is the core's; the rebuild is this crate's.** Core's `adopt` takes the arguments and the stdin reader as parameters — so the whole rule (no element, element with an undecodable payload, element with a good one) is unit-testable without a process — and it returns `Adopted { config, listener_fd }`: the configuration **text** the parent forwarded, and the listener as a **descriptor** rather than a socket. Turning that descriptor into a `std::net::TcpListener` is the platform's half, because `from_raw_socket` is unsafe and belongs in exactly one module (decision 14); this crate therefore wraps a call to core's rule and adds `launch::listener`, and nothing else about adoption is Windows-specific.

**The argv element survives, carrying something else.** `tinio-sandbox-fd=<n>` names the inherited **listener** descriptor, because a socket cannot ride on a stream — the descriptor-passing half of decision 16 is irreducible whatever the payload does. The helpers live in `tinio-sandbox-core` and are shared with the macOS plan, so this crate consumes them and defines none of its own.

- [ ] **Step 1: Write the failing test**

`crates/tinio-sandbox-windows/tests/payload.rs`:

```rust
//! The launch payload: a parent writes a frame to the child's stdin and hands
//! the listener over by naming its handle in argv.

#![cfg(windows)]

use std::ffi::OsString;
use std::io::Write;
use std::process::{Command, Stdio};

use tinio_sandbox_core::{LaunchPayload, is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg};
use tinio_sandbox_windows::{handles::Handle, launch, socket};

#[test]
fn child_role() {
    if std::env::var_os("TINIO_SANDBOX_TEST_PAYLOAD_CHILD").is_none() {
        return;
    }
    let adopted = launch::adopt(std::env::args_os(), std::io::stdin().lock())
        .expect("adopt")
        .expect("a payload arrived on stdin");
    assert_eq!(adopted.config(), "version = 1\n");
    // The element named the listener's handle and `adopt` handed back the
    // descriptor; rebuilding the socket is this crate's half.
    assert!(adopted.listener_fd.is_some(), "the element named a listener");
    let listener = launch::listener(&adopted).expect("rebuild the listener");
    assert!(listener.local_addr().expect("a bound socket").port() > 0);
    let report = std::env::var("TINIO_SANDBOX_TEST_REPORT").expect("report path");
    std::fs::write(report, "payload-ok\n").expect("write the report");
}

#[test]
fn a_child_reads_the_frame_the_parent_wrote() {
    if std::env::var_os("TINIO_SANDBOX_TEST_PAYLOAD_CHILD").is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let report = dir.path().join("report.txt");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    socket::mark_inheritable(&listener).expect("mark the listener inheritable");
    let element = listener_fd_arg(socket::value(&listener) as i64);

    let mut child = Command::new(std::env::current_exe().expect("current exe"))
        .args(["child_role", "--exact", &element.to_string_lossy()])
        .env("TINIO_SANDBOX_TEST_PAYLOAD_CHILD", "1")
        .env("TINIO_SANDBOX_TEST_REPORT", &report)
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn the child");

    let frame = serde_json::to_vec(&LaunchPayload { config: "version = 1\n".into() })
        .expect("the payload serializes");
    child
        .stdin
        .take()
        .expect("the child's stdin")
        .write_all(&frame)
        .expect("write the frame");
    // Dropping the handle closes the pipe, which is what makes the child's read
    // of stdin reach EOF (design §3 step 7).

    let status = child.wait().expect("wait");
    assert!(status.success(), "the child role failed: {status:?}");
    assert_eq!(std::fs::read_to_string(&report).unwrap(), "payload-ok\n");
}

#[test]
fn a_process_with_no_element_is_not_a_confined_child() {
    // The discriminator: no element means no launcher expected this process to be
    // confined, and stdin is never read — which is what keeps an ordinary
    // launcher from blocking on its own console.
    let adopted = launch::adopt(std::env::args_os(), std::io::empty()).unwrap();
    assert!(adopted.is_none());
    // `OsString`s and not `String`s: the rule takes a process's own arguments,
    // and a path that is not valid UTF-8 is a legal argument on Windows.
    let adopted = launch::adopt(
        [OsString::from("serve"), OsString::from("/srv/data")],
        std::io::empty(),
    )
    .unwrap();
    assert!(adopted.is_none());
}

#[test]
fn the_element_carries_a_listener_descriptor() {
    let element = listener_fd_arg(1040);
    assert_eq!(element, "tinio-sandbox-fd=1040");
    assert!(is_listener_fd_arg(&element));
    assert_eq!(listener_fd_from_arg(&element), Some(1040));
    assert!(!is_listener_fd_arg(std::ffi::OsStr::new("--port")));
    // The launcher's own arguments survive untouched, minus the element the
    // child consumes itself. `args` is what follows `argv[0]` — what
    // `std::env::args_os().skip(1)` hands over — and the cast stands in for the
    // listener's handle, whose value is the only thing the element carries.
    let args = launch::command_args(
        1040usize as Handle,
        &["/srv/data".into(), "--port".into(), "0".into()],
    );
    assert_eq!(args[0], element, "the element comes first");
    let kept: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|arg| !is_listener_fd_arg(arg))
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert_eq!(kept, ["/srv/data", "--port", "0"]);
}
```

The first case passes `std::io::empty()` as the stdin: core's rule must not read it when there is no element, and `empty()` cannot block — so the test would hang rather than pass if the rule were wrong about that.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-windows --test payload`
Expected: FAIL — `cannot find function 'listener'` in the crate's `launch` module.

- [ ] **Step 3: Write `launch.rs`**

```rust
//! The launcher's own plumbing: the payload write into the child's stdin, the
//! element that names the inherited listener, the child's adoption of both, and
//! the child-side stop hook.
//!
//! P0's core owns the payload frame, the event type, the relay's decode-and-
//! re-emit (`Observer`, whose relay is the core's), the adoption rule, the argv element helpers and the
//! supervision loop; the program, the argv, the two std-handle pipes, the
//! listener rebuild and the thread that drives the relay are what only a real
//! launcher needs, so they live here (and in the macOS plan's twin of this
//! module).

use std::ffi::OsString;
use std::io::Read;
use std::net::TcpListener;

use tinio_sandbox_core::{
    Adopted, Error, LaunchPayload, Result, listener_fd_arg,
};

use crate::handles::{self, Handle};
use crate::socket;

/// Adopt the launch payload, if this process inherited one.
///
/// Core's rule, called on this process's arguments and stdin: `Ok(None)` means
/// this process is the launcher. The rule itself (which element triggers a read,
/// what a bad frame means) is the core's and is unit-tested there; what this
/// wrapper adds is only that the two inputs are *this* process's.
///
/// # Errors
///
/// None of its own: the rule is the core's, and this wrapper only supplies *this*
/// process's argv and stdin. Core's `adopt` reads stdin only when the element is
/// present, and treats a read that fails or a frame that does not decode as the
/// launcher case — `Ok(None)` — which inside a confinement fails closed at the
/// next grant or bind.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// let adopted = launch::adopt(std::env::args_os(), std::io::stdin().lock())?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    stdin: impl Read,
) -> Result<Option<Adopted>> {
    tinio_sandbox_core::adopt(args, stdin)
}

/// Rebuild the listener core's adoption handed back as a descriptor.
///
/// # Errors
///
/// [`Error::Launch`] when the launch named no listener: a production launcher
/// always binds one (design §10 step 6), so this reports a launch that did not
/// come from the launcher path rather than fabricating a socket.
pub fn listener(adopted: &Adopted) -> Result<TcpListener> {
    let fd = adopted.listener_fd.ok_or(Error::Launch {
        operation: "rebuild the listener",
        path: "argv".into(),
        source: std::io::Error::other("the launch named no listener descriptor"),
    })?;
    socket::from_value(fd as u64)
}

/// The child's arguments: the listener element first, then the launcher's own
/// arguments in order.
///
/// `args` is what follows `argv[0]` — `std::env::args_os().skip(1)` in production,
/// `Options::args` under test — and the program is not among them: `command_line`
/// writes this process's own executable as the child's `argv[0]`, so the element
/// lands at index 1 of that line and the launcher's arguments keep their order
/// after it (design §3 step 1: the child reaches the same root and the same flags
/// by the same path).
///
/// **The launcher's side of that contract is `skip(1)` and nothing else.** A
/// caller that handed over its whole `args_os()` would put the launcher's own
/// executable path where the served root belongs — the child drops its `argv[0]`
/// and filters the element, so what is left is parsed as the launcher's
/// arguments.
pub fn command_args(listener: Handle, args: &[OsString]) -> Vec<OsString> {
    let mut child = Vec::with_capacity(args.len() + 1);
    child.push(listener_fd_arg(listener as i64));
    child.extend(args.iter().cloned());
    child
}

/// Run `handler` when the launcher relays a termination signal (design §3 step
/// 9).
///
/// The child-side counterpart of the relay: without it a forwarded stop has
/// nowhere to land, and the child dies on the spot rather than draining. The
/// handler runs off the control-handler thread, so it may take locks, allocate
/// and talk to a runtime — which is what makes this the hook the CLI's graceful
/// shutdown (T066/T067) lands on.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the hook cannot be
/// armed.
pub fn on_stop(handler: impl Fn() + Send + 'static) -> Result<()> {
    crate::control::on_stop(handler)
}

/// Unhook the stop relay.
///
/// The launcher and the child each arm their own handler; a process that lives
/// past one launch — the test harness, and a future CLI that serves twice —
/// disarms before the next one, so a stale handler cannot forward a stop to a
/// child that is already gone.
///
/// **A disarmed process is back on the default disposition**, which is the half
/// of this that "relayed nowhere" would leave out: the handler is removed rather
/// than left installed, so a later `CTRL_C` or `CTRL_BREAK` stops the process the
/// way it would stop one that never armed a relay, instead of being swallowed by
/// a handler that has nothing left to relay — the handler answers `TRUE`, which
/// means *handled*, so it would go on eating the close, logoff and shutdown
/// events that normally end a console session ([`crate::control::disarm`]).
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the handler cannot
/// be removed.
pub fn disarm() -> Result<()> {
    crate::control::disarm()
}

/// Re-export the core's element helpers so the child side of the call site reads
/// symmetrically, consuming the core's names rather than aliasing them.
pub use tinio_sandbox_core::{LISTENER_FD_PREFIX, is_listener_fd_arg, listener_fd_from_arg};
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p tinio-sandbox-windows --test payload`
Expected: FAIL to compile until Task 10 provides `control::{on_stop, disarm}`; comment those two calls out and leave the rest passing, then restore them in Task 10. Everything else — the element, the stdin frame, the listener rebuild — is complete and tested now, which is what the task is for.

---

### Task 8: The AppContainer launch

**Files:**
- Create: `crates/tinio-sandbox-windows/src/process.rs`
- Create: `crates/tinio-sandbox-windows/tests/launch.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (§11's error table gains the refused-attribute row)

**Interfaces:**
- Consumes: `ffi`, `handles::{last_error, step, inherited_stderr, Pipe, pipe, parent_end, write_payload}`, `sid::CapabilitySids`, `launch::command_args`.
- Produces: `process::command_line(program: &Path, args: &[OsString]) -> Vec<u16>`, `process::Container { package_sid, capabilities }`, `process::LaunchSpec { program, args, stdin, stdout, stderr, inherited, current_dir }`, `process::launch(spec, container) -> Result<LaunchedProcess>`, `LaunchedProcess::{handle, pid, wait, terminate}` and `Drop`.

This is the step the design's evidence table measured end to end: a Rust binary runs inside an AppContainer, and the capability and handle-list attributes are what put it there.

**Every attribute set is checked and fatal, and that is the whole of the confinement, not a belt beside it.** Measured on this host: an `UpdateProcThreadAttribute` handed a truncated `cbSize` fails with `err=87 ERROR_INVALID_PARAMETER` and sets *no* attribute at all — and `CreateProcessW` still succeeds, returning a child that runs **completely unconfined**. So the error `UpdateProcThreadAttribute` reports is the only signal that the container was never applied: nothing downstream notices, the child starts, serves and looks exactly like a confined one from the outside. Both calls below are therefore checked, the error is fatal, and it is reported through `step` with the attribute's own name so the log says which half of the container is missing. The design's §11 error table gains the row, because "the launch failed" and "the launch succeeded without the confinement" were previously the same row.

Three things are P2's additions to that measured shape: `CREATE_NEW_PROCESS_GROUP`, without which the term relay of Task 10 has nothing to address; the standard-error handle in the list, without which the confined child has no log; and the two std-handle pipes (Task 9's, supplied here), which are what decision 17's stdin payload and decision 19's stdout event stream ride on.

- [ ] **Step 1: Write the failing quoting tests**

`crates/tinio-sandbox-windows/src/process.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::Path;

    use super::*;

    /// Parse `line` with the API the child's own runtime uses.
    fn parse(line: &[u16]) -> Vec<String> {
        let mut argc = 0i32;
        // SAFETY: `line` is NUL-terminated and alive for the call; the returned
        // argv block is freed below with LocalFree.
        let argv = unsafe { ffi::CommandLineToArgvW(line.as_ptr(), &mut argc) };
        assert!(!argv.is_null(), "CommandLineToArgvW refused the line");
        let mut parsed = Vec::new();
        for index in 0..argc as isize {
            // SAFETY: argv holds argc entries, each a NUL-terminated string owned
            // by the block returned above.
            let entry = unsafe { *argv.offset(index) };
            parsed.push(unsafe { crate::from_wide(entry) });
        }
        // SAFETY: the block came from CommandLineToArgvW, which documents
        // LocalFree as its release.
        unsafe { ffi::LocalFree(argv.cast()) };
        parsed
    }

    #[test]
    fn the_command_line_round_trips_through_the_c_parser() {
        // The quoting must be the CommandLineToArgvW convention, because that is
        // what the child's argv is built from. Testing against the parser itself
        // beats hand-written expectations.
        let args: Vec<OsString> = [
            "",
            "plain",
            "with space",
            "with\ttab",
            r#"with"quote"#,
            r#"ends with backslash\"#,
            r#"trailing\"#,
            r#"\\"#,
            r#"a\"b"#,
            r#"\\\"#,
            "中文参数",
            "tinio-sandbox-fd=1040",
        ]
        .iter()
        .map(OsString::from)
        .collect();
        let line = command_line(Path::new(r"C:\Program Files\tinio\serve.exe"), &args);
        let parsed = parse(&line);
        assert_eq!(parsed[0], r"C:\Program Files\tinio\serve.exe");
        let expected: Vec<String> = args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(&parsed[1..], &expected[..], "round trip through CommandLineToArgvW");
    }

    #[test]
    fn the_command_line_is_nul_terminated() {
        let line = command_line(Path::new(r"C:\s.exe"), &[]);
        assert_eq!(line.last(), Some(&0));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows process::`
Expected: FAIL — `cannot find function 'command_line'`.

- [ ] **Step 3: Write `process.rs`**

```rust
//! Launching the confined child: the attribute list, the handle list and
//! `CreateProcessW` (design §3 step 6).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14). Its invariants are the ones the evidence table measured:
//! `SID_AND_ATTRIBUTES` is 16 bytes on x64 (a stride of 8 returns
//! `ERROR_INVALID_PARAMETER`), `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` requires
//! `bInheritHandles`, and every value named by `UpdateProcThreadAttribute`
//! outlives the attribute list, because the kernel reads it again at
//! `CreateProcessW` time rather than copying it.

#![allow(unsafe_code)]

use std::ffi::{OsStr, OsString, c_void};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::process::ExitStatusExt;
use std::path::Path;
use std::process::ExitStatus;
use std::ptr;

use tinio_sandbox_core::{Error, Result};

use crate::ffi::{self, Handle, Psid, SecurityCapabilities, StartupInfoExW, StartupInfoW};
use crate::handles::{last_error, step};
use crate::sid::CapabilitySids;

/// The AppContainer a child is created in.
pub struct Container {
    /// The package SID, kept alive by the caller until the launch returns.
    pub package_sid: Psid,
    /// The capability SIDs. Empty is the default, and the configuration the
    /// outbound guarantee rests on (design §7).
    pub capabilities: CapabilitySids,
}

/// What the launcher hands to `CreateProcessW`.
pub struct LaunchSpec<'a> {
    /// The executable, as an absolute path. Also `argv[0]`.
    pub program: &'a Path,
    /// The child's arguments, including the inherited listener's element.
    pub args: &'a [OsString],
    /// The child's standard input: the read end of the launcher's payload pipe
    /// (decision 17).
    pub stdin: Handle,
    /// The child's standard output: the write end of the launcher's event pipe
    /// (decision 19).
    pub stdout: Handle,
    /// The child's standard error: the launcher's own, inherited untouched.
    pub stderr: Handle,
    /// The extra handles the child inherits, on top of its standard handles —
    /// the inherited listener.
    pub inherited: &'a [Handle],
    /// The child's working directory; `None` inherits the launcher's.
    pub current_dir: Option<&'a Path>,
}

/// The child process, alive until [`LaunchedProcess::wait`] or `Drop`.
pub struct LaunchedProcess {
    process: Handle,
    pid: u32,
}

impl LaunchedProcess {
    /// The process handle, for [`crate::job::Job::assign`].
    pub fn handle(&self) -> Handle {
        self.process
    }

    /// The child's process id.
    ///
    /// Also its **process-group id**, because the child is created with
    /// `CREATE_NEW_PROCESS_GROUP`: that is the value the relay's
    /// `GenerateConsoleCtrlEvent` addresses (design §3 step 9).
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Wait for the child and return its exit status.
    ///
    /// The status is the core's currency: `supervise` forwards it and the call
    /// site exits with it (design §3 step 8). It is not the same obligation as
    /// the relay of §3 step 9 — that one runs *while* this waits.
    ///
    /// **`INFINITE`, so what bounds this call is the child's lifetime rather than
    /// any deadline of the launcher's.** A child that neither exits nor reports
    /// leaves the caller here for as long as that child lives: there is no status
    /// to forward, the call site's "exited without reporting" `Error` event is
    /// read only *after* a wait that has not returned, and the relay thread has
    /// nothing to re-emit while the child's stdout stays open — so the operator
    /// sees no failure, no readiness frame and no diagnostic. That window is
    /// recorded at the call site in Task 9 rather than closed here: a readiness
    /// deadline belongs to the readiness contract, not to this wait, and the
    /// design's §3 step 8 fixes the shape this function implements.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] when the wait or the exit-code query fails.
    pub fn wait(&mut self) -> Result<ExitStatus> {
        // SAFETY: `self.process` is a live process handle owned by this value for
        // the duration of the call.
        let waited = unsafe { ffi::WaitForSingleObject(self.process, ffi::INFINITE) };
        if waited != ffi::WAIT_OBJECT_0 {
            return Err(step("WaitForSingleObject", self.pid.to_string(), last_error()));
        }
        let mut code: u32 = 0;
        // SAFETY: as above, and `code` is a valid out-parameter.
        let ok = unsafe { ffi::GetExitCodeProcess(self.process, &mut code) };
        if ok == 0 {
            return Err(step("GetExitCodeProcess", self.pid.to_string(), last_error()));
        }
        // The other end of the launch's `info!` event: which child it was and how
        // it left, so a log read after the fact holds both halves of the pair
        // (shared §7).
        tracing::info!(step = "child exit", pid = self.pid, code = code);
        Ok(ExitStatus::from_raw(code))
    }

    /// Terminate the child now, without waiting for it.
    ///
    /// **Its one caller is Task 9's two job failures**, and it exists because
    /// neither of them may leave the child running: a child that is not in the
    /// job is one the launcher cannot guarantee to take with it, and the job is
    /// exactly what the two failed calls were building.
    /// [`crate::job::Job::assign`]'s own doc says the caller must terminate the
    /// process on a refused assignment; this is that call, made on a failure to
    /// create the job as well.
    ///
    /// `Drop` does not do this and must not. A `LaunchedProcess` dropped without
    /// a wait is either the failure path above — which has already terminated it
    /// — or a caller walking away from a child the job owns, where
    /// kill-on-close is the mechanism (decision 20, first half) and terminating
    /// here would be a second one.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] when the call is refused. The caller returns the failure
    /// that *led* here rather than this one — a launch that stopped must report
    /// why — and this error is not lost with it: [`step`] logged it where it was
    /// built, which is the rule shared §7 states for every failure in this crate.
    pub fn terminate(&self, code: u32) -> Result<()> {
        // SAFETY: `self.process` is a live process handle owned by this value,
        // and TerminateProcess takes an exit code and dereferences nothing.
        let ok = unsafe { ffi::TerminateProcess(self.process, code) };
        if ok == 0 {
            return Err(step("TerminateProcess", self.pid.to_string(), last_error()));
        }
        // A documented degradation rather than a step that went well: the launch
        // failed and a child had to be destroyed to keep the failure from leaving
        // a process behind (shared §7).
        tracing::warn!(step = "TerminateProcess", pid = self.pid, code = code);
        Ok(())
    }
}

impl Drop for LaunchedProcess {
    fn drop(&mut self) {
        // SAFETY: `self.process` is this value's own handle, closed once.
        // Not terminating here is deliberate — see `terminate`.
        unsafe { ffi::CloseHandle(self.process) };
    }
}

/// Build a command line from a program and its arguments.
///
/// The quoting is the `CommandLineToArgvW` convention — the one the child's own
/// runtime parses — and not a shell's: a backslash run before a quote is
/// doubled, and only an argument that needs quoting is quoted.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::process::command_line;
///
/// let line = command_line(Path::new(r"C:\s.exe"), &[]);
/// assert_eq!(line.last(), Some(&0));
/// ```
pub fn command_line(program: &Path, args: &[OsString]) -> Vec<u16> {
    let mut line = String::new();
    append_arg(&mut line, program.as_os_str());
    for arg in args {
        line.push(' ');
        append_arg(&mut line, arg);
    }
    line.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Append one argument, quoted the way `CommandLineToArgvW` unquotes it.
fn append_arg(line: &mut String, arg: &OsStr) {
    let text = arg.to_string_lossy();
    let needs_quotes = text.is_empty()
        || text
            .chars()
            .any(|c| matches!(c, ' ' | '\t' | '\u{0b}' | '"'));
    if !needs_quotes {
        line.push_str(&text);
        return;
    }
    line.push('"');
    let mut backslashes = 0usize;
    for ch in text.chars() {
        match ch {
            '\\' => {
                backslashes += 1;
                line.push('\\');
            }
            '"' => {
                // Double the pending backslashes, then escape the quote.
                for _ in 0..backslashes {
                    line.push('\\');
                }
                backslashes = 0;
                line.push_str("\\\"");
            }
            other => {
                backslashes = 0;
                line.push(other);
            }
        }
    }
    // A trailing backslash run would escape the closing quote.
    for _ in 0..backslashes {
        line.push('\\');
    }
    line.push('"');
}

/// Launch `program` inside `container`.
///
/// # Errors
///
/// [`Error::Launch`] naming the program and carrying the Win32 error, for a
/// refused attribute list, attribute, or `CreateProcessW`. The caller must roll
/// the ACLs back on every one of them (design §11).
pub fn launch(spec: &LaunchSpec<'_>, container: &Container) -> Result<LaunchedProcess> {
    let capabilities = SecurityCapabilities {
        app_container_sid: container.package_sid,
        capabilities: container.capabilities.as_ptr(),
        capability_count: container.capabilities.len(),
        reserved: 0,
    };

    // Two attributes: the container and the handle list.
    let mut size = 0usize;
    // SAFETY: a null list with a size out-parameter is the documented size query;
    // it fails with ERROR_INSUFFICIENT_BUFFER and fills `size`.
    let probed = unsafe { ffi::InitializeProcThreadAttributeList(ptr::null_mut(), 2, 0, &mut size) };
    if probed != 0 || size == 0 {
        return Err(step(
            "InitializeProcThreadAttributeList",
            spec.program,
            last_error(),
        ));
    }
    // An over-aligned buffer: the list requires `max_align_t` alignment, and a
    // `Vec<usize>` gives it without a manual allocator.
    let mut list = vec![0usize; size.div_ceil(std::mem::size_of::<usize>())];
    let list_ptr: *mut c_void = list.as_mut_ptr().cast();
    // SAFETY: `list_ptr` points at `size` bytes of suitably aligned storage that
    // outlives the list's use, as the API requires.
    let initialized = unsafe { ffi::InitializeProcThreadAttributeList(list_ptr, 2, 0, &mut size) };
    if initialized == 0 {
        return Err(step(
            "InitializeProcThreadAttributeList",
            spec.program,
            last_error(),
        ));
    }

    let result = (|| -> Result<LaunchedProcess> {
        // SAFETY: `list_ptr` is an initialized list, and `capabilities` is a
        // local that outlives the list's use.
        let ok = unsafe {
            ffi::UpdateProcThreadAttribute(
                list_ptr,
                0,
                ffi::PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
                (&capabilities as *const SecurityCapabilities as *mut SecurityCapabilities).cast(),
                std::mem::size_of::<SecurityCapabilities>(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(step(
                "set PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES",
                spec.program,
                last_error(),
            ));
        }

        // The child inherits its three standard streams and the listener, and
        // nothing else: `bInheritHandles` is TRUE because the handle list
        // requires it, and the list is what narrows that back down to these four.
        let mut handles: Vec<Handle> = Vec::with_capacity(spec.inherited.len() + 3);
        handles.extend_from_slice(spec.inherited);
        handles.push(spec.stdin);
        handles.push(spec.stdout);
        handles.push(spec.stderr);
        // SAFETY: as above: `handles` is a contiguous, live array that outlives
        // the list's use.
        let ok = unsafe {
            ffi::UpdateProcThreadAttribute(
                list_ptr,
                0,
                ffi::PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                handles.as_mut_ptr().cast(),
                handles.len() * std::mem::size_of::<Handle>(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(step(
                "set PROC_THREAD_ATTRIBUTE_HANDLE_LIST",
                spec.program,
                last_error(),
            ));
        }

        let startup = StartupInfoExW {
            startup_info: StartupInfoW {
                cb: std::mem::size_of::<StartupInfoExW>() as u32,
                lp_reserved: ptr::null_mut(),
                lp_desktop: ptr::null_mut(),
                lp_title: ptr::null_mut(),
                dw_x: 0,
                dw_y: 0,
                dw_x_size: 0,
                dw_y_size: 0,
                dw_x_count_chars: 0,
                dw_y_count_chars: 0,
                dw_fill_attribute: 0,
                dw_flags: ffi::STARTF_USESTDHANDLES,
                w_show_window: 0,
                cb_reserved2: 0,
                lp_reserved2: ptr::null_mut(),
                h_std_input: spec.stdin,
                h_std_output: spec.stdout,
                h_std_error: spec.stderr,
            },
            attribute_list: list_ptr,
        };

        let mut line = command_line(spec.program, spec.args);
        let application = wide_path(spec.program);
        let directory = spec.current_dir.map(wide_path);
        let mut info = ffi::ProcessInformation {
            process: ptr::null_mut(),
            thread: ptr::null_mut(),
            process_id: 0,
            thread_id: 0,
        };
        // SAFETY: every pointer above is live for the call — the command line and
        // path buffers are locals, the attribute list is initialized, and
        // `startup` names it. `bInheritHandles` is TRUE, which
        // PROC_THREAD_ATTRIBUTE_HANDLE_LIST requires. CREATE_NEW_PROCESS_GROUP
        // makes the child a group leader so the relay can address it (design §3
        // step 9), and it disables Ctrl+C for that group, which is why the relay
        // sends CTRL_BREAK.
        let created = unsafe {
            ffi::CreateProcessW(
                application.as_ptr(),
                line.as_mut_ptr(),
                ptr::null(),
                ptr::null(),
                1,
                ffi::EXTENDED_STARTUPINFO_PRESENT
                    | ffi::CREATE_UNICODE_ENVIRONMENT
                    | ffi::CREATE_NEW_PROCESS_GROUP,
                ptr::null_mut(),
                directory.as_ref().map_or(ptr::null(), |path| path.as_ptr()),
                &startup,
                &mut info,
            )
        };
        if created == 0 {
            return Err(step("CreateProcessW", spec.program, last_error()));
        }
        // `info!`, and the single most useful line in the file: the program and
        // the pid, which is also the child's process-group id (it is created with
        // CREATE_NEW_PROCESS_GROUP) and so the value the stop relay addresses. A
        // run whose log lacks this line confined nothing (shared §7).
        tracing::info!(
            step = "CreateProcessW",
            program = %spec.program.display(),
            pid = info.process_id
        );
        // The thread handle is not needed: the child runs on its own.
        // SAFETY: `info.thread` is the handle CreateProcessW just returned and
        // nothing else owns it.
        unsafe { ffi::CloseHandle(info.thread) };
        Ok(LaunchedProcess {
            process: info.process,
            pid: info.process_id,
        })
    })();
    // SAFETY: `list_ptr` is the list initialized above, deleted exactly once
    // here, after every value it named has been used.
    unsafe { ffi::DeleteProcThreadAttributeList(list_ptr) };
    result
}

/// A NUL-terminated UTF-16 copy of a path.
fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}
```

The `Error` import is used by `step`'s return type through `Result`; drop it from the `use` if the compiler does not ask for it.

- [ ] **Step 4: Run the quoting tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows process::`
Expected: PASS — two tests, the round trip being the real assertion.

- [ ] **Step 5: Write the failing integration test**

`crates/tinio-sandbox-windows/tests/launch.rs`:

```rust
//! The AppContainer launch: a Rust binary runs inside it, the whitelist holds,
//! and a program that does not exist fails with the path in the message.

#![cfg(windows)]

use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;

use tinio_sandbox_core::{AccessMode, LaunchPayload};
use tinio_sandbox_windows::{
    acl,
    handles, launch, socket,
    process::{self, Container, LaunchSpec},
    sid::{CapabilitySids, PackageSid, package_name},
};

/// The probe file the harness writes for the confined child to read.
///
/// **It goes into the report directory, and that is not cosmetic.** The child is
/// an AppContainer process: every read is checked against the package SID, and
/// the operator's own ACEs do not help it — so a probe written beside the
/// exposed tree (a sibling of `root`, `denied` and `report`) is a file the
/// confined child cannot open, and the role would panic on its first line
/// instead of doing anything the test could assert on. The report directory is
/// exposed read-write by every test here, which is what makes it the one place a
/// probe can live.
fn write_probe(root: &Path, denied: &Path, report_dir: &Path) -> std::path::PathBuf {
    let probe = report_dir.join("probe.txt");
    std::fs::write(
        &probe,
        format!(
            "root={}\ndenied={}\nreport={}\n",
            root.display(),
            denied.display(),
            report_dir.join("report.txt").display()
        ),
    )
    .expect("write the probe file");
    probe
}

#[test]
fn child_role() {
    // No payload means this process is the launcher, and the role is inert there
    // (design decision 17, exercised as written).
    let Some(adopted) = launch::adopt(std::env::args_os(), std::io::stdin().lock())
        .expect("adopt")
    else {
        return;
    };
    let probe_path = adopted.config().to_owned();
    let probe = std::fs::read_to_string(&probe_path).expect("read the probe file");
    let value = |key: &str| {
        probe
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")))
            .expect("a probe key")
            .to_string()
    };
    let granted = std::fs::read_to_string(Path::new(&value("root")).join("granted.txt"));
    let denied = std::fs::read_to_string(Path::new(&value("denied")).join("secret.txt"));
    std::fs::write(
        value("report"),
        format!(
            "granted: {}\ndenied: {}\n",
            granted.is_ok(),
            denied
                .map(|_| "READ".to_string())
                .unwrap_or_else(|err| format!("{:?}", err.kind()))
        ),
    )
    .expect("write the report");
}

#[test]
fn a_rust_binary_runs_inside_the_container_and_the_whitelist_holds() {
    let dir = tempfile::tempdir().expect("temp dir");
    let tree = dir.path().join("tree");
    let root = tree.join("root");
    let denied = tree.join("denied");
    let report_dir = tree.join("report");
    std::fs::create_dir_all(&root).expect("root");
    std::fs::create_dir_all(&denied).expect("denied");
    std::fs::create_dir_all(&report_dir).expect("report");
    std::fs::write(root.join("granted.txt"), b"granted").expect("seed");
    std::fs::write(denied.join("secret.txt"), b"secret").expect("seed");
    // The verified row this reproduces: a directory with `Users` /
    // `Authenticated Users` ACEs and no package-SID ACE is denied anyway.
    let users = std::process::Command::new(acl::icacls_binary())
        .arg(&denied)
        .args(["/grant", r"*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users on the denied tree");
    assert!(users.status.success(), "{}", String::from_utf8_lossy(&users.stderr));
    let probe = write_probe(&root, &denied, &report_dir);

    // **This is where the launch-succeeds assertion lives**, and it is the whole
    // reason Task 2 creates the profile rather than deriving the SID: with a
    // merely derived SID, `CreateProcessW` below fails `err=2
    // ERROR_FILE_NOT_FOUND` on a machine where this profile never existed —
    // which is every fresh machine — and it does so identically for this binary
    // and for `cmd.exe`, with or without the ACEs already granted.
    let sid = Arc::new(PackageSid::derive(&package_name(&root)).expect("derive"));
    acl::grant(&root, AccessMode::ReadOnly, &sid).expect("grant the root");
    acl::grant(&report_dir, AccessMode::ReadWrite, &sid).expect("grant the report dir");

    // The child's stdin carries the payload and its stdout carries the event
    // stream (decisions 17, 19); its listener is handed over by the argv element.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    socket::mark_inheritable(&listener).expect("mark the listener inheritable");
    let payload_pipe = handles::pipe().expect("the child's stdin");
    let event_pipe = handles::pipe().expect("the event pipe");
    // `command_args` puts the listener element first and the launcher's own
    // arguments after it, so what libtest sees on the child's side is exactly
    // `child_role --exact` — the element never reaches it (Task 7 fixes the
    // rule, and Task 9's `confine` states the same one from the launcher's end).
    let args = launch::command_args(
        socket::handle(&listener),
        &[OsString::from("child_role"), OsString::from("--exact")],
    );
    let frame = serde_json::to_vec(&LaunchPayload {
        config: probe.to_string_lossy().into_owned(),
    })
    .expect("the payload serializes");
    handles::write_payload(payload_pipe.write, &frame)
        .expect("write the frame into the child's stdin");
    let stderr = handles::inherited_stderr().expect("the launcher's stderr");

    let program = std::env::current_exe().expect("current exe");
    let mut launched = process::launch(
        &LaunchSpec {
            program: &program,
            args: &args,
            stdin: payload_pipe.read,
            stdout: event_pipe.write,
            stderr,
            inherited: &[socket::handle(&listener)],
            current_dir: None,
        },
        &Container {
            package_sid: sid.raw(),
            capabilities: CapabilitySids::resolve(&[]).expect("no capabilities"),
        },
    )
    .expect("launch inside the container");
    let status = launched.wait().expect("wait");

    let report = std::fs::read_to_string(report_dir.join("report.txt")).expect("the report");
    acl::revoke(&root, &sid).expect("roll back");
    acl::revoke(&report_dir, &sid).expect("roll back");

    assert_eq!(status.code(), Some(0), "the confined child exited non-zero: {report}");
    assert!(report.contains("granted: true"), "{report}");
    assert!(report.contains("denied: PermissionDenied"), "{report}");
}

#[test]
fn a_bad_program_path_is_an_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("temp dir");
    let sid = PackageSid::derive(&package_name(dir.path())).expect("derive");
    let err = process::launch(
        &LaunchSpec {
            program: Path::new(r"E:\does-not-exist\tinio-serve.exe"),
            args: &[],
            // The standard handles are never reached — `CreateProcessW` fails on
            // the program path first — and a null handle is the honest value for
            // a launch that never got that far.
            stdin: std::ptr::null_mut(),
            stdout: std::ptr::null_mut(),
            stderr: std::ptr::null_mut(),
            inherited: &[],
            current_dir: None,
        },
        &Container {
            package_sid: sid.raw(),
            capabilities: CapabilitySids::resolve(&[]).expect("no capabilities"),
        },
    )
    .expect_err("a missing program must fail");
    let rendered = err.to_string();
    assert!(rendered.contains("CreateProcessW"), "{rendered}");
    assert!(rendered.contains("does-not-exist"), "{rendered}");
}
```

The child's argv is built by `launch::command_args`, which is what the backend uses too — the child sees `tinio-sandbox-fd=<handle>` and strips it on its own side. **`command_args` takes what follows `argv[0]` and puts the element first**, so the launcher's side of the contract is `std::env::args_os().skip(1)` and nothing else — Task 9's `confine` states the same rule from the other end, where it is easy to get wrong. `command_line` then writes the program path as the child's `argv[0]`, so the child's `argv` is `<program> tinio-sandbox-fd=<handle> <the launcher's own arguments>`; `own_args` drops `argv[0]`, filters the element, and what is left is the launcher's arguments in order. For a test binary that is `["child_role", "--exact"]` and nothing else — `Options::args` follows the identical rule, so no stray argument is left for libtest to interpret.

- [ ] **Step 6: Run the integration test**

Run: `cargo test -p tinio-sandbox-windows --test launch`
Expected: PASS — three tests. If `denied` reports a different `ErrorKind`, print the report and assert on the raw Win32 code (5, `Access is denied`) instead: the design's evidence row is stated in those terms. **If the launch itself fails with `CreateProcessW` and `err=2` (`ERROR_FILE_NOT_FOUND`) for a program path that plainly exists, the profile is missing** — that is the measurement Task 2 is written against, and it means `PackageSid::derive` is not creating it. Do not chase the path: nothing about the path is wrong.

§11's error table gains the row this step is built on: **a refused attribute set is fatal, and the launch that would have followed is not made** — because `UpdateProcThreadAttribute` failing is invisible downstream (the child starts, unconfined, and `CreateProcessW` reports success). The design states it beside the existing Windows row, which covers a failed launch but not a launch that silently succeeded without its confinement.

---

### Task 9: `Backend::confine` — the composition

**Files:**
- Modify: `crates/tinio-sandbox-windows/src/backend.rs`
- Create: `crates/tinio-sandbox-windows/tests/common/mod.rs`
- Create: `crates/tinio-sandbox-windows/tests/confine.rs`
- Modify: `crates/tinio-sandbox/src/lib.rs` (the facade's Windows arm)

**Interfaces:**
- Consumes: everything above, plus core's `Backend`, `Confinement`, `Child` (with `with_relay`), `LaunchPlan`, `LaunchPayload`, `supervise`, `Event` and `Observer`.
- Produces: `impl Backend for Backend`; and two additions to the facade's Windows arm — the child-side listener rebuild under the platform-neutral name `adopted_listener` (the crate's `launch::listener`) and the stop hook `on_stop`, the two names the shared child path drives. Nothing else from this crate's `launch` module rides along: not `command_args`, not `disarm`, and not the module's `adopt` wrapper, because the shared path calls the core's `adopt` under the flat name the facade already carries. `Options`, `Capability`, `confine` and `socket` are **not** re-exported either: a backend's own configuration is not part of the facade's shared surface, and the launch path reaches it by naming this crate directly (design decision 2, §10).

The order is the design's §3: resolve the served root, compute the exposure set, grant it, create the two pipes, mark the handles inheritable, launch, assign the job, write the payload into the child's stdin, spawn the thread that drives core's `Observer` over the child's stdout, then hand the caller a `Child` whose wait closure waits for the process and rolls the ACLs back. Rollback happens on every path the launcher survives: a failed grant (the `Grants` guard), a failed launch, a failed job assignment, the clean wait, and a dropped `Child` (`Child` holds the closure, and the closure owns the `Grants`, so abandoning it drops the guard).

- [ ] **Step 1: Write the failing test**

`crates/tinio-sandbox-windows/tests/common/mod.rs`:

```rust
//! The harness the confinement tests share: a tree on a chosen volume, the
//! probe file a confined child reads, and one launcher call.

#![cfg(windows)]

use std::ffi::OsString;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use tinio_sandbox_core::{
    Backend, Confinement, LaunchPayload, LaunchPlan, Sandbox,
};
use tinio_sandbox_windows::{Options, sid};

/// The volumes the suite exercises.
///
/// `TINIO_SANDBOX_TEST_VOLUMES` (semicolon separated) selects them; without it,
/// the volume holding the system temp directory. The design's evidence shows
/// ReFS and NTFS agree on every probed ACL case, so a regression that broke one
/// would be invisible on a single-volume run — this is how both are covered.
pub fn volumes() -> Vec<PathBuf> {
    match std::env::var("TINIO_SANDBOX_TEST_VOLUMES") {
        Ok(raw) => raw
            .split(';')
            .map(str::trim)
            .filter(|entry| !entry.is_empty() && Path::new(entry).is_dir())
            .map(PathBuf::from)
            .collect(),
        Err(_) => vec![std::env::temp_dir()],
    }
}

/// One tree's worth of probes: what the child reads and what it writes.
pub struct Tree {
    /// The tree root, on the volume under test.
    pub path: PathBuf,
    /// The served root. Exposed with the caller's access mode.
    pub root: PathBuf,
    /// A sibling with no package-SID ACE, carrying `Users` instead.
    pub denied: PathBuf,
    /// A directory exposed read-write, where the child writes its report.
    pub report: PathBuf,
    /// The address the child should try to reach, when a test sets one. It
    /// travels in the probe file, so the confined child needs no environment
    /// channel to learn it.
    pub target: Option<SocketAddr>,
}

impl Tree {
    /// The tree whose directories sit under `path`, without touching the disk.
    ///
    /// The one rule `tree()` and [`launch_confined_at`] share: `root`, `denied`
    /// and `report` are `path`'s three children, in that order, and nothing about
    /// the layout is carried in a value a second process cannot receive. A
    /// launcher role is a fresh process that got a path in an environment
    /// variable and nothing else, so it rebuilds the same three directories from
    /// it rather than being handed a `Tree`.
    pub fn at(path: &Path) -> Self {
        Self {
            root: path.join("root"),
            denied: path.join("denied"),
            report: path.join("report"),
            path: path.to_path_buf(),
            target: None,
        }
    }

    /// The probe text, without writing it.
    pub fn probe_text(&self) -> String {
        let mut text = format!(
            "root={}\ndenied={}\nreport={}\n",
            self.root.display(),
            self.denied.display(),
            self.report.join("report.txt").display()
        );
        if let Some(target) = self.target {
            text.push_str(&format!("target={target}\n"));
        }
        text
    }

    /// Write the probe file and return its path — what the payload's config
    /// bytes carry.
    ///
    /// **It goes into the report directory and not into `path`**, and that is
    /// the difference between a suite that tests the confinement and one that
    /// tests nothing. `path` is the tree's own directory and is exposed to
    /// nobody; the confined child is an AppContainer, so its reads are checked
    /// against the package SID and the user's own ACEs do not help it — a probe
    /// written as a sibling of `root` and `report` is a file the child cannot
    /// open, and every role that reads it panics on its first line, taking the
    /// confined cases of Tasks 8, 9, 10 and 12 with it. The report directory is
    /// the one path every test here grants read-write, which is what makes it the
    /// only place a probe can live.
    pub fn probe(&self) -> PathBuf {
        let probe = self.report.join("probe.txt");
        std::fs::write(&probe, self.probe_text()).expect("write the probe file");
        probe
    }

    /// What the child reported, if it got that far.
    pub fn report_text(&self) -> String {
        std::fs::read_to_string(self.report.join("report.txt")).unwrap_or_default()
    }
}

/// Build the standard tree under `volume`.
///
/// The `denied` sibling carries `Users` and no package-SID ACE, which is the
/// verified row every confinement test reproduces: those ACEs do not help.
pub fn tree(volume: &Path) -> (tempfile::TempDir, Tree) {
    let dir = tempfile::TempDir::new_in(volume).expect("temp dir on the volume");
    let tree = Tree::at(&dir.path().join("tree"));
    std::fs::create_dir_all(&tree.root).expect("root");
    std::fs::create_dir_all(&tree.denied).expect("denied");
    std::fs::create_dir_all(&tree.report).expect("report");
    // Seeded *before* the grant: this is design §6's inheritance question as the
    // confinement suite sees it — Task 4 settled the ACL shape, and this is the
    // functional half of the same answer.
    std::fs::write(tree.root.join("granted.txt"), b"granted").expect("seed");
    std::fs::write(tree.denied.join("secret.txt"), b"secret").expect("seed");
    let granted = std::process::Command::new(tinio_sandbox_windows::acl::icacls_binary())
        .arg(&tree.denied)
        .args(["/grant", r"*S-1-5-32-545:(OI)(CI)R", "/q"])
        .output()
        .expect("grant Users on the denied tree");
    assert!(
        granted.status.success(),
        "{}",
        String::from_utf8_lossy(&granted.stderr)
    );
    (dir, tree)
}

/// Append one line to the report the parent reads.
pub fn append_to_report(report: &Path, line: &str) {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(report)
        .expect("open the report");
    writeln!(file, "{line}").expect("write the report");
}

/// The confined child's role, shared by every test binary that launches one.
///
/// Inert when there is no payload: that is design decision 17's discriminator,
/// exercised by the tests exactly as production uses it, and it is why the
/// launcher-side test functions can share a binary with this one.
pub fn child_role() {
    let Some(adopted) =
        tinio_sandbox_windows::launch::adopt(std::env::args_os(), std::io::stdin().lock())
            .expect("adopt")
    else {
        return;
    };
    // Core's rule hands back a descriptor; rebuilding it is this crate's half.
    let listener = tinio_sandbox_windows::launch::listener(&adopted)
        .expect("the harness bound a listener");
    child_role_body(adopted.config(), listener);
}

/// The role's body, so the heartbeat variant can share it (Task 10).
///
/// The first parameter is the payload's configuration **text** — `Adopted::config`
/// is a `String` in P0's surface, and this harness is the one reader that treats
/// it as a path to the probe file it wrote.
pub fn child_role_body(config: &str, listener: std::net::TcpListener) {
    use std::io::Write;

    let probe_path = config.to_owned();
    let probe = std::fs::read_to_string(&probe_path).expect("read the probe file");
    let value = |key: &str| {
        probe
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")))
            .expect("a probe key")
            .to_string()
    };
    let root = PathBuf::from(value("root"));
    let denied = PathBuf::from(value("denied"));
    let report = PathBuf::from(value("report"));

    let mut log = String::new();
    match std::fs::read_to_string(root.join("granted.txt")) {
        Ok(_) => log.push_str("granted-read: OK\n"),
        Err(err) => log.push_str(&format!("granted-read: refused ({})\n", err.kind())),
    }
    match std::fs::read_to_string(denied.join("secret.txt")) {
        Ok(_) => log.push_str("denied-read: READ\n"),
        Err(err) => log.push_str(&format!("denied-read: refused ({})\n", err.kind())),
    }
    match std::fs::write(denied.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("denied-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("denied-write: refused ({})\n", err.kind())),
    }
    match std::fs::write(root.join("written.txt"), b"nope") {
        Ok(()) => log.push_str("root-write: ALLOWED\n"),
        Err(err) => log.push_str(&format!("root-write: refused ({})\n", err.kind())),
    }
    append_to_report(&report, log.trim_end());

    // The outbound attempt, only when a test asked for one: a test that only
    // checks "connect failed" proves nothing, so the parent proves the target
    // reachable unconfined first (design §12).
    if let Some(target) = probe
        .lines()
        .find_map(|line| line.strip_prefix("target="))
        .and_then(|raw| raw.parse::<SocketAddr>().ok())
    {
        let outcome = match TcpStream::connect_timeout(&target, Duration::from_secs(5)) {
            Ok(_) => "outbound: SUCCEEDED".to_string(),
            Err(err) => format!("outbound: refused ({})", err.kind()),
        };
        append_to_report(&report, &outcome);
    }

    // The listener this process inherited: answer one connection, which is what
    // the parent is waiting for.
    let (mut stream, _) = listener.accept().expect("accept");
    stream.write_all(b"pong").expect("answer");
    stream.flush().expect("flush the answer");
}

/// Start one confined child over `tree` and hand back the supervision.
///
/// The in-process entry: the caller has a [`Tree`] because it built one, and the
/// probe it writes is the one that carries this tree's `target` when a test set
/// one (the network suite does).
pub fn launch_confined(
    tree: &Tree,
    root_access: tinio_sandbox_core::AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> Option<(Confinement, SocketAddr)> {
    let probe = tree.probe();
    start(tree, &probe, root_access, options, bind)
}

/// Start one confined child from a tree **path**, for a caller that is its own
/// process.
///
/// The launcher roles of the events and signal suites are separate processes,
/// handed a directory in `TINIO_SANDBOX_TEST_TREE`; they cannot be given a
/// [`Tree`], so this entry rebuilds one with [`Tree::at`] and writes the probe
/// under it. What a rebuilt tree cannot carry is a `target` — a launcher role is
/// not the test that set one — and nothing that calls this entry wants one.
pub fn launch_confined_at(
    path: &Path,
    root_access: tinio_sandbox_core::AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> Option<(Confinement, SocketAddr)> {
    let tree = Tree::at(path);
    let probe = tree.probe();
    start(&tree, &probe, root_access, options, bind)
}

/// The one body both entries above share: the exposure set, the listener, the
/// plan and the launch.
///
/// It takes the probe's path rather than writing it, because *where* the probe
/// goes is [`Tree::probe`]'s rule and *what* it says is the caller's — one entry
/// carries a target and the other cannot.
fn start(
    tree: &Tree,
    probe: &Path,
    root_access: tinio_sandbox_core::AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> Option<(Confinement, SocketAddr)> {
    let sandbox = Sandbox::new()
        .expose(&tree.root, root_access)
        .expose(&tree.report, tinio_sandbox_core::AccessMode::ReadWrite);
    let listener = TcpListener::bind(bind.unwrap_or_else(|| "127.0.0.1:0".parse().expect("loopback")))
        .expect("bind the listener");
    let address = listener.local_addr().expect("address");
    // The listener is attached, not carried through the payload: a socket
    // cannot ride on a stream (decision 16).
    let plan = LaunchPlan::for_launch(probe.to_string_lossy().into_owned()).with_listener(listener);
    // The child is one role function of this test binary: `Options::args` is the
    // seam that says so, because a test binary's own argv belongs to libtest.
    let options = Options {
        args: Some(vec![OsString::from("child_role"), OsString::from("--exact")]),
        ..options
    };
    let backend = tinio_sandbox_windows::Backend::with_options(options);
    let confinement = backend.confine(&sandbox, &plan).expect("confine");
    Some((confinement, address))
}

/// Start a confined child, answer one loopback connection through the inherited
/// listener, and wait for it.
pub fn run_confined(tree: &Tree, root_access: tinio_sandbox_core::AccessMode) -> Option<Run> {
    let (confinement, address) = launch_confined(tree, root_access, Options::default(), None)?;
    let Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(10))
        .expect("connect to the confined server");
    let mut pong = [0u8; 4];
    std::io::Read::read_exact(&mut stream, &mut pong).expect("read the child's answer");
    assert_eq!(&pong, b"pong");
    // `supervise` borrows the child, and the terminal read after it is what
    // separates "never reported" from "still starting" (decision 19).
    let status = tinio_sandbox_core::supervise(&mut child, || ()).expect("supervise");
    Some(Run {
        status: status.code().unwrap_or(-1),
        report: tree.report_text(),
        address,
    })
}

/// A confined run's outcome.
pub struct Run {
    /// The child's exit code.
    pub status: i32,
    /// What the child wrote to its report file.
    pub report: String,
    /// The address the parent bound.
    pub address: SocketAddr,
}

/// The package SID this run derives for `tree`.
pub fn sid_for(tree: &Tree) -> sid::PackageSid {
    sid::PackageSid::derive(&sid::package_name(&tree.root)).expect("derive the package SID")
}
```

`crates/tinio-sandbox-windows/tests/confine.rs`:

```rust
//! End-to-end confinement: the whitelist, the loopback round trip through the
//! inherited listener, and the rollback on every exit path — on every volume the
//! harness is pointed at.

#![cfg(windows)]

mod common;

use std::path::Path;

use tinio_sandbox_core::AccessMode;
use tinio_sandbox_windows::acl;

/// The confined child. The body lives in the harness because the network and
/// signal suites launch the same role from their own test binaries.
#[test]
fn child_role() {
    common::child_role();
}

#[test]
fn the_whitelist_holds_the_loopback_round_trip_and_the_rollback() {
    for volume in common::volumes() {
        let (_dir, tree) = common::tree(&volume);
        let sid = common::sid_for(&tree);
        let Some(run) = common::run_confined(&tree, AccessMode::ReadWrite) else {
            return;
        };
        assert_eq!(run.status, 0, "on {}: {}", volume.display(), run.report);
        // The pre-existing file this reads was seeded before the grant: the
        // functional half of design §6's inheritance answer (Task 4).
        assert!(run.report.contains("granted-read: OK"), "on {}: {}", volume.display(), run.report);
        assert!(run.report.contains("denied-read: refused"), "on {}: {}", volume.display(), run.report);
        assert!(run.report.contains("denied-write: refused"), "on {}: {}", volume.display(), run.report);
        assert!(run.report.contains("root-write: ALLOWED"), "on {}: {}", volume.display(), run.report);
        // Decision 15: the ACEs are gone once the launcher is done, on the clean
        // path as much as on the failure paths. **`has_ace` refusing a non-zero
        // `icacls` exit is what makes these assertions mean anything** (Task 4):
        // as an oracle it would otherwise have reported "no entry" for a path it
        // could not read at all, and every rollback assertion in this file would
        // pass for that reason rather than this one.
        assert!(!acl::has_ace(&tree.root, &sid).unwrap(), "on {}", volume.display());
        assert!(!acl::has_ace(&tree.report, &sid).unwrap(), "on {}", volume.display());
    }
}

#[test]
fn a_read_only_root_refuses_writes_and_still_reads() {
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let Some(run) = common::run_confined(&tree, AccessMode::ReadOnly) else {
        return;
    };
    assert_eq!(run.status, 0, "{}", run.report);
    assert!(run.report.contains("granted-read: OK"), "{}", run.report);
    assert!(run.report.contains("root-write: refused"), "{}", run.report);
    assert!(!tree.root.join("written.txt").exists());
    assert!(!acl::has_ace(&tree.root, &sid).unwrap());
}

#[test]
fn a_failed_launch_still_rolls_the_aces_back() {
    // Design §11: the rollback runs after a failed launch too, not only on the
    // clean path.
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);

    let sandbox = tinio_sandbox_core::Sandbox::new().expose(&tree.root, AccessMode::ReadWrite);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let plan = tinio_sandbox_core::LaunchPlan::for_launch(String::new()).with_listener(listener);
    let backend = tinio_sandbox_windows::Backend::with_options(tinio_sandbox_windows::Options {
        program: Some(r"E:\does-not-exist\serve.exe".into()),
        ..Default::default()
    });
    let err = tinio_sandbox_core::Backend::confine(&backend, &sandbox, &plan)
        .expect_err("a missing program must fail the launch");
    assert!(err.to_string().contains("CreateProcessW"), "{err}");
    assert!(
        !acl::has_ace(&tree.root, &sid).unwrap(),
        "the grant was rolled back after the failed launch"
    );
}

#[test]
fn a_dropped_supervision_also_rolls_back() {
    // The guard's path: the caller never waits, and the ACEs still go.
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let sid = common::sid_for(&tree);
    let Some((confinement, _address)) =
        common::launch_confined(&tree, AccessMode::ReadWrite, Default::default(), None)
    else {
        return;
    };
    assert!(acl::has_ace(&tree.root, &sid).unwrap(), "the grant is on while it runs");
    drop(confinement);
    assert!(
        !acl::has_ace(&tree.root, &sid).unwrap(),
        "dropping the supervision rolled the grant back"
    );
}
```

The failed-launch test reaches the backend through `Options::program` (Task 3's seam), which is the only way to make a launch fail *after* the ACLs are on:

```rust
    let backend = tinio_sandbox_windows::Backend::with_options(tinio_sandbox_windows::Options {
        program: Some(r"E:\does-not-exist\serve.exe".into()),
        ..Default::default()
    });
```

- [ ] **Step 1b: Write the failing relay test**

§12 asks for this one by name: "assert that text the child writes to stdout which is *not* a frame does **not** reach the parent's stdout — that is the relay's whole job, and a relay that forwards bytes would pass every other assertion in this list."

`crates/tinio-sandbox-windows/tests/events.rs`:

```rust
//! The stdout relay: core's `Observer` driving the child's stdout, dropping
//! everything that is not one of our frames.
//!
//! The launcher is a separate process here, because what is being asserted is
//! what lands on *its* stdout — a relay running inside the test process would
//! write into libtest's own capture, where nothing can be read back.

#![cfg(windows)]

mod common;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tinio_sandbox_core::Event;

/// The launcher's own role: run the production launch path and supervise it,
/// with its stdout owned by the test.
#[test]
fn launcher_role() {
    if std::env::var_os("TINIO_SANDBOX_TEST_LAUNCHER").is_none() {
        return;
    }
    let tree = PathBuf::from(std::env::var("TINIO_SANDBOX_TEST_TREE").expect("tree"));
    let Some((confinement, address)) = common::launch_confined_at(
        &tree,
        tinio_sandbox_core::AccessMode::ReadWrite,
        Default::default(),
        None,
    ) else {
        return;
    };
    // The test that spawned this process has no other way to learn the port: the
    // listener belongs to this process and the child's `Listening` event is the
    // protocol's, not this launcher's. One line in a file the test polls for is
    // the whole channel, and the test needs it because the child role ends by
    // accepting a connection.
    std::fs::write(tree.join("address"), address.to_string()).expect("publish the address");
    let tinio_sandbox_core::Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    let status = tinio_sandbox_core::supervise(&mut child, || ()).expect("supervise");
    std::process::exit(status.code().unwrap_or(1));
}

/// Wait for a launcher role to publish the address it bound.
///
/// **Bounded, so that a launcher which never got that far fails the test rather
/// than hanging it.** The alternative — reading the launcher's stdout, which
/// returns only when the launcher exits, and the launcher exits only when its
/// child does — is a wait with no edge at all when the launch fails.
fn wait_for_address(tree: &Path) -> std::net::SocketAddr {
    let file = tree.join("address");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(text) = std::fs::read_to_string(&file)
            && let Ok(address) = text.trim().parse()
        {
            return address;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("the launcher never published an address at {}", file.display());
}

/// The child role: one stray line, one real event, then the usual probes.
#[test]
fn child_role() {
    let Some(adopted) =
        tinio_sandbox_windows::launch::adopt(std::env::args_os(), std::io::stdin().lock())
            .expect("adopt")
    else {
        return;
    };
    let listener = tinio_sandbox_windows::launch::listener(&adopted).expect("a listener");
    // Deliberate protocol violations, to prove the relay is a decoder and not a
    // `copy`: a bare line, and a line that looks framed but is not ours.
    println!("this line is not a frame");
    println!("data: {{}}");
    print!("{}", Event::Error { message: "probe".into() }.encode());
    common::child_role_body(adopted.config(), listener);
}

#[test]
fn the_parent_re_emits_frames_and_drops_everything_else() {
    use std::io::Read;
    use std::process::{Command, Stdio};

    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let mut launcher = Command::new(std::env::current_exe().expect("current exe"))
        .args(["launcher_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_LAUNCHER", "1")
        .env("TINIO_SANDBOX_TEST_TREE", &tree.path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the launcher");

    // **The child has to be answered, or this test hangs instead of failing.**
    // It shares `child_role_body` with the confinement suite, so its last act is
    // `listener.accept()`: with nothing connecting, the child blocks, the
    // launcher waits on the child, and the read below — which returns only when
    // the launcher exits — never returns. So the address comes first, with a
    // bound, and the connection is made before a single byte of the relayed
    // stream is read.
    let address = wait_for_address(&tree.path);
    let mut stream = std::net::TcpStream::connect_timeout(&address, Duration::from_secs(10))
        .expect("connect to the confined child");
    let mut pong = [0u8; 4];
    stream.read_exact(&mut pong).expect("read the child's answer");
    assert_eq!(&pong, b"pong");

    let mut relayed = String::new();
    launcher
        .stdout
        .take()
        .expect("the launcher's stdout")
        .read_to_string(&mut relayed)
        .expect("read the relayed stream");
    launcher.wait().expect("the launcher exits with its child's status");

    // The frame's real shape, which is what the relay writes on: `Event` puts the
    // name in its own field, so an `event:` line comes first and the JSON object
    // on the `data:` line is the variant's *payload*, not the variant — a test
    // asserting the serde-enum shape (`{"Error":{…}}`) would be asserting a
    // protocol nobody implements (P0's encoding is normative).
    assert!(
        relayed.contains("event: error\n"),
        "the child's event reached the operator:\n{relayed}"
    );
    assert!(
        relayed.contains(r#"data: {"message":"probe"}"#),
        "the child's event payload reached the operator whole:\n{relayed}"
    );
    assert!(
        !relayed.contains("this line is not a frame"),
        "a bare line the child wrote reached the operator, so the relay forwarded bytes:\n{relayed}"
    );
    assert!(
        !relayed.contains("data: {}"),
        "a foreign frame reached the operator:\n{relayed}"
    );
    // Every line that reached stdout belongs to one of our frames: the `event:`
    // line naming one, the `data:` line carrying it, or the blank line that closes
    // it. Every real frame starts with `event: `, which is the shape a
    // `data: `-only assertion would have missed.
    for line in relayed.lines() {
        assert!(
            line.is_empty() || line.starts_with("event: ") || line.starts_with("data: "),
            "a non-frame line reached the operator: {line:?}"
        );
    }
}
```

`launch_confined_at` is defined in `tests/common/mod.rs` above, not in Task 10, because both suites use it — the events suite spawns its launcher as a process, and so does the signal suite. It and `launch_confined` are two entries over one body (`start`), and the only difference between them is how the caller identifies the tree: a `Tree` it built, or a **path** it received, in which case `Tree::at` rebuilds the same three directories the layout rule fixes. Task 10's step therefore adds only the heartbeat helpers.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-windows --test confine` and `cargo test -p tinio-sandbox-windows --test events`
Expected: FAIL — `the method 'confine' exists but its trait bound 'Backend' is not satisfied`, because the impl does not exist yet.

- [ ] **Step 3: Write the composition**

Append to `crates/tinio-sandbox-windows/src/backend.rs`:

```rust
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use tinio_sandbox_core::{
    Backend as BackendTrait, Child, Confinement, LaunchPayload, LaunchPlan, Result, Terminal,
};

use crate::acl::Grants;
use crate::handles;
use crate::job::Job;
use crate::launch;
use crate::process::{self, Container, LaunchedProcess, LaunchSpec};
use crate::sid::{CapabilitySids, PackageSid};
use crate::socket;

impl BackendTrait for Backend {
    /// Confine by launching a child, and hand it back for supervision.
    ///
    /// Returns [`Confinement::Supervise`] with a `Child` whose wait closure waits
    /// for the process and then removes the ACEs, in that order: the closure owns
    /// the [`Grants`] guard, so `supervise`'s teardown, a dropped `Child` and a
    /// panic all end with the operator's directories as they were (design
    /// decision 15). **The AppContainer profile goes back on the same paths and in
    /// the same order**: it is held by the `Arc<PackageSid>` the guard holds, so
    /// when the guard drops — after the rollback, at the very end of the launch —
    /// the last reference to the SID dies and `PackageSid`'s `Drop` removes the
    /// profile (Task 2). Nothing in this function calls it directly, and that is
    /// what keeps the profile outliving the child on every path a launcher
    /// survives: the child needs the profile for as long as it runs.
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] naming the failing step and the path, **after** rolling
    /// back everything already granted.
    fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement> {
        let served = served_root(sandbox)?;
        let program = self
            .options
            .program
            .clone()
            .unwrap_or(std::env::current_exe().map_err(|source| {
                handles::step("locate the launcher's own executable", "current_exe", source)
            })?);
        // The whitelist is the caller's exposures plus this binary's own
        // directory, resolved against the volume and checked for reparse points
        // (Task 3): what is granted below is the path the kernel names, never a
        // spelling that happens to resolve somewhere else.
        let all = canonical_exposures(exposures(sandbox, &program))?;
        // **A plan with no listener is the operator's mistake, not the platform's
        // shortcoming**, which is what makes it `Step` rather than `Unsupported`:
        // `Unsupported` is reserved for a mechanism this host does not have
        // (P0's `Error`, design §11), and a launcher platform always binds one.
        let listener = plan.listener().ok_or_else(|| {
            handles::step(
                "hand the listener over",
                "argv",
                io::Error::other("the launch plan carries no bound listener"),
            )
        })?;

        // The package name covers the served root, so a crashed run's residue is
        // this run's to take over (decision 21) and a second server on a
        // different root shares no SID with this one.
        let package = self
            .options
            .package_name
            .clone()
            .unwrap_or_else(|| crate::sid::package_name(&served));
        let sid = Arc::new(PackageSid::derive(&package)?);
        let capabilities = CapabilitySids::resolve(
            &self
                .options
                .capabilities
                .iter()
                .map(|capability| capability.sid())
                .collect::<Vec<_>>(),
        )?;

        // The ACLs go on before the child exists, or it cannot load at all
        // (design §3 step 5).
        //
        // **These two lines are `eprintln!` and not a traced event, deliberately.**
        // They are the operator's runbook — Task 11 Step 8 and the end-to-end test
        // both read them, the second of them by matching this exact prefix — and
        // they state the two facts a human wants in one place: the SID this run
        // owns and the whole exposure set it is about to grant. The
        // machine-readable half is traced instead, one `info!` per resolved
        // exposure (Task 3) and one for the profile (Task 2), rendered by the
        // subscriber the launcher arm installs (Task 11); neither form replaces
        // the other.
        eprintln!("sandbox: package SID {}", sid.as_str());
        eprintln!("sandbox: granting {}", describe(&all));
        // **The grant list is one entry per exposed path; the cost is per object
        // beneath them, and the filesystem is what walks.** Each call below is a
        // single `icacls` invocation — the tool answers `Successfully processed
        // 1 files`, because it was handed one path — but the ACE it writes is
        // `(OI)(CI)`, so every descendant those paths already hold picks up an
        // inherited copy. The `rollback` in the wait closure pays the same pass
        // again on the way out, which makes a run's ACL work proportional to the
        // served corpus twice over. **No document measures that** (Task 13
        // establishes the number), and the budget it competes with is the e2e
        // harness's 30 s `START_TIMEOUT` (`crates/tinio-e2e/tests/steps/clients.rs`).
        let mut grants = Grants::new(Arc::clone(&sid));
        for exposure in &all {
            // A failure here leaves the earlier grants to the guard's `Drop`,
            // which is the same path a panic takes.
            grants.apply(&exposure.host, exposure.access)?;
        }

        // Everything after the first grant runs inside this closure, so the single
        // `?` chain below cannot skip the rollback. The three values that outlive
        // it travel back together — the child, the job that owns it (which has to
        // live exactly as long as the child, and putting it in the wait closure is
        // what makes that true without leaking a handle), and the relay thread
        // whose answer `Child::wait` joins. The annotation is written out rather
        // than inferred because that tuple is what the `match` below destructures,
        // and a two-element type here would be a compile error at the
        // destructuring rather than at the return.
        let launched = (|| -> Result<(LaunchedProcess, Job, JoinHandle<Option<Terminal>>)> {
            // The listener travels by handle, not by stream (decision 16's
            // irreducible half): it is marked inheritable, put in the handle
            // list, and named in argv.
            socket::mark_inheritable(listener)?;
            // Two pipes: the child's stdin carries the payload (decision 17) and
            // its stdout carries the events the parent relays (decision 19).
            let payload_pipe = handles::pipe()
                .map_err(|source| handles::step("CreatePipe", "stdin", source))?;
            let event_pipe = handles::pipe()
                .map_err(|source| handles::step("CreatePipe", "stdout", source))?;
            // **The serializer's error goes through `step` and not through `?`.**
            // The core's only `From` impl is `Error::Wait(#[from] io::Error)`, so
            // there is nothing a `serde_json::Error` could convert into (P0's
            // `Error`), and a launcher failure wants the operation's name anyway —
            // which is what `step` supplies and what makes the log line and the
            // error agree.
            let payload = serde_json::to_vec(&LaunchPayload {
                config: plan.config().to_owned(),
            })
            .map_err(|err| {
                handles::step(
                    "serialize the launch payload",
                    "stdin",
                    std::io::Error::other(err),
                )
            })?;
            // The launcher re-invokes itself with its own arguments, unless a
            // test pointed it at one role function instead (design §3 step 1:
            // the child parses the same argv the launcher did). **`skip(1)` is
            // load-bearing**: `command_args` inserts the listener element at
            // index 1 of what it is given, and the child drops its own `argv[0]`
            // and then the element — so the arguments handed over must begin
            // *after* the program name, or the child parses this process's own
            // executable path where its root belongs.
            let argv = self
                .options
                .args
                .clone()
                .unwrap_or_else(|| std::env::args_os().skip(1).collect());
            let args = launch::command_args(socket::handle(listener), &argv);
            let stderr = handles::inherited_stderr()?;
            let container = Container {
                package_sid: sid.raw(),
                capabilities,
            };
            let launched = process::launch(
                &LaunchSpec {
                    program: &program,
                    args: &args,
                    stdin: payload_pipe.read,
                    stdout: event_pipe.write,
                    stderr,
                    inherited: &[socket::handle(listener)],
                    current_dir: None,
                },
                &container,
            )?;
            // **Neither job failure may leave the child running**, because the job
            // is the only thing that makes "killing the launcher takes the child
            // with it" true (decision 20). A child that never joined it is one
            // nothing will take with it, and `Job::assign`'s own doc says the
            // caller must terminate in that case; a failure to create the job at
            // all is the same case one step earlier. Both paths kill the child
            // first and report second, and the terminate's own failure is logged
            // where it is built rather than replacing the reason the launch
            // stopped.
            let job = match Job::create() {
                Ok(job) => job,
                Err(err) => {
                    let _ = launched.terminate(1);
                    return Err(err);
                }
            };
            if let Err(err) = job.assign(launched.handle()) {
                let _ = launched.terminate(1);
                return Err(err);
            }
            // **The parent's copies of the child's own pipe ends are closed
            // here, and the relay depends on it.** `CreateProcessW` duplicated
            // both handles into the child's table, so this process no longer needs
            // them — and keeping `event_pipe.write` open would mean the event pipe
            // never reaches end-of-stream when the child exits, so the relay's
            // read would block forever, and `Child::wait`'s join with it. Closing
            // the payload pipe's *read* end is tidiness by comparison, but it is
            // the same one-line habit and the same leak.
            handles::close(payload_pipe.read);
            handles::close(event_pipe.write);
            // Design §3 step 7: the frame goes into the child's stdin, and
            // closing this end is what ends its read.
            handles::write_payload(payload_pipe.write, &payload)?;
            // The signal relay is armed only once the child exists and is owned
            // by the job (design §3 step 9); Task 10 adds the call here.
            //
            // The relay's decode-and-re-emit is the core's (`Observer`'s, built and
            // measured in P0); what this crate owns is the thread that drives it
            // over the child's stdout, because only the launcher holds that pipe.
            // It runs for the child's lifetime and drops every frame that is not
            // one of ours (design §3 step 8, decision 19).
            //
            // The child's stdout, in the shape the core's async `Observer` asks
            // for. **A Win32 anonymous pipe has no async type in tokio**, and
            // `tokio::fs::File` is what reads one: it is the same class of
            // implementation tokio itself uses for a child's stdout on this
            // platform — a blocking read on the blocking pool rather than an IOCP
            // registration — and it never seeks on the read path. Its `AsyncSeek`
            // is never called here; a pipe is not seekable and a seek on one
            // fails with ERROR_INVALID_PARAMETER (measured).
            let events = tokio::fs::File::from_std(handles::parent_end(event_pipe.read));
            // **The handle is kept rather than dropped.** It goes to the core
            // through `Child::with_relay`, and `Child::wait` joins it: the child
            // being reaped and the relay noticing end-of-stream are two events
            // with no ordering of their own, so a slot read before the join is a
            // snapshot — and on a healthy child it usually reads nothing at all
            // (decision 19).
            let relay = thread::spawn(move || {
                // The relay is **async** — the core's `Observer` writes through
                // `tokio::io` — while this launcher is built on blocking Win32
                // calls. So the thread owns a current-thread runtime whose only
                // job is to poll it: the smallest thing that satisfies the
                // async surface, rather than a multi-threaded runtime the
                // launcher has no other use for.
                let reader = tokio::io::BufReader::new(events);
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread().build() else {
                    return None;
                };
                // `Observer` rather than `reemit`, and that is the whole point:
                // `reemit` discards what the child reported, and the caller needs
                // it to decide whether this child ever became ready (decision
                // 19's "the parent owns the printed-exactly-once guarantee").
                // **The return value is the answer's channel** — the thread
                // hands back what it recorded, and `Child::wait` takes it out of
                // the join. Nothing shared, nothing to poison.
                runtime
                    .block_on(tinio_sandbox_core::Observer::new(tokio::io::stdout()).observe(reader))
                    .ok()
                    .and_then(|observer| observer.terminal().cloned())
            });
            Ok((launched, job, relay))
        })();

        match launched {
            Ok((mut launched, job, relay)) => Ok(Confinement::Supervise(
                Child::new(
                    move || {
                    // §3 step 8: wait, undo, report the child's status. The job is
                    // held by this closure so it outlives the child, and its close
                    // takes a child that outlived supervision with it (decision 20).
                    let status = launched.wait();
                    if let Err(err) = grants.rollback() {
                        // The failure was built and logged by [`step`], with the
                        // `icacls` invocation as its `step`; what this event adds
                        // is the phase, which is what tells a reader of the log
                        // whether the residue came from a launch or from a
                        // shutdown (shared §7).
                        tracing::error!(step = "roll back after the child exited", err = %err);
                    }
                    let _ = &job;
                    status
                },
            )
            .with_relay(relay),
            )),
            Err(err) => {
                // Design §11: roll back before reporting, so the operator is left
                // with neither a confined process nor a stray ACE. The profile
                // goes with the same drop — `grants` holds the last `Arc` to the
                // SID by then, and `PackageSid`'s `Drop` is what deletes it — so a
                // launch that failed at any step leaves nothing of this run
                // behind.
                let _ = grants.rollback();
                Err(err)
            }
        }
    }
}
```

**The caller then turns the slot into the guarantee**, and it is the only place that can — `Child::terminal` is meaningless before the child is gone, and the wait is what makes it final:

```rust
// Decision 19: the parent owns "printed exactly once", and it is the only
// witness to a child that died before it could say anything. `supervise`
// **borrows** the child — it consumes nothing — so the slot is read once, after
// it returns, when the relay has been joined and its answer is final rather
// than a snapshot.
let status = supervise(&mut child, teardown)?;
if child.terminal().is_none() {
    emit(&Event::Error {
        message: format!("the confined child exited ({status}) without reporting"),
    });
}
```

where the read is `child.terminal()`, taken **after** the wait. The ordering is the point and Task 11 asserts it: read the slot after the wait, not before — before it, the relay may still be running and the answer is a snapshot rather than a result. That ordering is exactly why `supervise` takes `&mut Child` rather than consuming it (P0's Task 2), and why `Child::with_relay` exists: an earlier draft could only read the slot *before* the wait — the one place a by-value `supervise` left it — and so reported a healthy child as never having become ready.

**And the same ordering is what a child that neither reports nor exits defeats.** The wait is `WaitForSingleObject(…, INFINITE)` (Task 8, which is the design's §3 step 8 shape rather than anything this plan chose, and which this plan does not change), so a child that hangs without ever exiting keeps the launcher inside `supervise` for as long as that child lives. In that window a consumer sees **nothing at all**: no `Listening`, no `Error`, no diagnostic, no exit status — the block above never runs, because its "without reporting" message is only readable *after* a wait that has not returned, and the relay thread has nothing to re-emit because the child's stdout is still open. Nothing in this phase bounds that: the deadline that would turn it into a diagnosable failure belongs to the readiness contract and is added elsewhere (the design's Bookkeeping assigns the harness's readiness scrape to P1; the Self-Review's deferred list repeats it), so what P2 owes is this paragraph — the statement of the window, for whoever adds the deadline — rather than a second bound on a wait the design owns.

`Child::new` takes a `FnMut() -> Result<ExitStatus>`, and the relay thread's `JoinHandle<Option<Terminal>>` reaches it through `Child::with_relay`, so the two calls above fit as written. `supervise(&mut child, || ())` then runs the wait — which joins the relay before it returns — and the `Grants` guard's `Drop` covers a `Child` that is never waited on.

The facade's Windows arm in `crates/tinio-sandbox/src/lib.rs` gains **two names from this phase** — the child-side listener rebuild, under the platform-neutral name P3 also uses, and the stop hook — plus **this platform's cfg-gated alias**, which is the path a consumer names to reach the settings the shared surface deliberately does not carry (design decision 2; P1's Linux arm is the same line for `linux`), and nothing else from the platform's `launch` module:

```rust
#[cfg(target_os = "windows")]
pub use crate::_windows as windows;

#[cfg(target_os = "windows")]
pub use tinio_sandbox_windows::launch::{listener as adopted_listener, on_stop};

// **Nothing is added to the core's re-export list here, and that is the rule
// rather than an omission.** The items the launcher path names — `Event`,
// `Observer`, `Adopted`/`adopt`, the argv-element helpers — are core items, and
// P0's Task 3 facade list already carries every one of them; a second
// `pub use tinio_sandbox_core::{…}` in this arm would be exactly the drift P0
// exists to prevent. (`reemit` in particular is not on that list and must not
// be: it is `pub(crate)` in the core, so a facade naming it is a compile error
// rather than a shortcut.)
```

**The two names are there because the child-side path is shared.** `serve`'s `confined` is gated by `feature = "sandbox"` alone, with no `target_os`, so it is one path across all three platforms and can name neither platform crate; the facade is where each call such a path makes gets one platform-neutral name, `#[cfg]`-selected, and nothing else from the platform's `launch` module rides along — `adopted_listener` rebuilds the inherited listener from the descriptor the payload carried, `on_stop` is the hook a forwarded stop lands on, and the names no shared call site uses (`command_args`, the launcher's own argv construction, and `disarm`, which is reached at most from an already-Windows-gated arm) stay on the platform crate. P3's facade arm is the same shape — `pub use tinio_sandbox_macos::launch::listener as adopted_listener;` — which is why the name is spelled identically in both plans. **A platform's own settings are not on it either**, and where this arm reaches them is design decision 2's boundary to state rather than this plan's: the alias that section names is `tinio_sandbox::windows` here, and a consumer writes `tinio_sandbox::windows::Options` inside the `#[cfg(target_os = "windows")]` block it would have written anyway. `Backend as Platform` and `backend as platform` are P0's selection and P2 restates neither, and `confine` and `socket` stay in this crate, where any caller that has already named it can reach them.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-windows`
Expected: PASS — the whole crate's suite, including the volume loop.

- [ ] **Step 5: Run it on both volumes**

Run:
```powershell
$env:TINIO_SANDBOX_TEST_VOLUMES = 'C:\;E:\'
cargo test -p tinio-sandbox-windows -- --nocapture
```
Expected: PASS on both. `Get-Volume -DriveLetter C` and `-DriveLetter E` report the filesystems; the run must show at least one NTFS and one ReFS volume, which is the design's "must run on both" (§12). If the second volume is not ReFS on this machine, the commit body records which filesystems were actually covered rather than claiming ReFS.

---

### Task 10: The stop semantics, and the relay

**Files:**
- Create: `crates/tinio-sandbox-windows/src/control.rs`
- Modify: `crates/tinio-sandbox-windows/src/backend.rs` (one call)
- Modify: `crates/tinio-sandbox-windows/src/launch.rs` (restore `on_stop`)
- Create: `crates/tinio-sandbox-windows/tests/signal.rs`
- Modify: `crates/tinio-sandbox-windows/src/lib.rs`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (the stop-semantics finding)

**Interfaces:**
- Consumes: `ffi`, `handles::step`, `process::LaunchedProcess::pid`.
- Produces: `control::{arm, send_stop, on_stop, disarm}`.

Design §3 step 9 and decision 20's second half. **Two obligations live in the word "forward" and this task owns both, distinctly**: the *exit-status* forwarding is §3 step 8 and already exists (the wait closure returns the child's status to `supervise`, and the call site exits with it); the *signal* relaying is this task — the stop the launcher receives while it waits is passed to the child.

**Two relays, and they stay two threads.** The stdout relay of decision 19 — core's `Observer`, driven by a thread this crate spawns over the child's stdout, whose `JoinHandle` goes to `Child::with_relay` — drains the child's events and re-emits them; this task's relay forwards a stop. They are separate, deliberately: the stdout relay blocks on a pipe read and ends when the child closes its stdout, while the signal relay is woken by the console handler through an event object and must stay alive exactly as long as the child does — one thread cannot block on both without a third waitable object, and merging them would put a pipe read in the path between a stop arriving and the child hearing about it. Neither is on the handler's critical path: the handler itself makes one `GenerateConsoleCtrlEvent` call and signals one event, and the threads do everything that allocates. That is also what keeps the handler's constraint satisfied — everything it touches is a static atomic, an event handle and two integers, all reachable from a console-control or service-control callback.

**The premise was measured before the task was written, and it is not about containers at all.** §3 step 9 originally argued the hazard from container behaviour; the measurement of 2026-09-19 shows the hazard is created by `CREATE_NEW_PROCESS_GROUP` — the flag Task 8 passes so that the relay has a group to address — and therefore holds on a plain host console too.

- [ ] **Step 1: The measurement this task is written against**

It was made on 2026-09-19, on this machine's host console, before this plan was executed. **Its rows live in the design's evidence table** (the console-control rows, and §3 step 9); what follows is the conclusion, which is what the rest of the task builds on:

- A console-wide Ctrl+C — `GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0)`, which is what a keypress in that console produces — **does not reach** a child created with `CREATE_NEW_PROCESS_GROUP`: the launcher's own handler fires in the same millisecond and the child's never fires at all, while the identical call reaches a child created *without* the flag. So the requirement comes from this plan's own Task 8 flag, not from a container.
- A targeted `GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, <child pid>)` **is** delivered to the child's group, so the relay has something to send.
- The relay's forward **does** arrive, and a handler that returns TRUE consumes the event: the forward delivers, the child's hook decides. That is the division of labour `crate::control` is built on.
- `GenerateConsoleCtrlEvent`'s **return value is not evidence of delivery** — `CTRL_C_EVENT` addressed at a pid returns 1 with `GetLastError` 0 and delivers nothing — which is why [`send_stop`]'s documentation says so rather than promising a stop that arrived.

**So the task stands as written: the relay is built.** The branch that would have deleted it — "the child is reached anyway, so collapse the task to a comment" — was measured and is false on the host console. The one thing not measured is what a Windows *container* does with the stop: this machine has no Windows-container engine (`docker version` reports a Linux engine only, and the design's §3 step 9 says so). That row stays **unverified in this environment**, and it is no longer load-bearing, because `CREATE_NEW_PROCESS_GROUP` alone is enough to require the relay.

- [ ] **Step 2: Write the failing tests**

`crates/tinio-sandbox-windows/tests/signal.rs`:

```rust
//! The relay, measured: a stop delivered to the launcher's console process group
//! reaches the confined child only because the launcher forwards it (design §3
//! step 9).

#![cfg(windows)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tinio_sandbox_windows::control;

/// The launcher's own role: run the production launch path and supervise it.
///
/// It is a separate process so that a *console control event addressed to it* has
/// somewhere to land: sending one to the test process would test nothing.
#[test]
fn launcher_role() {
    if std::env::var_os("TINIO_SANDBOX_TEST_LAUNCHER").is_none() {
        return;
    }
    let tree = PathBuf::from(std::env::var("TINIO_SANDBOX_TEST_TREE").expect("tree"));
    let Some((confinement, _address)) = common::launch_confined_at(
        &tree,
        tinio_sandbox_core::AccessMode::ReadWrite,
        Default::default(),
        None,
    ) else {
        return;
    };
    let tinio_sandbox_core::Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    let status = tinio_sandbox_core::supervise(&mut child, || ()).expect("supervise");
    std::process::exit(status.code().unwrap_or(1));
}

/// The confined child, with a heartbeat the test reads instead of a log.
#[test]
fn child_role() {
    // The unrelayed control case passes the probe in the environment; the
    // confined case receives it in the payload. Either way the body is one
    // function, and a process with neither is the launcher.
    if let Ok(probe) = std::env::var("TINIO_SANDBOX_TEST_PROBE") {
        let probe = std::fs::read_to_string(&probe).expect("read the probe file");
        common::heartbeat(&common::probe_dir(&probe).join("beat"));
    }
    common::child_role_with_heartbeat();
}

/// Start the launcher in its own process group, so a targeted console control
/// event reaches it and nothing else.
fn spawn_launcher(tree: &Path) -> Child {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

    Command::new(std::env::current_exe().expect("current exe"))
        .args(["launcher_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_LAUNCHER", "1")
        .env("TINIO_SANDBOX_TEST_TREE", tree)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn the launcher")
}

/// Wait for `beat` to stop growing.
fn heartbeat_stopped(beat: &Path, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    let mut last = std::fs::read(beat).unwrap_or_default().len();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let now = std::fs::read(beat).unwrap_or_default().len();
        if now == last {
            return true;
        }
        last = now;
    }
    false
}

#[test]
fn a_stop_addressed_at_the_launcher_reaches_the_confined_child() {
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let beat = tree.report.join("beat");
    let mut launcher = spawn_launcher(&tree.path);

    // Wait for the heartbeat to appear: the child is confined and running.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !beat.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(beat.exists(), "the confined child never started");

    // Skip rather than fail where there is no console to address: a console
    // control event needs one, and a detached test runner has none.
    if let Err(err) = control::send_stop(launcher.id()) {
        eprintln!("skipping: no console to deliver a stop on — {err}");
        let _ = launcher.kill();
        let _ = launcher.wait();
        return;
    }

    assert!(
        heartbeat_stopped(&beat, Duration::from_secs(15)),
        "the confined child kept running after the launcher was stopped: the relay did not forward"
    );
    let status = launcher.wait().expect("the launcher exits with its child's status");
    assert!(status.code().is_some(), "the launcher reported its child's status");
}

#[test]
fn the_same_stop_without_a_relay_is_not_forwarded() {
    // The control that makes the test above mean something: the same console
    // control event, addressed the same way, to a launcher that does *not*
    // relay. Without this case, "the child died" would be consistent with the
    // child having been reached directly.
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let (_dir, tree) = common::tree(&volume);
    let beat = tree.report.join("beat");
    // **The probe is written here because nothing else writes it in this case.**
    // Every other test in the plan launches through `launch_confined`, and
    // `Tree::probe` is part of launching; this child is spawned directly, so the
    // file has to exist before the spawn — otherwise the child panics in
    // `read_to_string` before it ever beats, and `beat.exists()` below fails for
    // a reason that has nothing to do with the relay.
    let probe = tree.probe();
    let child = common::spawn_unrelayed_child_in_own_group(&probe);

    let deadline = Instant::now() + Duration::from_secs(30);
    while !beat.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(beat.exists(), "the child never started");

    if control::send_stop(child.id()).is_err() {
        eprintln!("skipping: no console to deliver a stop on");
        return;
    }
    assert!(
        !heartbeat_stopped(&beat, Duration::from_secs(5)),
        "a stop addressed at one process group reached a process in another: \
         design §3 step 9's premise does not hold here, and the relay is not needed"
    );
}
```

`spawn_unrelayed_child_in_own_group` spawns the same role with `CREATE_NEW_PROCESS_GROUP` and no `confine` in between, so the child is a grandchild in another group with nothing relaying — the control case. It lives in `common`, not in the suite, and Step 3 below is where it is written: the harness is where a role and the process that starts it agree on a contract. `common::launch_confined_at` and `common::child_role_with_heartbeat` are the other two additions this suite needs: the first takes the tree's *path* (a launcher role is its own process and cannot receive a `Tree`), and the second writes one byte to `<report>/beat` every 200 ms before doing everything `child_role` already does.

- [ ] **Step 3: Extend the harness**

`crates/tinio-sandbox-windows/tests/common/mod.rs` gains the heartbeat helpers — `launch_confined_at` landed in Task 9, since the events suite uses it too:

```rust
// `launch_confined_at` (Task 9) already gives a launcher process a confined
// child from a tree path; what is new here is the heartbeat.

/// The report directory a probe file names.
pub fn probe_dir(probe: &str) -> PathBuf {
    probe
        .lines()
        .find_map(|line| line.strip_prefix("report="))
        .map(|value| PathBuf::from(value))
        .and_then(|report| report.parent().map(Path::to_path_buf))
        .expect("the probe carries a report path")
}

/// Beat once every 200 ms into `<report>/beat`.
///
/// The file's growth is the child's liveness, and it stops the moment the process
/// dies — which is the observable a console control event leaves behind when no
/// handler is installed.
pub fn heartbeat(beat: &Path) -> ! {
    loop {
        append_to_report(beat, ".");
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
}

/// The confined child, beating while it does everything `child_role` does.
pub fn child_role_with_heartbeat() {
    // The adopt guard first: this role is inert in the launcher process, and the
    // heartbeat must not run there.
    let Some(adopted) =
        tinio_sandbox_windows::launch::adopt(std::env::args_os(), std::io::stdin().lock())
            .expect("adopt")
    else {
        return;
    };
    let listener = tinio_sandbox_windows::launch::listener(&adopted).expect("a listener");
    let probe_path = adopted.config().to_owned();
    let probe = std::fs::read_to_string(&probe_path).expect("read the probe file");
    let beat = probe_dir(&probe).join("beat");
    std::thread::spawn(move || heartbeat(&beat));
    child_role_body(adopted.config(), listener);
}

/// A child in its own process group with nothing supervising it: the control case
/// for the relay, and the case whose failure means the premise of design §3 step 9
/// does not hold here.
///
/// It takes the **probe file's path** rather than the tree's, because the caller
/// has already written it (`Tree::probe`) and this function must name the same
/// file: the child reads it, and a path that does not exist is a panic rather
/// than a skipped case. The environment is the channel here and it is the only
/// place in the harness that uses one — legitimate precisely because this child
/// is *not* confined, and the test is measuring a console control event rather
/// than a payload.
pub fn spawn_unrelayed_child_in_own_group(probe: &Path) -> std::process::Child {
    use std::os::windows::process::CommandExt;

    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

    std::process::Command::new(std::env::current_exe().expect("current exe"))
        .args(["child_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_PROBE", probe)
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .expect("spawn the unrelayed child")
}
```

Three small consequences in the harness: `child_role`'s body moves into `child_role_body(config: &str, listener: TcpListener)` — **`&str` and not `Vec<u8>`**, because that is what core's `Adopted::config()` returns and what `Config::parse` takes — and core's `Adopted` is consumed by the role, which rebuilds the socket before delegating, so both roles share it; the signal suite's `child_role` test dispatches on the environment first, so the unrelayed child beats and does nothing else; and the unrelayed case is the one place the harness uses the environment channel — which is fine there precisely because that child is not confined and the test is measuring a console control event, not a payload:

```rust
#[test]
fn child_role() {
    // The unrelayed control case passes the probe in the environment; the
    // confined case receives it in the payload. Either way the body is one
    // function, and a process with neither is the launcher.
    if let Ok(probe) = std::env::var("TINIO_SANDBOX_TEST_PROBE") {
        let probe = std::fs::read_to_string(&probe).expect("read the probe file");
        common::heartbeat(&common::probe_dir(&probe).join("beat"));
    }
    common::child_role_with_heartbeat();
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-windows --test signal`
Expected: FAIL — `cannot find function 'send_stop'`, and the control case failing would mean the premise does not hold (Step 1's second row).

- [ ] **Step 5: Write `control.rs`**

```rust
//! The termination relay (design §3 step 9, decision 20's second half).
//!
//! A Windows container signals its main process and nothing else, so on Windows
//! the launcher is the only process that learns the server should stop — and the
//! confined child, which is not its parent's process group, learns nothing.
//! Without the relay the server is hard-terminated at the end of the container's
//! grace period instead of draining, and nothing about that looks like a bug
//! from the outside.
//!
//! Windows has no `SIGTERM`, so the mechanism is a console control event:
//! `CTRL_BREAK_EVENT` is the only one that can be addressed to a process group,
//! and the child is created with `CREATE_NEW_PROCESS_GROUP` so it has one
//! ([`crate::process`]).
//!
//! The handler does two things and nothing else, both of them one call on values
//! that already exist: forward with `GenerateConsoleCtrlEvent` when the process
//! is a launcher, and signal an event object when it is a child. Everything that
//! allocates, locks or talks to a runtime happens on the thread waiting on that
//! event, which is where a service-control callback could equally well hand off —
//! so the handler's constraints hold whichever supervisor is asking the process
//! to stop.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14).

#![allow(unsafe_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use tinio_sandbox_core::{Error, Result};

use crate::ffi::{self, Handle};
use crate::handles::{last_error, step};

/// The child's process id — the only state the handler reads.
///
/// A process id rather than a handle: `GenerateConsoleCtrlEvent` takes a process
/// *group* id, the child is its own group leader, and an integer needs no
/// ownership in a handler.
static CHILD: AtomicU32 = AtomicU32::new(0);

/// Arm the relay for `child`.
///
/// Called by [`crate::backend`] once the child exists and is owned by the job,
/// before the caller starts supervising.
///
/// # Errors
///
/// [`Error::Launch`] when the handler cannot be installed or the relay's event
/// object cannot be created.
pub fn arm(child: u32) -> Result<()> {
    CHILD.store(child, Ordering::SeqCst);
    relay_event()?;
    // SAFETY: `console_handler` is a plain `extern "system"` function with the
    // signature SetConsoleCtrlHandler expects, and it only touches an atomic and
    // an event handle that live for the process's lifetime.
    let ok = unsafe { ffi::SetConsoleCtrlHandler(Some(console_handler), 1) };
    if ok == 0 {
        return Err(step("SetConsoleCtrlHandler", "relay", last_error()));
    }
    // Shared §7's "armed relay", with the value the forward will address: the
    // child's process id, which is its process-group id because it was created
    // with CREATE_NEW_PROCESS_GROUP.
    tracing::debug!(step = "SetConsoleCtrlHandler", role = "launcher", child = child);
    Ok(())
}

/// Deliver a stop to `group`'s process group.
///
/// This is the relay's whole action, and it is public because it is also the way
/// to play the container's part in a test.
///
/// **`Ok` means the call was accepted, not that the event was delivered.**
/// `GenerateConsoleCtrlEvent` returns 1 with `GetLastError` 0 for a
/// group-addressed `CTRL_C_EVENT` and delivers nothing at all (measured; the
/// design's evidence table has the rows). What this function sends is
/// `CTRL_BREAK_EVENT`, which *is* delivered to the group addressed — and it is
/// the only console control event that can be limited to one.
///
/// # Errors
///
/// [`Error::Launch`] when there is no console to deliver on, or the call is
/// refused.
pub fn send_stop(group: u32) -> Result<()> {
    // SAFETY: GenerateConsoleCtrlEvent takes two integers and dereferences
    // nothing.
    let ok = unsafe { ffi::GenerateConsoleCtrlEvent(ffi::CTRL_BREAK_EVENT, group) };
    if ok == 0 {
        return Err(step(
            "GenerateConsoleCtrlEvent",
            group.to_string(),
            last_error(),
        ));
    }
    // Accepted, not delivered — the doc above is explicit about that, so the
    // event says what this function knows: the call went out, addressed at
    // `group`.
    tracing::debug!(step = "GenerateConsoleCtrlEvent", group = group, event = "CTRL_BREAK_EVENT");
    Ok(())
}

/// Unhook the relay, and put the process back on the default disposition.
///
/// Installing the handler twice would forward every event twice, so a process
/// that lives past one launch — the test harness, and a future CLI that serves
/// more than once — disarms before the next: the handler is removed and the
/// child id is cleared, so a late event cannot be forwarded to a process that is
/// already gone.
///
/// **Removing it takes the same routine, and that is the whole of this function
/// rather than a detail of it.** `console_handler` answers `TRUE` for every event
/// it matches, and `TRUE` means *handled*: a handler left installed would go on
/// swallowing `CTRL_C`, `CTRL_BREAK`, `CTRL_CLOSE` and the logoff and shutdown
/// events instead of relaying them, so the process would neither be stopped by
/// them nor act on them: it would ignore a Ctrl+C, and it would neither act on a
/// closing window nor on a logoff or shutdown — the events that normally end a
/// console session, and the ones the system waits on, up to its own short grace
/// period, before it stops waiting. Swallowing those is the version of this that
/// is hardest to notice and hardest to recover from. A `NULL` routine with
/// `add = 0` does **not** remove a registered routine: it clears the
/// "ignore Ctrl+C" entry, which this process never set. Passing
/// `Some(console_handler)` back with `add = 0` is the documented removal, and
/// after it the process behaves like one that never armed a relay.
///
/// `serve`'s launcher arm never reaches this call — it forwards the status and
/// exits — so the window this closes is the one a process that outlives its
/// launch has: the test harness, and any future CLI that serves twice.
///
/// If the removal itself fails, the handler *is* still installed, and that is the
/// one case this function cannot hide: the id has been cleared first, so it
/// forwards nothing — but it goes on swallowing, and the returned error is the
/// only notice of it.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the handler cannot
/// be removed.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::control;
///
/// control::arm(1234)?;
/// control::disarm()?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn disarm() -> Result<()> {
    CHILD.store(0, Ordering::SeqCst);
    // SAFETY: the same `console_handler` this module installs, with `add = 0`,
    // which is the documented way to remove that routine; the pointer is passed
    // through and never dereferenced.
    let ok = unsafe { ffi::SetConsoleCtrlHandler(Some(console_handler), 0) };
    if ok == 0 {
        return Err(step("SetConsoleCtrlHandler", "disarm", last_error()));
    }
    tracing::debug!(step = "SetConsoleCtrlHandler", role = "disarm");
    Ok(())
}

/// Run `handler` when this process is stopped.
///
/// The child-side counterpart: the confined server needs somewhere for a
/// forwarded stop to land, or it dies on the spot instead of draining. The
/// handler runs on a dedicated thread rather than in the console handler, so it
/// may take locks, allocate and talk to a runtime — which is what makes this the
/// hook the CLI's graceful shutdown (T066/T067) lands on.
///
/// # Errors
///
/// [`Error::Launch`] when the hook cannot be armed.
pub fn on_stop(handler: impl Fn() + Send + 'static) -> Result<()> {
    let event = relay_event()?;
    std::thread::spawn(move || {
        // SAFETY: `event` is this process's own manual-reset event, and waiting
        // on it is what the handle is for.
        let waited = unsafe { ffi::WaitForSingleObject(event, ffi::INFINITE) };
        if waited == ffi::WAIT_OBJECT_0 {
            // **This is where a forwarded stop is traced, and the only place it
            // can be**: the handler that sent it runs on a thread the system
            // created and may not allocate or lock, which rules `tracing` out
            // there (shared §7, Global Constraints). This thread is the child's
            // own, with a stack and a runtime to come, so it is where the event
            // belongs.
            tracing::debug!(step = "on_stop", "the launcher forwarded a stop");
            handler();
        }
    });
    // SAFETY: the same `console_handler`, in the child: it stores nothing but
    // the event signal, and the thread above turns that into the caller's
    // closure. **It is named `console_handler` and not `handler` because the
    // parameter above already took that name** — a closure cannot be handed to
    // `SetConsoleCtrlHandler`, whose parameter is an `unsafe extern "system" fn`,
    // and an earlier draft passed `handler` here and did not compile.
    let ok = unsafe { ffi::SetConsoleCtrlHandler(Some(console_handler), 1) };
    if ok == 0 {
        return Err(step("SetConsoleCtrlHandler", "stop hook", last_error()));
    }
    Ok(())
}

/// The process-wide manual-reset event the handler signals.
///
/// One event for the process: a launcher relays, a child runs its hook, and no
/// process is both. The handler must not create it — a console handler runs on
/// an arbitrary thread at an arbitrary moment — so the cell is initialised by
/// [`arm`] or [`on_stop`], both of which run on the ordinary path, and the
/// handler only reads it.
static EVENT: OnceLock<Handle> = OnceLock::new();

/// Create the relay's event, once per process.
fn relay_event() -> Result<Handle> {
    if let Some(handle) = EVENT.get() {
        return Ok(*handle);
    }
    // SAFETY: a null security descriptor, a null name, manual reset and a
    // non-signalled initial state are all valid for CreateEventW.
    let handle = unsafe { ffi::CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if handle.is_null() {
        return Err(step("CreateEventW", "relay", last_error()));
    }
    let _ = EVENT.set(handle);
    Ok(handle)
}

/// The console control handler: one forward, one signal, and nothing else.
///
/// It runs on a thread the system creates, on an arbitrary stack, and it may be
/// the last thing this process does — so it reads one atomic, makes one
/// `GenerateConsoleCtrlEvent` call and signals one event. Everything that needs
/// a lock, an allocation or a runtime happens on the thread that waits on that
/// event.
///
/// **It answers `TRUE` for every event it matches, and `TRUE` means *handled*
/// rather than *forwarded*.** Nothing else gets a say in that event: the process's
/// default disposition does not run, so this process is not stopped by a Ctrl+C
/// and does not end with its console window. That is what the relay wants while a
/// child is being supervised, and it is why [`disarm`] removes this routine rather
/// than leaving it installed — the swallowing is meant to last exactly as long as
/// the relay does.
///
/// **It emits no `tracing` event either, and that is a constraint rather than an
/// oversight**: every macro in that crate formats, allocates and can take a lock
/// to reach its subscriber, none of which this thread may do. The forward is
/// traced on the child's hook thread instead ([`on_stop`]), which is the process
/// on the other end of it, and the launcher's half — the arming and the id it
/// addresses — was traced by [`arm`] before any event could arrive.
unsafe extern "system" fn console_handler(event: u32) -> i32 {
    match event {
        ffi::CTRL_C_EVENT
        | ffi::CTRL_BREAK_EVENT
        | ffi::CTRL_CLOSE_EVENT
        | ffi::CTRL_LOGOFF_EVENT
        | ffi::CTRL_SHUTDOWN_EVENT => {
            let child = CHILD.load(Ordering::SeqCst);
            if child != 0 {
                // A launcher: forward to the child's group.
                // `GenerateConsoleCtrlEvent` is one of the calls a console
                // handler may make, and this is its only use here.
                // SAFETY: two integers; nothing is dereferenced.
                unsafe { ffi::GenerateConsoleCtrlEvent(ffi::CTRL_BREAK_EVENT, child) };
            }
            if let Some(handle) = EVENT.get() {
                // A child: wake the hook's thread and let it do the real work.
                // SAFETY: the handle is this process's own event, created before
                // the handler was installed.
                unsafe { ffi::SetEvent(*handle) };
            }
            1
        }
        _ => 0,
    }
}
```

- [ ] **Step 6: Arm the relay, restore the child's hook, run the tests**

In `backend.rs`'s closure, right after the job assignment:

```rust
            // The relay is armed once the child exists and the job owns it
            // (design §3 step 9): a stop that arrives before the child exists has
            // nothing to forward to, and from this point on the job's
            // kill-on-close covers a launcher that dies first — the window before
            // the assignment is Task 5's recorded gap, not this one's.
            crate::control::arm(launched.pid())?;
```

and restore `launch::on_stop`'s body (Task 7 left it commented out).

Run:
```powershell
cargo test -p tinio-sandbox-windows --test signal -- --nocapture
cargo test -p tinio-sandbox-windows
```
Expected: PASS — the relay forwards, the control case shows a targeted stop does not reach another group's process, and the rest of the suite is unchanged. Both cases print their outcome; keep the output, the next step records it.

- [ ] **Step 7: Check the tests against the rows the design already carries**

`docs/superpowers/specs/2026-09-18-sandbox-design.md` already holds the stop-semantics rows — the console-wide Ctrl+C that does not reach a `CREATE_NEW_PROCESS_GROUP` child, the targeted `CTRL_BREAK_EVENT` that does, the forward that closes the gap, and the caveat that the API's return value is not evidence of delivery — written on 2026-09-19 from the same probe this task's tests reproduce. There is no branch left to record: the measurement is in, and what Step 6's run has to do is reproduce it. If it does **not** — if the control case shows the child reached with no relay in between — then §3 step 9, decision 20 and this task's code are wrong together, and the commit that says so deletes the relay rather than keeping it. The container row stays **unverified in this environment**, as Step 1 and the design both say.

---

### Task 11: `serve` on Windows, and the confined-server end-to-end test

**Files:**
- Modify: `crates/tinio-config/src/schema/server.rs`
- Modify: `crates/tinio-server/examples/serve.rs`
- Modify: `crates/tinio-server/Cargo.toml`
- Create: `crates/tinio-server/tests/sandbox_e2e_windows.rs`

**Interfaces:**
- Consumes: `tinio_sandbox::{AccessMode, Child, Confinement, Event, Exposure, LaunchPlan, Sandbox, adopt, adopted_listener, is_listener_fd_arg, on_stop, supervise}`; on the Windows arm `tinio_sandbox::windows::{Backend, Capability, Options}` — the facade's cfg-gated alias for this platform's crate, not a dependency of this crate's own (design decision 2) — plus `tinio_sandbox::windows::control::send_stop` in the end-to-end test; `tracing` and `tracing_subscriber` (both already dependencies of `tinio-server` — the launcher installs the sink `serve`'s own subscriber cannot reach); `tinio_config::server::probe_read_only`.
- Produces: nothing the library uses.

Three things change in the entry point, in the design's order: the payload discriminator comes first (§10 step 1), the configuration is read as **bytes** and parsed only where it stays in the process (decision 18), and the endpoint travels as an **event** on stdout — emitted by whichever process bound the socket, exactly once (decision 19). Since the child's stdout is a pipe the parent relays, the parent's own stdout is the operator's only view of the protocol, and nothing else on this path may write to it. The macOS arm does not exist yet, so a sandbox request there must fail closed rather than serve unconfined (§11).

- [ ] **Step 1: Add the targeted lookup to `tinio-config`**

**This step is the edit; P3 verifies it and does not write it again** (`2026-09-18-sandbox-shared.md` §4). P2 is the earlier of the two phases that need the function, so the whole of it — the function and its test — lands here, in the crate that owns the schema. The launcher must not build a `Config` (decision 18), and `Config::parse` is deliberately strict — it rejects unknown keys and validates the whole document, which is not what a one-key read should do.

`crates/tinio-config/src/schema/server.rs`:

```rust
/// Whether a configuration document sets `[server] read_only`, without parsing
/// the rest of it.
///
/// The platforms that launch a confined child must know this bit in the
/// unconfined launcher, where a parsed [`crate::Config`] must not be retained —
/// it holds credentials (design decision 18: "no retained parse, not no
/// contact"). Two fields are deserialized, everything else is discarded by
/// serde, and an empty document means "not read-only", which is the default
/// [`Config::default`] carries.
///
/// # Errors
///
/// The document's own parse error, for bytes that are not TOML or that set the
/// key to something other than a boolean: the launcher reports what it could not
/// read rather than guessing at an access mode.
///
/// # Examples
///
/// ```
/// use tinio_config::server::probe_read_only;
///
/// assert!(!probe_read_only(b"").unwrap());
/// assert!(probe_read_only(b"version = 1\n[server]\nread_only = true\n").unwrap());
/// ```
pub fn probe_read_only(bytes: &[u8]) -> Result<bool, crate::error::Error> {
    #[derive(serde::Deserialize)]
    struct ServerKey {
        #[serde(default)]
        read_only: bool,
    }
    #[derive(serde::Deserialize)]
    struct Document {
        #[serde(default)]
        server: Option<ServerKey>,
    }

    if bytes.is_empty() {
        return Ok(false);
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|err| crate::error::parse(Path::new("(bytes)"), err.to_string()))?;
    let document: Document = toml::from_str(text)
        .map_err(|err| crate::error::parse(Path::new("(bytes)"), err.to_string()))?;
    Ok(document.server.map(|server| server.read_only).unwrap_or(false))
}
```

with tests in the same file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_reads_only_the_read_only_key() {
        assert!(!probe_read_only(b"").unwrap());
        assert!(!probe_read_only(b"version = 1\n").unwrap());
        assert!(probe_read_only(b"version = 1\n[server]\nread_only = true\n").unwrap());
        assert!(!probe_read_only(b"version = 1\n[server]\nread_only = false\n").unwrap());
        assert!(probe_read_only(b"version = 1\n[server]\nread_only = \"yes\"\n").is_err());
        // Credentials and every other key are ignored, and nothing of them is
        // retained: this is the launcher's whole view of the document.
        let with_secrets = b"version = 1\n[server]\nread_only = true\n[auth]\naccess_key = \"a\"\nsecret_key = \"b\"\n";
        assert!(probe_read_only(with_secrets).unwrap());
    }
}
```

Run: `cargo test -p tinio-config probe_reads_only` — FAIL, then PASS.

- [ ] **Step 2: Write the failing tests in the example**

`crates/tinio-server/examples/serve.rs`, appended to the existing `mod tests`:

```rust
    // The executable's directory is **not** tested here. It is this crate's own
    // backend that merges it into the whitelist (`backend::exposures`, Task 3,
    // decision 22) and Task 3 is where its tests live; the helper below is P1's,
    // so a test of it here would be a second copy of both.

    // The gate repeats the one above it, and for the same reason: this test names
    // the facade's Windows alias, which does not exist on the other two
    // targets (shared §2).
    #[test]
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    fn the_capability_list_comes_from_the_environment_and_defaults_to_empty() {
        assert!(capabilities_from(None).unwrap().is_empty());
        assert!(capabilities_from(Some("")).unwrap().is_empty());
        assert_eq!(
            capabilities_from(Some("privateNetworkClientServer")).unwrap(),
            vec![tinio_sandbox::windows::Capability::PrivateNetworkClientServer]
        );
        assert!(capabilities_from(Some("bogus")).is_err());
    }

    #[test]
    fn the_endpoint_travels_as_an_event_and_round_trips() {
        let event = Event::Listening { addr: "127.0.0.1:9000".parse().expect("addr") };
        let frame = event.encode();
        // SSE: an `event:` line, a `data:` line and a blank line — the blank one
        // is what stops a message containing a newline from splitting into two
        // frames (decision 19). The exact bytes are pinned in the core, where the
        // encoding lives, so this asserts the shape rather than repeating them.
        assert!(frame.starts_with("event: listening\n"), "{frame:?}");
        assert!(frame.contains("\ndata: "), "{frame:?}");
        assert!(frame.ends_with("\n\n"), "{frame:?}");
        assert_eq!(Event::decode(frame.trim_end()), Some(event));
    }
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-server --example serve`
Expected: FAIL — `cannot find function 'capabilities_from'`.

- [ ] **Step 4: Write the example's launcher path**

In `crates/tinio-server/examples/serve.rs`. **`emit` and `emit_error` are the one pair this list does not define, and the reason is the file**: both belong to P1 — `2026-09-18-sandbox-linux.md` Task 7 writes them beside `serve`'s signature, because that is the phase that restructures this file — and one file cannot carry two `fn emit` (`E0428`). It is the rule the plan already applies to `Args`, `parse_args`, `sandbox_exposures` and `serve`'s signature, and it costs these arms nothing: what they add to the helper is a **caller**, and the only frame they write through it is their own `Error` — a startup refusal, or a child that exited without ever reporting — because `Listening` belongs to the process that serves and the parent relays it (decision 19).

**`sandbox_exposures` is P1's and is not defined here either**, which the same rule settles: `2026-09-18-sandbox-shared.md` §4 gives every helper in this file one owner, P1 Task 7 writes this one, and P2 adds only its caller — `Sandbox::from_exposures(sandbox_exposures(Path::new(&args.root), &state, root_access), true)` in the arm below. **The executable's own directory is not this helper's to expose**: on Windows that entry is this crate's backend's — `backend::exposures` merges it into the whitelist and refuses to widen a path the caller exposed read-write (Task 3, decision 22, with its own tests) — so the helper stays the platform-neutral set P3 also calls, and no second copy of the rule belongs in this file.

```rust
#[cfg(feature = "sandbox")]
use tinio_sandbox::{AccessMode, Event, LaunchPlan, Sandbox};

/// The container's capabilities from `TINIO_SANDBOX_CAPABILITIES`.
///
/// Provisional until `[server] sandbox` and its capability list land with the
/// CLI: it is the only channel an operator has today for the one knob design §7
/// names (`privateNetworkClientServer`, for a non-loopback bind).
///
/// **`target_os = "windows"` as well as the feature, and the test below carries
/// the same gate.** The facade publishes this platform's crate under a
/// `#[cfg(target_os = "windows")]` alias (design decision 2), while the
/// `sandbox` feature is on by default everywhere — so on Linux and macOS, which
/// is where P1's legs and CI build, an item gated by the feature alone names an
/// alias that does not exist there and fails to resolve. This is shared §2's rule
/// in its second shape: a crate-root `#![cfg]` does not reach `tests/` or
/// `examples/`, so every separate target repeats the gate it needs, and no item
/// that is configured out may be named by one that is not.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
fn capabilities_from(
    raw: Option<&str>,
) -> Result<Vec<tinio_sandbox::windows::Capability>, Box<dyn Error + Send + Sync>> {
    tinio_sandbox::windows::Capability::parse_list(raw).map_err(Into::into)
}

/// Windows' step of design §10 step 6: bind the port, hand the socket over,
/// launch the confined child, and return its supervision.
///
/// **Windows-only, and deliberately so.** The platforms that confine in place
/// have nothing to hand back here: Linux binds *after* the confinement and its
/// own process emits the `Listening` event (decisions 6 and 19), so its arm lives
/// inline in `main` and belongs to `2026-09-18-sandbox-linux.md` Task 7. A shared
/// helper returning `Option<Child>` would have to invent a `None` no launcher
/// platform ever produces — which is exactly what an earlier draft of this plan
/// did, and it cost a call into an API no phase defines.
#[cfg(all(feature = "sandbox", target_os = "windows"))]
fn confine_or_launch(
    sandbox: &Sandbox,
    address: SocketAddr,
    config_bytes: &[u8],
) -> Result<tinio_sandbox::Child, Box<dyn Error + Send + Sync>> {
    {
        use tinio_sandbox::windows::{Backend, Options};

        let listener = std::net::TcpListener::bind(address)?;
        // **No `Listening` event is emitted here, even though this is the
        // process that bound the port.** The event means the data plane is
        // serving, and only the confined child can say that; the launcher's part
        // is to relay the child's frame, not to anticipate it (decision 19). An
        // event emitted here would reach the consumer twice — the child's own
        // frame arrives through the relay a moment later — and it would announce
        // a server that had not started.
        // The configuration is text on the wire (its `String` type is what makes
        // that a compile-time fact); `config_bytes` is only bytes because the
        // reader above hands them over that way.
        let plan = LaunchPlan::for_launch(String::from_utf8_lossy(config_bytes).into_owned())
            .with_listener(listener);
        // The container's capability list is a Windows setting with no
        // equivalent on the other two platforms, and the `use` above reaches it
        // through the facade's alias for this platform's crate rather than
        // through `platform()` — the facade returns whatever backend the build
        // targets, and an options value only one of them has cannot be handed to
        // it (design decision 2 owns that boundary, and §10's "the difference is
        // configuration rather than dispatch"). What stays cfg-free is the
        // dispatch: `Confinement` is the core's, and the match below is the same
        // on every platform.
        let options = Options {
            capabilities: capabilities_from(
                std::env::var("TINIO_SANDBOX_CAPABILITIES").ok().as_deref(),
            )?,
            ..Default::default()
        };
        match Backend::with_options(options).confine(sandbox, &plan)? {
            tinio_sandbox::Confinement::Supervise(child) => Ok(child),
            tinio_sandbox::Confinement::InPlace => {
                Err("the Windows backend must supervise its child".into())
            }
        }
    }
}
```

**There is no non-Windows arm, and that is the point.** An earlier draft gave this helper a `#[cfg(not(windows))]` branch that called `sandbox.enter(&LinuxOptions::default())` and returned `Ok(None)` — a call into an API no phase defines (P1's backend answers `Confinement::InPlace` from `confine`, and there is no `enter`), kept alive only so the helper could keep one shape. P1's Linux arm stays exactly as `2026-09-18-sandbox-linux.md` Task 7 wrote it, inline in `main`; this plan neither restates it nor wraps it (the one-definer rule in `docs/superpowers/README.md`).

**Nothing is added to the facade for this step.** The arm above reaches the container's options through the facade's `tinio_sandbox::windows` alias, and the two platform names the child side needs — `adopted_listener` and `on_stop` — are the re-exports Task 9 puts on the facade. That is decision 2's boundary: the facade shares what every platform needs to *drive* a confinement, and a backend's own settings stay in its own crate.

(a) this process's arguments, with the launcher's own element removed so the parser never sees it:

**This is the file's single `own_args`, and P3 calls it rather than writing a second one** — the rule `2026-09-18-sandbox-shared.md` §4 states for every helper that lands in this one file, and the same shape as Step 1's `probe_read_only`: this task is the earlier of the two launcher plans, so the definition is written here, and `2026-09-18-sandbox-p3-macos.md` Task 8's two call sites (`confined` and `main`) are calls. Two definitions of one function in one module is `E0428`, which is exactly what a second copy beside this one would be.

```rust
/// This process's arguments: `argv[0]` dropped, and the launcher's private
/// element filtered out (design §3: the child's argv carries the payload handle
/// and nothing else that is security-relevant).
fn own_args() -> Vec<OsString> {
    env::args_os()
        .skip(1)
        .filter(|arg| {
            #[cfg(feature = "sandbox")]
            {
                !tinio_sandbox::is_listener_fd_arg(arg)
            }
            #[cfg(not(feature = "sandbox"))]
            {
                let _ = arg;
                true
            }
        })
        .collect()
}
```

(b) the confined child, design §10 step 8:

```rust
/// The confined child: the configuration is the payload's **text**,
/// never a re-read of the path in `argv` — the file it names is outside every
/// exposure, and re-reading it would be denied by the confinement this process is
/// running under (design §3 step 2). `Adopted::config` is a `String` because
/// `Config` requires UTF-8 (P0's `Adopted` and `Adopted::config`), so there is no
/// bytes-to-text step at this call site.
///
/// **Everything the parent resolved from a host path is re-derived here, from the
/// same three sources and in the same order** (design §10: "the child re-derives
/// the identical answer from the payload's configuration bytes, the environment
/// and its inherited argv"). That is the mode — `[server] read_only` out of the
/// payload's text, overlaid by `TINIO_READ_ONLY` and then by `--read-only` — the
/// state directory, and the **`FsStorage` that owns it**. What the child must
/// **not** do is re-read a host path: the configuration file is outside every
/// exposure, which is why its bytes travel in the payload in the first place
/// (decision 18).
///
/// **The `storage` passed at the call below is this phase's one early
/// construction of an `FsStorage` on the sandbox path, and it is the child's
/// rather than a second instance of the parent's.** The launcher builds none at all — constructing one opens the state
/// database, and two processes on one state directory is not something the
/// storage layer is written for — so what the parent is left with is a path, and
/// the child builds the storage because it never ran the parent's
/// pre-confinement sequence. An unconfined run never reaches here at all, and
/// takes its storage from `serve` (`None` in the shared signature). The `watch`
/// channel is the same shape of fact: the child is what reacts to a forwarded
/// stop, so the channel is created here and handed to `on_stop`, not carried
/// across the launch (design §3 step 9).
///
/// **This is the process that emits `Listening`** — the only one that can, since
/// it is the one serving — and everything it prints to stdout is relayed by the
/// parent after decoding (decision 19).
#[cfg(all(feature = "sandbox", target_os = "windows"))]
fn confined(adopted: tinio_sandbox::Adopted) -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = parse_args(own_args())?;
    let config = match adopted.config() {
        "" => None,
        text => Some(Config::parse(text)?),
    };
    // The same chain `main` ran, on this process's own inputs (design §9's
    // precedence, unchanged: the file, then the environment, then the flag).
    let file_read_only = config.as_ref().is_some_and(|config| config.server.read_only);
    let read_only = sources::env_read_only()?.unwrap_or(file_read_only) || args.read_only;
    let access = AccessMode::from(read_only);
    let state_dir = if read_only {
        Some(match state::env_state_dir()? {
            Some(dir) => dir,
            None => state::default_home_state_dir(Path::new(&args.root))?,
        })
    } else {
        None
    };
    // **This phase's one early construction of an `FsStorage`**, and the parent is
    // deliberately not the other: it resolves the state directory's *path* (its
    // arm below), because building the storage opens the database this process is
    // about to open. What has to agree between the two processes is
    // the pair `state_dir`/`access` — the path the exposure set named and the
    // mode the run resolved — and both are re-derived here from the same three
    // sources rather than handed over (design §10).
    // The lanes `serve` is handed beside the storage — `2026-09-18-sandbox-shared.md`
    // §4 says why the value has to travel rather than be rebuilt inside the body.
    let pipelines = /* the lanes the options literal below names */;
    let storage = FsStorage::new(
        /* the root and options, with this `state_dir` and `access` */
    )?;
    let listener = tinio_sandbox::adopted_listener(&adopted)?;
    // A forwarded stop lands here (design §3 step 9). What it does is start the
    // same shutdown the runtime already knows: the server stops accepting, the
    // background tasks are signalled and the pipelines drain. The bounded
    // version of that — ≤10 s, state file removed — is the CLI contract's
    // (T066/T067) and lands on this hook.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    tinio_sandbox::on_stop(move || {
        let _ = shutdown_tx.send(true);
    })?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(
            args,
            config,
            storage,
            access,
            pipelines,
            Some(listener),
            Some(shutdown_rx),
        ))
}
```

(c) `main`, in the design's order:

```rust
fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Design §10 step 1, before anything else, so a confined child does no
    // privileged work on its way in. The payload is the only discriminator: an
    // environment marker would be forgeable, and a forged marker is a bypass of
    // the whole feature (decision 17).
    // `target_os` and not only the feature: `confined` is this platform's, and
    // the two facade names it calls are `#[cfg(target_os = "windows")]` too. On
    // Linux `adopt` would return `None` here anyway — that backend confines in
    // place and never re-invokes itself — but the call has to compile, and a
    // feature-only gate would reference a function that does not exist there.
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    if let Some(adopted) = tinio_sandbox::adopt(env::args_os(), std::io::stdin().lock())? {
        return confined(adopted);
    }

    let args = parse_args(own_args())?;

    // Step 3 reads the bytes; the launcher never builds a `Config` (decision 18),
    // so the only thing taken from the document here is one boolean.
    let config_bytes = match &args.config_path {
        Some(path) => fs::read(path)?,
        None => Vec::new(),
    };

    // FR-023 has landed, and the mode is resolved here exactly as `serve`
    // resolves it: the file's `[server] read_only` — through the probe this plan
    // adds in Step 1, because the launcher keeps no parsed `Config` (decision
    // 18) — overlaid by `TINIO_READ_ONLY` and then by the flag.
    let read_only = sources::env_read_only()?
        .unwrap_or(tinio_config::server::probe_read_only(&config_bytes)?)
        || args.read_only;
    let access = AccessMode::from(read_only);

    // Read-only mode serves a root that must already exist: creating it would
    // write the one thing the mode promises to leave alone.
    if !read_only {
        fs::create_dir_all(&args.root)?;
    }
    // **The launcher builds no `FsStorage`, and that is a correctness rule rather
    // than a preference.** Constructing one opens the state database, and the
    // confined child opens the same database a moment later — two processes on
    // one state directory is not something the storage layer is written for, and
    // on this platform the parent is not the process that serves. What the parent
    // needs from the storage is exactly one thing: **the state directory's path**,
    // because it is one of the exposures. It resolves that path directly, in the
    // arm below, and passes `None` for the storage wherever it would have called
    // `serve` — which, because it returns out of this arm, is nowhere: its last
    // call on the sandbox path is `confine_or_launch`, and the child it starts is
    // the process that reaches `serve`.
    //
    // The **entry sequence** is the one the three plans share — parse, read the
    // bytes, resolve the mode, create the root, resolve the state directory, then
    // either confine or serve — and it is shared as an order, not as a value.
    // Each platform answers the *storage* question for itself, and the signature
    // is what lets them: P1's Linux `main` builds one before `confine` because the
    // state directory is one of the jail's mounts, this plan's sandbox path does
    // not because a launcher must not open the database its child is about to
    // open, and the child builds its own (design §10, and `confined` above). A
    // shared *value* was the assumption the three plans had drifted apart on; the
    // order is the thing they really have in common. Note that this is a rule
    // about the launcher/child pair and not about the process: the plain run, a
    // few lines below, builds its own storage because there it *is* the server.

    #[cfg(unix)]
    let config = parse_config(&config_bytes)?;
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

    // Windows: bind, grant, launch, supervise (design §3 steps 3–9). The
    // `Listening` frame is the child's; the only frame this arm emits itself is
    // the one that turns "the child never reported" from an absence into an event
    // (decision 19).
    #[cfg(all(feature = "sandbox", target_os = "windows"))]
    if args.sandbox {
        // The **resolved** mode and not the configuration file's own field:
        // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
        // `access`, so reading `config.server.read_only` here would expose the
        // served root `ReadWrite` on a run the operator asked to be read-only
        // (P1's arm states the same rule, and `access` would otherwise have no
        // reader in this `main` now that the launcher builds no storage).
        // No conversion, and nothing left to keep in step: `access` **is** the
        // type the exposure set takes. This line used to rebuild an `Access`
        // from it, because the sandbox owned a second enum for the same fact.
        let root_access = access;
        // **The launcher's own sink for shared §7's events, installed before it
        // grants or launches anything.** `serve` builds its subscriber inside its
        // own body at a fixed `Verbosity::Info`, and this process returns from the
        // supervision below without ever reaching it — so without this line the
        // profile, the exposure set, the pid and the job would leave no record at
        // all, and a confinement that leaves no visible record is the failure the
        // requirement exists to prevent. stderr and not stdout, because stdout is
        // the protocol's (decision 19); `INFO` and not `DEBUG`, matching what
        // `serve` enables, because what this arm emits is the "was this process
        // confined, and to what" half; and `try_init` rather than `init`, so a
        // caller that installed a subscriber of its own keeps it.
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_max_level(tracing::Level::INFO)
            .try_init();
        // **The state directory's path, resolved for the exposure set and for
        // nothing else** — this arm is the only consumer in the parent, which is
        // why the resolution lives here rather than above it. Read-only mode
        // relocates state to `TINIO_STATE_DIR` or `<home>/.tinio/roots/<hash>/`,
        // so a hand-built `<root>/.tinio` would expose the wrong path and leave
        // the confined server unable to write its own state; in normal mode the
        // backend's own `<root>/.tinio` is the answer, and the root's exposure
        // already covers it, so `sandbox_exposures` adds no second entry. The
        // child re-derives the identical path from its own three sources (design
        // §10).
        let state = if read_only {
            match state::env_state_dir()? {
                Some(dir) => dir,
                None => state::default_home_state_dir(Path::new(&args.root))?,
            }
        } else {
            Path::new(&args.root).join(".tinio")
        };
        // Design §9's refused combination, refused here rather than inside the
        // confinement: a read-only root with a state directory under it cannot
        // be served, and `FsStorage` would reject it a process too late.
        if read_only && state.starts_with(Path::new(&args.root)) {
            return Err("read-only mode cannot serve a root that holds the state directory; \
                 set TINIO_STATE_DIR outside the served root"
                .into());
        }
        // The device-file flag is an argument of `from_exposures` rather than
        // something the rebuilt set remembers, so it is stated here instead of
        // being re-applied afterwards.
        let sandbox = Sandbox::from_exposures(
            sandbox_exposures(Path::new(&args.root), &state, root_access),
            true,
        );
        // A startup failure is an event, not an absence (decision 19): a consumer
        // that decodes the stream hears about it instead of timing out.
        let mut child = match confine_or_launch(&sandbox, args.address, &config_bytes) {
            Ok(child) => child,
            Err(err) => {
                emit(&Event::Error {
                    message: err.to_string(),
                });
                return Err(err);
            }
        };
        // §3 step 8: wait, and forward the child's status. §3 step 9's relay is
        // this platform's, armed inside `confine`. `supervise` **borrows** the
        // child, which is what lets the terminal record be read *here* — after
        // the wait, when the relay has been joined and the answer is final. A
        // child that exited without ever reporting is the one thing the parent is
        // the only witness to (decision 19), and `Listening` is not the parent's
        // to emit: only the process serving the data plane can say it is serving.
        let status = tinio_sandbox::supervise(&mut child, || ())?;
        if child.terminal().is_none() {
            emit(&Event::Error {
                message: format!(
                    "the confined child exited ({status}) without reporting readiness"
                ),
            });
        }
        return std::process::exit(status.code().unwrap_or(1));
    }

    // Linux: P1's arm stays where P1 wrote it — inline in `main`, in
    // `2026-09-18-sandbox-linux.md` Task 7 — because it shares nothing with the
    // arm above but the decision to sandbox: the process confines *itself*, binds
    // its own listener after the confinement, and emits its own `Listening` event
    // (design decisions 6 and 19). This plan neither restates it nor wraps it.

    #[cfg(windows)]
    let config = parse_config(&config_bytes)?;

    // Only now may threads exist. A process that reached this line is the server,
    // and it is never the launcher: `--sandbox` has already returned out of the
    // arm above.
    //
    // **The storage is built here, and that is the same rule as the sandbox arm's
    // seen from the other side**: a *launcher* must not open the state database
    // because its child will, while this process is the server and owns the only
    // handle. The call is the one the body makes today, verbatim — and it is built
    // before `config` is moved into `serve`, because its options read the
    // configured `local_uid` mappings.
    //
    // The lanes `serve` is handed beside the storage (`2026-09-18-sandbox-shared.md`
    // §4). What this call passes beside them is its own platform fact: no
    // inherited listener, because this process binds its own, and no stop
    // receiver, because nothing forwards a signal to something that is not a
    // child.
    let pipelines = /* the lanes the options literal below names */;
    let storage = FsStorage::new(&args.root, FsOptions { /* verbatim from the body */ })?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(args, config, storage, access, pipelines, None, None))
}

/// Parse the configuration bytes, or `None` when the operator gave none.
fn parse_config(bytes: &[u8]) -> Result<Option<Config>, Box<dyn Error + Send + Sync>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(Config::parse(std::str::from_utf8(bytes)?)?))
}
```

(d) `serve` — **the one entry point all three plans call, and `2026-09-18-sandbox-shared.md` §4 is where its signature is written down**: what it takes, in what order, and which of those parameters are optional are that row's, and this plan neither repeats the declaration nor re-argues it. What is this plan's is what its own two callers do and what the body does with the two substitutions:

```rust
    // ... the existing body, unchanged up to the bind ...
    let listener = match inherited {
        // The child's listener came from the launcher (decision 16), and the
        // event is still the child's to send: `Listening` means the data plane
        // is serving, which the launcher — outside the confinement, doing no
        // request work — is not the process that knows (decision 19). Emitting
        // it here as well would put the endpoint on stdout twice.
        Some(std) => TcpListener::from_std(std)?,
        None => TcpListener::bind(address).await?,
    };
    emit(&Event::Listening {
        addr: listener.local_addr()?,
    });
    // ... the rest, except that the data plane's shutdown receiver is the
    // caller's when one was handed in — that is the channel the stop hook flips,
    // and the reason `confined` has a channel to hand over at all ...
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // ... `scanner.run(shutdown_rx)` and the sweeper keep the process's own
    // channel, exactly as P1 left them ...
    plane
        .serve(listener, stop.unwrap_or_else(|| shutdown_tx.subscribe()))
        .await?;
    // ... and the shutdown sequence at the end keeps sending on `shutdown_tx` ...
```

**Two callers reach it here, and this plan's third process never does.** `confined` builds the child's storage because the child never ran the parent's pre-confinement sequence, and the plain run builds its own at its call site; the launcher builds nothing at all, because constructing one opens the state database the child is about to open, so its last call is `confine_or_launch`.

**The substitution sits at the plane's call site and nowhere else, and that is the whole point of the line.** `scanner.run(shutdown_rx)` and `plane.serve(listener, …)` are two independent subscribers: rebinding `shutdown_rx` to the caller's receiver — the shape an earlier draft of this step had — would hand a forwarded stop to the **background scanner** and leave the server *accepting*, which is the exact opposite of what the hook exists for (design §3 step 9). What the plane takes is either the caller's receiver or a subscription to this process's own sender; the scanner's own receiver is untouched either way. Note also that both ends derive from the same `shutdown_tx` in the child: the hook's `send(true)` reaches the plane through the caller's receiver, and the shutdown sequence's own `send(true)` reaches the scanner and the sweeper — a stop from either source therefore stops everything, which is what a reader of the two channels might otherwise doubt.

**`Args` and `parse_args` are P1 Task 7's, and this step adds a caller to them rather than the field and the signature**: `2026-09-18-sandbox-shared.md` §4 gives every helper in this one file a single owner, and P1's task is where `struct Args` — `sandbox` included — and the signature that takes the argument iterator are written, so there is nothing for P2 to add beside them. What this step owes them is the call site above, `parse_args(own_args())`: the launcher's own element never reaches the iterator the parser is handed, so it is filtered in exactly one place. `tinio-server/Cargo.toml` gains the test target:

```toml
[[test]]
name = "sandbox_e2e_windows"
required-features = ["sandbox"]
```

**No platform dependency is added to that manifest, and this is the one thing about the backend this step used to get backwards.** The launch path names the container's `Options` and its tests name `Capability`, and the question of how a consumer reaches a platform's own settings is **design decision 2's to answer, not this plan's**: a consumer names the facade and nothing else, and reaches them through the facade's cfg-gated alias for the platform's crate — `tinio_sandbox::windows::Options`, inside a `#[cfg(target_os = "windows")]` block it would have written anyway. An earlier draft had `tinio-server` take a `[target.'cfg(target_os = "windows")'.dependencies]` table of its own, on the argument that P1's Linux arm did the same for its own platform's options; that ruling was reversed, because by the time P3's arm exists the crate would carry one target-gated dependency per platform while every arm lives in the same function, and because reaching past the facade for a platform crate contradicts the contract the facade exists to state. `tinio-sandbox-windows` is still a workspace member everywhere and its own `#![cfg(windows)]` is what leaves it empty on the other two targets.

- [ ] **Step 5: Run the example's tests, and confirm stdout is the protocol's alone**

Run:
```powershell
cargo test -p tinio-server --example serve
cargo build -p tinio-server --example serve
rg -n --pcre2 '(?<!e)println!|(?<!e)print!|stdout\(\)' crates/tinio-server/examples/serve.rs
```

**The negative lookbehind is load-bearing, and P1 solved the same problem the same way** (`2026-09-18-sandbox-linux.md` Task 7): `println!` is a substring of `eprintln!`, so the obvious pattern reports every diagnostic that is supposed to be on stderr — the usage text, the state-directory notes, and this plan's own launcher banner — and buries the one line the check is about. With `(?<!e)` the hits are the protocol's stdout writers and nothing else.

Expected: PASS, and the only hits are inside `emit` and the endpoint substitution above — the endpoint line this step replaces was the file's only stdout writer, which is what makes reserving stdout for the protocol cheap (decision 19). Any other hit is either a log that belongs on stderr or a protocol violation, and it is resolved here rather than discovered by a consumer that cannot tell it from an event. Note that the *child's* stdout is a pipe to the parent, so even a stray write there is dropped by the relay (Task 9's `events` test) — but the launcher's own stdout has no such filter, which is why the grep is the check.

- [ ] **Step 6: Write the end-to-end test**

`crates/tinio-server/tests/sandbox_e2e_windows.rs`:

```rust
//! The confined server on Windows, driven the way the harness drives it: spawn
//! `serve.exe --sandbox`, read the endpoint it relays, talk to it, then check the
//! three things only a separate process can show — that killing the launcher takes
//! the confined server with it, what a hard kill leaves behind, and that a run
//! which survives its child takes that residue over and removes it.

#![cfg(all(windows, feature = "sandbox"))]

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tinio_sandbox::Event;
use tinio_sandbox::windows::control;

/// `CREATE_NEW_PROCESS_GROUP` — the launcher is spawned as the leader of its own
/// process group, which is what makes a console control event addressed at *it*
/// reach it and nothing else (Task 10's suite spawns the same way, for the same
/// reason).
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

/// The `serve` **example** binary.
///
/// The rule is `crates/tinio-e2e/tests/steps/clients.rs`'s `serve_bin` — and
/// `2026-09-18-sandbox-linux.md` Task 8 owns its statement, including why
/// `assert_cmd::cargo::cargo_bin` and `env!("CARGO_BIN_EXE_serve")` cannot find an
/// example; what is here is the code, copied because a test crate cannot import
/// another crate's test support. An example is resolved relative to this test
/// binary's own location: `<target>/<profile>/deps/<test>` means the example sits
/// in the sibling `examples/` directory of the same profile — and the profile has
/// to match the one the tests were built with, or this looks in the wrong
/// directory, or worse, finds a stale binary.
fn serve_bin() -> PathBuf {
    let name = format!("serve{}", if cfg!(windows) { ".exe" } else { "" });
    let exe = std::env::current_exe().expect("current exe path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("the test binary lives in <target>/<profile>/deps");
    let path = profile_dir.join("examples").join(name);
    assert!(
        path.exists(),
        "the serve example is not built at {} — build it with the same profile as the tests \
         (`cargo build -p tinio-server --example serve`)",
        path.display()
    );
    path
}

/// One `serve --sandbox` run, from the test's side.
struct Run {
    launcher: Child,
    root: PathBuf,
    /// The launcher's stderr: its banner, every log the confined child writes, and
    /// shared §7's `info!` events — the launcher installs a subscriber for them
    /// precisely because it never reaches `serve`'s own (Task 11 Step 4).
    log: PathBuf,
    address: std::net::SocketAddr,
}

impl Run {
    /// Spawn `serve --sandbox` on `root` and wait for the endpoint.
    ///
    /// **stderr goes to a file and not to a pipe.** Everything the launcher and
    /// the confined child have to say is on stderr, and a pipe nobody drains
    /// becomes a child blocked on a full buffer — which reads as a hung server
    /// rather than as a full pipe. The file is rewritten by a second run on the
    /// same root, so read what you need from a run before starting the next.
    fn spawn(root: &Path, port: u16) -> Self {
        let log = root.join("launcher.log");
        let stderr = File::create(&log).expect("create the launcher's log");
        let mut launcher = Command::new(serve_bin())
            .args([
                root.to_str().expect("a UTF-8 root"),
                "--port",
                &port.to_string(),
                "--sandbox",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .creation_flags(CREATE_NEW_PROCESS_GROUP)
            .spawn()
            .expect("spawn serve");
        let address = wait_for_listening(&mut launcher, &log);
        Self {
            launcher,
            root: root.to_path_buf(),
            log,
            address,
        }
    }

    /// The package SID this run's launcher printed.
    ///
    /// Read from the banner rather than re-derived here: a `PackageSid` built in
    /// *this* process would create the very profile the assertion is about, and
    /// its `Drop` would delete that profile while the launcher still needed it
    /// (Task 2).
    fn package_sid(&self) -> String {
        self.log_text()
            .lines()
            .find_map(|line| line.strip_prefix("sandbox: package SID "))
            .expect("the launcher printed its package SID")
            .trim()
            .to_string()
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).expect("the launcher's log")
    }
}

/// Read the relayed event stream until `Listening` arrives, and hand back the
/// endpoint.
///
/// The consumer's contract, written the way a harness writes it: accumulate lines
/// into one frame, decode it, take the first event that is ours, and ignore
/// everything else rather than failing on it (design §12). **A frame is two field
/// lines and a blank one**, so a decoder fed a single line sees an event name with
/// no payload and drops it — decoding each line as it arrived would wait for an
/// endpoint that had already been announced.
fn wait_for_listening(launcher: &mut Child, log: &Path) -> std::net::SocketAddr {
    let mut reader = BufReader::new(launcher.stdout.take().expect("stdout"));
    let mut frame = String::new();
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).expect("read the event stream") == 0 {
            let log = std::fs::read_to_string(log).unwrap_or_default();
            panic!("serve emitted no Listening event; stderr:\n{log}");
        }
        if line.trim_end().is_empty() {
            match Event::decode(&frame) {
                Some(Event::Listening { addr }) => return addr,
                Some(Event::Error { message }) => panic!("the launcher reported: {message}"),
                None => frame.clear(),
            }
            continue;
        }
        frame.push_str(&line);
    }
}

/// `icacls` on `root`, with its exit status checked.
///
/// The oracle for what is on the operator's directory, and it has to refuse a
/// non-zero exit: an `icacls` that could not read the DACL would otherwise hand
/// back empty text, and "no package ACE is left" would be asserted about a
/// directory nothing had looked at.
fn icacls(root: &Path) -> String {
    let output = Command::new("icacls").arg(root).output().expect("run icacls");
    assert!(
        output.status.success(),
        "icacls {}: {}",
        root.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// How many ACEs on `root` name `sid`.
///
/// The count and not a presence check: the grant is `/grant:r`, so a takeover that
/// added a second entry instead of replacing the first would leave two — and
/// "the residue is reusable" is exactly the claim that no second entry appears.
fn ace_count(listing: &str, sid: &str) -> usize {
    listing.matches(sid).count()
}

#[test]
fn a_confined_server_serves_and_the_launchers_death_takes_it_with_the_ace_left_behind() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");
    let mut run = Run::spawn(&root, 0);

    let mut stream = TcpStream::connect(&run.address).expect("connect to the confined server");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    assert!(response.starts_with("HTTP/1.1 "), "{response}");

    let sid = run.package_sid();
    assert_eq!(
        ace_count(&icacls(&root), &sid),
        1,
        "one ACE while the server runs"
    );

    // Shared §7's `info!` half, read from the launcher's own stderr as it stands:
    // this is what an operator has after a run, and a launch that left no such line
    // is a launch nobody can reconstruct.
    let log = run.log_text();
    assert!(log.contains("sandbox: package SID"), "{log}");
    assert!(log.contains("CreateProcessW"), "{log}");

    // Kill the launcher, not the child: KILL_ON_JOB_CLOSE is the only thing that
    // can end the confined server now (decision 20, first half).
    run.launcher.kill().expect("kill the launcher");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match TcpStream::connect(&run.address) {
            Err(_) => break,
            Ok(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Ok(_) => panic!("the confined server outlived its launcher"),
        }
    }
    run.launcher.wait().expect("reap the launcher");

    // **The residue is asserted as a residue, not as an absence.** `kill` is
    // `TerminateProcess`, so the launcher's rollback never ran and the ACE is
    // still on the root — which is what the design's risk table says happens, and
    // a test claiming otherwise would be asserting something the mechanism cannot
    // deliver. What decision 21 promises is that the residue is *identifiable and
    // reusable*, and that is the next test's subject.
    //
    // **The SID compared is the one the launcher printed, never the `S-1-15-2-`
    // prefix**: `S-1-15-2-1` is ALL APPLICATION PACKAGES, which every directory an
    // AppContainer process has touched already carries, so a substring test would
    // find that entry and call it this run's residue.
    let residue = icacls(&root);
    assert_eq!(
        ace_count(&residue, &sid),
        1,
        "the hard kill left its ACE:\n{residue}"
    );
}

#[test]
fn a_restarted_launcher_takes_the_residue_over_and_a_clean_exit_removes_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let root = dir.path().join("root");

    let mut first = Run::spawn(&root, 0);
    let sid = first.package_sid();
    first.launcher.kill().expect("kill the first launcher");
    first.launcher.wait().expect("reap it");
    assert_eq!(
        ace_count(&icacls(&root), &sid),
        1,
        "the first run's residue is on the root"
    );

    // The same root derives the same package name, so the same SID — and
    // `/grant:r` replaces this SID's entry rather than adding a second one. That
    // pair *is* decision 21's claim, and it is the only form of it a test can
    // assert: the residue is reusable, not accumulated.
    let mut second = Run::spawn(&root, 0);
    assert_eq!(
        second.package_sid(),
        sid,
        "the same root derives the same SID"
    );
    let taken = icacls(&root);
    assert_eq!(
        ace_count(&taken, &sid),
        1,
        "the second run took the entry over rather than adding one:\n{taken}"
    );

    // **End the run with the launcher alive**, which is the only way its rollback
    // runs at all: the console control event goes to the launcher's group, the
    // launcher's relay forwards it to the child (design §3 step 9), the child's
    // stop hook shuts it down, and the launcher — still running — takes the ACEs
    // and the profile back. Killing the *launcher* would skip all of that, which
    // is why the test above asserts a residue and this one asserts its removal.
    if control::send_stop(second.launcher.id()).is_err() {
        eprintln!(
            "skipping the rollback half: no console to deliver a stop on. The takeover above was \
             asserted; what is not exercised here is the clean exit."
        );
        let _ = second.launcher.kill();
        let _ = second.launcher.wait();
        return;
    }
    let status = second
        .launcher
        .wait()
        .expect("the launcher exits with its child's status");
    assert!(
        status.code().is_some(),
        "the launcher reported its child's status"
    );
    let after = icacls(&root);
    assert_eq!(
        ace_count(&after, &sid),
        0,
        "the clean exit rolled the ACE back:\n{after}"
    );
}
```

- [ ] **Step 7: Run the test**

Run: `cargo test -p tinio-server --test sandbox_e2e_windows`
Expected: PASS — two tests. **The first is fully portable**: it serves, checks the job's kill, and then asserts the residue a hard kill leaves — the behaviour rather than a wish — while reading shared §7's `info!` events from the launcher's log. **The second needs a console** for its second half, because a console control event cannot be delivered without one, and prints its `skipping:` line rather than failing where there is none, exactly as Task 10's suite does. If the launcher reports `no standard handle`, run it from an interactive console: the confined child inherits the launcher's **stderr**, which is where every log goes (decision 19) and the one stream a detached runner may not have.

- [ ] **Step 8: Run it by hand, confined**

Run:
```powershell
cargo run -p tinio-server --example serve -- E:\tinio-smoke --port 0 --sandbox
```
Expected: `sandbox: package SID S-1-15-2-…` and `sandbox: granting …` on stderr, preceded by the traced `info!` lines this phase adds — the profile, each resolved exposure, `CreateProcessW` with its pid, and the Job object — and on stdout exactly one frame — `event: listening` then `data: {"addr":"127.0.0.1:<port>"}`, closed by a blank line — then a second terminal serves S3 against it. Ctrl-C in that console reaches the whole console group; to see the relay do its work instead, stop the *launcher* alone's process group (Task 10's `send_stop`) and watch the confined server stop with it.

- [ ] **Step 9: Verify the other build graphs**

Run:
```powershell
cargo check -p tinio-server --no-default-features
cargo check --workspace --all-targets
```
Expected: both succeed; the example and the test are skipped by `required-features`.

---

### Task 12: Settling the network question

**Files:**
- Create: `crates/tinio-sandbox-windows/tests/network.rs`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (the evidence table, §7's verification gap, the Risks row)

**Interfaces:**
- Consumes: `tests/common/mod.rs`.
- Produces: nothing the library uses; the evidence §7's Windows row is missing.

§7 records the gap and assigns it to P2: what was measured on the authoring host was loopback only, and every non-loopback refusal looked like the host's own firewall policy rather than container isolation. **A test that only checks "connect failed" proves nothing** — a machine with no route fails it too — so the harness proves the address reachable from outside the confinement first and only then asserts it is unreachable inside. The LAN question stays a recorded observation: decision 16 says nothing in the design depends on it, and the plan keeps it that way.

- [ ] **Step 1: Write the test**

`crates/tinio-sandbox-windows/tests/network.rs`:

```rust
//! The network guarantees: outbound is blocked, inbound is not, and the two
//! questions the design left open are measured rather than assumed.

#![cfg(windows)]

mod common;

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::time::Duration;

use tinio_sandbox_core::{AccessMode, Confinement};
use tinio_sandbox_windows::{Capability, Options};

/// The confined child. The body lives in the harness because the confinement
/// suite launches the same role from a different test binary.
#[test]
fn child_role() {
    common::child_role();
}

/// The address the baseline and the confined attempt both use.
///
/// `1.1.1.1:53` is the default and it was measured reachable from this machine
/// unconfined on 2026-09-19, together with `8.8.8.8:53`, `223.5.5.5:53` and
/// `114.114.114.114:53` — which matters, because the design's §7 gap was opened
/// by a host whose baseline was unknown. A machine that cannot reach any of them
/// skips loudly rather than reporting a container policy it did not observe
/// (`an_address_that_works_outside_is_refused_inside` prints the reason), and
/// `TINIO_SANDBOX_TEST_TARGET` points the suite at whatever this host can
/// actually reach.
fn target() -> SocketAddr {
    std::env::var("TINIO_SANDBOX_TEST_TARGET")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_else(|| "1.1.1.1:53".parse().expect("a literal"))
}

/// Whether the unconfined host can reach `address` at all.
fn baseline(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, Duration::from_secs(5)).is_ok()
}

/// What the confined child reports about reaching out.
///
/// The target travels in the probe file the child already reads, so nothing about
/// this run goes through the environment — the same rule the rest of the suite
/// follows.
fn confined_outbound(volume: &Path, options: Options, target: SocketAddr) -> Option<String> {
    let (_dir, mut tree) = common::tree(volume);
    tree.target = Some(target);
    let (confinement, address) = common::launch_confined(&tree, AccessMode::ReadWrite, options, None)?;
    let Confinement::Supervise(child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    // The child probes and reports before it accepts, so this connect both waits
    // for it and proves inbound still works.
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(30))
        .expect("connect to the confined child");
    let mut pong = [0u8; 4];
    std::io::Read::read_exact(&mut stream, &mut pong).expect("read the child's answer");
    assert_eq!(&pong, b"pong");
    let mut child = child;
    tinio_sandbox_core::supervise(&mut child, || ()).expect("supervise");
    Some(tree.report_text())
}

/// The outbound guarantee, with the control the design asks for.
#[test]
fn an_address_that_works_outside_is_refused_inside() {
    let target = target();
    if !baseline(target) {
        eprintln!(
            "skipping: this host cannot reach {target} even unconfined, so nothing would be proved by trying it confined. Set TINIO_SANDBOX_TEST_TARGET to an address this host reaches."
        );
        return;
    }
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let Some(report) = confined_outbound(&volume, Options::default(), target) else {
        return;
    };
    // The baseline passed, so a refusal here is the container's policy rather
    // than a missing route, and the round trip proved inbound still works.
    assert!(
        report.contains("outbound: refused"),
        "the container reached {target} with no capability: {report}"
    );
}

/// The positive control: the same address, with `internetClient`.
///
/// Recorded, not asserted. A pass shows the container's policy — not the host's
/// firewall — is what refused the previous test; a failure on a host whose
/// baseline works is a finding worth reading, not a broken build.
#[test]
fn the_same_address_is_reached_with_internet_client() {
    let target = target();
    if !baseline(target) {
        eprintln!("skipping: no baseline for {target}");
        return;
    }
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    let options = Options {
        capabilities: vec![Capability::InternetClient],
        ..Default::default()
    };
    match confined_outbound(&volume, options, target) {
        Some(report) => eprintln!(
            "recorded: with internetClient the confined child reported `{}`",
            report.trim()
        ),
        None => eprintln!("recorded: with internetClient the launcher did not return"),
    }
}

/// The LAN question decision 16 leaves open, recorded and never asserted.
///
/// Set `TINIO_SANDBOX_TEST_LAN_ADDR` to this host's LAN address (for example
/// `192.168.1.10:9000`) to record whether an inherited listener bound to `0.0.0.0`
/// accepts a connection that arrives on the LAN interface, with and without
/// `privateNetworkClientServer`. Nothing in the design depends on the answer; the
/// point is to replace a guess with a measurement and to record which capability,
/// if any, a non-loopback bind needs.
#[test]
fn the_lan_answer_is_recorded_when_asked_for() {
    let Ok(lan) = std::env::var("TINIO_SANDBOX_TEST_LAN_ADDR") else {
        eprintln!(
            "skipped: set TINIO_SANDBOX_TEST_LAN_ADDR=<this host's LAN ip>:<port> to record whether an inherited listener reaches the network"
        );
        return;
    };
    let address: SocketAddr = lan.parse().expect("a LAN address");
    let Some(volume) = common::volumes().into_iter().next() else {
        return;
    };
    for (label, capabilities) in [
        ("no capability", Vec::new()),
        (
            "privateNetworkClientServer",
            vec![Capability::PrivateNetworkClientServer],
        ),
    ] {
        let (_dir, tree) = common::tree(&volume);
        let bind = SocketAddr::from(([0, 0, 0, 0], address.port()));
        let Some((confinement, bound)) = common::launch_confined(
            &tree,
            AccessMode::ReadWrite,
            Options {
                capabilities,
                ..Default::default()
            },
            Some(bind),
        ) else {
            return;
        };
        let Confinement::Supervise(child) = confinement else {
            panic!("the Windows backend must launch a child");
        };
        let reachable = match TcpStream::connect_timeout(&address, Duration::from_secs(5)) {
            Ok(mut stream) => {
                let _ = stream.write_all(b"ping");
                true
            }
            Err(_) => false,
        };
        eprintln!(
            "recorded: [{label}] bound {bound}, the LAN address {address} {} reachable while the container served",
            if reachable { "was" } else { "was not" }
        );
        // The child is waiting for a connection its caller may never make. Drop
        // the supervision rather than waiting for it: the `Child`'s closure drops
        // with it, which closes the job handle and takes the child with it
        // (decision 20), and the `Grants` guard rolls the ACEs back on the same
        // path.
        drop(child);
    }
}
```

`Child` exposes no `kill` in P0's surface, and this task does not add one: dropping the `Confinement` is what ends a child nobody waited for, which is also the case `a_dropped_supervision_also_rolls_back` pins in Task 9.

- [ ] **Step 2: Run the outbound test**

Run: `cargo test -p tinio-sandbox-windows --test network an_address_that_works_outside_is_refused_inside -- --nocapture`
Expected: PASS on a host that reaches `1.1.1.1:53`, or a printed `skipping:` where it does not. **A failure here is a design finding, not a broken test**: it means an AppContainer with no capability reached an address this host reaches unconfined, and §7's Windows row would have to change. Do not weaken the assertion to make it pass.

- [ ] **Step 3: Run the positive control and the LAN diagnostic**

Run:
```powershell
cargo test -p tinio-sandbox-windows --test network the_same_address_is_reached_with_internet_client -- --nocapture
$env:TINIO_SANDBOX_TEST_LAN_ADDR = '<this host''s LAN address>:<port>'
cargo test -p tinio-sandbox-windows --test network the_lan_answer_is_recorded_when_asked_for -- --nocapture
```
Expected: a `recorded:` line from each. Keep both verbatim for the next step — they are the evidence §7's row and its risk row are waiting for.

- [ ] **Step 4: Record the evidence in the design**

`docs/superpowers/specs/2026-09-18-sandbox-design.md`:

1. The evidence table's outbound row becomes the measured row: host, target address, the baseline result, the confined result, and the `internetClient` control.
2. §7's **Verification gap** paragraph becomes the settled statement, including which capability, if any, a non-loopback bind needs. If the measurement contradicted the design, the row and the §7 paragraph say so and the Goal's second guarantee is re-scoped in the same edit rather than left implied.
3. The **Risks** table's Windows row is marked settled, or replaced with whatever the measurement turned up.

Add one sentence to §7's Windows note recording the LAN answer, phrased as an observation, and keep the design's existing statement that nothing depends on it (decision 16): the isolation claim rests on the capability set, never on the handoff.

---

### Task 13: The volume script, the documentation and the gate

**Files:**
- Create: `crates/tinio-sandbox-windows/scripts/verify-windows.ps1`
- Modify: `docs/tests.md`
- Modify: `docs/superpowers/specs/2026-09-18-sandbox-design.md` (§13's P2 row, decision 14's site list, and the Risks row this task quantifies)
- Modify: `README.md` (the crate row, if P0 did not add it, and the operator's note on what a large served tree costs)

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Write the verification script**

```powershell
#!/usr/bin/env pwsh
# Verify the Windows sandbox, on every volume that can host it.
# Usage: pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1 [cargo test args...]
$ErrorActionPreference = 'Stop'
if (-not $IsWindows) { Write-Error 'verify-windows.ps1 needs Windows'; exit 1 }

$requested = if ($env:TINIO_SANDBOX_TEST_VOLUMES) { $env:TINIO_SANDBOX_TEST_VOLUMES } else { 'C:\;E:\' }
$present = @($requested -split ';' | Where-Object { $_ -and (Test-Path $_) })
if ($present.Count -eq 0) { Write-Error "none of the volumes in '$requested' exist"; exit 1 }
$env:TINIO_SANDBOX_TEST_VOLUMES = $present -join ';'

Write-Host "volumes under test: $env:TINIO_SANDBOX_TEST_VOLUMES"
foreach ($volume in $present) {
    $letter = $volume.Substring(0, 1)
    # NTFS and ReFS agree on every probed ACL case (design evidence table), so
    # the log has to say which filesystem each leg actually covered.
    $fs = (Get-Volume -DriveLetter $letter).FileSystemType
    Write-Host "  $volume : $fs"
}

cargo test -p tinio-sandbox-windows -- @args
cargo test -p tinio-server --test sandbox_e2e_windows
```

- [ ] **Step 2: Run it**

Run: `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1`
Expected: the volume list with its filesystems, then every sandbox test green — including `tests/signal.rs`, which needs a console: run it from an interactive shell, not a detached one, or its two cases print their `skipping:` lines. A volume that is not present is skipped rather than failed — a workstation without an ReFS volume is a fact about the machine, not a broken build — so the printed filesystems are what records which ones were covered.

- [ ] **Step 3: Establish what the ACL pass costs, on a tree large enough to say**

Task 4 and Task 9 both state this cost without a number: the grant list is one `icacls` invocation per exposed path, but the ACE it writes is `(OI)(CI)`, so the filesystem's inheritance carries it to every descendant the served tree already holds — and the rollback pays the same pass on the way out, which makes a run's ACL work proportional to the corpus twice over. **Measure it here rather than assume it**, on a synthetic corpus, and record the two numbers:

```powershell
$root = Join-Path $env:TEMP 'tinio-acl-cost'
Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
# 200 directories x 1000 files. Creating the tree is the slow part of this
# block; the two `Measure-Command` lines below are the measurement.
foreach ($d in 1..200) {
    $dir = Join-Path $root "d$d"
    New-Item -ItemType Directory -Force $dir | Out-Null
    1..1000 | ForEach-Object { New-Item -ItemType File -Force (Join-Path $dir "f$_.bin") | Out-Null }
}
# Any SID that is not already on the tree: what is being timed is the propagation
# to 200,000 objects, not whose SID it is.
$sid = 'S-1-15-2-1-2-3'
'grant:    {0:N2} s' -f (Measure-Command { icacls $root /grant:r "*$sid:(OI)(CI)M" /q | Out-Null }).TotalSeconds
'rollback: {0:N2} s' -f (Measure-Command { icacls $root /remove:g "*$sid" /q | Out-Null }).TotalSeconds
Remove-Item -Recurse -Force $root
```

Expected: two numbers for one directory and 200,000 objects — the per-object cost that §6's "2N `icacls` invocations for N exposures" leaves out of the sentence. **Record them where a document already promises a number**: the design's Risks row for this ("Unquantified, and that is the risk rather than the cost itself") becomes the measured pair, and the `docs/tests.md` bullet from Step 4 gains one line saying how it was measured, so the next reader repeats it rather than trusting it. If the grant alone takes a large share of the 30 s readiness budget the harness allows (`crates/tinio-e2e/tests/steps/clients.rs`'s `START_TIMEOUT`), say so in both places — a number that strains an existing budget is the finding, not a reason to pick a smaller tree.

**And what it means for an operator with a very large tree belongs in the README**, next to the crate row rather than only in a plan: both passes are proportional to the number of objects under the exposed directories, they both happen before the server can report readiness, and the rollback happens again on the way out. The honest guidance is what the measurement supports rather than what is convenient — expose the narrowest directory that serves the corpus, because a parent directory holding far more than the server serves pays for all of it; and where the corpus is large enough for the two passes to matter, **measure the grant on a tree of the same object count before exposing it** (this step's block, sized to the corpus), because the cost follows object count and a small tree says nothing about a large one. If the numbers say the cost is negligible at the corpus sizes this tool is for, say that instead, with the size they were measured at.

- [ ] **Step 4: Record the commands in `docs/tests.md`**

Under the existing `## Platform-gated verification (WSL2)` section, in the file's compressed style:

```markdown
- Windows confinement (`--sandbox`): `pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1` — the AppContainer/DACL suite on every volume present (locally `C:\` NTFS and `E:\` ReFS), the signal and event relays, and the `serve` e2e. **CI needs no new job**: the portable Windows legs already run `cargo nextest run --workspace --exclude tinio-e2e`, so this crate's suite is picked up the moment it is a workspace member — on NTFS only, and with no console, so the two signal cases print their `skipping:` line there. ReFS and the console cases are what the local script adds. The signal relay's two cases need a console (a console control event cannot be delivered without one) and print a `skipping:` line rather than failing where there is none.
- The outbound assertion needs a baseline: the suite proves the target address is reachable unconfined before asserting it is unreachable confined, and skips loudly when the host cannot reach it at all (`TINIO_SANDBOX_TEST_TARGET` overrides `1.1.1.1:53`).
- Readiness is an event, not a scraped line (decision 19): stdout carries SSE-framed JSONL (`data: {…}` and a blank line) and every log is on stderr. `crates/tinio-e2e/tests/steps/clients.rs`'s `READY`/`wait_for_ready` moves to decoding that stream — in P1, which emits the first events (design Bookkeeping), not here.
```

- [ ] **Step 5: Record what landed**

The design's decision 14 site list is already equal to the crate's `#[allow(unsafe_code)]` files (Task 1, kept current by each task that adds a module). Confirm with `rg -c 'allow\(unsafe_code\)' crates/tinio-sandbox-windows/src` and correct either side if they disagree — decision 14 asks for the scope to be enumerated per capability with the count following the implementation, and the list and the code saying the same thing is what makes that checkable. Update §13's P2 row's verification column in the design to what Steps 1–2 actually produced, naming the filesystems and whether the relay's container probe was measurable here.

- [ ] **Step 6: Run the full local gate**

Run the **Standard acceptance (pre-push gate)** block in `docs/tests.md` in full — it is the bar, and the list this step used to carry was a shorter, divergent subset of it (shared §1). The two runs this phase adds to it are:

```powershell
pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1
cargo check -p tinio-server --no-default-features
```

Expected: all clean.

---

## Commit

**One commit, made by hand, at the end of the phase.** No task in this plan commits, so every change accumulates in the working tree and the phase is reviewed as one diff rather than a commit per task. `CLAUDE.md` owns the authorisation rule — git writes are asked for per operation, and the index is never written by an agent — so this section fixes the phase's **scope** and its message, and is not permission to run it.

Run the full Standard acceptance block (`docs/tests.md`) first, and commit only once the phase's own review is clean. The message is a conventional header plus single-line cause-and-effect bullets: what changed and why, never where it lives. It is a starting point rather than a description of finished work — where the phase's actual run makes a line of it false, that line changes.

```bash
git add crates/tinio-sandbox-windows crates/tinio-sandbox crates/tinio-sandbox-core crates/tinio-server crates/tinio-config docs/tests.md docs/superpowers/specs/2026-09-18-sandbox-design.md README.md
git commit -F - <<'EOF'
feat(sandbox-windows): confine serve under AppContainer

- AppContainer entered at process creation only, so a launcher stays outside
- Launcher outlives the child; job object kills the child with it
- Hard kill would otherwise orphan a confined server holding the listener
- Package SID derived from a fixed name, not generated: residue stays identifiable
- Next run takes its own ACEs over instead of accumulating orphans
- Container profile created with the SID: a derived SID alone cannot launch
- Grant reaches pre-existing children by inheritance; rollback on every exit path
- Failed launch rolls back too, not only the clean exit
EOF
```

## Self-Review

**Spec coverage.** Walking `docs/superpowers/specs/2026-09-18-sandbox-design.md` against the tasks above:

| Spec item | Task |
|---|---|
| §2 platform matrix, Windows row (path namespace visible, access denied) | 9 (the denial assertions), 11 |
| §3 step 2 (targeted `server.read_only` read, no `Config` built) | 11 |
| §3 step 3 (bind, mark inheritable) | 6, 9, 11 |
| §3 step 4 (the exposure set — canonicalised, a reparse point refused — the executable's directory, the device files) | 3, 9, 11 |
| §3 step 5 (DACLs applied before `CreateProcess`) | 4, 9 |
| §3 step 6 (both attributes, `EXTENDED_STARTUPINFO_PRESENT`, the Job) | 5, 8, 9 |
| §3 step 7 (the payload into the child's **stdin**, then closed; the listener's descriptor in argv) | 7, 9 |
| §3 step 8 (emit `Listening`, relay the child's stdout events, wait, roll back, forward the exit status) | 9, 11 |
| **§3 step 9 (forward termination signals while waiting)** | **10** |
| §6 ACL whitelist (`(OI)(CI)M` / `(OI)(CI)RX`, ancestors untouched) | 3, 4, 9 |
| §6's inheritance question — measured before this plan ran, so the test is a regression test | 4 (both volumes via 9), already recorded in the design |
| §6 residual cost (ACEs written onto the operator's directories, plus the two passes over the served tree that writing and removing them costs) | 4 (one entry per path, removed on every exit path including `Drop`), 9 (the grant list's comment), 13 (the measurement that replaces "unquantified" with a number) |
| §7 Windows row and its verification gap | 12 |
| §7 inbound asymmetry (`privateNetworkClientServer`, non-loopback bind) | 3 (the capability), 12 (recorded) |
| §11 error table's Windows rows (grant / `CreateProcess` / Job failure names the directory **and rolls back**; a refused attribute set is fatal because the launch it would have preceded still succeeds; the error carries both the Win32 code and a tool's exit status) | 1 (`Error::Launch`), 4, 8 (§11's new row), 9 (the two job failures terminate the child; the failed-launch and dropped-supervision tests) |
| §11 "launch payload absent or malformed → treat as launcher, fails closed" | 7, 11 (the platform without a launcher arm fails closed) |
| §12 unit, every platform (exposure set, capability policy, quoting, payload) | 3, 7, 8 |
| §12 the event contract (round-trip `Event`, a consumer that skips non-frames, and — on the launcher platforms — text the child writes to stdout which is **not** a frame never reaching the parent's stdout) | 9 (`tests/events.rs`), 11 (the consumer in the e2e) |
| §12 Windows integration (whitelist, rollback on exit and on a failed launch, killing the launcher takes the child) | 4, 5, 9, 11 |
| §12 both volumes | 9, 13 |
| §12 network isolation with a proven baseline | 12 |
| §13 P2 row | all |
| decision 2 (`Backend`/`Confinement` keeping the dispatch free of `#[cfg]`, and a platform's own options reached by naming its crate) | 9, 11 |
| decision 6 (Windows launches a child, no in-place path) | 9 |
| decision 14 (unsafe scope enumerated per capability, count after the fact; `lib.rs`'s single `from_wide` site included) | 1, 2, 13 |
| decision 15 (grant-only, rollback on every exit path — the ACEs **and** the profile) | 4, 9 |
| decision 16 (listener handoff; nothing depends on it) | 6, 9, 12 |
| decision 17 (the payload travels on the child's **stdin** and is closed; no environment marker; the argv element names the inherited **listener** descriptor and lives in the core) | 7 (the adoption and the element), 9 (the wiring), 11 (the call site) |
| decision 18 (the parent never builds a `Config`) | 11 |
| decision 19 (stdout is an SSE event stream; **the child writes `Listening` and the parent re-emits it**, decoding rather than forwarding; stderr unmediated) | 9 (the relay wiring), 11 (the emission and the consumer), 13 (§12's event-contract bullet) |
| decision 20 (Job object **and** signal forwarding; the `CreateProcessW`→assign window recorded rather than claimed) | 5 (the window), 9 (terminate on the two job failures), 10, 11 |
| decision 21 (derived, deterministic SID; the profile is the second artifact, reusable and removed) | 2, 4, 9, 11 (the residue and takeover e2e) |
| decision 22 (the executable's directory) | 3 (the backend's `exposures`, which is the only place the entry is produced; Task 11 calls the helper that does not carry it) |
| decision 26 (confined before the runtime) | 11 (the launcher never builds a runtime; the child builds it after adopting) |
| **shared §7 (every core operation traced, split into `info!` for "was this process confined, and to what" and `debug!` for the per-item detail, `error!` where an `Error` is built)** | 1 (`handles::step` builds and logs in one place, with the recording-subscriber test), 2, 3, 4, 5, 8, 10, 11 (the launcher installs the sink it would otherwise never reach) |

**Out of P2's scope, deliberately.** `[server] sandbox` and `TINIO_SANDBOX` land with the CLI, exactly as P1 deferred them (decision 1); `--sandbox` is the only channel that exists today, and decision 5's feature-off exit 2 belongs to the CLI, not to the example. `TINIO_SANDBOX_CAPABILITIES` in Task 11 is provisional scaffolding for that deferral, not a new contract. Decision 25's OTLP conflict is unchecked here for the same reason P1 left it: `serve` never passes an endpoint. macOS is P3 and the fallback path is P4; Task 11 only makes sure neither silently serves unconfined. The e2e harness's own readiness scrape — `crates/tinio-e2e/tests/steps/clients.rs`'s `READY` constant and `wait_for_ready` — is **not** P2's to change: the design's Bookkeeping assigns it to P1, which emits the first events, and it must move with the cucumber features that assert readiness rather than as a side effect of a sandbox task. P2's consumer-side decoding lives in `crates/tinio-server/tests/sandbox_e2e_windows.rs`. Read-only mode itself (FR-023) has **landed** and is not P2's to build: `serve` resolves it from `[server] read_only`, `TINIO_READ_ONLY` and `--read-only`, and relocates state to `TINIO_STATE_DIR` or `<home>/.tinio/roots/<hash>/` (`crates/tinio-config/src/state.rs`). What P2 owes it is the confinement — the root's `AccessMode` comes from `probe_read_only`, and the state directory must be exposed writable where it lands outside the root (Task 11). `Sandbox::device_files()` is a Linux concept and a no-op on Windows: the exposures this crate computes are the whole whitelist. Two things the relay makes *possible* but does not itself deliver, both stated in the code comments rather than implied: the child's stop hook starts the shutdown the runtime already knows, while the bounded drain (≤10 s, state file removed) is the CLI contract's and lands with T066/T067; and `PROC_THREAD_ATTRIBUTE_CHILD_PROCESS_POLICY` (restricting the confined child's own child creation) stays untaken, because it would raise the OS floor to Windows 10 without a design decision behind it.

**Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N". **Three rows that used to be open questions are settled and are now regression tests or measurements this plan is written against.** The third joined them on 2026-09-19: **a merely derived package SID cannot launch** — `CreateProcessW` fails `err=2` for this binary and for `cmd.exe` alike — so Task 2 creates the profile and Task 8's launch test is where the launch-succeeds assertion lives. The first two, each pointing at the design's evidence table rather than re-deriving the answer here: Task 4's inheritance question was measured on 2026-09-19 (one ACE per directory, no `/t` walk), and Task 10's stop semantics were measured the same day on this machine's host console (a console-wide Ctrl+C does not reach a `CREATE_NEW_PROCESS_GROUP` child; a targeted `CTRL_BREAK_EVENT` does; the relay's forward arrives). The container half of the stop claim stays **unverified in this environment** and is no longer load-bearing. What remains genuinely open is Task 12's outbound test — written to fail rather than pass when the design is wrong — Task 1's `unsafe extern` lint check, which only decides whether `ffi.rs` appears in decision 14's list, and **the ACL pass's cost per object under the served tree**, which no document has ever numbered: Task 13 Step 3 measures it on a synthetic corpus and puts the pair of seconds where §6 and the design's Risks row already promise a number, because "the filesystem walks, not the tool" is a cost as well as a fact.

**Type consistency.** The core's surface is P0 Task 2's, in the spellings that plan fixes, and `2026-09-18-sandbox-shared.md` §3 states the ownership of it; this plan consumes it in full, adds nothing to it, and does not repeat the list here. `PackageSid`, `CapabilitySids` (Task 2) are used by Tasks 4, 8, 9 — and the first of them is also what deletes the profile, so its `Drop` is a rollback path rather than a resource release. `handles::{Handle, system_directory, last_error, step, mark_inheritable, inherited_stderr, Pipe, pipe, parent_end, close, write_payload}` (Tasks 1, 7) are used by Tasks 6, 8, 9, and `system_directory` by `acl` (Task 4) alone — it is there so that module carries no `unsafe` of its own while still taking `icacls.exe`'s directory from the kernel rather than the environment; `Handle` is a **re-export of `ffi::Handle`** rather than a private import, because `job::Job::assign`, `process::LaunchSpec` and `socket::handle` are public and their signatures name it. `socket::{value, handle, mark_inheritable, from_value}` (Task 6) by Tasks 7, 8, 9, 11. `launch::{adopt, listener, command_args, on_stop, disarm}` (Tasks 7, 10) by Tasks 8, 9, 10, 11, of which the facade carries exactly two — `adopted_listener` (its name for `listener`) and `on_stop` (Task 9) — and no other name from that module; **`command_args` takes the arguments after `argv[0]` and puts the listener element first**, which is the one rule both launcher plans and the child's `own_args` have to agree on. `process::{command_line, Container, LaunchSpec, launch, LaunchedProcess}` (Task 8) by Tasks 9, 10, and `LaunchedProcess::terminate` by Task 9 alone (the two job failures). `acl::{ace_spec, grant_spec, icacls_binary, grant, revoke, has_ace, Grants}` (Task 4) by Tasks 8, 9, 11. `control::{arm, send_stop, on_stop, disarm}` (Task 10) by Tasks 9, 10, 11. `backend::{Backend, Options, Capability, exposures, canonical_exposures, merge, describe, served_root}` (Tasks 3, 9) by Tasks 9, 11, 12. `tests/common/mod.rs`'s `Tree::{at, probe}` and `{launch_confined, launch_confined_at, run_confined}` (Task 9) are the harness's own surface: the two launcher entries share one body, and only the path-taking one can be called from a process that never held a `Tree`. No name is introduced twice with different meanings, and the one argv element both launcher plans pass — `tinio-sandbox-fd=<n>` — is spelled once, in the core both plans consume.

**Where this plan meets its siblings.** The core is P0's in full and this plan consumes it — the payload frame, the event type, the relay's decode-and-re-emit (`Observer`), the adoption rule, the argv element helpers and the supervision loop all come from there, and P2 adds nothing to it. What is left in this crate is what only a Windows launcher can have: the program and argv, the two std-handle pipes, the listener rebuild, the thread that drives the relay, the AppContainer launch and the console-control relay. P2 agrees with P3 on the two shapes that were in question — `LaunchPlan::for_launch(config)` with the listener attached by `with_listener`, and a plan that carries no program and no argv — so the only per-platform difference left in the call site is the platform crate's own `Options`/`Capability`. **A third requirement is shared and is met differently on purpose**: an exposed path is granted in the form the kernel resolves, never in the form it happened to be written in. P3's `with_canonical_paths` *adds* the canonical spelling beside the original, because a profile naming an unresolved spelling matches nothing; Task 3's `canonical_exposures` *requires* the canonical spelling and refuses the reparse point that produced another one, because there the grant would land on the link's target. That is a rule the two plans agree on and not a function they share — neither crate imports the other's, and neither can. If P3 lands a helper this plan also names (`adopt`, `listener`, the element helpers), the mapping is mechanical and touches only the `use` lines of `backend.rs`, `launch.rs` and `examples/serve.rs`.
