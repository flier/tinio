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
//! `(literal (param "P"))` names the program the launch execs, and
//! `launch::command_args` resolves `P` from the same `program` argument it
//! writes as the argv's program element.
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. The profile format
//! is undocumented and unversioned: every allowance in [`RUNTIME_BASELINE`]
//! carries the observation that keeps it here, and none of them may be kept
//! "just in case".
//!
//! **The text is a template, and its layout is one file.** [`profile`] collects
//! the exposure set and the allowances as data and renders `TEMPLATE` — the
//! `.hbs` beside this module — so a rule's line lives where a reader looks for
//! the profile's shape rather than inside a chain of `format!` calls. The
//! rendered bytes are the contract and did not change with the move: they were
//! compared byte for byte against the string-building version before it was
//! deleted, and they are what `tests/exposures.rs` and the end-to-end leg
//! assert against.
//!
//! # What this platform does *not* spend
//!
//! **Device files are Linux's concept and this module renders nothing for them.**
//! The flag is not on [`Sandbox`] at all — the jail that binds the four nodes is
//! Linux's (design §4), so the flag belongs to that backend rather than to the set
//! every platform is handed — which leaves this text with nothing to read and no
//! rule to render. The four literals plus the two standard streams this profile
//! used to carry were measured unnecessary on both paths — the startup probe and
//! the real binary — and deleted rather than kept, which is the whole of the
//! allowance audit's verdict for them (`docs/superpowers/specs/2026-09-18-sandbox-design.md`
//! §5, whose audit paragraph is that record).
//!
//! **No home grant is spent at all**, and that is a measurement rather than a
//! default: `(allow file-read* (subpath (param "H")))` was kept for read-only
//! runs as a conservative guess, and a read-only run without it reaches
//! `listening`, serves, writes its access log, publishes `state` and forwards
//! SIGTERM to an exit `0` — from an explicit `TINIO_STATE_DIR` and from the
//! default home-derived one alike, with the working directory outside the home
//! and in it. The failure the grant would have covered is a **write**: the
//! read-only run's default state directory is refused `deny(1) file-write-mode
//! /Users/<user>/.tinio`, and no read rule can fix that. The measurement, and
//! the resulting instruction to give a confined read-only run an explicit
//! writable `TINIO_STATE_DIR`, are design §5's `home` paragraph.

use std::{ffi::OsString, fs::canonicalize, io, path::Path};

use serde::Serialize;
use tinio_util::render::renderer;
use tracing::{error, info};

use crate::_core::{Error, Exposure, Result, Sandbox, step};
// **The mechanism comes from the crate root rather than by path**, and the two
// modules are a cycle otherwise: `mechanism` documents itself by pointing at this
// table, and this table's render needs the predicate that module owns.
use crate::Mechanism;

/// The profile's layout. Values live in `ProfileView`, and the one thing this
/// file must not carry is a path: every exposure is a `(param "E<i>")`
/// reference, and the values travel beside the text as `-D` pairs.
const TEMPLATE: &str = include_str!("profile.hbs");

/// One allowance in the profile, with the evidence that justifies it.
///
/// `id` is stable and unique: it appears in the generated text as a trailing
/// `; tinio:<id>` comment, so the pruning run (`scripts/prune-profile.sh`) can
/// remove exactly one rule at a time and the shipped profile says where each
/// rule came from.
///
/// **`mechanism` is the one entry whose necessity depends on how the profile is
/// applied** ([`Mechanism::wraps_the_exec`]), and the field is the predicate
/// rather than a second copy of the rule's name: it keeps the render a filter
/// over this one table instead of a rule that a reader of the profile would have
/// to know was conditional, and [`RUNTIME_BASELINE`]'s own doc carries the
/// measurement behind it.
///
/// **`Serialize` is what the template renders, and that is the whole of this
/// type's relationship to it**: `TEMPLATE` reads `this.rule`, `this.id` and
/// `this.evidence`, which are these three fields by name — `mechanism` is
/// deliberately *not* one of them, so the field can never reach the text. There is
/// deliberately no separate view type — a wrapper holding the same fields would be
/// this struct spelled twice, and the one it would drift from is the table the
/// pruning run parses.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::profile::{Allowance, RUNTIME_BASELINE};
///
/// // One row of the table the render is a filter over: the rule, its stable id and
/// // the observation that keeps it here.
/// let entry: &Allowance = &RUNTIME_BASELINE[0];
/// assert_eq!(entry.id, "process-exec");
/// assert!(entry.rule.starts_with("(allow process-exec"));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Allowance {
    /// Stable identifier, unique across the profile.
    pub id: &'static str,
    /// The SBPL rule, without its provenance comment.
    pub rule: &'static str,
    /// What needs it. `"candidate"` means the design predicted it and no Mac
    /// has confirmed it yet — Task 5 replaces every one of those with the
    /// observation, or deletes the rule.
    pub evidence: &'static str,
    /// The mechanism this allowance is rendered for: `true` for the rules every
    /// mechanism needs, `false` for the ones only the mechanism that wraps the
    /// child's `exec` does.
    ///
    /// A `bool` rather than a `Mechanism` list because exactly one rule is
    /// conditional today, and the question the render asks is the same one this
    /// field's name asks — whether the *exec* mechanism is the one being
    /// rendered. The predicate that reads it is `Allowance::keeps`, so the day a
    /// second conditional rule arrives it is that function and not this field that
    /// grows.
    pub for_every_mechanism: bool,
}

impl Allowance {
    /// Whether this allowance is rendered for `mechanism`.
    fn keeps(self, mechanism: Mechanism) -> bool {
        self.for_every_mechanism || mechanism.wraps_the_exec()
    }
}

/// The allowances a `(deny default)` profile needs before a Rust process can
/// start at all.
///
/// **Measured on macOS 15.8, not predicted, and four entries long.** Each of the
/// four was established by removing the rule and watching what broke — none of
/// them is a guess, and the four failures are **not** four kinds of silence:
/// `sysctl` and `root-traversal` both end in a bare SIGABRT whose only record is
/// the kernel's `deny(1) …` line (`deny(1) sysctl-read hw.pagesize_compat` and
/// `deny(1) file-read-data /` respectively — two rules, two different refused
/// operations, one symptom, measured apart by the A/B design §5's `sysctl`
/// paragraph records — and `sysctl` is the rule that carries the most filters,
/// **eight**: the page-size name is what the abort needs, `hw.ncpu` rides on the
/// same rule for the count the confined child reads (which its own
/// `available_parallelism()` reports when the filter is there and
/// `error(PermissionDenied)` when it is not), and the other six are the sysctl
/// names the loader's own startup probes are refused, re-audited 2026-09-26 and
/// measured in that entry's `evidence`), `process-exec` exits 71 with
/// `execvp() of '<path>' failed`, and
/// `metadata` stops the operator's own read. **Every other entry this table
/// used to carry is deleted**, because the audit measured the real binary rather
/// than the startup probe and both paths agreed it was not needed: `process-fork`,
/// `signal-self`, `system`, `bin` and `private-etc` are five of them, and the
/// five device literals were a sixth row of the same verdict (that table is gone
/// entirely — device files are Linux's concept, so this module has no flag to read
/// and no literals to render; see its own doc). The
/// measurements, with the probe's column beside the product path's, are design
/// §5's audit paragraph; the rule this crate follows is that an entry which
/// survives without an observation naming what broke without it is deleted.
///
/// **The system read set is narrowed to what this crate needs, and the narrowing
/// is the point of the table.** The design's Goal is that the confined process
/// reaches "exactly the directories the operator exposed and nothing else"; §5's
/// measured text was the first version that ran, not the narrowest one that
/// could, and three of its grants are gone or narrowed here:
///
/// - `(subpath "/private")` is replaced by the single subpath below. The blanket
///   granted `/private/var`, which the server has no caller for, and it is the
///   one rule §5's own text flagged as reducible.
/// - `(subpath "/private/var/folders")` — the narrowing that replaced part of
///   that blanket — is **gone too, removed by measurement**. It was the only rule
///   that widened the read surface: with it present a read of a symlink's target
///   outside the exposure **succeeded** (`ok:outside`), and with it removed the
///   same read is `ERR(PermissionDenied)`, while everything the runtime needs is
///   unchanged (`parallelism=6`, `threads=4`, the report write and the marker read
///   both fine). It cannot be narrowed instead of deleted: Seatbelt compares the
///   rule's spelling against the kernel-resolved path, so a grant on the physical
///   `/private/var/folders` reaches every fixture under it and cannot say "this
///   subtree but not that one" — the same rule spelled `T=/private/var/folders/…`
///   left the escape permitted while `T=/var/folders/…` refused it. A profile with
///   **no** temporary-directory grant at all was measured serving every probe, so
///   nothing needs one; the server's temporary files belong inside its writable
///   exposure by design (macOS 15.8, 2026-09-25).
/// - `(subpath "/etc")` and `(subpath "/private/etc")` are both gone. `/etc` is a
///   symlink into `/private`, and Seatbelt compares the rule's literal spelling
///   against the kernel-resolved path (design §5), so a rule spelled `/etc` matched
///   nothing — the access it was meant to grant came from the `/private` blanket,
///   and the narrower `/private/etc` spelling turned out to be unnecessary too
///   (the audit's fourth deleted entry above).
///
/// Two allowances the design previously predicted are **absent** because no
/// observation ever kept them: `mach-lookup` (with it gone every check passes
/// except `getpwuid`, whose failure names
/// `com.apple.system.opendirectoryd.libinfo`, and the serve path resolves no
/// users) and a `mach-lookup` for `com.apple.system.logger`, which was invented
/// rather than observed. Re-adding either requires an observation, not a
/// citation. A third removal is a different kind and is written where it belongs,
/// on the table's own doc: `private-var-folders` was here and was taken out by a
/// measurement of its own.
///
/// **The profile grants no network operation at all, and neither channel needs one.** `accept`
/// on an inherited listener is not gated by the sandbox, which was measured with a profile
/// carrying zero network rules taking a real connection out of a handed-in listener's backlog
/// (design §5, §7); the launcher binds the data plane's TCP socket itself, so serving needs no
/// bind grant either. The **control** channel used to be the exception — the confined child
/// bound its own unix socket, and `bind()` *is* gated, so the profile carried
/// `(allow network-bind)` — and it no longer is: the launcher binds that socket too, outside the
/// sandbox, and the child adopts the descriptor. **The grant is gone rather than narrowed**, and
/// that is the point: it was a bind grant and not a filter, so it permitted binding anything,
/// and the narrower spelling `(allow network-bind (local (subpath (param "S"))))` does not
/// compile at all (`sandbox-exec` refuses the profile). Removing the child's bind removed the
/// need for the rule. See `tests/network.rs` for the assertions that hold both halves.
///
/// **`process-exec` is narrowed to the one exec that has to succeed.** The
/// measurement established that *an* exec must be permitted at all — without the
/// rule `sandbox-exec` exits 71 before any other rule is consulted — and it never
/// established that *every* binary must be. Unfiltered, and with `/usr` and
/// `/System` readable, the rule let the confined child exec anything it
/// could read — `/bin/sh`, `/usr/bin/ssh`, `launchctl` — and that is the confined
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
        evidence: "required: without it sandbox-exec cannot exec the target at all and exits 71 before any other rule is consulted. The code alone is not the discriminator — a missing target produces the same 71 — so the refusal is what stderr says: `execvp() of '<path>' failed: Operation not permitted`. **Measured on the real product path, 2026-09-26, at a frozen revision: the run never reaches the real child — the pre-flight dies `sandbox-exec refused the profile … exit=Some(71) diagnostic=sandbox-exec: execvp() of '/bin/echo' failed: Operation not permitted`, and the kernel names `deny(1) process-exec* /bin/echo`.** The literal is the one exec that must succeed: `P` is the launcher's `current_exe()`, the same value `command_args` writes as the argv's program element, so the rule's filter and the exec `sandbox-exec` performs are one value and cannot disagree. The pre-flight binds the same parameter to `/bin/echo`, which is the exec that probe performs. If the literal is wrong the failure is the refusal above — loud, not a silent widening — and a second exec the launch needs is not covered by any measurement this phase has, so the answer is the unfiltered rule with the over-grant and its follow-up written here, never the broad rule kept quietly",
        // **The one entry a mechanism can drop, and the drop is measured rather than
        // deduced.** The rule exists so `sandbox-exec` can `exec` the child it confines:
        // without it the helper exits `71` before any other rule is consulted, and the
        // pre-flight the `evidence` above quotes is that refusal. **The in-process route
        // renders without it**, because the exec and the dynamic loader have already
        // happened by the time the profile is applied — `Mechanism::wraps_the_exec`
        // is the predicate, and it is the profile's only conditional entry — so on that
        // route the rule could only ever grant an exec the confined process can no longer
        // make. Design §5's in-process A/B is the measurement: the same text applied with
        // this entry removed reached the work, read and wrote its exposure, accepted a
        // connection and served `HTTP/1.1 200 OK`. **`P` goes with it**: `command_args`
        // writes the pair only when the text references it, so the argument this rule
        // resolves is not left on an argv of a launch that has no rule to resolve it.
        for_every_mechanism: false,
    },
    Allowance {
        id: "sysctl",
        rule: "(allow sysctl-read (sysctl-name \"hw.pagesize_compat\") (sysctl-name \"hw.ncpu\") (sysctl-name \"hw.ephemeral_storage\") (sysctl-name \"kern.bootargs\") (sysctl-name \"kern.osvariant_status\") (sysctl-name \"kern.secure_kernel\") (sysctl-name \"kern.version\") (sysctl-name \"security.mac.lockdown_mode_state\"))",
        evidence: "required for the page-size filter; the other seven filters are the re-audit of 2026-09-26. **`hw.pagesize_compat` is the load-bearing filter**: measured on macOS 15.8, 2026-09-25 by a clean A/B on the real product path (`cargo build -p tinio`, then `tinio server /tmp/<short> --port 0 --sandbox`) — with no `sysctl` rule at all, and with `(allow sysctl-read (sysctl-name \"hw.ncpu\"))` alone, the confined child dies before `main` (`thread 'main' panicked at …/stack_overflow.rs: failed to allocate a guard page: Invalid argument (os error 22)`, then `fatal runtime error: initialization or cleanup bug, aborting`, then `status=signal: 6 (SIGABRT)`) while the pre-flight passes both times, and `log stream --predicate 'sender == \"Sandbox\"'` names `deny(1) sysctl-read hw.pagesize_compat` in both. The mechanism is the one §8 measured: std's pre-`main` `stack_overflow::init()` reads the page size and a refused read becomes a `-1` no profile without it can fix, which is why pinning `worker_threads(n)`, or dropping the probe's own `available_parallelism()` call, cannot remove the rule. **`hw.ncpu` is not load-bearing and is not claimed to be**: without it the run still reaches `event: listening`, answers `403`, writes its access log, publishes `state` and forwards SIGTERM to exit `0`, and the child merely answers `available_parallelism()` → `error(PermissionDenied)`; it rides on this rule because the count is a capability the child genuinely wants (`available_parallelism=ok:6` with it) and one entry carrying several filters is valid SBPL — the two-filter form, the two-separate-rules form and the unfiltered control all answer `ok:6`, so a second `id` and a second `evidence` string would buy nothing. **The six after it are the re-audit, and they are enumerated rather than unfiltered.** Measured on macOS 15.8 on 2026-09-26 from the synced tree with one `log stream --predicate 'sender == \"Sandbox\"'` per run, the confined child of `cargo test -p tinio-server --test sandbox_macos_e2e` — and of the plain `tinio server /tmp/<short> --port 0 --sandbox` path — is refused `deny(1) sysctl-read` for **exactly `hw.ephemeral_storage`, `kern.bootargs`, `kern.osvariant_status`, `kern.secure_kernel`, `kern.version` and `security.mac.lockdown_mode_state`**: the union over 19 product-path runs under `sandbox-exec` — 16 e2e runs plus 3 runs of the plain binary — is those six and nothing else, every per-run set is a subset of it (`log stream` coalesces repeated reports, so a single run can show only two of the six), and none of them is fatal — every run reached `event: listening`, served a request and exited `0`. **They are libsystem's own startup probes rather than the product's requests**: the pre-flight's own `/bin/echo` is refused five of the same six under this same profile, so the set belongs to the dynamic loader's variant checks, and under `sandbox-init` — which applies the profile after those checks have already run — six further e2e runs produced one `kern.secure_kernel` refusal between them. **The shape is the enumerated one, not the unfiltered `(allow sysctl-read)`**, because the measurement bounds the set on every path measured, while the unfiltered form would spend every name the kernel exposes (`kern.*`, `hw.*`, `net.*`, `security.*`) on six probes whose answers the product never reads. The widening's cost is those six facts — kernel version and boot arguments, OS-variant status, lockdown mode, and the secure-kernel and ephemeral-storage flags become readable by the confined process — and that is the whole of it: measured with a `sysctlbyname` probe under this rule, the six names answer `ok` while `kern.boottime`, `kern.ostype` and `net.inet.ip.forwarding` are still `Operation not permitted`, so the rule grants the names it spells and nothing else. **A refusal outside this set is a finding rather than a gap**: the verify script asserts zero unexpected refusals, so a macOS update that probes a seventh name reddens it for re-audit, which is the failure this rule wants.",
        for_every_mechanism: true,
    },
    Allowance {
        id: "metadata",
        rule: "(allow file-read-metadata)",
        evidence: "required: /tmp, /var and /etc are symlinks at the root, and a grant on the target does not permit reaching it through the link — `(subpath \"/private/tmp\")` and even the literal full path are refused without this. **Measured on macOS 15.8, 2026-09-19: with the rule removed, the exposed read itself fails `permission denied`** — so the cost is not paid somewhere peripheral, it is the operator's own exposure that stops reading. Cost: existence and timestamps can be probed outside the exposures, contents cannot. **Re-measured on the real product path, 2026-09-26, at a frozen revision: the child reaches `main`, then exits 2 with `tinio: the storage root /tmp/ab-metadata is not usable: Operation not permitted (os error 1) — pass an existing DIR`, the kernel naming `deny(1) file-read-metadata /tmp`** — the product path stops on the operator's own root, not somewhere peripheral",
        for_every_mechanism: true,
    },
    Allowance {
        id: "root-traversal",
        rule: "(allow file-read* (literal \"/\"))",
        evidence: "required: without it the process dies of SIGABRT and the only violation logged is `file-read-data /`. Grants on prefixes under the root do not substitute — the refused read is of the root directory itself. **Measured on macOS 15.8, 2026-09-19: with the rule removed the process exits rc=134 with `Sandbox: deny(1) file-read-data /` on `log stream`** — the same abort the design records, now with the exact log line to search for. **Re-measured on the real product path, 2026-09-25, and what died was the pre-flight's own `/bin/echo`, not `sandbox-exec`**: with the rule removed the launcher never reaches the real child at all — it reports `sandbox-exec refused the profile step=\"sandbox-exec -p\" exit=None diagnostic=` and exits 1 — `sandbox-exec -p` was the pre-flight step's spelling at that revision, and `preflight` is its name today, so the same refusal greps under that — while `log stream --predicate 'sender == \"Sandbox\"'` carries `Sandbox: echo(<pid>) deny(1) file-read-data /` beside the sysctl names the probe also hits. The entry's earlier framing (\"`sandbox-exec` died with no diagnostic\") is the same event described wrongly: the abort is diagnostic-free on the process side, and the kernel line is the diagnostic, which is why it is quoted here rather than the absence. The mechanism is dyld: **`ktrace` shows it resolving its shared cache through relative paths** (`../../System/Volumes/Preboot/Cryptexes/OS/…`), which walks to `/`, and the kernel refuses the read of the root directory itself — measured with the `metadata` confound removed, which is why `(allow file-read-metadata)` in this same table does not cover it. **The rule also grants enumerating `/`, which this entry never stated**: measured on macOS 15.8, 2026-09-25 under the real baseline, a confined `/bin/ls /` lists **16 entries**; with this rule removed it dies `rc=134` and lists **0**; with `(allow file-read-metadata)` removed instead and this rule kept it still lists **16** — so listing the root is this rule's own capability, carried by `file-read*` on the directory, not by the metadata rule",
        for_every_mechanism: true,
    },
];

// **There is no home grant here, and the measurement that removed it is design
// §5's `home` paragraph.** `(allow file-read* (subpath (param "H")))` used to be
// rendered for a read-only run, on the guess that the run's remaining lookups
// needed it; a read-only run without it reaches `listening`, serves, writes its
// access log, publishes `state` and forwards SIGTERM to exit `0`. The one
// home-related failure that survives is a **write** — the read-only run's
// home-derived default state directory is refused `deny(1) file-write-mode
// /Users/<user>/.tinio` — and no read rule can fix it, so a confined read-only
// run is given an explicit writable `TINIO_STATE_DIR` instead. That is why the
// `H` parameter, `home_directory()` and its validation are gone with the rule:
// nothing in the profile names a path outside the exposure set any more, and a
// home lookup that no rule resolves has no reason to exist here.

/// The SBPL text and the `-D` parameters it references.
///
/// The text contains no path: every exposure is rendered as `(param "E<n>")`,
/// and `params` carries the matching values in the same order. Generating both
/// from one traversal of `sandbox.exposures()` is what keeps them in step — and
/// the **one** parameter that is not an exposure is not generated here at all:
/// `P`, the `process-exec` rule's literal, is a launcher-owned value written by
/// `launch::command_args` from the program it also writes as the argv's program
/// element. A parameter a rule references but the launch never defines is a
/// compile error (`sandbox-exec`, exit 65), which is why the two are generated
/// from one traversal rather than listed separately.
///
/// The pairs are `(format!("E{i}"), exposure.host.as_os_str().to_os_string())`
/// for exposure `i` in `sandbox.exposures()` order, and the launch turns each
/// into one `-D NAME=VALUE` argv pair (`launch::command_args`).
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
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::{
///     Mechanism,
///     profile::{Profile, profile, rule_count},
/// };
///
/// let rendered: Profile = profile(
///     &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
///     Mechanism::SandboxExec,
/// )
/// .unwrap();
/// // The path is a `-D` value and never text: the rule refers to it by name.
/// assert!(rendered.text.contains("(subpath (param \"E0\"))"));
/// assert_eq!(rendered.params[0].1, OsString::from("/srv/data"));
/// // `rules` is `rule_count`'s answer carried here, so the field and the text
/// // cannot disagree.
/// assert_eq!(rendered.rules, rule_count(&rendered.text));
/// ```
#[derive(Debug)]
pub struct Profile {
    /// The profile text.
    pub text: String,
    /// `-D` arguments, in exposure order.
    ///
    /// The text's one **other** `(param …)` reference is not here: `P`, the
    /// `process-exec` rule's literal, is the launcher's own value, and
    /// `launch::command_args` writes that pair from the program it is handed —
    /// the same one it writes as the argv's program element.
    pub params: Vec<(String, OsString)>,
    /// The `(allow …)` line count, derived once where the text is rendered
    /// ([`rule_count`]) so that every consumer reads one number rather than scanning
    /// the text again: the render's own event, the launcher's profile event and the
    /// pre-flight's all carry this value.
    pub rules: usize,
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
/// compile step, and `launch::preflight` turns that into a startup error carrying
/// the binary's diagnostic, so the count guard is what makes the common case cheap
/// and legible rather than the only thing between an operator and a silent grant.
const MAX_EXPOSURES: usize = 4096;

/// The values `TEMPLATE` interpolates — the whole of what a rendered profile
/// is, as data.
///
/// Serde's field names are what the template reads, and there is deliberately no
/// path among them: every exposure is rendered from its position alone, so the
/// template can name one without ever spelling it.
#[derive(Serialize)]
struct ProfileView<'a> {
    /// One entry per exposure, in `sandbox.exposures()` order.
    exposures: &'a [ExposureView],
    /// [`RUNTIME_BASELINE`] by reference, in table order — the order is
    /// load-bearing, and the rules travel as themselves rather than as copies.
    baseline: Vec<&'a Allowance>,
}

/// One exposure, as much of it as the text may know: its index is its position
/// and its path is a `-D` value, never a template field.
#[derive(Serialize)]
struct ExposureView {
    /// Whether the exposure is `ReadWrite`, which is one keyword in the rule.
    writable: bool,
}

/// The rendered profile: `TEMPLATE` over `view`, with nothing escaped.
///
/// **`no_escape` is the decision, not the default.** Handlebars escapes HTML by
/// default, which would rewrite every `'` in an `evidence` string to `&#x27;`
/// and every `"` to `&quot;` — corrupting the provenance comments the tested
/// text is made of — so the escape function is replaced with the one that leaves
/// the template's own bytes alone. There is nothing to escape *for*: no field of
/// `ProfileView` is a path, and the text is SBPL, not HTML.
///
/// # Errors
///
/// [`Error::Step`] when the template does not render. `TEMPLATE` is
/// compile-time text and the view is built from it, so this is a programming
/// error rather than an operator-facing one; it is still an `Err` because
/// library code does not panic.
fn render(view: &ProfileView<'_>) -> Result<String> {
    // **The renderer is the workspace's one**, not three lines spelled here: strict mode and
    // the escape function are decisions about rendering, and this module's copy was the third
    // of them (`tinio_util::render` owns the rule and states why).
    renderer()
        .render_template(TEMPLATE, view)
        .map_err(|err| step("render the SBPL profile")(io::Error::other(err.to_string())))
}

/// The rule lines in a rendered profile: one `(allow …)` per exposure and one
/// per [`RUNTIME_BASELINE`] entry.
///
/// **The `(version 1)` and `(deny default)` header is not a rule**, and this is
/// the one place the count is derived — [`Profile::rules`] carries its answer, so
/// every emitter logs the field rather than scanning the text again — so the
/// number an operator compares across two runs means one thing: what the profile
/// grants. It filters the rendered text rather than adding the two collections up,
/// because the text is what the operator and `sandbox-exec` both read.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::profile::rule_count;
///
/// // The `(version 1)`/`(deny default)` header is not a rule.
/// assert_eq!(
///     rule_count("(version 1)\n(deny default)\n(allow file-read*)\n"),
///     1
/// );
/// ```
pub fn rule_count(text: &str) -> usize {
    text.lines()
        .filter(|line| line.starts_with("(allow "))
        .count()
}

/// The network operation `text` names, or `None` when the profile grants nothing
/// on the network — **the one home of that property**, because it is one security
/// property and it was asserted in two places.
///
/// One leg asserts it: this module's `the_profile_grants_no_network_operation_at_all`.
/// A second, byte-identical leg in `tests/network.rs` — the same four spellings and
/// the same check over the same render — is gone: a keyword added to one left the
/// other green, which is the drift this function removes, and the 2026-10-01 placement
/// cleanup deleted the copy the move had already made redundant
/// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`). The leg reads this
/// table, and the table is the whole surface.
///
/// **What is checked is each line's rule and not its provenance comment**, which is
/// the half of this promise the first version did not keep. A rendered line is
/// `<rule> ; tinio:<id> — <evidence>`, so a search over the whole text can be answered
/// by an `evidence` string: a rule added later whose evidence merely says "network"
/// reddened both legs with a message about a rule that mentions no such thing, and a
/// check that fails for prose is not the check its own doc claims. The split is on
/// this crate's own `; tinio:` marker rather than on any `;`, so a name or a value
/// that carries a semicolon is not mistaken for the start of a comment.
///
/// **The list is the operations SBPL can grant on the network plus the wildcard, and
/// the word check over the same rule text is what closes it**: a rule this profile
/// grows later has to name one of the five or mention `network` at all, which is why
/// the answer is the keyword rather than a `bool` — a failing leg prints the operation
/// that reached the text.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::profile::network_operation;
///
/// // Nothing granted on the network, and an evidence string that says the word
/// // does not change that: the answer is about the rules.
/// assert_eq!(
///     network_operation("(deny default)\n; tinio:example — needs no network rule\n"),
///     None
/// );
/// assert_eq!(
///     network_operation("(allow network-outbound)\n"),
///     Some("network-outbound")
/// );
/// ```
///
/// # Measured, macOS 15.8
///
/// A profile carrying zero network rules needs none. `accept` on a listener the
/// parent bound and handed in is not gated by the sandbox at all — a render with
/// zero network rules was observed taking a real connection out of a handed-in
/// listener's backlog, and the kernel logged no `accept` violation (design §5, §7).
///
/// Neither bind needs one either, and both are the launcher's: the data plane's,
/// and the **control** channel's. The confined child used to bind that unix socket
/// itself, which is why the table carried `(allow network-bind)` — a bind *grant*
/// rather than a filter, so it permitted binding anything — and the handoff removed
/// the child's bind, so the rule is gone rather than narrowed (decision 16).
pub fn network_operation(text: &str) -> Option<&'static str> {
    const OPERATIONS: [&str; 4] = [
        "network-bind",
        "network-outbound",
        "network-inbound",
        "network*",
    ];
    // **Per line, and only up to the provenance marker** (see the doc above): the
    // comment tail is an `evidence` string, and one that happens to say "network"
    // must not answer a question about what the profile grants.
    text.lines().find_map(|line| {
        let rule = match line.split_once("; tinio:") {
            Some((rule, _)) => rule,
            None => line,
        };
        OPERATIONS
            .iter()
            .copied()
            .find(|operation| rule.contains(operation))
            .or_else(|| rule.contains("network").then_some("network"))
    })
}

/// Render the profile for `sandbox`, for the mechanism that will apply it.
///
/// The order is stable — the `(version 1)`/`(deny default)` header, the
/// exposures in their order, then [`RUNTIME_BASELINE`] in its table order, with
/// nothing conditional after it — so a diff of two profiles reads as a diff of
/// two exposure sets.
/// **Everything the text interpolates is collected once, as data, and
/// `TEMPLATE` renders it**: this function builds a `ProfileView` and the
/// template turns that into bytes, so the text's shape is one file rather than
/// a sequence of `format!` calls. One traversal still produces both halves:
/// exposure `i` is collected as an `ExposureView` whose template line is
/// `(allow file-read* (subpath (param "E<i>")))`, or with `file-write*` before
/// the `(subpath …)` when it is writable, and `("E<i>", host)` is appended to
/// [`Profile::params`] as it goes.
///
/// **`mechanism` is which process applies this text, and it decides one entry.**
/// `Allowance::keeps` filters the baseline, so a `sandbox-exec` render carries
/// the `process-exec` rule that lets the helper `exec` its target and an
/// in-process render carries every other rule and not that one — the exec having
/// already happened by the time the child applies it
/// ([`Mechanism::wraps_the_exec`]). Everything else about the two renders is
/// identical, which is what keeps the exposure set's profile a function of the
/// exposures and the mechanisms' difference one table entry.
///
/// **The rendering does not escape anything, and that is deliberate rather than
/// `render`'s default forgotten.** The shared renderer registers `no_escape` for
/// every caller ([`tinio_util::render`], which the status report takes it from
/// too), and the reason is sharper here: Handlebars' default escape
/// function is HTML's, so it would rewrite the `'` in an `evidence` string to
/// `&#x27;` and corrupt the provenance comment the rule's trailing `; tinio:<id>
/// — <evidence>` carries — and the byte-for-byte text
/// `tests/exposures.rs` and the end-to-end leg assert against is that comment
/// included. There is no untrusted value in the view either: the text names no
/// path (its `(param …)` references are format strings this file owns) and the
/// one literal that is the operator's is the parameter *name* `E<i>`, whose `i`
/// is an index. `no_escape` leaves the output equal to the template's own bytes.
///
/// **A read-only set renders exactly what a read-write one does.** The home
/// grant that used to make the two differ is gone — measured, not assumed (design
/// §5's `home` paragraph) — so the access keyword on an exposure's own rule is
/// the whole of the difference between the modes. The device-file flag renders
/// nothing at all on this platform (this module's doc).
///
/// # Errors
///
/// [`Error::BadPath`] for an exposure that cannot be a parameter: not absolute,
/// not valid UTF-8, or carrying a control character — and for an exposure set
/// past `MAX_EXPOSURES`, where the error names the first exposure the profile
/// cannot express.
///
/// The template itself is the one failure this function did not have before the
/// text moved into `TEMPLATE`, and `render` maps it to [`Error::Step`] rather
/// than letting library code panic. A caller cannot reach it — the template is
/// compile-time text and `ProfileView` is built to fit it — so the operator's
/// error surface is unchanged.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_macos::{Mechanism, profile::profile};
///
/// let rendered = profile(
///     &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
///     Mechanism::SandboxExec,
/// )
/// .unwrap();
/// assert!(rendered.text.starts_with("(version 1)\n(deny default)\n"));
/// assert!(
///     rendered
///         .text
///         .contains("(allow file-read* (subpath (param \"E0\")))")
/// );
/// assert!(
///     !rendered
///         .text
///         .contains("file-write* (subpath (param \"E0\"))")
/// );
/// assert_eq!(
///     rendered.params[0],
///     (String::from("E0"), OsString::from("/srv/data"))
/// );
/// // The one entry the mechanism decides: the helper cannot exec without it,
/// // and the in-process route has already exec'd.
/// assert!(rendered.text.contains("(allow process-exec"));
/// assert!(
///     !profile(&Sandbox::new(), Mechanism::SandboxInit)
///         .unwrap()
///         .text
///         .contains("(allow process-exec")
/// );
/// ```
pub fn profile(sandbox: &Sandbox, mechanism: Mechanism) -> Result<Profile> {
    if sandbox.exposures().len() > MAX_EXPOSURES {
        let over = &sandbox.exposures()[MAX_EXPOSURES];
        return Err(refuse(
            &over.host,
            "exposure set exceeds the SBPL profile limit",
        ));
    }

    // Everything the template interpolates, collected before anything is
    // rendered: the exposures in their order and the baseline as it stands.
    let mut params = Vec::with_capacity(sandbox.exposures().len());
    let mut exposures = Vec::with_capacity(sandbox.exposures().len());
    for (index, exposure) in sandbox.exposures().iter().enumerate() {
        validate_path(&exposure.host)?;
        // The value never reaches the view: the text names the parameter and the
        // value is carried beside it, so a path cannot close a literal or start
        // a rule whatever it contains.
        let param = format!("E{index}");
        exposures.push(ExposureView {
            writable: !exposure.access.is_read_only(),
        });
        params.push((param, exposure.host.as_os_str().to_os_string()));
    }

    // Nothing is rendered for device files here, and that is the decision rather
    // than an omission: they are Linux's concept, the jail that binds the four
    // nodes is Linux's, and the flag never reaches this crate at all. The literals
    // this position used to emit were measured unnecessary on both paths and
    // deleted; the module doc says so.

    // **No conditional grant is collected here.** The only one this function ever
    // had was the home read, and the list it was appended to is now exactly the
    // exposures: a read-only run's profile and a read-write run's differ in the
    // access keyword and nowhere else (the measurement that removed the grant is
    // the block above `Profile`).

    // **The profile's network surface is empty, and there is no conditional here to get wrong.**
    // `accept` on the inherited listener is not gated by the sandbox at all (design §5, §7), the
    // data plane's bind is the launcher's, and the control channel's is too — so no rule in this
    // text mentions a network operation, on either channel.

    let text = render(&ProfileView {
        exposures: &exposures,
        // **The table is filtered rather than rewritten**, so the rule the text
        // carries and the table a reader consults for its provenance are one
        // entry: a mechanism that drops `process-exec` drops one member of this
        // list and changes nothing else about the render.
        baseline: RUNTIME_BASELINE
            .iter()
            .filter(|allowance| allowance.keeps(mechanism))
            .collect(),
    })?;
    let rules = rule_count(&text);

    // One event per operation, carrying the shape of the profile rather than
    // its text (shared §7): the counts are what an operator compares across two
    // runs, and the text is already in the process list and in the error
    // messages that carry it. `info!` and not `debug!` because this is the
    // answer to "what was this process confined to" — and because `serve`'s
    // subscriber is a fixed `Verbosity::Info`, so a `debug!` here would be a
    // record nobody could read. `rules` counts the `(allow …)` lines and not the
    // text's lines: the `(version 1)`/`(deny default)` header is not a rule
    // ([`rule_count`]).
    info!(
        step = "profile",
        mechanism = %mechanism,
        rules,
        params = params.len(),
        "the SBPL profile is rendered"
    );

    Ok(Profile {
        text,
        params,
        rules,
    })
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
/// **The resolution is the operator's own paths and nothing else.** The baseline
/// grants no temporary directory (`/private/var/folders`, the resolution of
/// `$TMPDIR`, was removed by measurement — see [`RUNTIME_BASELINE`]), so this
/// function is not a route to one: a caller that exposes nothing under
/// `$TMPDIR` gets no grant there, and the confined process's temporary files
/// belong inside its writable exposure by design.
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
pub fn with_canonical_paths(sandbox: &Sandbox) -> CanonicalExposures {
    let mut exposures = sandbox.exposures().to_vec();
    for exposure in sandbox.exposures() {
        let Ok(canonical) = canonicalize(&exposure.host) else {
            continue;
        };
        // **A canonical form joins the set as it stands and the merge below folds
        // it**, which is the Windows arm's spelling too: the most-permissive rule
        // for one path is [`Sandbox::from_exposures`]'s, so a canonical form that
        // is already present — or that another exposure canonicalises to — is its
        // to reconcile rather than this loop's to test for. The guard is only the
        // one rewrite this function exists for.
        if canonical != exposure.host {
            exposures.push(Exposure {
                host: canonical,
                access: exposure.access,
            });
        }
    }

    // Nothing of the caller's set is left behind: the rebuilt one is the exposures
    // this function canonicalised, and a `Sandbox` carries nothing else.
    CanonicalExposures(Sandbox::from_exposures(exposures))
}

/// An exposure set that has been through [`with_canonical_paths`] — **the distinction a raw
/// [`Sandbox`] cannot carry.**
///
/// `Sandbox` is what both stages are made of, so nothing but a comment separated "the set as the
/// caller spelled it" from "the set the profile may name": Seatbelt compares a rule's path
/// against the *real* path, so a rule rendered from the caller's spelling matches nothing, and a
/// launch that rendered the set as it arrived would be confined by rules that never fire. This
/// layer is that distinction — [`with_canonical_paths`] hands back no other value, and it is the
/// only way to reach [`Self::sandbox`].
///
/// **The renderer still takes a raw `Sandbox`**, and that is deliberate rather than an oversight:
/// a test that renders the set *as it stands* is how "this rule is the canonicalisation's doing"
/// is proved (see `tests/injection.rs`), so narrowing [`profile`]'s parameter would make that
/// property unstatable. What the layer buys is that a caller reaching the renderer from a launch
/// has to have gone through the canonicalisation to say so.
#[derive(Debug)]
pub struct CanonicalExposures(Sandbox);

impl CanonicalExposures {
    /// The set, for the renderer — the one crossing point this layer has.
    pub fn sandbox(&self) -> &Sandbox {
        &self.0
    }

    /// The exposures: the canonical spellings, and the caller's own where they differed.
    pub fn exposures(&self) -> &[Exposure] {
        self.0.exposures()
    }
}

/// Refuse a path this profile cannot name, logging where the `Error` is built.
///
/// Shared §7's rule for a failure: the event and the error carry the same
/// values, so a refusal that has no `step` to name is still diagnosable from the
/// log — and the reason string is written once, used by both, and cannot drift.
fn refuse(path: &Path, reason: &'static str) -> Error {
    error!(path = %path.display(), reason, "the path cannot be expressed in the profile");
    Error::BadPath {
        path: path.to_path_buf(),
        reason,
    }
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
    // **`has_root`, not `is_absolute`.** The rule the confinement needs is
    // "carries a root", and the difference is historical rather than live: this
    // crate is macOS-only now (its own `#![cfg]`), so the two answers agree on
    // every path it can be handed, and `has_root` is kept because it is the
    // weaker and unambiguous reading of the rule — a path with a root is one a
    // profile rule can be written against, whatever the host's separator rules
    // say. A POSIX spelling under a non-POSIX host is the case that made the
    // distinction matter, and it can no longer reach this function.
    if !path.has_root() {
        return Err(refuse(path, "not absolute"));
    }
    if raw.chars().any(char::is_control) {
        return Err(refuse(path, "control character"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::{OsStr, OsString},
        fs::{canonicalize, create_dir},
        os::unix::{ffi::OsStrExt, fs::symlink},
        path::PathBuf,
    };

    use tinio_sandbox_core::{AccessMode, Sandbox};

    use super::*;
    use crate::Mechanism;

    /// The exposure pairs a run's profile must produce: the parameters of
    /// `pairs` and nothing else.
    ///
    /// **Exact vectors, because there is no conditional parameter left.** The
    /// only one this generator ever added on top of the exposures was the home
    /// grant's `H`, and the grant is gone (the measurement is the block above
    /// `Profile`). Neither mode's set depends on the host's `$HOME` any more, so
    /// a hard-coded vector is the honest assertion on every runner.
    fn exposures_only(pairs: &[(&str, &str)]) -> Vec<(String, OsString)> {
        pairs
            .iter()
            .map(|(name, value)| (String::from(*name), OsString::from(*value)))
            .collect()
    }

    #[test]
    fn the_base_is_deny_default() {
        let rendered = profile(
            &Sandbox::new().expose("/srv/data", AccessMode::ReadWrite),
            Mechanism::SandboxExec,
        )
        .unwrap();
        let mut lines = rendered.text.lines();
        assert_eq!(lines.next(), Some("(version 1)"));
        assert_eq!(lines.next(), Some("(deny default)"));
    }

    #[test]
    fn the_mechanism_decides_exactly_the_tables_conditional_entry() {
        // **The render is a filter over `RUNTIME_BASELINE`, and this leg holds it
        // to that.** For every entry the table carries, a mechanism's text names
        // the entry's `; tinio:<id>` marker exactly when `Allowance::keeps` says
        // that mechanism gets it — so an entry added, re-flagged or dropped is
        // asserted here without this test naming it, and the comparison is by id
        // rather than by a count the table's own size would have to re-derive.
        //
        // `process-exec` is the one conditional entry and the entry the two
        // mechanisms exist to differ over (`Mechanism::wraps_the_exec`):
        // `sandbox-exec` cannot exec the child without it, while the in-process
        // route's exec and dynamic loader have already happened. Both directions
        // are asserted by name beside the loop, because a table that silently
        // stopped carrying the entry would pass the filter test vacuously.
        for mechanism in [Mechanism::SandboxExec, Mechanism::SandboxInit] {
            let rendered = profile(&Sandbox::new(), mechanism).unwrap();
            for allowance in RUNTIME_BASELINE {
                let marker = format!("; tinio:{} —", allowance.id);
                assert_eq!(
                    rendered.text.contains(&marker),
                    allowance.keeps(mechanism),
                    "`{}` under {mechanism}: the text and the table disagree",
                    allowance.id
                );
            }
        }

        let helper = profile(&Sandbox::new(), Mechanism::SandboxExec).unwrap();
        let in_process = profile(&Sandbox::new(), Mechanism::SandboxInit).unwrap();
        assert!(
            helper.text.contains("(allow process-exec"),
            "{}",
            helper.text
        );
        assert!(
            !in_process.text.contains("(allow process-exec"),
            "{}",
            in_process.text
        );
        // The parameter that rule resolves goes with it: `P` is the launcher's
        // (`launch::command_args` writes it, and the pre-flight binds it to
        // `/bin/echo`), so neither render declares it in the profile's own list
        // and the in-process launch leaves nothing on an argv it does not write.
        for rendered in [&helper, &in_process] {
            assert!(
                rendered.params.iter().all(|(name, _)| name != "P"),
                "{:?}",
                rendered.params
            );
        }
    }

    #[test]
    fn read_write_grants_the_write_and_read_only_does_not() {
        let rw = profile(
            &Sandbox::new().expose("/srv/data", AccessMode::ReadWrite),
            Mechanism::SandboxExec,
        )
        .unwrap();
        assert!(
            rw.text
                .contains("(allow file-read* file-write* (subpath (param \"E0\")))"),
            "{}",
            rw.text
        );
        assert_eq!(rw.params, exposures_only(&[("E0", "/srv/data")]));
        // **The read-write half carries no home rule and no `H` parameter**, which
        // is the whole of what the audit bought: the grant is spent only where a
        // home lookup survives, and a read-write run makes none.
        assert!(!rw.text.contains("(param \"H\")"), "{}", rw.text);
        assert!(
            rw.params.iter().all(|(name, _)| name != "H"),
            "{:?}",
            rw.params
        );

        let ro = profile(
            &Sandbox::new().expose("/srv/data", AccessMode::ReadOnly),
            Mechanism::SandboxExec,
        )
        .unwrap();
        assert!(
            ro.text
                .contains("(allow file-read* (subpath (param \"E0\")))"),
            "{}",
            ro.text
        );
        assert!(
            !ro.text.contains("file-write* (subpath (param \"E0\"))"),
            "{}",
            ro.text
        );
        // Read-only is the absence of the write keyword, not the absence of the
        // parameter: both access rules name the same exposure the same way, and
        // the parameter list is exactly the exposures in both modes.
        assert_eq!(ro.params, exposures_only(&[("E0", "/srv/data")]));
    }

    #[test]
    fn neither_mode_renders_a_path_outside_the_exposure_set() {
        // **The home grant is gone, and this is the leg that keeps it gone.** It
        // used to be rendered for a read-only run alone — `(allow file-read*
        // (subpath (param "H")))` with `H` = the whole of `$HOME` — on the guess
        // that a read-only run's remaining lookups needed it. A read-only run
        // measured green without it (design §5's `home` paragraph) deleted the rule,
        // the parameter with it, and this assertion is what a re-introduction
        // trips: the text names no parameter but the exposures, and `params`
        // carries no entry for one.
        let read_only = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadOnly)
            .expose("/srv/data/.tinio", AccessMode::ReadWrite);
        let read_write = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadWrite)
            .expose("/srv/state", AccessMode::ReadWrite);

        for (mode, rendered) in [
            (
                "read-only",
                profile(&read_only, Mechanism::SandboxExec).unwrap(),
            ),
            (
                "read-write",
                profile(&read_write, Mechanism::SandboxExec).unwrap(),
            ),
        ] {
            assert!(
                !rendered.text.contains("(param \"H\")"),
                "{mode} renders a home rule again:\n{}",
                rendered.text
            );
            assert!(
                rendered.params.iter().all(|(name, _)| name != "H"),
                "{mode} carries an `H` parameter: {:?}",
                rendered.params
            );
        }
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
        let rendered = profile(&sandbox, Mechanism::SandboxExec).unwrap();
        assert!(
            rendered
                .text
                .contains("(allow file-read* file-write* (subpath (param \"E1\")))"),
            "{}",
            rendered.text
        );
        assert_eq!(
            rendered.params,
            exposures_only(&[("E0", "/srv/data"), ("E1", "/home/u/.tinio/roots/h"),])
        );
    }

    #[test]
    fn the_sysctl_rule_carries_the_page_size_and_the_cpu_count() {
        // **Two filters, one rule, and neither is decoration.** The page-size
        // name is load-bearing *before `main`*: measured 2026-09-25 by the A/B
        // the `sysctl` entry's `evidence` carries, this filter reaches
        // `listening`, while no rule at all and an `hw.ncpu`-only rule both die
        // of the guard-page abort (SIGABRT, `deny(1) sysctl-read
        // hw.pagesize_compat`). The cpu-count name is **not** load-bearing and is
        // not asserted as if it were: dropping it costs the count the confined
        // child reads (`available_parallelism()` → `error(PermissionDenied)`,
        // then tokio sizes its pool from one thread) and nothing else, which is
        // the whole reason it rides on this rule rather than on one of its own.
        // Both together are valid SBPL, and the confined child's own probe
        // answers `ok:6` under them — so both filters are asserted, because a
        // rule that quietly lost one would still start and still serve.
        //
        // **The assertion reads the rendered rule, and the split is what makes
        // it an assertion.** A rendered line is `<rule> ; tinio:<id> — <evidence>`,
        // and this entry's evidence *quotes both filters* — so a `contains` over
        // the whole line asserts nothing at all: the measured negative control
        // (VM, 2026-09-25) put the rule back to the single-filter form and the
        // leg still passed on the evidence's own copy of the name. Everything
        // after the provenance marker is therefore dropped before the filters
        // are looked for.
        let rendered = profile(&Sandbox::new(), Mechanism::SandboxExec).unwrap();
        let line = rendered
            .text
            .lines()
            .find(|l| l.starts_with("(allow sysctl-read"))
            .unwrap_or_default();
        let rule = line
            .split_once("; tinio:")
            .map_or(line, |(rule, _evidence)| rule);
        assert!(
            rule.contains("(sysctl-name \"hw.pagesize_compat\")"),
            "the page-size filter, which is what the pre-`main` guard page needs, is gone:\n{}",
            rendered.text
        );
        assert!(
            rule.contains("(sysctl-name \"hw.ncpu\")"),
            "the cpu-count filter is gone, so the confined child sizes its runtime from one thread:\n{}",
            rendered.text
        );
        assert!(
            !rule.starts_with("(allow sysctl-read)"),
            "the unfiltered rule is back:\n{}",
            rendered.text
        );
        // An empty set is no exposures and no parameters: the exact `is_empty` is
        // the assertion, rather than a filter over the pairs, because a parameter
        // appearing from an empty set is a rule grant nobody asked for.
        assert!(rendered.params.is_empty(), "{:?}", rendered.params);
    }

    #[test]
    fn no_device_literal_is_rendered_on_this_platform() {
        // Device files are Linux's concept and this crate has no flag for them, so
        // the only thing left to assert is the text: the literals this profile used
        // to carry were measured unnecessary on both paths and deleted (design §5's
        // audit paragraph), and this leg is what fails if one comes back without a
        // measurement.
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        let rendered = profile(&sandbox, Mechanism::SandboxExec).unwrap();
        for device in [
            "/dev/null",
            "/dev/zero",
            "/dev/random",
            "/dev/urandom",
            "/dev/stdout",
            "/dev/stderr",
        ] {
            assert!(
                !rendered.text.contains(device),
                "{device} is rendered again:\n{}",
                rendered.text
            );
        }
    }

    #[test]
    fn the_profile_grants_no_network_operation_at_all() {
        // The property is asked of `network_operation`, which owns the keyword table and the
        // measurement behind it (design §5, §7) — this leg and `tests/network.rs`'s used to carry
        // the same four spellings and the same word check twice, so a keyword added to one left
        // the other green.
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        let rendered = profile(&sandbox, Mechanism::SandboxExec).unwrap();
        assert_eq!(
            network_operation(&rendered.text),
            None,
            "no rule in this profile may mention a network operation: {}",
            rendered.text
        );
        // The parameters are separate argv data and are never parsed as rules, so no exposure —
        // however its path is spelled — can widen this surface; `the_profile_text_carries_no_path`
        // is the measurement of that.
    }

    #[test]
    fn the_network_check_reads_the_rules_and_not_the_provenance_comments() {
        // **The positive control the property above needs** (SPEC-P3-08), and the reason the
        // check is written per line: a rendered line is `<rule> ; tinio:<id> — <evidence>`, so a
        // search over the whole text can be answered by an `evidence` string — a rule added
        // later whose evidence merely says "network" reddened both network legs with a message
        // about a rule that grants nothing. Three answers are pinned here: prose is not a rule,
        // a rule's own spelling still is one, and the keyword fallback still closes the list.
        assert_eq!(
            network_operation("(deny default)\n; tinio:example — needs no network rule\n"),
            None,
            "an evidence string is not a rule"
        );
        assert_eq!(
            network_operation("(allow file-read*)\n; tinio:example — no network here\n"),
            None,
            "a rule that grants nothing, under a comment that names an operation"
        );
        assert_eq!(
            network_operation("(allow network-bind) ; tinio:example — binds\n"),
            Some("network-bind"),
            "the same shape with the operation in the rule is answered"
        );
        assert_eq!(
            network_operation("(allow network-something-else) ; tinio:example — tail\n"),
            Some("network"),
            "a spelling outside the table is still closed by the word check"
        );
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
        let rendered = profile(&sandbox, Mechanism::SandboxExec).unwrap();
        assert!(!rendered.text.contains("/srv"), "{}", rendered.text);
        assert!(!rendered.text.contains("/home/u"), "{}", rendered.text);
        assert!(
            !rendered.text.contains("network-outbound"),
            "{}",
            rendered.text
        );
        assert!(
            rendered.text.contains("(param \"E0\")"),
            "{}",
            rendered.text
        );
        assert!(
            rendered.text.contains("(param \"E1\")"),
            "{}",
            rendered.text
        );
        // The hostile value is carried verbatim — it is data, and nothing here
        // needs to escape it, quote it or refuse it. The set is a read-only
        // root plus a read-write state directory, so it is also the shape a
        // read-only run builds — and it carries no parameter but the exposures.
        assert_eq!(
            rendered.params,
            exposures_only(&[
                ("E0", hostile.to_str().expect("the hostile path is UTF-8")),
                ("E1", "/home/u/data"),
            ])
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

        let err = profile(&sandbox, Mechanism::SandboxExec).unwrap_err();
        // The exposure the error names is the first one the profile cannot
        // express — the 4097th — not the first of the set.
        let Error::BadPath { path, reason } = err else {
            panic!("expected BadPath, got {err:?}")
        };
        assert_eq!(reason, "exposure set exceeds the SBPL profile limit");
        assert_eq!(path, PathBuf::from(format!("/srv/data/{MAX_EXPOSURES}")));
    }

    #[test]
    fn a_relative_path_is_refused() {
        let err = profile(
            &Sandbox::new().expose("relative/path", AccessMode::ReadOnly),
            Mechanism::SandboxExec,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "not absolute",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_non_utf8_path_is_refused_rather_than_mangled() {
        // A lossy conversion would name a *different* path than the one the
        // operator exposed, which is worse than refusing to start.
        let raw = OsStr::from_bytes(b"/srv/\xff\xfe");
        let err = profile(
            &Sandbox::new().expose(raw, AccessMode::ReadOnly),
            Mechanism::SandboxExec,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "not valid UTF-8",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn control_characters_are_refused() {
        let err = profile(
            &Sandbox::new().expose("/srv/a\nb", AccessMode::ReadOnly),
            Mechanism::SandboxExec,
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "control character",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn canonical_paths_are_added_when_they_differ() {
        // macOS resolves /tmp to /private/tmp and $TMPDIR to
        // /private/var/folders/…; a rule naming only the un-resolved path
        // would match nothing (the empirical check is Task 6's symlinked root).
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        create_dir(&real).unwrap();
        let link = dir.path().join("link");
        symlink(&real, &link).unwrap();
        let canonical_real = canonicalize(&real).unwrap();

        let sandbox = with_canonical_paths(&Sandbox::new().expose(&link, AccessMode::ReadWrite));
        let hosts: Vec<_> = sandbox.exposures().iter().map(|e| e.host.clone()).collect();
        assert!(hosts.contains(&link), "{hosts:?}");
        assert!(hosts.contains(&canonical_real), "{hosts:?}");

        // The rewrite is the module's whole point, so this test expects it
        // rather than working around it: `tempfile::tempdir()` lives under
        // `$TMPDIR` = `/var/folders/…`, which `canonicalize` rewrites to
        // `/private/var/folders/…`, so the canonical form of a tempdir path is
        // always a *second* path and the asserted length of this case is 2.
        // An earlier version asserted 1 and could only pass on a host without
        // that rewrite; the crate is macOS-only now, so the macOS answer is the
        // whole assertion and the second arm is gone with the other targets.
        let rewritten = with_canonical_paths(&Sandbox::new().expose(&real, AccessMode::ReadWrite));
        assert!(
            rewritten
                .exposures()
                .iter()
                .any(|e| e.host == canonical_real),
            "{:?}",
            rewritten.exposures()
        );
        assert_eq!(
            rewritten.exposures().len(),
            2,
            "{:?}",
            rewritten.exposures()
        );

        // A path that already is its canonical form is not duplicated — which
        // is a claim only a path this test canonicalised itself can support.
        let plain =
            with_canonical_paths(&Sandbox::new().expose(&canonical_real, AccessMode::ReadWrite));
        assert_eq!(plain.exposures().len(), 1, "{:?}", plain.exposures());
    }

    #[test]
    fn canonicalisation_leaves_the_access_mode_alone() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
        let out = with_canonical_paths(&sandbox);
        assert!(
            out.exposures()
                .iter()
                .all(|e| e.access == AccessMode::ReadOnly)
        );
    }
}
