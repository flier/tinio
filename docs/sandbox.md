# `--sandbox`: what an operator has to know

`--sandbox` confines the server to the given paths — private mount namespace, `pivot_root`, seccomp denying eleven syscalls + the whole x32 ABI.

Notes = Linux unless a heading names another platform. Each item = confinement working as designed — all three read as bugs the first time.

- **A selector's value outside its accepted set is a usage error (exit 2) on every platform**, never a fallback default.
- Selectors: `TINIO_SANDBOX_MODEL` (`userns` / `root`), `TINIO_SANDBOX_MACOS_MECHANISM`, `TINIO_SANDBOX_CAPABILITIES`.

## Linux: log lines and the exposure bind

The in-place arm installs the tracing subscriber **after** `confine` — the jail's own events are lost. Failure still reaches the operator: error, exit code, stderr (design decision 26).

Each exposure is bound from a descriptor opened once — never the path again; exposing a link fails at startup with the path named. A rename *before* the open can still change what is exposed.

Kernels below 5.2 fall back to a procfs-based bind, which **requires procfs mounted** where the jail is built — without it the host refuses to confine.

- `TINIO_SANDBOX_FORCE_PROC_FALLBACK` (any value) forces the fallback; it grants no authority.

## macOS: two mechanisms, `sandbox-exec` by default

- `TINIO_SANDBOX_MACOS_MECHANISM` selects how the profile reaches the confined child.
- `sandbox-exec` (default) wraps the child's `exec` with Apple's deprecated-but-shipped helper.
- `sandbox-init` invokes none — the child applies the rendered profile before serving; typo = usage error (exit 2).
- `sandbox-init` confinement is **one-shot and irreversible** — no fork or exec afterwards; a wrong profile costs a restart.
- Its profile is **one rule shorter** (`process-exec` is helper-only) — a measurement, not a missing grant (decision 6).

## Windows: a served root behind a junction is granted at its target, and the child resolves the operator's spelling

The launcher grants the **canonical** spelling of every exposure; a path through a reparse point is refused. A root handed as a directory junction is therefore granted on the junction's *target*.

The child re-invokes with the operator's argv and refuses before dispatch when it cannot resolve the junction as a confined process.

- macOS adds the resolved spelling *beside* the operator's; **ruled: Windows serves the canonical spelling alone**.
- Record: `docs/superpowers/plans/2026-09-25-sandbox-followups.md` §22.

## Windows: two per-user files, and the re-grant the salt costs

- The local app-data directory holds the two per-user files: `acl-salt` (created once) and `acl-ledger.jsonl`.
- The salt is mixed into the profile name — without it the name is a pure function of the served root.
- The ledger records what a run granted (design §6); **revocation stays manual**.
- The directory is **repaired, not only created** when its DACL is not this user's alone.
- **The first run after the salt lands re-grants every served root**; old unsalted ACEs stay as residue (decision 15).
- An unreadable or uncreatable salt warns; that run derives the *unsalted* name.
- The program directory's `ALL APPLICATION PACKAGES` RX is unchanged.

## The process has no capabilities, even when it runs as root

- Capabilities are dropped before the server starts — files are read as the account, not the superuser.
- `root` model = uid 0 with only the `CAP_CHOWN` the `local_uid` mapping needs; `userns` = the one mapped uid.
- A file the account cannot read is refused (`AccessDenied`).
- Run the server as the account that owns the data, or `chown -R` the served root to it.

## Outbound connections are denied, and the filter cannot see the address family

- Seccomp denies **eleven** syscalls by number — the socket paths out and three `io_uring` calls.
- The ptrace family too; the whole x32 ABI is refused.
- No address family passes: syslog, the Docker socket and D-Bus fail `EPERM` inside; so does a Unix socket in an exposed dir.
- Deliberate — server logs go to stderr. Debuggers die inside the jail (`gdb`, `perf`, `strace`); must deny.

## The host's path namespace does not exist inside the jail

- Jail = private mount namespace with `pivot_root`: the exposure set + the four device files `serve` binds.
- The device files: null, zero, random and urandom, each at its original absolute path.
- Everything else absent, not refused — a path that is neither fails "no such file or directory".
- Linux differs: Windows (AppContainer DACLs) and macOS (Seatbelt) confine by policy — host paths visible, refused.
- Config, helper binary or data path the server needs must be exposed.
