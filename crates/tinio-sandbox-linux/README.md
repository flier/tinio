# `tinio-sandbox-linux` — the Linux backend: in-place mount-namespace confinement

`tinio-sandbox-linux` implements `tinio-sandbox-core`'s `Backend` for Linux: `confine` is a one-way door applied to the **calling** process — a private user and mount namespace, a mirror of the exposure set with `pivot_root` over it, a capability drop and a seccomp filter on top. It launches nothing and supervises nothing (`FACTS` is `PlatformFacts::NONE`), so there is no child, no payload and no exit status to forward. The crate is behind `#![cfg(target_os = "linux")]`, so it compiles to an empty library on any other host and its own tests exist only on a Linux one. `tinio-sandbox` is the facade that selects it, `tinio-sandbox-core` owns the contract and the launcher machinery, and `docs/sandbox.md` owns the operator-facing `--sandbox` notes — the selectors, the capability drop, the syscall set and the jail's path namespace.

## Features

| Feature | Effect |
|---|---|
| `testing` | compiles `src/testing.rs`: this platform's host precondition and the shared recording devices, for this crate's own suites and `tinio-server`'s Linux e2e legs |

## Layout

| File | What it is |
|---|---|
| `src/enter.rs` | `confine`: build the jail and step into it |
| `src/plan.rs` | the ordered mount list that turns a `Sandbox` into a jail, and the device files it binds |
| `src/jail.rs` | where the jail lives on the host, and where each exposed path lands inside it |
| `src/mechanism.rs` | the model (`Auto`/`userns`/`root`), its resolution, and the availability probe behind `Auto` |
| `src/caps.rs` | which capabilities the confined process keeps |
| `src/seccomp.rs` | the filter that denies outbound connection setup |
| `src/options.rs` | the backend's construction-time options, `TINIO_SANDBOX_MODEL` among them |
| `src/metrics.rs` | the two metric families this process records about its own confinement (crate-internal; the `/metrics` endpoint that gathers them is the management plane's) |
| `src/testing.rs` | test-only support, behind `testing` |
| `src/bin/sandbox-child.rs` | the confined half of the jail test: the jail is a one-way door, so it runs in this child and reports one `key: value` line per check |
| `src/lib.rs` | `Backend`, `FACTS`, `DIAGNOSTICS` and the re-exports |

## What the host owes the confinement

- **An unprivileged user namespace, or uid 0 for the `root` model.** `Mechanism::Auto` — what an unset selector resolves to — probes for the first and falls back to the second, and refuses when neither is available rather than confining half-way. What each model keeps, and the procfs the sub-5.2 fallback binds through, are `docs/sandbox.md`'s to state.
- **A single-threaded process at the moment it confines itself.** Every step of the sequence — `unshare`, `no_new_privs`, the capability drop and the seccomp filter — applies to the calling task alone, so a second thread would keep the host mount namespace, the full credentials and an unfiltered network for the life of the process while the run reported itself confined. `confine` reads `/proc/self/task` and refuses with the count before the first irreversible step, and a `/proc` it cannot read refuses too. The remedy belongs to the entry point, which builds its subscriber and its runtime *after* the confinement.
- **A kernel that can mount what the plan mounts.** The jail's own residue is bounded rather than avoided: the mountpoint is a fixed root-derived name the next run takes over, so a hard kill leaves at most one directory per served root.
- **A host that can actually confine, for the crate's own tests.** The tests ask that capability before the child runs, so a host which cannot confine produces a failure rather than a suite of skips that reports success for having tested nothing.

## Gates

The crate's own verification is a Linux host, and nothing else can stand in for it:

```console
bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh
```

The script refuses to run without two distinct sets of things. **The confinement's own precondition comes first**: a Linux host, an unprivileged user namespace, and passwordless `sudo` for the privileged leg. **The client tooling its external legs shell out to comes second**: `aws`, `rclone` and `mc` on `PATH`, plus the boto3 virtualenv the `@boto3` scenarios run inside — `TINIO_BOTO3_PYTHON`, or its conventional home under the target directory. Neither set is a skip when it is missing: the script fails outright rather than reporting a run it could not do. It exercises both models, the confined server end to end, and the external e2e suite that the default workspace run never selects; `docs/tests.md`'s "Platform-gated verification" section owns the shared method, including which legs CI compiles and which are the operator's.
