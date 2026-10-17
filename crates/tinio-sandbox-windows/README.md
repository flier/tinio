# `tinio-sandbox-windows` — the Windows backend: an AppContainer, a DACL whitelist and a signal relay

`tinio-sandbox-windows` implements `tinio-sandbox-core`'s `Backend` for Windows. AppContainer membership is fixed at process creation, so this backend can only confine a process it creates: it writes the container's package-SID ACEs onto the exposed paths, launches the child inside the container, and supervises it. The confinement is three mechanisms rather than one — the DACL whitelist decides which paths the confined process may open, the container's capability set decides what it can reach over the network, and the relay forwards the stop the launcher receives. `tinio-sandbox` is the facade, the shared Windows primitives this crate is built on sit in `tinio-util` (the wide-string writer) and this crate's own `acl` directory (the ACE walk moved in from the dissolved `tinio-winsec`), and the operator-facing notes — the grant's cost, the launcher's own-directory rule, the junction spelling, the one per-user file — stay where they are, in `docs/sandbox.md`.

## Features

| Feature | Effect |
|---|---|
| `testing` | compiles `src/testing.rs`: the Windows confinement harness and the shared recording devices, for this crate's own suites and `tinio-server`'s Windows e2e targets |

## Layout

| File | What it is |
|---|---|
| `src/backend/` | the confinement flow: prepare the launch, grant the whitelist, spawn a supervised child, hand the supervision back |
| `src/backend/exposures/` | exposure policy: the caller's resolved set, the loader entries beside the program, and the reparse-point refusals |
| `src/backend/capability.rs` | the capability list the container may hold |
| `src/backend/grant.rs` | the grant phase's budget, its warning threshold, and the event that reports the measurement |
| `src/acl/` | writing the package SID's ACEs, reading a descriptor back, the per-path lock the read-merge-write is taken under, and the ACE walk moved in from the dissolved `tinio-winsec` |
| `src/sid.rs` | the derived package SID, the per-root launch lock, and the state directory — created plainly, repaired never |
| `src/process.rs`, `src/handles.rs` | `CreateProcessW`, its attribute and handle lists, and which handles cross |
| `src/job.rs` | the job object that ties the confined child's lifetime to the launcher's |
| `src/socket.rs`, `src/launch.rs` | the listener the parent binds and the child serves on, and the launcher's own payload and argv plumbing |
| `src/control.rs` | the signal relay |
| `src/sweep.rs` | reaping the AppContainer profiles an unclean exit left behind |
| `src/capability.rs` | whether **this host** can confine a process at all — the facade's `can_confine()` delegates here |
| `src/testing.rs` | test devices more than one crate's tests need, behind `testing` |
| `src/lib.rs` | `Backend`, `FACTS`, `DIAGNOSTICS`, the re-exports, and this crate's own FFI readers |

## What a confined process can and cannot reach

- Its token is the container's, and the whitelist names exactly the paths the run exposed: everything else on the volume is refused **for lack of a grant rather than absent from a view** — the path namespace stays fully visible, and access is what is denied.
- Its capability set governs outbound **network** reach, and it is a configured list rather than a derived one: with none granted, the container initiates nothing outbound. AppContainer's own loopback policy is a separate gate, and it refuses the crossing in both directions — a confined child connecting to a listener the *unconfined* launcher holds on `127.0.0.1` is dropped silently, with an empty capability set and with `internetClient` alike, which is the policy rather than the capability filters — so an empty set reaches nothing outward, not even the launcher's own loopback listener.
- The inherited listener is a convenience for the port and **not** a network-isolation mechanism: AppContainer network policy attaches to the process, so a socket the parent created carries no reach of its own.
- Its own loader reads what it imports under the container's token, so the program's own directory and the DLLs directly beside it are a separate rule of their own — `docs/sandbox.md` states that rule and its boundary.
- Windows has no `SIGTERM`, and a container signals its main process and nothing else: a stop reaches the confined child only because the launcher forwards it. What the child does with the stop afterward — the drain — is the CLI contract's.

## Residue a killed launcher can leave

- **Grants are standing**: nothing removes an ACE on the way out, on the clean path and the failure path alike, so the residue of a served root is one package-SID ACE per distinct root until an operator takes it back. The SID is *derived* from the root rather than generated, which is what makes the residue identifiable and reusable — the next run for that root takes its own over instead of adding another.
- **An exit that runs no destructor leaves the AppContainer profile's two halves behind** — its directory under the local app-data package tree and its `AppContainer\Storage` registry key. `src/sweep.rs` reaps those once per process on the launch path, before the run about to start derives its own name, and it can never fail a launch.
- Every place the residue can be found, and what the grant pass costs, are `docs/sandbox.md`'s and the design's Risks table's to state.

## Gates

```console
pwsh crates/tinio-sandbox-windows/scripts/verify-windows.ps1
```

The gate needs Windows with `icacls.exe` in the system directory and a console: the relay's cases fail rather than skip where a console control event cannot be delivered, because that event is the only mechanism this platform has for a stop. It runs the crate's suites over every volume the machine actually has — one it does not have is skipped rather than failed, and a selection that covers no volume at all is refused. `docs/tests.md`'s "Platform-gated verification" section owns the shared method, including why this leg is the operator's and not CI's.
