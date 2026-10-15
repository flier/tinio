# `tinio-sandbox-core` — the confinement contract: the `Backend` trait, its vocabulary, and the launcher machinery

`tinio-sandbox-core` is the confinement contract. `Backend` is one call — `confine(&mut self, &Sandbox, &LaunchPlan) -> Result<Confinement>` — and the crate carries the vocabulary both sides of that call are written in: the exposure set (`Sandbox`, `Exposure`, and `AccessMode`, which is `tinio-core`'s write policy re-exported so that a value the read-only work already produced is the value an exposure takes), the launch (`LaunchPlan`, `LaunchPayload`), the child's half (`Adopted`), the report (`Event`, `Enforcement`, `Refused`, `Attribution`, `Diagnostics`), the facts a caller branches on (`PlatformFacts`) and the one `Error` every backend answers with. It also carries the launcher machinery the two child-launching platforms reuse, so neither re-implements the handoff.

**It confines nothing by itself and depends on no platform crate.** Every mechanism is `tinio-sandbox-linux`'s, `tinio-sandbox-macos`'s or `tinio-sandbox-windows`'s, and a consumer reaches this contract through the `tinio-sandbox` facade rather than by this crate's name — which platform serves a build, and the name a shared path calls it through, is the facade's rule and not stated again here. What an operator has to know about `--sandbox` is `docs/sandbox.md`'s. The design is `docs/superpowers/specs/2026-09-18-sandbox-design.md`.

## What a platform owes

- **The answer decides the caller's next move, not the platform's**: `Confinement::InPlace` (the calling process is the confined one) or `Confinement::Supervise(Child)` — which is what keeps `#[cfg]` out of `main()`.
- **Any failure is fatal.** The caller refuses to run rather than continue unconfined (design §11), and `Error` is written for that caller: every variant names the class of refusal, whether the step that failed, the operator's own input, a platform that cannot confine at all, a token that may not, or a path the mechanism cannot express.
- **`confine` takes `&mut self`**, because both answers are honest that way: Linux changes the calling process irreversibly, and a launcher records what it set up.
- **Availability is deliberately not a method here.** A caller needs that answer *before* an arm chooses a backend, and a target with no implementation has no value to ask it on — so it is published beside the facade's platform selection.
- **A backend states its own mechanism.** `PlatformFacts` (does it launch a child, does it refuse a derived state directory, does the launcher probe one for writability) and `Diagnostics`/`Enforcement` are constants each backend publishes beside its own implementation, so a caller branches on the fact rather than on the OS. A backend that has not answered reads `NotReported`, never "nothing is applied".

## What the request says

`Sandbox` is host paths and the access each gets, and nothing else: no port, no config text, no product vocabulary. Exposing one path twice keeps the **most permissive** access, so a narrower entry can never silently downgrade a wider one, and `from_exposures` applies that same rule to a set built elsewhere rather than trusting the caller to have kept it. The first exposure is the served root. A request that exposes nothing has no root to serve, and that refusal is the caller's mistake rather than the platform's — a `Step` naming the call, never an `Unsupported` that would send an operator to look at their kernel.

## The launch and its end

A launcher writes a payload on the child's standard input and closes it; the child decides from its own argv that it is the child, and recovers its launch from that payload (`adopt`, `filter_argv`). The descriptors a handoff uses travel as numbered arguments under two fixed prefixes. The configuration travels as text rather than as a parsed structure, so an unconfined launcher never holds credentials it understands.

What follows is shared too: the child reports on its stdout (`Event`), the launcher relays it (`relay`), waits for the child and undoes what the launch set up (`supervise`), and classifies the end (`Attribution`) — including the case where the confinement never applied and nothing ran confined. A backend that cannot ask a question of its own process, because the call would be irreversible or would confine the launcher itself, asks it of a throwaway forked child instead (`probe`, unix only).

## Layout

| File | What it is |
|---|---|
| `src/backend.rs` | the `Backend` trait, `Confinement`, `PlatformFacts` — the contract itself |
| `src/sandbox.rs` | the request: `Sandbox`, `Exposure`, and a set's one rendering |
| `src/error.rs` | the shared `Error`, and the usage sentence each platform selector owns |
| `src/enforcement.rs` | what a confinement applied and what it refused |
| `src/event.rs` | what the confined process reports on stdout |
| `src/launcher/plan.rs`, `src/launcher/payload.rs` | the launcher's own facts, and the payload the child learns its launch from |
| `src/launcher/adopt.rs`, `src/launcher/control.rs` | the child's half of the handoff, and the inherited control handle's two `cfg` forms |
| `src/launcher/supervise.rs`, `src/launcher/relay.rs` | waiting for the child and undoing the launch; relaying what it reports |
| `src/launcher/probe.rs` | the fork-probe verdict handshake (unix only) |
| `src/testing.rs` | this crate's test helpers — test-only, unpublished |

## Gates

```console
cargo test -p tinio-sandbox-core
```

Nothing here confines a process, so no leg needs a privilege or an external tool; the supervisor's legs spawn the host's own `sh` or `cmd`. The fork-probe handshake's names exist only where a launcher forks a throwaway child, so its legs run on a unix host alone, and a `--target <platform> --tests` check is what type-checks the rest. The published-surface pass over the facade is `tinio-sandbox`'s own gate.
