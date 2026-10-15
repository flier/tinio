# `tinio-sandbox-macos` — the macOS backend: a Seatbelt (SBPL) profile, applied by `sandbox-exec` or in-process

`tinio-sandbox-macos` implements `tinio-sandbox-core`'s `Backend` for macOS. Its confinement is one artifact — a Seatbelt profile rendered as a pure function of the exposure set — and two routes that apply it: `sandbox-exec`, which wraps the child's `exec` from outside and is the default, and `sandbox-init`, which has the child apply the same text to itself before it serves. Neither of the other platforms' shapes applies here — nothing confines a process in place (Linux's) and nothing constrains a process the backend did not itself create (Windows's) — so it always launches a supervised child, publishing `PlatformFacts::launches_child` beside the two state-directory facts. The crate is behind `#![cfg(target_os = "macos")]`, so it compiles to an empty library elsewhere. `tinio-sandbox` is the facade that selects it, and `docs/sandbox.md` owns the mechanism selector and the operator-facing consequences of each route.

## Layout

| File | What it is |
|---|---|
| `src/profile.rs` | the SBPL profile as a pure function of the exposure set and the mechanism that will apply it |
| `src/profile.hbs` | the rule templates the renderer fills |
| `src/mechanism.rs` | the mechanism, its options and the environment selector |
| `src/launch.rs` | the spawn, and the descriptor handover both routes share |
| `src/apply/mod.rs` | applying the profile to **this** process, through `sandbox_init_with_parameters` |
| `src/apply/libsystem.rs` | the `libSystem` declarations behind that call, and this crate's one `unsafe_code` site |
| `src/relay.rs` | forwarding termination signals to the confined child while the launcher waits |
| `src/lib.rs` | `Backend`, `FACTS`, `DIAGNOSTICS` and the re-exports |

## The profile and its two routes

- **The profile is text**, and it is the whole confinement: a `(version 1)` header, a `(deny default)` base, and one rule per exposure. Exposures reach the kernel as `-D NAME=VALUE` argv pairs that the text refers to by name (`(param "E0")`), so no path ever appears in the profile text — the substitution is data, never syntax.
- **`sandbox-exec` applies to a process it execs**, so the launcher hands the child's `exec` to Apple's helper. The helper needs one entry the other route does not (`process-exec`), which makes the two renderings differ by exactly that rule. Apple deprecates the binary and still ships it, and nothing here treats it as a supported or versioned API: when it disappears this route degrades to a refusal — no sandbox available, fail closed — never to an unconfined run.
- **`sandbox-init` applies to the process that asks**, so the launcher spawns the child directly and the child confines itself before it serves. Nothing on that route invokes the helper, its one dependency is `libSystem`, and it is the answer on a host where the helper is missing or unwelcome. Its confinement is one-shot and irreversible.
- Both routes share everything after the spawn — the payload write, the event relay, the readiness channel and the killer — so the two differ in their argv and their pre-flight check and in nothing else.
- The profile is rendered before anything is spawned and both routes prove it first, so a profile the chosen mechanism cannot apply is refused as a launch that never started rather than as a child that died.
- What an operator sees of the choice — the selector, the spellings it accepts, and what a typo does — is `docs/sandbox.md`'s to state.

## The launcher binds and spawns, and nothing more

macOS is the platform whose launcher does no confinement work of its own. It binds the listener the confined child will serve on, hands that descriptor (and the management socket's) over with the launch payload, spawns, and supervises — there is no ACL pass to pay and no in-place arm to take, because the entire confinement is the rendered profile. The two state-directory facts it publishes are its own: it grants the state directory without its parent, and it proves that directory can take a file before it launches. The stop relay is required on Windows, where a container signals its main process and nothing else, and defensive here — `launchd` signals the whole job — but it is built on both, so the launcher contract stays uniform and a missing relay cannot pass for a server that ignores a termination signal. The handover is a step of its own: it clears the close-on-exec bit on the listener and on the management socket's duplicate after the profile's pre-flight and before the real spawn, so both descriptors cross the `exec` while the launcher's own copy of the control socket does not. Everything else about a launch is `tinio-sandbox-core`'s: the payload type, the listener-descriptor argv element, the event stream and its relay, the supervision loop and exit-status forwarding. This crate supplies the profile, the spawn, the handover and the child-side apply, and nothing else.

## What the profile guarantees

The backend publishes `Enforcement::Full`: every exposure is granted at the access it was exposed with and everything else is refused, applied either by the helper around the child's `exec` or by the child itself. The one shape it does not span is macOS's own — a profile is textual, so a parameter travels as a rendered path rather than as a structural grant — and that is `docs/sandbox.md`'s to state.

## Gates

The crate's own verification is a real Mac — no other host can stand in — and the script below is that run:

```console
bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh
```

It is the only place this crate's own tests run. Off a Mac the crate is an empty library, so `cargo test -p tinio-sandbox-macos` there is a green run of nothing; a non-Mac leg certifies nothing about this crate. The script runs the crate's tests, the confined server end to end, and the profile's necessity table. `scripts/prune-profile.sh` is a separate one-off experiment that establishes which baseline allowances the profile actually needs, and its verdicts are a floor rather than the authority — the product path is. `docs/tests.md`'s "Platform-gated verification" section owns the shared method.
