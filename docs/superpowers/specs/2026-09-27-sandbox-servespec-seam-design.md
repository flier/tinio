# Design: serve::Spec seam — business-agnostic confine entry on the facade

**Date**: 2026-09-27
**Status**: approved in chat — implements the consumer seam for landed backends; does not change confinement physics
**Scope**: `tinio-sandbox` facade surface (`serve::Spec`); `tinio-cli` sandbox module (enablement, product refusals, API bind/token/`state`); no `tinio-api` dependency on any `tinio-sandbox*` crate
**Owns**: where confine orchestration ends and CLI product policy begins. Confinement guarantees, platform mechanisms, and launcher physics stay in `2026-09-18-sandbox-design.md` and the platform crates. Three-channel enablement ownership stays in `2026-09-20-cli-integration-design.md` §7 / decision 10.

## Goal

CLI names sandbox **implementation** no more: no platform `Backend`/`Options`, no `Confinement` branch, no env→mechanism/capabilities assembly at the call site. Confine runs through one deep builder on the facade. Callers that are not tinio still use the same builder — inputs are paths, access, config text, bound sockets/handles, token string, and Linux knobs only (the confinement model among them, carried as `Spec::model`, with the facade's `requested_model()` as its one environment read).

## Seam

| Layer | Owns |
|---|---|
| CLI `plan` | `--sandbox` / `TINIO_SANDBOX` / `[server] sandbox` → `Request.enabled` |
| CLI product refusals | in-root / underived / unwritable state-dir; exposure set from resolution |
| CLI + `tinio-api` | bind listener/control; mint token; `state` publish / retract |
| `tinio-sandbox::serve::Spec` | assemble `Sandbox` + `LaunchPlan`; platform Options from env (the Linux arm from a carried `model` when one arrives); `Backend::confine`; map `Confinement` → `Option<Child>` |

**Caller binds first.** Builder takes already-bound `TcpListener` and `ControlHandle`(s) — never `Plane` / `LocalForm`.

**Sandbox does not parse** clap, config keys, or the three enablement channels.

## Public surface

`serve::Spec::new()` (or `from_sandbox`) then builder methods: `expose`, `config_text`, `listener`, `controls` / `control`, `with_token`, `local_uid`, `model` (**Linux-only**, `#[cfg(target_os = "linux")]`, with the `model_configured()` reader beside it), `apply() -> Result<Option<Child>>`. The facade re-exports `requested_model()` and `Mechanism` under the same Linux gate (2026-10-01): the function is the one read of `TINIO_SANDBOX_MODEL` — the value a caller carries back on `Spec::model` when it must state the request after the confinement, as the CLI's in-place re-emit does — and the enum is that vocabulary under a name a caller writes without a platform module path.

**`device_files` is not on this surface** (removed 2026-09-27, after review): the four device nodes are a Linux knob beside `local_uid`'s siblings, so the Linux backend asks for them itself (`tinio_sandbox_linux::Options::with_device_files`) rather than have every caller — of which the only production one always set it — spell a choice it cannot make. The inputs above stay the ones a caller can decide.

`apply` maps: Linux `InPlace` → `Ok(None)`; launcher `Supervise` → `Ok(Some(child))`; launcher `InPlace` → refuse (fail-closed). Child-side names (`adopted_listener`, `adopted_control`, `on_stop`) exist on every target; an in-place target refuses.

## Platform facts

`tinio-sandbox::platform_facts` is the only OS split a caller writes **beside the model knob's own gate** (a caller that carries a model writes one `#[cfg(target_os = "linux")]` at its call site — §Public surface). `launches_child` selects in-place apply versus bind-then-supervise. `refuses_derived_state_dir` and `probe_state_dir_writable` are macOS. **`control_socket` is not a fact** (removed 2026-09-30, `2026-09-29-sandbox-bdd-e2e.md` §8 H1): it had no reader anywhere, so the field set is the three above. **Ending the child a failed `state` publish would leave serving is not a fact** (revised 2026-09-28): every launcher platform installs a killer on the child it started, so `tinio_sandbox_core::Child::abort` owns that ending and the call site calls it unconditionally rather than consulting the fact table — the `retract_on_publish_failure` field and the CLI's `retract` helper are gone with it.

**The values are each backend's own, not the facade's** (revised 2026-09-27, after review): `PlatformFacts` is core vocabulary and every backend publishes a `FACTS` constant beside its mechanism — `tinio_sandbox_macos::FACTS`, `tinio_sandbox_windows::FACTS`, `tinio_sandbox_linux::FACTS`. `platform_facts()` selects the target's constant and that selection is now the facade's only `cfg`; a field can no longer say something about a mechanism in a crate that does not own it. Child-side `adopted_listener`, `adopted_control`, and `on_stop` exist on every target; an in-place target returns the refusal. A bad `TINIO_SANDBOX_MACOS_MECHANISM` is `Error::Usage { detail }` (2026-10-01: the `is_usage` classifier is gone and the variant itself is the class; the operator-visible sentence and exit 2 are unchanged). Control bind is `tinio_api::transport::bind_local`; descriptor copy is `tinio_sandbox::control_handles`. `Plane::open` is the adopt-or-bind split (`start::run` passes inherited controls and, on macOS where the launch carried one, the socket `adopted_control` rebuilt). `tinio-api` still does not depend on any `tinio-sandbox*` crate.

## Non-goals

- Moving `state` publish into sandbox
- Sandbox binding sockets itself
- Changing three-channel semantics or `docs/sandbox.md` operator facts
- New confinement mechanisms
