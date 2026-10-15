# `tinio-sandbox` — the facade over the per-platform confinement backends

`tinio-sandbox` is the one name a consumer writes to confine a process. It re-exports the whole of `tinio-sandbox-core`'s public surface, name for name, and adds the platform selection: `platform()`, `platform_facts()`, `diagnostics()`, `backend_available()`, `can_confine()`, the handoff names (`adopted_listener`, `adopted_control`, `on_stop`, `apply_self_confinement`, `disarm_signal_relay`, `control_handles`), and the business-agnostic confine entry (`serve::Spec` → `serve::Confined`).

It implements no mechanism of its own. What a confinement must promise is the contract's and belongs to `tinio-sandbox-core`; the mechanisms belong to the backend crates; what an operator has to know about `--sandbox` belongs to `docs/sandbox.md`. This crate decides which of them a build reaches, and maps their shapes into one.

## Which backend a target gets

| Target | The crate that serves it |
|---|---|
| Linux | `tinio-sandbox-linux`, confining in place |
| macOS | `tinio-sandbox-macos`, launching a confined child |
| Windows | `tinio-sandbox-windows`, launching a confined child |
| Any other | none — `backend_available()` answers `false`, and a confinement request is refused rather than served |

`backend_available()` names the same targets the selection's arms are written for, so a new backend's target is added to both in one edit; `can_confine()` is the run-time half of that question, asked of this host's token. `tinio-winsec` is not a backend: it is the Windows DACL helper the Windows backend reads descriptors through.

## The rule

**A platform-specific need shared code must call is a facade name with a `#[cfg]`-selected body, never a module re-export.** These bodies are where the platforms' disagreements stop: each platform answers its own error type, or its own success shape, and the facade turns it into the single `Result` a caller's `?` converts the same way on every target. Each backend's own module (`linux`, `macos`, `windows`) is published only on its own target; the names a shared path calls exist on every target and refuse where the mechanism is absent.

## Layout

| File | What it is |
|---|---|
| `src/lib.rs` | the facade: the re-export list and the `#[cfg]`-selected bodies |
| `src/serve.rs` | `serve::Spec` → `serve::Confined`: the confine entry with no product types in it |
| `tests/facade.rs` | the published surface, bound as a consumer writes it |

## Gates

```console
cargo test -p tinio-sandbox
```

The facade's own legs resolve on every target, and the per-target selection legs are `#[cfg]`-gated — a `--target <platform> --tests` check is what type-checks the arms this host does not run. No leg applies a confinement to the test process: the in-process apply is one-shot and irreversible.
