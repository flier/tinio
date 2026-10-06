# Cargo Workspace

## Rules

- Manifests: no comments (`docs/` owns rationale). Pin once at workspace; members path-only, inherit version/edition (`major.minor` only); publish only the facade; optional/target deps at the crate.
- Groups: workspace pins, deps, dev-deps, target — external then in-tree; alpha within group; no extra blank when a group is one.
- Patched dep: vendor it, keep the upstream version, header names the change; root names patch + exclude (sandbox design).
- `unsafe_code` `forbid` by default; `deny` + enumerated allow only where raw platform API forces it. Sandbox backends go `deny` as each lands (design decision 14); stub stays `forbid`. Clippy `absolute_paths` warn (`docs/style.md`).
- Workspace is the lookup. In-memory default does not select that backend (cli contract). Conformance: `docs/tests.md`.

## Features

- Workspace pins do not enable features (`default-features = false` there only to stop inherit / re-enable). Enable at the crate that uses them.
- Facade's feature list, never a dependency pin, decides the shipped binary — a dep pin that forces a feature on would survive `--no-default-features`.
- Transitive set spelled once at the crate that ships the operations; consumers forward the name, never the set (spec 2026-10-03). Carrier is optional normal deps so each-feature pruning stays valid. Seam is a runtime wiring test, not a compile cfg.
