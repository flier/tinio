# Semver gate record

The `semver` job in `.github/workflows/ci.yml` owns the gate: it checks the `tinio` facade against the last published release. No lint is downgraded; no step may fail open.

## Why a page, not a file

- `cargo-semver-checks` 0.51 configures lints only, per package or workspace-wide; it has no per-item allowlist file.
- Its config key is `[package.metadata.cargo-semver-checks.lints]`.
- Downgrading `enum_variant_added` there would hide every added variant, so the gate keeps its default `deny`.
- This page is the record; each exempted item carries its reason.

## Exempted items

- `storage::Error::NoTiers` — additive variant; a transition on a backend without tiers refuses with it.
- `storage::Error::InvalidRequest` — additive variant; a state the backend cannot reach (a tier class it refuses) reports it.
- Both are `enum_variant_added` (major) on an exhaustive, facade-re-exported enum.
- The release carrying them takes a major-equivalent bump (0.x: the next minor).
