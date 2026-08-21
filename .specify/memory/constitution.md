<!-- Sync Impact Report
  Version change: 1.0.1 -> 1.0.2
  Modified principles: none (wording-only amendment)
  Added sections: none
  Removed sections: none
  Follow-up TODOs: none
-->

# Constitution

## Core Principles

### I. Tiny Core (NON-NEGOTIABLE)

tinio exists to stay tiny. The public API surface MUST remain minimal and focused on I/O; every addition must justify its existence against the lifetime maintenance cost. Dependencies MUST be kept to the minimum required for the feature; any new dependency requires explicit justification in the feature spec. YAGNI applies: no speculative features, no organizational-only helpers.

**Rationale**: The project name is the contract. A small, focused core is easier to review, audit, and keep correct.

### II. Safety & Correctness

Library code MUST NOT panic on any input; all fallible operations MUST return `Result`/`Option` with meaningful error types. `unsafe` code is permitted only when (1) no safe alternative exists, (2) it is contained in the smallest possible scope, (3) it documents its safety invariants, and (4) the review explicitly justifies it. `unwrap()`, `expect()`, and `panic!()` MUST NOT appear in library code paths. Undefined behavior is never acceptable.

**Rationale**: Rust's memory-safety guarantees are this project's core value proposition; `unsafe` erodes them and must remain exceptional and audited.

### III. Idiomatic Rust APIs

Public APIs MUST follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/): consistent naming, `From`/`TryFrom` conversions, `AsRef`/`Borrow` where appropriate, and ownership/borrowing patterns natural to callers. Every public item MUST have doc comments including at least one runnable `rustdoc` example. Features MUST be `no_std`-compatible where they do not inherently require allocation or std I/O.

**Rationale**: Idiomatic Rust APIs are discoverable and composable; doc examples double as compile-time tests of the public contract.

### IV. Test-First (NON-NEGOTIABLE)

Tests are written before implementation: unit tests for behavior, doc tests for public API examples, and property-based tests (e.g., `proptest`) for I/O edge cases such as partial reads/writes, zero-length buffers, and end-of-stream conditions. A feature is not complete until its tests pass and cover error paths, not just the happy path. The Red-Green-Refactor cycle is strictly enforced.

**Rationale**: I/O code fails in edge cases, not happy paths; property tests systematically cover inputs that hand-written tests miss.

### V. Predictable Performance

Hot paths MUST avoid hidden allocations and unbounded buffering; O(n) behavior where O(1) is achievable counts as a regression. Performance-sensitive changes MUST include benchmarks (e.g., `criterion`) and MUST NOT regress existing benchmarks without discussion and a documented decision.

**Rationale**: A tiny I/O library is chosen for embedded and low-latency contexts; predictable cost is a feature.

### VI. Semver & MSRV Discipline

Releases follow Semantic Versioning strictly: breaking changes only in MAJOR, API additions only in MINOR, bug fixes only in PATCH. The Minimum Supported Rust Version (MSRV) MUST be documented, tested in CI, and only raised in a MINOR or MAJOR release with a changelog entry. API changes MUST be checked with `cargo-semver-checks` in CI.

**Rationale**: Users pin libraries like tinio in constrained environments; breaking their builds without notice is a trust violation.

## Rust Tooling & Quality Gates

The following MUST be enforced in CI and locally:

- `cargo fmt --check` — formatting is non-negotiable.
- `cargo clippy --all-targets -- -D warnings` — clippy warnings are errors.
- `cargo test` (unit, doc, integration) — the full suite MUST stay green, including `--no-default-features` where features exist.
- `cargo doc` MUST build without warnings; broken doc links fail CI.
- MSRV and current stable toolchain MUST both pass the test matrix.
- `cargo-semver-checks` runs on any PR touching the public API.
- `cargo audit` runs on the release branch; the maintainer reviews its report before every release — a vulnerable dependency blocks the release unless the risk is documented and accepted.

## Development Workflow

- Features follow the Spec Kit workflow: `/speckit-specify` -> `/speckit-plan` -> `/speckit-tasks` -> `/speckit-implement`, with specs and plans reviewed before implementation starts.
- Every change lands via a pull request; at least one reviewer other than the author MUST approve.
- Reviews MUST verify: constitution compliance, safety invariants of any `unsafe`, MSRV impact, dependency additions, and benchmark regressions.
- Public API changes MUST be recorded in `CHANGELOG.md` (or generated via `git-cliff`) and reflected in the crate docs.
- Complexity MUST be justified in the spec; a simple implementation wins over a clever one unless the spec demonstrates the need.

## Governance

This constitution supersedes all other development practices; where project docs conflict with it, the constitution wins.

Amendments:

1. MUST be proposed as a PR that edits this file.
2. MUST document the rationale and migration impact in the PR description.
3. MUST be approved by at least one maintainer other than the proposer.
4. MUST bump the version per the policy below and update `Last Amended`.

Versioning policy (for this constitution):

- MAJOR: removal or redefinition of a principle or governance rule.
- MINOR: new principle or materially expanded guidance.
- PATCH: clarifications, wording, typo fixes.

Compliance review: every PR and release review MUST verify compliance with this constitution; non-compliant changes are rejected even if tests pass. Runtime development guidance lives in `CLAUDE.md` and may change freely, but MUST NOT contradict this constitution.

**Version**: 1.0.2 | **Ratified**: 2026-08-21 | **Last Amended**: 2026-08-21
