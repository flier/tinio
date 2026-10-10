# Domain docs

How the engineering skills consume this repo's domain documentation.

## Before exploring, read

- `CONTEXT.md` at repo root — workspace glossary, English only.
- Feature vocabulary: `specs/<feature>/data-model.md` + `contracts/` own the terms a feature adds.
- `docs/adr/` — absent today; read when it exists.

Single-context: one `CONTEXT.md`, no `CONTEXT-MAP.md`; absent → proceed silently, no flag (`/domain-modeling` creates lazily).

## Vocabulary and conflicts

- Name concepts as `CONTEXT.md` defines them — no synonym drift.
- A term the glossary lacks = invented language, or a real gap worth noting.
- Decisions live in the feature's `docs/superpowers/specs/*-design.md` + `specs/<feature>/` contract, not an ADR.
- Output contradicting a decision says so; never overrides silently.
