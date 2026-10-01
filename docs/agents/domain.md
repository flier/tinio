# Domain docs

How the engineering skills consume this repo's domain documentation.

## Before exploring, read

- `CONTEXT.md` at repo root — workspace glossary, English only.
- Feature vocabulary: `specs/<feature>/data-model.md` + `contracts/` own the terms a feature adds.
- `docs/adr/` — absent today; read when it exists.

Single-context repo: one `CONTEXT.md`, no `CONTEXT-MAP.md`. File absent → proceed silently, no flag, no create proposal; `/domain-modeling` creates lazily.

## Use the glossary's vocabulary

Name concept as `CONTEXT.md` defines it — issue, refactor proposal, test name. No synonym drift. Term the glossary lacks → inventing language, or real gap worth noting.

## Flag conflicts

Decisions live in feature's `docs/superpowers/specs/*-design.md` (numbered decisions) + `specs/<feature>/` contract, not ADR file. Output contradicting one says so; never overrides silently.
