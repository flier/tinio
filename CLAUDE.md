# CLAUDE.md

## Language

- English only: docs, artifacts, comments, commits, PRs.
- Cross-check non-English before finishing docs.

## Git

- Never auto-commit/push/merge/rebase/stash — git writes: ask first, per operation.
- Never touch the index: no `git add` (including `-N`/`-A`), no `git reset`, no `git restore --staged`, no staging of any kind — and no tooling that does it on your behalf. Changes stay unstaged in the working tree.
- Leave changes in tree; report pending; user decides when to commit.
- No agent/AI trailers in commit messages (no `Co-Authored-By:`, "Generated with ...", or similar rows).

## Docs

- Prose: `docs/compress.md` — intent only; caveman; code/commands/paths exact. New or rewritten `docs/*.md` must land compressed; do not re-inflate with implementation walkthroughs.
- Cargo conventions: `docs/cargo.md`.
- Code style: `docs/style.md` — garde, imports (3+ segments, enum variants, `tokio::fs`), Error, lib.rs, newtypes, derives (`parse-display`, `derive_more`).
- Tests: `docs/tests.md` — async harness, cucumber/e2e cargo+run, Gherkin, migration vs unit/conformance.
- Specs & plans: `docs/superpowers/README.md` — one definer per cross-document fact, and the ownership check that runs before a phase closes.
- Plans track their own progress: once a task is finished and its review passes, tick that task's `- [ ]` step boxes in its plan file. A step deliberately not run keeps its box unticked and carries a one-line note saying why — a ticked box means the step happened.

## Manual Edits

- User hand edits are law — never overwrite or merge-over. Mid-task file change / conflict / unclear intent: stop, re-read, ask. Treat concurrent writers as the user.
