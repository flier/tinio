# CLAUDE.md

## Language

- English only: docs, artifacts, comments, commits, PRs.
- Cross-check non-English before finishing docs.

## Git

- Never auto-commit/push/merge/rebase/stash — git writes: ask first, per operation.
- Leave changes in tree; report pending; user decides when to commit.

## Docs

- Cargo conventions: `docs/cargo.md`.
- Code style: `docs/style.md` — garde, imports (3+ segments, enum variants, `tokio::fs`), Error, lib.rs, newtypes, compressed prose.

## Tests

- Async: `#[tokio::test]` / `async fn` directly — no `Runtime::block_on` / `rt(...)` wrappers. Sync: `#[test]`. Exception: deliberate runtime shape under test.

## Manual Edits

- User hand edits are law — never overwrite or merge-over. Mid-task file change / conflict / unclear intent: stop, re-read, ask. Treat concurrent writers as the user.
