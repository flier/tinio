# CLAUDE.md

## Language

- English only: docs, artifacts, comments, commits, PRs.
- Cross-check for non-English before finishing docs.

## Git

- Never auto-commit/push/merge/rebase/stash — git writes: ask first, per operation.
- Leave changes in tree; report pending; user decides when to commit.

## Docs

- Cargo conventions: `docs/cargo.md`.
- Code style (garde, imports, Error, lib.rs, newtypes): `docs/style.md`.

## Manual Edits

- User hand edits are law. Never overwrite them.
- File changed under me mid-task? Stop editing that file. Re-read. Re-review against the new code.
- Conflict with a manual edit? Back off. Do not merge-over. Re-think. Ask.
- Not sure what the user wants? Ask. Never guess-and-edit around user's code.
- Assume any "concurrent writer" is the user's own hand. Respect it, never fight it.
