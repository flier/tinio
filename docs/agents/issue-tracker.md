# Issue tracker

Work tracked in-repo. `github.com/flier/tinio` carries no issues and no PRs in use (measured 2026-10-01); `gh` installed and authenticated, nothing to read.

## Where work lives

- Feature work: `specs/<feature>/tasks.md` — task rows, `- [ ]`/`- [X]`, dated tick notes each. Row = ticket.
- Phase plan: `docs/superpowers/plans/<date>-<name>.md` — one checkbox per step, ticked as it happens (`docs/superpowers/README.md` §Plan progress).
- Design: `docs/superpowers/specs/<date>-<name>-design.md` + feature's `specs/<feature>/` contract and checklist files.
- Reviews: `docs/superpowers/reviews/`.

## Rules binding a ticket-shaped artifact

- One definer per fact, record rule, plan progress: `docs/superpowers/README.md`.
- English only: `docs/compress.md`.
- Git: ask before every write, never auto-commit, index untouched: `docs/git.md`.

## For the skills

No `issue create`. New work item = task row appended to owning `specs/<feature>/tasks.md` with spec ID, plus plan under `docs/superpowers/plans/` when design needed. Triage role annotates that row; no tracker object to label.
