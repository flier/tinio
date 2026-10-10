# Issue tracker

Work tracked in-repo; `github.com/flier/tinio` carries no issues or PRs in use, and `gh` has nothing to read.

## Where work lives

- Feature work: `specs/<feature>/tasks.md` — task rows, `- [ ]`/`- [X]`, dated tick notes each. Row = ticket.
- Phase plan: `docs/superpowers/plans/<date>-<name>.md` — a checkbox per step, ticked as it happens.
- Design: `docs/superpowers/specs/<date>-<name>-design.md` + feature's `specs/<feature>/` contract files.
- Reviews: `docs/superpowers/reviews/`.

## Rules for ticket-shaped work

- One definer per fact, record rule, plan progress: `docs/superpowers/README.md`.
- English only: `docs/compress.md`. Git: ask before every write, never auto-commit: `docs/git.md`.
- No `issue create`: a new work item = a task row in the owning `specs/<feature>/tasks.md` with spec ID.
- A plan under `docs/superpowers/plans/` when design is needed; triage annotates the row, no tracker object.
