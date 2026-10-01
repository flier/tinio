# CLAUDE.md

Always-loaded pointers. Each named doc = source of truth for its branch — do not restate.

- **Language** — English only (docs, artifacts, comments, commits, PRs); non-English in docs → cross-check. `docs/compress.md`.
- **Git** — ask first per write; never auto; index untouched; no AI trailers. `docs/git.md`.
- **Docs prose** — intent; caveman; manual edits. `docs/compress.md`.
- **Cargo** — `docs/cargo.md`.
- **Style** — garde, imports, Error, lib.rs, newtypes, derives. `docs/style.md`.
- **Tests** — async harness, cucumber/e2e, scripts vs Rust tests, concurrency. `docs/tests.md`.
- **Specs/plans** — one definer; ownership check; progress ticks. `docs/superpowers/README.md`.
- **Manual edits** — user hand edits = law; conflict → stop, ask. `docs/compress.md`.

## Agent skills

- **Issue tracker** — work lives in-repo: `specs/<feature>/tasks.md` rows + `docs/superpowers/plans/` checkboxes. No external tracker. `docs/agents/issue-tracker.md`.
- **Triage labels** — five canonical roles, label string = role name. `docs/agents/triage-labels.md`.
- **Domain docs** — single-context: root `CONTEXT.md`; no `docs/adr/` yet. `docs/agents/domain.md`.
