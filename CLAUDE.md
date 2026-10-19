# CLAUDE.md

Always-loaded pointers. Each named doc = source of truth for its branch — do not restate.

- **Language** — English only. `docs/compress.md`.
- **Git** — ask first per write; index untouched; no AI trailers. `docs/git.md`.
- **Docs prose** — intent; caveman; manual edits. `docs/compress.md`.
- **Cargo** — `docs/cargo.md`.
- **Style** — garde, imports, Error, lib.rs, newtypes, derives. `docs/style.md`.
- **Tests** — async harness, cucumber/e2e, scripts vs Rust tests, concurrency, verification scratch, temp cleanup before / after a round. `docs/tests.md`.
- **Benches** — flush regime, interleaved A/B verdicts, unmeasurable on this host. `docs/benches.md`.
- **Specs/plans** — one definer; ownership check; progress ticks. `docs/superpowers/README.md`.
- **Manual edits** — user hand edits = law; conflict → stop, ask. `docs/compress.md`.
- **Chat language** — dialogue Chinese; artifacts English. `docs/compress.md`.
- **Plan progress** — tick in the same round; subagents never edit plans. `docs/superpowers/README.md`.

## Agent skills

- **Issue tracker** — in-repo: task rows + plan checkboxes. `docs/agents/issue-tracker.md`.
- **Triage labels** — five canonical roles, label = role name. `docs/agents/triage-labels.md`.
- **Domain docs** — single-context, root `CONTEXT.md`. `docs/agents/domain.md`.
- **Execution** — every search names a repo path (no `/`, `~`, drive root); unknown API → compile a `target/` probe. `docs/agents/execution.md`.
