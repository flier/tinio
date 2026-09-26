# Docs Prose Compression

Agent-facing `docs/*.md` (cargo, tests, style, git, this file). Specs/plans under `docs/superpowers/` and `specs/` follow their own ownership rules — compress their prose the same way when editing, but do not relocate facts.

## Language

- English only: docs, artifacts, comments, commits, PRs.
- Non-English in docs: cross-check before finish.

## Intent

- State **intent / rule / ownership** — not how the current code implements it.
- Drop site enumerations, call chains, measured error strings, and "why forbid cannot be allowed" mechanics unless the doc's job is to own that rationale (then one short clause).
- One fact, one home. Link or name the definer; do not retell.

## Caveman

- No articles (`a`/`an`/`the`), filler, hedging, connective fluff.
- Fragments OK. Short synonyms. Drop "you should" / "make sure to" — state the action.
- Merge bullets that say the same thing. One example per pattern.
- Cut until further cut changes behavior.

## Preserve EXACTLY

- Fenced / indented code blocks (comments, spacing, command lines untouched)
- Inline `` `code` ``, paths, commands, env vars, URLs, dates, versions, IDs (`T055`, `FR-016`)
- Heading text; list nesting; table structure (compress cell prose only)

## Shape

- English bullets. No hard line wraps in paragraphs / list items / blockquotes — structural breaks only (blank lines, table rows, fences, headings).
- Pattern: `condition (ex): action — scope`.
- Positive rule > ban list when both work.

## Manual edits

- User hand edits = law — never overwrite/merge-over.
- Mid-task change/conflict/unclear intent: stop, re-read, ask. Concurrent writers = user.

## When editing

- New or rewritten `docs/*.md` prose must land already compressed.
- Expanding a rule: add the intent clause; do not re-inflate with implementation walkthroughs.
- Compress over hand edits: re-read first (Manual edits).
