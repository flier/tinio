# Docs Prose Compression

Agent-facing `docs/*.md` (cargo, tests, style, git, this file). Specs/plans under `docs/superpowers/` and `specs/`: compress the same way when editing, never relocate facts.

## Language

- English only: docs, code, artifacts, comments, commits, PRs.
- Chat with the user: Chinese.
- Non-English in docs: cross-check before finish.

## Intent

- State **intent / scope / rule / ownership** — never how the code implements it.
- Drop site lists, call chains, measured strings and "why forbidden" mechanics — unless the doc owns that rationale (one short clause).
- One fact, one home. Link or name the definer; do not retell.
- No tool invocations, absolute paths or code-level names in lists and prose — name the capability.
- No dates or provenance stamps — rules state what holds now.
- No volatile content — test counts, timings, measured numbers; state the rule, not the reading.

## Caveman

- No articles (`a`/`an`/`the`), filler, hedging, connective fluff.
- Fragments OK. Short synonyms. Drop "you should" / "make sure to" — state the action.
- Merge bullets that say the same thing. One example per pattern.
- Cut until further cut changes behavior.

## Preserve EXACTLY

- Fenced / indented code blocks (comments, spacing, command lines untouched) — commands live only here
- Inline `` `code` ``, relative paths, env vars, URLs, versions, IDs (`T055`, `FR-016`)
- Heading text; list nesting; table structure (compress cell prose only)

## Shape

- English bullets. No hard line wraps in paragraphs / list items / blockquotes — structural breaks only.
- List items ≤140 chars; merge near-duplicates within a section.
- Sections: ≤7 list items — split long ones, merge short ones.
- Pattern: `condition (ex): action — scope`.
- Positive rule > ban list when both work.

## Manual edits

- User hand edits = law — never overwrite/merge-over.
- Mid-task change/conflict/unclear intent: stop, re-read, ask. Concurrent writers = user.

## When editing

- New or rewritten `docs/*.md` prose must land already compressed.
- Expanding a rule: add the intent clause; do not re-inflate with implementation walkthroughs.
- Compress over hand edits: re-read first (Manual edits).
- Passes in order: drop mechanism, then ≤140-char items, then ≤7-item sections.

## Check

- Every list item ≤140 chars; every section ≤7 items.
- What survives: env var names, flags, IDs, relative paths, stable error codes.
- What may go: mechanism detail, measurements, test counts, timings, dates.
- Lists and prose carry no command, absolute path, code-level name or date.
- Fences, tables, headings, list nesting untouched; line endings unchanged.
- Report bytes and longest line per file, before and after.
