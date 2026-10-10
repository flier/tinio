# Specs and Plans

How the documents under `specs/`, `plans/` and `reviews/` relate to each other, and the check that runs before a phase is declared complete.

- **One spec, several plans**: a plan covers one subsystem and ships working software on its own.
- The failure mode is not disagreement but **agreement in more than one place** — two documents stating one fact, drifting on the next edit.

## Plan progress

- Task done + review pass → tick that task's `- [ ]` boxes in the same round.
- Skip deliberately → leave unticked + one-line why.
- Ticked = step happened.
- Subagent-run execution: the dispatcher ticks the boxes and writes the Evidence line once the review passes.
- A task's subagents never edit the plan — parallel tasks share the file and would collide.

## One definer per fact

Every fact more than one document could state has exactly one owner; every other document **refers to** it — by section, decision number, path — and never restates it. The families that have bitten:

- **A shared symbol's name and signature** — who defines it, what it is called, what it takes.
- **A crate's or module's public surface** — especially a facade: a platform-specific addition is a contract change, not a convenience.
- **Which file holds which list** — an enumeration has one home; a stated split rots once a second copy appears.
- **Which task owns the edit of a shared file** — one edits, the others verify.
- **Type names crossing a document boundary** — two spellings invent a third.
- **Crate names in example code** — an example is the strictest consumer of the public surface.

## The record rule

- **Records** — fenced code and test snippets, drafted commit messages, quoted logs, measurement tables — belong to the phase, not the tree.
- Superseded record: it **stays as written**; correction in **live prose beside it**, naming the landed contract + owning decision.
- For a fence the correction goes **immediately before** it; prose above that already states the landed contract is the correction.
- Live prose is not a record: headings, task bodies, checklists, `Expected:` lines and interface lists are corrected in place.

## The check, before a phase closes

- Run per phase, not per task: name each family's single owner, then search the **other** documents for a second statement.
- A hit is a defect — fix it before declaring the phase complete, not in a follow-up.
- The check is about **ownership**, not agreement: agreeing today precedes disagreeing tomorrow.

## Decisions taken on incomplete facts

- A decision put after checking one document: say so at the time, re-check it when the missing fact arrives.
- The tell is "where should X go?" asked after searching one plan — search every document the answer could live in.

## The worked case

- `specs/2026-09-18-sandbox-design.md` + the four launcher plans: every family collided — found only by running the check deliberately.
- Mitigations worth copying: a shared addition is named once and pointed at from everywhere else.
- A platform-specific need shared code must call is a facade name with a `#[cfg]`-selected body, never a module re-export.
- A plan that no longer owns an edit says so and tells the reader to verify instead.
- A second pass found the same defect one level up: `plans/2026-09-18-sandbox-shared.md` now owns the plans' shared conventions.
- Its §1 is nothing but pointers; §2–§6 are stated once — no phase owns them.
