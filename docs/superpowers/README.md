# Specs and Plans

How the documents under `specs/`, `plans/` and `reviews/` relate to each other, and the check that runs before a phase is declared complete.

A feature large enough to need a design document usually needs more than one plan, because a plan covers one subsystem and produces working software on its own. That shape — **one spec, several plans** — has a recurring failure mode, and it is not disagreement. It is **agreement in more than one place**: two documents stating the same fact, drifting apart the moment one of them is edited.

## Plan progress

- Task done + review pass → tick that task's `- [ ]` boxes.
- Skip deliberately → leave unticked + one-line why.
- Ticked = step happened.

## One definer per fact

Every fact that more than one document could state has exactly one owner. Every other document **refers to** it — by section, by decision number, by path — and never restates it.

The facts that have bitten are always in one of these families:

- **A shared symbol's name and signature.** A type, function or constant that two plans both consume: who defines it, what it is called, what it takes.
- **A crate's or module's public surface.** Especially a facade, which is the one place where "what is shared" is written down. A platform-specific addition to a shared surface is a change to that contract, not a local convenience.
- **Which file holds which list.** An enumeration has one home. Splitting it — the rule in one file, the items in another — is fine only when the split is stated, and it rots the moment a second copy appears.
- **Which task owns the edit of a shared file.** One task edits it; the others verify. A second plan writing the same line is a defect even when the text agrees, because the next edit will make them disagree.
- **Type names that cross a document boundary.** Two plans naming the same type differently is how a third name gets invented.
- **crate names that appear in example code.** An example that compiles on every platform is the strictest consumer of the public surface, so what it may name is a contract.

## The record rule

A plan states what a phase intended and did, and its **records** — fenced code and test snippets, drafted commit messages, quoted logs, measurement tables — belong to that phase rather than to the tree. When the landed code or a later ruling supersedes a record, the record **stays exactly as written** and the correction lives in **live prose beside it**, naming the landed contract and the decision that owns it. Editing a record to match the tree would rewrite history and destroy the diff a reader uses to see what changed; leaving it uncorrected would let a superseded contract read as current.

Where the correction goes depends on what the record is. For a fence, it goes **immediately before** it, so a reader meets the correction first; where a task's own prose above the fence already states the landed contract, that prose is the correction and a second one beside the fence would be a second statement of one fact.

Live prose is not a record. Headings, task bodies, checklists, `Expected:` lines and interface lists are claims about today and are corrected in place; the record rule protects the record, not the plan's account of the tree.

## The check, before a phase closes

Run it per phase, not per task. For each family above, name the single owner, then search the **other** documents for a second statement of the same fact.

A hit is a defect. Fix it before declaring the phase complete — not in a follow-up, because a phase that closed with a duplicated fact is a phase whose successors will edit one copy.

The check is deliberately about **ownership**, not about whether the documents currently agree. Two documents agreeing today is the state that precedes them disagreeing tomorrow; the defect is the second copy, not the divergence it will cause.

## Decisions taken on incomplete facts

When a decision was put to the operator after checking only one document, say so at the time, and re-check it when the missing fact arrives. This is the same failure as a duplicated fact, one step earlier: a decision whose premise was never established.

The tell is a question shaped "where should X go?" asked after searching one plan rather than all of them. Before asking, search every document the answer could live in.

## The worked case

`specs/2026-09-18-sandbox-design.md` and its four plans (`-p0-core`, `-p1-linux`, `-p2-windows`, `-p3-macos`) are what this document was written from. One review pass over them found **seven** collisions of the families above: a lint rule three plans each rewrote; a site list with two homes; a shared-surface boundary only one plan's mechanism satisfied; a module export against a single name; one type with three names; a child-side entry gated by the feature alone in one plan and by feature-plus-target in the other, which does not compile on the third platform; and one listener rebuild written as a method call in one plan and a facade name in the other. Most were found only because the check was run deliberately — which is the argument for running it rather than waiting to notice.

The mitigations that came out of that pass are worth copying: a shared addition is named once and pointed at from everywhere else; a platform-specific need that shared code must call is a facade name with a `#[cfg]`-selected body, never a module re-export; and a plan that no longer owns an edit says so and tells the reader to verify instead.

**A second pass over the same set found the same defect one level up, and gave it a fifth document.** The four plans each carried their own copy of the conventions they run under, of the crate topology, and of the list of what a launcher platform owns — the last of them carried twice, by P2's own note, "in the same words" — and `probe_read_only` was assigned to whichever launcher plan landed first, so one file was the subject of two tasks' edits. `plans/2026-09-18-sandbox-shared.md` now owns those facts and the four plans refer to it by section. Its §1 is nothing but pointers: each convention names the document that already owns it (`docs/compress.md`, `docs/git.md`, the constitution, `docs/cargo.md`, `docs/style.md`, `docs/tests.md`; `CLAUDE.md` is the always-loaded pointer into those), because a consolidated copy inside the plan set would have been a fifth copy rather than one fewer. Its §2–§6 are stated once because no phase owns them — the two crate shapes, the launcher-platform list, the names two documents must spell alike, the three relays, and the two verification conventions. Five documents for one feature, then, and the fifth exists because the alternative was four copies of a fact with no phase to belong to.
