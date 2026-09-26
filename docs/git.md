# Git

- Writes (`commit`/`push`/`merge`/`rebase`/`stash`): ask first, per op. Never auto.
- Index: never touch — no `git add` (`-N`/`-A` incl.), `git reset`, `git restore --staged`, staging of any kind, nor tooling that stages for you. Changes stay unstaged.
- Leave changes in tree; report pending; user decides commit.
- Commit msgs: no agent/AI trailers (`Co-Authored-By:`, "Generated with ...", similar).
