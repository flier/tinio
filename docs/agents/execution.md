# Agent execution rules

How a subagent's shell commands are scoped on this machine. Written after four runs were killed by commands that could not terminate.

## Searches

- Every search takes a repo path (`crates/…`, `docs/…`, a file). Never `/`, `~`, a drive root.
- An unscoped content search reads stdin and never returns; a trailing `head`/`sed` does too. Both look stuck.
- Locate a symbol by naming its file, never by recursing the workspace.
- Crate sources come from `CARGO_HOME`; the home-directory registry is empty — read the configured one.

## Unknown APIs

- Whether an attribute, trait impl or signature compiles is answered by **compiling**, not by reading source or searching the web.
- Probe: a `target/probe-<topic>/` crate with a `[workspace]` table and the one dependency, checked offline. Seconds.
- The error names the bound; quote the probe's output as evidence, never assert the conclusion.

## Time

- Never walk `target/`: multi-GB, looks like a hang. Name one file instead.
- A command that has not returned in ~2 min is suspect; past ~4 min, stop it and report rather than waiting.
- Builds are the exception (up to ~3 min cold), and a subagent gets its own `CARGO_TARGET_DIR` so it never waits on another's lock.
