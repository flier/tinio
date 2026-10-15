# `tinio-bench` — the measurement harness behind the workspace's A/B bench protocol

`tinio-bench` carries the three rules the workspace's A/B benches are held to: the `#[global_allocator]` that counts allocation requests, the shape of a leg (one named operation, one process, one reading), and the rule that turns interleaved rounds into a verdict. It stands apart as a crate because a `GlobalAlloc` is `unsafe` and the crates whose benches need the count set `unsafe_code = "forbid"`, which an `#![allow]` in a bench target cannot lift; only bench and test builds depend on it, and no production crate does. It does not own the leg (`crates/tinio-fs/benches/ab.rs`), the data root and the flush control (`tinio_util::bench`, the `bench` feature), the run orchestration (`xtask bench-ab`), or the operator's procedure — the flush regime, the A/B procedure and the "unmeasurable on this host" stance are `docs/benches.md`'s.

## The allocation channel

`CountingAlloc` counts every **allocation request** the process makes between a `reset` and a `usage` — not the live heap, so a value created and dropped inside one operation counts once. Installing it is safe code: one `#[global_allocator]` static in a bench target. Its count is exact for a single-threaded window, which is what the leg's timed loop is, and it is the one number on a reading the host's flush regime cannot move — which is why it is reported beside the times as a fact about the two legs and never folded into the verdict about them.

## The reading

Every leg prints one reading and exits: the operation it was asked for, its sample count and batch, the median, the 90th percentile and the minimum per invocation, the allocations and bytes per invocation, the run's flush control and the data root the run used. Three of the fields are decisions rather than decoration — the distribution is kept because the tail moves long before the median does; the batch is on the line so a mean over a batch is never read as a single timing; and the operation's name is on it so the orchestrator can see that both legs measured the operation it asked for. A reading carries a flush control even for an operation that never touches disk: without one it is comparable with nothing.

## The verdict rule

**A single run never establishes a difference.** A round is one back-to-back run of both legs, each in its own process, each carrying its own flush control, and three things are decided in order:

1. **Is the round usable?** Two legs a few seconds apart are comparable only if the host's regime held across both: a round whose flush controls differ beyond the band is dropped as regime-mixed, and the drop is reported rather than hidden.
2. **Is there an interval?** The surviving rounds give one ratio each, and their lowest and highest are the interval the verdict is reported with. Too few surviving rounds is `Unmeasurable`, never a weak verdict.
3. **Which side?** Every surviving round on one side of one is `Resolved` — a sign, never a magnitude. An interval that spans one is `NoDifference`: with these rounds, on this host, the two legs are not separable.

What no verdict claims is *causation*: two legs are two binaries, and a stable difference may come from the change under test or from anything else that differs between the two builds.

The decision carries its evidence as well as its answer: which rounds counted, which were dropped and why, the ratio series behind the interval, the legs' allocation counts when those differ, and the run's own flush controls when it spans more than the band — so a stored baseline that is not comparable with the run says so instead of being read against it.

## Layout

| File | What it is |
|---|---|
| `src/alloc.rs` | the counting allocator, its `reset`/`usage` window and the `Usage` it answers |
| `src/measure.rs` | `Op` and `run`: one named operation measured in samples of its batch, and the `Reading` a leg prints |
| `src/verdict.rs` | `Round`, `Rules`, `decide` and `Verdict`: the interleaved rule and the evidence it reports with it |
| `src/lib.rs` | the re-exports, and the test binary's own allocator installation |

## Gates

```console
cargo test -p tinio-bench
```

The rules above are unit-tested here. The leg that runs them is the consuming crate's bench target, and the operator's commands are `docs/benches.md`'s.
