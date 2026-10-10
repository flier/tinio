# Benches

## The rule

- **A criterion baseline is comparable only within its flush regime.** `small_write/put_1KiB` does two durable flushes plus a directory sync, so its number is the host's flush latency wearing a benchmark's clothes — and that latency moves ~3× on a minutes timescale. A stored baseline read back in another regime is a comparison of two hosts, not two revisions.
- **A single run never establishes a difference.** Interleave the two legs in time, alternate which goes first, and report the *interval* the rounds produce — only rounds that all fall on one side of "no difference" are a difference, and even that is a sign, never a magnitude.
- The evidence is filed, not restated here: `docs/superpowers/specs/2026-10-03-ops-engine-design.md` §Filed at close-out (the unchanged binary that reproduced `+183.70%`, and the refactor judged from two sequential runs whose sign flipped).

## The harness

Four pieces. Each owns one job; none of them restates another.

| Piece | Owns |
|---|---|
| `tinio_util::bench` (`bench` feature) | the pinned data root and the flush control — rationale in its module docs |
| `crates/tinio-bench` | the counting allocator, the per-leg runner, the verdict rule (`verdict.rs`, unit-tested) |
| `crates/tinio-fs/benches/ab.rs` | the **leg**: one named operation per process, one `ab-result` line of stdout |
| `cargo xtask bench-ab` | the run: spawns both legs round after round, alternating order, then decides |

- **Reading line** (stdout, one per leg per run; prose goes to stderr): `op= iters= batch= ops= p50_ns= p90_ns= min_ns= allocs_per_op= bytes_per_op= flush_ns= root=`. `root=` is last because a Windows path may contain spaces.
- **Three channels, not one.** *Time* is the median of `iters` timed samples, each sample `batch` invocations. *Flush control* is the run's regime. *Allocations* is exact: an integer count divided by the invocation total, and the one number here the host cannot move. When the times stay ambiguous, quote the allocation count.
- **`batch`** exists because the timer is not free: an `Instant::now()` pair costs tens of nanoseconds, so anything cheaper than a microsecond is measured in samples of hundreds to thousands of invocations. The reading is then a median of means. `ab --list` names the registered operations; a new one is a few lines in `ab.rs`.
- **A round is dropped** when its two legs' flush controls differ by more than the band (`--flush-band`, default 2×) — the two runs were not in one regime, whatever they measured. Survivors feed the ratio series: all on one side of 1.0 is `RESOLVED` (interval reported), any straddling is `NO RESOLVABLE DIFFERENCE`, fewer than `--min-rounds` (default 5) is `UNMEASURABLE`.
- **Exit codes**: `0` the rounds answered the question — a resolved difference as much as a resolved absence of one; `1` they did not (host too noisy, or a leg failed); `2` usage.

## (a) Quick regression screen

```sh
export CARGO_TARGET_DIR=E:/Temp/tinio-target
export TINIO_BENCH_DATA_DIR=$CARGO_TARGET_DIR/bench-data
cargo bench --profile release -p tinio-fs --bench streaming -- "small_write"
```

- Read the `flush_control:` line first, then the number. Compare with a stored baseline **only** if that baseline's run log carries a flush control close to this one (criterion itself stores none, so a baseline whose control was never logged is not comparable — at all).
- One leg of the harness is the harness-free version of the same screen, and it needs no criterion:
  ```sh
  cargo bench --profile release -p tinio-fs --bench ab -- --op head_object_plain --iters 500
  ```
- Either way this is a **screen, not a verdict**: it can see a 3× regression, and it cannot see a 5% one.

## (b) Decision-grade A/B

Build the two revisions' leg binaries, give each a stable path, then run the interleaved comparison. Both legs must measure under **one** data root, so set `TINIO_BENCH_DATA_DIR` explicitly when the two builds do not share a `CARGO_TARGET_DIR`.

```sh
cargo bench --profile release -p tinio-fs --bench ab --no-run
# Executable benches\ab.rs (…/release/deps/ab-8f3c….exe)  — copy it to a stable path
cp <target>/release/deps/ab-<hash>.exe E:/Temp/ab-a.exe     # revision under test
cp <other-tree>/release/deps/ab-<hash>.exe E:/Temp/ab-b.exe # reference revision

cargo xtask bench-ab --exe E:/Temp/ab-a.exe --exe-b E:/Temp/ab-b.exe \
  --a head_object_plain --b head_object_plain --rounds 7
```

- One build, two operations (a calibrated pad beside a real one) is the same command without `--exe-b`.
- A leg is any process that prints an `ab-result` line, so a criterion bench target can be adapted to a leg without touching what it measures.
- The tool refuses to dress a non-answer as one: `UNMEASURABLE` exits 1, and the report says what a rerun needs.

## Reading the flush control

- One thing, measured the same way everywhere: a 1 KiB write plus `sync_all`, median of five. `flush_ns` on the reading line *is* the run's regime.
- Same run, two legs, more than the band apart: the regime moved between them — that round is dropped and reported, not averaged.
- Whole run spanning more than the band: the report warns. The interleaved ratio stays valid; **no stored baseline from another regime is comparable with the numbers**.
- Flush-bound operations (`flush_1kib`, the streaming write/read legs, any durable path) are the ones whose number is the regime. CPU-only operations are not — but their host moves too, so they are interleaved like everything else.

## Unmeasurable on this host

- **Below the clock.** This host's `Instant` granularity measures 100 ns (three separate 1 µs / 20 µs / 500 µs busy-waits all land on exact 100 ns multiples), so a single-invocation measurement of anything under ~1 µs is a quantisation, not a time. Batch it, or read the allocation count instead.
- **When the flush control swings.** Measured on one A/B run: 293 µs → 1059 µs, a 3.6× span, inside seven rounds of two CPU-only legs. Interleaving saves the ratio; it does not save a point estimate.
- **When the interval spans 1.0.** That means this host, in this run, saw two builds the same size. It is not a proof of equality, and "the change is neutral" is not the same claim.
- **When a leg reports a time no operation can have** (a non-positive sample) the round is dropped and named.
- **Causation is never measurable here.** Two builds differ by more than the change under test: layout, inlining, and every dependency's code too. A stable ratio is a fact about the two binaries; only a change that is the *one* difference between them can be read into it.
