# Benches

## The rule

- **A criterion baseline is comparable only within its flush regime** — `small_write/put_1KiB` is flush-bound.
- The host's flush latency moves on a minutes timescale; another regime's baseline compares hosts, not revisions.
- **One run never establishes a difference**: interleave the legs, alternate order, report the interval — a sign, never a magnitude.
- The evidence is filed, not restated here: `docs/superpowers/specs/2026-10-03-ops-engine-design.md` §Filed at close-out.

## The harness

Four pieces. Each owns one job; none of them restates another.

| Piece | Owns |
|---|---|
| `tinio_util::bench` (`bench` feature) | the pinned data root and the flush control — rationale in its module docs |
| `crates/tinio-bench` | the counting allocator, the per-leg runner, the verdict rule (`verdict.rs`, unit-tested) |
| `crates/tinio-fs/benches/ab.rs` | the **leg**: one named operation per process, one `ab-result` line of stdout |
| `bench-ab` (xtask) | the run: both legs round after round, alternating order, then the verdict |

- **Reading line** (stdout, prose → stderr): `op= iters= batch= ops= p50_ns= p90_ns= min_ns= allocs_per_op= bytes_per_op= flush_ns= root=`.
- **Three channels**: time = median of `iters` × `batch` samples; flush control = the regime; allocations = exact, host-independent.
- `root=` is last on the reading line — a Windows path may contain spaces.
- **`batch`**: the timer is not free — sub-microsecond operations are measured in samples of hundreds to thousands.
- **A round is dropped** when the legs' flush controls differ beyond `--flush-band` (default 2×).
- Survivors: all ratios one side of 1.0 → `RESOLVED`; straddling → `NO RESOLVABLE DIFFERENCE`; under `--min-rounds` (5) → `UNMEASURABLE`.
- **Exit codes**: `0` answered (difference or resolved absence), `1` not (noisy host / failed leg), `2` usage.

## (a) Quick regression screen

```sh
export CARGO_TARGET_DIR=E:/Temp/tinio-target
export TINIO_BENCH_DATA_DIR=$CARGO_TARGET_DIR/bench-data
cargo bench --profile release -p tinio-fs --bench streaming -- "small_write"
```

- Read the `flush_control:` line first; compare with a stored baseline **only** if its log carries a close control.
- Harness-free variant, no criterion needed:
  ```sh
  cargo bench --profile release -p tinio-fs --bench ab -- --op head_object_plain --iters 500
  ```
- Either way this is a **screen, not a verdict**: it sees large regressions, not small ones.

## (b) Decision-grade A/B

Build the two revisions' leg binaries, give each a stable path, then run the interleaved comparison — both legs under **one** data root.

```sh
cargo bench --profile release -p tinio-fs --bench ab --no-run
# Executable benches\ab.rs (…/release/deps/ab-8f3c….exe)  — copy it to a stable path
cp <target>/release/deps/ab-<hash>.exe E:/Temp/ab-a.exe     # revision under test
cp <other-tree>/release/deps/ab-<hash>.exe E:/Temp/ab-b.exe # reference revision

cargo xtask bench-ab --exe E:/Temp/ab-a.exe --exe-b E:/Temp/ab-b.exe \
  --a head_object_plain --b head_object_plain --rounds 7
```

- One build, two operations (a calibrated pad beside a real one) is the same command without `--exe-b`.
- A leg is any process printing an `ab-result` line — a criterion target can be adapted without touching it.
- The tool refuses to dress a non-answer as one: `UNMEASURABLE` exits 1 and says what a rerun needs.

## Reading the flush control

- One thing, measured the same way everywhere: 1 KiB write + `sync_all`, median of five; `flush_ns` *is* the regime.
- Two legs beyond the band in one run: that round is dropped and reported, never averaged.
- Whole run beyond the band: the report warns; the ratio stays valid, no stored baseline is comparable.
- Flush-bound operations (`flush_1kib`, streaming, durable paths) are the regime; CPU-only ones are interleaved too.

## Unmeasurable on this host

- **Below the clock**: clock granularity here is coarse; sub-microsecond single-invocation measurements are quantisation, not time.
- **Flush control swings**: seen inside a run's rounds (CPU-only legs) — interleaving saves the ratio, not a point estimate.
- **Interval spanning 1.0** = two builds the same size on this host, not a proof of equality.
- **An impossible time** (non-positive sample) drops and names the round.
- **Causation is never measurable here**: read a stable ratio only when the change is the one difference between the builds.
