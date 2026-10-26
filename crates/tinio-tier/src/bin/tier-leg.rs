//! The bench-ab leg for the tier layer: one named operation per process, one
//! `ab-result` line out, driven by `cargo xtask bench-ab` (`docs/benches.md`).
//!
//! The operations are `warm_get`, `cold_get` and `put`, each over a 1 KiB
//! object in a fresh cell of the pinned bench root. `warm_get` reads the
//! filesystem plane, `cold_get` reads the settled cold plane (the cache cap
//! excludes the object, so every sample measures the cold path), and `put`
//! stages, commits and resets one write.
//!
//! The cold plane is `RedbContent` in memory: two revisions of the tier layer
//! are what the comparison is between, and a live PostgreSQL's round trip
//! would be the same constant on both legs while burying the difference under
//! test. The reading line, the allocation channel and the verdict rule belong
//! to `tinio-bench`; the data root and the flush control to
//! `tinio_util::bench`. This target registers operations and prints their
//! readings, nothing else.
//!
//! ```text
//! cargo build -p tinio-tier --bin tier-leg --features bench --profile release
//! tier-leg --list
//! tier-leg --op cold_get --iters 2000
//! ```

// This binary is bench support, not production code: its unwraps are fixture
// assertions (docs/style.md §Panics). Clippy's `allow-*-in-tests` keys do not
// reach a `src/bin` target.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a bench-ab leg's fixture assertions (docs/style.md §Panics)"
)]

use std::{
    env, fs,
    hint::black_box,
    process,
    time::{Duration, SystemTime},
};

use tinio_bench::{CountingAlloc, Op, run};
use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::testing;
use tinio_store_redb::RedbContent;
use tinio_tier::{Config, FlushMode, MoveOutcome, TierStorage};
use tinio_util::{
    bench,
    testing::{body, read_body},
};
use tokio::runtime::Runtime;

/// The counting allocator: the one channel the host's flush regime cannot
/// move.
#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc::new();

/// The object every operation measures: 1 KiB.
const OBJECT_BYTES: usize = 1024;

/// The operations this leg registers, in `--list` order.
const OP_NAMES: &[&str] = &["warm_get", "cold_get", "put"];

/// How many timed samples and warmup invocations a run defaults to.
const DEFAULT_ITERS: u32 = 2_000;
const DEFAULT_WARMUP: u32 = 200;

/// The brief one-line usage, printed when an argument is not understood.
const USAGE: &str =
    "usage: tier-leg [--list] --op <warm_get|cold_get|put> [--iters <n>] [--warmup <n>]";

/// What this invocation of the leg does.
struct Args {
    /// Print the registered operation names and exit.
    list: bool,
    /// The one operation to measure; exactly one is required.
    ops: Vec<String>,
    /// Timed samples per operation.
    iters: u32,
    /// Untimed invocations per operation.
    warmup: u32,
}

fn main() {
    let args = match Args::parse(env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("tier-leg: {message}");
            eprintln!("{USAGE}");
            process::exit(2);
        }
    };
    if args.list {
        for name in OP_NAMES {
            println!("{name}");
        }
        return;
    }
    // One named operation per process: the interleaved A/B protocol compares
    // two legs that each measured the operation it was asked for.
    let [name] = args.ops.as_slice() else {
        eprintln!(
            "tier-leg: exactly one --op per process, found {}",
            args.ops.len()
        );
        eprintln!("{USAGE}");
        process::exit(2);
    };
    if !OP_NAMES.contains(&name.as_str()) {
        eprintln!("tier-leg: no operation named {name:?}");
        eprintln!("{USAGE}");
        process::exit(2);
    }
    let op = op_named(name).expect("checked against OP_NAMES");
    let reading = run(op, args.iters, args.warmup);
    println!("{}", reading.line());
}

impl Args {
    /// The command line, or the message that says why it is not one.
    fn parse(mut argv: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Self {
            list: false,
            ops: Vec::new(),
            iters: DEFAULT_ITERS,
            warmup: DEFAULT_WARMUP,
        };
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--list" => args.list = true,
                // Cargo passes `--bench` to every benchmark target it runs.
                "--bench" => {}
                "--op" => args.ops.push(value(&mut argv, "--op")?),
                "--iters" => args.iters = count(&mut argv, "--iters")?,
                "--warmup" => args.warmup = count(&mut argv, "--warmup")?,
                other => return Err(format!("unknown argument {other:?}")),
            }
        }
        Ok(args)
    }
}

/// The value of `flag`.
fn value(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    argv.next().ok_or_else(|| format!("{flag} needs a value"))
}

/// The numeric value of `flag`, at least one.
fn count(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<u32, String> {
    let raw = value(argv, flag)?;
    match raw.parse::<u32>() {
        Ok(n) if n > 0 => Ok(n),
        _ => Err(format!("{flag} needs a positive count, not {raw:?}")),
    }
}

/// The operation `name` registers, or [`None`] for a name that registers none.
fn op_named(name: &str) -> Option<Op> {
    match name {
        "warm_get" => Some(read_op("warm_get", false)),
        "cold_get" => Some(read_op("cold_get", true)),
        "put" => Some(put_op()),
        _ => None,
    }
}

/// The tier one operation measures through.
struct Cell {
    /// The tier under test.
    tier: TierStorage<RedbContent>,
    /// The object's bucket.
    bucket: bucket::Name,
    /// The object's key.
    key: object::Key,
    /// The runtime every async call runs on.
    runtime: Runtime,
}

/// Open a fresh cell named `cell` and write the 1 KiB object; with `cold`,
/// demote it and remove the warm file.
fn cell(cell: &'static str, cold: bool) -> Cell {
    let root = bench::cell_dir(cell);
    let runtime = Runtime::new().expect("a bench runtime");
    let tier = TierStorage::open(
        &root,
        testing::fs_options(),
        config(),
        RedbContent::in_memory().expect("an in-memory cold plane"),
    )
    .expect("the tier opens");
    let bucket = bucket::name("data").expect("a valid bucket name");
    let key = object::key("bench.bin").expect("a valid key");
    runtime.block_on(async {
        tier.create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .expect("the bucket is created");
        tier.put_object(&bucket, &key, body(vec![b'x'; OBJECT_BYTES]))
            .await
            .expect("the fixture object is written");
        if cold {
            let moved = tier
                .demote(&bucket, &key, SystemTime::now())
                .await
                .expect("the demote runs");
            assert!(
                matches!(moved, MoveOutcome::Moved { .. }),
                "the fixture object reaches the cold plane"
            );
            fs::remove_file(root.join("data").join("bench.bin"))
                .expect("the leftover warm file is removed");
        }
    });
    Cell {
        tier,
        bucket,
        key,
        runtime,
    }
}

/// A GET operation: `warm_get` reads the filesystem plane, `cold_get` the
/// settled cold plane.
fn read_op(name: &'static str, cold: bool) -> Op {
    Op::new(name, 1, move || {
        let cell = cell(name, cold);
        Box::new(move || {
            let bytes = cell.runtime.block_on(async {
                let got = cell
                    .tier
                    .get_object(&cell.bucket, &cell.key, None)
                    .await
                    .expect("the object is served");
                read_body(got.body).await.expect("the body reads")
            });
            black_box(bytes);
        })
    })
}

/// A PUT operation: stage, commit and reset one 1 KiB write.
fn put_op() -> Op {
    Op::new("put", 1, || {
        let cell = cell("put", false);
        Box::new(move || {
            cell.runtime.block_on(async {
                cell.tier
                    .put_object(&cell.bucket, &cell.key, body(vec![b'x'; OBJECT_BYTES]))
                    .await
                    .expect("the object is written");
            });
        })
    })
}

/// The runtime configuration the cells share; the cache cap keeps a cold GET
/// off the object cache.
fn config() -> Config {
    Config {
        capacity_bytes: 16 * 1024 * 1024,
        max_object_bytes: 512,
        flush_mode: FlushMode::Sync,
        batch_bytes: 8 * 1024,
        batch_interval: Duration::from_secs(1),
        heat_window: Duration::from_secs(60),
        promote_reads: 3,
        stats_flush: Duration::from_secs(10),
        high_water_percent: 90,
        low_water_percent: 80,
        demote_idle: Duration::from_secs(3600),
        dwell: Duration::from_secs(3600),
        scan_interval: Duration::from_secs(60),
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::from_secs(300),
    }
}
