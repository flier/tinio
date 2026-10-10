//! The workspace's A/B measurement entry point: one named operation per run,
//! one `ab-result` line out, driven by `cargo xtask bench-ab`
//! (`docs/benches.md`).
//!
//! This target is the *leg*, not the comparison. It runs one operation, times
//! it, and prints the reading; the interleaving, the rounds and the verdict
//! belong to `cargo xtask bench-ab`, which spawns this binary once per leg.
//! The reason for the split is the whole point of the protocol: two builds are
//! two binaries, and a benchmark that takes the mean of one long run cannot
//! tell a difference from a drift of the host underneath it.
//!
//! ```text
//! cargo bench -p tinio-fs --bench ab -- --list
//! cargo bench -p tinio-fs --bench ab -- --op spin_20us --iters 2000
//! ```
//!
//! The operations are of three kinds, and each kind is here to make one claim
//! checkable rather than to be fast:
//!
//! - **the calibrated pads** (`spin_1us`, `spin_20us`, `spin_500us`) busy-wait
//!   until a stated instant has passed. `noop` is the floor they are read
//!   against, and the pair is what a protocol can be *checked* with: the
//!   answer is known before the measurement.
//! - **the allocation pair** (`alloc_0`, `alloc_4`) does the same work, four
//!   `Box`es apart. The count is exact and the host cannot move it — the one
//!   channel beside the times that a flush regime cannot fake.
//! - **the receiver pair** (`receiver_legacy`, `receiver_now`) measures the
//!   expression the ops-engine extraction removed, `Arc::new(self.clone())`,
//!   against the shape that replaced it, a `OnceLock` hit plus `Arc` clones
//!   (see the `receiver_handles` note in `crates/tinio-fs/src/backend/mod.rs`).
//!   `fs_storage_clone` prices the clone alone, and `flush_1kib` is the
//!   regime-sensitive operation the false 2.7× regression was read from — one
//!   1 KiB write plus `sync_all`, the flush control as a measurement.
//!
//! `data_root` and `flush_control` come from `tinio_util::bench` and the rest
//! from `tinio-bench`: this file registers operations and nothing else.

// This bench declares `harness = false`, so clippy's `allow-*-in-tests` keys do
// not reach it (design 2026-10-09 D7), and a bench is not production code
// (style.md §Panics covers production sources, D1): its unwraps are fixture
// assertions, kept as written.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a `harness = false` bench's fixture assertions (docs/style.md §Panics)"
)]

use std::{
    env,
    fs::File,
    hint::{black_box, spin_loop},
    io::Write,
    process,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

use tinio_bench::{CountingAlloc, Op, run};
use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::{FsStorage, testing::fs_options};
use tinio_util::{bench, testing::body};
use tokio::runtime::Runtime;

/// The counting allocator: every reading carries an allocation count, and the
/// count is exact even when the host is not (`tinio_bench::alloc`).
#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc::new();

/// The operations this target registers, in `--list` order.
const OP_NAMES: &[&str] = &[
    "noop",
    "spin_1us",
    "spin_20us",
    "spin_500us",
    "alloc_0",
    "alloc_4",
    "fs_storage_clone",
    "receiver_legacy",
    "receiver_now",
    "head_object_plain",
    "flush_1kib",
];

/// What this invocation of the leg does.
struct Args {
    /// Print the registered operation names and exit.
    list: bool,
    /// The CI smoke shape: the fewest iterations that still run every
    /// operation (`cargo bench ... -- --test`).
    test: bool,
    /// The operations to measure; every one when empty.
    ops: Vec<String>,
    /// Timed samples per operation.
    iters: u32,
    /// Untimed invocations per operation.
    warmup: u32,
}

/// How many timed samples and warmup invocations a run defaults to.
const DEFAULT_ITERS: u32 = 2_000;
const DEFAULT_WARMUP: u32 = 200;

/// The brief one-line usage, printed when an argument is not understood.
const USAGE: &str = "usage: ab [--list] [--op <name>]... [--iters <n>] [--warmup <n>] [--test]";

fn main() {
    let args = match Args::parse(env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("ab: {message}");
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
    let wanted: Vec<&str> = if args.ops.is_empty() {
        OP_NAMES.to_vec()
    } else {
        let mut wanted = Vec::with_capacity(args.ops.len());
        for name in &args.ops {
            if !OP_NAMES.contains(&name.as_str()) {
                eprintln!("ab: no operation named {name:?}");
                eprintln!("{USAGE}");
                process::exit(2);
            }
            wanted.push(name.as_str());
        }
        wanted
    };
    let (iters, warmup) = if args.test {
        (args.iters.min(50), args.warmup.min(10))
    } else {
        (args.iters, args.warmup)
    };
    for name in wanted {
        let op = op_named(name).expect("checked against OP_NAMES");
        let reading = run(op, iters, warmup);
        println!("{}", reading.line());
    }
}

impl Args {
    /// The command line, or the message that says why it is not one.
    fn parse(mut argv: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = Self {
            list: false,
            test: false,
            ops: Vec::new(),
            iters: DEFAULT_ITERS,
            warmup: DEFAULT_WARMUP,
        };
        while let Some(arg) = argv.next() {
            match arg.as_str() {
                "--list" => args.list = true,
                "--test" => args.test = true,
                // Cargo passes `--bench` to every benchmark target it runs;
                // the flag is the harness's, not this leg's.
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
        "noop" => Some(noop()),
        "spin_1us" => Some(spin("spin_1us", Duration::from_micros(1))),
        "spin_20us" => Some(spin("spin_20us", Duration::from_micros(20))),
        "spin_500us" => Some(spin("spin_500us", Duration::from_micros(500))),
        "alloc_0" => Some(alloc("alloc_0", 0)),
        "alloc_4" => Some(alloc("alloc_4", 4)),
        "fs_storage_clone" => Some(storage_clone()),
        "receiver_legacy" => Some(receiver_legacy()),
        "receiver_now" => Some(receiver_now()),
        "head_object_plain" => Some(head_object_plain()),
        "flush_1kib" => Some(flush_1kib()),
        _ => None,
    }
}

/// The floor: the cheapest thing a timer can be pointed at, batched so the
/// timer's own tens of nanoseconds stay out of the reading.
fn noop() -> Op {
    Op::new("noop", 1_000, || {
        Box::new(|| {
            black_box(cheap());
        })
    })
}

/// The same floor, plus a busy-wait to `target`.
///
/// The answer is known before the measurement: a sample takes `target` plus one
/// pass of the wait loop, tens of nanoseconds at most. This is what a protocol
/// can be checked with — `spin_20us` minus `noop` must read twenty
/// microseconds, or the protocol is not measuring what it says it measures.
fn spin(name: &'static str, target: Duration) -> Op {
    Op::new(name, 1, move || {
        Box::new(move || {
            let start = Instant::now();
            while start.elapsed() < target {
                spin_loop();
            }
        })
    })
}

/// The floor, plus `per_op` `Box<[u8; 64]>`es per invocation.
///
/// Four allocations is the *exact* difference between `alloc_0` and `alloc_4`:
/// the count is not a statistic, and the flush regime cannot move it.
fn alloc(name: &'static str, per_op: u32) -> Op {
    Op::new(name, 500, move || {
        Box::new(move || {
            for _ in 0..per_op {
                black_box(Box::new([0u8; 64]));
            }
            black_box(cheap());
        })
    })
}

/// One `FsStorage` clone: the deep copy `Arc::new(self.clone())` paid once per
/// operation before the receiver handles were cached.
fn storage_clone() -> Op {
    Op::new("fs_storage_clone", 100, || {
        let storage = storage("fs_storage_clone");
        Box::new(move || {
            black_box(storage.clone());
        })
    })
}

/// The removed expression, per operation: `Arc::new(self.clone())`.
fn receiver_legacy() -> Op {
    Op::new("receiver_legacy", 100, || {
        let storage = storage("receiver");
        Box::new(move || {
            black_box(Arc::new(storage.clone()));
        })
    })
}

/// The expression that replaced it: one resolve of an already-filled
/// `OnceLock`, plus the `Arc` clones the receiver hands out.
///
/// Not the backend's own constructor — that one is crate-private and no bench
/// can reach it — but the same two moves: a `get_or_init` that only loads, and
/// the `Arc::clone`s behind it.
fn receiver_now() -> Op {
    Op::new("receiver_now", 100, || {
        let handles: Arc<OnceLock<(Arc<u8>, Arc<u8>)>> = Arc::new(OnceLock::new());
        let _ = handles.get_or_init(|| (Arc::new(0), Arc::new(0)));
        Box::new(move || {
            let handles = handles.get_or_init(|| (Arc::new(0), Arc::new(0)));
            black_box((Arc::clone(&handles.0), Arc::clone(&handles.1)));
        })
    })
}

/// The plain read over a 1 KiB object — the leg the close-out's profile round
/// priced by hand and the `read_meta` criterion group guards.
///
/// The runtime, the bucket and the object are the fixture: what is timed is
/// `head_object` and nothing else.
fn head_object_plain() -> Op {
    Op::new("head_object_plain", 1, || {
        let storage = storage("head_object_plain");
        let runtime = Runtime::new().unwrap();
        let (bkt, key) = runtime.block_on(async {
            let bkt = bucket::name("data").unwrap();
            storage
                .create_bucket(&bkt, None, &Acl::default_private(None))
                .await
                .unwrap();
            let key = object::key("small.bin").unwrap();
            storage
                .put_object(&bkt, &key, body(vec![b'y'; 1024]))
                .await
                .unwrap();
            (bkt, key)
        });
        Box::new(move || {
            let head = runtime.block_on(async { storage.head_object(&bkt, &key).await.unwrap() });
            black_box(head);
        })
    })
}

/// The regime-sensitive operation the false 2.7× regression was read from: one
/// 1 KiB write plus `sync_all`, the flush control as a measurement.
fn flush_1kib() -> Op {
    Op::new("flush_1kib", 1, || {
        let path = bench::cell_dir("flush_1kib").join("probe.bin");
        let payload = vec![b'z'; 1024];
        Box::new(move || {
            let mut file = File::create(&path).unwrap();
            file.write_all(&payload).unwrap();
            file.sync_all().unwrap();
        })
    })
}

/// A storage over a fresh cell of the pinned bench root — the fixture every
/// storage operation here shares.
fn storage(cell: &str) -> FsStorage {
    FsStorage::new(bench::cell_dir(cell), fs_options()).unwrap()
}

/// The cheapest work that cannot be optimized away.
fn cheap() -> u64 {
    black_box(0u64).wrapping_add(1)
}
