//! The interleaved A/B runner behind `cargo xtask bench-ab`
//! (`docs/benches.md`).
//!
//! One named operation, two builds, one verdict. The runner spawns the
//! measurement binary (`crates/tinio-fs/benches/ab.rs`) once per leg per round,
//! **alternating which leg goes first**, and hands the series to
//! `tinio_bench::verdict` — the rule, its thresholds and the words for "the
//! interval spans no difference" all live there, unit-tested, so this file only
//! has to spawn, parse and report.
//!
//! Alternating is the whole point. A round run A-then-B and the next run B-then-A
//! means a host drifting in one direction pushes both legs' series the same way
//! and the *ratio* keeps its meaning, while one logged order would let a
//! monotone drift name the wrong winner. That is the failure both incidents
//! behind this tool share: a stored baseline compared against a run in another
//! flush regime, and a refactor judged from two sequential runs whose sign
//! flipped.
//!
//! Exit codes: `0` when the rounds answered the question — a resolved
//! difference as much as a resolved *absence* of one — `1` when they did not
//! (the host was too noisy, or a leg failed), `2` on a usage error.

use std::{
    path::Path,
    process::{Command, ExitCode},
};

use tinio_bench::{Round, Rules, Verdict, decide, verdict::Decision};

use crate::{Error, cli::BenchAbArgs};

/// The prefix of the one line a leg's stdout carries the reading on.
const RESULT_PREFIX: &str = "ab-result ";

/// Run the legs interleaved, then report what the rounds say.
pub(crate) fn run(args: &BenchAbArgs) -> Result<ExitCode, Error> {
    let exe_a = args.exe.as_path();
    let exe_b = args.exe_b.as_deref().unwrap_or(exe_a);
    print_plan(args, exe_a, exe_b);

    let mut rounds: Vec<Round> = Vec::with_capacity(args.rounds as usize);
    let mut roots: Vec<String> = Vec::new();
    for number in 1..=args.rounds {
        let a_first = number % 2 == 1;
        // The order the legs are run in is the point of the round; the tuple
        // only has to hand them back under their own names.
        let (a, b) = if a_first {
            (leg(exe_a, &args.a, args)?, leg(exe_b, &args.b, args)?)
        } else {
            let b = leg(exe_b, &args.b, args)?;
            let a = leg(exe_a, &args.a, args)?;
            (a, b)
        };
        roots.push(a.root.clone());
        roots.push(b.root.clone());
        let round = Round {
            a_ns: a.p50_ns,
            b_ns: b.p50_ns,
            a_flush_ns: a.flush_ns,
            b_flush_ns: b.flush_ns,
            a_allocs: a.allocs_per_op,
            b_allocs: b.allocs_per_op,
        };
        print_round(number, a_first, &round);
        rounds.push(round);
    }

    let rules = Rules {
        flush_band: args.flush_band,
        min_rounds: args.min_rounds,
    };
    report(&decide(&rounds, &rules), &roots)
}

/// One leg's reading, as the leg printed it.
struct Reading {
    /// The operation the leg actually measured — checked against the one asked
    /// for, so a leg that ignores `--op` cannot be reported as another.
    op: String,
    /// The leg's median, nanoseconds per operation.
    p50_ns: f64,
    /// The leg's allocation requests per operation.
    allocs_per_op: f64,
    /// The leg's flush control, nanoseconds.
    flush_ns: f64,
    /// The data root the leg measured under.
    root: String,
}

/// Spawn one leg and read its line.
fn leg(exe: &Path, op: &str, args: &BenchAbArgs) -> Result<Reading, Error> {
    let output = Command::new(exe)
        .args([
            "--op",
            op,
            "--iters",
            &args.iters.to_string(),
            "--warmup",
            &args.warmup.to_string(),
        ])
        .output()
        .map_err(|e| Error::Runtime(format!("cannot run {}: {e}", exe.display())))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        return Err(Error::Runtime(format!(
            "{} --op {op} exited with {}: {}",
            exe.display(),
            output.status,
            stderr.trim()
        )));
    }
    let reading: Reading = parse(&stdout).ok_or_else(|| {
        Error::Runtime(format!(
            "{} --op {op} printed no {RESULT_PREFIX:?} line; its stdout was: {}",
            exe.display(),
            stdout.trim()
        ))
    })?;
    if reading.op != op {
        return Err(Error::Runtime(format!(
            "{} was asked for {op:?} and measured {:?}",
            exe.display(),
            reading.op
        )));
    }
    Ok(reading)
}

/// The reading on the last `ab-result` line of a leg's stdout.
fn parse(stdout: &str) -> Option<Reading> {
    let line = stdout
        .lines()
        .rev()
        .find(|l| l.starts_with(RESULT_PREFIX))?;
    let mut reading = Reading {
        op: String::new(),
        p50_ns: 0.0,
        allocs_per_op: 0.0,
        flush_ns: 0.0,
        root: String::new(),
    };
    for token in line[RESULT_PREFIX.len()..].split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        match key {
            "op" => reading.op = value.to_string(),
            "p50_ns" => reading.p50_ns = value.parse().ok()?,
            "allocs_per_op" => reading.allocs_per_op = value.parse().ok()?,
            "flush_ns" => reading.flush_ns = value.parse().ok()?,
            _ => {}
        }
    }
    // `root` is the last key and a path may contain spaces, so it is the rest
    // of the line rather than one token.
    reading.root = line
        .split_once("root=")
        .map(|(_, root)| root.to_string())
        .unwrap_or_default();
    Some(reading)
}

/// The heading: what is being measured, how, and against what.
fn print_plan(args: &BenchAbArgs, exe_a: &Path, exe_b: &Path) {
    println!("bench-ab: {} leg A vs {} leg B", args.a, args.b);
    println!("  A: {} --op {}", exe_a.display(), args.a);
    println!("  B: {} --op {}", exe_b.display(), args.b);
    println!(
        "  {} rounds, each {} timed samples after {} warmup, order alternating A-B / B-A",
        args.rounds, args.iters, args.warmup
    );
    println!(
        "  a round is dropped when its flush controls differ by more than {:.1}×; a verdict needs \
         {} surviving rounds",
        args.flush_band, args.min_rounds
    );
    println!();
    println!(
        "{:>5} {:>5} {:>13} {:>13} {:>9} {:>12} {:>12} {:>9} {:>9}",
        "round",
        "order",
        "A p50_ns",
        "B p50_ns",
        "B/A",
        "A flush_ns",
        "B flush_ns",
        "A alloc",
        "B alloc"
    );
}

/// One round's row, printed as it happens so a long run is not silent.
fn print_round(number: u32, a_first: bool, round: &Round) {
    println!(
        "{number:>5} {:>5} {:>13.1} {:>13.1} {:>9.3} {:>12.0} {:>12.0} {:>9.3} {:>9.3}",
        if a_first { "A,B" } else { "B,A" },
        round.a_ns,
        round.b_ns,
        round.b_ns / round.a_ns,
        round.a_flush_ns,
        round.b_flush_ns,
        round.a_allocs,
        round.b_allocs
    );
}

/// The verdict, the evidence behind it, and the exit code it earns.
fn report(decision: &Decision, roots: &[String]) -> Result<ExitCode, Error> {
    println!();
    println!(
        "ratios over the surviving rounds: {}",
        decision
            .ratios
            .iter()
            .map(|ratio| format!("{ratio:.3}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    for (round, why) in &decision.discarded {
        println!("dropped round {round}: {why}");
    }
    println!();

    let unmeasurable = match decision.verdict {
        Verdict::Resolved { ratio, lo, hi } => {
            println!(
                "verdict: RESOLVED — leg B is {ratio:.3}× leg A, every surviving round on the same \
                 side of one (interval [{lo:.3}, {hi:.3}] over {} rounds)",
                decision.valid.len()
            );
            println!(
                "         a sign, not a magnitude — and not a cause: only a change under test that \
                 is the one difference between the two builds can be read into it"
            );
            false
        }
        Verdict::NoDifference { lo, hi } => {
            println!(
                "verdict: NO RESOLVABLE DIFFERENCE — the ratio interval [{lo:.3}, {hi:.3}] spans \
                 1.0 over {} rounds: on this host, with these rounds, the legs are not separable",
                decision.valid.len()
            );
            false
        }
        Verdict::Unmeasurable(why) => {
            println!("verdict: UNMEASURABLE — {why}");
            println!(
                "         nothing here says the two builds are equal: it says this host, for this \
                 run, could not tell them apart"
            );
            true
        }
    };

    if let Some((a, b)) = decision.allocation_delta {
        println!(
            "allocation channel: A {a:.3}/op vs B {b:.3}/op — exact, and the flush regime cannot \
             move it; it is the difference to quote when the times stay ambiguous"
        );
    } else if !decision.valid.is_empty() {
        println!(
            "allocation channel: both legs allocate the same per operation, so any difference this \
             run found is a time and nothing else"
        );
    }
    if let Some((lo, hi)) = decision.regime_span {
        println!(
            "warning: the run spans flush regimes ({lo:.0} ns .. {hi:.0} ns, {:.1}×) — the \
             interleaved ratio stays valid, but no stored baseline from another regime is \
             comparable with these numbers",
            hi / lo
        );
    }
    if let Some((first, rest)) = roots.split_first()
        && rest.iter().any(|root| root != first)
    {
        println!(
            "warning: the legs did not all measure under one data root, and the ratio assumes they did"
        );
    }
    Ok(if unmeasurable {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}
