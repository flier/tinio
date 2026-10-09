//! The development-credential policy of a start (audit run-2, P2): the built-in
//! `minioadmin`/`minioadmin` pair is a **loopback-only** fallback, so a start that would serve it
//! to another host refuses instead.
//!
//! **What the four legs hold, and why they are process-level.** The policy is the CLI's —
//! `tinio_server::identity::uses_builtin_fallback` is the predicate, `Resolution::credential_policy`
//! is the one decision, and `run`'s static refusal (before the fork and the directories) is the
//! enforcement — so no in-process assertion can tell whether a *start* refused or served: the
//! child's exit status and the lines it wrote are the whole evidence. This target therefore lives
//! in the `tinio` package, the one that owns the `tinio` bin (`env!("CARGO_BIN_EXE_tinio")` is
//! defined for a package's own binaries and for nothing else), and follows `lifecycle.rs`'s
//! harness shape.
//!
//! **Both of the child's streams go to files rather than pipes**, the shape `lifecycle.rs` records
//! the reasons for: a pipe this test holds is a handle any descendant of the server could keep
//! open, so a read of one can block on a process the test has already killed.
//!
//! **Every wait is bounded**, on `tinio_util::process::poll_exit` (the workspace's one wait shape):
//! each leg asserts on a child that is meant to be *gone* (the refusal) or *ready* (the three
//! starts that must not refuse). An unbounded `wait_with_output`, or a read of stderr to EOF, would
//! turn a regression into a hung test rather than a failing one: before the refusal existed, the
//! non-loopback fallback start served forever, which is exactly what
//! `the_builtin_pair_is_refused_on_a_non_loopback_bind` has to answer with a failure.
//!
//! **What "must not refuse" is asserted against is the readiness frame, not survival** (review
//! M3): a leg that only polls the child for 1500 ms shows it was alive, and an alive process can be
//! one that has not refused *yet* — a refusal moved after the poll, or a loaded host crossing the
//! bound, would pass. Every serving leg therefore waits, within the same [`START_BOUND`] the
//! refusal gets, for the `event: listening` **frame** the CLI writes on ready (the readiness point
//! of `crates/tinio-cli/src/events.rs` and `contracts/cli.md:59`), read off the child's stdout
//! **file**, and only then asserts the child is still alive.
//!
//! **Every spawn goes through `job::spawn`** — the one spelling a harness's spawn sites use, held
//! up by `crates/tinio-util/tests/spawn_guard.rs`'s scan of `crates/*/tests/**` — and every path
//! out kills and reaps the child, so a failing assertion never leaves a server holding the root's
//! state database.

// **A test-support root (docs/style.md §Panics), and a stated one because clippy's test keys do
// not reach it** (design 2026-10-09 D7): `allow-*-in-tests` covers `#[test]` functions and
// `#[cfg(test)]` modules, while this target's helpers (the readiness wait, the child teardown,
// the root fixture) are plain `fn`s in an integration target.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a test target's harness helpers (docs/style.md §Panics)"
)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use tinio_util::{
    job,
    process::{kill_and_reap, poll_exit},
};

/// The bound a start is given to refuse, to become ready (its `event: listening` frame) or to say
/// what it is serving: a leg waits no longer than this for an answer rather than reading a child
/// that is meant to serve forever. Generous — what the refusal costs is a config load, and a slow
/// first start on a loaded host is not the subject.
const START_BOUND: Duration = Duration::from_secs(15);

/// One step of the bounded stream wait: `poll_exit`'s own resolution — its interval is fixed at
/// 50 ms and is not a knob (its doc says why) — so the file is read between two `try_wait`s
/// rather than once at the end of the bound.
const POLL_STEP: Duration = Duration::from_millis(50);

/// The loopback warning's own sentence, pinned whole rather than by a fragment of it: `loopback`
/// appears in the refusal's message too ("a non-loopback bind needs explicit credentials"), so a
/// loopback branch that regressed into refusing everywhere would satisfy the bare substring.
const LOOPBACK_WARNING: &str = "serving the built-in minioadmin pair on loopback";

/// What the summary says the pair came from when the file carries no `[auth]` section, pinned
/// whole: the leg's claim is that the summary and the warning above it tell one story, and
/// `from the config file` is the sentence that must not stand beside [`LOOPBACK_WARNING`].
const LOOPBACK_ORIGIN: &str = "the built-in development pair (no [auth] configured)";

/// The readiness **frame**'s event line — the point `contracts/cli.md:59` makes a harness decode
/// and `crates/tinio-cli/src/events.rs` writes. The event line rather than the whole frame: the
/// `data:` line's ephemeral address is nothing this leg asserts, and the encoding's exact bytes
/// are pinned in the core, where the encoding lives.
const READY_FRAME: &str = "event: listening";

/// The summary's invariant half — the tail every arm of `credential_origin` is appended to, at
/// `crates/tinio-cli/src/commands/start/serve.rs` — and the loopback leg's wait anchor. Waiting on
/// the arm under test would turn a regressed arm into a 15 s timeout instead of a failed
/// assertion; this half is written whatever the arm says.
const SUMMARY_TAIL: &str = "; credentials ";

/// The `tinio` binary under test.
fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tinio"))
}

/// A scratch directory, a served root inside it, and a config path beside that root.
fn temp_root() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let cfg = dir.path().join("cfg.toml");
    (dir, root, cfg)
}

/// Point both of the child's streams at files — `out` for stdout's event frames, `log` for
/// stderr's warning and summary — clearing the credential variables so the config file, its
/// `[auth]` section or the absent one the other legs are about, is what the pair comes from.
///
/// **The refusal leg's stdout file is simply never read**: its claim is the exit status and the
/// message on stderr. It is a file and not `Stdio::null()` because one spawn shape serves all four
/// legs, and because a file is what the readiness legs need either way.
fn detached(command: &mut Command, out: &Path, log: &Path) {
    command
        .env_remove("TINIO_ACCESS_KEY")
        .env_remove("TINIO_SECRET_KEY")
        .stdout(Stdio::from(fs::File::create(out).expect("stdout file")))
        .stderr(Stdio::from(fs::File::create(log).expect("stderr file")));
}

/// One leg's arrange block: a scratch directory held alive for the child's lifetime, the two
/// stream files, and the `tinio server` command already pointed at them — the four legs differ
/// only in the config text, the host, and the tag their files carry.
struct Leg {
    /// Held so the scratch tree outlives the child the leg spawns into it.
    _dir: tempfile::TempDir,
    out: PathBuf,
    log: PathBuf,
    command: Command,
}

fn leg(tag: &str, config: &str, host: &str) -> Leg {
    let (dir, root, cfg) = temp_root();
    fs::write(&cfg, config).expect("config");
    let out = dir.path().join(format!("{tag}.stdout"));
    let log = dir.path().join(format!("{tag}.stderr"));
    let mut command = bin();
    command.args([
        "server",
        root.to_str().unwrap(),
        "--config",
        cfg.to_str().unwrap(),
        "--host",
        host,
        "--port",
        "0",
    ]);
    detached(&mut command, &out, &log);
    Leg {
        _dir: dir,
        out,
        log,
        command,
    }
}

/// Spawn `command`, wait for `want` to appear on the file at `stream` or for it to exit or for
/// `START_BOUND` to pass — whichever comes first — and return the file's text together with
/// whether `want` was what ended the wait and the child was still alive after it. Kills and reaps
/// it either way.
///
/// **One wait shape for both streams**, because the two claims want the same two facts out of
/// different carriers: a readiness leg waits on stdout for [`READY_FRAME`], and the loopback leg
/// waits on stderr for [`SUMMARY_TAIL`] — written *after* the readiness frame, so a wait that ends
/// on it has the warning, the frame and the summary all in place, and the assertions after it
/// cannot race the run.
fn stream_until(command: &mut Command, stream: &Path, want: &str) -> (String, bool) {
    /// One read of the stream file: the whole of "has it said `want` yet".
    fn has(path: &Path, want: &str) -> bool {
        fs::read_to_string(path).unwrap_or_default().contains(want)
    }
    let mut child = job::spawn(command).expect("spawn");
    let deadline = Instant::now() + START_BOUND;
    // No sleep of the loop's own: `poll_exit` already spends `POLL_STEP` per iteration (its
    // interval is fixed), so the file is read between two `try_wait`s and the wait's resolution
    // stays one `POLL_STEP` rather than two.
    let mut seen = has(stream, want);
    while !seen {
        if poll_exit(&mut child, POLL_STEP).is_some() || Instant::now() >= deadline {
            break;
        }
        seen = has(stream, want);
    }
    // Alive is true only when the wait ended on the wanted text itself — not on the child's exit
    // and not on the bound — with the child still running after it: the difference between "said
    // it and serving" and "said it and then died", which the text alone cannot tell.
    let alive = seen && poll_exit(&mut child, POLL_STEP).is_none();
    kill_and_reap(&mut child);
    (fs::read_to_string(stream).unwrap_or_default(), alive)
}

/// The built-in pair is a development credential: on a bind another host can reach, there is no
/// pair to serve at all, so the start refuses and names both escapes that would supply one — the
/// `[auth]` section and the `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` variables.
#[test]
fn the_builtin_pair_is_refused_on_a_non_loopback_bind() {
    let mut leg = leg("refusal", "version = 1\n", "0.0.0.0");
    let mut child = job::spawn(&mut leg.command).expect("spawn");
    let status = poll_exit(&mut child, START_BOUND);
    kill_and_reap(&mut child);
    let status = status.expect("a non-loopback fallback start must refuse instead of serving");
    let err = fs::read_to_string(&leg.log).expect("stderr");
    assert!(
        !status.success(),
        "a non-loopback fallback start must fail: {err}"
    );
    assert!(
        err.contains("auth"),
        "the error names the missing [auth] pair: {err}"
    );
    // The design (§2) requires the configuration error to name both escapes, not just `[auth]`:
    // an operator who hits this has to be told the environment variables are the other one. Both
    // must appear — the message carrying only `[auth]` (the shipped form before this leg) fails.
    assert!(
        err.contains("TINIO_ACCESS_KEY") && err.contains("TINIO_SECRET_KEY"),
        "the error names the environment escape too: {err}"
    );
}

/// The other half of the policy: on loopback the built-in pair still serves, and the start says so
/// — a development pair an operator may keep using, and one they are warned is not a credential.
///
/// **One stderr at readiness time must not tell both stories** (review I2): the warning says the
/// served pair is the built-in `minioadmin` one, and the summary's credential origin has to agree
/// with it rather than answer "from the config file" for a section no file carries. Waiting on the
/// summary's own tail is what makes the two assertions race-free — the summary is written after
/// the readiness frame, so a wait that ends on it has the frame, the warning and the summary all
/// in place.
#[test]
fn the_builtin_pair_starts_on_loopback() {
    let mut leg = leg("loopback", "version = 1\n", "127.0.0.1");
    let (err, serving) = stream_until(&mut leg.command, &leg.log, SUMMARY_TAIL);
    assert!(
        err.contains(LOOPBACK_WARNING),
        "a loopback fallback start warns: {err}"
    );
    assert!(
        err.contains(LOOPBACK_ORIGIN),
        "and its summary says which pair it serves, rather than claiming the config file supplied \
         one: {err}"
    );
    assert!(
        serving,
        "and keeps serving rather than exiting after the warning and its summary: {err}"
    );
    let stdout = fs::read_to_string(&leg.out).expect("stdout");
    assert!(
        stdout.contains(READY_FRAME),
        "and it reports readiness as the frame, not as a survival poll: {stdout}"
    );
}

/// And a configured `[auth]` pair is what the refusal is about: with one, the same non-loopback
/// bind serves — the credential the operator chose is the one the warning beside the bind is for.
///
/// **Readiness, not survival** (review M3): the leg waits for the `listening` frame and asserts the
/// child is alive *after* it. A refusal moved past the old 1500 ms poll, or a loaded host crossing
/// that bound, would have passed the survival poll this replaces; neither can produce the frame.
#[test]
fn explicit_credentials_start_on_a_non_loopback_bind() {
    let mut leg = leg(
        "explicit",
        "version = 1\n[auth]\naccess_key = \"AK\"\nsecret_key = \"SK\"\n",
        "0.0.0.0",
    );
    let (stdout, ready) = stream_until(&mut leg.command, &leg.out, READY_FRAME);
    assert!(
        ready,
        "an explicit [auth] must serve a non-loopback bind and frame its readiness: {stdout}"
    );
}

/// And the environment pair is a configured credential too: `TINIO_ACCESS_KEY`/
/// `TINIO_SECRET_KEY` are folded onto `[auth]` before the predicate is asked
/// (`resolve::fold_credentials`), so a host that exports them needs no `[auth]` section to serve
/// the same non-loopback bind — which is the escape the refusal's own message names.
#[test]
fn the_environment_pair_serves_a_non_loopback_bind() {
    let mut leg = leg("environment", "version = 1\n", "0.0.0.0");
    // Cleared first by [`detached`], so variables the *test runner* carries cannot decide this
    // leg's outcome, and then set to the pair the fold reads — the exported pair, spelled as the
    // child's environment rather than as a file the operator would have to write.
    leg.command
        .env("TINIO_ACCESS_KEY", "environment-ak")
        .env("TINIO_SECRET_KEY", "environment-sk");
    let (stdout, ready) = stream_until(&mut leg.command, &leg.out, READY_FRAME);
    assert!(
        ready,
        "the environment pair must serve a non-loopback bind and frame its readiness: {stdout}"
    );
}
