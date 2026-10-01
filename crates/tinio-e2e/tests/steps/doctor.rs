//! The doctor's report and its repairs (T057): the fixture a root carries and
//! the assertions over the report it produces — `doctor.feature`'s step layer,
//! split out of `steps/cli.rs` (one step file per domain, 2026-10-01).
//!
//! **The fixtures are store-built rather than hand-written.** A redb row is not
//! a file a scenario can write, so every seed here is made through the store
//! that owns the row's shape — a whole store through `FsStorage`, a single row
//! through `meta::store`/`bucket::store` — and the elements a row carries are
//! `tinio_core`'s, reached the way the binary reaches every product crate
//! (`cucumber.rs`'s `_core` alias).

use std::{
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use cucumber::{given, then, when};
use tinio_api::state::{STATE_FILE, STATE_VERSION, State, socket_path};
use tinio_fs::{FsStorage, bucket, database, meta, testing::fs_options};

use super::{
    World,
    cli::{Cli, cli, expand, harness},
};
use crate::_core::{bucket::name as bucket_name, etag::ETag, object::key as object_key};

/// The token the doctor fixture writes into a `state` file.
///
/// Distinctive on purpose: the redaction scenario asserts this exact string reaches neither
/// stream, in either rendering, and a token that read like a real one would make the assertion
/// unwitnessable (`crates/tinio/tests/common/mod.rs`'s `TOKEN` is the same constant for the Rust
/// residue's fixture, and the scenario's `the root's state file carries the fixture's token` is
/// what keeps the two absences from passing over a fixture that never had a token at all).
const DOCTOR_TOKEN: &str = "doctor-must-never-print-this-token";

/// A configuration complete enough that nothing but the defect under test is wrong.
///
/// **The `[auth]` pair is load-bearing rather than decorative**: "no credentials at all" is a
/// `warn` of its own (the check list's `configuration` leg), so a fixture without it could never
/// reach a report with no problem in it — and a clean report is what the store-less, the
/// read-only-client and the store-less-`--fix` scenarios assert. Spelled here rather than in each
/// scenario's docstring because it is one fixture and not one fact per scenario;
/// `crates/tinio/tests/common/mod.rs`'s `COMPLETE_CONFIG` is the residue leg's spelling of it.
const DOCTOR_CONFIG: &str = "version = 1\n\
     [auth]\n\
     access_key = \"tinio-access-key\"\n\
     secret_key = \"tinio-secret-key\"\n\
     [server]\n\
     port = 9000\n";

/// The root's reserved directory, created if the fixture has not created it yet — every store
/// seed and every `state` file the scenarios plant lives in it.
fn doctor_state_dir(cli: &Cli) -> PathBuf {
    let dir = cli.root().join(".tinio");
    fs::create_dir_all(&dir)
        .unwrap_or_else(|err| panic!("the reserved directory `{}`: {err}", dir.display()));
    dir
}

/// Writes the configuration a root needs before anything but the defect under test is wrong.
fn doctor_configure(cli: &Cli) {
    let dir = doctor_state_dir(cli);
    fs::write(dir.join(".tinio.toml"), DOCTOR_CONFIG).expect("the root's configuration");
}

/// A **valid** store, made by the storage that owns the file's shape: a redb file cannot be
/// hand-written, so the fixture asks `tinio-fs` for one and drops the handle, which is what leaves
/// it openable for the binary under test.
fn doctor_store(cli: &Cli) {
    drop(FsStorage::new(cli.root(), fs_options()).expect("the root's store"));
}

/// The `state` file a crashed run leaves, written **through the plane's own type** rather than as
/// a hand-built document: a Windows root path in a string literal would need its backslashes
/// escaped to be JSON at all, and the writer is what fixes the field names and the version.
fn doctor_stale_state(state_dir: &Path, root: &Path, control_name: &str) {
    let state = State::with_token(control_name, Some(9000), root, DOCTOR_TOKEN)
        .expect("the fixture's state document");
    state
        .write(state_dir)
        .unwrap_or_else(|err| panic!("the state file in `{}`: {err}", state_dir.display()));
}

/// The control-channel name a run of this root derives — `socket_path`'s answer, called rather
/// than re-spelled, and **over the canonical root**: the CLI canonicalises the root before it
/// derives anything (`resolve::resolve_root`), so a fixture that hashed the scratch path as
/// spelled would plant a `state` file naming a channel the run never probes.
fn doctor_derived_control_name(cli: &Cli) -> String {
    let root = cli.canonical_root();
    socket_path(&root.join(".tinio"), &root)
        .expect("the derived socket path")
        .to_string_lossy()
        .into_owned()
}

/// Binds `path` once and closes it: the file is left behind and nothing answers it — the socket a
/// crashed unix run leaves, and the only induction that reaches the reclaim probe's `Ok(true)`
/// arm.
#[cfg(unix)]
fn doctor_stale_socket(path: &Path) {
    use std::os::unix::net::UnixListener;

    drop(
        UnixListener::bind(path)
            .unwrap_or_else(|err| panic!("binding the stale socket `{}`: {err}", path.display())),
    );
}

/// One rendered report line as its severity word and its check's name, or `None` for a line that
/// is not a finding.
///
/// The shape is the report's own rendering (`Report::render_text`):
/// `{severity:<5} {check}: {detail}` per finding, then one `severity: {worst}` summary line — which
/// has no second field and so parses as `None` rather than as a row whose check is the whole
/// sentence. The split is the report's own (`crates/tinio-cli/src/report.rs`), so the two callers
/// below read the same fields the renderer wrote rather than a second grammar of the line.
fn doctor_row(line: &str) -> Option<(&str, &str)> {
    let (severity, rest) = line.split_once(' ')?;
    let (check, _) = rest.trim_start().split_once(':')?;
    Some((severity, check))
}

/// The severity a `--json` document reports for the run.
///
/// **One field is read; the document is not re-encoded.** `tinio-e2e` carries no JSON reader —
/// `serde_json` is not among its dependencies, and the suite's other captured-output steps match
/// text for the same reason — so a scenario would otherwise be unable to name the run's own
/// severity, which is the field a `--fix --json` refusal must carry at `error` and a
/// `--dry-run --json` proposal at `warn`. What makes the read unambiguous is the renderer's own
/// field order: `Report`'s `Serialize` writes `severity` before `findings` (the schema's
/// `required` order, `crates/tinio-cli/src/report.rs`), so the first `severity` field of a
/// document is the report's and every finding's follows it.
///
/// **The shape half of the contract is not here and does not move**: the key set against
/// `contracts/doctor-report.schema.json` is `crates/tinio/tests/doctor.rs`'s
/// `the_json_report_is_the_schema_shape`, which is what `docs/tests.md`'s T057 gate names.
fn report_severity(stdout: &str) -> &str {
    let after_key = stdout
        .lines()
        .find_map(|line| line.split_once("\"severity\""))
        .map(|(_, rest)| rest)
        .and_then(|rest| rest.trim_start().strip_prefix(':'))
        .unwrap_or_else(|| panic!("the run's stdout is not a report document:\n{stdout}"));
    after_key
        .trim_start()
        .strip_prefix('"')
        .and_then(|rest| rest.split('"').next())
        .unwrap_or_else(|| panic!("the document's `severity` is not a string:\n{stdout}"))
}

/// The composite fixture the plain and `--fix` journeys share: the private state a run that
/// crashed leaves behind — the complete configuration, a real store, a stale `state` file and a
/// leftover temp file. The store is what makes the fixture a *crashed* run rather than a
/// never-started one, and the leftover is the cleanup proposal `--dry-run` exists to promote
/// (`crates/tinio/tests/common/mod.rs`'s `crashed_root` is the Rust residue's spelling of it, and
/// the two must stay one fixture).
#[given("the root holds the private state a crashed run leaves")]
async fn doctor_crashed_root(world: &mut World) {
    let cli = harness(world);
    doctor_configure(cli);
    doctor_store(cli);
    let control = doctor_derived_control_name(cli);
    let dir = doctor_state_dir(cli);
    doctor_stale_state(&dir, cli.root(), &control);
    fs::create_dir_all(dir.join("tmp")).expect("the temp directory");
    fs::write(dir.join("tmp/leftover"), b"partial").expect("the leftover temp file");
}

/// A root with the complete configuration and nothing else — no store, no `state` file, no
/// residue. The store-less scenarios' own fixture, because the crashed root has a store on
/// purpose (a run that crashed had opened one).
#[given("a configured storage root")]
async fn doctor_configured_root(world: &mut World) {
    let cli = harness(world);
    doctor_configure(cli);
}

/// [`doctor_store`] as a step: the root a run has already opened and closed.
#[given("the root holds a store")]
async fn doctor_root_holds_a_store(world: &mut World) {
    let cli = harness(world);
    doctor_store(cli);
}

/// The socket fixture's own two steps, both built on [`doctor_stale_state`]: the `state` file and
/// the socket it names are one induction, so neither is a step of its own — a scenario that
/// planted a `state` file with no socket would be planting a channel the report probes and finds
/// silent, which is the crashed root's shape and not this one's.
///
/// The derived name first: the plain run's probe is the report's own `reclaim_stale(path, true)`
/// and the `--fix` run's is `reclaim_stale(path, false)`.
#[cfg(unix)]
#[given("the root's state dir holds a stale control socket")]
async fn doctor_stale_socket_at_derived(world: &mut World) {
    let cli = harness(world);
    let derived = PathBuf::from(doctor_derived_control_name(cli));
    let dir = doctor_state_dir(cli);
    doctor_stale_state(&dir, cli.root(), &derived.to_string_lossy());
    doctor_stale_socket(&derived);
}

/// The path rule's fixture: the `state` file's `control_name` is at a path that is **not**
/// `socket_path`'s output — the shape `[api.unix] path` produces, the documented remedy for a deep
/// root — with the stale socket at that same recorded path. A `--fix` that re-derived would leave
/// this socket behind while passing the derived-name scenario above.
#[cfg(unix)]
#[given(expr = "the root's state dir holds a stale control socket recorded at {string}")]
async fn doctor_stale_socket_recorded(world: &mut World, name: String) {
    let cli = harness(world);
    let dir = doctor_state_dir(cli);
    let recorded = dir.join(&name);
    doctor_stale_state(&dir, cli.root(), &recorded.to_string_lossy());
    doctor_stale_socket(&recorded);
}

/// The state dir a **read-only** run resolves: `TINIO_STATE_DIR`, the harness's relocated
/// directory — the only directory that mode's `doctor` reads, and the one its repairs write.
#[given("the relocated state dir holds a stale state file")]
async fn doctor_relocated_stale_state(world: &mut World) {
    let cli = harness(world);
    let control = doctor_derived_control_name(cli);
    let dir = cli.state.clone();
    fs::create_dir_all(&dir).expect("the relocated state directory");
    doctor_stale_state(&dir, cli.root(), &control);
}

/// The marker `needs_compact` reads besides the ratio, set through `Handle::mark_compact_needed`
/// — the marker protocol's own public write half rather than the state table's, because a fixture
/// must not be the second place the marker's storage shape is spelled out. A small temp-dir store
/// never crosses the 64 MiB floor on the ratio alone, so this is the only way to make one that
/// needs compaction.
#[given("the root's store needs compaction")]
async fn doctor_store_needs_compaction(world: &mut World) {
    let dir = doctor_state_dir(harness(world));
    let open = database::open(&dir).expect("the root's store");
    database::Handle::new(open.db)
        .mark_compact_needed(true)
        .await
        .expect("the compaction marker");
}

/// An **orphaned** `OBJECT_META` row: a recorded entry whose object file does not exist. Seeded
/// through `tinio_fs::meta::store` rather than a hand-written `meta.redb`, because a redb file is
/// not a shape a scenario can write (failure-handling.md §3 documents the *file* shapes; this one
/// is the store's).
#[given(expr = "the root's store holds an orphaned meta entry for {string} in the bucket {string}")]
async fn doctor_orphaned_meta(world: &mut World, key: String, bucket: String) {
    let dir = doctor_state_dir(harness(world));
    meta::store(&dir)
        .expect("the meta store")
        .set(
            &bucket_name(bucket).expect("a bucket name"),
            &object_key(key).expect("an object key"),
            &ETag::new("d41d8cd98f00b204e9800998ecf8427e").expect("an etag"),
            4,
            SystemTime::UNIX_EPOCH,
            0,
        )
        .await
        .expect("the orphaned meta row");
}

/// A **stale bucket record**: a `BUCKETS` row whose bucket directory does not exist.
#[given(expr = "the root's store holds a stale bucket record for {string}")]
async fn doctor_stale_bucket_record(world: &mut World, bucket: String) {
    let dir = doctor_state_dir(harness(world));
    bucket::store(&dir)
        .expect("the bucket store")
        .record(
            &bucket_name(bucket).expect("a bucket name"),
            SystemTime::now(),
        )
        .await
        .expect("the stale bucket record");
}

/// A multipart subtree whose bucket directory no longer exists — the bucket-orphaned stage's own
/// defect, which has no idle grace (an upload directory without a `UPLOADS` record does, and a
/// fresh fixture would survive it).
#[given(expr = "the root holds an abandoned multipart subtree for the missing bucket {string}")]
async fn doctor_abandoned_multipart(world: &mut World, bucket: String) {
    let dir = doctor_state_dir(harness(world));
    let subtree = dir.join("multipart").join(&bucket).join("u1");
    fs::create_dir_all(&subtree).expect("the multipart subtree");
    fs::write(subtree.join("part-1"), b"x").expect("the abandoned part");
}

/// The document a run **newer** than this build leaves: everything needed to dial it is still
/// there — the recorded `control_name` above all — and only the version is foreign. Read and
/// rewritten through the plane's own type, so the fixture cannot drift from the field names the
/// probe reads.
#[given("the root's state file records a version this build does not read")]
async fn doctor_foreign_version_state(world: &mut World) {
    let cli = cli(world);
    let dir = cli.state_dir();
    let mut state = State::read_any_version(&dir)
        .expect("the `state` file")
        .expect("the server this scenario is serving published a `state` file");
    state.version = STATE_VERSION + 1;
    state
        .write(&dir)
        .unwrap_or_else(|err| panic!("the foreign-version state file: {err}"));
}

/// The induction the failed-repair scenario rests on: the stale temp file is held open with **no
/// sharing at all** — Windows refuses to unlink a file opened that way, where a plain
/// `File::open` shares delete and would let the stage succeed. The handle is parked on the harness
/// because dropping it here would drop the fixture with it.
#[cfg(windows)]
#[given("the leftover temp file refuses to be removed")]
async fn doctor_leftover_refuses_removal(world: &mut World) {
    use std::os::windows::fs::OpenOptionsExt as _;

    let cli = cli(world);
    let path = cli.state_dir().join("tmp/leftover");
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap_or_else(|err| panic!("holding `{}` with no sharing: {err}", path.display()));
    cli.held.push(held);
}

/// The fixture change the `--dry-run` scenario makes between its two runs: the stale `state` file
/// is a defect that scenario is not about, and the state/socket leg reports it whichever mode
/// runs. Removing it leaves a root whose only issue is the cleanup proposal.
#[when("the root's state file is removed")]
async fn doctor_remove_state_file(world: &mut World) {
    let path = cli(world).state_dir().join(STATE_FILE);
    fs::remove_file(&path).unwrap_or_else(|err| panic!("removing `{}`: {err}", path.display()));
}

/// The report's rows that are not `ok`, **excluding `free space` and `host confinement`** — the
/// scenarios' answer to "was this root reported clean?".
///
/// `free space` reads the **host's** volume, so a machine below doctor's own floor
/// (`max(1 GiB, 5%)` of the filesystem) answers `warn` on a root that has nothing wrong with it.
/// Asserting the whole report clean would make every "clean root" scenario a function of the CI
/// disk's free space — which is exactly what happened on a 1.9 TB volume down to its last 3%.
/// The floor's two arms are pinned with explicit numbers in the unit tests
/// (`crates/tinio-cli/src/commands/doctor/tests.rs`), and this step is about the root.
///
/// `host confinement` is the second row that reads the **host** rather than the root: it asks
/// whether this machine's token can create an AppContainer profile, so a Low-integrity session,
/// a service account or an AppContainer-hostile runner answers `warn` on a root that has nothing
/// wrong with it, and asserting the whole report clean would make every "clean root" scenario a
/// function of the machine. Its two arms are pinned in the same unit file, by
/// `host_confinement_finding`'s own tests, so nothing about the root is hidden here either.
#[then("the report holds no problem other than the host's disk")]
async fn report_holds_no_problem(world: &mut World) {
    let stdout = &cli(world).last().stdout;
    let problems: Vec<&str> = stdout
        .lines()
        .filter(|line| {
            doctor_row(line).is_some_and(|(severity, check)| {
                severity != "ok" && check != "free space" && check != "host confinement"
            })
        })
        .collect();
    assert!(
        problems.is_empty(),
        "the report holds {} problem(s): {problems:?}\n{stdout}",
        problems.len()
    );
}

/// The run's stdout does **not** carry `text` — the absence half of a rule, from a plain run that
/// must not claim a repair it never ran to a rendering that must not quote what it read.
#[then(expr = "its stdout does not contain {string}")]
async fn stdout_does_not_contain(world: &mut World, text: String) {
    let stdout = &cli(world).last().stdout;
    assert!(
        !stdout.contains(&text),
        "stdout carries {text:?}:\n{stdout}"
    );
}

/// The same for the other stream: the doctor's failures ride the report, so a sentence that
/// belongs to a finding must not appear on stderr instead.
#[then(expr = "its stderr does not contain {string}")]
async fn stderr_does_not_contain(world: &mut World, text: String) {
    let stderr = cli(world).last().stderr();
    assert!(
        !stderr.contains(&text),
        "stderr carries {text:?}:\n{stderr}"
    );
}

/// A file the harness's placeholders name, with exactly this content — the read-back that proves
/// a repair did what its report says and a plain run did nothing at all. `{root}`, `{state}` and
/// `{scratch}` expand as they do on a command line ([`expand`]).
///
/// **The three file steps are registered for `Given` as well as `Then`**, because one of them
/// states a fixture rather than a result: the store-less `--fix` scenario has to say that its root
/// starts with no store, and a Gherkin `And` after a `Given` is a `Given`. The registration pairs
/// the two readings of one fact rather than adding a second sentence for the fixture's half;
/// `steps/acl.rs`'s `I send a "…" request …` is the suite's other multi-keyword step.
#[given(expr = "the file {string} holds {string}")]
#[then(expr = "the file {string} holds {string}")]
async fn file_holds(world: &mut World, path: String, text: String) {
    let path = expand(cli(world), &path);
    let found = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading `{}`: {err}", path.display()));
    assert_eq!(found, text, "`{}` does not hold {text:?}", path.display());
}

/// A file the harness's placeholders name is there at all — the shape a binary artifact needs,
/// where [`file_holds`] cannot be used because the content is not text.
#[given(expr = "the file {string} exists")]
#[then(expr = "the file {string} exists")]
async fn file_exists_step(world: &mut World, path: String) {
    let path = expand(cli(world), &path);
    assert!(path.is_file(), "`{}` is not a file", path.display());
}

/// Nothing is there: a reclaimed socket, a reclaimed `state` file, a store a diagnostics pass
/// must not have created.
#[given(expr = "the file {string} does not exist")]
#[then(expr = "the file {string} does not exist")]
async fn file_does_not_exist_step(world: &mut World, path: String) {
    let path = expand(cli(world), &path);
    assert!(!path.exists(), "`{}` exists", path.display());
}

/// The report names the **relocated** state dir — the answer to "which directory did this
/// read-only run diagnose", which no re-derivation on this side can carry.
#[then("its stdout names the relocated state dir")]
async fn stdout_names_relocated_state_dir(world: &mut World) {
    let cli = cli(world);
    let named = cli.state.display().to_string();
    let stdout = &cli.last().stdout;
    assert!(
        stdout.contains(&named),
        "the report names the state dir that answered (`{named}`):\n{stdout}"
    );
}

/// The derived control-channel name is in the report — the path a scenario cannot spell, because
/// it is a digest of the canonical root the harness's scratch tree determines.
#[cfg(unix)]
#[then("its stdout names the derived control socket path")]
async fn stdout_names_derived_socket(world: &mut World) {
    let cli = cli(world);
    let derived = doctor_derived_control_name(cli);
    let stdout = &cli.last().stdout;
    assert!(
        stdout.contains(&derived),
        "the report probes the derived path (`{derived}`):\n{stdout}"
    );
}

/// The derived socket file is there — a plain run probes and leaves it alone.
#[cfg(unix)]
#[then("the derived control socket exists")]
async fn derived_socket_exists(world: &mut World) {
    let path = PathBuf::from(doctor_derived_control_name(cli(world)));
    assert!(path.exists(), "`{}` was not left in place", path.display());
}

/// And it is gone (or never was): `--fix` reclaims it, and a `--fix` that re-derived leaves the
/// recorded one behind instead.
#[cfg(unix)]
#[then("the derived control socket does not exist")]
async fn derived_socket_absent(world: &mut World) {
    let path = PathBuf::from(doctor_derived_control_name(cli(world)));
    assert!(!path.exists(), "`{}` is still there", path.display());
}

/// The other half of [`stdout_names_derived_socket`], for the scenario whose channel is **not**
/// the derived one: a report that named both paths would satisfy the positive assertion beside it
/// while saying nothing about which socket was planted, and the point of that leg is that the
/// recorded name is the one the probe and the reclaim both speak about.
#[cfg(unix)]
#[then("its stdout does not name the derived control socket path")]
async fn stdout_omits_derived_socket(world: &mut World) {
    let cli = cli(world);
    let derived = doctor_derived_control_name(cli);
    let stdout = &cli.last().stdout;
    assert!(
        !stdout.contains(&derived),
        "the report names the derived path (`{derived}`), which is not the socket the fixture \
         planted:\n{stdout}"
    );
}

/// V5/R28: a read-only client that only *probes* derives `<home>/.tinio/roots/<hash>/` and must
/// not create it. `HOME`/`USERPROFILE` are the scenario's scratch directory for every child
/// ([`Cli::command`]), so the derived tree is the scratch dir's own `.tinio` — the one directory a
/// run could otherwise mint in a real home.
#[then("the home state dir was not created")]
async fn home_state_dir_not_created(world: &mut World) {
    let dir = cli(world).scratch().join(".tinio");
    assert!(
        !dir.exists(),
        "the run minted `{}` — the home-derived state tree a read-only client must not create",
        dir.display()
    );
}

/// The fixture really carries the token the redaction scenario asserts is never printed — without
/// this the two absences would pass over a `state` file that held no token at all.
#[then("the root's state file carries the fixture's token")]
async fn state_file_carries_token(world: &mut World) {
    let path = cli(world).state_dir().join(STATE_FILE);
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("reading `{}`: {err}", path.display()));
    assert!(
        text.contains(DOCTOR_TOKEN),
        "the fixture's token is what makes the redaction assertion witnessable, and `{}` does not carry it:\n{text}",
        path.display()
    );
}

/// The `--json` document's own severity for the run ([`report_severity`] reads the field).
#[then(expr = "the JSON report's severity is {word}")]
async fn json_report_severity_is(world: &mut World, severity: String) {
    let stdout = &cli(world).last().stdout;
    assert_eq!(
        report_severity(stdout),
        severity,
        "the document's own severity:\n{stdout}"
    );
}

/// Some rendered row carries `text` at `severity` — the relation a report's failure has to the
/// stage that raised it, which plain containment cannot state: the sentence being somewhere is
/// not the sentence being an error finding.
///
/// **One expression covers both English articles**, because the article is a function of the
/// severity word a scenario spells: `an error finding` and `a warn finding` are both English, and
/// a cucumber expression matches its literal text around a placeholder. `a(n)` is
/// cucumber-expressions' optional text (its grammar's `I have {int} cucumber(s)`), which expands
/// to `a(?:n)?`; the two registrations the two spellings used to need — one delegating to the
/// other — are one step again.
#[then(expr = "the report holds a(n) {word} finding carrying {string}")]
async fn report_holds_finding(world: &mut World, severity: String, text: String) {
    let stdout = &cli(world).last().stdout;
    let found = stdout.lines().any(|line| {
        doctor_row(line).is_some_and(|(word, _)| word == severity) && line.contains(&text)
    });
    assert!(found, "no `{severity}` finding carries {text:?}:\n{stdout}");
}

/// No row for `check` carries `text` — the other half of the rule above, and the one that keeps a
/// failed stage from being buried in the ok summary. A report with no such row at all is a
/// failure rather than a pass, so the absence can never be the absence of the row.
#[then(expr = "the report's {string} row does not carry {string}")]
async fn report_row_does_not_carry(world: &mut World, check: String, text: String) {
    let stdout = &cli(world).last().stdout;
    let rows: Vec<&str> = stdout
        .lines()
        .filter(|line| doctor_row(line).is_some_and(|(_, name)| name == check))
        .collect();
    assert!(
        !rows.is_empty(),
        "the report holds no `{check}` row at all:\n{stdout}"
    );
    assert!(
        rows.iter().all(|row| !row.contains(&text)),
        "a `{check}` row carries {text:?}:\n{stdout}"
    );
}

/// `--fix`'s own meta-store leg: `compact_if_needed` runs inside the store open's fix pass, so
/// the re-check sees a clean store and the marker is cleared. Read back through the store, not
/// through the report that claims it.
#[then("the root's store no longer needs compaction")]
async fn store_needs_no_compaction(world: &mut World) {
    let dir = doctor_state_dir(harness(world));
    let open = database::open(&dir).expect("the root's store");
    assert!(
        !open.compact_needed,
        "`--fix` compacted the store and cleared the marker"
    );
}

/// The orphan row is gone, read back through the store that owns the row.
#[then(expr = "the root's store holds no meta entry for {string} in the bucket {string}")]
async fn store_has_no_meta_entry(world: &mut World, key: String, bucket: String) {
    let dir = doctor_state_dir(harness(world));
    let row = meta::store(&dir)
        .expect("the meta store")
        .get(
            &bucket_name(bucket).expect("a bucket name"),
            &object_key(key).expect("an object key"),
        )
        .await
        .expect("the meta row");
    assert!(row.is_none(), "the orphan row is reclaimed: {row:?}");
}

/// The stale `BUCKETS` row is pruned, read back through the store's own listing.
#[then(expr = "the root's store holds no bucket record for {string}")]
async fn store_has_no_bucket_record(world: &mut World, bucket: String) {
    let dir = doctor_state_dir(harness(world));
    let rows = bucket::store(&dir)
        .expect("the bucket store")
        .load_all()
        .await
        .expect("the bucket rows");
    assert!(
        !rows.iter().any(|(name, _)| name == &bucket),
        "the stale record is pruned: {rows:?}"
    );
}
