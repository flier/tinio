//! Generate the PostgreSQL statement cache and reconcile it with `.sqlx/`.
//!
//! Generation is the driver's own prepare path, driven without `cargo-sqlx`:
//! `cargo check -p tinio-store-pg --all-targets` with `SQLX_OFFLINE=false` and a
//! fresh `SQLX_OFFLINE_DIR`, so each statement is described and written as
//! `query-<hash>.json` into that scratch directory. Cargo does not track
//! `SQLX_OFFLINE_DIR`, so an already-fresh crate skips the driver's macros and
//! leaves the scratch empty; that case is detected and retried after
//! `cargo clean -p tinio-store-pg`. `sqlx check` compares the generated set
//! with the cache and never writes to it; `sqlx update` makes the cache exactly
//! the generated set, and touches it only after generation succeeds.
//!
//! A generated entry is the driver's inference about the **live database**, not
//! a function of the SQL text alone: a column's `nullable` comes from
//! `pg_attribute.attnotnull`, patched from the plan the server gives the
//! statement, so a cache recorded against another shape — an older fixture
//! volume, a column that has since gained `NOT NULL` — re-reads as `changed`
//! for a statement whose SQL never moved. That is the drift this mode exists to
//! report: re-record it with `sqlx update` against the fixture the gate runs,
//! rather than editing the entry or the comparison to match.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
    process::{Command, ExitCode, Output},
};

use tinio_util::path::contains_under_any_spelling_either_way;

use crate::{
    Error,
    cli::{Mode, SqlxCommand},
};

/// Run the `sqlx` subcommand.
pub(crate) fn run(command: SqlxCommand) -> Result<ExitCode, Error> {
    let mode = command.mode();
    let (SqlxCommand::Check(args) | SqlxCommand::Update(args)) = command;
    let root = workspace_root()?;
    let dsn = resolve_dsn(args.dsn)?;
    let cache_dir = resolve_dir(args.cache_dir, root.join(".sqlx"))?;
    let scratch_dir = resolve_dir(args.scratch_dir, root.join("target/tmp/sqlx-cache"))?;
    guard_paths(&cache_dir, &scratch_dir)?;

    generate(&root, &dsn, &scratch_dir)?;
    let generated = read_entries(&scratch_dir).map_err(Error::Runtime)?;
    let cached = read_entries(&cache_dir).map_err(Error::Runtime)?;
    let diff = Diff::between(&generated, &cached);
    match mode {
        Mode::Check => Ok(report(&generated, &diff)),
        Mode::Write => write(&generated, &cache_dir, &diff),
    }
}

/// The workspace root: the parent of this crate's manifest directory.
fn workspace_root() -> Result<PathBuf, Error> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir.parent().map(Path::to_path_buf).ok_or_else(|| {
        Error::Runtime(format!(
            "xtask manifest directory has no parent: {}",
            manifest_dir.display()
        ))
    })
}

/// Resolve the dsn from `--dsn`, then `$DATABASE_URL`, then `$TINIO_PG_DSN`.
fn resolve_dsn(explicit: Option<String>) -> Result<String, Error> {
    resolve_dsn_from(
        explicit,
        env::var("DATABASE_URL").ok(),
        env::var("TINIO_PG_DSN").ok(),
    )
}

/// The dsn precedence rule over already-read values: `--dsn`, then
/// `$DATABASE_URL`, then `$TINIO_PG_DSN`. Pure, so tests never touch the
/// process-wide environment.
fn resolve_dsn_from(
    explicit: Option<String>,
    database_url: Option<String>,
    tinio_pg_dsn: Option<String>,
) -> Result<String, Error> {
    if let Some(dsn) = explicit
        && !dsn.is_empty()
    {
        return Ok(dsn);
    }
    for value in [database_url, tinio_pg_dsn] {
        if let Some(value) = value
            && !value.is_empty()
        {
            return Ok(value);
        }
    }
    Err(Error::Usage(
        "no dsn: pass --dsn, or set DATABASE_URL, then TINIO_PG_DSN".to_owned(),
    ))
}

/// Resolve a directory option, defaulting beside the workspace root.
fn resolve_dir(explicit: Option<PathBuf>, default: PathBuf) -> Result<PathBuf, Error> {
    match explicit {
        Some(path) => absolute(&path),
        None => Ok(default),
    }
}

/// Make a path absolute; which directory a spelling names is `guard_paths`'
/// question, not this one's.
fn absolute(path: &Path) -> Result<PathBuf, Error> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    env::current_dir()
        .map(|dir| dir.join(path))
        .map_err(|e| Error::Runtime(format!("cannot read current directory: {e}")))
}

/// Refuse overlapping scratch and cache paths before either is touched.
///
/// The containment rule is `tinio_util::path`'s, asked both ways and under
/// any spelling of either side. The copy this replaces compared
/// `Path::starts_with` over case-folded `to_string_lossy` spellings, so a
/// `\\?\`-prefixed (or non-Unicode) spelling of one directory read as a
/// different one; `path_contains` is the one definer of the question, and
/// this guard is one of its callers.
fn guard_paths(cache_dir: &Path, scratch_dir: &Path) -> Result<(), Error> {
    let (cache_within_scratch, scratch_within_cache) =
        contains_under_any_spelling_either_way(cache_dir, scratch_dir);
    if cache_within_scratch || scratch_within_cache {
        return Err(Error::Usage(format!(
            "scratch dir and cache dir overlap: {} / {}",
            scratch_dir.display(),
            cache_dir.display()
        )));
    }
    Ok(())
}

/// Run the driver's prepare build into the scratch directory.
fn generate(root: &Path, dsn: &str, scratch_dir: &Path) -> Result<(), Error> {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
    clear_scratch(scratch_dir)?;
    run_check(&cargo, root, dsn, scratch_dir)?;
    if !read_entries(scratch_dir)
        .map_err(Error::Runtime)?
        .is_empty()
    {
        return Ok(());
    }

    eprintln!("no entries generated; forcing a rebuild of tinio-store-pg");
    clean_package(&cargo, root)?;
    clear_scratch(scratch_dir)?;
    run_check(&cargo, root, dsn, scratch_dir)
}

/// Run `cargo check -p tinio-store-pg --all-targets` with the prepare env.
fn run_check(cargo: &str, root: &Path, dsn: &str, scratch_dir: &Path) -> Result<(), Error> {
    eprintln!("generating sqlx cache: cargo check -p tinio-store-pg --all-targets");
    let output = Command::new(cargo)
        .arg("check")
        .arg("-p")
        .arg("tinio-store-pg")
        .arg("--all-targets")
        .current_dir(root)
        .env("DATABASE_URL", dsn)
        .env("SQLX_OFFLINE", "false")
        .env("SQLX_OFFLINE_DIR", scratch_dir)
        .output()
        .map_err(|e| Error::Runtime(format!("cannot run `{cargo}`: {e}")))?;
    ensure_success(output, "`cargo check -p tinio-store-pg --all-targets`")
}

/// Drop the crate's build artifacts so its macros re-expand on the next check.
fn clean_package(cargo: &str, root: &Path) -> Result<(), Error> {
    let output = Command::new(cargo)
        .arg("clean")
        .arg("-p")
        .arg("tinio-store-pg")
        .current_dir(root)
        .output()
        .map_err(|e| Error::Runtime(format!("cannot run `{cargo} clean`: {e}")))?;
    ensure_success(output, "`cargo clean -p tinio-store-pg`")
}

/// Turn a failed child process into an error carrying its captured output.
fn ensure_success(output: Output, what: &str) -> Result<(), Error> {
    if output.status.success() {
        return Ok(());
    }
    Err(Error::Runtime(format!(
        "{what} failed with {}\n{}{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )))
}

/// Empty the scratch directory of generated entries, then ensure it exists.
fn clear_scratch(dir: &Path) -> Result<(), Error> {
    match fs::read_dir(dir) {
        Ok(entries) => {
            for entry in entries {
                let entry = entry
                    .map_err(|e| Error::Runtime(format!("cannot read {}: {e}", dir.display())))?;
                let path = entry.path();
                if path.extension().and_then(OsStr::to_str) == Some("json") {
                    fs::remove_file(&path).map_err(|e| {
                        Error::Runtime(format!("cannot clear {}: {e}", path.display()))
                    })?;
                }
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => {
            return Err(Error::Runtime(format!(
                "cannot read {}: {e}",
                dir.display()
            )));
        }
    }
    fs::create_dir_all(dir)
        .map_err(|e| Error::Runtime(format!("cannot create {}: {e}", dir.display())))
}

/// Read the `*.json` entries of a cache-like directory, keyed by file name.
fn read_entries(dir: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let mut entries = BTreeMap::new();
    let list = match fs::read_dir(dir) {
        Ok(list) => list,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(entries),
        Err(e) => return Err(format!("cannot read {}: {e}", dir.display())),
    };
    for item in list {
        let item = item.map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
        let path = item.path();
        if !path.is_file() || path.extension().and_then(OsStr::to_str) != Some("json") {
            continue;
        }
        let bytes = fs::read(&path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        entries.insert(item.file_name().to_string_lossy().into_owned(), bytes);
    }
    Ok(entries)
}

/// The change set between the generated entries and the committed cache.
struct Diff {
    added: Vec<String>,
    changed: Vec<String>,
    removed: Vec<String>,
    unchanged: usize,
}

impl Diff {
    /// Classify every generated and cached entry by name and content.
    fn between(generated: &BTreeMap<String, Vec<u8>>, cached: &BTreeMap<String, Vec<u8>>) -> Self {
        let mut added = Vec::new();
        let mut changed = Vec::new();
        let mut unchanged = 0;
        for (name, bytes) in generated {
            match cached.get(name) {
                None => added.push(name.clone()),
                Some(existing) if existing == bytes => unchanged += 1,
                Some(_) => changed.push(name.clone()),
            }
        }
        let removed = cached
            .keys()
            .filter(|name| !generated.contains_key(*name))
            .cloned()
            .collect();
        Self {
            added,
            changed,
            removed,
            unchanged,
        }
    }

    /// Whether the cache differs from the generated set at all.
    fn drift(&self) -> bool {
        !self.added.is_empty() || !self.changed.is_empty() || !self.removed.is_empty()
    }
}

/// Compare the generated set with the cache; report drift and pick the exit code.
fn report(generated: &BTreeMap<String, Vec<u8>>, diff: &Diff) -> ExitCode {
    if !diff.drift() {
        println!("sqlx cache is up to date ({} entries)", generated.len());
        return ExitCode::SUCCESS;
    }
    println!(
        "sqlx cache is not up to date: missing {}, extra {}, changed {}",
        diff.added.len(),
        diff.removed.len(),
        diff.changed.len()
    );
    for name in &diff.added {
        println!("  missing {name}");
    }
    for name in &diff.removed {
        println!("  extra {name}");
    }
    for name in &diff.changed {
        println!("  changed {name}");
    }
    ExitCode::from(1)
}

/// Make the cache exactly the generated set, one line per change.
fn write(
    generated: &BTreeMap<String, Vec<u8>>,
    cache_dir: &Path,
    diff: &Diff,
) -> Result<ExitCode, Error> {
    if generated.is_empty() && !diff.removed.is_empty() {
        return Err(Error::Runtime(
            "generation produced no entries; refusing to rewrite the cache".to_owned(),
        ));
    }
    fs::create_dir_all(cache_dir)
        .map_err(|e| Error::Runtime(format!("cannot create {}: {e}", cache_dir.display())))?;
    for name in &diff.added {
        write_entry(generated, name, cache_dir)?;
        println!("added {name}");
    }
    for name in &diff.changed {
        write_entry(generated, name, cache_dir)?;
        println!("changed {name}");
    }
    for name in &diff.removed {
        let path = cache_dir.join(name);
        fs::remove_file(&path)
            .map_err(|e| Error::Runtime(format!("cannot remove {}: {e}", path.display())))?;
        println!("removed {name}");
    }
    println!(
        "added {}, changed {}, removed {}, unchanged {}",
        diff.added.len(),
        diff.changed.len(),
        diff.removed.len(),
        diff.unchanged
    );
    Ok(ExitCode::SUCCESS)
}

/// Write one generated entry to its name in the cache.
fn write_entry(
    generated: &BTreeMap<String, Vec<u8>>,
    name: &str,
    cache_dir: &Path,
) -> Result<(), Error> {
    let bytes = generated
        .get(name)
        .ok_or_else(|| Error::Runtime(format!("generated entry `{name}` vanished")))?;
    let path = cache_dir.join(name);
    fs::write(&path, bytes)
        .map_err(|e| Error::Runtime(format!("cannot write {}: {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::Path};

    use super::{Diff, guard_paths, resolve_dsn_from};
    use crate::Error;

    /// A cache-like map from a list of `(name, bytes)` pairs.
    fn entries(pairs: &[(&str, &[u8])]) -> BTreeMap<String, Vec<u8>> {
        pairs
            .iter()
            .map(|(name, bytes)| ((*name).to_owned(), bytes.to_vec()))
            .collect()
    }

    #[test]
    fn explicit_dsn_wins_over_the_environment() {
        let resolved = resolve_dsn_from(
            Some("explicit".to_owned()),
            Some("database-url".to_owned()),
            Some("tinio-pg-dsn".to_owned()),
        )
        .expect("a dsn resolves");
        assert_eq!(resolved, "explicit");
    }

    #[test]
    fn database_url_wins_over_tinio_pg_dsn() {
        let resolved = resolve_dsn_from(
            None,
            Some("database-url".to_owned()),
            Some("tinio-pg-dsn".to_owned()),
        )
        .expect("a dsn resolves");
        assert_eq!(resolved, "database-url");
    }

    #[test]
    fn tinio_pg_dsn_is_the_last_fallback() {
        let resolved =
            resolve_dsn_from(None, None, Some("tinio-pg-dsn".to_owned())).expect("a dsn resolves");
        assert_eq!(resolved, "tinio-pg-dsn");
    }

    #[test]
    fn an_empty_explicit_dsn_falls_through() {
        let resolved = resolve_dsn_from(Some(String::new()), Some("database-url".to_owned()), None)
            .expect("a dsn resolves");
        assert_eq!(resolved, "database-url");
    }

    #[test]
    fn an_empty_database_url_falls_through() {
        let resolved = resolve_dsn_from(None, Some(String::new()), Some("tinio-pg-dsn".to_owned()))
            .expect("a dsn resolves");
        assert_eq!(resolved, "tinio-pg-dsn");
    }

    #[test]
    fn all_dsn_sources_absent_is_a_usage_error() {
        let error = resolve_dsn_from(None, None, None).expect_err("no dsn can resolve");
        assert!(matches!(&error, Error::Usage(_)));
        assert_eq!(
            error.to_string(),
            "usage: no dsn: pass --dsn, or set DATABASE_URL, then TINIO_PG_DSN"
        );
    }

    #[test]
    fn all_dsn_sources_empty_is_a_usage_error() {
        let error = resolve_dsn_from(
            Some(String::new()),
            Some(String::new()),
            Some(String::new()),
        )
        .expect_err("no non-empty dsn can resolve");
        assert!(matches!(&error, Error::Usage(_)));
    }

    /// The lexical half of the shared rule, which the guard's own `normalize`
    /// used to carry: a `..` or `.` that spells the same directory still
    /// overlaps. The first pair is the exploit in miniature -- the scratch
    /// directory is written with a `..` that climbs back over its own name.
    #[test]
    fn guard_paths_resolves_a_lexical_spelling() {
        let cache = Path::new("work/cache");
        for spelled in ["work/other/../cache/scratch", "work/./cache/scratch"] {
            assert!(
                guard_paths(cache, Path::new(spelled)).is_err(),
                "{spelled} names a scratch directory inside the cache"
            );
            assert!(guard_paths(Path::new(spelled), cache).is_err());
        }
    }

    /// The documented misuse `--check --cache-dir` is run against: a copy
    /// beside the cache. The shared rule is component-wise, never a string
    /// prefix, so a sibling whose name merely starts with the cache's stays
    /// disjoint.
    #[test]
    fn guard_paths_accepts_a_copy_beside_the_cache() {
        let cache = Path::new("work/.sqlx");
        let copy = Path::new("work/.sqlx-backup");
        assert!(guard_paths(cache, copy).is_ok());
        assert!(guard_paths(copy, cache).is_ok());
    }

    #[test]
    fn guard_paths_rejects_a_scratch_inside_the_cache() {
        let cache = Path::new("/tmp/work/cache");
        let scratch = Path::new("/tmp/work/cache/scratch");
        assert!(guard_paths(cache, scratch).is_err());
    }

    #[test]
    fn guard_paths_rejects_a_cache_inside_the_scratch() {
        let cache = Path::new("/tmp/work/scratch/cache");
        let scratch = Path::new("/tmp/work/scratch");
        assert!(guard_paths(cache, scratch).is_err());
    }

    #[test]
    fn guard_paths_accepts_disjoint_directories() {
        assert!(guard_paths(Path::new("/tmp/cache"), Path::new("/tmp/scratch")).is_ok());
    }

    #[test]
    fn guard_paths_reports_the_overlap_as_usage() {
        let error = guard_paths(Path::new("/tmp/cache"), Path::new("/tmp/cache/scratch"))
            .expect_err("an overlap is rejected");
        assert!(matches!(&error, Error::Usage(message) if message.contains("overlap")));
    }

    /// NTFS resolves the two spellings of `C:/Work/Cache` to one directory,
    /// so a cache and a scratch directory that differ only in case still
    /// overlap. The fold is the shared rule's (unix filesystems distinguish
    /// them, and there the pair is disjoint).
    #[test]
    fn guard_paths_folds_case() {
        let overlap = guard_paths(
            Path::new("C:/Work/Cache"),
            Path::new("c:/work/cache/scratch"),
        );
        if cfg!(windows) {
            assert!(overlap.is_err());
        } else {
            assert!(overlap.is_ok());
        }
    }

    /// **The spelling the copy this guard replaced could not pair.**
    /// `canonicalize` answers `\\?\D:\work\cache` while an operator writes
    /// `D:\work\cache`, and the two name one directory, so either side can
    /// arrive verbatim and the overlap is still an overlap. The sibling
    /// assertion keeps the leg a comparison rather than a blanket `true`.
    #[cfg(windows)]
    #[test]
    fn guard_paths_pairs_a_verbatim_spelling() {
        assert!(
            guard_paths(
                Path::new(r"\\?\D:\work\cache"),
                Path::new(r"D:\work\cache\scratch")
            )
            .is_err()
        );
        assert!(
            guard_paths(
                Path::new(r"D:\work\cache"),
                Path::new(r"\\?\D:\work\cache\scratch")
            )
            .is_err()
        );
        assert!(
            guard_paths(
                Path::new(r"\\?\D:\work\cache"),
                Path::new(r"D:\work\cache2")
            )
            .is_ok()
        );
    }

    /// **A spelling that is not text is a spelling too.** A Windows path is
    /// arbitrary UTF-16, so a cache whose name holds an unpaired surrogate
    /// has a verbatim and a plain spelling like any other; the shared rule
    /// reads the wide units rather than `to_str` to pair them, while the
    /// `to_string_lossy` fold this guard replaced saw two directories.
    #[cfg(windows)]
    #[test]
    fn guard_paths_pairs_a_spelling_that_is_not_text() {
        use std::{ffi::OsString, os::windows::ffi::OsStringExt, path::PathBuf};

        let mut units: Vec<u16> = r"D:\work\cache\".encode_utf16().collect();
        units.push(0xD800);
        let plain = PathBuf::from(OsString::from_wide(&units));
        let scratch = plain.join("scratch");
        let mut verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
        verbatim.extend_from_slice(&units);
        let verbatim = PathBuf::from(OsString::from_wide(&verbatim));

        assert!(
            plain.to_str().is_none(),
            "the leg is vacuous unless the path really is not text: {plain:?}"
        );
        assert!(guard_paths(&verbatim, &scratch).is_err());
        assert!(guard_paths(&scratch, &verbatim).is_err());
    }

    #[test]
    fn diff_classifies_every_kind() {
        let generated = entries(&[
            ("changed.json", b"new"),
            ("new.json", b"n"),
            ("same.json", b"s"),
        ]);
        let cached = entries(&[
            ("changed.json", b"old"),
            ("gone.json", b"g"),
            ("same.json", b"s"),
        ]);
        let diff = Diff::between(&generated, &cached);
        assert_eq!(diff.added, vec!["new.json".to_owned()]);
        assert_eq!(diff.changed, vec!["changed.json".to_owned()]);
        assert_eq!(diff.removed, vec!["gone.json".to_owned()]);
        assert_eq!(diff.unchanged, 1);
        assert!(diff.drift());
    }

    #[test]
    fn diff_of_identical_sets_has_no_drift() {
        let both = entries(&[("a.json", b"a"), ("b.json", b"b")]);
        let diff = Diff::between(&both, &both);
        assert!(diff.added.is_empty());
        assert!(diff.changed.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.unchanged, 2);
        assert!(!diff.drift());
    }

    #[test]
    fn diff_of_empty_generated_is_all_removed() {
        let cached = entries(&[("a.json", b"a")]);
        let diff = Diff::between(&BTreeMap::new(), &cached);
        assert!(diff.added.is_empty());
        assert!(diff.changed.is_empty());
        assert_eq!(diff.removed, vec!["a.json".to_owned()]);
        assert_eq!(diff.unchanged, 0);
        assert!(diff.drift());
    }

    #[test]
    fn diff_against_an_empty_cache_is_all_added() {
        let generated = entries(&[("a.json", b"a"), ("b.json", b"b")]);
        let diff = Diff::between(&generated, &BTreeMap::new());
        assert_eq!(diff.added, vec!["a.json".to_owned(), "b.json".to_owned()]);
        assert!(diff.changed.is_empty());
        assert!(diff.removed.is_empty());
        assert_eq!(diff.unchanged, 0);
        assert!(diff.drift());
    }
}
