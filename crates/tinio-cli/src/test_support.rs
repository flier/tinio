//! Test-only construction helpers for the command modules.
//!
//! `doctor`'s check list is a function of a `Resolution` and a chosen state dir, and the unit
//! tests drive it against a temp root rather than a binary: the seeders below write a root and
//! `run_checks` resolves it, picks the state-dir candidate and runs the checks. It exists so the
//! check list is testable without a process, and it is `#[cfg(test)]` because nothing in the
//! shipped binary needs it.

#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::parser::ValueSource;

use crate::{
    cli::{Discovery, Provenance},
    commands::{
        DirArgs, client_resolution,
        doctor::{Proposals, Run, StateDir, checks, state_dir_for_run},
    },
    report::Report,
    resolve::{self, Resolution},
};

/// Runs the check list against `root` as a `doctor` run would: the resolution, the chosen state
/// dir with its one probe, then `checks`.
pub(crate) async fn run_checks(root: &Path, read_only: bool, proposals: Proposals) -> Report {
    // Both graphs produce the pair through the shared client resolution, but only the api graph
    // selects through it — the no-api `chosen_state_dir` takes the resolution alone — so the pair
    // is dropped at the binding rather than bound and ignored behind a lint suppression.
    #[cfg(feature = "api")]
    let (resolution, candidates) = resolve_test(root, read_only);
    #[cfg(not(feature = "api"))]
    let (resolution, _) = resolve_test(root, read_only);
    #[cfg(feature = "api")]
    let state = chosen_state_dir(&resolution, &candidates).await;
    #[cfg(not(feature = "api"))]
    let state = chosen_state_dir(&resolution);
    // The fixture resolves a filesystem root, so the pg leg (and the root-skipping it drives)
    // stays out of the run entirely — the fs path is the one it pins.
    checks(
        &Run {
            resolution: &resolution,
            state: &state,
        },
        proposals,
    )
    .await
}

/// The resolution `doctor` runs on and the candidate pair its client probes — taken from the
/// shared **client resolution** ([`client_resolution`](crate::commands::client_resolution)),
/// which is gate-independent so both feature graphs compile it, rather than
/// re-derived here: the fixture cannot then pin a pair the command never produces.
///
/// `TINIO_STATE_DIR` is part of the shape rather than a convenience: without it a read-only run
/// derives — and, through the creating form, creates — the developer's real home state dir, which
/// a unit test must never touch. Supplying it is the pair the resolver really produces for a
/// client with that variable set, the same shape the process legs' `spawn` helper gives the
/// binary.
fn resolve_test(root: &Path, read_only: bool) -> (Resolution, Vec<PathBuf>) {
    // The mode travels the way production sends it: `resolve_mode` discards `discovery.read_only`
    // unless `Provenance::read_only` says a channel answered, so a fixture passing the default
    // `ValueSource` would silently exercise a **read-write** run (U2). `CommandLine` is the source
    // the authoritative parse reports for the flag, and the same one `lib.rs` builds.
    let provenance = Provenance {
        read_only: ValueSource::CommandLine,
        ..Provenance::default()
    };
    let located = resolve::locate(
        Discovery {
            dir: Some(root.to_path_buf()),
            config: None,
            read_only,
            state_dir: Some(root.join("home-state")),
            pg_dsn: None,
        },
        &provenance,
    )
    .expect("the temp root resolves");
    client_resolution(DirArgs::default(), located).expect("the temp root resolves")
}

/// The state dir this run uses: the **production** rule ([`state_dir_for_run`]), so the fixture
/// cannot pin a selection the command never makes (decision 19) and the early stop at a live
/// candidate is exercised here too.
#[cfg(feature = "api")]
async fn chosen_state_dir(resolution: &Resolution, candidates: &[PathBuf]) -> StateDir {
    state_dir_for_run(resolution, candidates)
        .await
        .expect("the run's state dir")
}

/// The no-api arm: the production no-api function, which answers the resolved directory.
#[cfg(not(feature = "api"))]
fn chosen_state_dir(resolution: &Resolution) -> StateDir {
    state_dir_for_run(resolution).expect("the run's state dir")
}

/// Writes a **complete** configuration into `root`'s reserved directory.
///
/// The `[auth]` pair is the load-bearing part: "no credentials at all" is a `warn` of its own,
/// so a seeded root that omitted the pair would make a clean-root assertion unreachable for a
/// reason that has nothing to do with the check under test.
pub(crate) fn seed_config(root: &Path) {
    seed_config_with(root, false);
}

/// [`seed_config`] with the follow policy chosen. The symlink check reads it through the run's
/// one `FsOptions` mapping: while following is enabled the link entries are served, so the row is
/// `ok` there and `warn` only while following is disabled (the doctor section's symlink-check bullet).
pub(crate) fn seed_config_with(root: &Path, follow_symlinks: bool) {
    seed_config_file(root, follow_symlinks, true);
}

/// [`seed_config`]'s file **without the `[auth]` pair**: a configuration that exists and has no
/// credential section, which is the case the no-`[auth]` row is about (the generated-credentials
/// case is a *missing* file, which neither fixture produces). The reserved directory, the file
/// name and the format stay spelled in [`seed_config_file`] alone — the row is asserted on the
/// detail, so a test that spelled its own TOML could drift from what a real root holds.
pub(crate) fn seed_config_without_auth(root: &Path) {
    seed_config_file(root, false, false);
}

/// The one place the reserved directory, the file name and the TOML are written.
fn seed_config_file(root: &Path, follow_symlinks: bool, with_auth: bool) {
    let dir = root.join(".tinio");
    fs::create_dir_all(&dir).expect("reserved dir");
    // The complete fixture's pair, absent only for the variant whose whole subject is that
    // absence ([`seed_config`]'s doc carries why the pair is load-bearing), so it is a flag
    // here rather than a second TOML literal in the test that needs it.
    let auth = if with_auth {
        "[auth]\n\
         access_key = \"minioadmin\"\n\
         secret_key = \"minioadmin-secret\"\n"
    } else {
        ""
    };
    fs::write(
        dir.join(".tinio.toml"),
        format!(
            "version = 1\n\
             [server]\n\
             host = \"127.0.0.1\"\n\
             port = 9000\n\
             [storage.fs]\n\
             follow_symlinks = {follow_symlinks}\n{auth}"
        ),
    )
    .expect("seed the configuration");
}

/// Plants a link entry inside a bucket — the entry the symlink check exists for while
/// `follow_symlinks` is disabled. `#[cfg(unix)]`, because it needs a real symlink.
#[cfg(unix)]
pub(crate) fn seed_bucket_with_symlink(root: &Path) {
    let bucket = root.join("bucket");
    fs::create_dir_all(&bucket).expect("bucket dir");
    fs::write(root.join("target.txt"), b"data").expect("target");
    symlink(root.join("target.txt"), bucket.join("link.txt")).expect("the link entry");
}
