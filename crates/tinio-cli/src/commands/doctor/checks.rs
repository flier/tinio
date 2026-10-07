#[cfg(windows)]
use std::os::windows::ffi::OsStrExt;
#[cfg(windows)]
use std::ptr;
use std::{
    fs,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
    thread,
    time::Duration,
};

use futures::StreamExt;
#[cfg(unix)]
use rustix::fs as rustix_fs;
#[cfg(windows)]
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

use super::audit::walk_findings;
#[cfg(feature = "api")]
use crate::_api::{State, client, state::STATE_VERSION};
#[cfg(all(feature = "api", unix))]
use crate::_api::{
    bind,
    state::socket_path,
    transport::{LocalForm, resolve_socket_path},
};
// The crate-wide facade alias, imported rather than spelled `crate::_sandbox::...`: a
// three-segment expression path is what this crate's `absolute_paths` lint refuses.
use crate::_sandbox;
#[cfg(all(feature = "api", unix))]
use crate::commands::start;
use crate::{
    _core::cleanup::{self, Cleanup},
    _fs::{
        Error as FsError, FsCleanup, FsOptions, FsStorage,
        database::{self, Integrity, Stats, check_integrity, needs_compact, open},
    },
    _server::identity,
    Error,
    report::{Finding, PRIVATE_STATE, Report, Severity},
    resolve::{self, Resolution},
};

/// Which pass is asking for the store — the one thing that decides whether it may be created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// The diagnostics pass: **never creates a store**. An absent `meta.redb` is answered
    /// [`Store::Absent`](Store) without opening anything, because [`database::open`] would
    /// create the file and [`database::check_integrity`] would fail on it, and a run that
    /// diagnoses a root must leave it exactly as it found it.
    Diagnostics,
    /// The repair path: creates the store when it is missing. The store is private state, a
    /// repair owns it, and a first server start would create it anyway.
    ///
    /// **A read-only run never asks for it** (FR-023 amended): its metadata source is the root's
    /// own database, opened read-only, and the relocated tool-state dir holds tool state only —
    /// asking here would create the very file the mode promises not to write, and
    /// `FsStorage::new_from_db` would drop the handle unread a line later.
    Fix,
}

/// What [`open_store`](self::open_store) answered.
#[derive(Debug)]
pub enum Store {
    /// There is no store and this pass must not create one: the meta legs answer `ok`
    /// ("nothing to check") and the private-state leg `ok` ("nothing to clear").
    Absent,
    /// The store this pass may use, opened once, plus the verdict taken before it was opened.
    Opened {
        /// The [`database::Open`] [`database::open`] returned: its `db` is the handle the storage is
        /// built from, and its `stats` and `compact_needed` are the fragmentation leg's (and
        /// `--fix`'s compact's) inputs. The whole struct is kept, so this crate never has to
        /// name `redb`: the private-state leg destructures it and moves its `db` into the
        /// storage, which is why the two legs need no sharing at all — `checks` owns the
        /// `Store`.
        open: database::Open,
        /// [`database::check_integrity`]'s answer, taken before the `open` above existed.
        integrity: Integrity,
    },
}

/// The store, opened at most once — this command's only access to `meta.redb`.
///
/// **Measured on 2026-09-20** (a scratch crate against the locked crates): `check_integrity`
/// on a state dir with no `meta.redb` answers `Err` ("path not found"); `FsStorage::new`
/// creates `<root>/.tinio/` and `meta.redb`; and with an `FsStorage` alive, both
/// `database::open` and `check_integrity` answer `Err: Database already open. Cannot acquire
/// lock.` — after it is dropped, both succeed. Hence the probe and the single open, whose
/// `Open` this returns whole so its `db` and its two values reach both legs.
pub fn open_store(dir: &Path, pass: Pass) -> Result<Store, Error> {
    let present = fs::symlink_metadata(dir.join("meta.redb")).is_ok();
    if !present && pass == Pass::Diagnostics {
        return Ok(Store::Absent);
    }
    // The integrity check opens the file exclusively and is done with it before the handle
    // below is taken. A store that is absent is `Healthy` for the pass that is about to
    // create it; `--fix` reports the arms through its re-check (Task 3).
    let integrity = if present {
        check_integrity(dir).map_err(FsError::Database)?
    } else {
        Integrity::Healthy
    };
    let open = open(dir).map_err(FsError::Database)?;
    Ok(Store::Opened { open, integrity })
}

/// The control channel's path for the chosen state dir: the name the `state` file recorded
/// when there is one, the configured `[api.unix] path` when there is not, and plan A's
/// `state::socket_path` derivation last.
///
/// One place, because the two callers are the socket check's probe and `--fix`'s reclaim, and
/// they must ask about the same file: an `[api.unix] path` override makes the recorded name
/// the only truth, so a `--fix` that re-derived would unlink a path that was never the socket
/// and leave the stale one behind (decision 24: clients read the name, they never re-derive
/// it).
#[cfg(all(feature = "api", unix))]
pub fn socket_path_of(run: &Run<'_>) -> Result<PathBuf, Error> {
    let Run { resolution, state } = *run;
    match state.control_name.as_deref() {
        Some(recorded) => Ok(PathBuf::from(recorded)),
        None => match configured_socket_path(resolution, &state.dir)? {
            Some(configured) => Ok(configured),
            None => socket_path(&state.dir, &resolution.root).map_err(Error::Api),
        },
    }
}

/// The configured local channel's path, when the configuration names one: `start::api_options`
/// is the one owner of *which* path the configuration selects and this reads its answer.
///
/// It matters only when no `state` file recorded a name: a bind would use the configured
/// path, and the check's remedy names `[api.unix] path`. `api_options` takes the arguments'
/// defaults because `doctor` carries no `--api*` flag — the configuration is the only input.
///
/// **The path travels as the config wrote it, so this joins it exactly as the bind does.**
/// `api_options` deliberately does not join a relative one (its own comment says the join is
/// the bind's), and `Plane::bind` applies `transport::resolve_socket_path(path,
/// &options.state_dir)` — relative joins the state dir, absolute stays verbatim — before
/// `bind_unix` measures it against `SUN_PATH_MAX`. That function is therefore called here,
/// never re-implemented: an unjoined `"s.sock"` would make the length row measure 6 bytes
/// where the bind enforces the joined length, and `reclaim_stale` would probe a path relative
/// to this process's cwd instead of the state dir's socket.
///
/// A configuration `api_options` **refuses** (an `[api.http] host` that is not a bind
/// address) is a leg that did not run, never a silent fallback to the derived name: the
/// refusal travels out as an `Err` and the socket row renders it as `not inspected: <reason>`.
/// A configuration with no local channel at all still answers `Ok(None)`, which is the
/// documented fallback to the derived name.
#[cfg(all(feature = "api", unix))]
fn configured_socket_path(
    resolution: &Resolution,
    state_dir: &Path,
) -> Result<Option<PathBuf>, Error> {
    let selection = start::api_options(resolution, &start::Args::default())?;
    Ok(match selection.local {
        Some(LocalForm::Unix(path)) => Some(resolve_socket_path(&path, state_dir)),
        Some(LocalForm::Pipe(_)) | None => None,
    })
}
/// The state dir this run chose, and what answered there.
#[derive(Debug, Clone)]
pub struct StateDir {
    /// The directory every state-dependent leg reads, and the one `--fix` repairs.
    pub dir: PathBuf,
    /// `true` when `dir` is the candidate the resolution named, `false` when the other one
    /// answered instead (decision 19) — the report's first row says which.
    pub resolved: bool,
    /// What [`liveness`](self::liveness) found there, so the control-channel refusal (Task 3)
    /// and the stale-state leg cannot disagree about the same directory.
    pub liveness: Liveness,
    /// The `control_name` the `state` file recorded, from the same probe: `Some` whenever a
    /// state file was read (whatever the verdict), `None` when there was none or it did not
    /// decode. It is what the socket probe and a `--fix`'s unlink must use — `[api.unix]`
    /// `path` may override the derived name, so re-deriving it aims at a file that was never
    /// the channel (decision 24, and `socket_path_of` is the one place
    /// the choice is made).
    pub control_name: Option<String>,
    /// Whether the recorded channel answered. A same-version server sets [`Liveness::Live`];
    /// a foreign-version document stays [`Liveness::Unreadable`] and still sets this when the
    /// dial succeeds, which is the question `--fix` refuses on before any repair.
    pub channel_answers: bool,
}

/// One run's two inputs, travelling together.
///
/// Almost every leg is a function of both: the root, the mode and the configuration come from
/// the [`Resolution`], while the `state` file, the socket and the store are read from the chosen
/// [`StateDir`]. Passing them as two parameters makes it possible to hand a leg one run's
/// resolution and another's state dir without the compiler noticing — the pair has no meaning
/// apart, so it is passed as one value.
#[derive(Debug, Clone, Copy)]
pub struct Run<'a> {
    /// The resolution the command resolved: the root, the mode, the configuration.
    pub resolution: &'a Resolution,
    /// The state directory this run chose, and what answered there (decision 19).
    pub state: &'a StateDir,
}

/// What the `state` file and the control channel say about a state dir.
#[derive(Debug, Clone)]
pub enum Liveness {
    /// A server answered its control channel.
    Live,
    /// A `state` file is there with nothing answering behind it: the crashed state a repair
    /// may clear, and the case whose recorded `control_name` the socket probe must use.
    Stopped,
    /// No `state` file at all — a root that has never run a server, which is why the socket,
    /// if anything is asked about it, falls back to the derived name.
    NoStateFile,
    /// The `state` file is there but cannot be read or decoded — a version this build does
    /// not read, or bytes that are not a document. Nothing about the server is known, so this
    /// is neither `Live` nor `Stopped`, and the leg that meets it reports
    /// "not inspected: `<reason>`" rather than guessing.
    Unreadable(String),
}

/// How the cleanup leg's dry-run proposals are reported — the whole difference between a
/// plain `doctor` and `--dry-run` (Task 3 sets it from the flag).
///
/// `pub` with its siblings, and not for show: all of them appear in the signature of a
/// `pub fn`, and a private type in a public signature is `private_interfaces` — a deny-by
/// `-D warnings` lint, which is what the lint steps of this plan run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proposals {
    /// A plain run: a "would …" row is information about what a fix would change, not a
    /// defect, so it renders `ok` and leaves the exit code alone.
    Informational,
    /// `--dry-run`: the same rows render as findings, so the run lists exactly what a fix
    /// would change and exits 1 when there is something to change (the doctor section's `--dry-run` sentence).
    Findings,
}

/// One probe's answer: the verdict, and the name the `state` file recorded when it was read.
#[derive(Debug, Clone)]
pub struct Probe {
    /// The verdict.
    pub liveness: Liveness,
    /// The recorded `control_name`, for `socket_path_of`.
    pub control_name: Option<String>,
    /// Whether the recorded channel answered. See [`StateDir::channel_answers`].
    pub channel_answers: bool,
}

/// The one probe: plan A's `State::read` plus `client::probe` — never written twice, so the
/// refusal, the report and the repair cannot disagree. It carries the recorded
/// `control_name` out with it, because that name is only knowable here.
///
/// **The decode is the lenient one, and every decoded document is dialled once.** A document whose
/// `version` is not this build's is not a `State` this build may act on, so the verdict stays
/// [`Liveness::Unreadable`] — but the dial still happens, and [`Probe::channel_answers`] records
/// it. The `control_name` in that document is the channel its writer actually bound; throwing it
/// away is what made a probe dial the *derived* name instead, miss a recorded override, and treat
/// a newer server as silence. A file that does not decode at all still carries no name and is not
/// dialled, because there is none to dial.
#[cfg(feature = "api")]
pub async fn liveness(state_dir: &Path) -> Result<Probe, Error> {
    Ok(match State::read_any_version(state_dir) {
        Ok(None) => Probe {
            liveness: Liveness::NoStateFile,
            control_name: None,
            channel_answers: false,
        },
        Ok(Some(state)) => {
            let channel_answers = client::probe(&state).await;
            Probe {
                liveness: if state.version != STATE_VERSION {
                    Liveness::Unreadable(format!(
                        "version {} is not the {STATE_VERSION} this build reads",
                        state.version
                    ))
                } else if channel_answers {
                    Liveness::Live
                } else {
                    Liveness::Stopped
                },
                control_name: Some(state.control_name),
                channel_answers,
            }
        }
        Err(err) => Probe {
            liveness: Liveness::Unreadable(err.to_string()),
            control_name: None,
            channel_answers: false,
        },
    })
}

/// Without the plane there is no `state` file and no control channel: nothing to probe.
#[cfg(not(feature = "api"))]
pub async fn liveness(_state_dir: &Path) -> Result<Probe, Error> {
    Ok(Probe {
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    })
}

/// The fixed arm of the disk-space floor: `max(1 GiB, 5%)`, whose percentage arm needs the
/// filesystem's total and therefore lives in [`below_floor`], not in a constant.
pub const DEFAULT_FLOOR_BYTES: u64 = 1024 * 1024 * 1024;

/// Whether `free` bytes on a `total`-byte filesystem are below the floor — the whole rule on
/// its own, so the unit test pins both arms with explicit numbers instead of a filesystem.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::commands::doctor::below_floor;
///
/// assert!(below_floor(0, 0), "an empty filesystem is below any floor");
/// // A 4 GiB volume: the fixed 1 GiB arm decides, never the percentage.
/// assert!(!below_floor(1024 * 1024 * 1024, 4 * 1024 * 1024 * 1024));
/// ```
pub fn below_floor(free: u64, total: u64) -> bool {
    free < DEFAULT_FLOOR_BYTES.max(total / 20)
}

/// The free-space check: probe `root`'s filesystem, then report the amount, its percentage of
/// the volume and the floor rule's verdict.
pub fn free_space(root: &Path) -> Finding {
    match space(root) {
        Some((free, total)) => free_space_finding(root, free, total),
        None => Finding::check(
            "free space",
            Severity::Warn,
            format!("not inspected: {} could not be probed", root.display()),
        ),
    }
}

/// The probed pair as a finding: `warn` below the floor, `ok` at or above it, with both the
/// free amount and its percentage in the detail — which is what makes the warn actionable on
/// a volume whose size the operator does not have in their head.
fn free_space_finding(root: &Path, free: u64, total: u64) -> Finding {
    let percentage = free.saturating_mul(100).checked_div(total).unwrap_or(0);
    let detail = format!(
        "{} free of {} ({}%) for {}",
        gib(free),
        gib(total),
        percentage,
        root.display()
    );
    Finding::check(
        "free space",
        if below_floor(free, total) {
            Severity::Warn
        } else {
            Severity::Ok
        },
        detail,
    )
}

/// The name of the host-confinement row, owned here for the check that emits it: a rename
/// reaching only one site would still pass on a host that confines, and silently misreport on one
/// that does not.
///
/// **The readers across the crate boundary spell it themselves, and that is the boundary's cost
/// rather than a copy someone forgot to share.** The cucumber legs drive the `tinio` binary and
/// hold no dependency on this crate, so a rename here has to reach `tinio-e2e`'s `steps/doctor.rs`
/// (the `check != "host confinement"` filter that excludes the row) and the two
/// `carrying "host confinement"` steps in `doctor.feature` by hand. What those legs read the name
/// from is the contract — `contracts/cli.md`'s `doctor` check list — rather than a constant this
/// crate could hand them.
pub(crate) const HOST_CONFINEMENT: &str = "host confinement";

/// The row's `ok` detail on Windows. **`Ok(())` establishes only that this token was not
/// refused**, so the sentence may not promise a profile: the predicate answers `Ok` both when
/// its probe created and deleted the throwaway profile and when the probe could not answer at
/// all — an unrecognised HRESULT is logged at `warn`, answered `Ok` and cached nowhere
/// (`capability.rs`'s `get_or_probe`). A real refusal still reaches the operator through the
/// other arm, and the real profile creation keeps classifying its own HRESULT.
#[cfg(windows)]
pub(crate) const HOST_CONFINEMENT_OK: &str =
    "this host's token was not refused an AppContainer profile";

/// The row's `ok` detail on Linux: **the answer is a real probe of this host now** — the facade's
/// arm resolves the requested model against the kernel (its user-namespace probe and the effective
/// uid, `Options::resolve`), so `Ok` means the model resolution found a mechanism this host can
/// apply rather than that no question was asked.
#[cfg(target_os = "linux")]
pub(crate) const HOST_CONFINEMENT_OK: &str =
    "this host resolves the requested model to a confinement mechanism it can apply";

/// The row's `ok` detail on every other non-Windows target: the facade's arm answers `Ok(())`
/// without probing anything, because the platform's confinement has no host-level precondition
/// to fail — macOS's Seatbelt profile applies to this process.
#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) const HOST_CONFINEMENT_OK: &str =
    "this host has no host-level confinement precondition to fail";

/// Whether this host can confine a process, as a finding.
///
/// **The `ok` detail is the platform's own truth, and the arms differ because the mechanisms
/// do**: on Windows, `Ok(())` means only that this token was not refused — the profile may have
/// been created, or the probe may have been unable to answer — so the row claims the non-refusal
/// and never the creation. Linux's arm is this kernel's answer to the model resolution, so its
/// `ok` sentence claims exactly that and no more; the remaining targets' confinement has no
/// host-level precondition to fail, so their arm probes nothing and says exactly that. The
/// refusal arm renders the predicate's error, and no `ok` sentence is that error's negation —
/// which is what keeps the row honest on every target it is compiled for.
///
/// **A second self-cleaning write is allowed here, and it is the design's ruling rather than
/// an oversight**: the profile this asks about is created and deleted by the predicate, and
/// no storage root is touched — `writability`'s note names both probes.
pub(crate) fn host_confinement_finding(answer: Result<(), _sandbox::Error>) -> Finding {
    let (severity, detail) = match answer {
        Ok(()) => (Severity::Ok, HOST_CONFINEMENT_OK.to_string()),
        Err(err) => (Severity::Warn, err.to_string()),
    };
    // **`Finding::check`, not a struct literal**: `Finding.check` is a String
    // (`report.rs:39-47`), and the constructor is the shape `free_space_finding` uses beside it.
    Finding::check(HOST_CONFINEMENT, severity, detail)
}

/// A byte count in GiB, to one decimal — the resolution the floor is discussed at.
fn gib(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
}

/// `(free, total)` bytes on `root`'s filesystem: `f_bavail × f_frsize` and
/// `f_blocks × f_frsize` on unix.
#[cfg(unix)]
fn space(root: &Path) -> Option<(u64, u64)> {
    let stat = rustix_fs::statvfs(root).ok()?;
    let fragment = stat.f_frsize as u64;
    Some((
        (stat.f_bavail as u64).saturating_mul(fragment),
        (stat.f_blocks as u64).saturating_mul(fragment),
    ))
}

/// `(free, total)` bytes on `root`'s filesystem: `lpFreeBytesAvailableToCaller` and
/// `lpTotalNumberOfBytes` of `GetDiskFreeSpaceExW`.
///
/// **One of this crate's enumerated `#[allow(unsafe_code)]` sites**: the enumeration and its
/// reason live with `signal.rs`'s console handler, which owns the list — this file is one of
/// its entries, and repeating the others here would be the second copy the one-definer rule
/// forbids. The invariant is Windows's: the call writes the two `u64` out
/// parameters **only on success**, and the path reaches it as a NUL-terminated wide string —
/// the encoding `GetDiskFreeSpaceExW` requires.
#[cfg(windows)]
#[allow(unsafe_code)]
fn space(root: &Path) -> Option<(u64, u64)> {
    let mut path: Vec<u16> = root.as_os_str().encode_wide().collect();
    if path.contains(&0) {
        return None;
    }
    path.push(0);
    let mut available = 0u64;
    let mut total = 0u64;
    // SAFETY: `path` is a live, NUL-terminated wide string, and both out parameters are live
    // `u64`s for the duration of the call. The API writes them only on success.
    let ok =
        unsafe { GetDiskFreeSpaceExW(path.as_ptr(), &mut available, &mut total, ptr::null_mut()) };
    (ok != 0).then_some((available, total))
}

/// The check list, in the contract's order, against the **chosen** state dir.
///
/// The report's first row is the run's own frame: decision 19's dual-candidate probe has to be
/// visible in the product and not only in the code that chose it. The store is opened at most
/// once across the legs that need it, and the handle is dropped before the next leg reads the
/// filesystem.
pub async fn checks(run: &Run<'_>, proposals: Proposals) -> Report {
    let Run { resolution, state } = *run;
    let mut report = Report::default();
    report.push(state_dir_finding(state));
    report.extend(storage_root_findings(
        &resolution.root,
        resolution.read_only,
    ));
    report.push(config_finding(resolution));

    // The one `FsOptions` this run builds, and the one read of the configuration behind it:
    // the walk's follow policy, the fragmentation threshold and the cleanup's write policy
    // are read from the very struct the storage is built from, never from a second read that
    // could disagree with it.
    let options = fs_options(run);
    let threshold = options.compact_threshold_percent;
    let follow_symlinks = options.follow_symlinks;

    match open_store(&state.dir, Pass::Diagnostics) {
        Ok(Store::Absent) => {
            report.push(Finding::check(
                PRIVATE_STATE,
                Severity::Ok,
                "there is no store; nothing to clear",
            ));
            report.push(Finding::check(
                "meta.redb",
                Severity::Ok,
                "not present; nothing to check",
            ));
        }
        Ok(Store::Opened { open, integrity }) => {
            // `Stats` is `Copy` and the marker is a `bool`, so both are read **before**
            // `open` — which owns the `db` — moves into the cleanup helper: one open, one
            // owner, and the fragmentation leg still reads the values that open carried.
            let stats = open.stats;
            let compact_needed = open.compact_needed;
            report.extend(cleanup_findings(run, open, options, proposals).await);
            report.extend(store_findings(integrity, &stats, compact_needed, threshold));
        }
        Err(err) if store_is_held(&err) => {
            let detail = format!("not inspected: {err}");
            report.push(Finding::check(
                PRIVATE_STATE,
                Severity::Warn,
                detail.clone(),
            ));
            report.push(Finding::check("meta.redb", Severity::Warn, detail));
        }
        Err(err) => {
            report.push(Finding::check(
                PRIVATE_STATE,
                Severity::Error,
                err.to_string(),
            ));
            report.push(Finding::check(
                "meta.redb",
                Severity::Error,
                err.to_string(),
            ));
        }
    }

    report.extend(walk_findings(&resolution.root, follow_symlinks).await);
    report.push(free_space(&resolution.root));
    // The push sits exactly after `free_space`'s because the predicate is the host's own
    // answer, not the root's, and the row is the last thing about the machine the checks
    // speak for. It is not free: Windows' probe creates and deletes an AppContainer profile
    // inside this call, and Linux's arm runs the model resolution's own user-namespace probe
    // (a forked child) here.
    report.push(host_confinement_finding(_sandbox::can_confine()));
    #[cfg(feature = "api")]
    report.push(state_findings(run));
    #[cfg(all(feature = "api", unix))]
    report.extend(socket_findings(run));
    report
}

/// The run's frame: the state directory every state-dependent leg read, and which of decision
/// 19's two candidates it is.
fn state_dir_finding(state: &StateDir) -> Finding {
    let detail = if state.resolved {
        format!(
            "using {} (the candidate this run resolved)",
            state.dir.display()
        )
    } else {
        format!(
            "using {} (the alternate candidate; the resolved one did not answer)",
            state.dir.display()
        )
    };
    Finding::check("state directory", Severity::Ok, detail)
}

/// Whether the failure is "another process holds the store" — the one `database` failure that
/// is a fact about this run rather than about the store. The predicate belongs to the crate
/// that owns the error (`database::Error::is_already_open`), so this crate asks it instead of
/// matching redb's message text or depending on `tinio-store-redb` and `redb` to reach the
/// variant.
fn store_is_held(err: &Error) -> bool {
    matches!(err, Error::Fs(FsError::Database(inner)) if inner.is_already_open())
}

/// The repair-stage row for an [`open_store`] failure. A store another process holds is the
/// diagnostics "not inspected" warn; every other open failure is an `error`. Neither is a note
/// on the ok summary.
pub(super) fn store_open_finding(err: &Error) -> Finding {
    if store_is_held(err) {
        Finding::check("meta.redb", Severity::Warn, format!("not inspected: {err}"))
    } else {
        Finding::check(
            "meta.redb",
            Severity::Error,
            format!("the state store could not be opened: {err}"),
        )
    }
}

/// The storage root's two rows: it exists and is a readable directory, and — unless the run is
/// read-only, where a read-only root is valid (the doctor section's `--read-only` paragraph) — it is writable.
fn storage_root_findings(root: &Path, read_only: bool) -> Vec<Finding> {
    let mut out = Vec::with_capacity(2);
    out.push(readability(root));
    if read_only {
        out.push(Finding::check(
            "storage root writable",
            Severity::Ok,
            "skipped: read-only mode accepts a read-only root",
        ));
    } else {
        out.push(writability(root));
    }
    out
}

/// The readability row, the first half of the contract's "exists, is readable and writable".
///
/// The three ways a root can fail to be read are told apart, because they are three different
/// operator actions: a path that is not there, one that is not a directory, and one this process
/// may not enumerate. `is_dir()` alone cannot see the third — it answers from the type bits the
/// stat returned — so the row **opens the directory**: `read_dir` is what a reader does, and it
/// is where a permission denied surfaces. The walk's own `read_dir` is a separate leg with its
/// own "not inspected" row, so a root that is readable here and partly unreadable below still
/// reports both facts.
fn readability(root: &Path) -> Finding {
    let (severity, detail) = match fs::metadata(root) {
        Err(err) => (
            Severity::Error,
            format!("{} could not be read: {err}", root.display()),
        ),
        Ok(metadata) if !metadata.is_dir() => (
            Severity::Error,
            format!("{} is not a directory", root.display()),
        ),
        Ok(_) => match fs::read_dir(root) {
            Ok(_) => (
                Severity::Ok,
                "exists and is a readable directory".to_string(),
            ),
            Err(err) => (
                Severity::Error,
                format!("{} exists but cannot be read: {err}", root.display()),
            ),
        },
    };
    Finding::check("storage root", severity, detail)
}

/// The writability probe: one probe file, created and removed, so the answer is the
/// filesystem's rather than the mode bits' (an ACL can deny a write the mode permits).
///
/// **This is the diagnosis's first of two temporary writes, and each undoes itself on the
/// ordinary path**: the root is left exactly as the run found it (`contracts/cli.md`: the
/// commands that launch nothing "must leave that root exactly as it found it"). The second
/// is the confinement predicate's, which creates and deletes an AppContainer profile inside
/// the [`host_confinement_finding`] row — a probe of the machine, not of the root. Neither is
/// an absolute promise: a probe file whose removal fails is a finding here and the name walk
/// reports it like any other, and the design's Risks record the orphaned
/// `tinio.sandbox.capability-probe` a run killed between create and delete leaves for a later
/// sweep to reap. `--dry-run` withholds *repairs*, never a check, so both probes run in every
/// mode; what the flag changes is that no repair stage writes.
fn writability(root: &Path) -> Finding {
    // A fresh name per probe: a bare pid collides when two doctor processes diagnose one root,
    // and `create_new` refuses a name a third party pre-created — so the probe removes only the
    // file it created.
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let serial = NEXT.fetch_add(1, Ordering::Relaxed);
    let probe = root.join(format!(".tinio-doctor-probe-{}-{serial}", process::id()));
    // The create and the unlink are separate outcomes: a root that accepted the write **is**
    // writable, even when doctor cannot clean up after itself, and one shared `error` row
    // would misreport that.
    if let Err(err) = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        return Finding::check(
            "storage root writable",
            Severity::Error,
            format!("{}: {err}", root.display()),
        );
    }
    let removed = match fs::remove_file(&probe) {
        Ok(()) => Ok(()),
        // Best-effort retry: a scanner or indexer holding the fresh handle for an instant is
        // the usual cause, and a second unlink a moment later usually succeeds.
        Err(_) => {
            thread::sleep(Duration::from_millis(50));
            fs::remove_file(&probe)
        }
    };
    match removed {
        Ok(()) => Finding::check(
            "storage root writable",
            Severity::Ok,
            "a probe file was created and removed",
        ),
        Err(err) => Finding::check(
            "storage root writable",
            Severity::Warn,
            format!(
                "{} is writable; the probe file {} could not be removed: {err}",
                root.display(),
                probe.display()
            ),
        ),
    }
}

/// The configuration's row: the load outcome the resolution carries, the absent file's warn,
/// and the credential rule.
///
/// The absent-`[auth]` row is a `warn` rather than an `error` because a start still comes up;
/// what its text says is the **loopback** story, and that is the fact the credential remediation
/// changed here. A config that exists without the section is not the generated-credentials case
/// — [`discover::load_or_create`](crate::_config::discover::load_or_create) only generates when
/// the file is *absent* — so a start serves `identity::root_auth_pair`'s built-in `minioadmin`
/// development pair on loopback and refuses a non-loopback bind instead.
///
/// A root whose configuration does not parse is the case `doctor` exists for, so the failure
/// is a finding rather than a `?` that would abort the run. Nothing is read here: the
/// resolution already loaded the file and recorded the outcome beside the parsed config, and a
/// second read could answer differently from the config the other legs are running on.
fn config_finding(resolution: &Resolution) -> Finding {
    if let Some(err) = &resolution.config_error {
        return Finding::check("configuration", Severity::Error, err.clone());
    }
    if !resolution.config_path.is_file() {
        return Finding::check(
            "configuration",
            Severity::Warn,
            format!(
                "{} does not exist; a first start would auto-create it",
                resolution.config_path.display()
            ),
        );
    }
    // The predicate, not a second spelling of it: the row describes the very
    // policy the start's refusal and warning enforce (`commands::start::run`'s
    // static half, `commands::start::serve`'s loopback warning; P2), so the two
    // must not be able to disagree.
    if identity::uses_builtin_fallback(&resolution.config) {
        return Finding::check(
            "configuration",
            Severity::Warn,
            "no [auth] pair: a start would serve the built-in minioadmin development pair on \
             loopback, and would refuse a non-loopback bind instead",
        );
    }
    Finding::check(
        "configuration",
        Severity::Ok,
        format!("{} parses and validates", resolution.config_path.display()),
    )
}

/// One `Finding` per item the private-state leg found, plus one per stage that failed.
///
/// The cleanup runs `dry_run: true` in **every** mode: the check reports, the [`Proposals`]
/// mapping decides how loudly, and only `--fix` (Task 3) constructs it with `dry_run: false`.
/// The storage is built from the one database [`open_store`] opened, which this helper takes
/// over as the caller's `Open` — so there is never a second handle to the same store — and
/// from the caller's `options`, so the write policy is the same mapping's the caller read its
/// follow policy from.
async fn cleanup_findings(
    run: &Run<'_>,
    store: database::Open,
    options: FsOptions,
    proposals: Proposals,
) -> Vec<Finding> {
    // The `Open` is destructured here, so the one database `database::open` returned moves
    // into the storage constructor: no sharing, and this crate never names `redb`.
    let database::Open { db, .. } = store;
    let access = options.access;
    let storage = match FsStorage::new_from_db(&run.resolution.root, options, db) {
        Ok(storage) => storage,
        Err(err) => {
            return vec![Finding::check(
                PRIVATE_STATE,
                Severity::Error,
                err.to_string(),
            )];
        }
    };
    let cleanup = FsCleanup::new(
        &storage,
        cleanup::CleanupOptions {
            dry_run: true,
            access,
        },
    );
    match collect_actions(&cleanup, proposals).await {
        Ok(findings) if findings.is_empty() => vec![Finding::check(
            PRIVATE_STATE,
            Severity::Ok,
            "nothing to clear",
        )],
        Ok(findings) => findings,
        Err(err) => vec![Finding::check(
            PRIVATE_STATE,
            Severity::Error,
            err.to_string(),
        )],
    }
}

/// Drives the repair stream: every `Ok` action becomes a row through the [`Proposals`]
/// mapping, every `Err` an `error` row in **both** modes — a stage that failed is a finding,
/// not an early return, so the report still carries every other check's answer.
async fn collect_actions(
    cleanup: &FsCleanup,
    proposals: Proposals,
) -> Result<Vec<Finding>, FsError> {
    let mut stream = cleanup.repair(cleanup::RepairKind::Full).await?;
    let mut findings = Vec::new();
    while let Some(action) = stream.next().await {
        findings.push(match action {
            Ok(action) => match proposals {
                Proposals::Findings => Finding::from_action(action),
                Proposals::Informational => Finding::informational(action),
            },
            Err(err) => Finding::check(PRIVATE_STATE, Severity::Error, err.to_string()),
        });
    }
    Ok(findings)
}

/// The store's two rows: the integrity verdict, and the fragmentation rule the module owns
/// ([Needs compact](needs_compact)), evaluated on the `stats` the same `Open` carried — never
/// against a byte count this leg compared itself.
fn store_findings(
    integrity: Integrity,
    stats: &Stats,
    compact_needed: bool,
    threshold: u8,
) -> Vec<Finding> {
    vec![
        integrity_finding(integrity),
        compact_finding(stats, compact_needed, threshold),
    ]
}

/// [`check_integrity`]'s three arms as three rows, with the corrupted arm's remedy named: the
/// metadata is derivable, so the file may be deleted and rebuilt (`fs-backend.md:89`).
///
/// `pub(crate)` like the file's other directly tested helpers (`name_finding`,
/// `symlink_finding`, `inspection_finding`, `walk_audit`): the `Repaired` arm has no store a
/// test can seed, so the unit fixture drives the mapping itself.
pub(crate) fn integrity_finding(integrity: Integrity) -> Finding {
    match integrity {
        Integrity::Healthy => Finding::check("meta.redb", Severity::Ok, "integrity check passed"),
        Integrity::Repaired => Finding::check(
            "meta.redb",
            Severity::Warn,
            "the integrity check failed and was repaired automatically",
        ),
        Integrity::Corrupted(err) => Finding::check(
            "meta.redb",
            Severity::Error,
            format!(
                "corrupted ({err}); the metadata is derivable — delete the file and let the \
                 next start rebuild it"
            ),
        ),
    }
}

/// The fragmentation rule: the `compact_needed` marker or the ratio rule at the configured
/// threshold.
fn compact_finding(stats: &Stats, compact_needed: bool, threshold: u8) -> Finding {
    let needed = compact_needed || needs_compact(stats, threshold);
    let detail = format!(
        "{} of {} allocated bytes fragmented; {}",
        stats.fragmented_bytes,
        stats.allocated_bytes,
        if needed {
            "compaction would reclaim space"
        } else {
            "compaction is not needed"
        }
    );
    Finding::check(
        "meta.redb fragmentation",
        if needed { Severity::Warn } else { Severity::Ok },
        detail,
    )
}

/// The `state` file's own row (`#[cfg(feature = "api")]`): nothing there is `ok`, a file a
/// server answers behind is a diagnosis and not a defect, and a file nothing answers for is
/// the crashed state a repair may clear (the doctor section's state/socket qualifier).
///
/// **The row exists in every `api` build, not only unix ones.** The contract's state check is
/// the plane's, and only the socket half is platform-specific (a Windows named pipe cannot
/// outlive its process, so there is never socket residue to reclaim there); gating the whole
/// leg on `unix` would make a Windows `api` run report a crashed server as nothing at all.
///
/// **The run's own verdict, not a second probe.** `StateDir.liveness` is what `run` probed to
/// choose this directory, and `--fix` refreshes it after the repair — so this row, the
/// "state directory" row above it and the socket leg below all read one verdict, taken once.
/// The recorded `control_name` is the same snapshot's (`socket_path_of`), because the socket
/// probe and the reclaim must speak about the channel the `state` file named.
#[cfg(feature = "api")]
fn state_findings(run: &Run<'_>) -> Finding {
    let Run { resolution, state } = *run;
    match &state.liveness {
        Liveness::Live => Finding::check(
            "state file",
            Severity::Ok,
            format!("a server is running for {}", resolution.root.display()),
        ),
        Liveness::Stopped => Finding::check(
            "state file",
            Severity::Warn,
            format!(
                "a crashed server left a stale `state` file in {}; `--fix` reclaims it",
                state.dir.display()
            ),
        ),
        Liveness::NoStateFile => Finding::check(
            "state file",
            Severity::Ok,
            format!("no `state` file in {}", state.dir.display()),
        ),
        Liveness::Unreadable(reason) => Finding::check(
            "state file",
            Severity::Warn,
            format!("not inspected: {reason}"),
        ),
    }
}

/// The control socket's two rows (`#[cfg(all(feature = "api", unix))]`): the length against
/// the limit the bind enforces, and the stale-file probe. The `state` file's own verdict is
/// `state_findings`' row, not this one's.
///
/// The gate is the pair and not the feature alone: the check calls `tinio_api`, which is an
/// optional dependency, and on Windows the local channel is a named pipe — a pipe cannot
/// outlive its process, so there is no file to probe and no residue to reclaim.
#[cfg(all(feature = "api", unix))]
fn socket_findings(run: &Run<'_>) -> Vec<Finding> {
    let Run { resolution, state } = *run;
    let path = match socket_path_of(run) {
        Ok(path) => path,
        Err(err) => {
            return vec![Finding::check(
                "control socket",
                Severity::Warn,
                format!("not inspected: {err}"),
            )];
        }
    };
    let mut out = Vec::with_capacity(2);
    let length = path.as_os_str().len();
    out.push(Finding::check(
        "control socket path length",
        if length < bind::SUN_PATH_MAX {
            Severity::Ok
        } else {
            Severity::Warn
        },
        format!(
            "{} is {length} bytes; `sun_path` allows {max} — set `[api.unix] path` to an \
             absolute path in a short directory",
            path.display(),
            max = bind::SUN_PATH_MAX
        ),
    ));
    out.push(match state.liveness {
        // A live server is a diagnosis, not a refusal: only `--fix` refuses (Task 3). Its
        // socket is in use, so there is nothing stale to probe here.
        Liveness::Live => Finding::check(
            "stale control socket",
            Severity::Ok,
            format!(
                "in use by the server running for {}",
                resolution.root.display()
            ),
        ),
        Liveness::Stopped | Liveness::NoStateFile => match bind::reclaim_stale(&path, true) {
            Ok(true) => Finding::check(
                "stale control socket",
                Severity::Warn,
                format!("a stale control socket at {}", path.display()),
            ),
            Ok(false) => Finding::check(
                "stale control socket",
                Severity::Ok,
                format!("no control socket at {}", path.display()),
            ),
            Err(err) => Finding::check(
                "stale control socket",
                Severity::Warn,
                format!("not inspected: {err}"),
            ),
        },
        Liveness::Unreadable(ref reason) => Finding::check(
            "stale control socket",
            Severity::Warn,
            format!("not inspected: {reason}"),
        ),
    });
    out
}

/// The offline [`FsOptions`] this run's storage is built from: **plan B's one mapping**
/// ([`resolve::fs_options`], `None` because an offline command builds no pipelines) with the
/// one field the chosen-dir rule overrides.
///
/// `state_dir` is pinned to the directory this run **chose** (`state.dir`), never the
/// resolution's own, because the storage must open the store where the run decided to look
/// (decision 19). That override is the whole of the difference: the configured `[storage.fs]`
/// values (follow policy, thresholds, batch sizes), the pipelines and the owner map are the
/// server's own, so this leg runs on the storage the server would build. Paraphrasing those
/// fields here would be the second mapping plan C's ownership check forbids — and a wrong one:
/// the schema's `follow_symlinks` default is `false`, so "no `[storage]` section means follow"
/// silently followed symlinks where the server does not.
pub(crate) fn fs_options(run: &Run<'_>) -> FsOptions {
    let Run { resolution, state } = *run;
    let mut options = resolve::fs_options(resolution, None);
    options.state_dir = Some(state.dir.clone());
    options
}
