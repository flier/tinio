//! The AppContainer profiles a run that died without unwinding leaves behind —
//! and the sweep that reaps them: **once per process, on the launch path, before
//! the root this launch serves derives its own name** (design decision 21).
//!
//! **Why it exists, as measured.** A clean run deletes its profile:
//! [`crate::sid::PackageSid`]'s `Drop` calls `DeleteAppContainerProfile`, which
//! takes `%LOCALAPPDATA%\Packages\<name>` **and** the
//! `HKCU\…\AppContainer\Storage\<name>` key with it. A run that dies *without*
//! unwinding — a killed launcher, an aborted test, the `STATUS_STACK_OVERFLOW`
//! abort of 2026-09-28 and the 19 orphans it left — never reaches that call, so
//! **both** halves stay. Measured on this host 2026-09-28: **815**
//! `tinio.sandbox.*` directories under `Packages` beside **815** matching
//! `Storage` keys, which is one key per uncleaned exit and not a key per clean
//! run: the pairs match because an unclean exit leaves one of each, and a clean
//! exit leaves neither (measured against a freshly created profile and against a
//! residue profile a real confined run had left — see
//! `either_half_alone_is_reaped`, which carries the readings; the design's
//! residue row had inferred the opposite from counts alone, and this module's
//! measurement supersedes that clause). The operator ruled: reap them at launch.
//!
//! **Six rules, and every one of them is a way this sweep could delete a live
//! server's profile instead of stale residue:**
//!
//! 1. **Only names this crate creates.** A candidate is a name starting with
//!    [`PACKAGE_PREFIX`] — a directory under `Packages` spelled that way, or a
//!    `Storage` subkey spelled that way. Nothing else in either place is ever
//!    read as a candidate, let alone removed: `Packages` holds one directory per
//!    installed package on the machine and `Storage` one key per profile, and
//!    both list hundreds of entries this crate has nothing to do with.
//! 2. **A conservative liveness gate.** The sweep does not run at all while any
//!    *other* process named `tinio` is alive. A confined server under a profile
//!    this crate made **is** a `tinio` process — the launcher re-invokes its own
//!    image ([`crate::backend::Options::program`] defaults to
//!    `env::current_exe`), so the child wears the launcher's name — and the
//!    per-root [`ProfileLock`] is **not** evidence against one: every launcher
//!    releases that lock at `CreateProcessW`, so a server that has been serving
//!    for hours holds nothing. The gate is the only rule that can see such a
//!    server, and it is deliberately blunt: a stray `tinio` process on the
//!    machine costs a postponed sweep, never a reaped profile.
//! 3. **A per-name lock try, non-blocking.** [`ProfileLock::try_acquire`] is the
//!    same object `prepare_launch` takes for one root's launch window. A name
//!    whose lock is held (or whose lock the kernel refuses to establish, which
//!    must not be read as "nobody is in the window") is skipped.
//! 4. **Both halves, and either one alone.** Reaping a name is
//!    `DeleteAppContainerProfile(name)` **plus** the `Storage` key under `HKCU`,
//!    and both calls are made for every candidate: the delete removes the profile
//!    (directory included, and today the key too), the registry calls remove the
//!    key whatever state it is in, and a name whose other half is already gone is
//!    reaped rather than refused.
//! 5. **Never a failed launch.** Every failure warns at `step = "sweep the stale
//!    profiles"` and the launch proceeds. The sweep is hygiene, not a gate: a
//!    machine where it cannot run is a machine that keeps its residue, not a
//!    machine that cannot serve.
//! 6. **One summary line.** Exactly one `info!` event per sweep carries the count
//!    reaped, the count skipped for each reason and the gate's process count, so
//!    an operator sees the first run shrink the 815. Per-name detail is `debug!`.
//!
//! **What the sweep does not guarantee, stated rather than implied.** A live
//! server's profile is not reaped *only* because the gate sees its process: in
//! the product that process is named `tinio`, and a launcher renamed to something
//! else — or a confined child started by a launcher whose image is not `tinio` —
//! is invisible to the gate. A machine that never launches this launcher again
//! keeps its residue. And this module never removes a filesystem path itself: it
//! names the profile through [`delete_appcontainer_profile`] and the key to the
//! registry, and what those do with a directory that has been replaced by a reparse
//! point is their own behaviour.
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14): the Toolhelp process walk, the `Storage` key listing and the registry
//! deletion are raw Win32 reached through `windows-sys`, and every unsafe block
//! below carries its safety argument.

#![allow(unsafe_code)]

use std::{
    env, fs, io,
    mem::size_of,
    path::{Path, PathBuf},
    ptr::{null, null_mut},
    sync::Once,
};

use tinio_util::wide::to_wide;
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_FILE_NOT_FOUND, ERROR_NO_MORE_ITEMS, ERROR_SUCCESS, GetLastError,
        INVALID_HANDLE_VALUE,
    },
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
            TH32CS_SNAPPROCESS,
        },
        Registry::{
            HKEY, HKEY_CURRENT_USER, KEY_READ, RegCloseKey, RegDeleteKeyW, RegDeleteTreeW,
            RegEnumKeyExW, RegOpenKeyExW,
        },
        Threading::GetCurrentProcessId,
    },
};

use crate::sid::{Deleted, PACKAGE_PREFIX, ProfileLock, delete_appcontainer_profile};

/// The `step` every event this module emits carries.
///
/// **One step for the whole sweep, chosen so the mechanism has one greppable
/// record**: the summary line, every skip and every failure are
/// `step="sweep the stale profiles"`, and what distinguishes them is the level and
/// the fields. A reader asking "did this machine reap anything, and if not, why
/// not" reads that one string.
const SWEEP_STEP: &str = "sweep the stale profiles";

/// The directory under `%LOCALAPPDATA%` that holds one directory per profile —
/// **the one place this module lists directories**.
const PACKAGES_DIRECTORY: &str = "Packages";

/// The registry subkey under `HKCU` that holds one key per package name, and the
/// second place a name is a name.
///
/// **It is not `HKCU\Software\Microsoft\…`**: the AppContainer profile store lives
/// under the `Classes` branch of the user hive, which is what the design's residue
/// row and an operator's own package listings spell
/// `HKCU\Software\Classes\Local Settings\Software\Microsoft\Windows\CurrentVersion\AppContainer\Storage`.
const STORAGE_KEY: &str = r"Software\Classes\Local Settings\Software\Microsoft\Windows\CurrentVersion\AppContainer\Storage";

/// The image base name of a confined server — **the whole of what the liveness
/// gate can see**, and the launcher's own name because the launcher re-invokes its
/// own image. Bytes, because [`image_is_the_launcher`] compares them in the
/// snapshot's own UTF-16 field without building a `String`.
///
/// **TODO — this name is a stand-in, and the object the crate already has is the
/// real answer.** [`ProfileLock`] is one kernel mutex per served root
/// (`Global\tinio.profile.<package>`), and the only thing that makes it a
/// *window's* lock rather than a *profile's* is where it is held: the launcher
/// takes it in `prepare_launch` before the profile is derived and releases it at
/// `CreateProcessW`, and [`crate::sid::PackageSid`]'s `Drop` takes it again around
/// the delete. Hold that object — or the same named object under a second name, if
/// the window's exclusion should not become the child's lifetime — for the
/// profile's **whole life, in the process that lives under it**, and rule 3's
/// non-blocking try alone tells live from residue: a name whose lock is held has a
/// server behind it, a name whose lock is free is residue, and this constant,
/// [`image_is_the_launcher`] and the machine-wide walk [`other_tinio_processes`]
/// makes can all go.
///
/// **That holder has to be the confined child, which is the reason the change is
/// not made here.** The launcher is not the process a profile is kept alive for:
/// the server started inside the AppContainer is the one that lives under the
/// profile, so taking the lock there is a change on the child's side of the launch
/// rather than a change to the sweep. Until it lands, the launch path's only view
/// of a live server is this name.
const LAUNCHER_IMAGE: &[u8] = b"tinio";

/// The two places one profile name appears, resolved together — **the seam a leg
/// points at a temporary directory and a scratch registry key**, because the
/// product path resolves both itself.
///
/// A struct rather than two reads inside the sweep, and that is what keeps a test
/// off the operator's machine: a sweep handed a scratch `packages` directory and a
/// scratch `storage` path can only find the names a leg put there, while the
/// directory removal [`delete_appcontainer_profile`] performs stays the product call
/// it is (the name it is given is the fixture's own, derived from a temporary
/// root).
struct Places {
    /// Where a profile's own directory lives: `%LOCALAPPDATA%\Packages`.
    packages: PathBuf,
    /// The `HKCU` subkey one level above the per-name keys.
    storage: String,
}

impl Places {
    /// The product's two places, or `None` when `%LOCALAPPDATA%` is unset — the
    /// same degradation [`crate::sid::state_directory`] answers `None` for.
    fn product() -> Option<Self> {
        let packages =
            env::var_os("LOCALAPPDATA").map(|base| PathBuf::from(base).join(PACKAGES_DIRECTORY))?;
        Some(Self {
            packages,
            storage: STORAGE_KEY.to_owned(),
        })
    }
}

/// What one sweep did, as the numbers the summary line carries.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Sweep {
    /// Names reaped: every half this call was asked to remove is gone.
    reaped: usize,
    /// Names skipped because their lock could not be taken.
    locked: usize,
    /// Names a half of which could not be removed — warned about one by one.
    failed: usize,
    /// How many other `tinio` processes the gate saw. `0` when the walk itself
    /// failed, which `skipped` tells apart from a genuine zero.
    others: usize,
    /// Why the sweep did not run at all, when it did not.
    skipped: Option<Skipped>,
}

/// Why a sweep that did not run did not run — **both reasons are "the machine is
/// not provably free of a live server"**, which is the side to err on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skipped {
    /// Another process named `tinio` is alive (rule 2).
    AnotherLauncher,
    /// The process list could not be walked, so "no other `tinio`" could not be
    /// shown at all (rule 2, the failed-reading side).
    UnknownProcesses,
}

impl Skipped {
    /// The spelling the summary line carries as its `skipped` field.
    fn reason(self) -> &'static str {
        match self {
            Self::AnotherLauncher => "another tinio process is alive",
            Self::UnknownProcesses => "the process list could not be walked",
        }
    }
}

/// The process check one sweep runs — **the seam a leg drives, and the product's
/// own walk**.
///
/// The gate's rule is about the machine's real process list and the product path
/// always uses [`other_tinio_processes`]. A leg whose subject is something else —
/// the two listings, the per-name lock, the two halves — states `Ok(0)` instead,
/// so a `tinio` running elsewhere on the machine (an operator's own server,
/// another test binary's confined child) cannot turn that leg into an assertion
/// about a sweep that never ran. The gate itself is asserted against the real
/// walk in `the_liveness_gate_skips_the_sweep_while_another_tinio_is_alive`.
type ProcessCheck = fn() -> io::Result<usize>;

/// Reap the stale profiles, **once per process** — the launch path's entry.
///
/// Called from [`crate::backend::prepare_launch`] before the served root derives
/// its package name, so the profile this launch is about to create cannot be a
/// candidate: it does not exist yet. [`Once`] and not a check inside [`sweep`],
/// because what the guard bounds is the *cost* (one enumeration of two machine-wide
/// lists, then two removes per stale name) on a path that runs once per launch: a
/// second launch in this process has already had this machine's residue state pass
/// through one sweep, and residue that appeared since is the next process's
/// business.
///
/// **A gated sweep is not retried**, which is the one visible consequence of that:
/// a launcher that starts while another `tinio` is alive sweeps nothing, and the
/// next *process* is the one that reaps.
///
/// Nothing here can fail a launch: an unresolvable `%LOCALAPPDATA%` and every
/// error the sweep itself meets are warnings, and the caller has no `Result` to
/// check.
#[tracing::instrument(target = "tinio::sandbox", level = "info", skip_all)]
pub(crate) fn stale_profiles() {
    static SWEPT: Once = Once::new();
    SWEPT.call_once(|| {
        let Some(places) = Places::product() else {
            tracing::warn!(
                step = SWEEP_STEP,
                variable = "LOCALAPPDATA",
                "no package directory could be resolved, so this run reaps none of \
                 the profiles an unclean exit leaves (design decision 21)"
            );
            return;
        };
        sweep(&places);
    });
}

/// One sweep over `places`, with the product's own process walk.
fn sweep(places: &Places) -> Sweep {
    sweep_with(places, other_tinio_processes)
}

/// One sweep, and its one summary line.
///
/// The rules are the module's; what this function adds is the order they are asked
/// in: the gate first (it can only *stop* the sweep), then both listings, then per
/// name the lock try and the two removes. The summary is emitted here rather than
/// by the caller so a leg driving [`sweep_with`] records the line the product
/// writes.
fn sweep_with(places: &Places, processes: ProcessCheck) -> Sweep {
    let mut report = Sweep::default();
    // **The gate, and the reason it is a process count rather than a lock**: a
    // confined server holds no lock, so the only thing that can see one is that it
    // is a `tinio` process (rule 2). The walk failing is the *unsafe* direction to
    // read as zero, so it skips the sweep rather than proceeding on "no evidence".
    match processes() {
        Ok(others) => report.others = others,
        Err(error) => {
            report.skipped = Some(Skipped::UnknownProcesses);
            tracing::warn!(
                step = SWEEP_STEP,
                error = %error,
                "the process list could not be walked, so this launch cannot show \
                 that no confined server is alive: no stale profile is reaped, and \
                 the launch continues (design decision 21)"
            );
        }
    }
    if report.skipped.is_none() && report.others > 0 {
        report.skipped = Some(Skipped::AnotherLauncher);
    }
    if report.skipped.is_none() {
        for name in candidates(places) {
            // **The lock is the peer's window, and it is the only exclusion the
            // product has for one name** (rule 3). It is released at
            // `CreateProcessW`, so this says "no launcher is between its derive and
            // its child" and says nothing about a child that already exists —
            // which is what the gate above is for.
            let Some(_window) = ProfileLock::try_acquire(&name) else {
                report.locked += 1;
                tracing::debug!(
                    step = SWEEP_STEP,
                    name = %name,
                    "a peer holds this name's lock, so its profile and key are left \
                     alone"
                );
                continue;
            };
            if reap(places, &name) {
                report.reaped += 1;
                tracing::debug!(step = SWEEP_STEP, name = %name, "reaped");
            } else {
                report.failed += 1;
            }
        }
    }
    tracing::info!(
        step = SWEEP_STEP,
        reaped = report.reaped,
        locked = report.locked,
        failed = report.failed,
        other_tinio_processes = report.others,
        skipped = report.skipped.map(Skipped::reason),
    );
    report
}

/// Every name this crate owns in either of `places`' two listings — the union of
/// the directory names and the key names, each filtered by [`PACKAGE_PREFIX`]
/// (rule 1), sorted so two runs of one machine sweep in one order and deduplicated
/// so a name that is in both places is one candidate.
///
/// **A listing that cannot be read is a warning and an empty half, not a failed
/// sweep**: the other half is still worth reaping, and a `Packages` directory that
/// is not there (a machine where nothing has ever launched a container) is a
/// steady state rather than an error. The prefix is what keeps a wrong reading
/// harmless: even a listing that returned something unexpected yields only names
/// this crate derives.
fn candidates(places: &Places) -> Vec<String> {
    let mut names = directory_names(&places.packages);
    names.extend(storage_names(&places.storage));
    names.sort_unstable();
    names.dedup();
    names
}

/// The names under `packages` that are this crate's — **directories only**.
///
/// The prefix is the whole claim this module makes about a name, and a *directory*
/// under `Packages` with that prefix can only have been made by
/// [`crate::sid::PackageSid::derive`], which is the creation the profile's own
/// directory comes from. A file with the same name is skipped: it is not a
/// profile's directory. A name that is not valid UTF-8 is skipped for the same
/// reason a directory named `Other` is — every name this crate derives is ASCII.
fn directory_names(packages: &Path) -> Vec<String> {
    let listing = match fs::read_dir(packages) {
        Ok(listing) => listing,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(error) => {
            tracing::warn!(
                step = SWEEP_STEP,
                path = %packages.display(),
                error = %error,
                "the package directory could not be listed, so no profile directory \
                 is a candidate this run; the Storage keys still are"
            );
            return Vec::new();
        }
    };
    let mut names = Vec::new();
    for entry in listing {
        let Ok(entry) = entry else {
            tracing::warn!(
                step = SWEEP_STEP,
                path = %packages.display(),
                "an entry of the package directory could not be read and is not a \
                 candidate; the rest of the listing is"
            );
            continue;
        };
        // The prefix is tested on the borrowed name — see the doc above — so the
        // allocation happens only for the handful this crate ever wrote.
        let file = entry.file_name();
        let Some(name) = file
            .to_str()
            .filter(|name| name.starts_with(PACKAGE_PREFIX))
        else {
            continue;
        };
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => names.push(name.to_owned()),
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    step = SWEEP_STEP,
                    path = %entry.path().display(),
                    error = %error,
                    "an entry's type could not be read, so it is not a candidate \
                     and nothing is removed for it"
                );
            }
        }
    }
    names
}

/// The subkeys of the `Storage` key that are this crate's names.
///
/// A key that is not there is the empty list. Anything else is a warned empty
/// list: the caller still has the directory half, and a listing that stopped
/// halfway leaves the names it did read as candidates.
fn storage_names(storage: &str) -> Vec<String> {
    let key = match open_key(storage) {
        Ok(Some(key)) => key,
        Ok(None) => return Vec::new(),
        Err(error) => {
            tracing::warn!(
                step = SWEEP_STEP,
                key = %storage,
                error = %error,
                "the profile registry key could not be opened, so no key name is a \
                 candidate this run; the profile directories still are"
            );
            return Vec::new();
        }
    };
    let mut names = Vec::new();
    let mut index = 0u32;
    loop {
        // `RegEnumKeyExW` writes the name without its terminator and reports the
        // length it wrote; 255 units is `MAX_PATH`'s own room and many times a
        // package name (the derivation is 30 characters).
        let mut units = [0u16; 255];
        let mut length = units.len() as u32;
        // SAFETY: `key` is the open key above and stays open for the loop; `units`
        // is a writable array of `length` units that stays alive for the call, and
        // every optional out-parameter is a null pointer, which the call documents
        // as "not wanted".
        let status = unsafe {
            RegEnumKeyExW(
                key,
                index,
                units.as_mut_ptr(),
                &mut length,
                null(),
                null_mut(),
                null_mut(),
                null_mut(),
            )
        };
        if status == ERROR_NO_MORE_ITEMS {
            break;
        }
        if status != ERROR_SUCCESS {
            tracing::warn!(
                step = SWEEP_STEP,
                key = %storage,
                index = index,
                error = %io::Error::from_raw_os_error(status as i32),
                "the profile registry key could not be listed to its end, so the \
                 names read before this point are candidates and the rest are not"
            );
            break;
        }
        index += 1;
        let name = String::from_utf16_lossy(&units[..length as usize]);
        if name.starts_with(PACKAGE_PREFIX) {
            names.push(name);
        }
    }
    // SAFETY: `key` is the handle `open_key` returned and is closed exactly once,
    // here.
    unsafe { RegCloseKey(key) };
    names
}

/// The handle `RegOpenKeyExW` gives for a `HKCU` subkey, or `None` when it is not
/// there.
///
/// # Errors
///
/// The status the call returned for anything that is not "there" or "not there" —
/// the caller warns with it and carries on with the other half of the sweep.
fn open_key(path: &str) -> io::Result<Option<HKEY>> {
    let wide = to_wide(path);
    let mut key: HKEY = null_mut();
    // SAFETY: `wide` is a NUL-terminated UTF-16 buffer alive for the call, `key` is
    // a valid out-parameter, and the handle it returns is either closed by the
    // caller or left to the error path below (nothing to close when the call
    // failed).
    let status = unsafe { RegOpenKeyExW(HKEY_CURRENT_USER, wide.as_ptr(), 0, KEY_READ, &mut key) };
    match status {
        ERROR_SUCCESS => Ok(Some(key)),
        ERROR_FILE_NOT_FOUND => Ok(None),
        other => Err(io::Error::from_raw_os_error(other as i32)),
    }
}

/// Reap one name — **both halves, whichever of them is there** (rule 4).
///
/// `true` when every half this call was asked to remove is gone. The two removes
/// are attempted independently, so a profile the delete could not take does not
/// stop the key from going and vice versa; a half that is already gone is success
/// rather than a failure ([`delete_profile`] and [`delete_storage_key`] each say
/// why), and a half that is refused is warned about at the sweep's step and counted
/// as a failure.
fn reap(places: &Places, name: &str) -> bool {
    let profile = delete_profile(name);
    let key = delete_storage_key(&places.storage, name);
    profile && key
}

/// The profile half of a reap — [`delete_appcontainer_profile`] for `name`, the
/// call that takes `%LOCALAPPDATA%\Packages\<name>` with it.
///
/// **`true` is "no profile with this name is left"**, not "this call removed one":
/// [`Deleted::Gone`] is what the API answers for a name that was never created
/// (measured 2026-09-26; [`crate::sid::PackageSid`]'s own `Drop` records the reading
/// for the same call), so the half that is there is the half that goes and the
/// absent one is free rather than a failure. **The refusal is this presentation's
/// obligation**: warned at the sweep's own step and answered `false`, so the sweep
/// continues past a name it could not take (rule 5).
fn delete_profile(name: &str) -> bool {
    match delete_appcontainer_profile(name) {
        Deleted::Gone => true,
        Deleted::Refused(hr) => {
            tracing::warn!(
                step = SWEEP_STEP,
                name = %name,
                hr = hr,
                "a stale profile could not be deleted and may still be on this machine; \
                 the launch continues and the next sweep tries again (design decision 21)"
            );
            false
        }
    }
}

/// The `HKCU\…\Storage\<name>` key and everything under it — **the half a reap
/// cannot leave to the profile delete**, whatever that call does with it today.
///
/// Two calls, and both are needed: `RegDeleteTreeW` removes the values and the
/// subkeys (the `Children` key a profile's own state fills), and `RegDeleteKeyW`
/// removes the key that is left. **Measured on this host 2026-09-28:
/// `DeleteAppContainerProfile` removes the key itself** — on a freshly created
/// profile and on a residue one alike — so on today's Windows this call usually
/// answers `ERROR_FILE_NOT_FOUND`, which is success rather than a failure. It stays
/// because the rule is that both halves of a name go, because a delete that is
/// refused leaves the key standing with the directory, and because a key with no
/// profile at all is a residue a machine can reach that the delete would never be
/// asked about (the leg `either_half_alone_is_reaped` builds exactly that state).
fn delete_storage_key(storage: &str, name: &str) -> bool {
    let path = format!("{storage}\\{name}");
    let wide = to_wide(&path);
    // SAFETY: `wide` is a NUL-terminated UTF-16 buffer alive for both calls, and
    // the path is the sweep's own storage key joined with a candidate name — never
    // a path a caller spelled.
    let status = unsafe { RegDeleteTreeW(HKEY_CURRENT_USER, wide.as_ptr()) };
    if status == ERROR_FILE_NOT_FOUND {
        return true;
    }
    if status != ERROR_SUCCESS {
        tracing::warn!(
            step = SWEEP_STEP,
            key = %path,
            error = %io::Error::from_raw_os_error(status as i32),
            "a stale profile's registry key could not be emptied; the directory half \
             is unaffected and the launch continues (design decision 21)"
        );
        return false;
    }
    // SAFETY: the same buffer and the same path, whose subkeys and values the call
    // above removed.
    let status = unsafe { RegDeleteKeyW(HKEY_CURRENT_USER, wide.as_ptr()) };
    if status != ERROR_SUCCESS && status != ERROR_FILE_NOT_FOUND {
        tracing::warn!(
            step = SWEEP_STEP,
            key = %path,
            error = %io::Error::from_raw_os_error(status as i32),
            "a stale profile's registry key could not be removed; the directory half \
             is unaffected and the launch continues (design decision 21)"
        );
        return false;
    }
    true
}

/// How many processes on this machine are named [`LAUNCHER_IMAGE`] and are not
/// this one — **the liveness gate's whole input** (rule 2), read as the length of
/// [`tinio_processes`].
///
/// # Errors
///
/// The snapshot call's own error. The caller treats it as "unknown" and skips the
/// sweep: a walk that failed cannot show that no server is alive, and the sweep's
/// only safe reading of "cannot show" is to do nothing.
fn other_tinio_processes() -> io::Result<usize> {
    tinio_processes().map(|pids| pids.len())
}

/// The live processes named [`LAUNCHER_IMAGE`], this one excluded — **the same walk as
/// [`other_tinio_processes`], as pids rather than a count.**
///
/// A leg needs the pids and not the count: the liveness-gate test spawns its own `tinio.exe`
/// and waits for *that process* to appear and disappear, and a predicate on the total would
/// make an unrelated `tinio` on the machine either fail the wait or pass it for the wrong
/// reason.
fn tinio_processes() -> io::Result<Vec<u32>> {
    // SAFETY: `CreateToolhelp32Snapshot` takes no pointers; a process snapshot
    // ignores the process id, and the handle it returns is closed exactly once
    // below, on every path out of this function.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        // SAFETY: `GetLastError` reads this thread's last-error value; it takes no
        // arguments and has no preconditions.
        let error = unsafe { GetLastError() };
        return Err(io::Error::from_raw_os_error(error as i32));
    }
    // The call requires `dwSize` to be set by the caller; everything else starts
    // zeroed, which is what the snapshot fills.
    let mut entry = PROCESSENTRY32W {
        dwSize: size_of::<PROCESSENTRY32W>() as u32,
        ..PROCESSENTRY32W::default()
    };
    // SAFETY: `GetCurrentProcessId` reads this process's own id and takes no
    // arguments.
    let this_process = unsafe { GetCurrentProcessId() };
    let mut others = Vec::new();
    // SAFETY: `snapshot` is the live handle above and `entry` is a valid, writable
    // value whose `dwSize` the call requires and this function set.
    let mut walked = unsafe { Process32FirstW(snapshot, &mut entry) };
    while walked != 0 {
        if entry.th32ProcessID != this_process && image_is_the_launcher(&entry.szExeFile) {
            others.push(entry.th32ProcessID);
        }
        // SAFETY: as above: the same snapshot handle and the same live entry.
        walked = unsafe { Process32NextW(snapshot, &mut entry) };
    }
    // SAFETY: `snapshot` is this call's own handle and is closed exactly once.
    unsafe { CloseHandle(snapshot) };
    Ok(others)
}

/// Whether one snapshot entry's image name is the launcher's.
///
/// **The comparison folds case and drops a `.exe` suffix and nothing else**: the
/// snapshot reports the image's base name (`tinio.exe`), the filesystem supplies it
/// in whatever case it was created with, and what rule 2 wants is the image
/// *identity* rather than a path. An image name that is not text, or a name that
/// merely starts with `tinio` (`tinio-server`, `tinio-cli`), is not the launcher
/// and is not a reason to postpone a sweep — the confined child is a copy of the
/// launcher's own image, so it is spelled exactly this way. **What it can recognise
/// is [`LAUNCHER_IMAGE`]'s**: that constant's doc records the limit and the remedy,
/// and this function owns the comparison's shape rather than the choice of what
/// counts as a server.
fn image_is_the_launcher(units: &[u16]) -> bool {
    // The field is NUL-padded, so the name is what precedes the first terminator —
    // **a slice of the caller's field rather than a copy of it**, which is what the
    // doc above means by comparing it where it lies: `collect` here would hand back
    // a `Vec` grown from nothing (a `take_while` reports no size hint) for every
    // process on the machine, for a name that is discarded unless it is the
    // launcher's.
    let end = units
        .iter()
        .position(|unit| *unit == 0)
        .unwrap_or(units.len());
    let name = &units[..end];
    // The suffix is matched folded too, so `TINIO.EXE` is as much the launcher as
    // `tinio.exe` is.
    const EXE: &[u8] = b".exe";
    let stem = match name.len().checked_sub(EXE.len()) {
        Some(cut) if matches_folded(&name[cut..], EXE) => &name[..cut],
        _ => name,
    };
    matches_folded(stem, LAUNCHER_IMAGE)
}

/// Whether `units` is `expected` read case-insensitively — **ASCII-folded, and a
/// unit above ASCII is not the byte it would truncate to**, so it simply fails the
/// comparison.
fn matches_folded(units: &[u16], expected: &[u8]) -> bool {
    units.len() == expected.len()
        && units.iter().zip(expected).all(|(unit, expected)| {
            u8::try_from(*unit).is_ok_and(|byte| byte.to_ascii_lowercase() == *expected)
        })
}

#[cfg(test)]
mod tests {
    use std::{
        mem,
        process::{Child, Command, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use tinio_util::{
        testing::{assert_recorded_at, assert_recorded_below, assert_step, host_cannot},
        win32::system_directory,
    };

    use super::*;
    use crate::{
        sid::{PackageSid, package_name},
        testing::{capture, recorder},
    };

    /// How long a leg waits for a process to appear in, or leave, the snapshot —
    /// the same order as the crate's other process legs, and generous because a
    /// process *creation* is what is being waited on rather than a log line.
    const APPEARANCE: Duration = Duration::from_secs(60);

    /// The check a leg that is not about the gate states: **no other `tinio`
    /// process, by the leg's own word**.
    ///
    /// The product's real walk is asserted where the gate is the subject
    /// (`the_liveness_gate_skips_the_sweep_while_another_tinio_is_alive`); every
    /// other leg drives this, so that a `tinio` running elsewhere on the machine —
    /// an operator's server, another test binary's confined child — cannot gate the
    /// sweep under it and turn a leg into an assertion about a pass that never ran.
    fn no_other_tinio() -> io::Result<usize> {
        Ok(0)
    }

    /// A real process named `tinio.exe`, alive for longer than any leg needs it —
    /// **the honest way to drive the liveness gate**: the gate's input is the
    /// machine's process list, so a leg that wants the gate to fire has to give the
    /// machine a process the gate can see, and then assert the count the product
    /// reads.
    ///
    /// A copy rather than a stub, because the name is the whole signal: `ping.exe`
    /// renamed to `tinio.exe` is a process whose image base name is `tinio.exe` and
    /// which stays alive on its own (`-n 30` is thirty pings to the loopback
    /// address, a second apart) while the leg runs. A leg that cannot spawn it fails
    /// rather than skips: a host that cannot give the gate its input would leave
    /// rule 2 unasserted.
    struct FakeTinio {
        child: Child,
        _home: tempfile::TempDir,
    }

    impl FakeTinio {
        fn spawn() -> Self {
            let home = tempfile::tempdir().expect("temp dir");
            let image = home.path().join("tinio.exe");
            fs::copy(system_directory().join("ping.exe"), &image)
                .expect("a copy of ping.exe named tinio.exe");
            let child = Command::new(&image)
                .args(["-n", "30", "127.0.0.1"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("the renamed process starts");
            Self { child, _home: home }
        }

        /// The process's own id, so a failure names what was running.
        fn id(&self) -> u32 {
            self.child.id()
        }
    }

    impl Drop for FakeTinio {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// Wait for `check` to hold, polling — this crate's process legs share the
    /// idiom, kept local because the subject is a count rather than a file.
    fn wait_until(what: &str, mut check: impl FnMut() -> bool) {
        let deadline = Instant::now() + APPEARANCE;
        while Instant::now() < deadline {
            if check() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("{what} did not happen within {APPEARANCE:?}");
    }

    /// One leg's scratch [`Places`]: a temporary `Packages` directory and a key
    /// tree of its own under `HKCU\Software\tinio-sweep-tests-<unique>\Storage`,
    /// removed when the value drops.
    ///
    /// **A leg must not point a sweep at the operator's two real places**, because
    /// a sweep reaps *every* name it can enumerate there — the operator's own
    /// residue, the profiles of tests running beside this one, and the 815 the
    /// design's residue row records. The seed is the temporary directory's unique
    /// name, so two legs of two runs never share a scratch key.
    struct Scratch {
        _home: tempfile::TempDir,
        places: Places,
    }

    impl Scratch {
        fn new() -> Self {
            let home = tempfile::tempdir().expect("temp dir");
            let seed = home
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .expect("a temporary directory's name is text")
                .to_owned();
            let places = Places {
                packages: home.path().join("Packages"),
                storage: format!(r"Software\tinio-sweep-tests-{seed}\Storage"),
            };
            fs::create_dir_all(&places.packages).expect("the scratch package directory");
            Self {
                _home: home,
                places,
            }
        }

        /// A directory of this leg's, under the scratch package directory, named
        /// `name` — what a profile's directory looks like to the listing.
        fn directory(&self, name: &str) -> PathBuf {
            let path = self.places.packages.join(name);
            fs::create_dir_all(&path).expect("a scratch profile directory");
            path
        }

        /// A key of this leg's, under the scratch `Storage` key, named `name` — what
        /// a profile's own key looks like to the listing.
        fn key(&self, name: &str) -> String {
            let path = format!("{}\\{name}", self.places.storage);
            create_key(&path);
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            // The whole scratch tree, so the shared parent under `Software` is the
            // only thing two legs have in common and neither removes it.
            let root = self
                .places
                .storage
                .strip_suffix(r"\Storage")
                .expect("the scratch storage path ends in Storage")
                .to_owned();
            delete_key_tree(&root);
        }
    }

    /// `RegCreateKeyExW` on `path` under `HKCU`, for a fixture — the registry's own
    /// create, so a leg writes the shape a profile's creation writes.
    fn create_key(path: &str) {
        use windows_sys::Win32::System::Registry::{KEY_SET_VALUE, RegCreateKeyExW};
        let wide = to_wide(path);
        let mut key: HKEY = null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, `key` is a valid
        // out-parameter, the class and security-descriptor pointers are null (the
        // documented "none"), and the handle is closed below.
        let status = unsafe {
            RegCreateKeyExW(
                HKEY_CURRENT_USER,
                wide.as_ptr(),
                0,
                null(),
                0,
                KEY_READ | KEY_SET_VALUE,
                null(),
                &mut key,
                null_mut(),
            )
        };
        assert_eq!(status, ERROR_SUCCESS, "RegCreateKeyExW: {path}");
        // SAFETY: `key` was just returned by the create and is closed once.
        unsafe { RegCloseKey(key) };
    }

    /// `RegDeleteTreeW` + `RegDeleteKeyW` on a subkey path under `HKCU`, used only
    /// to clean a fixture up.
    fn delete_key_tree(path: &str) {
        let wide = to_wide(path);
        // SAFETY: `wide` is NUL-terminated and alive for both calls; the path is a
        // scratch key this module built, never a caller's.
        let status = unsafe { RegDeleteTreeW(HKEY_CURRENT_USER, wide.as_ptr()) };
        if status != ERROR_SUCCESS && status != ERROR_FILE_NOT_FOUND {
            return;
        }
        // SAFETY: the same buffer, whose subkeys and values the call above removed.
        unsafe { RegDeleteKeyW(HKEY_CURRENT_USER, wide.as_ptr()) };
    }

    /// Whether a key exists, read back through `RegOpenKeyExW` rather than inferred
    /// from the sweep's own calls — the oracle for the key half of every leg.
    fn key_is_there(path: &str) -> bool {
        match open_key(path) {
            Ok(Some(key)) => {
                // SAFETY: `key` was just opened and is closed once.
                unsafe { RegCloseKey(key) };
                true
            }
            Ok(None) => false,
            Err(error) => panic!("RegOpenKeyExW: {path}: {error}"),
        }
    }

    /// A served root no other leg uses, and the name this crate derives for it. The
    /// name is what every fixture is about, and it is unique per call because the
    /// temporary directory is.
    fn unclaimed_root() -> (tempfile::TempDir, String) {
        let root = tempfile::tempdir().expect("temp dir");
        let name = package_name(&root.path().join("served"));
        (root, name)
    }

    /// The real directory of a name, as the product's own creation leaves it.
    fn real_directory(name: &str) -> PathBuf {
        Places::product().expect("LOCALAPPDATA").packages.join(name)
    }

    /// The real `Storage` key of a name.
    fn real_key(name: &str) -> String {
        format!("{STORAGE_KEY}\\{name}")
    }

    /// **Residue a run that died without unwinding leaves is reaped on both
    /// halves** — the whole subject of this module, and the one leg whose fixture is
    /// the product's own creation rather than a stand-in.
    ///
    /// The fixture is made the way the failure makes it: [`PackageSid::derive`]
    /// creates the profile, and [`mem::forget`] gives the value up *without* running
    /// the `Drop` that would delete it — which is exactly the state a killed
    /// launcher, an aborted test or a stack-overflow abort leaves, and the state the
    /// design's residue row measured 815 times on this host. Both halves are
    /// asserted present before the sweep, so a sweep that did nothing cannot pass,
    /// and both are asserted gone after it, so a sweep that made only one of its two
    /// removes cannot pass either.
    ///
    /// It would fail without [`reap`]'s two calls in either direction: without
    /// [`delete_profile`] the directory stays (that call is what takes it, and it is
    /// also what takes the key today), and without [`delete_storage_key`] a key that
    /// outlived its profile — the state [`either_half_alone_is_reaped`] builds —
    /// would never go, because only a call that names the key removes one.
    ///
    /// The scratch places are what the name is enumerated from, so this leg cannot
    /// reap anything but its own fixture; the directory half is the real profile the
    /// fixture created, which is what makes the assertion about
    /// `DeleteAppContainerProfile` rather than about a listing.
    #[test]
    fn a_residue_an_unclean_exit_left_is_reaped_on_both_halves() {
        let (_root, name) = unclaimed_root();
        let directory = real_directory(&name);
        let key = real_key(&name);
        // The unclean exit: the value exists and is given up without its `Drop`.
        mem::forget(PackageSid::derive(&name).expect("the profile is created"));
        assert!(
            directory.is_dir(),
            "the fixture did not leave a profile directory, so the leg would prove \
             nothing: {}",
            directory.display()
        );
        assert!(
            key_is_there(&key),
            "the fixture's key is not there, so the key half would assert nothing"
        );

        let scratch = Scratch::new();
        let scratch_key = scratch.key(&name);
        let report = sweep_with(&scratch.places, no_other_tinio);

        assert_eq!(
            report,
            Sweep {
                reaped: 1,
                ..Sweep::default()
            },
            "one name was to be reaped and nothing else was to happen"
        );
        assert!(
            !directory.is_dir(),
            "the stale profile's directory is still there: {}",
            directory.display()
        );
        assert!(
            !key_is_there(&key),
            "the stale profile's registry key is still there: {key}"
        );
        assert!(
            !key_is_there(&scratch_key),
            "the scratch key is still there, so the name was not reaped"
        );
    }

    /// **A name whose lock a peer holds is skipped, and the same name is reaped once
    /// the peer is done** — rule 3, in the one shape that can be asserted without a
    /// race.
    ///
    /// The lock is the exclusion the product has for a launch window, so a sweep
    /// that ignored it could delete the profile a peer is between its derive and its
    /// `CreateProcessW` on — the one interleaving that turns a launch of a program
    /// that exists into `err=2 ERROR_FILE_NOT_FOUND` (design decision 21's window).
    /// **The holder is another thread on purpose**: a mutex is recursive for its
    /// owner, so a leg that held the lock on its own thread and drove a sweep from
    /// that same thread would see the try succeed and assert the opposite of the
    /// rule.
    ///
    /// It would fail without [`ProfileLock::try_acquire`] being non-blocking in
    /// *both* directions: a sweep that used [`ProfileLock::acquire`] would block here
    /// until the peer released (the leg's second sweep could not run at all), and one
    /// that took no lock at all would reap the name while the peer holds it — which
    /// the first half reads off the profile directory, the scratch key and the
    /// report's `locked` count.
    #[test]
    fn a_locked_name_is_skipped_until_the_lock_is_released() {
        let (_root, name) = unclaimed_root();
        let directory = real_directory(&name);
        mem::forget(PackageSid::derive(&name).expect("the profile is created"));
        assert!(directory.is_dir(), "the fixture left no residue to protect");

        let scratch = Scratch::new();
        let scratch_key = scratch.key(&name);
        let (holding_tx, holding_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let held = name.clone();
        // `ProfileLock` is `!Send` (a raw handle), so the lock lives on the thread
        // that took it and the leg talks to that thread instead of moving the value.
        let peer = thread::spawn(move || {
            let window = ProfileLock::acquire(&held).expect("the per-root lock");
            holding_tx.send(()).expect("the peer holds the lock");
            release_rx.recv().expect("the leg releases the lock");
            drop(window);
        });
        holding_rx
            .recv_timeout(APPEARANCE)
            .expect("the peer took the lock");

        let report = sweep_with(&scratch.places, no_other_tinio);
        assert_eq!(
            report,
            Sweep {
                locked: 1,
                ..Sweep::default()
            },
            "the name was not skipped for its lock"
        );
        assert!(
            directory.is_dir(),
            "the profile was reaped while a peer held its lock: {}",
            directory.display()
        );
        assert!(
            key_is_there(&scratch_key),
            "the key was reaped while a peer held the name's lock: {scratch_key}"
        );

        release_tx.send(()).expect("the peer releases");
        peer.join().expect("the peer thread");
        let report = sweep_with(&scratch.places, no_other_tinio);
        assert_eq!(
            report.reaped, 1,
            "the name is reaped once the lock is free: {report:?}"
        );
        assert!(
            !directory.is_dir(),
            "the residue is still there after the peer released: {}",
            directory.display()
        );
        assert!(!key_is_there(&scratch_key));
    }

    /// **The liveness gate stops the whole sweep while another `tinio` process is
    /// alive, and the residue is reaped once the gate opens** — rule 2, driven
    /// against the machine's real process list rather than a stand-in.
    ///
    /// The gate's input is the process list, so no check is injected: the leg makes a
    /// real process whose image is `tinio.exe` ([`FakeTinio`]), waits until the
    /// product's own walk reports it, and then asserts the two things a live confined
    /// server would need — that the sweep does nothing while it is there, and that
    /// the gate is open again once it is gone. **The residue is real**: a profile the
    /// fixture left behind, whose survival of the gated sweep is read off its own
    /// directory.
    ///
    /// **The leg's precondition is the machine's, and it says so**: the walk must
    /// report no other `tinio` before the leg starts, or there is no way to tell the
    /// fake's process from a real worker's — that is [`host_cannot`]'s case, and it
    /// fails rather than skips unless the operator opted out. Once the fake is gone,
    /// the reap half drives [`no_other_tinio`], because what it asserts is that
    /// *nothing beyond the gate* stops the sweep; the gate's own reading is the first
    /// half's.
    ///
    /// It would fail without the gate: a sweep that reaped while another `tinio` was
    /// alive would delete a running server's profile (the server holds no lock — the
    /// launcher releases it at `CreateProcessW`), and the first half reads exactly
    /// that off the fixture's directory and the scratch key. **The second half is
    /// what makes the first mean something**: a gate that never opens — how a rule-2
    /// bug in the other direction would look — fails the reap assertion after the
    /// fake is gone.
    #[test]
    fn the_liveness_gate_skips_the_sweep_while_another_tinio_is_alive() {
        let before = other_tinio_processes().expect("the process list is walkable");
        if before > 0 {
            host_cannot(
                "the liveness gate's leg",
                &format!("{before} other tinio process(es) are already alive"),
                ": stop them, or run this leg on a machine where no tinio is serving",
            );
            return;
        }
        let (_root, name) = unclaimed_root();
        let directory = real_directory(&name);
        mem::forget(PackageSid::derive(&name).expect("the profile is created"));
        assert!(directory.is_dir(), "the fixture left no residue");

        let fake = FakeTinio::spawn();
        wait_until("the gate's walk reports the renamed process", || {
            tinio_processes()
                .expect("the process list is walkable")
                .contains(&fake.id())
        });

        let scratch = Scratch::new();
        let scratch_key = scratch.key(&name);
        let report = sweep(&scratch.places);
        assert_eq!(
            report.skipped,
            Some(Skipped::AnotherLauncher),
            "the sweep ran while another tinio process was alive"
        );
        assert!(report.others > 0, "the gate saw no process: {report:?}");
        assert_eq!(report.reaped, 0);
        assert!(
            directory.is_dir(),
            "a live server's profile would have been reaped: {} (the fake tinio is \
             pid {})",
            directory.display(),
            fake.id()
        );
        assert!(key_is_there(&scratch_key));

        // **This fake's pid and not the count**: the wait is about the process this leg
        // spawned, and a `tinio` serving elsewhere on the machine would otherwise keep the
        // predicate false (or, before the spawn, make it true for the wrong reason).
        let fake_pid = fake.id();
        drop(fake);
        wait_until("the walk stops seeing the renamed process", || {
            !tinio_processes()
                .expect("the process list is walkable")
                .contains(&fake_pid)
        });
        let report = sweep_with(&scratch.places, no_other_tinio);
        assert_eq!(
            report.reaped, 1,
            "the residue is not reaped once the gate opens: {report:?}"
        );
        assert!(!directory.is_dir());
        assert!(!key_is_there(&scratch_key));
    }

    /// **Nothing that is not a `tinio.sandbox.*` name is looked at, let alone
    /// removed** — rule 1, in both listings at once.
    ///
    /// `Packages` holds one directory per installed package on this machine and
    /// `Storage` one key per profile, so a filter that was wrong by one character
    /// would delete the profile of any package on the machine. The fixture spells
    /// every near miss there is: a foreign package's directory and key, a name that
    /// is the prefix without its dot, a name that continues the prefix with a letter
    /// (`tinio.sandboxx.…`), and a *file* named like one of ours.
    ///
    /// It would fail without [`candidates`]' prefix filter and [`directory_names`]'
    /// directory check: an unfiltered listing would reap the foreign directory (its
    /// name reaches `DeleteAppContainerProfile`, and its stand-in directory is read
    /// back here) and the foreign key would be removed by the `Storage` half.
    #[test]
    fn nothing_outside_the_crates_own_names_is_a_candidate() {
        let scratch = Scratch::new();
        let foreign_directory = scratch.directory("Microsoft.WindowsStore_8wekyb3d8bbwe");
        let bare_prefix = scratch.directory(PACKAGE_PREFIX.trim_end_matches('.'));
        let longer_prefix = scratch.directory("tinio.sandboxx.0123456789abcdef");
        let foreign_key = scratch.key("1527c705-839a-4832-9118-54d4bd6a0c89_cw5n1h2txyewy");
        // A file whose name is one of ours: the listing must not treat it as a
        // profile's directory, and nothing here removes paths of its own in any case.
        let ours_but_a_file = scratch
            .places
            .packages
            .join("tinio.sandbox.0123456789abcdef");
        fs::write(&ours_but_a_file, b"not a profile").expect("seed the file");
        // The two names of this crate's: one that is a directory *and* a key (one
        // candidate, because the union deduplicates) and one that is a key alone.
        let ours_directory = scratch.directory("tinio.sandbox.fedcba9876543210");
        let ours_directory_key = scratch.key("tinio.sandbox.fedcba9876543210");
        let ours_key = scratch.key("tinio.sandbox.0011223344556677");

        let report = sweep_with(&scratch.places, no_other_tinio);

        assert_eq!(
            report.reaped, 2,
            "the two names of this crate — one from each listing — are the ones \
             reaped: {report:?}"
        );
        for (what, path) in [
            ("a foreign package directory", &foreign_directory),
            ("a directory named with the prefix alone", &bare_prefix),
            (
                "a directory whose name only starts like ours",
                &longer_prefix,
            ),
        ] {
            assert!(path.is_dir(), "{what} was removed: {}", path.display());
        }
        assert!(ours_but_a_file.is_file(), "the file is not a candidate");
        assert!(
            !key_is_there(&ours_directory_key),
            "this crate's own name is the one the key half goes for: \
             {ours_directory_key}"
        );
        assert!(!key_is_there(&ours_key));
        assert!(
            key_is_there(&foreign_key),
            "a foreign package's registry key was removed: {foreign_key}"
        );
        // The stand-in directory of this crate's own name is not removed by the
        // sweep: the directory half is `DeleteAppContainerProfile`'s, and this name
        // has no real profile behind it. That is what the reap-level legs assert.
        assert!(ours_directory.is_dir());
    }

    /// **Two names that are half of this crate's — one with a key and no profile,
    /// one with a profile and no key — and neither warning as it is reaped** — rule
    /// 4, in the direction that matters now that the two halves' fate is measured.
    ///
    /// **Measured on this host 2026-09-28, and it corrects the design's earlier
    /// reading**: `DeleteAppContainerProfile` removes the `Storage` key *as well as*
    /// `Packages\<name>` — both on a profile that was created and never used
    /// (`derive` → directory true, key true, one `Children` subkey; after the drop,
    /// both false) and on a residue profile a real confined run left behind (the
    /// same, with `hr=0` from the delete). So the residue row's "the deletion path
    /// does not take them with it" was an inference from counts that does not hold,
    /// and the 817 keys beside 817 directories are what *unclean* exits left — each
    /// of them left both halves. **The key removal stays in the reap anyway**: a name
    /// is two things, the operator's rule is that both of them go, and a machine
    /// where a delete is refused or a key outlives its profile is exactly the state
    /// this leg's first half builds by hand.
    ///
    /// It would fail if [`reap`] short-circuited on an absent half — an early return
    /// from either call would leave the other half standing, and each half here is
    /// read back with its own oracle (the directory with `is_dir`, the key with
    /// [`key_is_there`]) — and it would fail if an absent half were reported as a
    /// failed removal, which is the `assert_recorded_below` at the end: a warning on
    /// every name whose other half is simply not there would put a warning in every
    /// operator's log for a steady state.
    #[test]
    fn either_half_alone_is_reaped() {
        let places = Places::product().expect("LOCALAPPDATA");
        let _serial = recorder();
        let capture = capture();
        let recording = capture.record();

        // (1) The key without its directory: the key this crate's own creation
        // writes, under the real `Storage` root, for a name whose profile was never
        // created (the state a machine reaches when a delete is refused, or the half
        // an older Windows might leave).
        let (_root, key_only) = unclaimed_root();
        create_key(&real_key(&key_only));
        assert!(
            !real_directory(&key_only).is_dir(),
            "the fixture has a directory, so this case is the other one"
        );
        assert!(key_is_there(&real_key(&key_only)));
        assert!(
            reap(&places, &key_only),
            "a key without a directory is reaped"
        );
        assert!(
            !key_is_there(&real_key(&key_only)),
            "the key outlived the reap that was asked for it"
        );

        // (2) The directory without its key: the profile left the way an unclean
        // exit leaves it, with the key taken away by the fixture first.
        let (_root, directory_only) = unclaimed_root();
        let directory = real_directory(&directory_only);
        mem::forget(PackageSid::derive(&directory_only).expect("the profile is created"));
        assert!(directory.is_dir());
        assert!(
            delete_storage_key(STORAGE_KEY, &directory_only),
            "the key could not be removed for the fixture"
        );
        assert!(!key_is_there(&real_key(&directory_only)));
        assert!(
            reap(&places, &directory_only),
            "a directory without a key is reaped"
        );
        assert!(
            !directory.is_dir(),
            "the directory outlived the reap that was asked for it: {}",
            directory.display()
        );

        let logged = capture.text();
        drop(recording);
        assert_recorded_below(&logged, tracing::Level::WARN);
    }

    /// **One summary line per sweep, with the counts an operator reads** — rule 6.
    ///
    /// The line is the whole of what a launch leaves behind about its sweep, so it
    /// has to carry the count reaped and the reason a sweep did nothing, and it has
    /// to be *one* line however much residue there was. This leg reads both shapes
    /// off the crate's own recording device: a sweep that reaped one name, and one
    /// that skipped a locked name.
    ///
    /// It would fail if the summary were emitted per name (the line count would rise
    /// with the residue) or not at all (no `INFO` at the sweep's step), and it would
    /// fail if a skipped sweep said nothing — which is the case an operator sees when
    /// they wonder why the 815 did not shrink.
    #[test]
    fn one_summary_line_reports_what_the_sweep_did() {
        let _serial = recorder();
        let capture = capture();
        let recording = capture.record();

        let scratch = Scratch::new();
        scratch.key("tinio.sandbox.0123456789abcdef");
        assert_eq!(sweep_with(&scratch.places, no_other_tinio).reaped, 1);

        let scratch = Scratch::new();
        let name = "tinio.sandbox.0123456789abcdef";
        scratch.directory(name);
        let scratch_key = scratch.key(name);
        let held = name.to_owned();
        let (holding_tx, holding_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let peer = thread::spawn(move || {
            let window = ProfileLock::acquire(&held).expect("the per-root lock");
            holding_tx.send(()).expect("the peer holds the lock");
            release_rx.recv().expect("the leg releases the lock");
            drop(window);
        });
        holding_rx
            .recv_timeout(APPEARANCE)
            .expect("the peer took the lock");
        assert_eq!(sweep_with(&scratch.places, no_other_tinio).locked, 1);
        release_tx.send(()).expect("the peer releases");
        peer.join().expect("the peer thread");
        assert!(key_is_there(&scratch_key));

        let logged = capture.text();
        drop(recording);
        assert_step(&logged, SWEEP_STEP);
        assert_recorded_at(&logged, "reaped=1", tracing::Level::INFO);
        let summaries = logged
            .lines()
            .filter(|line| line.contains(SWEEP_STEP) && line.contains("INFO"))
            .count();
        assert_eq!(
            summaries, 2,
            "one summary line per sweep and not one per name: {logged}"
        );
    }

    /// The one leg about naming rather than removal: [`image_is_the_launcher`] is the
    /// gate's whole judgement about a process, and a fold that was too eager would
    /// read this crate's *test* binaries as servers — leaving the sweep dead on every
    /// developer machine — while one that was too narrow would miss the confined
    /// child.
    ///
    /// It would fail if the comparison were case-sensitive (Windows supplies the
    /// image name in whatever case it was created with), if it kept the `.exe` suffix
    /// (which every snapshot entry carries), or if it matched a prefix
    /// (`tinio-server`, `tinio_sandbox_windows-1234` are not the launcher: the
    /// product's child is a copy of the launcher's own image).
    #[test]
    fn the_launcher_image_is_matched_by_its_own_name() {
        let units = |text: &str| -> Vec<u16> {
            let mut units: Vec<u16> = text.encode_utf16().collect();
            // The snapshot's field is fixed-width and NUL-padded.
            units.resize(260, 0);
            units
        };
        assert!(image_is_the_launcher(&units("tinio.exe")));
        assert!(image_is_the_launcher(&units("TINIO.EXE")));
        assert!(image_is_the_launcher(&units("Tinio.exe")));
        assert!(image_is_the_launcher(&units("tinio")));
        for other in [
            "tinio-server.exe",
            "tinio-cli.exe",
            "tinio_sandbox_windows-1234.exe",
            "not-tinio.exe",
            "tinio.exe.bak",
            "",
        ] {
            assert!(
                !image_is_the_launcher(&units(other)),
                "{other} is not the launcher's image"
            );
        }
    }
}
