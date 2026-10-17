//! **The one lock per granted path, held across the whole read-merge-write.** It is a file
//! lock rather than a mutex because the holder can be another *process* — two confined runs
//! sharing a served root — and the kernel releases it when a holder dies, so a crashed run
//! cannot wedge a directory for the runs after it. A lock that cannot be taken is reported and
//! not fatal: the grant proceeds without the exclusion rather than refusing to confine a
//! server over a lock file.

// **The lock's own Win32 calls are the unsafe code this module owns** — the lock file's
// open, the exclusive hold on it, its release and the handle's close — so the exemption
// sits here rather than on the directory, the rule `descriptor.rs`'s note states.
#![allow(unsafe_code)]

use std::{
    env,
    fmt::Display,
    fs, io,
    path::{Path, PathBuf},
    ptr::{null, null_mut},
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

use tinio_util::wide::to_wide;
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_LOCK_VIOLATION, GENERIC_READ, GENERIC_WRITE, HANDLE,
        INVALID_HANDLE_VALUE,
    },
    Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE,
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, OPEN_ALWAYS, UnlockFileEx,
    },
    System::IO::OVERLAPPED,
};

use crate::{handles, sid::root_digest};

/// The directory under the caller's temp directory the per-path lock files live
/// in, and the extension their names carry. **One file per distinct path ever
/// granted, and they are left behind on purpose** — [`AclLock`]'s doc states why
/// neither reaping them nor sharing delete is sound.
const ACL_LOCK_DIRECTORY: &str = "tinio-acl-locks";
const ACL_LOCK_SUFFIX: &str = ".lock";

/// What an operator reads when the exclusion is not in force — **one sentence for every way
/// [`AclLock::acquire`] can fail to take it**, because what it costs is the same exposure
/// whichever call refused and only the error field differs.
///
/// The `lock` field is the one thing the call sites do not all carry: a failure to resolve the
/// directory has no lock file to name, and inventing the path it would have had would report a
/// file the call never reached.
const NO_LOCK: &str =
    "no per-path ACL lock, so a peer run's concurrent grant to this path may be lost";

/// The lock directory, created — **once per process**, because both of its inputs
/// are process constants and [`AclLock::acquire`] runs once per granted path.
///
/// **Resolving it per call would be the expensive half**: `env::temp_dir()` is a
/// `GetTempPath2W` and an allocation std does not cache, and `create_dir_all` on a
/// directory that is there is a failing `mkdir` plus a `stat`. One grant phase
/// writes an ACE per sibling DLL, so that is the whole set of them re-resolved for
/// a value that cannot have moved.
///
/// A failure is not cached, so the second path reports the same error the first one
/// did rather than a silence.
pub(super) fn lock_directory() -> io::Result<&'static Path> {
    static DIRECTORY: OnceLock<PathBuf> = OnceLock::new();
    match DIRECTORY.get() {
        Some(directory) => Ok(directory),
        None => {
            let directory = env::temp_dir().join(ACL_LOCK_DIRECTORY);
            fs::create_dir_all(&directory)?;
            Ok(DIRECTORY.get_or_init(|| directory))
        }
    }
}

/// How long a waiter may hold the lock before its wait is reported: **one
/// second**, at [`tracing::warn!`].
///
/// **The number is a judgement about which wait is ordinary and which is worth an
/// operator's eye, and both halves of that are measured elsewhere in this
/// module.** The critical section is one descriptor write plus the read that
/// decides it: a satisfied path is a read and no write at all, and a write was
/// measured at **0.136 ms per object** for this workspace's own tree
/// (`Grants`'s doc), so a peer's ordinary grant is milliseconds and a wait of a
/// second means the peer is inside a whole-tree descriptor write — design §6's
/// seconds-per-phase case, the same magnitude [`crate::backend::grant`]'s 15 s
/// phase threshold is set against. A threshold below a second would speak for a
/// peer's routine grant; a threshold at the phase warning would be silent for
/// exactly the wait a second launcher on a shared root is most likely to see.
const ACL_LOCK_WARN_AFTER: Duration = Duration::from_secs(1);

/// The interval a waiter polls at while a peer holds the lock — **the kernel's
/// own quantum, not a spin**: `LockFileEx` is asked again every 10 ms, so a
/// contended wait costs one thread wake per interval and no busy loop, and an
/// uncontended lock costs no sleep at all, because the first attempt takes it.
/// **The sleep blocks the calling thread, and this phase already is a blocking
/// one**: the opens, the descriptor reads and the `icacls` spawn that wrote here
/// until 2026-09-27 are all synchronous calls on the same path, so a waiter that
/// blocks it — for as long as a peer's whole-tree write takes — is the shape the
/// grant phase has rather than a new one.
const ACL_LOCK_POLL: Duration = Duration::from_millis(10);

/// **The one lock per path, held across the whole read-merge-write.**
///
/// **What it serialises is the pair of operations this module makes on one DACL,
/// and the race is real rather than theoretical.** A grant reads the object's
/// descriptor, merges its SID's entry into what it read, and writes the result
/// back; two of those overlapping on one path lose an ACE, because the second
/// writer's merge is built from the descriptor it read *before* the first wrote —
/// so its `SetSecurityInfo` replaces the whole DACL with a copy that never had
/// the peer's entry in it. The two callers that meet here are two confined runs
/// sharing a served root: their package SIDs are per-root and so *different* for
/// different roots, and `SET_ACCESS` only replaces the same SID's entry, which is
/// why the loss is silent — each run's own grant is present, and the other run's
/// is gone.
///
/// **A file lock and not a mutex, because the holder can be a process rather than
/// a thread.** The file is named by [`root_digest`] of the *normalised* path —
/// the same normalisation [`package_name`](crate::sid::package_name) derives under,
/// and deliberately its **unsalted** reading, so two spellings of one directory take
/// one lock rather than one each and a salted run and a degraded one still take
/// **one** lock for one path ([`root_digest`]'s own doc carries that reason) — lives
/// under `tinio-acl-locks\` in the caller's temp
/// directory, is opened without `FILE_SHARE_DELETE`, and is held exclusively for
/// the duration of one [`grant_to`](super::grant::grant_to) or one [`revoke`](super::grant::revoke). **The kernel releases it
/// when the holder dies**, which is the property a lock file cannot have and the
/// reason this is not an advisory marker: a crashed run cannot wedge a directory
/// for every later run, and no stale-lock rule has to be written.
///
/// **The wait is reported rather than bounded.** A holder is inside a real ACL
/// phase, which is seconds on a large tree, so a bounded wait would fail a grant
/// that was about to succeed — this is not the profile lock, whose own bound is
/// the launch window and whose `INFINITE` wait is defensible for exactly that
/// reason (`ProfileLock`'s doc). A waiter therefore keeps polling and emits one
/// [`tracing::warn!`] when its wait crosses [`ACL_LOCK_WARN_AFTER`], naming the
/// path and the time waited; the per-path grant event that follows is where the
/// wait ends. Nothing here is silent: a host that cannot create the lock file or
/// cannot lock it warns with the Win32 status and proceeds **without** the
/// exclusion — the behaviour every run had before this lock existed — because
/// refusing to confine a server over a lock file would trade a rare lost ACE for
/// a guaranteed start failure, which is the same ruling the profile lock
/// carries.
///
/// **The lock files are left behind, and that is one decision with the share
/// mode.** A lock file is never reaped and never opened for deletion:
/// `FILE_SHARE_DELETE` is deliberately withheld, so no process can unlink a lock
/// a peer holds — which is exactly what makes unlinking harmless-looking and
/// wrong, since a second waiter would then create a *new* file at the same name
/// and lock that instead of the one the holder has. What accumulates instead is
/// one zero-byte file per distinct path this crate has ever granted, in one
/// directory a cleaner or an operator can delete once nothing is serving —
/// **measured on this host 2026-09-28, after a handful of this crate's own suite
/// runs: 450 files, 0 bytes in total**, every name a 16-hex digest and
/// `.lock`.
///
/// **The exclusion is between processes of one account**, because the temp
/// directory is the caller's own: two accounts granting one directory are not
/// serialised. That is the shape of the hazard this closes — a served root is one
/// operator's, and the two runs that race on it run as that operator — and the
/// alternative (a `Global\` object, the profile lock's route) would need a name
/// every account may open, which is a broader object than this one for a case no
/// run here creates. `Drop` releases the lock and closes the handle on every path
/// out of [`grant_to`](super::grant::grant_to), panics included.
///
/// **Measured on this host, 2026-09-28**, with a probe that speaks to the file the
/// way a *process* rather than a thread does (a PowerShell `FileStream.Lock` over
/// the same byte range in a second process): while that peer held it for 20 s,
/// this process's [`AclLock::acquire`] waited **18 770 ms** — the remaining hold —
/// and then took the lock, so the exclusion really is cross-process and not only
/// cross-thread; an uncontended acquire took the lock on its **first attempt, with
/// no sleep at all**; and a holder killed with `TerminateProcess` inside its hold
/// left the range takeable **0 ms later**, which is the property this primitive
/// was chosen for — a killed run cannot wedge a directory for the runs after it.
/// The race this lock closes, and the failure the test beside it reproduces, are
/// in `acl`'s own tests.
pub(crate) struct AclLock {
    /// The lock file's handle, open without share-delete and held exclusively.
    handle: HANDLE,
}

impl AclLock {
    /// Take this path's lock, waiting — and reporting a long wait — until the
    /// peer that holds it is done.
    ///
    /// `None` when no lock could be created or taken, which the crate treats as
    /// "no exclusion" rather than as a failure: both reasons are logged, because
    /// a guarantee that is not in force has to be visible.
    pub(crate) fn acquire(path: &Path) -> Option<Self> {
        let file = match lock_directory() {
            Ok(directory) => directory.join(format!("{}{ACL_LOCK_SUFFIX}", root_digest(path))),
            Err(error) => {
                // **No `lock` field here, because there is no lock file to name** — the
                // sentence is [`NO_LOCK`]'s either way, which is what keeps the three
                // refusals below from drifting into three descriptions of one exposure.
                tracing::warn!(
                    step = "lock the granted path",
                    path = %path.display(),
                    error = %error,
                    NO_LOCK
                );
                return None;
            }
        };
        // **The warning the two failures below earn**, over the fields all three share.
        let no_lock = |error: &dyn Display| {
            tracing::warn!(
                step = "lock the granted path",
                path = %path.display(),
                lock = %file.display(),
                error = %error,
                NO_LOCK
            );
        };
        let name = to_wide(file.as_os_str());
        // SAFETY: `name` is NUL-terminated and alive for the call, the access is
        // the read and write a lock needs, the share mode deliberately withholds
        // `FILE_SHARE_DELETE` (the type's doc says why), no security attributes
        // or template are passed, and `OPEN_ALWAYS` creates the zero-byte file on
        // first use and opens the existing one after that. A successful call
        // returns a handle this value owns and closes exactly once in `Drop`.
        let handle = unsafe {
            CreateFileW(
                name.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                null(),
                OPEN_ALWAYS,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE || handle.is_null() {
            no_lock(&handles::last_error());
            return None;
        }
        let started = Instant::now();
        let mut reported = false;
        loop {
            // One `OVERLAPPED` per attempt, zeroed: the byte range below is at
            // offset 0 of the lock file and the structure carries nothing else a
            // synchronous `LockFileEx` with `LOCKFILE_FAIL_IMMEDIATELY` reads.
            let mut overlapped = OVERLAPPED::default();
            // SAFETY: `handle` is the live handle this call opened, the flags ask
            // for an exclusive lock that fails rather than blocks, the byte range
            // is the single byte at offset 0 (the same range every waiter uses,
            // so they conflict by construction), and `overlapped` is a correctly
            // sized value alive for the call.
            let locked = unsafe {
                LockFileEx(
                    handle,
                    LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    1,
                    0,
                    &raw mut overlapped,
                )
            };
            if locked != 0 {
                return Some(Self { handle });
            }
            let error = handles::last_error();
            if error.raw_os_error() != Some(ERROR_LOCK_VIOLATION as i32) {
                // A failure that is not contention: the lock is not taken, and
                // the handle goes with the attempt.
                // SAFETY: `handle` is this call's own handle and nothing else has
                // taken it.
                unsafe { CloseHandle(handle) };
                no_lock(&error);
                return None;
            }
            let waited = started.elapsed();
            if waited >= ACL_LOCK_WARN_AFTER && !reported {
                // **Once, at the crossing.** A repeated event would fill a log
                // for a peer that is legitimately inside a whole-tree descriptor
                // write, and what the operator needs from this line is which path
                // is being waited on and how long the wait has been — the grant's
                // own per-path event, emitted when this function finishes, is
                // where the wait ends.
                reported = true;
                tracing::warn!(
                    step = "wait for the ACL lock",
                    path = %path.display(),
                    waited_ms = waited.as_millis() as u64,
                    threshold_ms = ACL_LOCK_WARN_AFTER.as_millis() as u64,
                    "another run holds this path's ACL lock and is inside its ACL \
                     phase (a descriptor write propagates over the whole subtree, \
                     which is seconds on a large tree, design §6); this grant \
                     waits for it — the lock is released by the kernel if the \
                     holder dies, so the wait is not stranded"
                );
            }
            thread::sleep(ACL_LOCK_POLL);
        }
    }
}

impl Drop for AclLock {
    fn drop(&mut self) {
        let mut overlapped = OVERLAPPED::default();
        // SAFETY: `handle` is this value's own handle and the range is the one
        // byte `acquire` locked at offset 0, so the unlock is this handle's own;
        // the `OVERLAPPED` is a correctly sized value alive for the call. The
        // return value is not read: closing the handle below releases the lock
        // anyway, so an unlock that failed would change nothing.
        unsafe { UnlockFileEx(self.handle, 0, 1, 0, &raw mut overlapped) };
        // SAFETY: the same handle, closed exactly once, here.
        unsafe { CloseHandle(self.handle) };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use tinio_util::testing::{assert_recorded_at, assert_step};

    use super::*;
    use crate::{
        _core::AccessMode,
        acl::{
            Grants,
            descriptor::{OwnedSid, acl_names},
            icacls::checked,
            spec::Target,
            test_support::overlap,
        },
        sid::{PackageSid, package_name},
        testing::{capture, recorder},
    };

    /// **The race the per-path lock closes, reproduced on purpose: two threads
    /// grant two SIDs to one directory, and both ACEs have to survive.**
    ///
    /// **It is a real race rather than a theoretical one, and this test is built
    /// so that it fires.** `grant_to` reads the object's DACL, merges its own
    /// SID's entry into what it read, and writes the whole descriptor back, so a
    /// second writer whose read happened before the first's write replaces that
    /// write with a copy that never carried the peer's entry. Both threads here arm
    /// [`overlap`], which holds each thread's read-to-write gap open for 50 ms, and
    /// a barrier releases them together — so both reads are certainly before either
    /// write, whatever the scheduler does with them. **With `AclLock::acquire`
    /// removed from `grant_to` this test fails, and it was measured rather than
    /// argued** (this host, 2026-09-28): three consecutive runs each failed the
    /// first assertion below, naming the losing SID, with the failed run's own
    /// `icacls` output carrying **one** package-SID entry for the directory the two
    /// threads had just granted. What that failure looks like in production is
    /// silence, and that is why the assertions have to come after both threads are
    /// done: each thread's *own* grant is present, and the missing one belongs to
    /// the peer.
    ///
    /// **Two SIDs and not one SID twice**, because that is the shape two confined
    /// runs sharing a served root have: `package_name` is a digest of the root and
    /// this user's salt (decision 21, and `crate::sid::Salt` for the second input),
    /// so two roots are two SIDs, and `SET_ACCESS` replaces only the
    /// same SID's entry — which is why neither run can notice the other's loss.
    ///
    /// The oracle is both halves this suite uses elsewhere: this crate's own read
    /// ([`acl_names`], `GetNamedSecurityInfoW` plus `EqualSid`) and `icacls`'s
    /// rendering of the same directory, so the assertion does not rest on this
    /// crate's view of the DACL alone.
    #[test]
    #[expect(
        clippy::arc_with_non_send_sync,
        reason = "test context: `PackageSid` owns a raw `PSID` and is deliberately neither `Send` nor `Sync`, and the `Arc` is the shared ownership the code under test is given"
    )]
    fn two_grants_to_one_path_from_two_threads_keep_both_aces() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        // Two roots under one directory, which are two package names and so two
        // profiles and two SIDs — the derivation is lexical, so neither has to
        // exist for it.
        let roots = [path.join("first"), path.join("second")];
        let barrier = Barrier::new(roots.len());
        // **One profile per thread, created where it is used**: a `PackageSid` owns
        // a raw SID and the profile behind it and crosses no thread boundary, so
        // what each thread hands back is the rendered SID — which is what the
        // assertions below need, and which is still true of the ACE after the
        // profile is gone (every grant is standing, decision 15).
        let granted: Vec<String> = thread::scope(|scope| {
            let handles: Vec<_> = roots
                .iter()
                .map(|root| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        // Armed before anything else: the gap this thread's
                        // `grant_to` will hold open is the whole race.
                        overlap::arm();
                        let sid = PackageSid::derive(&package_name(root)).expect("a profile");
                        let text = sid.as_str().to_owned();
                        let grants = Grants::new(Arc::new(sid));
                        // Past the barrier, both threads are inside `grant_to` and
                        // before their own read-to-write gap.
                        barrier.wait();
                        grants
                            .apply(path, Target::Directory, AccessMode::ReadWrite)
                            .expect("grant");
                        text
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("no thread panicked"))
                .collect()
        });

        let listing = checked("icacls (a test's own listing)", path, &[]).expect("read");
        let rendered = String::from_utf8_lossy(&listing.stdout);
        for text in &granted {
            // The SID is resolved from the thread's own rendering: the profile it
            // was created under is already deleted, and the ACE is not.
            let sid = OwnedSid::parse(text, path).expect("the SID the thread granted");
            assert!(
                acl_names(path, sid.raw()).expect("read the DACL"),
                "{text} lost the race: one thread's merge was built from a DACL read \
                 before the other's write, so that write is not in the descriptor — \
                 which is what one lock per path around the read-merge-write exists to \
                 prevent (directory DACL:\n{rendered})"
            );
            assert!(
                rendered.contains(text.as_str()),
                "the tool's own rendering of the directory does not carry {text}, so \
                 this crate's read and the oracle disagree:\n{rendered}"
            );
        }
    }

    /// **Shape constraint 2 of the item this lock landed under, as a test: the
    /// wait is reported and never silent** — at `warn!`, naming the path and the
    /// time waited, and once rather than once per poll.
    ///
    /// The lock is held by this thread and waited on by a second one, because a
    /// wait has to be observed while it is happening and only the holder can end
    /// it. What the assertion is about is the crossing: the hold is
    /// [`ACL_LOCK_WARN_AFTER`] plus a margin, so the waiter polls many times and
    /// exactly one line crosses the threshold.
    ///
    /// **The quiet half is not pinned here and the reason is the wall clock**: "a
    /// wait under the threshold emits nothing" would need a second hold of nearly
    /// the threshold to be meaningful, which is the same second again for the
    /// absence of one line. The count assertion below is the half that costs
    /// nothing and is worth having: a report per poll would fill a log with the
    /// same sentence.
    ///
    /// **The recording is the waiting thread's own, not an every-thread one**,
    /// which is a rule rather than a preference: the warning is emitted on the
    /// waiter, and an every-thread recording is a process-wide buffer that a
    /// `record()` in another test's thread asserts against while it is open —
    /// measured here, as two `backend::tests` legs that take a thread-local
    /// recording failed the first time this test was written the other way.
    #[test]
    fn a_wait_past_the_threshold_is_reported_once() {
        let _serial = recorder();
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path();
        let held = AclLock::acquire(path).expect("the first holder");
        let (waited, logged) = thread::scope(|scope| {
            let waiter = scope.spawn(|| {
                let capture = capture();
                let recording = capture.record();
                let started = Instant::now();
                let taken = AclLock::acquire(path);
                let waited = started.elapsed();
                drop(taken);
                let logged = capture.text();
                drop(recording);
                (waited, logged)
            });
            // Longer than the threshold and short enough to keep the suite quick:
            // what is asserted is the crossing, not a bound on the wait.
            thread::sleep(ACL_LOCK_WARN_AFTER + Duration::from_millis(300));
            drop(held);
            waiter.join().expect("the waiter did not panic")
        });

        assert!(
            waited >= ACL_LOCK_WARN_AFTER,
            "the wait did not cross the threshold, so this recording proves nothing: \
             {waited:?}"
        );
        assert_step(&logged, "wait for the ACL lock");
        assert_recorded_at(
            &logged,
            "another run holds this path's ACL lock",
            tracing::Level::WARN,
        );
        assert!(
            logged.contains(&format!("path={}", path.display())),
            "the report names the path it is waiting on:\n{logged}"
        );
        assert!(
            logged.contains(&format!("threshold_ms={}", ACL_LOCK_WARN_AFTER.as_millis())),
            "the rule the wait crossed is in the line, not only in this source:\n{logged}"
        );
        assert_eq!(
            logged.matches("wait for the ACL lock").count(),
            1,
            "one wait reports once, however many polls it took:\n{logged}"
        );
    }
}
