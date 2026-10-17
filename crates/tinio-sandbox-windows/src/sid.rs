//! The AppContainer package SID, and the capability SIDs a container is created
//! with.
//!
//! **The package name is derived from the served root and this user's salt**, and
//! the salt is the one piece of persistent state the confinement keeps: 32 CSPRNG
//! bytes in `%LOCALAPPDATA%\tinio\acl-salt`, mixed into the digest so that a local
//! user who cannot read that file cannot compute a served root's SID — which,
//! without it, is a pure function of the root and so computable by anyone
//! ([`package_name`] and [`Salt`] carry the exposure and the degradation).
//!
//! This module is one of the crate's enumerated `unsafe` sites (design decision
//! 14): the SID derivation and the two SID/string conversions are the crate's
//! only security-identifier FFI. **The salt's file API is not one of them** — the
//! file is created by a plain `create_new`, with no security descriptor at all, so
//! this module still only names it — and the enumeration is unchanged by it.

#![allow(unsafe_code)]

use std::{
    env,
    fmt::Display,
    fs,
    io::{self, Write as _},
    os::windows::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr,
};

use sha2::{Digest, Sha256};
use tinio_util::{fsutil::create_new_private, path::plain_spelling, wide::to_wide};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, LocalFree, WAIT_ABANDONED, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Security::{
        Authorization::{ConvertSidToStringSidW, ConvertStringSidToSidW},
        FreeSid,
        Isolation::{
            CreateAppContainerProfile, DeleteAppContainerProfile,
            DeriveAppContainerSidFromAppContainerName,
        },
        PSID, SID_AND_ATTRIBUTES,
    },
    System::{
        SystemServices::SE_GROUP_ENABLED,
        Threading::{CreateMutexW, INFINITE, ReleaseMutex, WaitForSingleObject},
    },
};

use crate::{
    _core::{Error, Result, launch},
    backend::Capability,
    capability,
    handles::last_error,
};

/// `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` — what `CreateAppContainerProfile`
/// returns for a profile that is already there. A residue from a crashed run is
/// success here rather than an error (design decision 21).
///
/// **The collision comes back with the SID out-parameter left null**, which this
/// module measured on 2026-09-21 rather than assumed: the Windows SDK's own
/// sample answers the same code by falling back to
/// `DeriveAppContainerSidFromAppContainerName`, and the takeover below is that
/// pattern. An earlier draft of the plan asserted the opposite — that the
/// creation fills the SID in anyway — and its test failed for it.
///
/// **It is `windows-sys`'s one gap for this crate, and the reason is that
/// `HRESULT_FROM_WIN32` is a macro rather than a constant** — `winnt.h` expands
/// it, so there is no item to name. This is the arithmetic for the one error
/// code the crate tests: `FACILITY_WIN32` (`0x07`) in the high half and
/// `ERROR_ALREADY_EXISTS` (183, which `windows_sys::Win32::Foundation` *does*
/// export) in the low one — `0x8007_0000 | 183`.
const HRESULT_ALREADY_EXISTS: i32 = 0x8007_00B7u32 as i32;

/// `S-1-15-2-1` — **ALL APPLICATION PACKAGES**, the one package SID every
/// AppContainer token carries whatever package the process belongs to.
///
/// **The program directory is granted this SID, once per machine, and that is a
/// cost decision whose security price is stated rather than hidden.** Every other
/// grant this crate makes is for the package SID of one *root* ([`package_name`]),
/// so a new root is a new SID: rewriting the program directory's ACE per root
/// would rewrite that directory's security descriptor on every launch — and a
/// descriptor write propagates eagerly over the whole subtree, measured at
/// ≈0.04 ms per object for this shape on ReFS and 10–20 s for this workspace's
/// `target\debug` (71 757 objects, `docs/tests.md`'s Windows section). The program
/// directory does not depend on the root, so authorizing it once for a SID every
/// container already holds removes that per-root cost entirely.
///
/// **The price is that the program directory becomes readable and executable by
/// any AppContainer application on the machine**, and that the ACE is never taken
/// back. It is confined to the directory itself and the `*.dll` files directly
/// beside the program — the same narrow shapes decision 22 grants — and a build
/// output directory is usually world-readable already; a program directory that
/// holds configuration or keys beside the binary would leak them. **This SID is
/// for that read-only directory alone**: a served root or a state directory
/// granted it would let a process confined to root A write root B, which is the
/// cross-root hole the per-root derivation exists to prevent.
pub const ALL_APPLICATION_PACKAGES: &str = "S-1-15-2-1";

/// The prefix every name this crate derives starts with — **and the whole of the
/// claim the launch-path sweep makes about a name it may reap** (`crate::sweep`,
/// design decision 21): a directory under `%LOCALAPPDATA%\Packages` whose name
/// starts with this, or a `Storage` key whose name does, is this crate's; nothing
/// else in either place is ever touched. One constant rather than a literal per
/// derivation, so a change to the spelling cannot leave the sweep reaping yesterday's
/// names — or, worse, a name that only *looks* like this crate's.
pub(crate) const PACKAGE_PREFIX: &str = "tinio.sandbox.";

/// The AppContainer package name for `root` — **under this user's persisted
/// salt, which is what the product path always derives with.**
///
/// Derived, so a hard kill's residue is the *same* SID on the next run and the
/// launcher takes its own residue over instead of accumulating new ACEs (design
/// decision 21). Root-derived rather than a constant so that two servers serving
/// different roots do not share a package SID — a shared SID would let either
/// one read the other's granted tree — and bounded so that the residue stays one
/// directory per root, the same reasoning as decision 10's jail name. **Salted
/// because the root alone is public knowledge**: without the salt the name is a
/// pure function of the canonical root, so any local user can compute a served
/// root's SID, create a profile under it and read everything the standing grants
/// expose — design §6 owns the exposure and what each layer does about it.
///
/// **The normalisation is what makes the takeover work**, and it is
/// `normalised_root`'s, which carries the lexical rule and the reasons for each
/// half of it; what is derived from that value is `name_digest`'s, whose doc
/// carries the two readings the digest is taken over and where the salt joins
/// them. What this function is is the name: the digest in the form a profile, a
/// log line and an operator's package listing spell it.
///
/// **This is the one function that touches the salt file**, and it degrades
/// rather than failing: an unreadable or uncreatable salt warns through
/// `Salt::load` and this call answers [`package_name_unsalted`] — the name every
/// grant made before the salt existed was made under, which is exactly the
/// exposure the salt closes, for that run and for the residue that run leaves.
/// Nothing is silent about it: the warning names the file and says which
/// derivation ran.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::package_name;
///
/// let name = package_name(Path::new(r"E:\srv\tinio"));
/// assert!(name.starts_with("tinio.sandbox."));
/// // One directory, three spellings, one name — and so one SID.
/// assert_eq!(name, package_name(Path::new(r"e:/srv/tinio/.")));
/// ```
pub fn package_name(root: &Path) -> String {
    match Salt::load() {
        Some(salt) => package_name_with(root, &salt),
        None => package_name_unsalted(root),
    }
}

/// The same derivation under one given salt — **the pure function
/// [`package_name`] is built from**, and what a leg that needs two names for one
/// root calls.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::{Salt, package_name_with};
///
/// let root = Path::new(r"E:\srv\tinio");
/// let one = Salt::from_bytes([0x11; 32]);
/// let other = Salt::from_bytes([0x22; 32]);
/// // Stable under one salt, and two names under two.
/// assert_eq!(package_name_with(root, &one), package_name_with(root, &one));
/// assert_ne!(
///     package_name_with(root, &one),
///     package_name_with(root, &other)
/// );
/// ```
pub fn package_name_with(root: &Path, salt: &Salt) -> String {
    format!("{PACKAGE_PREFIX}{}", name_digest(root, Some(salt)))
}

/// The derivation with the salt's step left out — **what the product runs when no
/// salt can be read**, and the name every grant made before the salt landed was
/// made under.
///
/// It exists as a named function rather than as a branch inside [`package_name`]
/// because two readers need to say what it is: the degradation, which warns and
/// falls back to it, and a test or a tool that has to reproduce a pre-salt name.
/// **Nothing in the product derives a name this way when a salt is readable**,
/// and a caller that reaches for it where a salt exists has chosen the
/// pre-computation exposure that the salt closes.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::{Salt, package_name_unsalted, package_name_with};
///
/// let root = Path::new(r"E:\srv\tinio");
/// assert!(package_name_unsalted(root).starts_with("tinio.sandbox."));
/// // The salt is the only difference between the two derivations.
/// assert_ne!(
///     package_name_unsalted(root),
///     package_name_with(root, &Salt::from_bytes([0x11; 32]))
/// );
/// ```
pub fn package_name_unsalted(root: &Path) -> String {
    format!("{PACKAGE_PREFIX}{}", name_digest(root, None))
}

/// How many bytes the salt is: **32, the CSPRNG's own width and a SHA-256's**, so
/// the value is not the digest's weak half. `getrandom` fills exactly this many
/// (`Salt::create_at`) and a file holding anything else is refused rather than
/// stretched or padded ([`Salt::from_bytes`]).
pub const SALT_BYTES: usize = 32;

/// The crate's per-user state directory under `%LOCALAPPDATA%`, and the one file
/// this crate keeps in it: the salt the derivation is mixed with.
///
/// **One directory, beside the profile machinery's own data** —
/// `%LOCALAPPDATA%\Packages\<name>` is where `CreateAppContainerProfile` puts a
/// profile (measured 2026-09-26: `DeleteAppContainerProfile` takes that directory
/// with it).
const STATE_DIRECTORY: &str = "tinio";
const SALT_FILE: &str = "acl-salt";

/// The directory this crate keeps its per-user state in: `%LOCALAPPDATA%\tinio`.
///
/// **Resolved from the environment variable rather than through a platform API,
/// and that is not a defence being claimed** — the same rule `acl::icacls_binary`
/// states: an actor who can set this launcher's
/// environment is a local actor, and local actors are outside this feature's
/// threat model, so what the variable buys is that the state directory is the
/// operator's own even on a host where a shell API would answer differently.
///
/// `None` when the variable is unset, which is the degradation both callers warn
/// about rather than an error either of them can act on.
pub(crate) fn state_directory() -> Option<PathBuf> {
    env::var_os("LOCALAPPDATA").map(|base| PathBuf::from(base).join(STATE_DIRECTORY))
}

/// Report a derivation that ran without the salt, and answer the `None` every
/// such path returns.
///
/// **The sentence lives here rather than at the nine sites that earn it**, because
/// it is one statement about one exposure — *the name this run derives is the one
/// any local user can derive, so they can compute the served root's SID and create
/// the same AppContainer profile* (design §6) — and only its cause clause varies.
/// Nine hand-kept copies would let the exposure be described two ways in one log.
///
/// `cause` is that clause: what the file, the environment or the CSPRNG did. The
/// error is rendered into the event rather than into the sentence, so an operator
/// reading structured logs gets it as a field and a reader of the sentence still
/// gets the whole of what was lost.
fn degrade(file: Option<&Path>, cause: &str, error: &dyn Display) -> Option<Salt> {
    tracing::warn!(
        step = "load the ACL salt",
        path = file.map(|file| file.display().to_string()),
        error = %error,
        "{cause}, so this run derives the unsalted package name for its served root: \
         any local user can compute that root's SID and create the same AppContainer \
         profile (design §6)"
    );
    None
}

/// Where a private file is written before it is published: **beside the published
/// name, and named after the value being published**, so no two publishing runs can
/// name one file.
///
/// **`io::Result`, because the name is the caller's and the stem is derived from it.**
/// A path whose last component carries no stem — `..`, or a directory a caller handed
/// in — is an `InvalidInput` the caller propagates, rather than a panic on an argument
/// this module did not choose (`docs/style.md` §Panics: the four excepted categories are
/// prefixed `literal:`, `infallible:`, `guarded:` and `argued:`, and this site is none of
/// them). The `?` costs nothing on the path that matters: `publish_private` already
/// returns `io::Result<()>`.
fn temporary_beside(file: &Path, bytes: &[u8]) -> io::Result<PathBuf> {
    let stem = file
        .file_stem()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "`{}` has no file stem, so no temporary can be named after it",
                    file.display()
                ),
            )
        })?
        .to_string_lossy();
    Ok(file.with_file_name(format!(
        "{stem}.{}.tmp",
        hex::encode(&Sha256::digest(bytes)[..8])
    )))
}

/// Write `bytes` into a file at `temporary` and publish that same object at `file` by
/// linking it — **the one spelling of "the published name never exists without its
/// value"**, which is how the salt publishes itself ([`Salt::create_at`]).
///
/// **The temporary is removed on every path out, the failure path included**, so the
/// only residue this can leave is one file nothing ever reads — and a removal this
/// process could not make is warned about rather than swallowed.
pub(crate) fn publish_private(file: &Path, bytes: &[u8]) -> io::Result<()> {
    let temporary = temporary_beside(file, bytes)?;
    let published = write_and_link(&temporary, file, bytes);
    if let Err(error) = fs::remove_file(&temporary)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(
            step = "load the ACL salt",
            path = %temporary.display(),
            error = %error,
            "the private file a value was published through could not be removed"
        );
    }
    published
}

/// One live SID's `S-1-…` text — **the one call that renders a raw SID**, so the
/// production path, [`PackageSid::derive`], and the legs that compare a raw SID
/// against a spelling ([`crate::acl`], this module's own tests) all read the same
/// conversion and the same failure.
///
/// `None` when `ConvertSidToStringSidW` refuses, which is the API's contract
/// breaking rather than a condition a caller can act on: the callers differ only in
/// what they say about it — a launch error in production, a panic in a test.
pub(crate) fn sid_text(sid: PSID) -> Option<String> {
    let mut text: *mut u16 = ptr::null_mut();
    // SAFETY: `sid` is a live SID the caller holds — the one
    // `CreateAppContainerProfile` returned, or one inline in an ACE — and `text` is
    // a valid out-parameter; the buffer is freed below with LocalFree, its
    // documented release.
    let ok = unsafe { ConvertSidToStringSidW(sid, &mut text) };
    if ok == 0 || text.is_null() {
        return None;
    }
    // SAFETY: `text` is the NUL-terminated UTF-16 buffer the conversion just
    // allocated, and nothing else reads it.
    let rendered = unsafe { crate::from_wide(text) };
    // SAFETY: `text` is the conversion's own allocation, freed exactly once.
    unsafe { LocalFree(text.cast()) };
    Some(rendered)
}

/// The per-user salt: **the one secret the derivation holds, and the one value
/// this module never renders.**
///
/// **What it is for.** `name_digest`'s own doc carries the placement; what this
/// type owns is the lifecycle: `Self::load` reads (or creates) the file, and the
/// value it returns is what [`package_name`] mixes in. A user who cannot read the
/// file cannot compute the name for any root, which is the exposure the salt
/// closes; a user who *can* — the same account, or an administrator reading the
/// profile — is not stopped by it, and design §6 says so rather than implying
/// otherwise.
///
/// **No accessor, and that is a decision rather than an omission.** The value is a
/// secret, and every way out of this module is either a file name or a digest of
/// it: a `bytes()`/`to_vec()`/`Display` on it would put the salt into whatever
/// collects log lines and error messages, which is a place another local user can
/// read. [`Self::from_bytes`] is the one constructor a caller reaches, so a test
/// or a tool can build a value to derive with and cannot read one back out of a
/// loaded file.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_windows::sid::{Salt, package_name_unsalted, package_name_with};
///
/// // A value a caller already holds — nothing here touches the file
/// // `package_name` reads and creates.
/// let salt = Salt::from_bytes([0x11; 32]);
/// let root = Path::new(r"E:\srv\tinio");
/// // What the value is for: the same root derives a different name under the
/// // salt than it does without one, which is the pre-computation the salt
/// // closes (design §6).
/// assert_ne!(package_name_with(root, &salt), package_name_unsalted(root));
/// // And it is the only input that moves the name, so the normalisation is
/// // still what decides which root a name is about.
/// assert_eq!(
///     package_name_with(root, &salt),
///     package_name_with(Path::new(r"e:/srv/tinio/."), &salt)
/// );
/// ```
pub struct Salt {
    /// The 32 bytes, private for the reason the type's doc gives.
    bytes: [u8; SALT_BYTES],
}

impl Salt {
    /// A salt from its bytes — the constructor a test or a tool uses to derive
    /// under a value it already holds, and the only public one.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::sid::{Salt, package_name_with};
    ///
    /// let salt = Salt::from_bytes([0x11; 32]);
    /// let name = package_name_with(std::path::Path::new(r"E:\srv\tinio"), &salt);
    /// assert!(name.starts_with("tinio.sandbox."));
    /// ```
    pub fn from_bytes(bytes: [u8; SALT_BYTES]) -> Self {
        Self { bytes }
    }

    /// This user's salt, **creating the file on first use**.
    ///
    /// `None` means the run derives without one, and **every way of reaching it is
    /// warned here rather than at the call site** — [`ProfileLock::acquire`]'s
    /// precedent, and for its reason: the caller's answer is the same either way
    /// (it derives a name and confines a server), so the place that knows *what*
    /// was lost is the one that has to say it. Every such warning is a launch that
    /// continues with the exposure the salt exists to close, which is the
    /// operator's to see in the log and not to infer from a name that looks
    /// plausible.
    fn load() -> Option<Self> {
        let Some(file) = salt_file() else {
            return degrade(
                None,
                "no per-user salt directory could be resolved (`LOCALAPPDATA` is \
                 unset)",
                &"(unset environment variable)",
            );
        };
        Self::load_at(&file)
    }

    /// The load-or-create sequence for the file `file` names — `Self::load`'s
    /// body, and the seam a leg points at a temporary directory.
    ///
    /// **The read is the whole of the steady state**: a salt file that is there and
    /// holds [`SALT_BYTES`] is used as it stands, so a run after the first costs one
    /// read of 32 bytes. Everything else — a file that is not there, is not 32 bytes,
    /// or cannot be read — is [`Self::create_at`] or a warned `None`.
    pub(crate) fn load_at(file: &Path) -> Option<Self> {
        match fs::read(file) {
            Ok(bytes) => Self::from_bytes_at(file, bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Self::create_at(file),
            Err(error) => degrade(Some(file), "this user's salt file cannot be read", &error),
        }
    }

    /// The bytes a salt file holds, or the degradation a file of the wrong length
    /// earns: **one value, [`SALT_BYTES`] long, and no other question asked of it.**
    ///
    /// **A file that is not [`SALT_BYTES`] long is refused rather than stretched,
    /// padded or re-created**: the file is either this crate's own value or
    /// something that is not this crate's value, and a name derived from half a
    /// salt is a name nobody can reproduce — including the next run, whose
    /// residue takeover decision 21 rests on would then never find its SID
    /// again.
    ///
    /// **Nothing is read back from the file's descriptor**: the file is used as it is found,
    /// and what stands between it and another account is the state directory's own grants
    /// (`%LOCALAPPDATA%`'s, which the launcher never widens — the design spec's
    /// two-directory table, and design §6 for the scope that leaves).
    fn from_bytes_at(file: &Path, bytes: Vec<u8>) -> Option<Self> {
        let Ok(bytes) = <[u8; SALT_BYTES]>::try_from(bytes.as_slice()) else {
            return degrade(
                Some(file),
                &format!(
                    "this user's salt file does not hold a salt: {} bytes where \
                     {SALT_BYTES} are",
                    bytes.len()
                ),
                &"a file that is not a salt",
            );
        };
        Some(Self { bytes })
    }

    /// Publish a fresh salt at `file` — **or read the one a peer published
    /// first**, when this call loses the creation race.
    ///
    /// **The value is published whole, and that is why the create is a link and
    /// not an open.** The file appears under its final name only once it holds all
    /// [`SALT_BYTES`]: the bytes go into a temporary file named after them, and
    /// `fs::hard_link` — which fails rather than replacing when the name is
    /// taken — is what puts that object at the salt's name. A create-then-write at
    /// the final name would leave a window in which a concurrent first launch reads
    /// a file with no bytes in it and degrades; a rename would *replace* a winner's
    /// salt with the loser's, and the two runs would then hold different salts while
    /// only one of them stood on disk. Measured on this host 2026-09-28: eight
    /// threads released together on one fresh path all read one value, and the file
    /// held 32 bytes (the legs are `eight_races_on_a_fresh_salt_file_agree_on_one_
    /// value` and `the_salt_file_is_created_once_and_a_second_launch_reads_it`).
    ///
    /// The temporary is one object with the published name — a hard link names one
    /// object — and it is removed on every path out of this function, the failure
    /// path included.
    fn create_at(file: &Path) -> Option<Self> {
        let Some(directory) = file.parent() else {
            // `salt_file` always joins a name onto a directory, so this is the
            // type's contract rather than a host's state.
            return None;
        };
        // **Nothing is read back and nothing is put back**: the directory is the platform's
        // own answer from the create onwards, and this call installs no descriptor (the design
        // spec's two-directory table, and design §6 for the scope that leaves).
        if let Err(error) = fs::create_dir_all(directory) {
            return degrade(
                Some(file),
                "this user's state directory could not be created",
                &error,
            );
        }
        let mut bytes = [0u8; SALT_BYTES];
        if let Err(error) = getrandom::fill(&mut bytes) {
            return degrade(Some(file), "the CSPRNG refused", &error);
        }
        let published = publish_private(file, &bytes);
        match published {
            Ok(()) => Some(Self { bytes }),
            // **The race's loser reads the winner's value**, which is the state
            // the brief rule asks for: one machine, one salt, whichever run got
            // there first. A hard link fails rather than replacing, so
            // `AlreadyExists` is exactly "someone else published first" — and the
            // file it published is complete by construction, so this read cannot
            // see a partially written one.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => match fs::read(file) {
                Ok(bytes) => Self::from_bytes_at(file, bytes),
                Err(error) => degrade(
                    Some(file),
                    "another run published this user's salt file and it cannot \
                         be read",
                    &error,
                ),
            },
            Err(error) => degrade(
                Some(file),
                "this user's salt could not be published",
                &error,
            ),
        }
    }
}

/// [`publish_private`]'s link step: the file is created, the value goes into it, and the
/// name is published — **in the order that makes the salt's name appear with its value
/// already in it**.
///
/// The handle is closed before the link, so the two names never name an object another
/// handle is still writing through; the value is on disk by then and the link is what
/// publishes it.
fn write_and_link(temporary: &Path, file: &Path, bytes: &[u8]) -> io::Result<()> {
    // **The workspace's one secret-file create**: `create_new` refuses a name that
    // appeared between the check and the call, and the file takes what the state
    // directory gives it — which is what `%LOCALAPPDATA%` gave the directory, and
    // what the design spec records as the salt's whole defence.
    let mut handle = create_new_private(temporary)?;
    handle.write_all(bytes)?;
    drop(handle);
    fs::hard_link(temporary, file)
}

/// The salt file this thread's derivation reads, **or the path a leg pointed it
/// at** — `None` when `%LOCALAPPDATA%` is unset.
fn salt_file() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(path) = test_path::get() {
        return Some(path);
    }
    state_directory().map(|directory| directory.join(SALT_FILE))
}

/// The state-directory path a test points the reader at, so a leg can drive the
/// whole create/read cycle on a temporary directory instead of the operator's own
/// state directory — **the seam the salt reads**, since what it resolves is one
/// directory's.
///
/// **A thread-local, the shape `acl`'s `opens`/`overlap` seams take**, and it
/// exists because the product path resolves the path itself: the alternative was a
/// `package_name`-shaped function taking a file argument, which is a second
/// product entry point whose only caller would be a test.
#[cfg(test)]
pub(crate) mod test_path {
    use std::{
        cell::RefCell,
        path::{Path, PathBuf},
    };

    thread_local! {
        static PATH: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// Run `body` with this thread's state-directory file pointed at `path`.
    pub fn with<T>(path: &Path, body: impl FnOnce() -> T) -> T {
        let previous = PATH.with(|slot| slot.replace(Some(path.to_path_buf())));
        let value = body();
        PATH.with(|slot| *slot.borrow_mut() = previous);
        value
    }

    /// This thread's pointed-at path, if a leg pointed one.
    pub fn get() -> Option<PathBuf> {
        PATH.with(|slot| slot.borrow().clone())
    }
}

/// One path's lexical normalisation — **the identity of the directory, as every
/// derivation from a root reads it**: a package name ([`package_name`]) and a
/// per-path ACL lock key ([`crate::acl`]'s) are both built from this one value, so
/// two spellings of one directory cannot take two profiles or two locks.
///
/// **The normalisation is lexical rather than a filesystem call.** A `\\?\` or
/// `\\?\UNC\` prefix is stripped first — [`plain_spelling`], whose own doc carries
/// the unit-level rule that strip is written under — because `Path::components`
/// keeps the prefix, so `\\?\E:\data` would otherwise hash apart from `E:\data`.
/// `Path::components` then drops a trailing separator, drops `.` components and
/// collapses repeated separators, so `E:\data`, `E:\data\`, `E:\data\.` and
/// `\\?\E:\data` are one value. Case is folded for the same reason (`E:\Data` is
/// that directory on Windows) and separators are normalised because `/` and `\`
/// are interchangeable to the filesystem and to nothing else.
/// **`..` is deliberately left alone**: resolving it lexically would let one
/// spelling derive another directory's name without that directory being the one
/// exposed, and a lock key that named a different directory than the grant writes
/// would exclude nothing.
///
/// **The units are the *normalised* path's and not those of the spelling that
/// arrived**, which is the one thing a derivation could get wrong in the other
/// direction: hashing the raw spelling would split `E:\Data` from `e:/data` and
/// `\\?\E:\data` from `E:\data` — one directory each — and a second name is a
/// second profile, a second ACE and a residue no run ever takes over, and a
/// second lock key is a spelling the exclusion does not cover (decision 21).
pub(crate) fn normalised_root(root: &Path) -> PathBuf {
    // `components` does the lexical work the doc above describes — it drops `.`
    // components and a trailing separator, collapses repeated separators, and the
    // `PathBuf` re-joins with this volume's own.
    plain_spelling(root).components().collect()
}

/// The 16-hex digest of [`normalised_root`] — **the *unsalted* reading, and the one
/// path's ACL lock file is named after**.
///
/// **It is taken over two readings of that one normalisation, because only one of
/// them can be a string.** The folded text below is the readable half, and it is
/// lossy by construction: `to_string_lossy` replaces every unit of a path that is
/// not valid UTF-8 with U+FFFD, so two roots that differ only in such a unit —
/// `E:\data\<U+D800>` and `E:\data\<U+DC00>`, both legal directory names and
/// neither of them text — fold to one string, one package name, one SID and so one
/// AppContainer profile. One profile means one ACE tree shared by two roots, and
/// the first `Drop` deletes the profile the other is still launching under. The
/// UTF-16 units of the same normalised path are the second reading, they go into
/// the digest losslessly, and those two roots are two names again.
///
/// What those units are folded by is `fold_unit`'s business, and it is not
/// `str::to_lowercase`: that doc carries the many-to-one mapping which is the
/// reason the lossy half could not be the whole digest.
///
/// **This spelling passes no salt, and the lock file's exclusion is why**: a run
/// that could read this user's salt file and a run that could not — the degraded
/// one, warned about beside the derivation — must still take **one** lock for one
/// path, or the two runs that are most likely to be concurrent on a shared root
/// are the two that do not exclude each other. The name is what has to be salted;
/// the exclusion is a property of the path.
pub(crate) fn root_digest(root: &Path) -> String {
    name_digest(root, None)
}

/// The digest [`package_name`] appends, under an optional per-user salt — **the one
/// hash body behind every name this crate derives.**
///
/// **The salt is hashed last, and the placement is what makes "unsalted" the same
/// derivation with one `update` left out.** Both readings of the normalised path go
/// in first — the lossy folded text and the UTF-16 units under it, which
/// [`root_digest`]'s doc carries — and the 32 salt bytes are appended to that
/// input. Two things follow, and both are the reason rather than a description of
/// it:
///
/// - **Nothing computes a name without the file that holds the salt.** With the
///   salt in the preimage, an account that cannot read `%LOCALAPPDATA%\tinio\acl-salt`
///   cannot evaluate this function at all for any root, which is the entire
///   difference between "a local user can compute this root's SID" and "a local
///   user cannot".
/// - **A secret at the *end* of the preimage is the one shape SHA-256's length
///   extension cannot reach.** Against a secret prefix, a holder of one
///   `(input, digest)` pair can extend it into the digest of that input plus
///   padding plus anything else they choose; against a digest whose last 32 input
///   bytes are the secret there is no such continuation, so a name an observer has
///   already seen yields nothing about a second one. Both orders hide the *name*;
///   the suffix is the one that also survives the pair being known, and neither
///   claims anything against a reader of the salt file itself (design §6).
///
/// **`None` is [`package_name_unsalted`]'s derivation** — the input this function
/// hashed before the salt existed, byte for byte — so the degraded name is the
/// historical one rather than a second derivation that happens to look similar.
fn name_digest(root: &Path, salt: Option<&Salt>) -> String {
    // One lexical normalisation, read twice: as the folded text below and as the
    // UTF-16 units under it.
    let normalised = normalised_root(root);
    let folded = normalised
        .to_string_lossy()
        .replace('/', r"\")
        .to_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(folded.as_bytes());
    for unit in normalised.as_os_str().encode_wide() {
        hasher.update(fold_unit(unit).to_le_bytes());
    }
    // **The one input the path does not determine, appended rather than
    // interleaved**: it is not a second spelling of the path and not a second
    // normalization, so it joins the readings after them.
    if let Some(salt) = salt {
        hasher.update(salt.bytes);
    }
    hex::encode(hasher.finalize())[..16].to_string()
}

/// One UTF-16 unit with the case fold the name's identity is derived under.
///
/// **The fold is Unicode's *uppercase* mapping applied one unit at a time, and both
/// halves of that are measured on this host (NTFS, 2026-09-24).** Case has to be
/// folded at all because `E:\Data` is `E:\data` here; it may not be folded by
/// `to_lowercase`, because that mapping is many-to-one across code points this
/// volume holds **apart**: U+212A KELVIN SIGN lowercases to U+006B, so
/// `E:\data\K` and `E:\data\k` would share a name and a profile while the probe
/// creates **two** directories. Uppercase is the direction that keeps them apart —
/// U+212A is already uppercase and maps to itself, `k` maps to `K` — and it is also
/// the direction that keeps the merges the volume does have: U+00E4 `ä` and
/// U+00C4 `Ä` both uppercase to U+00C4 and the probe creates **one** directory,
/// where folding ASCII alone would have split them into two names and two profiles.
///
/// **A unit with no one-unit uppercase mapping is left alone**, which mirrors the
/// per-unit table NTFS itself carries and errs the safe way: `ß` and an unpaired
/// surrogate have no single unit to fold to, so a pair that might be one directory
/// gets two names rather than a pair that *is* two directories getting one.
fn fold_unit(unit: u16) -> u16 {
    let Some(character) = char::from_u32(u32::from(unit)) else {
        return unit;
    };
    let mut upper = character.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(folded), None) if folded.len_utf16() == 1 => {
            u16::try_from(u32::from(folded)).unwrap_or(unit)
        }
        _ => unit,
    }
}

/// The lock one root's launch window and one root's profile deletion exclude each
/// other through — **one kernel object per served root, held across processes**.
///
/// **Why a profile needs one.** One root is one `package_name` is one SID is one
/// profile: a second launcher's `CreateAppContainerProfile` answers
/// `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)` with a null SID out-parameter and
/// takes the first launcher's profile over, and `PackageSid`'s `Drop` deletes it
/// when that value goes, because nothing distinguishes "I created it" from "I
/// took it over" (measured on this host, 2026-09-26). The damage that costs is
/// exactly one interleaving — a `CreateProcessW` that lands between another
/// process's delete and its own completion fails `err=2
/// ERROR_FILE_NOT_FOUND` for a program that exists, three ways measured — so the
/// window a launch needs is `[derive the profile … CreateProcessW]`, and both
/// ends of it are taken through this lock: the launcher holds it for the whole
/// window, and [`PackageSid`]'s `Drop` holds it around the delete.
///
/// **Held, not owned for the child's life.** The lock is released at
/// `CreateProcessW`, so two runs on one root still run at once — a running
/// confined child is unharmed by a peer's delete (measured: same token, same
/// package SID, still writes through its ACE), which is why nothing here tries
/// to keep the profile alive for the child. The only case it serialises is a peer
/// whose launch overlaps the same root's window, and its bound is that window:
/// the ACL phase, seconds on a large tree.
///
/// **The name is the package name, so the object is per root and machine-wide**:
/// `Global\tinio.profile.<package_name>`. `Global\` rather than `Local\` because
/// two launchers on one root need not share a session — a service serving a root
/// and an operator's shell on it are one root — and measured on this host
/// 2026-09-27: a non-elevated process creates the object, a peer process opens it
/// by name, and a peer's wait blocks while it is held.
///
/// **Acquisition never fails a launch, and the abandonment rule is why it can
/// be waited on without a bound.** A holder killed inside its window leaves the
/// kernel to release the object, and the waiter's `WaitForSingleObject` answers
/// `WAIT_ABANDONED` rather than hanging or failing: the lock is the waiter's, the
/// invariant it protects holds vacuously (the holder is gone, so no
/// `CreateProcessW` is in flight), and the event says so — a crash is worth an
/// operator's eye, not a refusal. A host that refuses the object outright
/// (`CreateMutexW` failing, which is the machine-wide namespace's one real
/// exposure) degrades to the pre-lock behaviour — warned, not silent — rather
/// than refusing to confine the server at all.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::sid::{ProfileLock, package_name};
///
/// let name = package_name(std::path::Path::new(r"E:\srv\tinio"));
/// // `None` when the kernel refuses the object: a launch continues without the
/// // exclusion rather than failing, which the warning beside it records.
/// let window = ProfileLock::acquire(&name);
/// drop(window);
/// ```
pub struct ProfileLock {
    handle: HANDLE,
}

impl ProfileLock {
    /// Take this root's lock, waiting until the peer that holds it is done.
    ///
    /// `None` when no lock could be created, which the crate treats as "no
    /// exclusion" rather than as a failure: what is being serialised is a
    /// handover between two launchers, and a launcher that refused to start over
    /// it would trade a rare misdiagnosis for a total failure. Both reasons are
    /// logged, because a guarantee that is not in force has to be visible.
    #[must_use]
    pub fn acquire(package: &str) -> Option<Self> {
        let handle = open(
            package,
            "no per-root launch lock, so a peer's exit may delete this root's \
             profile while a launch is in flight",
        )?;
        // SAFETY: `handle` is the mutex the call above returned and is live for
        // the call; the wait is INFINITE and not alertable, so it returns only
        // when the mutex is this thread's — or when the kernel releases an
        // abandoned one, which is the case below.
        let waited = unsafe { WaitForSingleObject(handle, INFINITE) };
        match waited {
            WAIT_OBJECT_0 => Some(Self { handle }),
            // **The abandonment rule**: the holder died without releasing
            // (it was killed inside its window), so the kernel hands the mutex
            // over rather than gating on a process that no longer exists. The
            // lock is this thread's, the window it would have been protecting is
            // gone with the holder, and the warning is what makes a crashed
            // launcher visible in a log.
            WAIT_ABANDONED => {
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    "the per-root launch lock was abandoned by a launcher that \
                     died holding it; taken over"
                );
                Some(Self { handle })
            }
            other => {
                let error =
                    io::Error::other(format!("WaitForSingleObject: WAIT_EVENT 0x{other:08X}"));
                // SAFETY: `handle` is this call's own allocation and nothing has
                // taken it.
                unsafe { CloseHandle(handle) };
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    error = %error,
                    "no per-root launch lock, so a peer's exit may delete this \
                     root's profile while a launch is in flight"
                );
                None
            }
        }
    }

    /// The same lock, **taken only if no peer is holding it** — the non-blocking
    /// form the launch-path sweep drives (`crate::sweep`, design decision 21), and
    /// `None` whenever the name must be left alone.
    ///
    /// **Three states are `None`, and all three mean "do not touch this name"**: a
    /// peer is inside its window on this root (`WAIT_TIMEOUT` — the mutex is held),
    /// a launcher **died** inside its window (`WAIT_ABANDONED` — the kernel would
    /// hand the object over, and the sweep has nothing to gain from taking a lock
    /// whose window ended in a crash; the next sweep sees a fresh object once this
    /// handle is closed), and the kernel refusing the object at all, which is
    /// [`Self::acquire`]'s degradation and must not be read as "nobody is in the
    /// window". A caller that wanted to wait would use [`Self::acquire`]; this
    /// function's whole point is that a sweep has no business waiting on a name it
    /// can simply leave for the next run.
    #[must_use]
    pub(crate) fn try_acquire(package: &str) -> Option<Self> {
        let handle = open(
            package,
            "no per-root launch lock could be created, so this name cannot be shown \
             to be outside a peer's launch window and is left alone",
        )?;
        // SAFETY: `handle` is the mutex the call above returned and is live for the
        // call; a zero timeout makes the wait a probe rather than a block, so the
        // only outcomes are "this thread owns it" and the states below.
        let waited = unsafe { WaitForSingleObject(handle, 0) };
        match waited {
            WAIT_OBJECT_0 => Some(Self { handle }),
            WAIT_TIMEOUT => {
                // SAFETY: `handle` is this call's own and no wait took it.
                unsafe { CloseHandle(handle) };
                None
            }
            WAIT_ABANDONED => {
                // SAFETY: as above — the abandoned object is closed rather than
                // taken, which is what the warning says was decided.
                unsafe { CloseHandle(handle) };
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    "the per-root launch lock was abandoned by a launcher that died \
                     holding it; this name is left alone rather than swept"
                );
                None
            }
            other => {
                // SAFETY: the same handle, this call's own.
                unsafe { CloseHandle(handle) };
                tracing::warn!(
                    step = "WaitForSingleObject",
                    package = package,
                    error = %io::Error::other(format!("WaitForSingleObject: WAIT_EVENT 0x{other:08X}")),
                    "the per-root launch lock could not be probed, so this name is \
                     left alone rather than swept"
                );
                None
            }
        }
    }
}

/// The per-root mutex, created or opened — **the create both [`ProfileLock::acquire`]
/// and [`ProfileLock::try_acquire`] stand on**, so the name, the call and what a
/// refusal costs an operator have one home.
///
/// **`Global\` and no fallback to the session namespace.** Measured on this host
/// 2026-09-27: a non-elevated process creates and shares it. A host that refuses it
/// is a host where the exclusion is absent, which is the behaviour every launcher
/// had before this lock existed — stated in `refusal` rather than papered over with
/// a second scope that would exclude only part of the window's peers.
///
/// The handle is the caller's, and is closed by `ProfileLock`'s `Drop` or by the
/// caller directly when no wait took it.
fn open(package: &str, refusal: &str) -> Option<HANDLE> {
    let wide = to_wide(format!(r"Global\tinio.profile.{package}"));
    // SAFETY: `wide` is NUL-terminated and alive for the call, a null security
    // descriptor gives the object this process's default DACL, and `binitialowner = 0`
    // leaves the ownership to the caller's wait — so the create itself never owns the
    // mutex and never needs a release of its own.
    let handle = unsafe { CreateMutexW(ptr::null(), 0, wide.as_ptr()) };
    if handle.is_null() {
        tracing::warn!(
            step = "CreateMutexW",
            package = package,
            error = %last_error(),
            "{refusal}"
        );
        return None;
    }
    Some(handle)
}

impl Drop for ProfileLock {
    fn drop(&mut self) {
        // SAFETY: `handle` is this value's own mutex, taken by the wait in
        // `acquire` on this thread, so the release is this thread's to make —
        // and the handle is closed exactly once, here.
        if unsafe { ReleaseMutex(self.handle) } == 0 {
            // Only reachable if the guard were dropped on another thread than
            // the one that took it, which the type's own `!Send` shape (a raw
            // `HANDLE`) makes a compile error rather than a state to handle.
            tracing::warn!(
                step = "ReleaseMutex",
                error = %last_error(),
                "the per-root launch lock was not released by its owner"
            );
        }
        // SAFETY: the same handle, closed once, as the mutex is no longer this
        // thread's.
        unsafe { CloseHandle(self.handle) };
    }
}

/// A profile call's HRESULT, as the error it belongs to: the classified refusal, or
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) naming the step and the code.
///
/// **One rule, one place.** Both of [`PackageSid::derive`]'s `hr != 0` arms state it — a
/// capability answer becomes the refusal, anything else keeps the old path — and they used to
/// spell it in two shapes, one with a silent `unwrap_or_else` and one with a bare `format!` of
/// the same sentence. The classification itself stays [`capability::classify`]'s; what lives
/// here is only the fallback and the `hr=0x{:08X}` rendering, so the arms cannot drift apart.
fn classified(step: &'static str, name: &str, hr: i32) -> Error {
    capability::classify(hr).unwrap_or_else(|| {
        launch(
            step,
            name,
            io::Error::other(format!("{step}: hr=0x{hr:08X}")),
        )
    })
}

/// A created package SID **and the profile behind it**, alive for as long as
/// anything needs them.
///
/// The two are one value deliberately. A SID whose profile does not exist cannot
/// launch, so a value that carried one without the other would be a value the
/// launcher could not use; the raw SID is freed with `FreeSid` when this value
/// drops, the string form with `LocalFree`, and the profile with
/// `DeleteAppContainerProfile` — so the profile's lifetime is the value's, which
/// is the launcher's, which is the child's.
///
/// **One profile has several such values, in several processes**, because the
/// profile's name is derived from the served root and nothing else: a second
/// launcher on a root *takes the first's profile over* rather than creating a
/// second one, and the first launcher's `Drop` then deletes it. That is the state
/// this doc used to deny ("there is no state in which this crate holds one
/// without the other"): the takeover's value holds a SID whose profile its peer
/// has removed, and it cannot launch until something creates the profile again
/// (measured 2026-09-26). The damage that costs is one launch in flight, which is
/// why the delete and the launcher's window are both taken under
/// [`ProfileLock`].
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::sid::PackageSid;
///
/// // Creating the profile here means deleting it when `sid` drops.
/// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
/// assert!(sid.as_str().starts_with("S-1-15-2-"));
/// ```
pub struct PackageSid {
    raw: PSID,
    text: String,
    name: String,
}

impl PackageSid {
    /// Create the profile for `name` and take its package SID.
    ///
    /// The name is both the profile's identity and its display name, so a
    /// profile left behind by a hard kill is recognisable in the shell's own
    /// list of packages as well as by a derivation (decision 21).
    ///
    /// **A profile that already exists is taken over rather than refused**, and
    /// the SID of that takeover is the derivation's: the creation reports the
    /// collision with no SID, so the name — which is all the derivation needs —
    /// is read instead. The value the caller gets is the same either way, and
    /// nothing is created twice (design decisions 15 and 21).
    ///
    /// # Errors
    ///
    /// [`Error::HostCannotConfine`]
    /// when the token is denied the profile itself (`E_ACCESSDENIED`): the
    /// platform can confine and this token may not, and the variant names the
    /// cause and the two actions.
    ///
    /// [`Error::Launch`] when the profile
    /// cannot be created for any other reason or the SID cannot be rendered —
    /// a name that is too long or malformed surfaces here, before anything has
    /// been granted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// use tinio_sandbox_windows::sid::{PackageSid, package_name};
    ///
    /// let name = package_name(Path::new(r"E:\srv\tinio"));
    /// let sid = PackageSid::derive(&name).unwrap();
    /// assert!(sid.as_str().starts_with("S-1-15-2-"));
    /// ```
    pub fn derive(name: &str) -> Result<Self> {
        let wide = to_wide(name);
        let mut raw: PSID = ptr::null_mut();
        // SAFETY: `wide` is a NUL-terminated UTF-16 buffer alive for the call and
        // `raw` is a valid out-parameter; the display name and the description
        // are the same buffer so the profile is identifiable, and a null
        // capability array with a count of zero is the documented "no
        // capabilities". The returned SID is freed in `Drop` with FreeSid, as the
        // API documents.
        let hr = unsafe {
            CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut raw,
            )
        };
        // `ERROR_ALREADY_EXISTS` is success and not a special case: it is exactly
        // what the run after a hard kill sees, and treating it as a failure would
        // make the residue unrecoverable rather than reusable (design decision
        // 21). It is *not* the whole story, though — the creation reports the
        // collision and leaves `raw` null (measured 2026-09-21, and the Windows
        // SDK's sample falls back the same way), so the SID comes from the
        // derivation the name always yields.
        let taken_over = hr == HRESULT_ALREADY_EXISTS;
        if taken_over {
            // SAFETY: `wide` is NUL-terminated and alive for the call and `raw`
            // is a valid out-parameter; the SID is freed in `Drop` with FreeSid,
            // as the API documents.
            let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut raw) };
            if hr != 0 {
                return Err(classified(
                    "DeriveAppContainerSidFromAppContainerName",
                    name,
                    hr,
                ));
            }
        } else if hr != 0 {
            // **The floor.** Whatever a preflight said, the call that actually fails classifies
            // its own HRESULT, so an operator on a host no preflight anticipated still reads a
            // cause and two actions rather than a bare code.
            //
            // **The refusal's event belongs to this arm** — the create, the call whose profile
            // an operator is asking about — and `capability::refuse` is its one producer; the
            // takeover arm above classifies without announcing.
            if capability::classify(hr).is_some() {
                return Err(capability::refuse(hr, name));
            }
            return Err(classified("CreateAppContainerProfile", name, hr));
        }
        if raw.is_null() {
            // Neither call leaves the out-parameter null on the success it
            // reports, so this is the API's contract breaking rather than a
            // condition the caller can act on — and the operation named is the
            // one that actually ran.
            let operation = if taken_over {
                "DeriveAppContainerSidFromAppContainerName"
            } else {
                "CreateAppContainerProfile"
            };
            return Err(launch(
                operation,
                name,
                io::Error::other("the package SID was not filled in"),
            ));
        }
        let Some(rendered) = sid_text(raw) else {
            // SAFETY: `raw` is this call's own allocation and nothing else has
            // taken it.
            unsafe { FreeSid(raw) };
            return Err(launch("render the package SID", name, last_error()));
        };
        // `info!`, and it is the event an operator's "was this process confined,
        // and to what" question is answered from: the name the profile was created
        // under and the SID it returned, as fields rather than a sentence. When a
        // run left ACEs behind, this is the line that says whose they are (shared
        // §7's split, and decision 21).
        tracing::info!(
            step = "CreateAppContainerProfile",
            name = name,
            sid = %rendered,
            taken_over = taken_over
        );
        Ok(Self {
            raw,
            text: rendered,
            name: name.to_owned(),
        })
    }

    /// The `S-1-15-2-…` form: what a log line prints, what `icacls` is given
    /// where a test or an operator runs it, and what a test compares against. One
    /// accessor, not two — an earlier draft carried `sid_text` as a second name for
    /// exactly this string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::sid::PackageSid;
    ///
    /// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
    /// assert!(sid.as_str().starts_with("S-1-15-2-"), "{}", sid.as_str());
    /// ```
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The profile's name — **the value this SID was created (or taken over)
    /// under**, which is the derivation's own name for the served root and not a
    /// second reading of it.
    ///
    /// An operator greps the profile in the shell's package list by this name, and
    /// the `S-1-15-2-…` string is what an ACE renders. **Nothing here re-derives it**:
    /// a second call to [`package_name`] would read the salt file again and could
    /// answer with a different name than this value's profile exists under, which
    /// is the one thing the takeover rule (decision 21) cannot survive.
    ///
    /// **The accessor is kept although no in-tree caller takes it**, and the ruling is
    /// recorded rather than re-derived: it is the type's public half for an operator-facing
    /// reader — the profile a run holds has a name, and the one way to spell it without
    /// re-deriving is here — so it stays beside [`as_str`](Self::as_str) rather than being
    /// removed with its last caller.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::sid::{PackageSid, package_name};
    ///
    /// let name = package_name(std::path::Path::new(r"E:\srv\tinio"));
    /// let sid = PackageSid::derive(&name).unwrap();
    /// assert_eq!(sid.name(), name);
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The raw SID, for the `SECURITY_CAPABILITIES` value the launch builds.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox_windows::sid::PackageSid;
    ///
    /// let sid = PackageSid::derive("tinio.sandbox.0123456789abcdef").unwrap();
    /// assert!(!sid.raw().is_null());
    /// ```
    pub fn raw(&self) -> PSID {
        self.raw
    }
}

/// What one `DeleteAppContainerProfile` call found — **the two states the API can
/// distinguish, and there is deliberately no third.**
///
/// `DeleteAppContainerProfile` answers `S_OK` for a name that was never created
/// exactly as it does for one it removed (measured on this host 2026-09-26, and
/// recorded beside each presentation below), so "deleted one" and "there was none"
/// are one answer: neither this type nor a caller of it may claim to tell them
/// apart. [`Self::Gone`] is "no profile with this name is left" — the only thing
/// the call knows — and [`Self::Refused`] is the call being refused, which is the
/// only state in which a profile with that name may still be on the machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deleted {
    /// No profile with this name is left. **Whether this call removed one is not
    /// knowable and is deliberately not claimed**: a run whose profile a peer had
    /// already taken is exactly the case that cannot be told from a deletion.
    Gone,
    /// This call was refused with this HRESULT, so a profile with this name may
    /// still be on the machine. What that leaves behind is each presentation's to
    /// say: [`PackageSid`]'s `Drop` warns and keeps the name, [`clear_profile`]
    /// answers the classified refusal, and `crate::sweep` warns and goes on.
    Refused(i32),
}

/// Delete the AppContainer profile `name` — **the crate's one `to_wide` + `unsafe` +
/// `SAFETY` note for this API, which all three presentations render from.**
///
/// **It is a primitive rather than a policy, because the three presentations do not
/// share one.** [`PackageSid`]'s `Drop` takes [`ProfileLock`] around the call and
/// emits the `step = "DeleteAppContainerProfile"` event with its `absent` field;
/// [`clear_profile`] renders the refusal [`capability::classify`] recognises and
/// emits nothing, because it runs under a no-op dispatcher; and `crate::sweep`'s
/// private `delete_profile` warns at its own step and answers `bool`, because a
/// sweep continues past a name it could not take. **Locking and eventing are those
/// sites' business and deliberately not this function's.** The profile-name rules
/// are this module's ([`package_name`], [`normalised_root`]), which is why the call
/// lives beside them.
pub(crate) fn delete_appcontainer_profile(name: &str) -> Deleted {
    let wide = to_wide(name);
    // SAFETY: `wide` is NUL-terminated and alive for the call, and `name` is the
    // profile this caller means to clear.
    let hr = unsafe { DeleteAppContainerProfile(wide.as_ptr()) };
    if hr == 0 {
        Deleted::Gone
    } else {
        Deleted::Refused(hr)
    }
}

/// Clear the profile `name`, answering the probe's one refusal — **the capability
/// probe's clear, and quietly so.**
///
/// **`Err` is the refusal [`capability::classify`] recognises, and that is the whole
/// contract.** A token the API refuses is refused the clear too: a clear of an absent
/// name under the Low token answers `0x80070005` on this host (measured by Task 4 fix
/// 3, and 3000/3000 under churn by the wave-E review), so the caller may answer "cannot
/// confine" from this call without asking the create. **`Ok` is not "the name is
/// gone"**: `DeleteAppContainerProfile` answers `S_OK` for an absent name for a token
/// the API admits (measured 2026-09-26, the same call [`PackageSid`]'s `Drop` records),
/// but an *unrecognised* non-zero HRESULT is `Ok` here too and leaves the name possibly
/// still present — the caller's create handles that, and an unrecognised code is
/// deliberately not a verdict (the wave-E review's finding 2).
///
/// **The probe clears the name before it creates it**, which is why this call exists:
/// for a token the API admits the clear is the cleanup that makes the caller's create a
/// real creation rather than a takeover of a crashed probe's residue.
///
/// **One kernel call, three presentations, and each presentation carries its own
/// obligation.** All three go through [`delete_appcontainer_profile`], the crate's one
/// call of this API and the one [`Deleted`] it answers; what differs is what each site
/// owes its readers. [`PackageSid`]'s `Drop` carries the
/// `step = "DeleteAppContainerProfile"` event with its `absent` field, which an operator
/// reads a run's log for and the e2e suite asserts on. This presentation carries the
/// refusal alone and emits nothing, which is the shape the probe needs — it runs under
/// a no-op dispatcher, so an event here would be discarded anyway. **`sweep`'s private
/// `delete_profile` is the third presentation**, and it carries the reaper's
/// continuation: warned at its own step and answering `bool`, because a sweep continues
/// past a name it could not take.
pub(crate) fn clear_profile(name: &str) -> Result<()> {
    match delete_appcontainer_profile(name) {
        // **An unrecognised refusal is still `Ok`**, for the reason the doc above
        // gives: only the code `classify` knows is a verdict.
        Deleted::Refused(hr) => match capability::classify(hr) {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        },
        Deleted::Gone => Ok(()),
    }
}

impl Drop for PackageSid {
    fn drop(&mut self) {
        // SAFETY: `raw` is this value's own allocation, produced by the profile
        // creation and freed exactly once here.
        unsafe { FreeSid(self.raw) };
        // **The delete is taken under this root's lock, against every launch on
        // it.** A peer holding the lock is between its derive and its
        // `CreateProcessW`, and that is the one interval where this delete
        // damages anything: the peer's own launch then fails `err=2
        // ERROR_FILE_NOT_FOUND` for a program that exists (measured 2026-09-26).
        // A running child is unharmed either way, which is why the lock is taken
        // around the delete rather than held for the child's life.
        let _lock = ProfileLock::acquire(&self.name);
        match delete_appcontainer_profile(&self.name) {
            Deleted::Refused(hr) => {
                // **The refusal is all this branch means.** It is
                // *not* "someone else deleted it": the API does not report that as a
                // failure at all — it answers `S_OK` for a name that is not there
                // (measured 2026-09-26) — so this branch is this call being
                // refused, and what is left behind is a profile this run could not
                // take back. A residue rather than a failure, and `Drop` has no
                // caller to report one to: the next run derives the same name and
                // takes this profile over (decision 21). Warned, because shared §7's
                // "a documented degradation is logged" is what makes an undeleted
                // profile visible in a log rather than inferable from a package
                // listing.
                tracing::warn!(
                    step = "DeleteAppContainerProfile",
                    name = %self.name,
                    hr = hr,
                    absent = false,
                    "the profile could not be deleted and may still be on this \
                     machine; the next run on this root takes it over (decision 21)"
                );
            }
            Deleted::Gone => {
                // The other half of the pair the creation emits, so a log answers
                // "was this process confined, and to what" from both ends of the
                // profile's life (shared §7).
                //
                // **[`Deleted::Gone`] is "no profile with this name is left", which is
                // not the same statement as "this call deleted one"**, and the field
                // says the first because that is what the call knows: measured on
                // this host 2026-09-26, `DeleteAppContainerProfile` answers `S_OK`
                // for a name that was never created just as it does for one it
                // removed, so an event that claimed the removal would be reporting
                // an action this value cannot observe — and a run whose profile a
                // peer had already taken is exactly that case.
                tracing::info!(
                    step = "DeleteAppContainerProfile",
                    name = %self.name,
                    sid = %self.text,
                    absent = true
                );
            }
        }
    }
}

/// The capability SIDs a container is created with.
///
/// Kept alive until the attribute list is deleted, because
/// `UpdateProcThreadAttribute` requires every value it is given to outlive the
/// list, and the kernel reads it again at `CreateProcessW` time rather than
/// copying it.
///
/// **Every entry carries [`SE_GROUP_ENABLED`], and a container's capability is
/// worth nothing without it.** The SID alone is placed in the child's token and
/// then honoured by nothing — not by the WFP AppContainer filters that enforce
/// the network policy, and not by an access check against a resource whose ACL
/// names the capability SID. [`CapabilitySids::resolve`] carries the
/// measurement.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_windows::sid::CapabilitySids;
///
/// // The default: no capability at all, which is what the outbound guarantee
/// // rests on (design §7).
/// let caps = CapabilitySids::resolve(&[]).unwrap();
/// assert!(caps.is_empty());
/// ```
pub struct CapabilitySids {
    /// **The one owner of every SID this value resolved.** The entries carry each
    /// SID as the kernel takes it, and `Drop` releases the same pointer rather
    /// than a second vector kept index-aligned with this one.
    entries: Vec<SID_AND_ATTRIBUTES>,
}

impl CapabilitySids {
    /// Resolve the capabilities a container may hold into entries.
    ///
    /// **The argument is [`Capability`], and that is a security boundary rather
    /// than a convenience for the caller.** Every entry below is stamped
    /// [`SE_GROUP_ENABLED`], and an enabled group SID is what the kernel honours
    /// — so every SID this function resolves is a group the container really
    /// holds. While the argument was `&[&str]` any text reached it, and a caller
    /// inside this workspace passing `S-1-5-32-544` (`BUILTIN\Administrators`)
    /// would have handed the container that group. The three-variant enum makes
    /// every SID outside the AppContainer capability range unspellable, because
    /// [`Capability::sid`] is the only producer of the text below.
    ///
    /// **There is no string-taking form beside this one, private or `pub(crate)`**,
    /// because nothing needs one: the enum is the crate's only source of
    /// capability SIDs, and the reverse direction does not exist either — a
    /// capability arrives as the name an operator wrote
    /// ([`Capability::from_name`]), never as a SID to parse back
    /// (`backend`'s `capability_names_round_trip_through_from_name` states why).
    ///
    /// # Errors
    ///
    /// [`Error::Launch`] when
    /// `ConvertStringSidToSidW` refuses to resolve a SID. **The refusal the
    /// string-taking signature used to document — "not a SID" — is unreachable
    /// through this one**: the three inputs are compile-time constants of the
    /// `S-1-15-3-N` form the conversion accepts, which
    /// `every_capability_the_enum_names_resolves_to_its_own_enabled_sid` pins by
    /// resolving all three, and which
    /// `tests/confine.rs`'s `the_whitelist_refuses_a_path_outside_the_exposures`
    /// drives end to end (its capability arm carries what the 2026-10-01 cleanup
    /// deleted `a_capability_from_the_options_reaches_the_container` for, and the
    /// list reaching the container's token is `backend`'s
    /// `a_granted_capability_is_enabled_in_the_container`). What is left is the
    /// FFI call failing for its own reason
    /// (its allocation refused), and that is still reported rather than
    /// unwrapped: the null check cannot be dropped, because a null `Sid` in the
    /// list is a failed `CreateProcessW` rather than a capability.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// let caps = CapabilitySids::resolve(&[Capability::InternetClient]).unwrap();
    /// assert_eq!(caps.len(), 1);
    ///
    /// // Empty is the default, and the configuration the outbound guarantee
    /// // rests on (design §7).
    /// assert!(CapabilitySids::resolve(&[]).unwrap().is_empty());
    /// ```
    pub fn resolve(capabilities: &[Capability]) -> Result<Self> {
        // Built incrementally as a `Self`, so the failure below drops the SIDs
        // already resolved instead of leaking them: the value is the only thing
        // that owns them, and it exists from the first iteration onwards.
        let mut this = Self {
            entries: Vec::with_capacity(capabilities.len()),
        };
        for text in capabilities.iter().map(|capability| capability.sid()) {
            let wide = to_wide(text);
            let mut raw: PSID = ptr::null_mut();
            // SAFETY: `wide` is NUL-terminated and alive for the call; the
            // returned SID is freed in `Drop` with LocalFree, as documented.
            let ok = unsafe { ConvertStringSidToSidW(wide.as_ptr(), &mut raw) };
            if ok == 0 || raw.is_null() {
                return Err(launch("resolve a capability SID", text, last_error()));
            }
            // **The attribute is not decoration, and leaving it zero made every
            // capability this crate granted inert.** Measured 2026-09-22 on this
            // host: a container launched with `internetClient` and
            // `Attributes: 0` carries `S-1-15-3-1` in its token — the list
            // reaches the kernel — and the SID is nevertheless honoured by
            // nothing. The confined child was refused `1.1.1.1:53` with
            // `permission denied` however many capabilities it was given, and a
            // directory carrying an ACE for the capability SID stayed unreadable
            // to it. With `SE_GROUP_ENABLED` the same child **reaches** that
            // address, and that ACE grants the read. So the SID being present is
            // not what the check is against; an enabled SID is, and there is no
            // second place this crate builds one.
            this.entries.push(SID_AND_ATTRIBUTES {
                Sid: raw,
                Attributes: SE_GROUP_ENABLED as u32,
            });
        }
        Ok(this)
    }

    /// The contiguous array the `SECURITY_CAPABILITIES` value points at.
    ///
    /// `*const` rather than the `*mut` that structure's field is declared with:
    /// nothing writes through it, and the launch is where the two meet.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// let caps = CapabilitySids::resolve(&[Capability::InternetClient]).unwrap();
    /// // One pointer, one count, and `len` is the count that goes with it.
    /// let _entries = caps.as_ptr();
    /// assert_eq!(caps.len(), 1);
    /// ```
    pub fn as_ptr(&self) -> *const SID_AND_ATTRIBUTES {
        self.entries.as_ptr()
    }

    /// How many capabilities are present.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// assert_eq!(CapabilitySids::resolve(&[]).unwrap().len(), 0);
    /// assert_eq!(
    ///     CapabilitySids::resolve(&[Capability::InternetClient])
    ///         .unwrap()
    ///         .len(),
    ///     1
    /// );
    /// ```
    pub fn len(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Whether the container gets no capability at all — the default, and the
    /// configuration the outbound guarantee rests on (design §7).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_windows::{backend::Capability, sid::CapabilitySids};
    ///
    /// assert!(CapabilitySids::resolve(&[]).unwrap().is_empty());
    /// assert!(
    ///     !CapabilitySids::resolve(&[Capability::InternetClient])
    ///         .unwrap()
    ///         .is_empty()
    /// );
    /// ```
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Drop for CapabilitySids {
    fn drop(&mut self) {
        for entry in &self.entries {
            // SAFETY: each `entry.Sid` came from ConvertStringSidToSidW, which
            // documents LocalFree as the release, and each is freed once.
            unsafe { LocalFree(entry.Sid.cast()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        os::windows::ffi::OsStringExt,
        path::{Path, PathBuf},
        sync::{Arc, Barrier, mpsc},
        thread,
        time::Duration,
    };

    use tinio_util::testing::{assert_recorded_at, assert_step};

    use super::*;
    use crate::testing::{capture, recorder};

    /// How many runs the creation race is opened with. **Eight rather than two**,
    /// because the window a loser reads in is the winner's create-to-link interval:
    /// a single pair can miss it by scheduling alone, and the value of the leg is
    /// that it does not depend on which runner won.
    const RUNNERS: usize = 8;

    /// A SID's `S-1-…` text, for the tests that compare a raw SID against the
    /// spelling this module or [`Capability`] carries.
    fn render_sid(sid: PSID) -> String {
        sid_text(sid).expect("a live SID renders")
    }

    #[test]
    fn the_package_name_is_root_derived_and_stable() {
        let a = package_name(Path::new(r"E:\data"));
        assert_eq!(a, package_name(Path::new(r"E:\data")));
        assert_ne!(a, package_name(Path::new(r"E:\other")));
        assert!(a.starts_with("tinio.sandbox."), "{a}");
        assert_eq!(a.len(), "tinio.sandbox.".len() + 16, "{a}");
        // AppContainer profile names are limited in length; the digest keeps
        // this short.
        assert!(a.len() < 64, "{a}");
    }

    #[test]
    fn one_directory_has_one_package_name_however_it_is_spelled() {
        // A second name for the same directory would mean a second SID, a second
        // ACE on the same tree, and a residue neither run would ever take over
        // (decision 21). Case, separator, a trailing separator and a `.` are all
        // the same directory.
        let expected = package_name(Path::new(r"E:\Data\Root"));
        assert_eq!(expected, package_name(Path::new(r"e:/data/root")));
        assert_eq!(expected, package_name(Path::new("E:\\Data\\Root\\")));
        assert_eq!(expected, package_name(Path::new(r"E:\Data\.\Root")));
        // `\\?\` and `\\?\UNC\` are the same directory as the plain spelling.
        // Leaving the prefix in the hash splits decision 21's one-SID takeover.
        assert_eq!(expected, package_name(Path::new(r"\\?\E:\Data\Root")));
        assert_eq!(expected, package_name(Path::new(r"\\?\E:\data\root\.")));
        assert_eq!(
            package_name(Path::new(r"\\server\share\data")),
            package_name(Path::new(r"\\?\UNC\server\share\data"))
        );
        // `..` stays lexical: resolving it would name a different directory.
        assert_ne!(
            package_name(Path::new(r"E:\other")),
            package_name(Path::new(r"\\?\E:\data\..\other"))
        );
    }

    /// One root's units: `E:\data\` followed by one code unit that is not text.
    fn root_with(tail: u16) -> Vec<u16> {
        let mut units: Vec<u16> = r"E:\data\".encode_utf16().collect();
        units.push(tail);
        units
    }

    #[test]
    fn two_roots_that_differ_only_outside_utf8_get_different_package_names() {
        // **This is the collapse the lossy half alone could not see.** A Windows
        // path is arbitrary UTF-16 and an unpaired surrogate is a legal unit in
        // one, so `E:\data\<U+D800>` and `E:\data\<U+DC00>` are two directories —
        // and `to_string_lossy` reads both as `e:\data\ufffd`. One package name
        // for the two would be one SID, one AppContainer profile and so one ACE
        // tree between them, and the first `Drop` would delete the profile the
        // other was still launching under (decision 21). No file has to exist for
        // the derivation, which is lexical.
        let high = PathBuf::from(OsString::from_wide(&root_with(0xD800)));
        let low = PathBuf::from(OsString::from_wide(&root_with(0xDC00)));
        // **The two are one root to the lossy reading, and two to the name** —
        // which is the whole fix, asserted on the two halves rather than inferred
        // from the code: the fold the old derivation hashed is identical here, so
        // a name that differed only because the text differed would be
        // indistinguishable from the collapse.
        let folded = |path: &Path| {
            plain_spelling(path)
                .components()
                .collect::<PathBuf>()
                .to_string_lossy()
                .replace('/', r"\")
                .to_lowercase()
        };
        assert_eq!(
            folded(&high),
            folded(&low),
            "one string to the lossy reading"
        );
        assert_ne!(package_name(&high), package_name(&low));
        // Each is still stable, which is what the takeover needs.
        assert_eq!(package_name(&high), package_name(&high));
        // And neither is the name the lossy spelling used to give both of them:
        // the text half is still hashed, but it can no longer decide the name.
        let replacement = PathBuf::from(format!("E:\\data\\{}", '\u{fffd}'));
        assert_ne!(package_name(&high), package_name(&replacement));
        assert_ne!(package_name(&low), package_name(&replacement));
        // The rest of the name's shape is unchanged by the second reading.
        assert!(package_name(&high).starts_with("tinio.sandbox."));
        assert_eq!(package_name(&high).len(), "tinio.sandbox.".len() + 16);
    }

    #[test]
    fn a_kelvin_sign_and_an_ascii_k_are_two_roots() {
        // **Measured on this host, NTFS, 2026-09-24: `…\K` (U+212A) and `…\k`
        // create two directories, and `…\Ä` and `…\ä` create one.** `str::to_lowercase`
        // is many-to-one in the first case (U+212A → U+006B), so the text half
        // alone merges two directories; [`fold_unit`] folds uppercase, which keeps
        // them apart and still merges the second case.
        assert_ne!(
            package_name(Path::new("E:\\data\\\u{212a}")),
            package_name(Path::new(r"E:\data\k"))
        );
        assert_eq!(
            package_name(Path::new("E:\\data\\\u{00e4}")),
            package_name(Path::new("E:\\data\\\u{00c4}"))
        );
        // The fold this module has always promised still holds, because the case
        // fold the units take is the ASCII one the filesystem itself applies.
        assert_eq!(
            package_name(Path::new(r"E:\Data")),
            package_name(Path::new(r"e:\data"))
        );
    }

    #[test]
    fn a_verbatim_prefix_is_stripped_off_a_path_that_is_not_text() {
        // The prefix strip used to need `to_str`, so it was skipped for exactly
        // the paths this module may not be lossy about: `\\?\E:\data\<U+D800>`
        // and `E:\data\<U+D800>` are one directory and must be one name, or the
        // residue of the verbatim spelling is never taken over.
        let mut verbatim: Vec<u16> = r"\\?\".encode_utf16().collect();
        verbatim.extend_from_slice(&root_with(0xD800));
        let prefixed = PathBuf::from(OsString::from_wide(&verbatim));
        let plain = PathBuf::from(OsString::from_wide(&root_with(0xD800)));
        assert_eq!(package_name(&prefixed), package_name(&plain));
        // The same for the UNC form, unit by unit.
        let mut unc: Vec<u16> = r"\\?\UNC\server\share\".encode_utf16().collect();
        unc.push(0xDC00);
        let mut plain_unc: Vec<u16> = r"\\server\share\".encode_utf16().collect();
        plain_unc.push(0xDC00);
        assert_eq!(
            package_name(&PathBuf::from(OsString::from_wide(&unc))),
            package_name(&PathBuf::from(OsString::from_wide(&plain_unc)))
        );
        // The three legal spellings the existing test pins are untouched by it.
        assert_eq!(
            package_name(Path::new(r"\\?\E:\Data\Root")),
            package_name(Path::new(r"E:\data\root"))
        );
        assert_eq!(
            package_name(Path::new(r"\\?\UNC\server\share\data")),
            package_name(Path::new(r"\\server\share\data"))
        );
    }

    /// **One salt, one name; two salts, two names** — the derivation's own half of
    /// the hardening, with no file involved.
    ///
    /// It would fail without the salt in [`name_digest`]'s input: two accounts would
    /// derive one name for one root, which is the pre-computation exposure the salt
    /// exists to close. The one-byte case is there because a salt that only differed
    /// in its whole-width pattern would be a coarser knob than the 32 bytes suggest,
    /// and the unsalted comparison is what makes the degradation a *different* name
    /// rather than the same name with a different story.
    #[test]
    fn one_salt_gives_one_name_and_two_salts_two() {
        let root = Path::new(r"E:\srv\tinio");
        let one = Salt::from_bytes([0x11; SALT_BYTES]);
        let other = Salt::from_bytes([0x22; SALT_BYTES]);
        assert_eq!(package_name_with(root, &one), package_name_with(root, &one));
        assert_ne!(
            package_name_with(root, &one),
            package_name_with(root, &other)
        );
        assert_ne!(
            package_name_with(root, &one),
            package_name_with(root, &Salt::from_bytes([0x12; SALT_BYTES])),
            "one byte of salt is enough to move the name"
        );
        assert_ne!(package_name_with(root, &one), package_name_unsalted(root));
        // The salt changes the name and nothing else about it.
        assert!(package_name_with(root, &one).starts_with("tinio.sandbox."));
        assert_eq!(
            package_name_with(root, &one).len(),
            "tinio.sandbox.".len() + 16
        );
        // The normalisation is still what decides which *root* a name is about, so
        // the salt did not take over the spelling rule the takeover rests on.
        assert_eq!(
            package_name_with(root, &one),
            package_name_with(Path::new(r"e:/srv/tinio/."), &one)
        );
    }

    /// **The salt file is created once, holds [`SALT_BYTES`], and a second run
    /// derives under the value it reads** rather than under a fresh one.
    ///
    /// It would fail without [`Salt::load_at`]'s read path: a run that generated a
    /// value every time would leave every ACE of the previous run standing under a
    /// SID nothing derives again — decision 21's takeover would never fire — and the
    /// replacement half is what tells a read from a create, because both produce a
    /// 32-byte file.
    #[test]
    fn the_salt_file_is_created_once_and_a_second_launch_reads_it() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-salt");
        let root = Path::new(r"E:\srv\tinio");

        let first = test_path::with(&file, || package_name(root));
        let created = fs::read(&file).expect("the first run created the salt file");
        assert_eq!(created.len(), SALT_BYTES, "the file holds a salt");
        let created: [u8; SALT_BYTES] = created.as_slice().try_into().expect("32 bytes");
        assert_eq!(
            first,
            package_name_with(root, &Salt::from_bytes(created)),
            "the name the first run derived is the one the bytes it wrote give"
        );

        assert_eq!(
            test_path::with(&file, || package_name(root)),
            first,
            "a second run reads the value the first one published"
        );

        // **The value on disk is what decides**, which is the difference between
        // reading the file and caching a value per process.
        let replaced = [0x5A; SALT_BYTES];
        fs::write(&file, replaced).expect("replace the salt");
        assert_eq!(
            test_path::with(&file, || package_name(root)),
            package_name_with(root, &Salt::from_bytes(replaced)),
            "the derivation follows the file, not a remembered value"
        );
    }

    /// **A fresh salt file and eight runs released together derive one name.**
    ///
    /// The window is real: the file is created by whichever run gets there first and
    /// every other run finds it — or finds the name taken, which is the loser's path
    /// — and the value that ends up on disk is one of them. It would fail without the
    /// publish [`Salt::create_at`] makes: a create-then-write at the final name lets a
    /// loser read a file with no bytes in it and degrade to the unsalted derivation,
    /// and a *rename* publish lets a loser replace the winner's salt, after which the
    /// two runs derive two names while only one of them stands on disk — the residue
    /// of the other is then never taken over (decision 21).
    #[test]
    fn eight_races_on_a_fresh_salt_file_agree_on_one_value() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-salt");
        let root = Path::new(r"E:\srv\tinio");
        let barrier = Arc::new(Barrier::new(RUNNERS));

        let names: Vec<String> = thread::scope(|scope| {
            let runners: Vec<_> = (0..RUNNERS)
                .map(|_| {
                    let file = file.clone();
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        test_path::with(&file, || package_name(root))
                    })
                })
                .collect();
            runners
                .into_iter()
                .map(|runner| runner.join().expect("no runner panicked"))
                .collect()
        });

        assert!(
            names.iter().all(|name| *name == names[0]),
            "the runners derived different names for one root and one salt file — a \
             loser read a file the winner had not finished publishing: {names:?}"
        );
        let published = fs::read(&file).expect("the salt file was created");
        assert_eq!(published.len(), SALT_BYTES, "{published:?}");
        assert_eq!(
            names[0],
            package_name_with(
                root,
                &Salt::from_bytes(published.as_slice().try_into().expect("32 bytes"))
            ),
            "the value every runner derived under is the one on disk"
        );
    }

    /// **A salt that cannot be created degrades — to the historical name, with a
    /// warning, and never silently.**
    ///
    /// The injection is a file standing where the state directory would be, so the
    /// directory create fails: the earliest of the calls the create makes, and one no
    /// permission on the salt file alone would reach. What the leg asserts is the two
    /// halves of the policy — the derivation falls back to
    /// [`package_name_unsalted`], and the fallback is *said*, at `WARN`, with the
    /// step that decided it. Without the warning a run would derive a name whose
    /// exposure the salt exists to close and nothing would say so.
    #[test]
    fn a_salt_that_cannot_be_created_degrades_to_the_unsalted_name() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let blocker = directory.path().join("a-file");
        fs::write(&blocker, b"not a directory").expect("seed the blocker");
        let file = blocker.join("acl-salt");
        let root = Path::new(r"E:\srv\tinio");

        let capture = capture();
        let recording = capture.record();
        let name = test_path::with(&file, || package_name(root));
        let logged = capture.text();
        drop(recording);

        assert_eq!(
            name,
            package_name_unsalted(root),
            "the degraded run derives the unsalted name and does not pretend"
        );
        assert_step(&logged, "load the ACL salt");
        assert_recorded_at(&logged, "unsalted package name", tracing::Level::WARN);
        assert!(!file.exists(), "nothing was created behind the blocker");
    }

    /// **A salt file of another length is refused rather than stretched, padded or
    /// written over.**
    ///
    /// It would fail if [`Salt::from_bytes_at`] accepted any non-empty file: half a
    /// salt derives a name no run can reproduce — the next run reads a different
    /// value, so decision 21's takeover never finds its own residue — and padding
    /// would put a name behind a value the file does not hold. The refusal is the
    /// same warned fallback as an uncreatable file, for the same reason.
    #[test]
    fn a_salt_file_of_the_wrong_length_is_refused_rather_than_used() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-salt");
        fs::write(&file, b"not a salt, only sixteen").expect("seed the wrong file");
        let root = Path::new(r"E:\srv\tinio");

        let capture = capture();
        let recording = capture.record();
        let name = test_path::with(&file, || package_name(root));
        let logged = capture.text();
        drop(recording);

        assert_eq!(name, package_name_unsalted(root));
        assert_step(&logged, "load the ACL salt");
        assert_recorded_at(&logged, "does not hold a salt", tracing::Level::WARN);
        assert_eq!(
            fs::read(&file).expect("the file is still there").len(),
            b"not a salt, only sixteen".len(),
            "a file that is not a salt is left as it was found"
        );
    }

    #[test]
    fn creating_the_profile_yields_the_derived_sid_and_a_second_run_takes_it_over() {
        // Measured on 2026-09-19: a SID that is *only* derived cannot launch —
        // handing it to PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES makes
        // CreateProcessW fail with err=2 ERROR_FILE_NOT_FOUND for our own binary
        // and for cmd.exe alike, ACE already granted or not — and the same call
        // succeeds once `CreateAppContainerProfile` has run. Task 8's launch test
        // is where that is asserted end to end; this one pins the two facts the
        // rest of the plan rests on: the profile's SID is the derivation's SID,
        // and creating an existing profile is a takeover rather than an error.
        let name = package_name(Path::new(r"E:\data"));
        let sid = PackageSid::derive(&name).unwrap();
        assert!(sid.as_str().starts_with("S-1-15-2-"), "{}", sid.as_str());

        let wide = to_wide(&name);
        let mut derived: PSID = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `derived`
        // is a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut derived) };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let rendered = render_sid(derived);
        // SAFETY: `derived` came from the derivation above, whose documented
        // release is FreeSid, and it is freed exactly once here.
        unsafe { FreeSid(derived) };
        assert_eq!(
            rendered,
            sid.as_str(),
            "the profile's SID is the derived SID"
        );

        // The second creation is what a run after a hard kill does, and it is
        // success: `HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS)`, reported **without**
        // the SID out-parameter filled in — measured on this host 2026-09-21, and
        // the Windows SDK's sample answers that code the same way. What the
        // takeover rests on is therefore not the collision's output but the name,
        // which the derivation below turns into the same SID (decision 21).
        let mut again: PSID = ptr::null_mut();
        // SAFETY: as above: a null capability array with a count of zero is the
        // documented "no capabilities", and whatever the out-parameter holds is
        // freed below.
        let hr = unsafe {
            CreateAppContainerProfile(
                wide.as_ptr(),
                wide.as_ptr(),
                wide.as_ptr(),
                ptr::null(),
                0,
                &mut again,
            )
        };
        assert_eq!(hr, HRESULT_ALREADY_EXISTS, "hr=0x{hr:08X}");
        // SAFETY: `again` is this call's own allocation when it handed one back at
        // all, and the collision promises nothing about that — so the free is
        // guarded rather than assumed.
        if !again.is_null() {
            unsafe { FreeSid(again) };
        }

        // The residue is *reusable*, which is the property rather than the
        // collision's return value: the same name derives the same SID the first
        // creation returned, so a takeover has something to launch under.
        let mut reused: PSID = ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated and alive for the call, and `reused` is
        // a valid out-parameter; the SID is freed below with FreeSid.
        let hr = unsafe { DeriveAppContainerSidFromAppContainerName(wide.as_ptr(), &mut reused) };
        assert_eq!(hr, 0, "hr=0x{hr:08X}");
        let reused_rendered = render_sid(reused);
        // SAFETY: `reused` came from the derivation above, whose documented
        // release is FreeSid, and it is freed exactly once here.
        unsafe { FreeSid(reused) };
        assert_eq!(
            reused_rendered,
            sid.as_str(),
            "the residue is reusable: one name, one SID"
        );

        // And the module's own takeover, which is what the launcher calls: the
        // same name while the first value still holds the profile alive.
        let taken_over = PackageSid::derive(&name).unwrap();
        assert_eq!(
            taken_over.as_str(),
            sid.as_str(),
            "the takeover keeps the SID"
        );
    }

    #[test]
    fn a_denied_profile_creation_classifies_rather_than_reports_a_bare_hr() {
        // `capability::`, not `crate::capability::`: this crate's `[lints.clippy]` sets
        // `absolute_paths`, and `-D warnings` turns the longer spelling into a build failure.
        let err =
            capability::classify(capability::E_ACCESSDENIED).expect("E_ACCESSDENIED classifies");
        assert!(matches!(err, Error::HostCannotConfine { .. }), "{err}");
        // And the code that must NOT classify keeps the old path.
        assert!(capability::classify(0x8007_0003u32 as i32).is_none());
        // **What `derive` does with these answers has no runtime-injectable predicate.** No test
        // forces `userenv` to answer `E_ACCESSDENIED` on a host that works, so the wiring — both
        // of `derive`'s `hr != 0` arms handing their HRESULT to `classified`, and the create arm
        // routing its refusal through `capability::refuse` — is not observable as a value; it
        // lives in `derive`'s own body and its comments. The behavioral half above and the
        // `classified` leg below drive what each delegate *answers*; the delegation itself is a
        // review-time property.
    }

    /// **The helper's own behaviour, driven rather than read.** `classified` is the one place
    /// [`PackageSid::derive`]'s HRESULTs are classified; no test can make `userenv` answer
    /// `E_ACCESSDENIED` on a host that works, but the classification takes a bare HRESULT — so
    /// both of its arms are reachable as *values*, and this seam reaches them. The slice above
    /// keeps the delegation pinned; this leg pins what the delegate answers: the capability
    /// answer is the host refusal, and anything else stays a launch failure naming the step that
    /// ran, with the code verbatim.
    #[test]
    fn classified_maps_the_denied_hr_to_the_host_refusal_and_anything_else_names_the_step() {
        let denied = classified(
            "CreateAppContainerProfile",
            "name",
            capability::E_ACCESSDENIED,
        );
        assert!(
            matches!(denied, Error::HostCannotConfine { hr, .. } if hr == capability::E_ACCESSDENIED),
            "{denied}"
        );
        match classified("CreateAppContainerProfile", "name", 0x8007_0003u32 as i32) {
            Error::Launch {
                operation,
                path,
                source,
            } => {
                let rendered = source.to_string();
                assert_eq!(operation, "CreateAppContainerProfile", "{rendered}");
                assert_eq!(path, PathBuf::from("name"), "{rendered}");
                assert!(rendered.contains("hr=0x80070003"), "{rendered}");
            }
            other => panic!("a non-denied HRESULT is a launch failure, got {other}"),
        }
    }

    #[test]
    fn no_capabilities_is_the_default_and_is_free() {
        let caps = CapabilitySids::resolve(&[]).unwrap();
        assert!(caps.is_empty());
        assert_eq!(caps.len(), 0);
    }

    /// **The property the string-taking signature could not state**: what this
    /// value hands `CreateProcessW` is one of the enum's own SIDs — no caller
    /// spells one — and every entry is enabled, since an inert `Attributes: 0` is
    /// what made each capability this crate granted do nothing (the measurement
    /// is on [`CapabilitySids::resolve`]).
    ///
    /// **That an enabled SID is honoured is not assertable here**, because it
    /// takes a container: `backend::tests`'s
    /// `a_granted_capability_is_enabled_in_the_container` reads it out of the
    /// child's token through an ACL and a socket, and that is where the
    /// measurement lives.
    #[test]
    fn every_capability_the_enum_names_resolves_to_its_own_enabled_sid() {
        let all = [
            Capability::InternetClient,
            Capability::InternetClientServer,
            Capability::PrivateNetworkClientServer,
        ];
        let caps = CapabilitySids::resolve(&all).unwrap();
        assert_eq!(caps.len(), all.len() as u32);
        for (index, capability) in all.iter().enumerate() {
            // SAFETY: `as_ptr` points at `len` initialized entries and `index` is
            // below that count, because it enumerates the list they were resolved
            // from; the reference borrows the array for the loop body, which ends
            // before the value drops.
            let entry = unsafe { &*caps.as_ptr().add(index) };
            assert_eq!(entry.Attributes, SE_GROUP_ENABLED as u32, "{capability:?}");
            assert_eq!(render_sid(entry.Sid), capability.sid(), "{capability:?}");
        }
    }

    /// **The window's whole purpose**: a peer's delete waits for the launch that is
    /// holding the lock, so it can never land between the derive and
    /// `CreateProcessW`.
    ///
    /// The shape is a second holder's delete deferred while the first holds the
    /// lock, which is deterministic — a raced pair of real launchers would not be,
    /// because the window is a race by construction and a test that won it would
    /// pass for the wrong reason. The negative assertion below is still a bound, so
    /// what makes it mean anything is the **progress signal**: the peer says it is
    /// at its drop before the wait starts, because a `derive` is a userenv profile
    /// creation whose duration no bound controls and a peer that had not got there
    /// yet would satisfy the same `is_err()`.
    #[test]
    fn a_peer_delete_waits_for_the_window_a_launch_holds() {
        let dir = tempfile::tempdir().expect("temp dir");
        let name = package_name(dir.path());
        // The launch's window, held by hand for as long as this test needs it: in
        // the launcher it is `prepare_launch`'s derive through `CreateProcessW`.
        let window = ProfileLock::acquire(&name).expect("the per-root lock");
        let (derived_tx, derived_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let peer = thread::spawn(move || {
            // A peer process's value for the same root: it takes the profile over,
            // and dropping it is that peer's exit.
            let sid = PackageSid::derive(&name).expect("the takeover");
            derived_tx.send(()).expect("the peer reached its drop");
            drop(sid);
            done_tx.send(()).expect("the peer reports its exit");
        });
        derived_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the peer derived its value and is at its drop");
        assert!(
            done_rx.recv_timeout(Duration::from_millis(500)).is_err(),
            "the peer's delete ran inside the launch window, so it could land \
             between the derive and CreateProcessW and turn a launch of a program \
             that exists into err=2 ERROR_FILE_NOT_FOUND"
        );
        drop(window);
        done_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("the delete lands once the window closes");
        peer.join().expect("the peer thread");
    }

    /// **`S_OK` from `DeleteAppContainerProfile` is not "a deletion happened"**, and
    /// the value whose profile a peer already deleted is the case that proves it:
    /// measured on this host 2026-09-26, the call answers `S_OK` for a name that was
    /// never created, for one deleted a moment ago and for a name never used at all
    /// — so a `Drop` that logged a deletion on `hr == 0` logged one that did not
    /// happen, and an operator reading the stream would believe the profile was taken
    /// back when it may never have been there.
    ///
    /// The other half of the same defect is the warning: `hr != 0` is not "someone
    /// else deleted it" (there is no such answer — see above), it is the delete
    /// having failed, so what is left behind is a profile this run could not remove.
    /// That branch has no test: the API refuses a delete for a name it will not
    /// resolve, and this crate cannot build one of those without failing its own
    /// derivation first.
    #[test]
    fn a_delete_that_found_no_profile_is_not_reported_as_a_deletion() {
        let _recorder = recorder();
        let dir = tempfile::tempdir().expect("temp dir");
        let name = package_name(dir.path());
        // The takeover, then the creator's exit: `second` is a live `PackageSid`
        // whose profile the delete above has already removed — the exact state the
        // measured `S_OK` answers for.
        let first = PackageSid::derive(&name).expect("create the profile");
        let second = PackageSid::derive(&name).expect("take it over");
        drop(first);

        let capture = capture();
        let logged = {
            let _recording = capture.record();
            drop(second);
            capture.text()
        };
        assert!(
            logged.contains(r#"step="DeleteAppContainerProfile""#),
            "the event the profile's removal is read from: {logged}"
        );
        assert!(
            logged.contains(&format!("name={name}")) && logged.contains("absent=true"),
            "the event must report what the call leaves behind rather than a \
             deletion it cannot claim: {logged}"
        );
        // The delete succeeded here, so the warning the refused-delete branch writes
        // must not be what this recorded: a regression that took `hr != 0` for this
        // case would say the profile may still be on the machine, and `absent=false`
        // is the field that carries it.
        assert!(
            !logged.contains("absent=false") && !logged.contains("WARN"),
            "this delete answered `S_OK`, so the event is the `INFO` one and not \
             the warning a refused delete writes: {logged}"
        );
    }

    /// The lock is **per served root**, which is the same key the profile is named
    /// from: two roots are two profiles and must not wait for one another.
    ///
    /// The consequence of getting this wrong is a lock that serialises unrelated
    /// roots — a launch blocked behind another root's ACL phase, which is minutes on
    /// a large tree — so the assertion is that a second root's acquisition completes
    /// while the first root's lock is held.
    #[test]
    fn a_different_root_is_a_different_lock() {
        let held_dir = tempfile::tempdir().expect("temp dir");
        let other_dir = tempfile::tempdir().expect("temp dir");
        let held = ProfileLock::acquire(&package_name(held_dir.path())).expect("the per-root lock");
        let other_name = package_name(other_dir.path());
        assert_ne!(other_name, package_name(held_dir.path()));
        let (done_tx, done_rx) = mpsc::channel();
        let peer = thread::spawn(move || {
            let lock = ProfileLock::acquire(&other_name).expect("the other root's lock");
            done_tx.send(()).expect("the peer reports the acquisition");
            drop(lock);
        });
        done_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("another root's launch is not behind this root's window");
        peer.join().expect("the peer thread");
        drop(held);
    }
}
