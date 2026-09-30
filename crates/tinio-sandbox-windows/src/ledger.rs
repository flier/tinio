//! The ACL grant ledger: **one appended line per path a run granted**, so an
//! operator can see which standing grants exist and take the ones they no longer
//! want back (design §6, decision 15).
//!
//! **What it is for, and what it is not.** Every grant this crate makes is
//! standing: nothing takes an ACE back on the way out — not on the clean path and
//! not on the failure path — so the residue of a served root is one package-SID ACE
//! per distinct root forever until someone removes it (decision 15, and the Risks
//! table's residue row owns where the four kinds of residue are found). This module
//! is the record of what was granted, at
//! `%LOCALAPPDATA%\tinio\acl-ledger.jsonl` — **the file beside the salt**
//! ([`crate::sid`]'s state directory, resolved once there), one JSON object per
//! line: what the path is to the run (`kind`, below), the path as the caller
//! spelled it, its normalisation, the package name and the `S-1-15-2-…` SID every
//! ACE of the run names, the UTC instant, and the program directory whose
//! `ALL APPLICATION PACKAGES` grant is the machine-wide one.
//!
//! **A run's ACEs are more than its root's, and the hook writes a line for each
//! of them.** `grant_whitelist` hands `record_run` the run's whole set in one call,
//! once its grant phase has returned `Ok` — the served root, then every other path
//! the caller exposed, the state directory (`TINIO_STATE_DIR`) in the product,
//! which is granted read-write and stands outside the root — because a ledger that
//! listed only the root would miss a standing ACE this crate wrote, and the file's
//! whole purpose is that an operator's audit lists them all. The extra line is this
//! module's line to the field, with `kind` naming which half of the run it is
//! about: on a `served-root` line the normalisation is what the package name is
//! derived from, and on an `exposure` line it is a normalisation of the path the
//! same run's SID holds an ACE on (`Grant::kind` carries the distinction and
//! `record_run`'s caller is where the two are told apart).
//!
//! **A run's lines are one transaction, and that is the shape `record_run` exists
//! for.** The ledger is one file, and the per-line shape this entry point replaces
//! took its lock, asked its descriptor back and opened it once per line —
//! microseconds apart, on a file that had not changed between them — so a run
//! exposing N paths paid N lock acquisitions and N `ensure_private` read-backs, and
//! each read-back is a token open, two `GetTokenInformation` calls and a DACL read
//! with an ACE walk (`crate::sid`'s `ensure_state_directory` is where that price is
//! written down, and it says there that the ledger reached it once per line). One
//! transaction makes the cost the run's rather than the path's; `record_run`'s own
//! doc carries what its single lock and its single read-back cover, and
//! `append_run`'s carries the order they are taken in.
//!
//! **The file is published the way the salt is published, because it publishes
//! what the salt hides.** A line carries the package name and the full package
//! SID in cleartext, and a reader of the line can call
//! `CreateAppContainerProfile` with that name to obtain a token carrying the SID
//! every standing ACE names — so a ledger created under the state directory's
//! inherited grants would hand another local account the exposure the salt exists
//! to close (measured on this host 2026-09-28: an inherited
//! `CodexSandboxUsers:(I)(M)` on the ledger beside `current-user-only` on the
//! salt). `append_run` therefore creates the file the way
//! [`crate::sid::Salt`] creates the salt: `tinio_winsec::create_restricted` makes
//! a private object, the run's first line goes into it, and `fs::hard_link`
//! publishes it under the ledger's name — so the name never exists without both its
//! value and its descriptor. A ledger that already exists is read back once, before
//! the run's first appended byte, and put right if it is not the current user's
//! alone (`ensure_private`), which is what repairs a file an earlier revision of
//! this crate created under the directory's inherited ACL, and what refuses to
//! write a run's lines into a file another account planted at the ledger's name.
//!
//! **What that protects, and what it does not.** It protects the *record*: the
//! names and SIDs of past grants are this user's alone on disk, which is the same
//! statement the salt's own descriptor makes. It does **not** protect a live
//! profile's own name: `%LOCALAPPDATA%\Packages\<name>` exists for as long as the
//! profile does, its name is the derivation's, and an account with access to
//! `%LOCALAPPDATA%\Packages` — measured on this host as an inherited descriptor
//! that reaches the operator's group — can read that name and create a profile
//! under it. The salt closes *pre-computation* (nobody derives a name for a root
//! they do not serve); it does not close the name a live profile publishes, and
//! design §6 states both halves rather than implying the first is the second.
//!
//! **It is an audit surface and never a control, and the reason is the rule the
//! item was opened on**: *a SID belonging to a run started after the ledger was read
//! must not be removed*. A reader that had been handed the ledger as a work list —
//! "remove every SID in this file" — would remove a SID whose run began after the
//! read, and there is no ordering between "the ledger was read" and "the run
//! started" that a file can carry. The rule is kept here **by construction rather
//! than by care: this module appends and never removes, never edits and never
//! rewrites**, so no entry can go stale under a reader, and the "read the ledger,
//! then revoke" sequence has no implementation inside the crate at all. What an
//! operator does with the lines is theirs, and the removal primitive is
//! [`crate::acl::revoke`] — `icacls <dir> /remove:g "*<sid>"`, which is the same
//! call an operator makes by hand and the one the launcher deliberately never
//! makes. A line this module wrote is a statement about a past grant; nothing here
//! claims the SID is still there.
//!
//! **A run's lines are written after the grant phase has succeeded, and that
//! placement is the point.** [`grant_whitelist`](mod@crate::backend) calls
//! `record_run` once, after every grant of the launch has returned `Ok` — so the
//! ledger never carries a root whose grant failed, and a run that dies inside the
//! phase leaves the ACEs it wrote unrecorded. That is the direction to err in: an
//! audit line is a claim that a grant *happened*, and a failed phase that recorded
//! itself would be a false one. **It is the phase's last act and it follows the
//! phase's own `info!` report**, so the duration that line publishes is the grant
//! phase's and does not absorb this file's I/O. **Nothing is recorded from inside
//! [`crate::acl`]'s per-path write** either, where a partial pass would have to
//! decide per path what a "successful grant" is — the phase's success is what
//! releases the run's lines, so a line can only ever state the phase's own verdict
//! rather than a path's.
//!
//! **A ledger that cannot be written warns and the run continues.** It is an audit
//! surface, and a server that refused to confine itself because a JSON line could
//! not be appended would trade the guarantee the operator asked for against a
//! record of it — the same ruling the profile lock and the per-path ACL lock carry
//! (`crate::sid::ProfileLock`, [`crate::acl`]'s `AclLock`). The failure is *never
//! silent*: `record_run` has no `Result` to return, so the signature itself is the
//! "cannot fail a grant" statement, and the warning beside the failure is where an
//! operator sees that this run went unrecorded. **The unit that fails is the run
//! rather than the line**, which one transaction makes possible: the lock, the
//! descriptor read-back and the handle serve the whole set, so a ledger this run
//! cannot write leaves it unrecorded with one warning rather than one per path, and
//! a write that dies part-way stops there — the lines already appended stay, since
//! this module never rewrites, and the rest of the run is what the warning says is
//! missing. **A line that cannot be published *privately* is one of those failures
//! and not a write that proceeds anyway**: a ledger whose descriptor cannot be read
//! back, or put right when it is not the current user's alone, leaves this run
//! unrecorded rather than publishing the name and the SID the file exists to keep to
//! one user (`ensure_private`).
//!
//! **Two runs cannot interleave their lines, and the lock is the per-path one the
//! grant layer already owns.** `record_run` takes `AclLock`
//! (`crate::acl`'s) — the same kernel file lock a grant holds across its
//! descriptor write, keyed on the *ledger's own path* rather than on a served root
//! — around the open and every line of the run, so two runs appending to one ledger
//! are a sequence rather than an interleaving, and neither can land a line between
//! two of the other's. The lock's own doc carries the primitive and the wait it
//! reports; what belongs here is that the exclusion is per file and not per root,
//! because the thing both writers share is the file.
//!
//! **A path is recorded as text and the digest is the identity.** The two spellings
//! are rendered for a human reader, and a Windows path is arbitrary UTF-16 — so a
//! root with a unit that is not text renders with U+FFFD in it, exactly as
//! [`crate::sid::package_name`]'s lossy half does. A reader who needs the identity
//! of a root that is not text reads the *package name*: the digest behind it is
//! taken over the lossless UTF-16 units as well, and it is the value the ACEs and
//! the profile carry.
//!
//! **Four dependency edges, all on crates the workspace lock already resolves and
//! this crate's tree already reaches** — the treatment design decision 14's
//! enumeration gives `windows-sys`, an edge rather than a crate.
//! [`serde`]'s `derive` is what a line is rendered through, so the record borrows
//! the grant's own fields instead of copying them into a value tree first.
//! [`serde_json`] builds the line, because a record that spells JSON by hand has to
//! escape Windows path separators and quotes itself, which is the class of bug an
//! audit file cannot have. [`time`] (with `formatting`) renders the instant as
//! RFC 3339, the spelling `tinio-api`'s state file already publishes
//! (`crates/tinio-api/src/state.rs`), so two operator-facing records of this
//! workspace answer "when" the same way. [`tinio_winsec`] is the file API the
//! salt already uses — `create_restricted` for the create-time descriptor,
//! `acl_is_current_user_only` and `restrict_to_current_user` for the read-back and
//! the repair — so nothing here is a new `unsafe` site: decision 14's enumeration
//! is unchanged, and this module carries no `allow(unsafe_code)`.

use std::{
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use serde::Serialize;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    acl::AclLock,
    sid::{self, ensure_state_directory, state_directory},
};

/// The ledger's file name inside [`state_directory`].
const LEDGER_FILE: &str = "acl-ledger.jsonl";

/// **What the line's path is to the run** — the one field that says whether the
/// normalisation beside it is what the package name was derived from or only a
/// normalisation of a path that name's SID holds an ACE on.
///
/// **It exists because a run writes more than one standing ACE and the lines must
/// not be confusable.** `record_run`'s caller writes one line for the served root
/// and one for every other path the caller exposed (the state directory in the
/// product), and without this field a reader counting "served roots" would count
/// the state directory as one — while the derived half of the record, `normalised`,
/// is a different statement on the two lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathKind {
    /// **The served root**: the path the package name was derived from, whose
    /// normalisation is the derivation's input
    /// ([`sid::package_name`](crate::sid::package_name)).
    ServedRoot,
    /// **One more path the caller exposed**, which this run's package SID holds a
    /// standing ACE on without having derived anything from it — read-write on the
    /// state directory, and the reason the ledger records it at all.
    Exposure,
}

impl PathKind {
    /// The spelling the line carries, so the JSON and this enum cannot drift.
    fn as_str(self) -> &'static str {
        match self {
            Self::ServedRoot => "served-root",
            Self::Exposure => "exposure",
        }
    }
}

/// **One grant, as the record's fields are read** — everything the line carries
/// apart from the instant, which is the writer's own.
///
/// **A run's lines are a slice of these, the served root first and the caller's
/// other exposures after it** ([`record_run`]), which is why the fields stay apart
/// per line rather than being folded into one run-wide value: `kind`,
/// `program_directory` and the two path spellings differ line by line, and only
/// `package` and `sid` are the run's own.
///
/// The two spellings of the path are both here on purpose: `root` is what the
/// caller exposed and what an operator recognises, `normalised` is that path's
/// normalisation ([`sid::normalised_root`], called by [`entry`] itself rather than
/// by its caller, so the record cannot name a normalisation the derivation does not
/// use) — which is the derivation's input on a [`PathKind::ServedRoot`] line and a
/// statement about the path alone on a [`PathKind::Exposure`] one, exactly as
/// `kind`'s own doc says.
pub(crate) struct Grant<'a> {
    /// **Which half of the run this line is about** — see [`PathKind`].
    pub(crate) kind: PathKind,
    /// The served root as the caller spelled it (`served_root`), or one more path
    /// the caller exposed, as the caller spelled that.
    pub(crate) root: &'a Path,
    /// The package name the run's profile was created under
    /// ([`crate::sid::package_name`]).
    pub(crate) package: &'a str,
    /// The package SID, in the `S-1-15-2-…` form every ACE of the run names.
    pub(crate) sid: &'a str,
    /// The program directory whose `ALL APPLICATION PACKAGES` grant is standing,
    /// when the program has one. `None` is the line's own `null`: no directory, no
    /// machine-wide grant.
    pub(crate) program_directory: Option<&'a Path>,
}

/// **A run's lines, appended in one transaction** — one line for each grant the
/// run made, the served root first.
///
/// **It takes the run's whole set rather than one grant, and that is its shape.**
/// The caller holds the set already: `grant_whitelist` has the root's path and the
/// caller's other exposures in hand, and building a [`Grant`] per path into one
/// slice is what lets the whole set share one lock, one descriptor read-back and
/// one handle instead of repeating all three per line (the module doc argues what
/// that costs the per-line shape, and `append_run` carries the order the three are
/// taken in).
///
/// **Where the call site sits is unchanged and is the point**: after every grant
/// of the launch has returned `Ok`, so the ledger never carries a root whose grant
/// failed, and after the phase's own `info!` report, so the duration that line
/// publishes does not absorb this file's I/O. A run that dies inside the phase
/// leaves the ACEs it wrote unrecorded, which is the direction to err in — a line
/// is a claim that a grant *happened*.
///
/// **It returns nothing and cannot fail**, which is the failure policy's whole
/// mechanism: a caller cannot `?` it, so a ledger that cannot be written can never
/// fail a launch, and the warning beside the failure is the only channel out. A
/// failure is now the *run's* rather than the line's — one open, one read-back and
/// one handle serve the set — and what that means for a partial append is at the
/// warning it is reported from.
pub(crate) fn record_run(grants: &[Grant<'_>]) {
    let Some(file) = ledger_file() else {
        tracing::warn!(
            step = "record the ACL grants",
            variable = "LOCALAPPDATA",
            "no state directory could be resolved, so this run's ACL grants were \
             not recorded; the standing ACEs are on the operator's directories \
             all the same (decision 15)"
        );
        return;
    };
    append_run_and_warn(&file, grants);
}

/// [`record_run`] against the file `file` names — **the seam a leg points at a
/// temporary ledger**, `Salt::load_at`'s shape in this module and for its reason:
/// the product path resolves the file itself, so a leg that wants a temporary one
/// would otherwise need a second product entry point.
///
/// It is the same body the product's call reaches: the failure policy, the
/// publish, the descriptor read-back and the appends are one implementation, so a
/// leg cannot pin a shape the launcher does not take.
#[cfg(test)]
pub(crate) fn record_run_at(file: &Path, grants: &[Grant<'_>]) {
    append_run_and_warn(file, grants);
}

/// One run's appends, with the failure policy's warning — **the one place the two
/// entry points above meet**, so a leg's file and the operator's file fail the same
/// way and say the same thing.
///
/// **The sentence is the run's and not the line's, and that is the honest
/// reading.** The transaction writes the lines in order, so a write that fails
/// after the first one leaves a prefix of the run's lines in the file: they stay —
/// this module appends and never rewrites — and the standing ACEs they name are
/// real, so what is reported as missing is the rest of the run rather than a line
/// that is not there. The per-line shape could fail between two lines in exactly
/// the same way; what changed is that the run is now the unit that fails, which is
/// also why this is one warning rather than one per path.
fn append_run_and_warn(file: &Path, grants: &[Grant<'_>]) {
    if let Err(error) = append_run(file, grants) {
        tracing::warn!(
            step = "record the ACL grants",
            path = %file.display(),
            error = %error,
            "this run's ACL grants were not recorded; the standing ACEs are on \
             the operator's directories all the same, and the audit surface is \
             missing this run (decision 15)"
        );
    }
}

/// The ledger this process writes, **or `None` when the state directory cannot
/// be resolved** (`%LOCALAPPDATA%` unset).
///
/// **It is public because it is the one spelling of the path, and the leg that
/// reads the file back needs that spelling.** The ledger is
/// `%LOCALAPPDATA%\tinio\acl-ledger.jsonl`, the state directory's file beside
/// the salt; a leg in another crate that spelled those names itself would keep
/// passing while the product renamed either half, and then fail on a later run
/// with a ledger that is "not there" — a rename reported as a missing file,
/// which is the one thing a reader cannot tell apart from a write that never
/// happened. What crosses with it is only the question "where does this process
/// write it": the record's fields, `record_run` and its test seam stay `pub(crate)`,
/// so a consumer can find the file and still not build a line.
///
/// **`None` rather than a path invented for the case**, for the reason the state
/// directory itself resolves to one: this file is an audit surface an operator
/// reads by hand, and a default would point that operator at a file no run of
/// this process wrote.
///
/// # Examples
///
/// ```
/// if let Some(path) = tinio_sandbox_windows::ledger::path() {
///     assert!(path.ends_with("acl-ledger.jsonl"), "{}", path.display());
/// }
/// ```
pub fn path() -> Option<PathBuf> {
    state_directory().map(|directory| directory.join(LEDGER_FILE))
}

/// The ledger's path, **or the path a leg pointed it at** — `None` when
/// `%LOCALAPPDATA%` is unset.
///
/// **Everything but the seam is [`path`]'s**, which is the spelling a consumer
/// outside the crate reads: a leg points the append at a temporary file, while
/// the product question is the one [`path`] answers. The join has one spelling
/// for both, so the file a run appends to and the file [`path`] names cannot
/// drift.
fn ledger_file() -> Option<PathBuf> {
    #[cfg(test)]
    if let Some(path) = sid::test_path::get() {
        return Some(path);
    }
    path()
}

/// **One run's lines, through one handle, under one lock** — the whole of the
/// transaction: the directory, the lock, the file (published if it is not there),
/// the descriptor read-back and the handle are each had once, and every line of the
/// run goes through what those produced.
///
/// **The directory is created here, and [`ensure_state_directory`] is idempotent**:
/// the ledger's directory is the salt's, and a ledger written by a run whose salt
/// could not be created is still a record worth having, so this call does not
/// depend on the salt's having succeeded. **A run with no lines takes none of
/// this**: the directory, the lock and the handle are there for lines to go into,
/// and a set that is empty would leave a file whose only content is the descriptor
/// the next run would have given it anyway.
///
/// **The lock is taken before the file is opened and held across every line**, so a
/// second run's lines cannot land between this run's bytes: the exclusion is keyed
/// on `file`'s own path, which is the one thing two appenders share, and it covers
/// the run's whole output rather than one line's — there is no moment between two of
/// this run's lines at which this run is outside its own critical section, which is
/// what "the exclusion covers every byte" now means.
///
/// **The descriptor is read back once, before the run's first appended byte**
/// ([`append_handle`], below the publish), which is the module doc's ordering and
/// what the plant check rests on: a file another account created at the ledger's
/// name is refused here rather than appended into, and a file an earlier revision
/// of this crate left under the directory's inherited grants is put right before
/// anything of this run's goes into it. The per-line shape made that statement
/// about every line after the first — the read-back was repeated per line because
/// each line had its own append — and one transaction makes it about the run's
/// whole output at once, which is strictly the stronger claim.
///
/// **The file is published whole if it is not there and appended to if it is**, and
/// the two halves are the module doc's: `publish` puts a private object under the
/// ledger's name with the run's first line already in it, and the lines after that
/// one are appended through the handle — including the case the link lost, where
/// nothing of this run's is in the file yet and the whole set goes through the
/// handle.
fn append_run(file: &Path, grants: &[Grant<'_>]) -> io::Result<()> {
    let Some((first, rest)) = grants.split_first() else {
        return Ok(());
    };
    if let Some(directory) = file.parent() {
        ensure_state_directory(directory)?;
    }
    let _lock = AclLock::acquire(file);
    // **The set the handle still owes lines to**: all of it, unless `publish` put
    // the first line into the file it created.
    let mut lines = grants;
    if !file.is_file() {
        match publish(file, entry(first)?.as_bytes()) {
            Ok(()) => lines = rest,
            // **The name was taken after all, so a peer published first** — its
            // file is complete by construction and holds the descriptor the create
            // call gave it, and this run's lines go into it below. The condition is
            // `file.is_file()` and not the error alone because the *temporary* is
            // the other thing a link can collide with: a leftover of that shape is
            // this run's loss (the error travels out and warns) and not a reason
            // to append into a file that is not there.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && file.is_file() => {}
            Err(error) => return Err(error),
        }
    }
    let mut handle = append_handle(file)?;
    for grant in lines {
        // **Each line is rendered as it is written rather than with the set**, so
        // the instant a line carries is the moment of its own append and two lines
        // of one run cannot carry the same time ([`entry`]'s rule).
        handle.write_all(entry(grant)?.as_bytes())?;
    }
    Ok(())
}

/// Write `bytes` into a private temporary **created current-user-only and only if
/// it is not there**, and publish that same object at `file` by linking it —
/// [`crate::sid::publish_private`], the salt's own primitive, and for its reason:
/// the ledger's name appears only once it holds its first line, and the descriptor
/// travels in the create call, so the file never exists under the state directory's
/// inherited ACL.
///
/// **The temporary's name is derived from the line being published**, the salt's
/// rule: no two publishing runs can name one file. A leftover temporary of a run
/// killed between the create and the removal is the one collision that rule cannot
/// prevent (the same line cannot be appended twice by the same run), and what it
/// costs is one line: the link answers `AlreadyExists`, [`append_run`] sees that
/// the ledger is not there either, and the error is warned about rather than the
/// line being appended into the temporary.
///
/// [`write_private`] is this module's own step inside that primitive, and the
/// reason the descriptor rule has a home here: the ledger's first line goes into a
/// file whose descriptor is read back before the name is published.
fn publish(file: &Path, bytes: &[u8]) -> io::Result<()> {
    sid::publish_private(file, bytes, "record the ACL grants", |temporary, file| {
        write_private(temporary, bytes).and_then(|()| fs::hard_link(temporary, file))
    })
}

/// `bytes` into a file the current user alone may read — **the descriptor is read
/// back before the name is published**, so a create-time descriptor that did not
/// take is repaired while the object still has no name an operator's tools would
/// look at, and the link that follows publishes a file that is known private
/// rather than one that is claimed to be.
fn write_private(temporary: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut handle = tinio_winsec::create_restricted(temporary)?;
    let written = handle.write_all(bytes);
    drop(handle);
    written?;
    ensure_private(temporary)
}

/// The one handle a run's lines go through — **the descriptor is read back and put
/// right first, and the two steps are one function so that no caller can open the
/// ledger without the read-back ahead of it**.
///
/// The handle is opened for append and holds no position of its own, so every line
/// goes to the end of the file as it is written; it lives for the run's whole set
/// and is closed by its `Drop` after the last line — one close where the per-line
/// shape had one per line.
fn append_handle(file: &Path) -> io::Result<fs::File> {
    ensure_private(file)?;
    fs::OpenOptions::new().append(true).open(file)
}

/// **Nothing is appended to a ledger that is not the current user's alone** — a
/// descriptor that is not is put back before the first byte of the run's lines, and
/// one that cannot be read or put back is an error, which leaves the run unrecorded
/// rather than publishing the package name and the SID into a file another account
/// can read.
///
/// **The file this is asked about is the one a plant can be**: an account the state
/// directory's descriptor reaches can create `acl-ledger.jsonl` before this crate's
/// first run, and every line appended to *that* file is a line published to its
/// creator — which is the exposure the module doc states, since the line carries
/// the name a `CreateAppContainerProfile` call turns into the SID the standing ACEs
/// name. The re-assertion is `tinio-api`'s `State::write` rule in this module's
/// shape (`crates/tinio-api/src/state.rs` asks `acl_is_current_user_only` of its
/// temporary and repairs it when the create-time descriptor did not take), and the
/// repair is the same call the create path uses.
///
/// **What it repairs is the file an earlier revision of this crate wrote.** The
/// ledger used to be created through a plain append open, so a file made before
/// 2026-09-28 carries the state directory's inherited grants — measured on this
/// host as `CodexSandboxUsers:(I)(M)` beside the salt's single current-user entry —
/// and the first run this revision makes is the moment that file is put back.
/// **What it does not reach is a reader that already has the file open**: a DACL
/// change does not revoke access a handle was granted at open time, so an account
/// that planted the file and kept a read handle keeps reading what this run writes.
/// **The repair is what makes everything after it private, and with one transaction
/// per run that is the run's whole output**: the read-back precedes the first byte
/// `record_run`'s transaction appends, so no line of this run goes into a
/// descriptor that was not this user's alone when the run's handle was opened —
/// where the per-line shape could only say it of the lines after the one that
/// repaired the file. The plant itself is answered by not adopting the file's
/// *name* — which is what the salt does, and which the ledger cannot, its whole
/// shape being one file appended to across runs.
fn ensure_private(file: &Path) -> io::Result<()> {
    if tinio_winsec::acl_is_current_user_only(file)? {
        return Ok(());
    }
    tinio_winsec::restrict_to_current_user(file)?;
    tracing::warn!(
        step = "record the ACL grants",
        path = %file.display(),
        "the ledger was not this user's alone, so it was put back to the current \
         user before this run's lines were written: an earlier revision of this \
         crate created it under the state directory's inherited grants, or another \
         account created it first (design §6)"
    );
    Ok(())
}

/// **The line as serde writes it, borrowed throughout** — a render of the grant
/// itself rather than a value tree whose strings are copied again on the way out,
/// so the only text built is the instant and the two path spellings, which have to
/// be rendered for a reader anyway.
///
/// **The field names are the line's keys and their order is the line's key order,
/// and that is the record's format**: an operator reads an older run's lines beside
/// a new one's, so nothing here is renamed and nothing is reordered. Pinning it in
/// a declaration is also what a `json!` object could not do — that one renders in
/// `serde_json`'s map order, which is sorted unless another crate in the build
/// turns `preserve_order` on.
#[derive(Serialize)]
struct Record<'a> {
    at: &'a str,
    kind: &'a str,
    root: &'a str,
    normalised: &'a str,
    package: &'a str,
    sid: &'a str,
    program_directory: Option<&'a str>,
}

/// The record's line, newline included.
///
/// **The instant is formatted here rather than by the caller**, so a line always
/// carries the moment of *its own* append: a caller that stamped a batch would give
/// two lines one time. [`append_run`] keeps that true for a run's set by rendering
/// each line as it writes it rather than rendering the set before the first byte.
fn entry(grant: &Grant<'_>) -> io::Result<String> {
    let at = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(io::Error::other)?;
    let root = grant.root.display().to_string();
    let normalised = sid::normalised_root(grant.root).display().to_string();
    let program_directory = grant
        .program_directory
        .map(|directory| directory.display().to_string());
    let mut line = serde_json::to_string(&Record {
        at: &at,
        kind: grant.kind.as_str(),
        root: &root,
        normalised: &normalised,
        package: grant.package,
        sid: grant.sid,
        program_directory: program_directory.as_deref(),
    })
    .map_err(io::Error::other)?;
    line.push('\n');
    Ok(line)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        thread,
        time::Duration,
    };

    use serde_json::Value;
    use tinio_util::testing::{assert_recorded_at, assert_step};

    use super::*;
    use crate::{
        acl::grant_everyone_read,
        sid::test_path,
        testing::{capture, recorder},
    };

    /// A grant with a fixed root and SID, so a leg asserts the record's shape
    /// rather than re-deriving the values it wrote.
    fn grant(program_directory: Option<&Path>) -> Grant<'_> {
        Grant {
            kind: PathKind::ServedRoot,
            root: Path::new(r"E:\srv\tinio"),
            package: "tinio.sandbox.0123456789abcdef",
            sid: "S-1-15-2-1-2-3-4-5-6-7",
            program_directory,
        }
    }

    /// The other half of a run's lines: one more path the caller exposed, which the
    /// same package SID holds a standing ACE on.
    fn exposure(path: &Path) -> Grant<'_> {
        Grant {
            kind: PathKind::Exposure,
            root: path,
            program_directory: None,
            ..grant(None)
        }
    }

    /// The lines a ledger holds, each parsed — the reader an operator's `jq` would
    /// be, spelled here so a leg fails on a line that does not parse.
    fn lines(file: &Path) -> Vec<Value> {
        let text = fs::read_to_string(file).expect("read the ledger");
        text.lines()
            .map(|line| serde_json::from_str(line).expect("every line is one JSON object"))
            .collect()
    }

    /// **A recorded grant is one JSON object with the fields the item asked for,
    /// and it reads back as such.**
    ///
    /// It would fail without [`entry`]: an append that wrote a sentence, two lines
    /// or an object without the SID would leave an operator unable to map a
    /// standing ACE back to the root it was granted for, which is the whole purpose
    /// of the file. The normalised spelling is asserted against the crate's own
    /// derivation rather than against a second literal, so the two cannot drift.
    ///
    /// **Both of this run's lines go in through one call**, which is [`record_run`]'s
    /// shape: the served root and the one more path the caller exposed travel in one
    /// slice, and the two objects that land are the run's one transaction — so a
    /// shape that wrote the first and dropped the rest of the set fails here too.
    #[test]
    fn a_recorded_grant_reads_back_as_one_object() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");
        let program = Path::new(r"E:\build\debug");
        let state = Path::new(r"E:\srv\state");

        test_path::with(&file, || {
            record_run(&[grant(Some(program)), exposure(state)])
        });

        let entries = lines(&file);
        assert_eq!(entries.len(), 2, "one grant per line: {entries:?}");
        let entry = &entries[0];
        assert_eq!(entry["root"], r"E:\srv\tinio");
        assert_eq!(
            entry["kind"], "served-root",
            "the line says which half of the run it is about"
        );
        assert_eq!(
            entry["normalised"],
            sid::normalised_root(Path::new(r"E:\srv\tinio"))
                .display()
                .to_string(),
            "the record names the normalisation the derivation reads"
        );
        assert_eq!(entry["package"], "tinio.sandbox.0123456789abcdef");
        assert_eq!(entry["sid"], "S-1-15-2-1-2-3-4-5-6-7");
        assert_eq!(entry["program_directory"], r"E:\build\debug");
        let at = entry["at"].as_str().expect("the instant is a string");
        assert!(
            at.ends_with('Z') && at.contains('T') && at.len() >= 20,
            "the instant is RFC 3339 in UTC: {at}"
        );
        // And the file ends in a newline, which is what makes a half-written line
        // detectable at all.
        let text = fs::read_to_string(&file).expect("read the ledger");
        assert!(text.ends_with("}\n"), "one line, terminated: {text:?}");

        // **And the run's second path is a line of its own, told apart by `kind`** —
        // the field's whole purpose, since the package name and the SID are the
        // run's on both lines and only this one says the path is not what the name
        // was derived from (finding 3 of the 2026-09-28 review).
        let extra = &entries[1];
        assert_eq!(extra["kind"], "exposure");
        assert_eq!(extra["root"], r"E:\srv\state");
        assert_eq!(
            extra["sid"], "S-1-15-2-1-2-3-4-5-6-7",
            "the extra path is granted the run's own package SID"
        );
        assert!(
            extra["program_directory"].is_null(),
            "the program directory is a property of the root's line: {extra:?}"
        );
        // The normalisation is still of the path the line names, which is the
        // statement `kind` qualifies: on this line it is not the derivation's
        // input.
        assert_eq!(
            extra["normalised"],
            sid::normalised_root(state).display().to_string()
        );
    }

    /// **A programme directory the run did not have is `null`, not a second
    /// spelling of "nothing"** — a reader that asks `program_directory` gets an
    /// answer for every line.
    #[test]
    fn a_grant_without_a_program_directory_records_null() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");

        test_path::with(&file, || record_run(&[grant(None)]));

        let entries = lines(&file);
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert!(entries[0]["program_directory"].is_null());
    }

    /// **A run with no lines writes nothing at all** — no file, no lock, and no
    /// warning, which is [`append_run`]'s guard against the empty slice.
    ///
    /// It would fail without the guard by warning about a ledger it had nothing to
    /// put in it, or by publishing a file whose only content is the descriptor the
    /// next run would have given it anyway — the audit surface's every line is a
    /// statement about a grant, and an empty set has none to make.
    #[test]
    fn a_run_with_no_lines_writes_nothing() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");

        let capture = capture();
        let recording = capture.record();
        test_path::with(&file, || record_run(&[]));
        let logged = capture.text();
        drop(recording);

        assert!(!file.exists(), "an empty run created a ledger: {file:?}");
        assert!(
            !logged.contains("were not recorded"),
            "an empty run warned about a run with nothing to record:\n{logged}"
        );
    }

    /// **A waiting run's whole set lands after the lock, and none of it before.**
    ///
    /// The window this test opens is the writer's critical section: the lock is
    /// held by this thread, so the spawned run must not touch the file until it is
    /// released — and after it is, its lines are whole and parse. **The run holds
    /// two grants on purpose**: one transaction covers the run's every line, so a
    /// shape that took the lock per line and dropped it between them would let the
    /// first line land here, which is the interleaving two runs appending to one
    /// ledger would produce.
    #[test]
    fn a_second_append_waits_for_the_lock_the_writer_holds() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");

        let held = AclLock::acquire(&file).expect("the lock file is creatable");
        let appending = thread::spawn({
            let file = file.clone();
            move || {
                test_path::with(&file, || {
                    record_run(&[grant(None), exposure(Path::new(r"E:\srv\state"))])
                })
            }
        });
        // Long enough that a writer with no lock would have written, and short
        // enough to keep the suite quick.
        thread::sleep(Duration::from_millis(200));
        assert!(
            !file.exists() || fs::metadata(&file).expect("metadata").len() == 0,
            "a line landed while the lock was held, so two runs could interleave \
             their lines"
        );
        drop(held);
        appending
            .join()
            .expect("the appending thread did not panic");

        let entries = lines(&file);
        assert_eq!(
            entries.len(),
            2,
            "the waiting run's lines landed: {entries:?}"
        );
    }

    /// **A ledger that cannot be written warns once and does not fail the run.**
    ///
    /// The injection is a path a file stands in the way of, so the directory create
    /// itself fails — the earliest of the calls the run's transaction makes, and one
    /// no permission on the file alone would reach. What the leg asserts is the
    /// halves of the policy: the warning is emitted at `WARN` with the ledger's own
    /// step, [`record_run`] returns at all, which it can only do because it has no
    /// failure to propagate — a grant site cannot turn this into a launch error —
    /// and **the run is the unit that fails**: two grants are handed in and one
    /// warning comes out, where the per-line shape emitted one per line.
    #[test]
    fn a_ledger_that_cannot_be_written_warns_and_the_run_continues() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let blocker = directory.path().join("a-file");
        fs::write(&blocker, b"not a directory").expect("seed the blocker");
        let file = blocker.join("acl-ledger.jsonl");

        let capture = capture();
        let recording = capture.record();
        test_path::with(&file, || {
            record_run(&[grant(None), exposure(Path::new(r"E:\srv\state"))])
        });
        let logged = capture.text();
        drop(recording);

        assert_step(&logged, "record the ACL grants");
        assert_recorded_at(
            &logged,
            "this run's ACL grants were not recorded",
            tracing::Level::WARN,
        );
        assert_eq!(
            logged.matches("were not recorded").count(),
            1,
            "a run's lines fail as one transaction and are warned about once:\n{logged}"
        );
        assert!(
            !file.exists(),
            "the append reached a path behind a file after all, so this leg would \
             prove nothing"
        );
    }

    /// **The line is written under the file's own lock, not a served root's** — the
    /// key is the ledger's path, so two runs on *different* roots appending to one
    /// ledger exclude each other, which is the shape the hazard has.
    ///
    /// It would fail without [`append_run`]'s lock: a second run's grant of another
    /// root is the writer this file knows nothing else about, and a key that changed
    /// with the root would serialise the wrong pair.
    #[test]
    fn the_lock_is_keyed_on_the_ledger_and_not_on_a_served_root() {
        let directory = tempfile::tempdir().expect("temp dir");
        let file: PathBuf = directory.path().join("acl-ledger.jsonl");
        let held = AclLock::acquire(&file).expect("the lock file is creatable");
        let mut other = Grant {
            root: Path::new(r"E:\other"),
            ..grant(None)
        };
        other.package = "tinio.sandbox.fedcba9876543210";

        let appending = thread::spawn({
            let file = file.clone();
            move || test_path::with(&file, || record_run(&[other]))
        });
        thread::sleep(Duration::from_millis(200));
        assert!(
            !file.exists() || fs::metadata(&file).expect("metadata").len() == 0,
            "a second root's run did not take the ledger's own lock"
        );
        drop(held);
        appending
            .join()
            .expect("the appending thread did not panic");
        assert_eq!(lines(&file).len(), 1);
    }

    /// **The quiet half of the failure policy: an append that lands says nothing.**
    ///
    /// It would fail without the success path's silence — a warning emitted on the
    /// write as well as on the failure would put "the audit surface is missing this
    /// run" in the log of every run that has one, which is the kind of line an
    /// operator learns to skip. The recording is the writing thread's own
    /// ([`crate::testing::Capture::record`]), so a test running beside this one is
    /// not asserting on this test's lines.
    #[test]
    fn an_append_that_lands_warns_about_nothing() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");

        let capture = capture();
        let recording = capture.record();
        test_path::with(&file, || record_run(&[grant(None)]));
        let logged = capture.text();
        drop(recording);

        assert!(
            !logged.contains("were not recorded"),
            "the append warned about a ledger it could write:\n{logged}"
        );
        assert!(file.exists(), "the append wrote the ledger");
    }

    /// **A ledger that is not the current user's alone is put back to the current
    /// user before the run's lines are written.**
    ///
    /// The file this leg plants is what an account the state directory's descriptor
    /// reaches can create at the ledger's name before this crate's first run, and
    /// what an earlier revision of this crate left behind on this host: one whose
    /// descriptor is the directory's inherited grants rather than the private one
    /// [`publish`] makes. It would fail without [`ensure_private`]: the run would
    /// append the package name and the SID into a file another trustee can read —
    /// the exposure the module doc states, since that line is what a
    /// `CreateAppContainerProfile` call turns back into the SID every standing ACE
    /// names. The trustee is granted through `icacls` rather than left to whatever
    /// this machine's `%TEMP%` passes down, so the leg is about the repair on any
    /// host.
    #[test]
    fn an_append_puts_a_ledger_that_is_not_the_current_users_alone_back() {
        let _serial = recorder();
        let directory = tempfile::tempdir().expect("temp dir");
        let file = directory.path().join("acl-ledger.jsonl");
        fs::write(&file, b"{\"at\":\"a line this crate did not write\"}\n")
            .expect("plant a ledger");
        grant_everyone_read(&file);
        assert!(
            !tinio_winsec::acl_is_current_user_only(&file).expect("read the DACL back"),
            "the planted ledger is already this user's alone, so this leg would prove \
             nothing"
        );

        let capture = capture();
        let recording = capture.record();
        test_path::with(&file, || record_run(&[grant(None)]));
        let logged = capture.text();
        drop(recording);

        assert!(
            tinio_winsec::acl_is_current_user_only(&file).expect("read the DACL back"),
            "the append left the ledger readable by the trustee it was planted with"
        );
        assert_step(&logged, "record the ACL grants");
        assert_recorded_at(
            &logged,
            "was put back to the current user",
            tracing::Level::WARN,
        );
        let entries = lines(&file);
        assert_eq!(
            entries.len(),
            2,
            "the planted line, then this run's: {entries:?}"
        );
        assert_eq!(entries[1]["sid"], "S-1-15-2-1-2-3-4-5-6-7");
    }
}
