//! **The whitelist's table: the target a grant addresses, the mask and the flags a grant
//! is written with, and the two spellings of that one pair** — `ace_spec`'s string, which is
//! the tool's, and `ace_bits`'s numbers, which are `SetSecurityInfo`'s.
//!
//! No object is opened and no descriptor is read here, which is why this module carries no
//! `unsafe`: what it wants to know about an ACE is [`access_matches`], the table asked of an
//! entry the walk hands over.

use windows_sys::Win32::{
    Security::{
        ACCESS_ALLOWED_ACE, CONTAINER_INHERIT_ACE, INHERIT_ONLY_ACE, NO_INHERITANCE,
        OBJECT_INHERIT_ACE,
    },
    Storage::FileSystem::{FILE_GENERIC_EXECUTE, FILE_GENERIC_READ, FILE_GENERIC_WRITE},
};

use crate::_core::AccessMode;

/// `DELETE` (`0x0001_0000`), which `icacls` folds into `(M)` and not into `(RX)`;
/// `windows-sys` exports it as a generic-mapping bit only.
const DELETE: u32 = 0x0001_0000;

/// `icacls`'s `(M)` — the alias [`ace_spec`] spells and the mask
/// [`access_matches`] compares: `FILE_GENERIC_READ | FILE_GENERIC_WRITE |
/// FILE_GENERIC_EXECUTE | DELETE` (`0x0013_01BF`).
const MODIFY: u32 = FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE | DELETE;

/// `icacls`'s `(RX)` — the alias [`ace_spec`] spells and the mask
/// [`access_matches`] compares: the read and execute halves (`0x0012_00A9`).
const READ_AND_EXECUTE: u32 = FILE_GENERIC_READ | FILE_GENERIC_EXECUTE;

/// The shape a grant takes.
///
/// **The first variant is the ordinary one, and that is a correction rather than
/// an ordering.** This crate shipped a `Directory`/`File` pair whose `Directory`
/// meant "a directory, non-inherited", so every pre-existing call site that spelled
/// `Target::Directory` silently changed meaning when decision 22's entry stopped
/// propagating — and a confined child then could not read a pre-existing file under
/// an exposed directory. The rule the variants now encode is: **the obvious
/// spelling is the caller's**, and the exception is named after the one decision
/// that makes it.
///
/// **The two dimensions behind it are independent.** *What the path is* decides
/// whether inheritance is meaningful at all: `(OI)(CI)` is meaningful on a
/// directory only, so an inheritance-flagged grant on a **file** is a **silent
/// no-op** when `icacls` writes it (exit 0, nothing written) and an ACE nothing
/// inherits from when the API does, while the file shape sent to a directory
/// leaves every descendant ungranted. *Whether it propagates* is a policy:
/// a caller's exposed directory must reach the descendants it already holds (design
/// §6's whitelist, one ACE per exposed path instead of a `/t` walk over
/// every file in the tree), and decision 22's program-directory entry deliberately
/// does not.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, ace_spec};
///
/// // The ordinary directory: the caller's exposure, which propagates.
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadWrite),
///     "(OI)(CI)M"
/// );
/// // The exception, and the plain file shape.
/// assert_eq!(
///     ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
///     "RX"
/// );
/// assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// **An exposed directory, and the ordinary case**: the caller exposed this
    /// path, so the ACE carries `(OI)(CI)` and reaches the descendants the
    /// directory already holds (design §6's whitelist). A caller with a directory
    /// in hand means this one.
    Directory,
    /// **A file**: it takes the explicit non-inherited form and nothing else,
    /// because the flags are a no-op on one. This variant is passed by its two
    /// callers rather than read off the filesystem to pick it — `backend::confine`
    /// filters the file-shaped entries out before asking `shape_for`, and
    /// [`Grants::apply_siblings`](super::grant::Grants::apply_siblings) passes the literal for each enumerated `*.dll`.
    File,
    /// **The program's own directory, and decision 22's exception**: a directory
    /// whose ACE deliberately stops at it, because what the confined child may
    /// reach there is the files its loader reads rather than everything a build
    /// directory holds (Ruling 78).
    ProgramDirectory,
}

/// The permission specification `icacls` grants for a target and an access mode.
///
/// `M` is `FILE_GENERIC_READ | FILE_GENERIC_WRITE | FILE_GENERIC_EXECUTE |
/// DELETE` (`0x001301BF`) and `RX` is the read and execute halves
/// (`0x001200A9`). `(OI)(CI)` means object-and-container inherit, **is meaningful
/// on a directory only**, and is what carries an exposed directory's ACE to the
/// descendants that were already there — and what makes a grant on a directory
/// cost a pass over the whole tree (design §6 and its Risks row).
///
/// [`Target::Directory`] is therefore the only variant that carries the flags: a
/// file cannot take them (the no-op above) and the program's directory must not
/// (decision 22).
///
/// **`Target::File` and `Target::ProgramDirectory` come out of this function
/// byte-identical, and that is a coincidence rather than one shape with two
/// names.** Both are non-inherited, so both are `RX`/`M`; what separates them is a
/// *policy* — the program directory is decision 22's "this ACE stops at the
/// directory" — which a file cannot carry and which a directory must not lose
/// silently. The variants are therefore deliberately **not** merged: the type is
/// the only thing that keeps a call site from handing a file the directory-shaped
/// intent or the other way round, and `(OI)(CI)` is the single token in this table
/// that distinguishes a shape at all — it is what separates [`Target::Directory`]
/// from both of the others.
///
/// **The string and the numbers a grant is actually written with are one table
/// read twice**: `ace_bits` is the mask and the inheritance flags
/// `SetSecurityInfo` takes, this is the tool's spelling of the same pair, and
/// `the_aliases_icacls_writes_are_the_masks_this_read_compares` pins the two
/// against each other on a real DACL.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, ace_spec};
///
/// // The caller's exposed directories propagate to what they already hold.
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadOnly),
///     "(OI)(CI)RX"
/// );
/// assert_eq!(
///     ace_spec(Target::Directory, AccessMode::ReadWrite),
///     "(OI)(CI)M"
/// );
/// // The program's directory and every file do not.
/// assert_eq!(
///     ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
///     "RX"
/// );
/// assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
/// ```
#[cfg(any(test, feature = "testing"))]
pub const fn ace_spec(target: Target, access: AccessMode) -> &'static str {
    match (target, access) {
        (Target::Directory, AccessMode::ReadOnly) => "(OI)(CI)RX",
        (Target::Directory, AccessMode::ReadWrite) => "(OI)(CI)M",
        (Target::ProgramDirectory | Target::File, AccessMode::ReadOnly) => "RX",
        (Target::ProgramDirectory | Target::File, AccessMode::ReadWrite) => "M",
    }
}

/// The same table as [`ace_spec`], in the two numbers the write takes: **the
/// access mask and the inheritance flags an `EXPLICIT_ACCESS_W` carries**, which
/// is what `grfAccessPermissions` and `grfInheritance` are set from.
///
/// **The numbers are the aliases' own, not a second opinion about them**: `M` is
/// `MODIFY` (`0x0013_01BF`) and `RX` is `READ_AND_EXECUTE` (`0x0012_00A9`), the
/// two constants the read compares against, and the flags are
/// `OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE` for the one shape that
/// propagates — `NO_INHERITANCE` for the two that do not, which is why a file
/// grant never carries `(OI)(CI)` even though passing it would compile.
///
/// **The pair is `const` and private, and the table is small on purpose**: it is
/// the whole of what `SET_ACCESS` is told, and every other part of the ACE — the
/// SID, the type, the order the entry lands in — is the kernel's
/// (`SetEntriesInAclW` inserts a new allow ACE ahead of the existing allows, which
/// is the order `icacls` produced and the reason its alias list reads the same).
pub(super) const fn ace_bits(target: Target, access: AccessMode) -> (u32, u32) {
    let mask = match access {
        AccessMode::ReadOnly => READ_AND_EXECUTE,
        AccessMode::ReadWrite => MODIFY,
    };
    let inheritance = match target {
        Target::Directory => OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
        Target::ProgramDirectory | Target::File => NO_INHERITANCE,
    };
    (mask, inheritance)
}

/// The `*<sid>:<perm>` argument `icacls` is given for `target`.
///
/// The leading `*` is what stops the tool resolving the string as an account
/// name: an AppContainer package SID has no account behind it, and a name form
/// would be looked up in the local accounts and fail — or, worse, be answered by
/// an account that happens to carry the name.
///
/// **The launcher no longer hands this to the tool — the tool is what an
/// operator, a fixture and [`revoke`](super::grant::revoke) still use** — so what the function is for
/// now is that one table describes both writers: `ace_bits` is what
/// `SetSecurityInfo` is told and this is the same pair in the tool's own spelling,
/// which is what this module's own tests and `tinio-server`'s Windows fixtures
/// need to write an ACE the launcher's read will recognize.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_windows::acl::{Target, grant_spec};
///
/// assert_eq!(
///     grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
///     "*S-1-15-2-1-2-3:(OI)(CI)RX"
/// );
/// assert_eq!(
///     grant_spec("S-1-15-2-1-2-3", Target::File, AccessMode::ReadWrite),
///     "*S-1-15-2-1-2-3:M"
/// );
/// ```
#[cfg(any(test, feature = "testing"))]
pub fn grant_spec(sid: &str, target: Target, access: AccessMode) -> String {
    format!("*{sid}:{}", ace_spec(target, access))
}

/// Whether `ace` — an access-allowed entry, as [`walk_dacl`] hands one over, so
/// the type test that makes this structure its layout has already been made —
/// carries what a grant for `target`/`access` would write.
///
/// **It asks [`ace_bits`], the one table the write is made from**, which is what
/// keeps the read and the write from disagreeing: the *rule* differs — the write
/// sets exactly those bits, the read accepts any entry that **contains** them —
/// and that difference is this function's, not a second copy of the numbers. A
/// DACL may carry `R`, `F`, a wider mask or one written by another tool, and every
/// one of those that includes the request satisfies it; an equality would report a
/// path that already has more than enough as unsatisfied and rewrite its
/// descriptor, which is the cost this read exists to avoid.
///
/// **The masks' provenance is the table's** (`icacls`'s own aliases, derived from
/// the generic rights rather than transcribed) and so is the flags': a
/// `Target::Directory` entry must carry `(OI)(CI)`, because an inherited grant that
/// lost its flags reaches no descendant and is not the grant the caller asked for,
/// while the two non-propagating shapes ask for no flags at all — at least the
/// requested flags, and therefore any flags when none are requested.
///
/// **`F` needs no branch of its own**: what `icacls` stores for `(F)` is
/// `FILE_ALL_ACCESS` (`0x001F_01FF`), which carries every bit both masks have, so
/// the same containment test answers it — and the generic bit is not a shortcut to
/// that: an ACE whose mask is `GENERIC_ALL` and nothing else grants no specific
/// right at all, which is the false skip this test used to take.
/// **An `INHERIT_ONLY` ACE fails every target**: such an entry reaches nothing on
/// this object, so it is not the grant a caller asked for however wide its mask.
pub(super) fn access_matches(ace: &ACCESS_ALLOWED_ACE, access: AccessMode, target: Target) -> bool {
    let flags = u32::from(ace.Header.AceFlags);
    // An inherit-only ACE is not effective on this object at all, so it cannot
    // satisfy a grant whatever its mask or flags say.
    if flags & INHERIT_ONLY_ACE != 0 {
        return false;
    }
    // **`F` is not a branch of its own, and the branch that stood here was
    // wrong.** `icacls`'s `(F)` stores `FILE_ALL_ACCESS` (`0x001F_01FF`), which
    // contains every bit the table's masks are built from, so the containment
    // below answers it; the `GENERIC_ALL` disjunct that used to sit beside it
    // fired only for a mask literally carrying bit 28 — which an access check
    // expands into *no specific right at all* — and a skip that fired there left
    // a confined child without the access it was launched for (`(F)`'s own
    // measurement, and the false skip it pins, are in `a_mask_carrying_only_the_
    // generic_bit_does_not_satisfy_a_grant`).
    let (mask, wanted) = ace_bits(target, access);
    let mask_ok = ace.Mask & mask == mask;
    let flags_ok = wanted == NO_INHERITANCE || flags & wanted == wanted;
    mask_ok && flags_ok
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use windows_sys::Win32::{
        Foundation::GENERIC_ALL, Security::ACE_HEADER, Storage::FileSystem::FILE_ALL_ACCESS,
    };

    use super::*;
    use crate::acl::walk::ACCESS_ALLOWED_ACE_TYPE;

    /// **The two renderings of the one table agree**: [`ace_spec`]'s string is the
    /// tool's spelling of the pair [`ace_bits`] writes, and this walks all six
    /// shapes rather than the two a caller happens to use.
    ///
    /// The string is read structurally — the `(OI)(CI)` token iff the shape
    /// propagates, and the mask's own alias — because a comparison of two
    /// hand-written tables would be a third copy of them. What can still drift is
    /// only the *spelling*, and that is what
    /// `the_aliases_icacls_writes_are_the_masks_this_read_compares` measures against
    /// the tool.
    #[test]
    fn the_spec_string_and_the_bits_are_one_table() {
        for target in [Target::Directory, Target::ProgramDirectory, Target::File] {
            for access in [AccessMode::ReadOnly, AccessMode::ReadWrite] {
                let (mask, flags) = ace_bits(target, access);
                let spec = ace_spec(target, access);
                assert_eq!(
                    spec.contains("(OI)(CI)"),
                    flags != NO_INHERITANCE,
                    "{target:?}/{access:?}: the string and the flags disagree"
                );
                assert_eq!(
                    spec.ends_with("RX"),
                    mask == READ_AND_EXECUTE,
                    "{target:?}/{access:?}: the string and the mask disagree"
                );
                assert_eq!(
                    spec.ends_with('M'),
                    mask == MODIFY,
                    "{target:?}/{access:?}: the string and the mask disagree"
                );
            }
        }
    }

    #[test]
    fn only_an_exposed_directory_propagates_its_ace() {
        // Three shapes, and the two dimensions are why: the caller's exposed
        // directories keep `(OI)(CI)` — design §6's whitelist, and the reason a
        // pre-existing descendant is reachable without a `/t` walk — while the
        // program's directory (decision 22) and every file take the plain form. An
        // inheritance-flagged grant on a file is a silent no-op (design §6's
        // evidence row), which `the_file_shaped_spec_is_what_lands_on_a_file` pins.
        assert_eq!(
            ace_spec(Target::Directory, AccessMode::ReadOnly),
            "(OI)(CI)RX"
        );
        assert_eq!(
            ace_spec(Target::Directory, AccessMode::ReadWrite),
            "(OI)(CI)M"
        );
        assert_eq!(
            ace_spec(Target::ProgramDirectory, AccessMode::ReadOnly),
            "RX"
        );
        assert_eq!(
            ace_spec(Target::ProgramDirectory, AccessMode::ReadWrite),
            "M"
        );
        assert_eq!(ace_spec(Target::File, AccessMode::ReadOnly), "RX");
        assert_eq!(ace_spec(Target::File, AccessMode::ReadWrite), "M");
    }

    #[test]
    fn the_grant_argument_names_the_sid_literally() {
        // The `*` prefix is what stops icacls resolving the SID as a name, and the
        // spec it prefixes is the target's.
        assert_eq!(
            grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
            "*S-1-15-2-1-2-3:(OI)(CI)RX"
        );
        assert_eq!(
            grant_spec(
                "S-1-15-2-1-2-3",
                Target::ProgramDirectory,
                AccessMode::ReadOnly
            ),
            "*S-1-15-2-1-2-3:RX"
        );
        assert_eq!(
            grant_spec("S-1-15-2-1-2-3", Target::File, AccessMode::ReadWrite),
            "*S-1-15-2-1-2-3:M"
        );
    }

    /// **`GENERIC_ALL` on its own is not the container**, and the disjunct that
    /// treated it as one was a false skip: bit 28 is a *request* bit that an
    /// access check expands into nothing, so an ACE whose mask carries only it
    /// grants no specific right to anyone. Every ACE `icacls` writes carries the
    /// specific bits as well — `(M)` is `0x0013_01BF` and `(RX)` is `0x0012_00A9`
    /// — which is why the disjunct was redundant where it was not wrong, and why
    /// removing it costs the skip nothing.
    #[test]
    fn a_mask_carrying_only_the_generic_bit_does_not_satisfy_a_grant() {
        // The shape the walk hands a predicate: a header, a mask, the SID.
        let aced = |flags: u8, mask: u32| ACCESS_ALLOWED_ACE {
            Header: ACE_HEADER {
                AceType: ACCESS_ALLOWED_ACE_TYPE,
                AceFlags: flags,
                AceSize: 0,
            },
            Mask: mask,
            SidStart: 0,
        };
        let both = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        for access in [AccessMode::ReadOnly, AccessMode::ReadWrite] {
            assert!(
                !access_matches(&aced(both, GENERIC_ALL), access, Target::Directory),
                "`GENERIC_ALL` alone carries no specific right, so it satisfies \
                 no grant however it is asked ({access:?})"
            );
        }
        // The control: the mask the generic bit *expands to* is the container,
        // and containment answers it for both accesses.
        assert!(access_matches(
            &aced(both, FILE_ALL_ACCESS),
            AccessMode::ReadOnly,
            Target::Directory
        ));
        assert!(access_matches(
            &aced(both, FILE_ALL_ACCESS),
            AccessMode::ReadWrite,
            Target::Directory
        ));
    }

    /// **The orphan shape**: an ACE that carries `(OI)(CI)` and `INHERIT_ONLY` at
    /// once reaches the children and **not** the object it sits on, so it is not
    /// the grant a directory's own entry is — the reason `icacls`'s listing writes
    /// it `(IO)` beside the flags.
    ///
    /// The mask and the flags this tests are built rather than read, because the
    /// filesystem is what removes `(IO)` on the way down: a child's inherited copy
    /// is effective there, so a real DACL cannot hold the combination on the
    /// directory the question is asked about.
    #[test]
    fn an_inherit_only_entry_is_not_the_grant_it_resembles() {
        let ace = |flags: u8| ACCESS_ALLOWED_ACE {
            Header: ACE_HEADER {
                AceType: ACCESS_ALLOWED_ACE_TYPE,
                AceFlags: flags,
                AceSize: size_of::<ACCESS_ALLOWED_ACE>() as u16,
            },
            Mask: FILE_GENERIC_READ | FILE_GENERIC_EXECUTE,
            SidStart: 0,
        };
        let both = (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8;
        assert!(
            access_matches(&ace(both), AccessMode::ReadOnly, Target::Directory),
            "`(OI)(CI)RX` on a directory is the grant"
        );
        assert!(
            !access_matches(
                &ace(both | INHERIT_ONLY_ACE as u8),
                AccessMode::ReadOnly,
                Target::Directory
            ),
            "the same ACE with `(IO)` is not effective on this object, so it is not its grant"
        );
        assert!(
            !access_matches(&ace(0), AccessMode::ReadOnly, Target::Directory),
            "and an effective entry without the flags does not reach the descendants"
        );
        assert!(
            access_matches(&ace(0), AccessMode::ReadOnly, Target::File),
            "the file-shaped question asks for no flags"
        );
    }
}
