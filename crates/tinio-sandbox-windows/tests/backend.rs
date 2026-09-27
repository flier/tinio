//! The backend's exposure set against a real directory: decision 22's rule as
//! Ruling 78 reshaped it — the program's directory read-only, the `*.dll` files
//! directly beside the program read-only, and nothing else.
//!
//! **Nothing here launches a process or edits a DACL.** The rule is what this
//! suite is about, and every assertion is over the vector `backend::exposures`
//! returns — resolved through `canonical_exposures` where the spelling matters —
//! which is the one place the program-directory entry is produced. The tests that
//! never touch the disk use paths that do not exist, so they assert the shape of
//! the rule rather than the presence of a tree. **The shape decision itself is
//! `backend`'s own and is pinned in `src/backend/tests.rs`, where the helper that
//! answers it is `pub(super)` and unreachable from here.**

#![cfg(target_os = "windows")]

use std::{fs, path::Path};

use tinio_sandbox_core::{AccessMode, Sandbox};
use tinio_sandbox_windows::{
    acl::{self, Target},
    backend::{self, canonical_exposures},
};

/// Whether `path` names `leaf` directly under `dir`.
///
/// The directory is compared by equality and the leaf by name, because the two
/// halves of the assertion come from different places: the directory is the one
/// `exposures` produces, while the leaf is a spelling the rule adds.
fn is_the_file(dir: &Path, leaf: &str, path: &Path) -> bool {
    path.parent() == Some(dir) && path.file_name().and_then(|name| name.to_str()) == Some(leaf)
}

/// Whether `path`'s final component is `leaf` — the comparison the tests that
/// never touch the disk use, because `exposures` does not resolve anything and
/// their paths are spelled rather than resolved.
fn ends_with(path: &Path, leaf: &str) -> bool {
    path.file_name().and_then(|name| name.to_str()) == Some(leaf)
}

/// **Decision 22's rule, as Ruling 78 reshaped it**: the program's own directory
/// read-only, **and** the `*.dll` files directly beside the program read-only —
/// not the other files, and not a subdirectory.
///
/// The files that must *not* appear are the rule rather than an afterthought: a
/// directory holding a text file, and a subdirectory holding a DLL, is exactly the
/// exposure whole-directory inheritance used to grant and this rule deliberately
/// does not (Ruling 78 refused option D).
#[test]
fn the_program_directory_and_the_sibling_dlls_are_exposed_read_only() {
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    let nested = program_dir.join("nested");
    fs::create_dir_all(&nested).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(program_dir.join("app-local.dll"), b"x").expect("write a sibling");
    fs::write(program_dir.join("notes.txt"), b"x").expect("write a non-DLL sibling");
    fs::write(nested.join("nested.dll"), b"x").expect("write a nested DLL");
    let program = fs::canonicalize(program_dir.join("serve.exe")).expect("canonical program");
    let canonical_dir = fs::canonicalize(&program_dir).expect("canonical directory");

    let sandbox = Sandbox::new().expose(dir.path().join("root"), AccessMode::ReadWrite);
    let all = backend::exposures(&sandbox, &program);

    let entry = all
        .iter()
        .find(|exposure| exposure.host == canonical_dir)
        .expect("the program's directory is exposed");
    assert_eq!(
        entry.access,
        AccessMode::ReadOnly,
        "the program's directory is exposed read-only: {all:?}"
    );

    let dll = all
        .iter()
        .find(|exposure| is_the_file(&canonical_dir, "app-local.dll", &exposure.host))
        .expect("the sibling DLL is exposed");
    assert_eq!(
        dll.access,
        AccessMode::ReadOnly,
        "the sibling DLL is exposed read-only: {all:?}"
    );
    assert_eq!(
        all.iter()
            .filter(|exposure| is_the_file(&canonical_dir, "app-local.dll", &exposure.host))
            .count(),
        1,
        "the sibling DLL is exposed once: {all:?}"
    );

    assert!(
        !all.iter()
            .any(|exposure| is_the_file(&canonical_dir, "notes.txt", &exposure.host)),
        "a non-DLL sibling is not exposed: {all:?}"
    );
    assert!(
        !all.iter()
            .any(|exposure| is_the_file(&nested, "nested.dll", &exposure.host)),
        "a DLL below the program's directory is not exposed: {all:?}"
    );
}

/// The program's directory is exposed even when it holds no DLL at all — the
/// product's own `target/debug/examples` is 539 files and 0 DLLs — and a program
/// whose directory does not exist is not a failure of the exposure set: the
/// directory entry is still produced and the resolution that follows reports the
/// missing path, which is what `confine`'s failed-launch test relies on
/// (a directory that did not exist would fail before a single ACE was granted, and
/// that test needs the ACLs on).
#[test]
fn a_program_directory_with_no_dll_adds_no_file_entry() {
    // **A tree this test owns, so the count below is a fact about the fixture
    // rather than about `E:\build`.** The count is the assertion here ("the
    // served root and the program's directory, and no DLL entry"), and the only
    // way it can be one is for the program's directory to be one this test
    // filled — a fixed path would make the test depend on what happens to be
    // beside `E:\build\serve.exe` on the host running it.
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("build");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    let sandbox = Sandbox::new().expose(dir.path().join("srv"), AccessMode::ReadOnly);
    let all = backend::exposures(&sandbox, &program_dir.join("serve.exe"));
    assert_eq!(
        all.len(),
        2,
        "the served root and the program's directory, and no DLL entry: {all:?}"
    );
    assert!(
        all.iter()
            .any(|exposure| ends_with(&exposure.host, "build")),
        "the program's directory is exposed whatever it holds: {all:?}"
    );

    let missing = backend::exposures(&sandbox, Path::new("E:/nowhere/serve.exe"));
    assert!(
        missing
            .iter()
            .any(|exposure| ends_with(&exposure.host, "nowhere")),
        "a program directory that does not exist is still exposed: {missing:?}"
    );
}

/// The exposure set keeps every entry it produced before the rule changed — the
/// caller's exposures with their own access mode, and the program's directory —
/// and adds nothing but the DLLs beside it.
#[test]
fn every_entry_the_callers_set_carried_is_still_there() {
    let sandbox = Sandbox::new()
        .expose("E:/srv/tinio", AccessMode::ReadWrite)
        .expose("E:/state", AccessMode::ReadWrite);
    let all = backend::exposures(&sandbox, Path::new("E:/build/serve.exe"));
    assert!(
        all.iter().any(|exposure| ends_with(&exposure.host, "tinio")
            && exposure.access == AccessMode::ReadWrite),
        "{all:?}"
    );
    assert!(
        all.iter()
            .any(|exposure| ends_with(&exposure.host, "state")),
        "{all:?}"
    );
    let program_dir = all
        .iter()
        .find(|exposure| ends_with(&exposure.host, "build"))
        .expect("the program's directory is exposed");
    assert_eq!(program_dir.access, AccessMode::ReadOnly);
}

/// The public half of the production combination: a caller exposes the launcher's
/// own directory through a **raw** path, and `canonical_exposures` keeps the
/// caller's entry and its access, so the merge cannot narrow a path the caller
/// wrote.
///
/// **The shape decision that follows the merge is not reachable from here.**
/// `backend::shape_for` and the caller-set helper it compares against are private,
/// and the assertion that pins the precedence lives in `src/backend/tests.rs`,
/// where both can be called. What this file keeps is what it can
/// reach: the two public pure helpers (`backend::exposures`,
/// `backend::canonical_exposures`) on a real tree and a raw spelling, which is the
/// input that private test's production combination starts from.
#[test]
fn the_callers_access_survives_the_exposure_of_its_own_directory() {
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    // The raw path is what the caller writes; a canonical one would hide the
    // spelling question this test exists to keep in view.
    let caller_exposed = program_dir.clone();
    assert!(
        !caller_exposed
            .as_os_str()
            .to_string_lossy()
            .starts_with(r"\\?\"),
        "the caller's spelling is raw: {caller_exposed:?}"
    );
    let sandbox = Sandbox::new().expose(&caller_exposed, AccessMode::ReadWrite);
    let program = program_dir.join("serve.exe");

    let canonical_dir = fs::canonicalize(&program_dir).expect("the program's directory resolves");
    let all =
        canonical_exposures(backend::exposures(&sandbox, &program)).expect("the entries resolve");
    assert_eq!(
        all.len(),
        1,
        "the caller's directory and the launcher's entry are one path: {all:?}"
    );
    assert_eq!(
        all[0].host, canonical_dir,
        "the granted spelling is the volume's: {all:?}"
    );
    assert_eq!(
        all[0].access,
        AccessMode::ReadWrite,
        "the caller's access survives the merge: {all:?}"
    );
}

/// **The launcher's own file entry is granted while a caller's file is refused,
/// which is the distinction P7's refusal is built on.** Every entry `exposures`
/// adds for a sibling `*.dll` is a file, and it never enters the caller's set:
/// `confine` grants it through `acl::Grants::apply_siblings`' one `*.dll`
/// invocation, not through the per-path loop the caller's entries take — so a
/// refusal that read the *granted* set would refuse the launcher's own launch.
/// This pins both halves on the public surface, with the caller-file refusal
/// itself asserted where `canonical_caller` is reachable
/// (`src/backend/tests.rs`).
#[test]
fn the_executables_own_file_entries_are_granted_rather_than_refused() {
    let dir = tempfile::tempdir().expect("temp dir");
    let program_dir = dir.path().join("bin");
    fs::create_dir_all(&program_dir).expect("create the program's directory");
    fs::write(program_dir.join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(program_dir.join("app-local.dll"), b"x").expect("write a sibling");
    let served = dir.path().join("srv");
    fs::create_dir_all(&served).expect("create the served root");
    // The caller exposes only the served root. Everything else in the granted
    // set is the launcher's own, which is what makes the assertion below mean
    // "a launcher entry" rather than "a caller entry that happens to be a file".
    let sandbox = Sandbox::new().expose(&served, AccessMode::ReadOnly);
    let all = backend::exposures(&sandbox, &program_dir.join("serve.exe"));

    let launcher_file = all
        .iter()
        .find(|exposure| ends_with(&exposure.host, "app-local.dll"))
        .expect("the sibling DLL is exposed");
    assert!(
        launcher_file.host.is_file(),
        "the launcher's sibling entry really is a file: {launcher_file:?}"
    );

    // The launcher's file entry resolves, and the granted set keeps it: nothing
    // in the public path refuses it, and its grant is `apply_siblings`'.
    let canonical_dll = fs::canonicalize(&launcher_file.host).expect("the sibling DLL resolves");
    let granted = canonical_exposures(all).expect("the launcher's own entries resolve");
    assert!(
        granted
            .iter()
            .any(|exposure| exposure.host == canonical_dll),
        "the launcher's file entry survives into the granted set: {granted:?}"
    );
    assert!(
        granted.iter().any(|exposure| exposure.host.is_file()),
        "the launcher's own file entries are not filtered out of the granted set: {granted:?}"
    );
}

/// A program directory with no DLL is one entry and no pattern, and a directory
/// with one is a pattern the tool matches against that directory — the shape
/// the grant goes through.
///
/// The negative half is the assertion that matters: an unmatched pattern is an
/// `icacls` error rather than a no-op (measured 2026-09-21), so answering `None`
/// for a directory with no sibling is what keeps `Grants` from recording a grant
/// that never landed.
#[test]
fn the_sibling_pattern_covers_exactly_the_directly_inside_dlls() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(dir.path().join("serve.exe"), b"not really an image").expect("write the program");
    fs::write(dir.path().join("notes.txt"), b"x").expect("write a non-DLL");
    assert_eq!(
        backend::SiblingScan::scan(dir.path()).pattern(),
        None,
        "a directory with no DLL has no pattern to grant"
    );

    fs::write(dir.path().join("app-local.dll"), b"x").expect("write a sibling");
    let pattern = backend::SiblingScan::scan(dir.path())
        .pattern()
        .expect("a sibling makes a pattern");
    assert_eq!(
        pattern.file_name().and_then(|name| name.to_str()),
        Some("*.dll")
    );
    // **The base is the assertion that matters**, and it is the one the first
    // draft of this test missed: a pattern built in the *parent* has the same
    // `*.dll` leaf and the same suffix, so only the base distinguishes the rule
    // that grants the program's own siblings from one that grants its parent's
    // (and misses the program's).
    assert_eq!(
        pattern.parent(),
        Some(dir.path()),
        "the pattern is built in the program's directory, not its parent: {pattern:?}"
    );
    assert_eq!(pattern, dir.path().join("*.dll"), "{pattern:?}");
    assert_eq!(
        backend::SiblingScan::scan(&dir.path().join("never-created")).pattern(),
        None,
        "a directory that cannot be read has no pattern either"
    );

    // The enumerate side the pattern must agree with: the exposure set names
    // exactly the DLL directly inside the directory, and the pattern is that same
    // directory's — one level up would name a set the grant never covers.
    let sandbox = Sandbox::new().expose(dir.path().join("root"), AccessMode::ReadOnly);
    let all = backend::exposures(&sandbox, &dir.path().join("serve.exe"));
    let enumerated = all
        .iter()
        .filter(|exposure| exposure.host.is_file())
        .count();
    assert_eq!(enumerated, 1, "one sibling DLL is exposed: {all:?}");
    let base = pattern.parent().expect("the pattern has a directory");
    assert!(
        all.iter()
            .all(|exposure| !exposure.host.is_file() || exposure.host.parent() == Some(base)),
        "every enumerated sibling lies in the pattern's directory: {all:?}"
    );
    assert_eq!(
        backend::SiblingScan::scan(base).pattern(),
        Some(pattern),
        "the pattern is derived from that same directory"
    );
}
/// **The three spec strings, pinned where no DACL is needed** (Ruling 85's
/// division of labour): the caller's exposed directories keep `(OI)(CI)` — the
/// inherited form whose whole purpose is that a file already inside them stays
/// reachable — while the program's own directory and every file take the plain
/// form. A launcher that sent the plain form to the caller's directories is the
/// regression this pins, and it is invisible to any check that only reads whether
/// a grant reported success.
#[test]
fn the_three_shapes_are_the_ones_the_launcher_grants() {
    let sid = "S-1-15-2-1-2-3";

    // The caller's exposures: inherited, so a pre-existing descendant of the
    // served root or the report directory is reachable by the child (design §6).
    assert_eq!(
        acl::grant_spec(sid, Target::Directory, AccessMode::ReadWrite),
        "*S-1-15-2-1-2-3:(OI)(CI)M"
    );
    assert_eq!(
        acl::grant_spec(sid, Target::Directory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:(OI)(CI)RX"
    );

    // The program's own directory: a directory, and deliberately no propagation.
    assert_eq!(
        acl::grant_spec(sid, Target::ProgramDirectory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:RX"
    );

    // A file: the plain form, because the inheritance flags are a no-op on one.
    assert_eq!(
        acl::grant_spec(sid, Target::File, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:RX"
    );
    assert_eq!(
        acl::grant_spec(sid, Target::File, AccessMode::ReadWrite),
        "*S-1-15-2-1-2-3:M"
    );
}
/// **The ordinary spelling is the caller's, and this is the assertion that keeps
/// it so.** The variant a call site reaches for when it has a directory in hand
/// must be the inherited one; decision 22's entry is the only site in this crate
/// that spells the exception, and it is named after the decision. This pins the
/// direction rather than the mechanism: a future rename that made the plain form
/// the obvious spelling would fail here before it could silently narrow a caller's
/// exposure.
#[test]
fn the_obvious_directory_spelling_is_the_one_that_propagates() {
    assert_eq!(
        acl::grant_spec("S-1-15-2-1-2-3", Target::Directory, AccessMode::ReadOnly),
        "*S-1-15-2-1-2-3:(OI)(CI)RX",
        "a caller with a directory in hand means the inherited shape"
    );
    assert_eq!(
        acl::grant_spec(
            "S-1-15-2-1-2-3",
            Target::ProgramDirectory,
            AccessMode::ReadOnly
        ),
        "*S-1-15-2-1-2-3:RX",
        "and the exception is spelled out"
    );
}
