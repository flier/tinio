//! The `service` verbs' pure halves: the label, the install context, and the unit it carries.
//!
//! The verbs themselves need a supervisor and the elevation to talk to it, which is why the
//! platform handshake is a manual leg (`docs/tests.md`, "Service verbs (manual)") and every
//! decision that is *not* the platform's is asserted here.

use std::{ffi::OsString, fs, path::Path};

use service_manager::{ServiceLevel, ServiceManagerKind};
use tinio_cli::{
    resolve::resolve_root,
    service::{install_context, label_for, level_for, permission_remedy, server_argv},
};

#[test]
fn the_label_is_derived_from_the_root_and_overridable() {
    // The derivation is over the **canonical** root, so the two spellings of one
    // directory are one service: `label_for` is called with what `resolve::resolve_root`
    // returned, never with the operator's spelling.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("data");
    fs::create_dir_all(&root).expect("root");
    let canonical = root.canonicalize().expect("canonical");
    let derived = label_for(&canonical, None);
    assert!(derived.starts_with("tinio-"), "{derived}");
    assert_eq!(
        derived.len(),
        "tinio-".len() + 16,
        "16 hex characters, as the control pipe's name is"
    );
    assert_eq!(
        derived,
        label_for(&canonical, None),
        "the same root always derives the same label"
    );
    // `./data` and its canonical form are one service, and the reason is the **caller**:
    // `label_for` digests the bytes it is handed, and `resolve_root` is what hands every
    // caller the canonical ones. Asserted through that entry point rather than by feeding
    // the raw join to `label_for`, because on Windows (a `\\?\` verbatim prefix) and on
    // macOS (a symlinked `TMPDIR`) the two spellings are not byte-equal even though they
    // are the same directory — a difference the label must not be made to paper over.
    let resolved = resolve_root(Some(&root), &root).expect("the resolved root");
    assert_eq!(resolved, canonical, "`resolve_root` canonicalises");
    assert_eq!(
        derived,
        label_for(&resolved, None),
        "`<dir>/data` and its canonical form agree"
    );
    assert_ne!(derived, label_for(&dir.path().join("other"), None));
    assert_eq!(label_for(&canonical, Some("my-tinio")), "my-tinio");
}

#[test]
fn a_read_only_unit_records_the_resolved_state_dir() {
    // The supervisor does not keep the installing environment. Read-only state is not a
    // function of the root, so the argv the unit runs must name the path install resolved.
    let argv = server_argv(Path::new("/srv/data"), true, Path::new("/var/lib/tinio"));
    assert_eq!(
        argv,
        [
            OsString::from("server"),
            OsString::from("/srv/data"),
            OsString::from("--read-only"),
            OsString::from("--state-dir"),
            OsString::from("/var/lib/tinio"),
        ]
    );
    let read_write = server_argv(Path::new("/srv/data"), false, Path::new("/srv/data/.tinio"));
    assert_eq!(
        read_write,
        [OsString::from("server"), OsString::from("/srv/data")]
    );
}

#[test]
fn the_install_context_carries_our_unit_and_the_server_argv() {
    let context = install_context(
        Path::new("/srv/data/.tinio"),
        Path::new("/usr/local/bin/tinio"),
        vec!["server".into(), "/srv/data".into()],
        "tinio-abc",
    )
    .expect("the install context");
    assert_eq!(context.program, Path::new("/usr/local/bin/tinio"));
    assert_eq!(
        context.args,
        vec![OsString::from("server"), OsString::from("/srv/data")]
    );
    assert!(
        context.autostart,
        "a service that does not come back after a reboot is not a service"
    );
    // The working directory is the **state dir** the run resolved, not the root: read-only
    // mode relocates it (decision 6), and a unit that hard-codes `<root>/.tinio` would send
    // a read-only deployment's relative paths into the tree the mode promises not to write.
    assert_eq!(
        context.working_directory.as_deref(),
        Some(Path::new("/srv/data/.tinio"))
    );
    #[cfg(unix)]
    {
        let contents = context
            .contents
            .expect("our unit, not the crate's Type=simple template");
        assert!(contents.contains("Type=notify"), "{contents}");
        assert!(
            contents.contains("WorkingDirectory=/srv/data/.tinio"),
            "{contents}"
        );
    }
    #[cfg(not(unix))]
    assert!(
        context.contents.is_none(),
        "the unit text is systemd's, and this crate does not compile it off unix"
    );
}

#[test]
fn the_user_level_asks_for_no_elevation() {
    // The level is the manager's, not the context's: `ServiceInstallCtx` has no `level` field
    // (`service-manager 0.11.0/src/lib.rs:298-341`) — the crate carries it on the manager
    // (`ServiceManager::set_level`) — so what `--user` maps to is asserted here, on the one
    // pure function that maps it, rather than on a context field that does not exist.
    assert_eq!(level_for(false), ServiceLevel::System);
    assert_eq!(level_for(true), ServiceLevel::User);
}

#[test]
fn a_permission_failure_names_the_remedy_not_the_errno() {
    // The risk row decision 22 answers: three supervisors report the same failure as
    // `EACCES` or "Access is denied", and passing that through tells the operator nothing.
    let unix = permission_remedy(ServiceManagerKind::Systemd, ServiceLevel::System);
    assert!(
        unix.contains("--user"),
        "the unix remedy is the user level: {unix}"
    );
    let windows = permission_remedy(ServiceManagerKind::Sc, ServiceLevel::System);
    assert!(
        windows.contains("Administrator"),
        "the Windows remedy is elevation: {windows}"
    );
    // Already at the user level, the remedy is not "pass --user again".
    let user = permission_remedy(ServiceManagerKind::Systemd, ServiceLevel::User);
    assert!(
        !user.contains("--user"),
        "the flag is already in effect: {user}"
    );
    assert!(!user.contains("EACCES"), "never the raw error: {user}");
}
