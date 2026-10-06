#[cfg(unix)]
use std::fs::create_dir_all;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::{
    fs::{read_dir, write},
    io::ErrorKind,
    time::Duration,
};

use redb::{Database, ReadableDatabase, backends::InMemoryBackend};
use tokio::{fs, time};

use super::*;
#[cfg(windows)]
use crate::testutil::link_directory;
use crate::{
    _core::{
        acl::Acl,
        multipart::part_number,
        object,
        storage::{
            BucketOps, Error as StorageError, ListPartsParams, MultipartOps, ObjectOps, VersionOps,
        },
        version::State,
    },
    _util::{
        path::path_contains,
        testing::{body, read_body},
    },
    bucket::name,
    database::{self, StateTable},
    testutil::fs_options,
};

#[test]
fn compact_threshold_percent_is_validated() {
    let root = tempfile::tempdir().unwrap();
    let err = FsStorage::new(
        root.path(),
        FsOptions {
            compact_threshold_percent: 4,
            ..fs_options()
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue(_)));
    assert!(
        err.to_string().contains("compact_threshold_percent"),
        "{err}"
    );
}

#[tokio::test]
async fn new_from_db_constructs_over_an_opened_database() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let db = database::open(state.path()).unwrap().db;
    let storage = FsStorage::new_from_db(
        root.path(),
        FsOptions {
            state_dir: Some(state.path().to_path_buf()),
            ..fs_options()
        },
        db,
    )
    .unwrap();
    assert_eq!(storage.state_dir(), state.path());
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    assert!(storage.bucket_names("").await.unwrap() == vec![b]);
}

#[tokio::test]
async fn read_only_state_relocation_keeps_root_clean() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    let state = home.path().join("roots").join("abc");
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            state_dir: Some(state.clone()),
            ..fs_options()
        },
    )
    .unwrap();
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .put_object(&b, &"a.txt".into(), body(b"hello"))
        .await
        .unwrap();
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    // All private state lives under the state dir.
    assert!(state.join("meta.redb").exists());
    // The root holds only the bucket directory — no `.tinio/`, no
    // new files.
    let root_entries: Vec<String> = read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        root_entries,
        ["data"],
        "root must have zero private state: {root_entries:?}"
    );
    assert!(!root.path().join(".tinio").exists());
}

/// The read-only read-path fixture both tests below share: a root whose
/// read-write run created bucket `data` with `notes.txt` = `"hello"` and a
/// planted tag, then released the root's database — redb refuses a
/// read-only open concurrent with a `Database`, so the writer is gone
/// before the caller's read-only run starts. The tempdir is returned to
/// hold the root for the caller's storage.
async fn planted_read_only_root() -> (tempfile::TempDir, Name, object::Key, object::Tags) {
    let root = tempfile::tempdir().unwrap();
    let b = name("data").unwrap();
    let key: object::Key = "notes.txt".into();
    let tags = object::Tags::from_pairs([("planted".to_owned(), "yes".to_owned())]).unwrap();
    {
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage.put_object(&b, &key, body(b"hello")).await.unwrap();
        storage.put_object_tags(&b, &key, &tags).await.unwrap();
    }
    (root, b, key, tags)
}

/// FR-023 amended (design 2026-10-03, **Read paths**): a read-only run
/// reads the root's **own** `.tinio/meta.redb`, opened read-only — so the
/// metadata a read-write run wrote (here an object's tag) is *served*,
/// and the object still serves from its file. This is the amendment's
/// core claim: history, tags, ACLs and CORS are readable in this mode
/// rather than lost.
#[tokio::test]
async fn a_read_only_store_serves_the_metadata_a_read_write_run_wrote() {
    let (root, b, key, tags) = planted_read_only_root().await;

    // The read-only run over the same root.
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            ..fs_options()
        },
    )
    .unwrap();
    assert_eq!(
        storage.state_dir(),
        storage.root().join(".tinio").as_path(),
        "a read-only storage reads the root's own state dir"
    );

    let read = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(read.info.size, 5, "the object's row is served");
    assert_eq!(read_body(read.body).await.unwrap(), b"hello");
    let served = storage.get_object_tags(&b, &key).await.unwrap();
    assert_eq!(
        served, tags,
        "the tag a read-write run wrote is served by the read-only run"
    );
}

/// FR-023 amended: the `new_from_db` orchestration path must read the
/// **root's** database in a read-only run, never the caller's handle.
///
/// `doctor` opens (and, on `--fix`, creates) its store at the **tool-state**
/// dir and hands it to [`FsStorage::new_from_db`]; before this rule that
/// handle became the storage's metadata source, so the metadata legs of a
/// read-only doctor run answered from a database that is not the root's.
/// The fix drops the caller's handle and opens the root's own read-only —
/// the same source [`FsStorage::new`] uses — and this path creates no
/// database of its own in the caller's directory.
#[tokio::test]
async fn new_from_db_in_read_only_mode_reads_the_roots_database() {
    let (root, b, key, tags) = planted_read_only_root().await;
    let tool = tempfile::tempdir().unwrap();

    // (a) The caller's handle in doctor's shape: a store at the tool-state
    // dir. The tag exists only in the **root's** database, so serving it is
    // the proof that the root's database is the metadata source on this
    // path.
    let caller_state = tool.path().join("state");
    let caller = database::open(&caller_state).unwrap().db;
    let storage = FsStorage::new_from_db(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(caller_state),
            ..fs_options()
        },
        caller,
    )
    .unwrap();
    assert_eq!(
        storage.get_object_tags(&b, &key).await.unwrap(),
        tags,
        "the metadata comes from the root's database, not the caller's"
    );
    assert_eq!(
        storage.state_dir(),
        storage.root().join(".tinio").as_path(),
        "the storage's one state dir is the root's own"
    );

    // (b) This path creates no database of its own: a caller whose handle
    // is in memory leaves the tool-state dir untouched.
    let fresh = tool.path().join("fresh");
    let caller = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let storage = FsStorage::new_from_db(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(fresh.clone()),
            ..fs_options()
        },
        caller,
    )
    .unwrap();
    assert_eq!(storage.get_object_tags(&b, &key).await.unwrap(), tags);
    assert!(!fresh.exists(), "the path created `{}`", fresh.display());
}

/// FR-023 amended: a root with objects but **no** `.tinio/` cannot be
/// read as a metadata source. The read-only open fails, the run degrades
/// to today's shape — objects serve from their files, every metadata read
/// answers empty — and the failure is what the open's `tracing::warn!`
/// reports (the reason's one user-visible carrier).
#[tokio::test]
async fn a_read_only_store_over_a_root_with_no_database_degrades_to_an_empty_view() {
    let root = tempfile::tempdir().unwrap();
    // Objects with no database at all: this root was never opened
    // read-write (the `@fs @read-only` fixture's shape).
    fs::create_dir(root.path().join("data")).await.unwrap();
    fs::write(root.path().join("data/a.txt"), b"hello")
        .await
        .unwrap();
    let b = name("data").unwrap();
    let key: object::Key = "a.txt".into();

    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            ..fs_options()
        },
    )
    .unwrap();

    // The object serves from its file...
    let read = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(read.info.size, 5);
    assert_eq!(read_body(read.body).await.unwrap(), b"hello");
    // ...and the metadata view is empty (the degrade's observable).
    assert!(
        storage.get_object_tags(&b, &key).await.unwrap().is_empty(),
        "a degraded run serves no metadata"
    );
    // A read-only run writes nothing under the root — not even the
    // `.tinio/` whose absence degraded it.
    assert!(
        !root.path().join(".tinio").exists(),
        "the degraded read-only run must not create the state dir"
    );
}

/// FR-023 amended: read-only mode's storage state dir is the **root's
/// own** `.tinio/` — with no state dir configured, with one outside the
/// root, and with one inside it alike — because that is where the rows a
/// read-write run wrote live. The configured value is the caller's tool
/// state and is not the storage's in this mode.
///
/// **Nothing is created** — neither the root's own `.tinio/` (the run
/// opens it read-only, and a root with none degrades: the tests above),
/// nor the configured directory the storage does not use.
#[tokio::test]
async fn read_only_uses_the_roots_own_state_dir_whatever_is_configured() {
    let root = tempfile::tempdir().unwrap();
    let home = tempfile::tempdir().unwrap();
    // A bucket with one object and no database: the degrade's observable
    // is the served file beside the empty metadata view.
    fs::create_dir(root.path().join("data")).await.unwrap();
    fs::write(root.path().join("data/a.txt"), b"hello")
        .await
        .unwrap();
    let b = name("data").unwrap();
    let key: object::Key = "a.txt".into();
    for configured in [None, Some(home.path().join("relocated"))] {
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                state_dir: configured,
                ..fs_options()
            },
        )
        .unwrap();
        assert_eq!(
            storage.state_dir(),
            storage.root().join(".tinio").as_path(),
            "the storage's one state dir is the root's own in read-only mode"
        );
        assert_eq!(
            read_body(storage.get_object(&b, &key, None).await.unwrap().body)
                .await
                .unwrap(),
            b"hello",
            "this root holds no database, so the run degrades: the file serves"
        );
        assert!(
            storage.get_object_tags(&b, &key).await.unwrap().is_empty(),
            "and the degraded metadata view answers empty"
        );
    }
    assert!(
        !root.path().join(".tinio").exists(),
        "the read-only runs created nothing under the root"
    );
    assert!(
        !home.path().join("relocated").exists(),
        "nor the out-of-root one: the storage never holds the tool state"
    );
}

/// The old in-root refusal survives the amendment, and now for the
/// reason the amendment makes exact: the configured state dir is the
/// caller's **tool state** — the files a run *writes* — so a read-only
/// run that names one inside the served root would write the very tree
/// the mode promises to leave alone, whatever the storage reads.
#[test]
fn read_only_refuses_an_in_root_state_dir() {
    let root = tempfile::tempdir().unwrap();
    let err = FsStorage::new(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(root.path().join("state")),
            ..fs_options()
        },
    )
    .unwrap_err();
    assert!(matches!(err, Error::InvalidValue(_)), "{err:?}");
    assert!(
        err.to_string().contains("outside the storage root"),
        "{err}"
    );

    // Clause 1 (the verbatim shape of the same root) and the Windows case
    // relaxation — NTFS resolves all three forms into the root, so all
    // three are refused. The verbatim leg keeps the first clause from
    // being deleted as dead code on Windows; the differently-cased leg is
    // the regression the case-insensitive comparison protects. Only the
    // leaf is cased up there: uppercasing a whole temp path would make the
    // refusal depend on `to_lowercase(to_uppercase(p)) == to_lowercase(p)`,
    // which a character like `ß` (→ `SS` → `ss`) breaks.
    #[cfg(windows)]
    for dir in [
        PathBuf::from(format!(r"\\?\{}\state", root.path().display())),
        root.path().join("STATE"),
    ] {
        let err = FsStorage::new(
            root.path(),
            FsOptions {
                access: AccessMode::ReadOnly,
                state_dir: Some(dir),
                ..fs_options()
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::InvalidValue(_)), "{err:?}");
    }

    let state = tempfile::tempdir().unwrap();
    FsStorage::new(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(state.path().to_path_buf()),
            ..fs_options()
        },
    )
    .unwrap();

    // **A state directory that does not exist yet is judged as the one that
    // does**, and here that judgement is the accepted one: the resolution lands
    // beside the root rather than inside it, so non-existence decides nothing on
    // its own. This is read-only mode's ordinary input — the directory is the
    // run's tool state, which the serving half creates — and the leg below is
    // the refusal it must not lose by making existence a condition.
    let home = tempfile::tempdir().unwrap();
    let absent = home.path().join("state");
    assert!(!absent.exists());
    FsStorage::new(
        root.path(),
        FsOptions {
            access: AccessMode::ReadOnly,
            state_dir: Some(absent),
            ..fs_options()
        },
    )
    .unwrap();
}

/// **The same refusal reached through the other spelling of one directory** — the
/// hole a literal comparison leaves open, and the shape macOS hands the guard for
/// free: `tempfile` answers `$TMPDIR` as `/var/folders/…` while construction
/// canonicalizes the root to `/private/var/folders/…`, so the two agree on no
/// prefix at all, the guard passes, and a read-only run's tool state writes
/// inside the tree the mode promises to leave alone. The explicit symlink is what
/// makes the pair deterministic on every unix host rather than only where the temp
/// directory happens to sit behind a link, and the `path_contains` assertion below
/// is what keeps the leg from passing vacuously: the literal comparison really does
/// answer `false` for this pair.
///
/// **Both halves of the state directory's existence are refused**, because the
/// guard runs before anything creates the tool state and the refusal cannot be a
/// question about a directory that is not there yet. The resolution walks to the
/// nearest existing ancestor — the link, which is the one component that does exist
/// — and rejoins what is below it, so the absent leaf and the present one are the
/// same directory to this guard. (Measured on macOS 15.8, 2026-09-25: a first
/// version of that walk canonicalized the leaf itself, answered `ENOENT`, and let
/// the exploit through — the launcher's copy of this rule carries the same
/// measurement.)
#[cfg(unix)]
#[test]
fn read_only_refuses_an_in_root_state_dir_spelled_through_a_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    create_dir_all(&real).unwrap();
    let canonical_root = canonicalize(&real).unwrap();
    let link = dir.path().join("link");
    symlink(&real, &link).unwrap();
    let spelled = link.join("state");
    let absolute = absolute(&spelled).unwrap();
    assert!(
        !path_contains(&absolute, &canonical_root),
        "the leg is vacuous unless the literal comparison answers `false`: \
         {absolute:?} vs {canonical_root:?}"
    );

    for exists in [false, true] {
        if exists {
            create_dir_all(&spelled).unwrap();
        }
        assert_eq!(spelled.exists(), exists);
        let err = FsStorage::new(
            &canonical_root,
            FsOptions {
                access: AccessMode::ReadOnly,
                state_dir: Some(spelled.clone()),
                ..fs_options()
            },
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::InvalidValue(_)),
            "exists: {exists}: {err:?}"
        );
        assert!(
            err.to_string().contains("outside the storage root"),
            "{err}"
        );
    }
}

/// The verbatim leg of the containment rule at the shapes the read-only
/// guard meets: `std::fs::canonicalize` answers a verbatim root
/// (`\\?\C:\data`, or `\\?\UNC\srv\share\data` for a UNC root, a spelling
/// no fixture can produce here — it needs a real UNC root) while
/// `std::path::absolute` answers the plain form (`C:\data`,
/// `\\srv\share\data`), so a plain state dir has to match the verbatim root
/// it is compared against. The sibling case runs against the same verbatim
/// root, so the leg is a comparison and not a blanket `true`; an
/// already-plain root keeps answering too, which is the shape the rule
/// meets in the launcher, where neither path came from a canonicalizer.
///
/// [`path_contains`] is that rule — `tinio-util`'s one definer (FR-023,
/// decision 17a) — so this test pins a behaviour of a dependency rather
/// than of this crate: it is kept here because the read-only guard in
/// [`FsStorage::resolve`] is what would lose the refusal.
#[cfg(windows)]
#[test]
fn a_verbatim_root_contains_the_plain_spelling_of_a_candidate() {
    assert!(path_contains(
        Path::new(r"C:\data\state"),
        Path::new(r"\\?\C:\data")
    ));
    // A verbatim UNC root is compared in its plain UNC spelling.
    assert!(path_contains(
        Path::new(r"\\srv\share\data\state"),
        Path::new(r"\\?\UNC\srv\share\data")
    ));
    assert!(path_contains(
        Path::new(r"c:\DATA\state"),
        Path::new(r"C:\data")
    ));
    assert!(!path_contains(
        Path::new(r"C:\data2\state"),
        Path::new(r"\\?\C:\data")
    ));
    assert!(!path_contains(
        Path::new(r"C:\other"),
        Path::new(r"\\?\C:\data")
    ));
}

/// The Windows half of the containment rule: the comparison folds case,
/// because NTFS resolves `…\DATA` and `…\data` to the same directory while
/// [`Path::starts_with`] compares literally — and it stays component-wise,
/// so `…\data2` is a sibling of `…\data`, not inside it. The fold is what
/// keeps a differently-cased in-root state dir from reading as outside.
///
/// The rule is [`path_contains`], `tinio-util`'s one definer, and the
/// case fold lives there: the crate compares nothing itself any more, so
/// this test is the consumer-side net over the shared answer.
#[cfg(windows)]
#[test]
fn containment_folds_case_and_stays_component_wise() {
    let root = Path::new(r"C:\data");
    assert!(path_contains(Path::new(r"c:\DATA\state"), root));
    assert!(
        !path_contains(Path::new(r"C:\data2\state"), root),
        "a name that merely starts with the root's is not inside it"
    );
    assert!(!path_contains(Path::new(r"C:\other"), root));
}

/// The unix half of the same rule ([`path_contains`], the one definer):
/// `starts_with` compares case-sensitively, because unix filesystems
/// distinguish `…/Data` from `…/data`, and still component-wise.
#[cfg(not(windows))]
#[test]
fn containment_is_case_sensitive_and_component_wise() {
    let root = Path::new("/data");
    assert!(path_contains(Path::new("/data/state"), root));
    assert!(!path_contains(Path::new("/Data/state"), root));
    assert!(!path_contains(Path::new("/data2/state"), root));
}

#[tokio::test]
async fn new_consumes_the_compact_marker() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    {
        let db = database::open(state.path()).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut state = StateTable::open(&mut txn).unwrap();
            state.set_compact_marker(true).unwrap();
        }
        txn.commit().unwrap();
    }
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            state_dir: Some(state.path().to_path_buf()),
            ..fs_options()
        },
    )
    .unwrap();
    assert!(!storage.evaluate_compact(20).await.is_err());
    drop(storage);
    // The marker is cleared after the startup compact.
    let db = database::open(state.path()).unwrap().db;
    let txn = db.begin_read().unwrap();
    let state = StateTable::open_readonly(&txn).unwrap();
    assert!(!state.compact_marker().unwrap());
}

#[test]
fn new_refuses_a_file_root() {
    // The root must be a directory: a file root canonicalizes fine but
    // is not a bucket container — `RootNotDirectory` (startup fails
    // loudly, never halfway).
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("not-a-dir");
    write(&file, b"x").unwrap();
    let err = FsStorage::new(&file, fs_options()).unwrap_err();
    assert!(
        matches!(err, Error::RootNotDirectory(_)),
        "expected RootNotDirectory, got {err:?}"
    );
}

#[test]
fn a_root_failure_that_is_not_a_refusal_keeps_the_platform_canonicalizers_answer() {
    // **The canonicalize fallback is for one refusal and nothing else.** It
    // exists because a confined Windows process is refused
    // `std::fs::canonicalize` for every path, and `soft-canonicalize` resolves
    // the same path through calls that container grants; every other failure
    // is the platform canonicalizer's to report. `soft_canonicalize` resolves
    // lexically once the filesystem stops answering — a path that does not
    // exist comes back `Ok` — so a fallback reached for a missing root would
    // turn this constructor's `Error::Io(NotFound)` into a
    // `RootNotDirectory` about a path that is not there at all. Windows is
    // where the refusal is reachable; this half is every platform's.
    let root = tempfile::tempdir().unwrap();
    let missing = root.path().join("no-such-root");
    let err = FsStorage::new(&missing, fs_options()).unwrap_err();
    assert!(
        matches!(&err, Error::Io(e) if e.kind() == ErrorKind::NotFound),
        "a missing root is the platform canonicalizer's own error, got {err:?}"
    );
}

#[tokio::test]
async fn bucket_dir_of_a_missing_bucket_answers_no_such_bucket() {
    let root = tempfile::tempdir().unwrap();
    // The symlink-probing path (follow_symlinks) lstat-probes the
    // directory — a missing bucket answers the contract error there.
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    let b = name("missing-bucket").unwrap();
    let err = storage.bucket_dir(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "expected NoSuchBucket, got {err:?}"
    );
}

#[tokio::test]
async fn write_lock_stats_reports_the_database_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let snapshot = storage.write_lock_stats();
    assert_eq!(snapshot.count, 0);
    assert_eq!(snapshot.wait_sum_us, 0);
    assert_eq!(snapshot.total_sum_us, 0);
}

#[tokio::test]
async fn set_max_concurrent_uploads_is_enforced() {
    let root = tempfile::tempdir().unwrap();
    let mut storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    let k = object::key("k").unwrap();
    storage.set_max_concurrent_uploads(1);
    storage
        .multipart_store()
        .create(
            &b,
            &k,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let err = storage
        .multipart_store()
        .create(
            &b,
            &k,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("uploads"), "{err}");
}

#[tokio::test]
async fn bucket_names_skips_root_level_files() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    fs::write(root.path().join("notes.txt"), b"not a bucket")
        .await
        .unwrap();
    let names = storage.bucket_names("").await.unwrap();
    assert_eq!(names, vec![b], "{names:?}");
}

#[tokio::test]
async fn bucket_names_filters_by_prefix() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    for raw in ["alpha-1", "alpha-2", "beta-1"] {
        storage
            .create_bucket(&name(raw).unwrap(), None, &Acl::default_private(None))
            .await
            .unwrap();
    }
    let names = storage
        .bucket_names("alpha")
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.as_ref().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, ["alpha-1", "alpha-2"], "{names:?}");
    // The empty-prefix form keeps the scanner/cleanup behavior.
    assert_eq!(storage.bucket_names("").await.unwrap().len(), 3);
}

/// The follow-policy fixture of the two bucket-name tests (R3 — one
/// copy, used by both): a real bucket, a linked bucket directory, and a
/// broken link — returns `bucket_names("")` under the policy.
/// Platform-split inside: `symlink` on unix, `link_directory` (junction)
/// on Windows.
async fn bucket_names_with_links(follow_symlinks: bool) -> Vec<String> {
    let root = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks,
            ..fs_options()
        },
    )
    .unwrap();
    let b = name("real-bucket").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    #[cfg(unix)]
    symlink(target.path(), root.path().join("linked")).unwrap();
    #[cfg(windows)]
    link_directory(target.path(), &root.path().join("linked"));
    #[cfg(unix)]
    symlink(
        root.path().join("does-not-exist"),
        root.path().join("broken"),
    )
    .unwrap();
    #[cfg(windows)]
    link_directory(
        &root.path().join("does-not-exist"),
        &root.path().join("broken"),
    );
    storage
        .bucket_names("")
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.as_ref().to_string())
        .collect()
}

#[cfg(unix)]
#[tokio::test]
async fn bucket_names_follows_symlinked_bucket_dirs() {
    // A symlinked bucket directory is a bucket only when following is
    // enabled (the follow policy, one source of truth); a broken link is
    // never a bucket.
    assert_eq!(
        bucket_names_with_links(true).await,
        vec!["linked".to_string(), "real-bucket".to_string()]
    );
    assert_eq!(
        bucket_names_with_links(false).await,
        vec!["real-bucket".to_string()]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn bucket_dir_resolves_dangling_and_looping_links() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    symlink(
        root.path().join("no-such-target"),
        root.path().join("dangle"),
    )
    .unwrap();
    symlink(root.path().join("loop"), root.path().join("loop")).unwrap();

    let dangle = name("dangle").unwrap();
    let err = storage.bucket_dir(&dangle).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "dangling link: {err:?}"
    );

    let looped = name("loop").unwrap();
    let err = storage.bucket_dir(&looped).await.unwrap_err();
    assert!(matches!(err, Error::Io(_)), "link loop: {err:?}");
}

// macOS/APFS rejects non-UTF-8 names at creation (EILSEQ) — the
// scenario is unrepresentable there; Linux/BSD keep the coverage.
#[cfg(all(unix, not(target_os = "macos")))]
#[tokio::test]
async fn bucket_names_skips_non_utf8_directory_names() {
    // A top-level directory whose name is not valid UTF-8 cannot be a
    // bucket name (the contract is UTF-8) — skipped, never a panic.
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    use crate::_core::acl::Acl;
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    let mut name = b"bad-".to_vec();
    name.push(0xff);
    fs::create_dir(root.path().join(OsStr::from_bytes(&name)))
        .await
        .unwrap();
    let names = storage.bucket_names("").await.unwrap();
    assert_eq!(names, vec![b], "{names:?}");
}

#[cfg(windows)]
#[tokio::test]
async fn bucket_dir_of_a_dangling_junction_is_no_such_bucket() {
    // A bucket dir that is a DANGLING junction resolves to no target:
    // `bucket_dir` answers NoSuchBucket (the bucket is the target, and
    // the target is gone — F13).
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    link_directory(
        &root.path().join("no-such-target"),
        &root.path().join("data"),
    );
    let b = name("data").unwrap();
    let err = storage.bucket_dir(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "{err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn bucket_names_follows_junction_bucket_dirs() {
    // A junction bucket directory is a bucket only when following is
    // enabled (the follow policy, one source of truth); a broken link
    // is never a bucket.
    assert_eq!(
        bucket_names_with_links(true).await,
        vec!["linked".to_string(), "real-bucket".to_string()]
    );
    assert_eq!(
        bucket_names_with_links(false).await,
        vec!["real-bucket".to_string()]
    );
}

#[tokio::test]
async fn lock_bucket_mutations_warns_after_the_threshold() {
    // A waiter past the warn threshold (1 s) still acquires the lock —
    // the wait is warned (D-E), never silently absorbed and never
    // failed.
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    let held = storage.lock_bucket_mutations(&b).await;
    let task = tokio::spawn({
        let storage = storage.clone();
        let b = b.clone();
        async move {
            let guard = storage.lock_bucket_mutations(&b).await;
            drop(guard);
        }
    });
    time::sleep(Duration::from_millis(1500)).await;
    drop(held);
    task.await.unwrap();
}

#[tokio::test]
async fn ensure_bucket_refuses_a_file_in_place_of_a_bucket() {
    // A bucket path that exists as a FILE is no bucket (NoSuchBucket,
    // never an IO error).
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    fs::write(root.path().join("data"), b"not-a-dir")
        .await
        .unwrap();
    let err = storage.ensure_bucket(&b).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::NoSuchBucket(_))),
        "{err:?}"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn resolve_object_file_rejects_a_junction_leaf_when_following_is_disabled() {
    use crate::_core::acl::Acl;
    // A key whose path resolves through a junction is refused with
    // AccessDenied when following is disabled (the documented 403 — a
    // link inside the bucket never escapes the root, s3-surface.md).
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let b = name("data").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    fs::write(outside.path().join("x.txt"), b"s").await.unwrap();
    link_directory(outside.path(), &root.path().join("data/subdir"));
    let k = object::key("subdir/x.txt").unwrap();
    let err = storage.resolve_object_file(&b, &k).await.unwrap_err();
    assert!(
        matches!(err, Error::Storage(StorageError::AccessDenied(_))),
        "{err:?}"
    );
}

// --- state-layout tests (meta-redb-spec acceptance 2 and 7; moved inline
// from `tests/layout.rs`, 2026-10-01) ---
//
// The state dir holds only `meta.redb`, `tmp/`, and `multipart/` part
// files — no `buckets.json`, no `upload.json`, no `meta/objects/` fan-out,
// and no separate lock file (redb 4.2 takes the file lock inside
// `meta.redb` itself — meta-redb-spec §4). Deleting `meta.redb` is safe:
// the metadata is derivable and recomputed on demand (self-healing
// restart).

/// The shared offline defaults plus the test's state-dir override (F33):
/// the one-argument form of the imported `fs_options` (the bare name is
/// taken by it).
fn fs_options_in(state_dir: &Path) -> FsOptions {
    FsOptions {
        state_dir: Some(state_dir.to_path_buf()),
        ..fs_options()
    }
}

/// The entries of `dir` as a sorted set.
fn entries(dir: &Path) -> Vec<String> {
    let mut out: Vec<String> = read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    out.sort();
    out
}

/// A bucket name spelled as a literal (the panicking `From<&str>` the
/// layout tests use for their fixed names).
fn bucket_name(raw: &str) -> Name {
    raw.into()
}

/// One full lifecycle over an explicit state dir; then assert the layout.
#[tokio::test]
async fn state_dir_holds_only_redb_tmp_multipart_and_blobs() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options_in(state.path())).unwrap();

    let b = bucket_name("data");
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .put_object(&b, &"hello.txt".into(), body(b"hello"))
        .await
        .unwrap();

    // Multipart lifecycle leaves part files only.
    let key = "big.bin".into();
    let upload = storage
        .create_multipart_upload(
            &b,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    storage
        .upload_part(
            &b,
            &key,
            &upload.upload_id,
            part_number(1).unwrap(),
            body(b"x"),
            None,
        )
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: key.clone(),
            upload_id: upload.upload_id.clone(),
            max_parts: 10,
            part_number_marker: None,
        })
        .await
        .unwrap();
    assert_eq!(listing.parts.len(), 1);

    // The versioned write: a supersede moves the outgoing content into the
    // bucket's own block subtree (`blobs/<bucket>/<fan-out>/<hash>`), so
    // the layout below covers the block store too.
    storage
        .set_bucket_versioning(&b, State::Enabled)
        .await
        .unwrap();
    storage
        .put_object(&b, &"hello.txt".into(), body(b"hello again"))
        .await
        .unwrap();
    assert!(
        state.path().join("blobs/data").is_dir(),
        "the bucket's block subtree"
    );

    // The state dir: meta.redb, the multipart tree and the block store
    // (tmp/ appears on demand when the atomic writer stages a body).
    let mut allowed = vec!["meta.redb", "multipart", "blobs"];
    if state.path().join("tmp").exists() {
        allowed.push("tmp");
    }
    allowed.sort();
    assert_eq!(entries(state.path()), allowed);

    // No legacy layout anywhere.
    assert!(!state.path().join("buckets.json").exists());
    assert!(!state.path().join("meta").exists());
    let upload_dir = state.path().join("multipart/data").join(&upload.upload_id);
    let upload_entries = entries(&upload_dir);
    assert_eq!(
        upload_entries,
        ["part-1"],
        "no sidecars: {upload_entries:?}"
    );
}

/// Deleting `meta.redb` must not break the store: entries are recomputed
/// on demand (the metadata is derivable — meta-redb-spec §3).
#[tokio::test]
async fn deleting_meta_redb_self_heals() {
    let root = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let state_dir = state.path().to_path_buf();
    let storage = FsStorage::new(root.path(), fs_options_in(&state_dir)).unwrap();
    let b = bucket_name("data");
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .put_object(&b, &"a.txt".into(), body(b"hello"))
        .await
        .unwrap();
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    drop(storage);

    // Wipe the database (simulating corruption beyond repair), then reopen.
    fs::remove_file(state_dir.join("meta.redb")).await.unwrap();
    let storage = FsStorage::new(root.path(), fs_options_in(&state_dir)).unwrap();
    // The object is still served — the ETag is recomputed on demand.
    let head = storage.head_object(&b, &"a.txt".into()).await.unwrap();
    assert_eq!(head.size, 5);
    let content = storage.get_object(&b, &"a.txt".into(), None).await.unwrap();
    let body = read_body(content.body).await.unwrap();
    assert_eq!(body, b"hello");
}
