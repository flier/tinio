//! Reserved-path behavior over the data plane (task T026, FR-020), ported
//! from `tinio-server/tests/reserved_paths.rs`.
//!
//! Any-depth `.tinio` segments: write → AccessDenied, read → NoSuchKey,
//! listings skip. The nested-root scenario: an inner server's root placed
//! inside an outer bucket is never served by the outer server. The fs
//! out-of-band steps (`I write … in the served root`) build the inner
//! state directly on disk, not through the S3 API.

use cucumber::{given, then};
use tokio::fs;

/// Out-of-band filesystem write into the served root (fs-only): places an
/// inner server's state or a foreign bucket entry directly on disk.
#[given(expr = "I write {string} to {string} in the served root")]
async fn write_in_root(world: &mut super::World, text: String, rel: String) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    let path = root.join(rel);
    fs::create_dir_all(path.parent().expect("relative path has a parent"))
        .await
        .unwrap();
    fs::write(&path, text.as_bytes()).await.unwrap();
}

/// Create a top-level directory in the served root without the S3 API
/// (US1-AS1: an existing directory must appear as a bucket in
/// ListBuckets — the out-of-band mirror semantics of FR-001/FR-002).
#[given(expr = "I create the directory {string} in the served root")]
async fn dir_in_root(world: &mut super::World, name: String) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    fs::create_dir_all(root.join(&name)).await.unwrap();
}

/// A directory link inside the served root (unix symlink, Windows
/// junction — `mklink /J` needs no Developer Mode, unlike `symlink_dir`)
/// pointing at a directory next to the served root. The target must not
/// be inside the root, or the link could not prove the escape policy.
/// The default `follow_symlinks = false` policy must refuse access
/// resolving through it and exclude it from listings.
#[given(regex = r#"I create a directory link "([^"]+)" in the served root"#)]
async fn link_in_root(world: &mut super::World, rel: String) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    let parent = root.parent().expect("served root has a parent dir");
    let link = root.join(&rel);
    fs::create_dir_all(link.parent().expect("relative path has a parent"))
        .await
        .unwrap();
    // A sibling directory as the target: same filesystem, outside the root.
    let target = parent.join(format!("{}-outside", rel.replace('/', "_")));
    fs::create_dir_all(&target).await.unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &link).unwrap();
    #[cfg(windows)]
    {
        // `mklink` parses the destination with cmd's tokenizer: normalize
        // to the native separator first (a forward slash reads as a switch).
        let dst = link.to_string_lossy().replace('/', "\\");
        let src = target.to_string_lossy().replace('/', "\\");
        let status = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J", &dst, &src])
            .status()
            .expect("run mklink");
        assert!(
            status.success(),
            "mklink /J failed for {link:?} -> {target:?}"
        );
    }
}

/// The last object listing must not mention a symlink entry (the default
/// `follow_symlinks = false` policy excludes links — a link inside a
/// bucket cannot escape the storage root).
#[then(expr = "the listing omits {string}")]
async fn listing_omits(world: &mut super::World, entry: String) {
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    assert!(
        !text.contains(&entry),
        "symlink entry {entry:?} leaked into the listing: {text}"
    );
}

/// The denied write must not have touched the reserved file (the old
/// test's `fs::read(...) == b"secret"`).
#[then(expr = "the file {string} in the served root contains {string}")]
async fn file_contains(world: &mut super::World, rel: String, text: String) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    let contents = fs::read(root.join(&rel))
        .await
        .expect("read file in served root");
    assert_eq!(contents, text.as_bytes(), "content of {rel} mismatch");
}

/// The last response was an empty object listing that never mentions the
/// reserved entries (the old test's `!contains(".tinio")` plus zero
/// `<Key>` entries).
#[then("the listing is empty and omits the reserved entries")]
async fn listing_empty_omits_reserved(world: &mut super::World) {
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    assert!(
        !text.contains(".tinio"),
        "reserved entries leaked into the listing: {text}"
    );
    assert_eq!(
        super::common::count_tag(&world.last.body, "<Key>"),
        0,
        "expected no keys: {text}"
    );
}

/// The last response was an object listing that never mentions the
/// reserved entries (the old test's `!contains(".tinio")`).
#[then("the listing omits the reserved entries")]
async fn listing_omits_reserved(world: &mut super::World) {
    let text = String::from_utf8_lossy(&world.last.body).into_owned();
    assert!(
        !text.contains(".tinio"),
        "reserved entries leaked into the listing: {text}"
    );
}

/// The served root holds exactly the reserved state dir and the scenario's
/// bucket — the denied requests wrote nothing else.
#[then("the served root contains only the state dir and the bucket")]
async fn root_entries(world: &mut super::World) {
    let root = world
        .server
        .as_ref()
        .expect("server running")
        .root()
        .expect("fs-backed server root");
    assert_eq!(
        super::common::sorted_entries(root).await,
        [".tinio", "data"],
        "unexpected root entries"
    );
}
