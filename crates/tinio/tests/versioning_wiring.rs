//! The composed-graph seam for `tinio`'s `versioning` feature.
//!
//! The feature's transitive set is spelled once — `tinio-server`'s optional
//! normal dependencies on `tinio-fs`/`tinio-mem`, whose only job is to carry
//! the backends' `versioning` features into the graph of every dependent (the
//! server's library never names a backend) — and every consumer reaches the
//! backends through it: the facade forwards to `tinio-cli` and
//! `tinio-server`, the CLI forwards to the server, and no manifest below that
//! re-spells a backend edge.
//!
//! Nothing about a compile of the default graph would notice that carrier
//! going away: the version operations are written against the `tinio-core`
//! contract, so they keep compiling either way, and a backend compiled
//! without its version machinery answers the contract's **default**
//! `AccessDenied` refusal for every version operation instead — a build that
//! ships the versioning surface wired to nothing. This file is the notice:
//! the round trip below runs against each backend exactly as the facade's
//! graph composes it, so a regression fails here instead of shipping.
//!
//! The facade's dev-dependency edges on `tinio-fs`/`tinio-mem` deliberately
//! name no features. Were they to enable `versioning` themselves — the
//! compensation this seam replaced — both tests would pass however the
//! carrier above them was broken, and there would be no seam left to check.

#![cfg(feature = "versioning")]

use tinio::{Storage, bucket, object};
use tinio_core::{
    acl::Acl,
    version::{ListParams, State},
};
use tinio_fs::{FsStorage, testing::fs_options};
use tinio_mem::MemoryStorage;
use tinio_util::testing::body;

/// The filesystem backend, composed into the facade's graph.
#[tokio::test]
async fn the_filesystem_backends_version_machinery_is_composed_in() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    assert_versioning_round_trip(&storage).await;
}

/// The in-memory backend, composed into the facade's graph.
#[tokio::test]
async fn the_memory_backends_version_machinery_is_composed_in() {
    let storage = MemoryStorage::new().unwrap();
    assert_versioning_round_trip(&storage).await;
}

/// A versioning round trip — the enable write, its read-back, and one
/// version-addressed listing — whose stripped answers are the contract's
/// defaults: the refusal class for the write and the listing, `Unversioned`
/// for the read-back. Each step names the default it must not have received,
/// so a stripped backend fails at the first one.
async fn assert_versioning_round_trip<S: Storage>(storage: &S) {
    let bucket = bucket::name("composed-graph").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .expect("the fixture bucket was refused");

    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect(
            "the enable write answered the contract's stripped default: the backend's version \
             machinery is not in this graph (`tinio-server`'s feature-carrying edges to \
             `tinio-fs`/`tinio-mem` are gone, or lost their `versioning` feature)",
        );
    assert_eq!(
        storage.bucket_versioning(&bucket).await.unwrap(),
        State::Enabled,
        "the versioning state written did not read back (a stripped backend answers `Unversioned`)"
    );

    let key = object::key("composed.txt").unwrap();
    storage
        .put_object(&bucket, &key, body(b"composed"))
        .await
        .expect("the write into a versioned bucket was refused");
    let listing = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: 100,
                ..ListParams::default()
            },
        )
        .await
        .expect("the version listing answered the contract's stripped default (`AccessDenied`)");
    assert_eq!(
        listing.entries.len(),
        1,
        "one write into an enabled bucket must list one version: {listing:?}"
    );
    assert_eq!(listing.entries[0].key, key);
    assert!(
        listing.entries[0].id.is_some(),
        "an enabled bucket's write must carry a version id: {listing:?}"
    );
    assert!(!listing.entries[0].is_delete_marker);
}
