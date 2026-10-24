//! Memory Object Lock. The shared Storage journeys live in
//! `tinio_util::testing::object_lock::storage`. This module is that suite's
//! row fixture and its one caller — mem has no journey of its own.

use async_trait::async_trait;

use crate::{
    _core::{acl::Acl, bucket, object, object_lock, storage, version},
    _store_redb::version_object_lock,
    _util::testing::object_lock::storage::{StorageLockFixture, assert_storage_object_lock},
    Error, MemoryStorage,
};

struct MemLockFixture;

#[async_trait]
impl StorageLockFixture for MemLockFixture {
    type Store = MemoryStorage;

    async fn open(&self) -> MemoryStorage {
        MemoryStorage::new().unwrap()
    }

    async fn plant_lock(
        &self,
        storage: &MemoryStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
        lock: &object_lock::Lock,
    ) {
        let (b, k, id) = (
            bucket.as_ref().as_str().to_owned(),
            key.as_ref().to_owned(),
            version::wire_of(id),
        );
        let lock = lock.clone();
        storage
            .db
            .write(move |txn| -> Result<(), Error> {
                version_object_lock::Table::open(txn)?.put(&b, &k, &id, &lock)?;
                Ok(())
            })
            .unwrap();
    }

    async fn clear_lock(
        &self,
        storage: &MemoryStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) {
        let (b, k, id) = (
            bucket.as_ref().as_str().to_owned(),
            key.as_ref().to_owned(),
            version::wire_of(id),
        );
        storage
            .db
            .write(move |txn| -> Result<(), Error> {
                version_object_lock::Table::open(txn)?.remove(&b, &k, &id)?;
                Ok(())
            })
            .unwrap();
    }

    async fn lock_present(
        &self,
        storage: &MemoryStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> bool {
        let (b, k, id) = (
            bucket.as_ref().as_str().to_owned(),
            key.as_ref().to_owned(),
            version::wire_of(id),
        );
        storage
            .db
            .read(move |txn| -> Result<bool, Error> {
                Ok(version_object_lock::Table::open_readonly(txn)?
                    .get(&b, &k, &id)?
                    .is_some())
            })
            .unwrap()
    }

    async fn stored_lock(
        &self,
        storage: &MemoryStorage,
        bucket: &bucket::Name,
        key: &object::Key,
        id: Option<&version::Id>,
    ) -> Option<object_lock::Lock> {
        let (b, k, id) = (
            bucket.as_ref().as_str().to_owned(),
            key.as_ref().to_owned(),
            version::wire_of(id),
        );
        storage
            .db
            .read(move |txn| -> Result<Option<object_lock::Lock>, Error> {
                Ok(version_object_lock::Table::open_readonly(txn)?.get(&b, &k, &id)?)
            })
            .unwrap()
    }

    async fn fail_versioning_step(
        &self,
        storage: &MemoryStorage,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<(), storage::Error> {
        storage
            .ops()
            .create_bucket_with_lock(
                bucket,
                None,
                &Acl::default_private(None),
                config,
                || async { Err(storage::corrupt("the versioning step failed")) },
            )
            .await
    }
}

#[tokio::test]
async fn the_storage_object_lock_journeys() {
    assert_storage_object_lock::<MemoryStorage, _>(&MemLockFixture).await;
}
