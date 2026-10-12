//! The multipart completion's commit: assemble, publish, record, and take the
//! publish back when the record does not land.

use std::time::SystemTime;

use futures::stream::{self, StreamExt};
use tinio_core::{
    ETag,
    acl::{Acl, OwnerId},
    bucket::Name,
    checksum,
    content::{ContentKey, ObjectContent, stage_body},
    meta::{
        ObjectMeta, UploadMeta,
        object::{PartRow, Row},
    },
    object,
    storage::{self, BodyStream},
};

#[cfg(feature = "versioning")]
use crate::version::OpenedWrite;
use crate::{_store_pg::content::LiveGeneration, Error, storage::PgStorage};
impl PgStorage {
    /// Assemble `bodies` under `key`, publish them, and record the object.
    ///
    /// The version branch opens before the publish. A failure of the row write
    /// or the slot move afterwards takes the publish back: a recording bucket
    /// through the engine's versioned undo, any other bucket by restoring the
    /// content head to the generation that was live before this call.
    #[expect(
        clippy::too_many_arguments,
        reason = "the completion's commit is one assembly: the parts, the composed etag, the checksum and the upload's tags, owner and ACL"
    )]
    pub(crate) async fn commit_completed_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        bodies: Vec<BodyStream>,
        retained: Vec<PartRow>,
        etag: ETag,
        checksum: Option<checksum::Recorded>,
        tags: object::Tags,
        owner: Option<OwnerId>,
        acl: Acl,
    ) -> Result<object::Info, Error> {
        #[cfg(feature = "versioning")]
        let write = self.open_put_write(bucket, key).await?;
        #[cfg(feature = "versioning")]
        let before = if write.records() {
            ObjectMeta::get(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
        } else {
            None
        };
        // What the failure paths below put back: the version plane's write and
        // the row read before the publish, or nothing at all in a build with no
        // version plane — whose rollback restores the live generation alone.
        #[cfg(feature = "versioning")]
        let undo = (&write, before.as_ref());
        #[cfg(not(feature = "versioning"))]
        let undo = ();

        let content_key = ContentKey::Object(key.clone());
        let previous = self
            .store
            .live_generation(bucket, &content_key)
            .await
            .map_err(storage::Error::from)?;
        let body: BodyStream = Box::pin(stream::iter(bodies).flatten());
        let mut staged = self
            .store
            .stage(bucket, &content_key)
            .await
            .map_err(storage::Error::from)?;
        stage_body(&mut staged, body).await?;
        // The publish answers the generation it committed, so the undo below keeps that value
        // instead of re-reading the head it just moved.
        let published = self
            .store
            .publish_generation(staged, None)
            .await
            .map_err(storage::Error::from)?;
        let entry = self
            .store
            .stat(bucket, key)
            .await
            .map_err(storage::Error::from)?
            .ok_or_else(|| storage::no_such_key(key))?;

        let mtime = entry.mtime.unwrap_or_else(SystemTime::now);
        let row = Row {
            etag: etag.clone(),
            size: entry.len,
            mtime,
            identity: entry.identity,
            tags,
            checksum,
            owner,
            acl,
        };
        if let Err(err) = ObjectMeta::put_with_parts(&*self.store, bucket, key, &row, &retained)
            .await
            .map_err(Error::from)
        {
            self.roll_back_completion(bucket, key, &content_key, published, previous, undo)
                .await?;
            return Err(err);
        }
        #[cfg(feature = "versioning")]
        if let Err(err) = self.close_put_write(bucket, key, &write).await {
            self.roll_back_completion(bucket, key, &content_key, published, previous, undo)
                .await?;
            return Err(err);
        }

        let stored = UploadMeta::list_parts(&*self.store, bucket, upload_id, 1.into())
            .await
            .map_err(storage::Error::from)?;
        self.store
            .drain_upload_checked(bucket, key, upload_id)
            .await
            .map_err(storage::Error::from)?;
        let removed: Vec<ContentKey> = stored
            .iter()
            .map(|part| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: part.number.into(),
            })
            .collect();
        ObjectContent::remove_many(&*self.store, bucket, &removed)
            .await
            .map_err(storage::Error::from)?;

        #[cfg(feature = "versioning")]
        let (version_id, records_versions) = (write.identity(), write.records());
        #[cfg(not(feature = "versioning"))]
        let (version_id, records_versions) = (None, false);
        Ok(object::Info {
            key: key.clone(),
            size: row.size,
            last_modified: mtime,
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
            owner: row.owner,
            acl: row.acl,
            version_id,
            bucket_records_versions: records_versions,
        })
    }

    /// Undo the publish a failed completion committed: the version plane's
    /// versioned undo (which falls back to the live-generation restore for a
    /// bucket that records no versions).
    #[cfg(feature = "versioning")]
    async fn roll_back_completion(
        &self,
        bucket: &Name,
        key: &object::Key,
        content_key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
        undo: (&OpenedWrite, Option<&Row>),
    ) -> Result<(), Error> {
        let (write, before) = undo;
        self.take_back(bucket, key, content_key, published, previous, write, before)
            .await
    }

    /// [`roll_back_completion`](Self::roll_back_completion) for a build with no
    /// version plane: the publish is undone by putting the live generation back
    /// to the one it moved away from.
    #[cfg(not(feature = "versioning"))]
    async fn roll_back_completion(
        &self,
        bucket: &Name,
        _key: &object::Key,
        content_key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
        _undo: (),
    ) -> Result<(), Error> {
        self.restore_published_live(bucket, content_key, published, previous)
            .await
    }

    /// Put the key back to the generation that was live before this completion's
    /// publish — the rollback a bucket that records no versions runs, and the
    /// fallback a recording bucket's versioned undo leaves it. A recording
    /// bucket's own undo lives with the version plane (`take_back`).
    pub(crate) async fn restore_published_live(
        &self,
        bucket: &Name,
        content_key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
    ) -> Result<(), Error> {
        self.store
            .restore_live(bucket, content_key, published, previous)
            .await
            .map_err(storage::Error::from)?;
        Ok(())
    }
}
