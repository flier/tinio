//! `(bucket, upload_id, part_number)` → the part's own write stamp, in unix
//! milliseconds — a sibling of `parts`, the way `part_checksums` is a sibling
//! of it (spec 2026-08-31, the precedent): one more per-part datum takes its
//! own table under the **same key shape** rather than a wider `parts` value,
//! so the ETag row's value keeps its pinned shape and a database written
//! before the datum existed keeps reading.
//!
//! The stamp is the instant the store last wrote that part, which is what a
//! listed part answers as its `LastModified`
//! ([`crate::_core::meta::upload::PartRow::written_at`]'s rule). A part with
//! no row here — an older database, or a part written before this round —
//! reads `None`, and the projection then answers the upload's creation
//! instant: the fallback is today's answer rather than a new one.
//!
//! The value is milliseconds, the wire the PostgreSQL store's
//! `parts.modified_at` column carries, and `0` is that wire's no-stamp value:
//! a zero reads as no stamp, exactly as a missing row does.

use std::time::SystemTime;

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::{from_nanos, to_nanos},
    error::Error,
    scan::{drain_triple, has_prefix_triple},
    table::{self, TableDef},
};

/// Nanoseconds per millisecond — the stored value's unit against
/// [`to_nanos`]/[`from_nanos`].
const NANOS_PER_MILLI: u64 = 1_000_000;

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    /// The instant the store wrote the part, in unix milliseconds (`0` = no
    /// stamp).
    type Value = u64;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("part_stamps");
}

/// Handle to the part-stamps table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The stored write stamp of one part. `None` is a missing row (an older
    /// database, or a part written before the stamp existed) or the wire's own
    /// no-stamp value; both take the caller's fallback.
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<SystemTime>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, part_number))?
            .map(|value| value.value())
            .filter(|millis| *millis != 0)
            // The stored wire is a millisecond count: the read answers that
            // millisecond's start, and a count no `u64` nanosecond value can
            // hold saturates rather than overflowing a corrupt row into a
            // panic (the same arm the pg store's stamp read takes).
            .map(|millis| from_nanos(millis.saturating_mul(NANOS_PER_MILLI))))
    }

    /// Whether `upload_id` has any stamp row (the list-parts "rows may race"
    /// probe — one contiguous block from `(bucket, upload_id, 0)`).
    pub fn has_upload(&self, bucket: &str, upload_id: &str) -> Result<bool, Error> {
        has_prefix_triple(&self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert the part's write stamp (encoded here). An instant before the
    /// epoch encodes as the wire's no-stamp `0` — the read then takes the
    /// caller's fallback (a store never writes one: its clock is now).
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        written_at: SystemTime,
    ) -> Result<(), Error> {
        self.0.insert(
            (bucket, upload_id, part_number),
            to_nanos(written_at) / NANOS_PER_MILLI,
        )?;
        Ok(())
    }

    /// Remove the part's stamp row (idempotent).
    pub fn remove(&mut self, bucket: &str, upload_id: &str, part_number: u32) -> Result<(), Error> {
        self.0.remove((bucket, upload_id, part_number))?;
        Ok(())
    }

    /// Delete every row of one upload (mirror
    /// [`crate::part::Table::drain_upload`]: a stamp never outlives the part
    /// upload it stamps).
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::testutil::handle;

    #[test]
    fn part_stamp_rows_round_trip_and_drain() {
        let h = handle();
        // Millisecond-exact instants: the stored wire is a millisecond count,
        // so this is what the round trip must answer back exactly.
        let first = SystemTime::UNIX_EPOCH + Duration::from_millis(1_500);
        let second = first + Duration::from_secs(2);
        h.write(|txn| -> Result<(), Error> {
            let mut stamps = Table::open(txn)?;
            assert!(!stamps.has_upload("data", "u1")?);
            stamps.put("data", "u1", 1, first)?;
            stamps.put("data", "u1", 2, second)?;
            stamps.put("other", "u9", 1, first)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let stamps = Table::open_readonly(txn)?;
            assert!(stamps.has_upload("data", "u1")?);
            assert!(!stamps.has_upload("data", "u2")?);
            assert_eq!(stamps.get("data", "u1", 1)?, Some(first));
            assert_eq!(stamps.get("data", "u1", 2)?, Some(second));
            assert!(
                stamps.get("data", "u1", 3)?.is_none(),
                "a row-less part has no stamp, and the caller's fallback stands"
            );
            Ok(())
        })
        .unwrap();
        // The wire's own no-stamp value reads as no stamp, while the row
        // itself remains (the pg store's `modified_at = 0` row means the
        // same thing).
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(("data", "u2", 1), 0u64)
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let stamps = Table::open_readonly(txn)?;
            assert!(stamps.get("data", "u2", 1)?.is_none());
            assert!(stamps.has_upload("data", "u2")?, "the row itself remains");
            Ok(())
        })
        .unwrap();
        // remove is the single-row clear, drain_upload the upload's rows and
        // drain_bucket the bucket's — a sibling bucket's rows survive.
        h.write(|txn| -> Result<(), Error> {
            let mut stamps = Table::open(txn)?;
            stamps.remove("data", "u1", 1)?;
            stamps.remove("data", "u1", 1)?;
            stamps.drain_upload("data", "u1")?;
            stamps.drain_bucket("data")?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let stamps = Table::open_readonly(txn)?;
            assert!(
                !stamps.has_upload("data", "u1")? && !stamps.has_upload("data", "u2")?,
                "no stamp row outlives its upload"
            );
            assert_eq!(
                stamps.get("other", "u9", 1)?,
                Some(first),
                "another bucket's rows survive the teardown"
            );
            Ok(())
        })
        .unwrap();
    }
}
