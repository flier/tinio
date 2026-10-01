//! `(bucket, upload_id, part_number)` → `(algorithm wire name, base64
//! value)` — one part's computed checksum (spec 2026-08-31).

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::checksum,
    error::Error,
    scan::{drain_triple, has_prefix_triple},
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    type Value = (&'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("part_checksums");
}

/// Handle to the part-checksums table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The stored per-part checksum. `None` is a missing row or a
    /// domain-invalid wire (self-healing — the part is listed without a
    /// checksum, F07).
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<checksum::Part>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, part_number))?
            .and_then(|v| checksum::Part::from_wire_opt(v.value().0, v.value().1)))
    }

    /// Whether `upload_id` has any checksum row (the list-parts "rows may
    /// race" probe — one contiguous block from `(bucket, upload_id, 0)`).
    pub fn has_upload(&self, bucket: &str, upload_id: &str) -> Result<bool, Error> {
        has_prefix_triple(&self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Insert or replace the part's checksum (encoded here).
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        part: &checksum::Part,
    ) -> Result<(), Error> {
        let (algorithm, value) = part.to_wire();
        self.0
            .insert((bucket, upload_id, part_number), (algorithm, value))?;
        Ok(())
    }

    /// The digest-slot write of a part upload: put the tee's digest, or
    /// remove a stale row from a previous upload of this part number (it
    /// would corrupt the Complete composition).
    pub fn set(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        part: Option<&checksum::Part>,
    ) -> Result<(), Error> {
        match part {
            Some(part) => self.put(bucket, upload_id, part_number, part),
            None => self.remove(bucket, upload_id, part_number),
        }
    }

    /// Remove the part's checksum row (idempotent — re-upload clears the
    /// stale value).
    pub fn remove(&mut self, bucket: &str, upload_id: &str, part_number: u32) -> Result<(), Error> {
        self.0.remove((bucket, upload_id, part_number))?;
        Ok(())
    }

    /// Delete every row of one upload (mirror [`crate::part::Table::drain_upload`]).
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
    use super::*;
    use crate::{
        _core::checksum::{Algorithm, Part, Value},
        testutil::handle,
    };

    #[test]
    fn part_checksum_rows_round_trip_self_heal_and_drain() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut pc = Table::open(txn)?;
            assert!(!pc.has_upload("data", "u1")?);
            pc.put(
                "data",
                "u1",
                1,
                &Part {
                    algorithm: Algorithm::Crc32,
                    value: Value("NhCmhg==".into()),
                },
            )?;
            pc.put(
                "data",
                "u1",
                2,
                &Part {
                    algorithm: Algorithm::Sha256,
                    value: Value("BB==".into()),
                },
            )?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let pc = Table::open_readonly(txn)?;
            assert!(pc.has_upload("data", "u1")?);
            assert!(!pc.has_upload("data", "u2")?);
            assert_eq!(
                pc.get("data", "u1", 1)?,
                Some(Part {
                    algorithm: Algorithm::Crc32,
                    value: Value("NhCmhg==".into()),
                })
            );
            assert!(pc.get("data", "u1", 3)?.is_none());
            Ok(())
        })
        .unwrap();
        // A garbage part-checksum wire self-heals on the table accessor.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?
                .insert(("data", "u1", 1), ("BLAKE3", "AAAA"))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let pc = Table::open_readonly(txn)?;
            assert!(
                pc.get("data", "u1", 1)?.is_none(),
                "a corrupt part checksum wire is no checksum"
            );
            assert!(pc.has_upload("data", "u1")?, "the row itself remains");
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            let mut pc = Table::open(txn)?;
            pc.remove("data", "u1", 1)?;
            pc.drain_upload("data", "u1")?;
            Ok(())
        })
        .unwrap();
        assert!(
            !h.read(|txn| Table::open_readonly(txn)?.has_upload("data", "u1"))
                .unwrap()
        );
    }

    #[test]
    fn part_checksum_set_replaces_and_clears_the_slot() {
        let h = handle();
        let part = Part {
            algorithm: Algorithm::Crc32,
            value: Value("NhCmhg==".into()),
        };
        h.write(|txn| -> Result<(), Error> {
            let mut pc = Table::open(txn)?;
            // None on a missing row: a no-op (the part carried no checksum).
            pc.set("data", "u1", 1, None)?;
            // Some writes the digest.
            pc.set("data", "u1", 1, Some(&part))?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(
                Table::open_readonly(txn)?.get("data", "u1", 1)?,
                Some(part.clone())
            );
            Ok(())
        })
        .unwrap();
        // The None arm clears the stale row from a previous upload of this
        // part number (the digest-slot discipline, F-spec 2026-08-31).
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.set("data", "u1", 1, None)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let pc = Table::open_readonly(txn)?;
            assert!(pc.get("data", "u1", 1)?.is_none());
            assert!(!pc.has_upload("data", "u1")?);
            Ok(())
        })
        .unwrap();
        // And the Some arm re-writes after the clear.
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.set("data", "u1", 1, Some(&part))?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            assert_eq!(Table::open_readonly(txn)?.get("data", "u1", 1)?, Some(part));
            Ok(())
        })
        .unwrap();
    }
}
