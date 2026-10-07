//! `(bucket, key, part_number)` → `(size, algorithm wire name or "",
//! base64 checksum value or "")` — the completed object's retained part
//! list (spec 2026-08-31, GetObjectAttributes ObjectParts): the parts
//! the object was composed of at its last multipart completion, in part
//! order, with the stored per-part checksums. `""` marks a part stored
//! without a checksum. The key shape mirrors `PARTS` (same `(bucket,
//! upload-id/object-key, part_number)` ordering).

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::checksum,
    error::Error,
    scan::drain_triple,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    type Value = (u64, &'static str, &'static str);

    const DEF: TableDefinition<'static, Self::Key, Self::Value> =
        TableDefinition::new("object_parts");
}

/// Handle to the object-parts table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

/// One retained part of a completed object: part number, size, and the
/// stored per-part checksum (`None` = none was computed, or the stored
/// wire is domain-invalid — self-healing, F07).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    /// Part number (`1..=10000` at the API; the table stores `u32`).
    pub part_number: u32,
    /// Part size in bytes.
    pub size: u64,
    /// The stored per-part checksum (`None` when none, or when the stored
    /// wire is domain-invalid — self-healing like the etag).
    pub checksum: Option<checksum::Part>,
}

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// The key's rows in part-number order (owned — the guard cannot
    /// outlive the closure). A domain-invalid checksum wire self-heals
    /// to `checksum: None` (F07 — the part is still listed).
    pub fn list(&self, bucket: &str, key: &str) -> Result<Vec<Stored>, Error> {
        let mut out = Vec::new();
        for item in self.0.range((bucket, key, 0)..)? {
            let (k, v) = item?;
            let (b, stored_key, n) = k.value();
            if b != bucket || stored_key != key {
                break;
            }
            let (size, algorithm, value) = v.value();
            out.push(Stored {
                part_number: n,
                size,
                checksum: checksum::Part::from_wire_opt(algorithm, value),
            });
        }
        Ok(out)
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one part row (encoded here).
    pub fn put(&mut self, bucket: &str, key: &str, part: &Stored) -> Result<(), Error> {
        let (algorithm, value) = match part.checksum.as_ref() {
            Some(p) => p.to_wire(),
            None => ("", ""),
        };
        self.0.insert(
            (bucket, key, part.part_number),
            (part.size, algorithm, value),
        )?;
        Ok(())
    }

    /// Delete every row of `key` — an overwrite/copy/delete must not
    /// leave a completed object's stale parts behind (the new object has
    /// none). Idempotent.
    pub fn remove_key(&mut self, bucket: &str, key: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, key, 0), |b, k, _| {
            b == bucket && k == key
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
    fn object_part_rows_list_in_order_and_remove_key() {
        let h = handle();
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put(
                "data",
                "big.bin",
                &Stored {
                    part_number: 1,
                    size: 100,
                    checksum: Some(Part {
                        algorithm: Algorithm::Crc32,
                        value: Value("AA==".into()),
                    }),
                },
            )?;
            t.put(
                "data",
                "big.bin",
                &Stored {
                    part_number: 2,
                    size: 200,
                    checksum: None,
                },
            )?;
            t.put(
                "data",
                "big.bin",
                &Stored {
                    part_number: 3,
                    size: 50,
                    checksum: Some(Part {
                        algorithm: Algorithm::Sha256,
                        value: Value("BB==".into()),
                    }),
                },
            )?;
            t.put(
                "data",
                "other.bin",
                &Stored {
                    part_number: 1,
                    size: 1,
                    checksum: None,
                },
            )?;
            // A garbage checksum wire self-heals: the part is listed without
            // a checksum (F07).
            t.insert(("data", "big.bin", 4), (10u64, "BLAKE3", "AA=="))
                .map_err(Error::from)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            let rows = t.list("data", "big.bin")?;
            assert_eq!(
                rows,
                vec![
                    Stored {
                        part_number: 1,
                        size: 100,
                        checksum: Some(Part {
                            algorithm: Algorithm::Crc32,
                            value: Value("AA==".into()),
                        }),
                    },
                    Stored {
                        part_number: 2,
                        size: 200,
                        checksum: None,
                    },
                    Stored {
                        part_number: 3,
                        size: 50,
                        checksum: Some(Part {
                            algorithm: Algorithm::Sha256,
                            value: Value("BB==".into()),
                        }),
                    },
                    Stored {
                        part_number: 4,
                        size: 10,
                        checksum: None,
                    },
                ]
            );
            // A different key's rows do not bleed in.
            assert_eq!(t.list("data", "other.bin")?.len(), 1);
            Ok(())
        })
        .unwrap();
        h.write(|txn| -> Result<(), Error> {
            Table::open(txn)?.remove_key("data", "big.bin")?;
            Ok(())
        })
        .unwrap();
        assert!(
            h.read(|txn| Table::open_readonly(txn)?.list("data", "big.bin"))
                .unwrap()
                .is_empty()
        );
    }
}
