//! `(bucket, upload_id, part_number)` → etag hex.

use redb::{ReadableTable, TableDefinition};

use crate::{
    _core::etag::ETag,
    error::Error,
    scan::drain_triple,
    table::{self, TableDef},
};

/// The per-table marker: the table definition for the shared handle arms.
#[doc(hidden)]
pub enum Def {}

impl TableDef for Def {
    type Key = (&'static str, &'static str, u32);
    type Value = &'static str;

    const DEF: TableDefinition<'static, Self::Key, Self::Value> = TableDefinition::new("parts");
}

/// Handle to the parts table (writable or read-only).
pub type Table<'txn, T = redb::Table<'txn, <Def as TableDef>::Key, <Def as TableDef>::Value>> =
    table::Table<'txn, Def, T>;

impl<'txn, T> table::Table<'txn, Def, T>
where
    T: ReadableTable<<Def as TableDef>::Key, <Def as TableDef>::Value>,
{
    /// Stored etag hex of one part, if present.
    pub fn get_hex(&self, bucket: &str, upload_id: &str, n: u32) -> Result<Option<String>, Error> {
        Ok(self
            .0
            .get((bucket, upload_id, n))?
            .map(|guard| guard.value().to_string()))
    }

    /// Page of `(part_number, etag_hex)` from `start`, capped at `max`,
    /// with a truncated flag (one lookahead past the page).
    pub fn list_from(
        &self,
        bucket: &str,
        upload_id: &str,
        start: u32,
        max: usize,
    ) -> Result<(Vec<(u32, String)>, bool), Error> {
        let iter = self.0.range((bucket, upload_id, start)..)?;
        let mut recorded = Vec::new();
        let mut truncated = false;
        for item in iter {
            let (k, v) = item?;
            let (b, id, n) = k.value();
            if b != bucket || id != upload_id {
                break;
            }
            if recorded.len() == max {
                truncated = true;
                break;
            }
            recorded.push((n, v.value().to_string()));
        }
        Ok((recorded, truncated))
    }
}

impl<'txn> table::Table<'txn, Def> {
    /// Upsert one part etag.
    pub fn put(&mut self, bucket: &str, upload_id: &str, n: u32, etag: &ETag) -> Result<(), Error> {
        let etag_hex = etag.as_str();
        self.0.insert((bucket, upload_id, n), etag_hex.as_str())?;
        Ok(())
    }

    /// Delete every part row of `bucket`.
    pub fn drain_bucket(&mut self, bucket: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, "", 0), |b, _, _| b == bucket)
    }

    /// Delete every part row of one upload.
    pub fn drain_upload(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        drain_triple(&mut self.0, (bucket, upload_id, 0), |b, id, _| {
            b == bucket && id == upload_id
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{etag, handle};

    #[test]
    fn part_rows_list_from_pagination_and_boundary() {
        let h = handle();
        let e1 = etag("d41d8cd98f00b204e9800998ecf8427e");
        let e2 = etag("900150983cd24fb0d6963f7d28e17f72");
        h.write(|txn| -> Result<(), Error> {
            let mut t = Table::open(txn)?;
            t.put("data", "u1", 1, &e1)?;
            t.put("data", "u1", 2, &e2)?;
            t.put("data", "u2", 1, &e1)?;
            t.put("other", "u9", 1, &e1)?;
            Ok(())
        })
        .unwrap();
        h.read(|txn| -> Result<(), Error> {
            let t = Table::open_readonly(txn)?;
            // A full page, no truncation; it stops before the sibling upload.
            let (page, truncated) = t.list_from("data", "u1", 0, 10)?;
            assert_eq!(page, [(1, e1.to_string()), (2, e2.to_string())]);
            assert!(!truncated);
            // A page max of 1 truncates when the second part is present.
            let (page, truncated) = t.list_from("data", "u1", 0, 1)?;
            assert_eq!(page, [(1, e1.to_string())]);
            assert!(truncated);
            // A start-into page resumes from the offset.
            let (page, _) = t.list_from("data", "u1", 2, 10)?;
            assert_eq!(page, [(2, e2.to_string())]);
            // A missing upload -> empty, no truncation.
            let (page, truncated) = t.list_from("data", "nope", 0, 10)?;
            assert!(page.is_empty() && !truncated);
            Ok(())
        })
        .unwrap();
    }
}
