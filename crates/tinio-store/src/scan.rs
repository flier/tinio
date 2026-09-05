//! Prefix range scans over redb tables.

use redb::{ReadableTable, Table, Value};

use crate::error::Error;

/// Delete every entry of `table` whose key keeps matching `keep`, walking
/// from `lower` up and stopping at the first mismatch (keys are ordered —
/// one bucket's entries are contiguous, so the scan costs O(range) plus one
/// lookahead). Collect the owned keys first, then remove (redb has no bulk
/// range delete).
///
/// No exclusive upper bound is used: tuple comparison is element-wise in
/// byte order, and no valid `&str` sorts above every continuation of a
/// prefix (e.g. `bucket + '\u{10FFFF}'` still sorts below `bucket-x`, as
/// `'x'` < `F4`). The mismatch break is the only correct boundary.
macro_rules! drain_impl {
    ($name:ident, $key:ty, $pat:pat, $($arg:ident: $arg_ty:ty),+ => $collect:expr, $remove_pat:pat, $remove:expr) => {
        pub fn $name<'txn, V: Value + 'static>(
            table: &mut Table<'txn, $key, V>,
            lower: $key,
            mut keep: impl FnMut($($arg_ty),+) -> bool,
        ) -> Result<(), Error> {
            // Collect the owned keys first, then remove (redb has no bulk
            // range delete); the mismatch break is the interval boundary.
            let mut iter = table.range(lower..)?;
            let mut keys = Vec::new();
            for item in &mut iter {
                let (k, _) = item?;
                let $pat = k.value();
                if !keep($($arg),+) {
                    break;
                }
                keys.push($collect);
            }
            for $remove_pat in keys {
                table.remove($remove)?;
            }
            Ok(())
        }
    };
}

drain_impl!(
    drain_pair,
    (&str, &str),
    (a, b),
    a: &str,
    b: &str
    =>
    (a.to_string(), b.to_string()),
    (a, b),
    (a.as_str(), b.as_str())
);

drain_impl!(
    drain_triple,
    (&str, &str, u32),
    (a, b, n),
    a: &str,
    b: &str,
    n: u32
    =>
    (a.to_string(), b.to_string(), n),
    (a, b, n),
    (a.as_str(), b.as_str(), n)
);

/// Walk `table` from `lower` while `keep` holds and call `visit` on each
/// `(key, value)` (the mismatch break is the interval boundary — see
/// [`drain_pair`]). The visit error type is generic so backends can
/// accumulate their own errors (fs's strict walk fails with `CorruptMeta`);
/// the range/read errors convert through `E: From<[`crate::error::Error`]>`.
pub fn for_each_pair<V, T, F, E>(
    table: &T,
    lower: (&str, &str),
    mut keep: impl FnMut(&str, &str) -> bool,
    mut visit: F,
) -> Result<(), E>
where
    V: Value + 'static,
    T: ReadableTable<(&'static str, &'static str), V>,
    F: FnMut(&str, &str, V::SelfType<'_>) -> Result<(), E>,
    E: From<Error>,
{
    for item in table.range(lower..).map_err(Error::from)? {
        let (k, v) = item.map_err(Error::from)?;
        let (a, b) = k.value();
        if !keep(a, b) {
            break;
        }
        visit(a, b, v.value())?;
    }
    Ok(())
}

/// Whether any row at or after `lower` belongs to `keep`'s block — the
/// first key at or after the lower bound is in the block iff any exist
/// (the mismatch break is the interval boundary).
pub fn has_prefix_pair<V, T, E>(
    table: &T,
    lower: (&str, &str),
    mut keep: impl FnMut(&str, &str) -> bool,
) -> Result<bool, E>
where
    V: Value + 'static,
    T: ReadableTable<(&'static str, &'static str), V>,
    E: From<Error>,
{
    let mut iter = table.range(lower..).map_err(Error::from)?;
    match iter.next() {
        Some(item) => {
            let (k, _) = item.map_err(Error::from)?;
            let (a, b) = k.value();
            Ok(keep(a, b))
        }
        None => Ok(false),
    }
}

/// Triple-key counterpart of [`has_prefix_pair`] — `(bucket, upload_id)`
/// / `(bucket, key)` prefix probes over the `(bucket, x, part_number)`
/// key shape.
pub fn has_prefix_triple<V, T, E>(
    table: &T,
    lower: (&str, &str, u32),
    mut keep: impl FnMut(&str, &str, u32) -> bool,
) -> Result<bool, E>
where
    V: Value + 'static,
    T: ReadableTable<(&'static str, &'static str, u32), V>,
    E: From<Error>,
{
    let mut iter = table.range(lower..).map_err(Error::from)?;
    match iter.next() {
        Some(item) => {
            let (k, _) = item.map_err(Error::from)?;
            let (a, b, n) = k.value();
            Ok(keep(a, b, n))
        }
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

    use super::*;

    fn mem_db() -> Database {
        Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap()
    }

    #[test]
    fn drain_pair_removes_only_the_matching_prefix() {
        let table: TableDefinition<(&'static str, &'static str), &'static str> =
            TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "a"), "1").unwrap();
                t.insert(("data", "b"), "2").unwrap();
                t.insert(("other", "c"), "3").unwrap();
            }
            txn.commit().unwrap();
        }
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                drain_pair(&mut t, ("data", ""), |b, _| b == "data").unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        let keys: Vec<(String, String)> = t
            .iter()
            .unwrap()
            .map(|r| {
                let (k, _) = r.unwrap();
                let (a, b) = k.value();
                (a.to_string(), b.to_string())
            })
            .collect();
        assert_eq!(keys, [("other".to_string(), "c".to_string())]);
    }

    #[test]
    fn for_each_pair_stops_before_a_longer_prefix_key() {
        // Bucket "data" must never leak into "data-x": the scan starts
        // at the lower bound and stops at the first key failing `keep`
        // (the no-exclusive-upper-bound ruling, redb-notes pit 14).
        let table: TableDefinition<(&'static str, &'static str), u64> = TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "a"), 1).unwrap();
                t.insert(("data", "x"), 2).unwrap();
                t.insert(("data-x", "b"), 3).unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        let mut seen: Vec<(String, String)> = Vec::new();
        for_each_pair(
            &t,
            ("data", ""),
            |b, _| b == "data",
            |b, k, _| -> Result<(), Error> {
                seen.push((b.to_string(), k.to_string()));
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            seen,
            [("data".into(), "a".into()), ("data".into(), "x".into())]
        );
    }

    #[test]
    fn has_prefix_pair_probes_only_the_first_row() {
        let table: TableDefinition<(&'static str, &'static str), u64> = TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "a"), 1).unwrap();
                t.insert(("data-x", "b"), 2).unwrap();
                t.insert(("zeta", "z"), 3).unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        // A matching first key -> true; a non-matching one (the row at the
        // lower bound belongs elsewhere) -> false — one row, no walk.
        assert!(has_prefix_pair::<_, _, Error>(&t, ("data", ""), |b, _| b == "data").unwrap());
        assert!(!has_prefix_pair::<_, _, Error>(&t, ("dat", ""), |b, _| b == "dat").unwrap());
        // An empty block (below every key) -> false.
        assert!(!has_prefix_pair::<_, _, Error>(&t, ("aaa", ""), |b, _| b == "aaa").unwrap());
    }

    #[test]
    fn has_prefix_triple_stops_before_a_longer_upload_prefix() {
        let table: TableDefinition<(&'static str, &'static str, u32), u64> =
            TableDefinition::new("t");
        let db = mem_db();
        {
            let txn = db.begin_write().unwrap();
            {
                let mut t = txn.open_table(table).unwrap();
                t.insert(("data", "u1", 1), 1).unwrap();
                t.insert(("data", "u2", 1), 2).unwrap();
            }
            txn.commit().unwrap();
        }
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(table).unwrap();
        assert!(
            has_prefix_triple::<_, _, Error>(&t, ("data", "u1", 0), |b, id, _| {
                b == "data" && id == "u1"
            })
            .unwrap()
        );
        assert!(
            !has_prefix_triple::<_, _, Error>(&t, ("data", "u3", 0), |b, id, _| {
                b == "data" && id == "u3"
            })
            .unwrap()
        );
    }
}
