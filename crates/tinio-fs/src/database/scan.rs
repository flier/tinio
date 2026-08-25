//! Prefix range scans over redb tables.

use redb::{ReadableTable, Table, Value};

use super::error::Error;

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
        pub(crate) fn $name<'txn, V: Value + 'static>(
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
/// [`drain_pair`]).
pub(crate) fn for_each_pair<V, T, F>(
    table: &T,
    lower: (&str, &str),
    mut keep: impl FnMut(&str, &str) -> bool,
    mut visit: F,
) -> Result<(), Error>
where
    V: Value + 'static,
    T: ReadableTable<(&'static str, &'static str), V>,
    F: FnMut(&str, &str, V::SelfType<'_>) -> Result<(), Error>,
{
    for item in table.range(lower..)? {
        let (k, v) = item?;
        let (a, b) = k.value();
        if !keep(a, b) {
            break;
        }
        visit(a, b, v.value())?;
    }
    Ok(())
}
