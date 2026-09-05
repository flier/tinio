//! The shared table-handle layer: one parameterized handle over any
//! per-table [`TableDef`] marker. The `Deref`/`DerefMut` and the
//! `open` / `ensure` / `open_readonly` arms live here once (the only
//! per-table difference is the [`TableDefinition`], supplied by the
//! marker); the per-table modules provide the marker, a defaulted type
//! alias over this handle, and their domain methods.

use std::marker::PhantomData;

use derive_more::{Deref, DerefMut};
use redb::TableDefinition;

use crate::error::Error;

/// Per-table constants: the key/value types and the table definition of
/// one table — the handle arms (`Deref`, `open`/`ensure`/
/// `open_readonly`) are generic over the marker.
pub trait TableDef {
    type Key: redb::Key + 'static;
    type Value: redb::Value + 'static;
    const DEF: TableDefinition<'static, Self::Key, Self::Value>;
}

/// The shared table handle: `D` is the per-table marker, `T` the
/// underlying redb table (writable or read-only) the handle derefs to.
/// `T` defaults to the writable [`redb::Table`], so `Table<'txn, D>`
/// is the write handle; the read side passes
/// [`redb::ReadOnlyTable`] (or `impl ReadableTable`).
///
/// Modules alias this with the marker pinned, so callers spell just
/// `Table<'txn>`.
#[derive(Deref, DerefMut)]
pub struct Table<
    'txn,
    D: TableDef,
    T = redb::Table<'txn, <D as TableDef>::Key, <D as TableDef>::Value>,
>(
    #[deref]
    #[deref_mut]
    pub(crate) T,
    PhantomData<D>,
    PhantomData<&'txn ()>,
);

impl<'txn, D: TableDef> Table<'txn, D> {
    /// Open the table in a write transaction.
    pub fn open(txn: &'txn mut redb::WriteTransaction) -> Result<Self, Error> {
        Ok(Self(txn.open_table(D::DEF)?, PhantomData, PhantomData))
    }

    /// Create the table if this is a fresh database.
    pub fn ensure(txn: &mut redb::WriteTransaction) -> Result<(), Error> {
        txn.open_table(D::DEF)?;
        Ok(())
    }
}

impl<'txn, D: TableDef> Table<'txn, D, redb::ReadOnlyTable<D::Key, D::Value>> {
    /// Open the table in a read transaction.
    pub fn open_readonly(txn: &'txn redb::ReadTransaction) -> Result<Self, Error> {
        Ok(Self(txn.open_table(D::DEF)?, PhantomData, PhantomData))
    }
}
