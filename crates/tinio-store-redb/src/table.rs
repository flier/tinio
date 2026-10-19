//! The shared table-handle layer: one parameterized handle over any
//! per-table [`TableDef`] marker. The `Deref`/`DerefMut` and the
//! `open` / `ensure` / `open_readonly` arms live here once (the only
//! per-table differences are the [`TableDefinition`] and the missing-table
//! tolerance [`TableDef::MAY_BE_ABSENT`], both supplied by the marker);
//! the per-table modules provide the marker, a defaulted type alias over
//! this handle, and their domain methods.

use std::{marker::PhantomData, sync::OnceLock};

use derive_more::{Deref, DerefMut};
use redb::{ReadableDatabase as _, TableDefinition};

use crate::{error::Error, store};

/// Per-table constants: the key/value types and the table definition of
/// one table — the handle arms (`Deref`, `open`/`ensure`/
/// `open_readonly`) are generic over the marker.
pub trait TableDef {
    type Key: redb::Key + 'static;
    type Value: redb::Value + 'static;
    const DEF: TableDefinition<'static, Self::Key, Self::Value>;
    /// Whether a read-only handle may meet this table **missing** and
    /// answer it as empty. `false` for every base table: a state database
    /// without one is a database this build did not write, so its absence
    /// is the redb error it is. `true` for the three version tables and
    /// `bucket_lifecycle` — the four of the shared schema's twelve that a
    /// root may legitimately predate (spec 2026-10-03 for the version
    /// machinery, spec 2026-10-18 for the lifecycle row): "no history" and
    /// "no configuration" must not read as "this root cannot be read"
    /// (FR-023 amended, design 2026-10-03 **Read paths**).
    const MAY_BE_ABSENT: bool = false;
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

impl<'txn, D: TableDef, T> Table<'txn, D, T> {
    /// Wrap a table the caller already opened. The caller is a plane whose
    /// tables are **absent by nature** and must therefore decide the
    /// missing-table case itself, which [`Table::open_readonly`]'s
    /// per-definition tolerance cannot express: the content plane's two
    /// tables are the backend's own (they are not among the shared schema's
    /// twelve), so their absence means "no content", not "this root cannot be
    /// read" — while a *metadata* base table's absence stays the error it is.
    pub(crate) fn from_table(table: T) -> Self {
        Self(table, PhantomData, PhantomData)
    }
}

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
    ///
    /// **A table that may be absent reads as empty when it is missing.**
    /// A read-only handle may meet a database that predates a table — a
    /// root written by a build without the version machinery has no
    /// version tables, one written before the lifecycle machinery has no
    /// `bucket_lifecycle` — and "no history"/"no configuration" must not
    /// read as "this root cannot be read". The tolerance is per table
    /// ([`TableDef::MAY_BE_ABSENT`]): only the three version tables and
    /// `bucket_lifecycle` allow it, and any other missing table propagates
    /// redb's `TableDoesNotExist` as the error it is. The stand-in is a real,
    /// empty table: redb hands out no table that does not exist and its
    /// table types have no public constructor, so the missing table is
    /// answered from a process-wide in-memory database (`empty_table`,
    /// below), whose tables are never written.
    pub fn open_readonly(txn: &'txn redb::ReadTransaction) -> Result<Self, Error> {
        match txn.open_table(D::DEF) {
            Ok(table) => Ok(Self(table, PhantomData, PhantomData)),
            Err(redb::TableError::TableDoesNotExist(_)) if D::MAY_BE_ABSENT => {
                Ok(Self(empty_table::<D>()?, PhantomData, PhantomData))
            }
            Err(err) => Err(err.into()),
        }
    }
}

/// The empty table a missing **may-be-absent** table reads as: `D`'s own
/// table in a process-wide in-memory database, created on the first ask
/// for that table and handed out from a read snapshot.
///
/// The database is the same one for every definition and every caller —
/// it holds no rows, so its snapshots cannot disagree — and it is never
/// written after the creation of a table. Reads of it answer `None` and
/// empty ranges: exactly what "the table is not there" means to a
/// [`Table::open_readonly`] caller.
///
/// Only a definition that allows absence is ever answered from here
/// ([`TableDef::MAY_BE_ABSENT`], asserted below): a base table's absence
/// is a database this build did not write, and the caller must see it.
fn empty_table<D: TableDef>() -> Result<redb::ReadOnlyTable<D::Key, D::Value>, Error> {
    debug_assert!(
        D::MAY_BE_ABSENT,
        "only a table that may be absent reads as empty"
    );
    let db = empty_database()?;
    // The already-created case costs one snapshot; only a definition
    // never asked for before pays for the creation. The arm is the
    // missing-table one alone: any other failure is the error it is, not
    // a table this function then creates (the wide `if let Ok` this
    // replaced swallowed a corrupt or unreadable table and wrote over
    // it).
    let txn = db.begin_read()?;
    match txn.open_table(D::DEF) {
        Ok(table) => return Ok(table),
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(err) => return Err(err.into()),
    }
    drop(txn);
    {
        let txn = db.begin_write()?;
        txn.open_table(D::DEF)?;
        txn.commit()?;
    }
    Ok(db.begin_read()?.open_table(D::DEF)?)
}

/// The in-memory database behind [`empty_table`]. Created once — a
/// second thread that loses the race drops its own database and adopts
/// the first — and never removed: the empty tables handed out keep their
/// memory alive through it for as long as they live.
fn empty_database() -> Result<&'static redb::Database, Error> {
    static EMPTY: OnceLock<redb::Database> = OnceLock::new();
    if let Some(db) = EMPTY.get() {
        return Ok(db);
    }
    let created = store::open_in_memory()?;
    Ok(EMPTY.get_or_init(|| created))
}

#[cfg(test)]
mod tests {
    use redb::{Database, ReadableDatabase, backends::InMemoryBackend};

    use crate::{
        _core::version::State, bucket_lifecycle, bucket_versioning, error::Error, objects, state,
        version, version_index,
    };

    fn database() -> Database {
        Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap()
    }

    /// FR-023 amended (design 2026-10-03 **Read paths**): the missing-table
    /// tolerance is per table. A database predating the version machinery —
    /// no version table at all — answers every version table empty, and a
    /// database that has them still reads their own rows. `bucket_lifecycle`
    /// is the fourth may-be-absent table (spec 2026-10-18, LC6), so a root
    /// written before it reads "no configuration" rather than failing.
    #[test]
    fn a_missing_may_be_absent_table_reads_as_empty_on_a_read_only_handle() {
        let db = database();

        // No version table in this database at all.
        let txn = db.begin_read().unwrap();
        assert!(
            version::Table::open_readonly(&txn)
                .unwrap()
                .get("b", "k", 1)
                .unwrap()
                .is_none()
        );
        assert!(
            version_index::Table::open_readonly(&txn)
                .unwrap()
                .seq_of("b", "k", "v1")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            bucket_versioning::Table::open_readonly(&txn)
                .unwrap()
                .get("data")
                .unwrap(),
            None
        );
        assert!(
            bucket_lifecycle::Table::open_readonly(&txn)
                .unwrap()
                .get("data")
                .unwrap()
                .is_none(),
            "a root written before the lifecycle row has no bucket_lifecycle table"
        );
        drop(txn);

        // The same definition over a database that has the table reads
        // the table, not the stand-in.
        let mut write = db.begin_write().unwrap();
        bucket_versioning::Table::open(&mut write)
            .unwrap()
            .set("data", State::Enabled)
            .unwrap();
        write.commit().unwrap();
        let txn = db.begin_read().unwrap();
        assert_eq!(
            bucket_versioning::Table::open_readonly(&txn)
                .unwrap()
                .get("data")
                .unwrap(),
            Some(State::Enabled)
        );
    }

    /// The other half of the scoping: a missing base table is a database
    /// this build did not write, so its absence stays the redb error it
    /// is — never an empty read. (The degraded read-only view is not this
    /// case: it writes the shared schema into its own in-memory database;
    /// see `store::Handle::empty`.)
    #[test]
    fn a_missing_base_table_is_the_redb_error_it_is() {
        let db = database();
        let txn = db.begin_read().unwrap();
        for err in [
            objects::Table::open_readonly(&txn).map(|_| ()).unwrap_err(),
            state::Table::open_readonly(&txn).map(|_| ()).unwrap_err(),
        ] {
            assert!(
                matches!(err, Error::Table(redb::TableError::TableDoesNotExist(_))),
                "{err}"
            );
        }
    }
}
