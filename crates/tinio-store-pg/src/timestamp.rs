//! The one conversion between the domain's [`SystemTime`] and PostgreSQL's
//! native `timestamptz` ([`OffsetDateTime`]).
//!
//! The domain's stored wire for an instant is unsigned unix nanoseconds, with
//! `0` as the pre-epoch sentinel (`tinio_core::storage::{to_nanos, from_nanos}`);
//! the migration that made the wall-clock columns native wrote exactly that
//! wire into `timestamptz`. Every read and write of such a column goes through
//! these functions, so no call site rebuilds an instant from an integer itself
//! and PostgreSQL's own type is the only form on the wire.

use std::time::SystemTime;

use time::OffsetDateTime;

use crate::_core::storage::{from_nanos, to_nanos};

/// The `timestamptz` a domain instant is stored as.
///
/// `u64` unix nanoseconds always name an instant inside [`OffsetDateTime`]'s
/// range, so the domain wire maps exactly; the fallback keeps the conversion
/// total rather than panicking on a value that cannot arise.
pub(crate) fn to_timestamp(time: SystemTime) -> OffsetDateTime {
    match OffsetDateTime::from_unix_timestamp_nanos(i128::from(to_nanos(time))) {
        Ok(value) => value,
        Err(_) => OffsetDateTime::UNIX_EPOCH,
    }
}

/// The unix milliseconds `parts.modified_at` stores.
pub(crate) fn to_millis(time: SystemTime) -> i64 {
    i64::try_from(to_nanos(time) / 1_000_000).unwrap_or(i64::MAX)
}

/// The domain instant a stored `timestamptz` names.
///
/// A value before the epoch -- or past the domain wire's `u64` range -- reads
/// as the domain's `0` sentinel, the same mapping `tinio_core::storage::to_nanos`
/// gives a pre-epoch clock, so a hand-edited column can never panic a read.
pub(crate) fn from_timestamp(value: OffsetDateTime) -> SystemTime {
    match u64::try_from(value.unix_timestamp_nanos()) {
        Ok(nanos) => from_nanos(nanos),
        Err(_) => SystemTime::UNIX_EPOCH,
    }
}

/// A fixture instant `seconds` after the unix epoch, through the same
/// conversion a stored value takes.
#[cfg(test)]
pub(crate) fn unix_seconds(seconds: i64) -> SystemTime {
    from_timestamp(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(seconds))
}

/// A fixture instant `micros` after the unix epoch, through the same
/// conversion a stored value takes; `timestamptz` resolves to microseconds, so
/// this is the finest precision a round trip through it can keep.
#[cfg(test)]
pub(crate) fn unix_micros(micros: i64) -> SystemTime {
    from_timestamp(OffsetDateTime::UNIX_EPOCH + time::Duration::microseconds(micros))
}
