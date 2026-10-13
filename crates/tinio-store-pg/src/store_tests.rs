// The tests address the standard library by absolute path on purpose: a
// shortened path would not prove the public one.
#![allow(clippy::absolute_paths)]

use sqlx::postgres::types::Oid;

use super::{
    Access, LockHolder, MIGRATION_LOCK_KEY, OWNERSHIP_LOCK_KEY, OWNERSHIP_LOST_EXIT, OwnershipLoss,
    PgStore, SchemaState, describe_holder, key_parts, ownership_lost,
};

#[test]
fn only_read_write_is_writable() {
    assert!(Access::ReadWrite.is_writable());
    assert!(!Access::ReadOnly.is_writable());
}

#[test]
fn a_matching_schema_pends_nothing() {
    let state = SchemaState {
        applied: 1,
        supported: 1,
    };
    assert!(state.matches());
    assert_eq!(state.pending(), 0);
    let behind = SchemaState {
        applied: 0,
        supported: 1,
    };
    assert!(!behind.matches());
    assert_eq!(behind.pending(), 1);
}

/// The direction a pool cannot be rebuilt in (PG6, layer 2) is refused **before
/// any SQL**, which is what makes this leg runnable without a database: the
/// pool connects lazily, and the mismatch is a fact about the pool.
#[tokio::test]
async fn a_writable_store_over_a_read_only_pool_is_refused_without_connecting() {
    let pool =
        crate::pool::Pool::open_read_only("postgres://user:password@db.example:5432/tinio", 1)
            .expect("the dsn parses");
    let err = PgStore::connect_with_pool(pool, Access::ReadWrite)
        .await
        .expect_err("a writable store over a read-only pool is refused");
    assert!(
        matches!(err, crate::error::Error::ReadOnlyPoolForAWriter),
        "{err}"
    );
    assert!(err.to_string().contains("Pool::open"), "{err}");

    let inspected = PgStore::inspect_with_pool(
        crate::pool::Pool::open_read_only("postgres://user:password@db.example:5432/tinio", 1)
            .expect("the dsn parses"),
        Access::ReadWrite,
    )
    .await
    .expect_err("the inspection path refuses it the same way");
    assert!(
        matches!(inspected, crate::error::Error::ReadOnlyPoolForAWriter),
        "{inspected}"
    );
}

/// The status a lost ownership lock terminates with is a supervisor's
/// contract, not an internal detail: an operator's restart policy keys on the
/// number, so the value is pinned rather than left to the one call site.
#[test]
fn the_ownership_loss_exit_status_is_pinned() {
    assert_eq!(OWNERSHIP_LOST_EXIT, 75);
}

/// What a lost re-verify reports: the probe that *answered* "not held" carries
/// no reason, and the prose [`super::exit_on_lost_ownership`] renders is the
/// same answer either way.
#[test]
fn a_loss_reports_the_probe_failure_when_there_was_one() {
    let answered = OwnershipLoss {
        key: OWNERSHIP_LOCK_KEY,
        reason: None,
    };
    assert_eq!(answered.key, OWNERSHIP_LOCK_KEY);
    assert_eq!(answered.reason, None);
    let failed = OwnershipLoss {
        key: OWNERSHIP_LOCK_KEY,
        reason: Some("connection closed".to_owned()),
    };
    assert_ne!(answered, failed);
    assert_eq!(failed.reason.as_deref(), Some("connection closed"));
}

/// Every answer a holding session's one `pg_stat_activity` row can give.
///
/// The unnameable answer is the shape PostgreSQL itself produces for a role
/// that is neither a superuser nor a member of `pg_read_all_stats`: the row
/// exists, but both identifying columns read as `NULL`, which is what the
/// fixture's own superuser role can never be made to produce live. The
/// address-only answer the empty `application_name` below stands for does have
/// a live leg: `db_tests::an_unnamed_live_holder_is_described_by_its_address`.
#[test]
fn a_holding_session_is_described_from_what_pg_stat_activity_showed() {
    let described = |application_name: &str, client_addr: &str| {
        describe_holder(&LockHolder {
            application_name: application_name.to_owned(),
            client_addr: client_addr.to_owned(),
        })
    };
    assert_eq!(
        described("tinio-store-pg", "172.17.0.1"),
        "the session at 172.17.0.1 (application_name tinio-store-pg)"
    );
    // A session over a unix socket has an application_name and no address.
    assert_eq!(
        described("tinio-store-pg", ""),
        "the session named tinio-store-pg"
    );
    assert_eq!(described("", "172.17.0.1"), "the session at 172.17.0.1");
    let unnameable = described("", "");
    assert!(unnameable.contains("cannot identify"), "{unnameable}");
    assert!(unnameable.contains("pg_read_all_stats"), "{unnameable}");
}

#[test]
fn an_advisory_key_splits_into_the_two_halves_pg_locks_records() {
    // `pg_locks` holds the key's high 32 bits in `classid` and its low 32
    // bits in `objid` (`objsubid` 1 tells the one-key form from the two-int
    // one), and both statements of the ownership guard filter on those two
    // halves.
    assert_eq!(
        key_parts(OWNERSHIP_LOCK_KEY),
        (Oid(0x7469_6E69), Oid(0x6F5F_6F77))
    );
    assert_eq!(
        key_parts(MIGRATION_LOCK_KEY),
        (Oid(0x7469_6E69), Oid(0x6F5F_6D67))
    );
    // Keys sharing a high half stay distinct locks, because the low half is
    // compared too.
    assert_eq!(key_parts((1 << 32) | 7), (Oid(1), Oid(7)));
}

#[test]
fn a_failed_ownership_recheck_counts_as_lost() {
    assert!(!ownership_lost(&Ok::<bool, &str>(true)));
    assert!(ownership_lost(&Ok::<bool, &str>(false)));
    assert!(ownership_lost(&Err("connection closed")));
}
