//! The database-backed legs for the diagnostic probes.
//!
//! The crate is addressed by its public path: a shortened path would not prove
//! the path an external caller uses. Each leg skips when `TINIO_PG_DSN` is
//! unset, so the default workspace run stays database-free (PG8).

use tinio_store_pg::{diagnostics, pool::Pool};

use crate::test_support::base_dsn;

/// **The off arm, made live.** The compose fixture serves plaintext — its dsn carries
/// `?sslmode=disable` — so the server reports `ssl = false` for the session and leaves the
/// version, cipher and key length `NULL`. The probe must read exactly that, because it is what
/// the "plaintext, and only because the dsn says so" row is built on.
///
/// The on arm has no server to produce it: the fixture offers no TLS, and standing up a
/// certificate-bearing PostgreSQL for one leg is out of this change's scope. The statement's
/// shape is pinned instead by the macro check and the `.sqlx` cache (a wrong column or type is a
/// build error), and the encrypted rendering is pinned by the `tinio-pg` doctor's pure legs.
#[tokio::test]
async fn the_fixture_session_is_plaintext() {
    let Some(dsn) = base_dsn() else {
        eprintln!("skipping: TINIO_PG_DSN is not set");
        return;
    };
    let pool = Pool::open(&dsn, 1).expect("the dsn parses");
    let tls = diagnostics::tls_state(&pool)
        .await
        .expect("the probe answers");
    assert!(!tls.enabled, "{tls:?}");
    assert_eq!(tls.version, None, "{tls:?}");
    assert_eq!(tls.cipher, None, "{tls:?}");
    assert_eq!(tls.bits, None, "{tls:?}");
    pool.close().await;
}
