//! Shared test helpers (`#[cfg(test)]` only).

use std::future::Future;

/// Run `f` to completion on a fresh multi-thread runtime.
pub(crate) fn rt<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}
