//! In-memory [`Cleanup`] implementation.
//!
//! There is no durable state to repair. [`MemoryCleanup`] reports a single
//! lint action so the contract shape can be exercised in tests.

use async_trait::async_trait;
use futures::stream;
use tinio_core::{
    cleanup::{ActionStream, Cleanup, RepairAction, RepairActionLevel, RepairKind},
    storage,
};

/// No-op cleanup for the in-memory backend.
pub struct MemoryCleanup;

#[async_trait]
impl Cleanup for MemoryCleanup {
    type Error = storage::Error;

    async fn repair(
        &self,
        _kind: RepairKind,
    ) -> Result<ActionStream<storage::Error>, storage::Error> {
        let actions: Vec<Result<RepairAction, storage::Error>> = vec![Ok(RepairAction {
            level: RepairActionLevel::Lint,
            description: "in-memory backend has no persistent state".into(),
        })];
        Ok(Box::pin(stream::iter(actions)))
    }

    async fn reclaim_meta_orphans(&self) -> Result<ActionStream<storage::Error>, storage::Error> {
        Ok(Box::pin(stream::empty()))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn cleanup_contract_shape() {
        let cleanup = MemoryCleanup;
        let mut stream = cleanup.repair(RepairKind::Startup).await.unwrap();
        let action = stream.next().await.unwrap().unwrap();
        assert_eq!(action.level, RepairActionLevel::Lint);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn reclaim_meta_orphans_is_empty() {
        let cleanup = MemoryCleanup;
        let mut stream = cleanup.reclaim_meta_orphans().await.unwrap();
        assert!(stream.next().await.is_none());
    }
}
