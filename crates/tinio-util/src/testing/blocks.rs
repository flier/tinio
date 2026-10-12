//! The in-memory block store the tests of more than one crate run the blob engine over.
//!
//! `tinio-version`'s engine double and `tinio-ops`'s version plane each held one of these —
//! the same map behind the same contract — and neither `tests/` tree can import from the
//! other crate's, so the one copy lives here, behind the `testing` feature both
//! dev-dependencies already turn on.

use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;

use crate::_core::{
    BodyStream, ByteRange,
    blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
    storage,
};

/// An in-memory block store (the shape `tinio_core::blob`'s own example pins): staged bytes are
/// held in memory, filed on publish. `Clone` shares the map, so a clone is the same store.
#[derive(Debug, Clone, Default)]
pub struct MemBlocks(Arc<Mutex<BTreeMap<BlockHash, Vec<u8>>>>);

/// The sink a staged block's bytes are held in.
#[derive(Debug, Default)]
pub struct Sink(Vec<u8>);

impl MemBlocks {
    /// How many blocks the store holds.
    pub fn len(&self) -> usize {
        self.0.lock().expect("the block plane").len()
    }

    /// Whether the store holds no block — the pair [`len`](Self::len) needs, and the question a
    /// leg meaning "nothing was filed" asks.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The stored blocks after `after`, in address order.
    fn entries(&self, after: Option<&BlockHash>) -> Vec<Entry> {
        self.0
            .lock()
            .expect("the block plane")
            .iter()
            .filter(|(hash, _)| after.is_none_or(|after| *hash > after))
            .map(|(hash, bytes)| Entry {
                hash: *hash,
                len: bytes.len() as u64,
                published: SystemTime::now(),
            })
            .collect()
    }
}

#[async_trait]
impl StagedBlock for Sink {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.0.extend_from_slice(&chunk);
        Ok(())
    }
}

#[async_trait]
impl BlockStore for MemBlocks {
    type Error = storage::Error;
    type Staged = Sink;

    async fn stage(&self) -> Result<Self::Staged, Self::Error> {
        Ok(Sink::default())
    }

    async fn publish(
        &self,
        staged: Self::Staged,
        hash: &BlockHash,
    ) -> Result<Published, Self::Error> {
        let mut blocks = self.0.lock().expect("the block plane");
        if blocks.contains_key(hash) {
            return Ok(Published::Existing);
        }
        blocks.insert(*hash, staged.0);
        Ok(Published::Stored)
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
        Ok(self.0.lock().expect("the block plane").contains_key(hash))
    }

    async fn open(
        &self,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        let blocks = self.0.lock().expect("the block plane");
        let bytes = blocks
            .get(hash)
            .ok_or_else(|| storage::io(io::Error::new(io::ErrorKind::NotFound, "no such block")))?;
        let slice = match range {
            Some(range) => {
                let (start, end) = range.resolve(bytes.len() as u64)?;
                bytes[start as usize..=end as usize].to_vec()
            }
            None => bytes.clone(),
        };
        Ok(Box::pin(stream::once(
            async move { Ok(Bytes::from(slice)) },
        )))
    }

    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
        self.0.lock().expect("the block plane").remove(hash);
        Ok(())
    }

    async fn list(
        &self,
        after: Option<&BlockHash>,
        limit: usize,
    ) -> Result<Vec<Entry>, Self::Error> {
        Ok(self.entries(after).into_iter().take(limit).collect())
    }

    async fn walk(
        &self,
        after: Option<&BlockHash>,
    ) -> Result<EntryStream<Self::Error>, Self::Error> {
        Ok(Box::pin(stream::iter(
            self.entries(after).into_iter().map(Ok),
        )))
    }
}
