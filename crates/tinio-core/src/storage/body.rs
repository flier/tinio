//! Streaming body helpers.

use std::{io, pin::Pin};

use bytes::Bytes;
use futures::{Stream, StreamExt};

/// A `Send + Sync` stream of body chunks.
///
/// Upload bodies (put/part) and download bodies (get) flow through this
/// type. Chunks are `bytes::Bytes`, so both sides can stream with bounded
/// buffers and zero-copy chunk sharing. `Sync` is required by the s3s
/// hosting layer (`StreamingBlob::wrap`).
///
/// # Examples
///
/// ```rust
/// use futures::stream;
/// use tinio_core::BodyStream;
///
/// let body: BodyStream = Box::pin(stream::empty());
/// ```
pub type BodyStream = Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>;

/// Drain a [`BodyStream`] into an owned buffer.
///
/// The contract streams bodies, but backends that materialize uploads (the
/// in-memory backend) collect them via this helper.
pub async fn collect_body(mut body: BodyStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}
