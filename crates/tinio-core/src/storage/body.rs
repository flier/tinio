//! Streaming body helpers.

use std::io;

use bytes::Bytes;
use futures::{StreamExt, stream::BoxStream};

/// A `Send` stream of body chunks.
///
/// Upload bodies (put/part) and download bodies (get) flow through this
/// type. Chunks are `bytes::Bytes`, so both sides can stream with bounded
/// buffers and zero-copy chunk sharing.
///
/// # Examples
///
/// ```rust
/// use futures::stream;
/// use tinio_core::BodyStream;
///
/// let body: BodyStream = Box::pin(stream::empty());
/// ```
pub type BodyStream = BoxStream<'static, io::Result<Bytes>>;

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
