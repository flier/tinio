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

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn empty_stream_collects_to_empty() {
        let body: BodyStream = Box::pin(stream::empty());
        assert_eq!(collect_body(body).await.unwrap(), Vec::<u8>::new());
    }

    #[tokio::test]
    async fn chunks_are_concatenated_in_order() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
            Ok(Bytes::from_static(b"!")),
        ]));
        assert_eq!(collect_body(body).await.unwrap(), b"hello world!".to_vec());
    }

    #[tokio::test]
    async fn a_failed_chunk_aborts_with_its_error() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"head")),
            Err(io::Error::other("boom")),
            Ok(Bytes::from_static(b"tail")),
        ]));
        let err = collect_body(body).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(err.to_string(), "boom");
    }
}
