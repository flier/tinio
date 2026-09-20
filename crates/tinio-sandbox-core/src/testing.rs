//! Helpers shared by this crate's test modules: `loopback` (the address every
//! test's frames name) and `blocking` (a blocking reader presented as the async
//! reader the relay asks for). Crate items rather than a copy per module,
//! because three test modules want the address and two want the adapter.

use std::{
    io::{self, Read},
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncBufRead, AsyncRead, BufReader, ReadBuf};

/// `127.0.0.1:port`, the loopback address the tests' frames name.
pub(crate) fn loopback(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

/// A blocking `std::io::Read` presented as the `AsyncBufRead` the relay takes,
/// handing over at most `chunk` bytes per read.
///
/// **Both readers a test drives are blocking ones** — a slice of frame bytes and
/// a real child's pipe — while `Observer::observe` takes an `AsyncBufRead`.
/// `tokio::fs::File::from_std` is how the launcher platforms bridge the two, and
/// it is tokio's `fs` feature, which this crate's dev-dependencies deliberately
/// stop short of; so the blocking read happens where the async one goes.
///
/// **Which is safe only because of where it runs**: a `poll_read` that blocks
/// its executor is a bug anywhere else, and every caller here drives it from a
/// thread that does nothing but wait — the relay thread a launcher spawns, whose
/// shape the supervision test reproduces.
///
/// `chunk` is the caller's because the two uses want opposite things: the
/// relay's frame tests force every frame to be assembled from many reads, while
/// the pipe wants whole chunks.
pub(crate) fn blocking(reader: impl Read + Unpin, chunk: usize) -> impl AsyncBufRead + Unpin {
    struct Blocking<R> {
        reader: R,
        chunk: Vec<u8>,
    }

    impl<R: Read + Unpin> AsyncRead for Blocking<R> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.as_mut().get_mut();
            let take = buf.remaining().min(this.chunk.len());
            let read = this.reader.read(&mut this.chunk[..take])?;
            buf.put_slice(&this.chunk[..read]);
            Poll::Ready(Ok(()))
        }
    }

    BufReader::new(Blocking {
        reader,
        chunk: vec![0; chunk],
    })
}
