//! Serving the router over the transports the config selected.
//!
//! **This module is where the bind's runtime-free listeners become tokio's.** Task 2 created
//! `std` ones (and, on Windows, a raw pipe handle) so `Plane::bind` could be the
//! single-instance lock before the CLI's runtime existed; `from_std` and
//! `NamedPipeServer::from_raw_handle` both need a reactor, and `serve_transport` runs inside
//! the runtime, so the conversions live here and nowhere else (`Channel` never enters tokio's
//! types).
//!
//! The unix socket is an axum `Listener` once converted, so `axum::serve` drives it directly,
//! with `with_graceful_shutdown` on the plane's channel — ceasing to accept is the plane's
//! job, and the ≤ 10 s bound on the drain is the CLI's (decision 12). The Windows named pipe is
//! not a `Listener` at all: `NamedPipeServer` has no impl, and the pipe's lack of TCP's
//! half-close makes keep-alive unsafe there, so it gets the manual connection loop
//! `research.md:99` prescribes. TCP and TLS (Task 5, Task 6) are `Listener`s again.

#[cfg(any(unix, feature = "tls"))]
use std::fs;
#[cfg(any(windows, feature = "tls"))]
use std::io;
#[cfg(unix)]
use std::os::unix::{fs::MetadataExt, net::UnixListener};
#[cfg(windows)]
use std::os::windows::io::{OwnedHandle, RawHandle};
// `tls` bounds the handshake; `windows` bounds the pipe drain ([`DRAIN_LIMIT`]). One import for
// both, because the default graph has both and an `use` twice over does not compile.
#[cfg(any(feature = "tls", windows))]
use std::time::Duration;
// The named-pipe pool's own imports: the stream wrapper behind the per-connection bridge.
#[cfg(windows)]
use std::{
    future::poll_fn,
    pin::Pin,
    sync::MutexGuard,
    task::{Context as TaskContext, Poll},
};
use std::{
    mem,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process,
    sync::{Arc, Mutex},
};

#[cfg(feature = "tls")]
use axum::serve::Listener;
#[cfg(windows)]
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixListener as TokioUnixListener;
#[cfg(feature = "tls")]
use tokio::time::sleep;
// **Two `#[cfg]`s need this name, and the `any` is load-bearing.** The Windows arm uses it in
// `wait_for_peer_close`, and the TLS arm uses it for the handshake bound — and the TLS arm is
// `#[cfg(feature = "tls")]` with **no** platform condition, so on macOS and Linux this import
// must be here for the TLS build. Gated on `windows` alone it is absent exactly where the
// handshake's `timeout` is compiled, which is a hard `E0425` for every non-Windows TLS build
// (measured on macOS 15.8, 2026-09-25: `cargo build -p tinio` fails in this file at line 1051).
// `tls` is a default feature of this crate, so that was every default build on those targets; it
// stayed invisible because the Windows and macOS legs each satisfied one half of the old gate.
#[cfg(any(windows, feature = "tls"))]
use tokio::time::timeout;
#[cfg(windows)]
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::windows::named_pipe::NamedPipeServer,
    task::JoinSet,
};
use tokio::{net::TcpListener as TokioTcpListener, sync::watch};
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig, server::TlsStream};
#[cfg(windows)]
use windows_sys::Win32::Foundation::{ERROR_BROKEN_PIPE, ERROR_NO_DATA};

#[cfg(windows)]
use crate::dacl;
use crate::{
    Error,
    bind::{self, Channel},
    metrics::Gauges,
    router::{Auth, Context, Shared, router},
    state::{State, remove_if_present},
};

/// The upper bound on [`wait_for_peer_close`]: how long one instance keeps waiting for a client
/// that is still attached **after its response has been written**, before the instance is
/// force-reclaimed.
///
/// **The waiting it bounds is at kernel speed, and the bound is ~1 400× its worst measurement.**
/// On a client that hangs up after reading, which is every client this crate ships, the wait ends
/// the moment the client's handle closes: measured over 201 sequential dials on this branch's host
/// (`the_pipe_pool_serves_back_to_back_dials_with_no_gap`), 80 µs to 181 µs — p50 97 µs, p99
/// 177 µs, max 181 µs. A quarter of a second is four orders of magnitude above the mean, so a
/// **healthy** client cannot reach this bound, whatever the scheduler does to it.
///
/// **What the bound buys, and what it costs.** A client that completes an exchange and then holds
/// its handle open — measured with both instances wedged, `two_stalled_clients_do_not_deafen_the_name`
/// — otherwise parks that instance forever: `DisconnectNamedPipe` is only safe after the peer is
/// gone, so the loop waits, and with `bind::INSTANCES` of them wedged the *name* goes deaf, which
/// no retry can repair. At the limit the instance is disarmed anyway, which discards whatever the
/// pipe still holds unread — and that is exactly what cannot be lost: the response has already
/// been **written**, and `tokio`'s write on a pipe handle is `WriteFile` + `FlushFileBuffers`,
/// which blocks until the client has consumed it. What a wedged client holding the connection open
/// has taken is the whole response; what it is holding is the *connection*, and nothing this
/// server sends after it — `keep_alive(false)` means there is nothing after it. So the discarded
/// bytes are at most what that client already chose not to read, and it is the client that is
/// wrong, not the response that is lost.
///
/// **It is not a request timeout and must not become one.** The wait starts *after* hyper has
/// returned the connection, so no request is in flight inside it; a request that is slow to be
/// answered is slow in the handler, which this constant does not touch. The plane's own shutdown
/// stays the CLI's ≤ 10 s bound (decision 12) — this is a per-instance liveness backstop under it.
#[cfg(windows)]
const DRAIN_LIMIT: Duration = Duration::from_millis(250);

/// Serves `shared` over a bound local channel until `stop` flips, then drains and returns.
///
/// The channel is self-describing: the Windows form carries its own name (`Channel { name, .. }`
/// there), so no caller passes an identity in and neither platform's branch re-derives one — the
/// same name [`Channel::identity`] reports is what `state.control_name` records.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(unix)]
/// # {
/// use std::{path::Path, sync::Arc};
///
/// use tinio_api::{
///     bind::bind_unix,
///     router::{Auth, Context},
///     state::State,
///     transport::serve_local,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// let path = Path::new("/run/tinio/tinio-0123456789abcdef.sock");
/// let state = State::new(path.to_string_lossy(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, stop));
/// let channel = bind_unix(path)?;
/// serve_local(channel, shared, rx).await
/// # }
/// # }
/// ```
pub async fn serve_local(
    channel: Channel,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    #[cfg(unix)]
    {
        // The channel's own fields, destructured: on this platform the struct is the whole type,
        // so there is no variant to match on and no unreachable arm to write.
        let Channel { listener, .. } = channel;
        // The one conversion: `std` (bound before the runtime) to tokio (served inside it).
        let listener = TokioUnixListener::from_std(listener).map_err(Error::Io)?;
        axum::serve(listener, router(shared))
            .with_graceful_shutdown(until_stopped(stop))
            .await
            .map_err(|err| Error::Transport(format!("unix channel: {err}")))
    }
    #[cfg(windows)]
    {
        // The pipe form carries its own name and its whole pool, so the identity is passed on
        // rather than re-derived and no instance is created here.
        let Channel { name, handles } = channel;
        serve_pipe(handles, &name, shared, stop).await
    }
}

/// The shape every `with_graceful_shutdown` call in this plan takes: resolve when the plane's
/// channel flips (or when its sender is gone, which is a shutdown too).
///
/// `serve_local`'s unix arm and `serve_tcp` are its callers on every platform; `serve_https` is
/// the third (Task 6), so all three transports stop on the plane's one channel.
pub(crate) async fn until_stopped(mut stop: watch::Receiver<bool>) {
    let _ = stop.changed().await;
}

/// The named-pipe accept side: **one accept loop per bound instance**, HTTP/1 keep-alive off, and
/// the shutdown channel watched while no client is connecting.
///
/// **The pool is the name's liveness, and the pool comes from the bind or from a launcher.**
/// Every client dials the one name, so it can only reach an instance that is *listening*: the
/// channel is served by the instances `bind_pipe` created, and while one of them serves a
/// connection another is still waiting for the next client. That is what makes a second
/// `status` after a first answer rather than `ERROR_PIPE_BUSY`.
///
/// **Nothing here creates an instance, and that is the point.** Inside an AppContainer this
/// process's identity is the package SID, so `CreateNamedPipeW` with the channel's
/// current-user-only DACL ([`dacl::create_pipe`]) is refused `ERROR_ACCESS_DENIED`: a child that
/// made its own successor would answer once and then stop listening. The unconfined bind creates
/// the pool ([`crate::bind::bind_pipe`]) and a confined launcher creates it and hands every
/// handle over ([`Plane::adopt`]), so this loop only ever serves instances that already exist.
///
/// **Reusing one instance was measured and rejected.** `DisconnectNamedPipe` then
/// `ConnectNamedPipe` on the instance in hand leaves a tick in which it is neither connected nor
/// listening; because `client::exchange` returns as soon as it has the response while the
/// server's disarm runs later, a client's *next* dial lands in that tick and is killed
/// mid-request — measured as `Transport("/status: connection error")`, the same "Pipe is broken"
/// the confined run reports, where this rotation reports zero retries over two hundred dials.
#[cfg(windows)]
async fn serve_pipe(
    handles: Vec<OwnedHandle>,
    identity: &str,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    if handles.is_empty() {
        return Err(Error::Transport(
            "the control channel was bound with no instances: nothing could ever answer on it"
                .to_string(),
        ));
    }
    // Built once, below the loops: nothing about the router varies per instance, and its clone is
    // an `Arc` bump (`axum::Router` keeps its state behind one), so a per-instance rebuild would
    // re-derive what this one already holds.
    let app = router(shared);
    // One task per instance, and the set is also the drain: a stop ends the accept side, and
    // every connection already accepted runs to completion before this returns.
    let mut listeners = JoinSet::new();
    for handle in handles {
        // The raw handle becomes tokio's own type here, inside the runtime — the constructor
        // needs the reactor, which is why the bind could only hand over an `OwnedHandle`
        // (`dacl` is the crate's one raw-Windows module, Task 2's note).
        let pipe = dacl::adopt_pipe(handle).map_err(Error::Io)?;
        listeners.spawn(pipe_instance_loop(
            pipe,
            app.clone(),
            stop.clone(),
            identity.to_string(),
        ));
    }

    while let Some(served) = listeners.join_next().await {
        match served {
            Ok(Ok(())) => {}
            // One instance's accept loop failing is the channel failing: the pool exists to keep
            // the name answering, and a loop that cannot `connect` any more is that guarantee
            // gone. The rest are aborted so the plane reports one error rather than hanging on
            // instances that are still waiting for clients that will never be served faithfully.
            Ok(Err(err)) => {
                listeners.shutdown().await;
                return Err(err);
            }
            Err(err) => {
                listeners.shutdown().await;
                return Err(Error::Internal(format!(
                    "a management pipe instance loop: {err}"
                )));
            }
        }
    }
    Ok(())
}

/// One instance's whole life: wait for a client, serve it to the end, disarm, wait again.
///
/// **One task per instance is what preserves the drain.** A stop observed while this task is
/// *waiting* ends it; a stop observed while it is *serving* cannot, because the connection is not
/// a branch of the `select!` — so the client that got in before the shutdown is still answered
/// (`management.rs`'s leg with a connection open at the stop is the assertion), which is the same
/// property the previous single-instance loop got from its `JoinSet`.
#[cfg(windows)]
async fn pipe_instance_loop(
    mut pipe: NamedPipeServer,
    app: axum::Router,
    mut stop: watch::Receiver<bool>,
    identity: String,
) -> Result<(), Error> {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            connected = pipe.connect() => {
                eprintln!("PROBE: instance connected");
                connected.map_err(|err| Error::Transport(format!("{identity}: pipe connect: {err}")))?;
            }
        }
        pipe = serve_pipe_connection(pipe, app.clone(), &identity).await?;
        eprintln!("PROBE: instance released back to the loop");
    }
    Ok(())
}

/// Serves **one** connection on `pipe`, drains it, and gives the instance back to its loop.
///
/// **The disarm is why the instance comes back at all, and the drain is why the disarm is safe.**
/// `DisconnectNamedPipe` discards whatever the instance still holds unread, while
/// `keep_alive(false)` lets hyper return as soon as it has *written* the response — not when the
/// client has read it. Disarming straight after the connection therefore destroys responses under
/// load: measured on this host, the first two dials succeed and every dial after them fails its
/// exchange while the server accepts and serves each one. [`wait_for_peer_close`] is the fix, and
/// it is not optional.
///
/// The handover is forced by `hyper`: `serve_connection` consumes its stream and bounds it
/// `Io: 'static`, while tokio implements `AsyncRead`/`AsyncWrite` for `NamedPipeServer` itself
/// rather than for `&NamedPipeServer` — so the connection owns the instance through [`Pool`] and
/// its end is where the loop gets it back.
#[cfg(windows)]
async fn serve_pipe_connection(
    pipe: NamedPipeServer,
    app: axum::Router,
    identity: &str,
) -> Result<NamedPipeServer, Error> {
    // `http1` borrows the builder mutably (`hyper-util`'s `auto::Builder::http1(&mut self)`), so
    // the binding is `mut`.
    let mut builder = auto::Builder::new(TokioExecutor::new());
    // axum's `Router` is a tower service and this loop is hyper's own, so the bridge is explicit —
    // `serve_connection` wants `hyper::service::Service`, and axum wraps with exactly this adapter
    // in its own server (`axum-0.8.9/src/serve/mod.rs:388`).
    let service = TowerToHyperService::new(app);
    // The stream the connection is served on: `hyper`'s bridge over [`Pool`], which owns the
    // instance for the connection's life. The slot this frame shares with the stream is what lets
    // the instance come back out once the connection is over.
    let io = TokioIo::new(Pool::new(pipe));
    let slot = io.inner().slot();
    eprintln!("PROBE: serving a connection");
    if let Err(err) = builder
        .http1()
        // The pipe has no half-close, so keep-alive would let a client wait forever for a
        // response delimitation TCP would have provided (`research.md:99`).
        .keep_alive(false)
        .serve_connection(io, service)
        .await
    {
        // A connection-level failure is the client's, not the channel's: the instance is still
        // returned below and the instance's next `connect` works.
        tracing::debug!(%identity, error = %err, "management pipe connection ended");
    }
    let mut pipe = Pool::release(slot).ok_or_else(|| {
        Error::Internal("the connection's stream still holds the pipe instance".to_string())
    })?;
    // Before the disarm, and never after it: this is the drain that keeps the response the client
    // has not read yet from being thrown away (see this function's doc).
    wait_for_peer_close(&mut pipe, identity).await;
    // Back to the listening state, so this instance's next `connect` can accept. `ERROR_NO_DATA`
    // is the client having hung up first: the instance is disconnected either way, which is the
    // state the next `connect` wants, so it is not a failure.
    if let Err(err) = pipe.disconnect()
        && err.raw_os_error() != Some(ERROR_NO_DATA as i32)
    {
        return Err(Error::Transport(format!(
            "{identity}: pipe disconnect: {err}"
        )));
    }
    Ok(pipe)
}

/// Waits until the client's end is gone, so the [`DisconnectNamedPipe`] that follows has nothing
/// left to discard — **bounded by [`DRAIN_LIMIT`]**, after which the instance is reclaimed anyway
/// and the reclaim is reported as an event.
///
/// A zero-length read is `Ok` in tokio's `poll_read` spelling; the broken-pipe error is the same
/// news on an abrupt close, and any other failure is the client's own teardown. The wait is
/// wrapped in a `timeout` rather than given a deadline of its own so the cancellation is the
/// usual one: the read is simply dropped, which loses nothing it had consumed (it reads one byte
/// and nothing else), and the disarm below follows either way.
///
/// A client that connects and never lets go holds this instance **for at most the limit** — the
/// measurement and the argument that nothing it could still be owed is discarded are on
/// [`DRAIN_LIMIT`]. The other instances of the pool keep listening while this one waits, which is
/// still the property the pool exists for; the bound is what keeps the *name* from going deaf
/// when every instance is held at once.
///
/// **The event is the observability, and it is `warn` on purpose.** A forced reclaim means a
/// client is sitting on a connection it has already been answered on — a client bug or a wedged
/// process, never normal traffic — so it is worth a line in the log rather than a debug detail,
/// and it costs one event on a path that is otherwise silent.
///
/// [`DisconnectNamedPipe`]: https://learn.microsoft.com/windows/win32/api/namedpipeapi/nf-namedpipeapi-disconnectnamedpipe
#[cfg(windows)]
async fn wait_for_peer_close(pipe: &mut NamedPipeServer, identity: &str) {
    use std::time::Instant;

    let started = Instant::now();
    let mut buf = [0u8; 1];
    let closed = timeout(
        DRAIN_LIMIT,
        poll_fn(|cx| {
            let mut read = ReadBuf::new(&mut buf);
            match Pin::new(&mut *pipe).poll_read(cx, &mut read) {
                Poll::Ready(Ok(())) => Poll::Ready(()),
                Poll::Ready(Err(err)) if err.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) => {
                    Poll::Ready(())
                }
                Poll::Ready(Err(_)) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            }
        }),
    )
    .await;
    if closed.is_err() {
        tracing::warn!(
            %identity,
            limit = ?DRAIN_LIMIT,
            waited = ?started.elapsed(),
            "the client did not close after its response; reclaiming the pipe instance"
        );
    }
}

/// The instance one connection is served on, **handing it back when the connection ends**.
///
/// **Every part of this shape is forced by `serve_connection`.** It takes its stream by value and
/// bounds it `Io: 'static`, so the instance cannot be borrowed from a frame that must use it again
/// — the connection has to own it. It also consumes the stream, so the owner cannot read the value
/// back out of the same binding afterwards. So the instance travels in a slot both that frame and
/// the connection hold ([`Arc`]), the connection's end drops the stream and leaves the instance in
/// the slot, and [`Pool::release`] is the frame taking it back out.
///
/// **The traits are tokio's, not `hyper`'s.** tokio implements `AsyncRead`/`AsyncWrite` for
/// `NamedPipeServer` itself, so the value in the slot is one, and `hyper_util::rt::TokioIo` is the
/// one bridge to `hyper`'s traits that `serve_connection` wants — putting the slot behind a second
/// `TokioIo` would mean going tokio -> `TokioIo` -> tokio -> `TokioIo`, which is why this wrapper
/// implements the tokio side and lets one `TokioIo` sit on top of it.
#[cfg(windows)]
struct Pool {
    /// `Some` until the connection's stream is dropped; `None` only once it has been taken out.
    slot: Arc<Mutex<Option<NamedPipeServer>>>,
}

#[cfg(windows)]
impl Pool {
    /// Wraps `inner` for one connection.
    fn new(inner: NamedPipeServer) -> Self {
        Self {
            slot: Arc::new(Mutex::new(Some(inner))),
        }
    }

    /// The shared slot, for the frame that outlives the connection.
    fn slot(&self) -> Arc<Mutex<Option<NamedPipeServer>>> {
        Arc::clone(&self.slot)
    }

    /// Closes the slot and gives the instance back: the last `Arc` handle, with the connection's
    /// stream already dropped, is the one that can. `None` means the stream outlived the
    /// connection, which its `await` rules out.
    fn release(slot: Arc<Mutex<Option<NamedPipeServer>>>) -> Option<NamedPipeServer> {
        Arc::try_unwrap(slot).ok()?.into_inner().ok()?.take()
    }

    /// The inner stream, for the poll that is running now. The mutex's poison case is a bug
    /// rather than a state this loop can be in: no crate code panics while holding the guard.
    fn held(&self) -> MutexGuard<'_, Option<NamedPipeServer>> {
        self.slot
            .lock()
            .expect("the instance slot is not poisoned while a connection is served")
    }
}

#[cfg(windows)]
impl AsyncRead for Pool {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut held = self.held();
        eprintln!("PROBE: server poll_read");
        let out = Pin::new(
            held.as_mut()
                .expect("the instance is in the slot for the connection's life"),
        )
        .poll_read(cx, buf);
        eprintln!("PROBE: server poll_read -> {out:?}");
        out
    }
}

#[cfg(windows)]
impl AsyncWrite for Pool {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut held = self.held();
        eprintln!("PROBE: server poll_write {} bytes", buf.len());
        let out = Pin::new(
            held.as_mut()
                .expect("the instance is in the slot for the connection's life"),
        )
        .poll_write(cx, buf);
        eprintln!("PROBE: server poll_write -> {out:?}");
        out
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let mut held = self.held();
        Pin::new(
            held.as_mut()
                .expect("the instance is in the slot for the connection's life"),
        )
        .poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        let mut held = self.held();
        Pin::new(
            held.as_mut()
                .expect("the instance is in the slot for the connection's life"),
        )
        .poll_shutdown(cx)
    }

    /// Forwarded rather than left to the default, which buffers: the pipe has a vectored write of
    /// its own, and one copy of a response is enough.
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let mut held = self.held();
        Pin::new(
            held.as_mut()
                .expect("the instance is in the slot for the connection's life"),
        )
        .poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.held()
            .as_ref()
            .is_some_and(AsyncWrite::is_write_vectored)
    }
}

/// The local channel's platform form, as the caller resolved it.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_api::transport::LocalForm;
///
/// let unix = LocalForm::Unix(PathBuf::from("/run/tinio/tinio-0123456789abcdef.sock"));
/// let pipe = LocalForm::Pipe(r"\\.\pipe\tinio-mine".to_string());
/// assert!(matches!(unix, LocalForm::Unix(_)));
/// assert!(matches!(pipe, LocalForm::Pipe(_)));
/// ```
#[derive(Debug, Clone)]
pub enum LocalForm {
    /// A unix socket path.
    Unix(PathBuf),
    /// A Windows pipe name.
    Pipe(String),
}

/// Bind the local control channel `form` names.
///
/// The unix and pipe binds stay in this crate. A caller does not match the
/// variant to pick a platform function.
///
/// # Errors
///
/// The bind failed, or `form` is the other platform's variant.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::transport::{LocalForm, bind_local};
///
/// let channel = bind_local(&LocalForm::Unix(PathBuf::from("/run/tinio/tinio.sock")))?;
/// assert!(!channel.identity().is_empty());
/// # Ok::<(), tinio_api::Error>(())
/// ```
pub fn bind_local(form: &LocalForm) -> Result<Channel, Error> {
    match form {
        LocalForm::Unix(path) => {
            #[cfg(unix)]
            {
                bind::bind_unix(path)
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport(
                    "a unix control channel cannot be bound on this platform".into(),
                ))
            }
        }
        LocalForm::Pipe(name) => {
            #[cfg(windows)]
            {
                bind::bind_pipe(name)
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport(
                    "a named-pipe control channel cannot be bound on this platform".into(),
                ))
            }
        }
    }
}

/// The HTTPS settings, present only when the transport is HTTPS.
///
/// # Examples
///
/// ```rust
/// use std::{net::SocketAddr, path::PathBuf};
///
/// use tinio_api::transport::HttpsOptions;
///
/// let options = HttpsOptions {
///     addr: "127.0.0.1:9443".parse().expect("addr"),
///     cert: PathBuf::from("/etc/tinio/cert.pem"),
///     key: PathBuf::from("/etc/tinio/key.pem"),
/// };
/// assert_eq!(options.addr, SocketAddr::from(([127, 0, 0, 1], 9443)));
/// assert!(options.cert.ends_with("cert.pem"));
/// ```
#[derive(Debug, Clone)]
pub struct HttpsOptions {
    /// The bind address.
    pub addr: SocketAddr,
    /// The PEM certificate chain.
    pub cert: PathBuf,
    /// The PEM private key.
    pub key: PathBuf,
}

/// What the caller asked for, before exclusivity is applied.
///
/// # Examples
///
/// ```rust
/// use tinio_api::transport::Selection;
///
/// // The CLI's starting point: nothing selected, which `select` refuses.
/// let empty = Selection::default();
/// assert!(empty.local.is_none() && empty.http.is_none() && empty.https.is_none());
/// ```
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// The local channel, when enabled.
    pub local: Option<LocalForm>,
    /// TCP HTTP, when its section or a `--api http://…` flag enabled it.
    pub http: Option<SocketAddr>,
    /// TCP HTTPS, when its section plus cert/key or a `--api https://…` flag enabled it.
    pub https: Option<HttpsOptions>,
}

/// A bound transport, ready to serve.
///
/// # Examples
///
/// ```no_run
/// use std::net::SocketAddr;
///
/// use tinio_api::{
///     router::Auth,
///     transport::{HttpsOptions, Selection, select},
/// };
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// let http = select(Selection {
///     http: Some("127.0.0.1:9001".parse::<SocketAddr>().expect("addr")),
///     ..Selection::default()
/// })?;
/// assert_eq!(http.auth(), Auth::Loopback);
/// assert!(http.identity().starts_with("127.0.0.1:"));
///
/// // HTTPS is the arm the `tls` feature compiles in; without the feature, `select`
/// // refuses the same selection instead.
/// # #[cfg(feature = "tls")]
/// # {
/// let https = select(Selection {
///     https: Some(HttpsOptions {
///         addr: "127.0.0.1:9443".parse().expect("addr"),
///         cert: "/etc/tinio/cert.pem".into(),
///         key: "/etc/tinio/key.pem".into(),
///     }),
///     ..Selection::default()
/// })?;
/// assert_eq!(https.identity(), "127.0.0.1:9443");
/// # }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Transport {
    /// The local channel.
    Local {
        /// The bound channel, which is also what `state` records: [`Channel::identity`] is the
        /// control name, and the token rule is always [`Auth::Local`].
        channel: Channel,
    },
    /// TCP HTTP.
    Tcp {
        /// The bound listener — `std`, converted to tokio's inside `serve_tcp`, because
        /// `select` runs before the runtime exists.
        listener: TcpListener,
        /// The bound address.
        addr: SocketAddr,
    },
    /// TCP HTTPS (feature `tls`).
    #[cfg(feature = "tls")]
    Https {
        /// The bound listener — `std`, as in the `Tcp` arm.
        listener: TcpListener,
        /// The bound address.
        addr: SocketAddr,
        /// The PEM certificate chain.
        cert: PathBuf,
        /// The PEM private key.
        key: PathBuf,
    },
}

/// Applies the three-choose-one rule and binds the winner.
///
/// A local arm keeps the **bound** channel, never the requested one: [`Transport::identity`] is
/// [`Channel::identity`], so what the bind created is what `state` records and what every client
/// dials. `Plane::bind` is what guarantees that identity is absolute.
///
/// # Examples
///
/// ```no_run
/// use tinio_api::{
///     router::Auth,
///     transport::{Selection, select},
/// };
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// // Exactly one: none is a startup error, and two are a usage error.
/// assert!(select(Selection::default()).is_err());
///
/// let transport = select(Selection {
///     http: Some("127.0.0.1:9001".parse().expect("addr")),
///     ..Selection::default()
/// })?;
/// assert_eq!(transport.identity(), "127.0.0.1:9001");
/// assert_eq!(transport.auth(), Auth::Loopback);
/// # Ok(())
/// # }
/// ```
pub fn select(selection: Selection) -> Result<Transport, Error> {
    let enabled = usize::from(selection.local.is_some())
        + usize::from(selection.http.is_some())
        + usize::from(selection.https.is_some());
    if enabled == 0 {
        return Err(Error::Transport(
            "no management transport is enabled: keep the local channel or configure exactly one of http/https"
                .to_string(),
        ));
    }
    if enabled > 1 {
        return Err(more_than_one());
    }

    match (selection.local, selection.http, selection.https) {
        (Some(LocalForm::Unix(path)), None, None) => {
            #[cfg(unix)]
            {
                let channel = bind::bind_unix(&path)?;
                // The *bound* channel, not the requested path: `state.control_name` is the
                // channel's own identity, which is absolute, because a relative one stops
                // resolving the moment the daemon `chdir("/")`s — which is exactly when
                // `status` and `stop` dial it.
                Ok(Transport::Local { channel })
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(Error::Transport(
                    "unix:// is not a Windows transport".to_string(),
                ))
            }
        }
        (Some(LocalForm::Pipe(name)), None, None) => {
            #[cfg(windows)]
            {
                let channel = bind::bind_pipe(&name)?;
                Ok(Transport::Local { channel })
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport(
                    "pipe:// is not a unix transport".to_string(),
                ))
            }
        }
        (None, Some(addr), None) => {
            // Bound with the runtime-free type and left that way: `serve_tcp` converts inside
            // the runtime, which is the only place `from_std` may be called at all.
            let listener = TcpListener::bind(addr).map_err(Error::Io)?;
            listener.set_nonblocking(true).map_err(Error::Io)?;
            // The bound address, not the requested one: `--port 0` and a wildcard bind both
            // change the answer, and the token rule follows what was actually bound.
            let addr = listener.local_addr().map_err(Error::Io)?;
            Ok(Transport::Tcp { listener, addr })
        }
        (None, None, Some(options)) => {
            #[cfg(feature = "tls")]
            {
                let listener = TcpListener::bind(options.addr).map_err(Error::Io)?;
                listener.set_nonblocking(true).map_err(Error::Io)?;
                let addr = listener.local_addr().map_err(Error::Io)?;
                Ok(Transport::Https {
                    listener,
                    addr,
                    cert: options.cert,
                    key: options.key,
                })
            }
            #[cfg(not(feature = "tls"))]
            {
                let _ = options;
                Err(Error::Transport(
                    "https:// needs a build with the `tls` feature".to_string(),
                ))
            }
        }
        // Unreachable by the count above — and written as an `Err` rather than
        // `unreachable!()` because this is library code, where the crate's constraints forbid
        // panics and a `clippy::unreachable` under `-D warnings` would have to be allowed
        // anyway. It is the same refusal the count already made, spelled where the compiler
        // can see it — which is why both hands back come from [`more_than_one`].
        _ => Err(more_than_one()),
    }
}

/// The refusal the count check above and the compiler-visible `_` arm both give: one definition,
/// so the live arm and the dead one cannot drift.
fn more_than_one() -> Error {
    Error::Transport(
        "more than one management transport is enabled: the local channel, http and https are mutually exclusive"
            .to_string(),
    )
}

/// The absolute form of the local channel's socket path: an absolute path is used verbatim, a
/// relative one joins `state_dir` (design decision 24). `state_dir` is absolute — the caller
/// resolved it — so the answer is too, which is what `state.control_name` needs.
///
/// The CLI resolves its configured path through this too, rather than joining it itself: the
/// rule has one owner, so the path recorded in `state.control_name` is the path bound.
pub fn resolve_socket_path(path: &Path, state_dir: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        state_dir.join(path)
    }
}

/// Serves `shared` over a bound TCP listener until `stop` flips, then drains and returns.
///
/// The `std` listener `select` bound becomes tokio's here — inside the runtime, which is the
/// only place `from_std` may be called — and the graceful shutdown is what makes "cease
/// accepting, drain, then exit" one mechanism for the data plane and the control plane alike
/// (decision 12; the ≤ 10 s bound itself is the CLI's, which awaits this future).
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context},
///     state::State,
///     transport::serve_tcp,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// // The shape `select` produces: bound as `std`, non-blocking, converted here.
/// let listener = TcpListener::bind("127.0.0.1:9001")?;
/// listener.set_nonblocking(true)?;
/// let addr = listener.local_addr()?;
/// let state = State::new(addr.to_string(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), stop));
/// serve_tcp(listener, shared, rx).await
/// # }
/// ```
pub async fn serve_tcp(
    listener: TcpListener,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let listener = TokioTcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(listener, router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("tcp channel: {err}")))
}

/// Serves `shared` over TLS with the PEM pair at `cert`/`key`, until `stop` flips.
///
/// The `std` listener `select` bound becomes tokio's **here, inside the runtime**, for the reason
/// [`serve_tcp`] documents: `from_std` panics without a reactor, and `select` ran before one
/// existed. Everything after that conversion is `serve_tcp`'s shape — the same router, the same
/// `until_stopped` — with [`TlsListener`] terminating the handshake on each connection.
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, path::Path, sync::Arc};
///
/// use tinio_api::{
///     router::{Auth, Context},
///     state::State,
///     transport::serve_https,
/// };
/// use tokio::sync::watch;
///
/// # async fn serve() -> Result<(), tinio_api::Error> {
/// // The shape `select` produces: bound as `std`, non-blocking, converted inside.
/// let listener = TcpListener::bind("127.0.0.1:9443")?;
/// listener.set_nonblocking(true)?;
/// let addr = listener.local_addr()?;
/// let state = State::new(addr.to_string(), None, Path::new("/srv/data"))?;
/// let (stop, rx) = watch::channel(false);
/// let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), stop));
/// serve_https(
///     listener,
///     Path::new("/etc/tinio/cert.pem"),
///     Path::new("/etc/tinio/key.pem"),
///     shared,
///     rx,
/// )
/// .await
/// # }
/// ```
#[cfg(feature = "tls")]
pub async fn serve_https(
    listener: TcpListener,
    cert: &Path,
    key: &Path,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    let config = tls_config(cert, key)?;
    // The conversion, inside the runtime: `select` bound this before one existed.
    let listener = TokioTcpListener::from_std(listener).map_err(Error::Io)?;
    axum::serve(TlsListener::new(listener, config), router(shared))
        .with_graceful_shutdown(until_stopped(stop))
        .await
        .map_err(|err| Error::Transport(format!("https channel: {err}")))
}

/// The rustls server config, from a PEM chain and key.
///
/// No client authentication: the management plane authenticates with the token header, and a
/// client certificate would be a second scheme nobody configured.
#[cfg(feature = "tls")]
fn tls_config(cert: &Path, key: &Path) -> Result<Arc<ServerConfig>, Error> {
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(
        fs::File::open(cert).map_err(Error::Io)?,
    ))
    .collect::<Result<Vec<_>, _>>()
    .map_err(|err| Error::Transport(format!("{}: {err}", cert.display())))?;
    if certs.is_empty() {
        return Err(Error::Transport(format!(
            "{}: no certificate",
            cert.display()
        )));
    }
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(
        fs::File::open(key).map_err(Error::Io)?,
    ))
    .map_err(|err| Error::Transport(format!("{}: {err}", key.display())))?
    .ok_or_else(|| Error::Transport(format!("{}: no private key", key.display())))?;

    // No process-level provider installation needed, and two reasons — the only two that hold.
    // **One provider is compiled in**: the workspace pin (`tokio-rustls` with
    // `default-features = false` and `ring`, `docs/cargo.md`) admits no other, so `builder()`
    // cannot hit the ambiguous-provider panic that would otherwise require an explicit
    // `CryptoProvider::install_default`. **No CMake or NASM is required**: `ring` is not a
    // pure-Rust provider and does build C — it lists `cc` and pulls `windows-sys 0.52.0` into
    // `Cargo.lock` — but that is a plain C compile, where `aws-lc-rs` would additionally demand
    // both CMake and NASM. Do not restate this as "no C toolchain".
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| Error::Transport(format!("TLS configuration: {err}")))?;
    Ok(Arc::new(config))
}

/// An axum [`Listener`] that terminates TLS on each accepted connection,
/// completing the handshake before it hands the connection over.
///
/// **The handshake runs inside `accept`, and that is the whole reason `HANDSHAKE_TIMEOUT`
/// exists.** `TlsAcceptor::accept` has no timeout of its own, and axum spawns its per-connection
/// task only *after* `Listener::accept` returns (`axum-0.8.9/src/serve/mod.rs:189-191`), so
/// without the timeout a client that connected and then said nothing would park the accept loop —
/// and `/status`, `/stop` and the readiness probe behind it — for as long as it stayed connected,
/// unauthenticated, at the price of one TCP connection.
///
/// **No `Debug` derive, deliberately**: [`tokio_rustls::TlsAcceptor`] implements only `Clone`
/// (`tokio-rustls-0.26.5/src/server.rs:18-21`), so deriving `Debug` here does not compile, and
/// nothing needs it — `axum::serve` awaits a listener without printing it.
///
/// # Examples
///
/// ```no_run
/// use std::{net::TcpListener, sync::Arc};
///
/// use tinio_api::transport::TlsListener;
/// use tokio_rustls::rustls::ServerConfig;
///
/// # fn config() -> Arc<ServerConfig> { unimplemented!() }
/// # fn router() -> axum::Router { unimplemented!() }
/// # async fn serve() -> Result<(), Box<dyn std::error::Error>> {
/// let listener = TcpListener::bind("127.0.0.1:9443")?;
/// listener.set_nonblocking(true)?;
/// // `from_std` needs the reactor, so the conversion belongs inside the runtime; `serve_https`
/// // is where this crate does it.
/// let listener = tokio::net::TcpListener::from_std(listener)?;
/// // `axum::serve` drives it like any other listener: every connection it accepts has already
/// // completed its handshake, and every one it could not complete was dropped on its own.
/// axum::serve(TlsListener::new(listener, config()), router()).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "tls")]
pub struct TlsListener {
    inner: TokioTcpListener,
    acceptor: TlsAcceptor,
}

/// How long a connection may take to complete its TLS handshake before it is dropped.
///
/// Five seconds is orders of magnitude more than a real handshake needs on a management port,
/// and the bound is what keeps a silent or stalled client from holding the accept loop
/// (`TlsListener`'s doc states the mechanism). It is a liveness floor, not a performance target:
/// nothing in this crate measures a handshake.
#[cfg(feature = "tls")]
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "tls")]
impl TlsListener {
    /// Wraps a bound TCP listener, terminating TLS with `config`.
    ///
    /// The listener arrives as tokio's own type, never `std`'s: `from_std` needs the reactor, so
    /// the conversion belongs at the call site that has one — which is
    /// [`serve_https`], not here.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::{net::TcpListener, sync::Arc};
    ///
    /// use axum::serve::Listener;
    /// use tinio_api::transport::TlsListener;
    /// use tokio_rustls::rustls::ServerConfig;
    ///
    /// # fn config() -> Arc<ServerConfig> { unimplemented!() }
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let listener = TcpListener::bind("127.0.0.1:9443")?;
    /// listener.set_nonblocking(true)?;
    /// let listener = tokio::net::TcpListener::from_std(listener)?;
    /// let listener = TlsListener::new(listener, config());
    /// // It is still a listener: the bound address answers as it did before the wrap.
    /// assert_eq!(listener.local_addr()?.port(), 9443);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(listener: TokioTcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            inner: listener,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

#[cfg(feature = "tls")]
impl Listener for TlsListener {
    type Addr = SocketAddr;
    type Io = TlsStream<TcpStream>;

    /// The trait's method has **no** `Result` (`axum-0.8.9/src/serve/listener.rs:9-24`: "if the
    /// underlying accept call can return an error, this function must take care of logging and
    /// retrying"), so both failures below are absorbed here rather than surfaced: an `accept`
    /// failure retries with axum's own back-off — silence for a client hang-up, one `error!` and
    /// a one-second sleep for anything else, on every occurrence, which is what keeps a persistent
    /// `EMFILE` a slow loop instead of a spin (`listener.rs:140-163`) — and a handshake that
    /// fails or times out drops that one client, never the loop.
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    // The back-off goes with the log, exactly as axum pairs them: a connection
                    // error is routine and retried at once, anything else is logged and slept on
                    // so a listener out of handles or out of addresses cannot spin.
                    if !is_connection_error(&err) {
                        tracing::error!(error = %err, "the TLS listener's accept failed");
                        sleep(Duration::from_secs(1)).await;
                    }
                    continue;
                }
            };
            match timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(stream)).await {
                Ok(Ok(stream)) => return (stream, addr),
                Ok(Err(err)) => {
                    tracing::debug!(%addr, error = %err, "TLS handshake failed");
                }
                Err(_) => {
                    // The socket goes with the elapsed future: the client that never spoke
                    // releases the loop instead of holding it.
                    tracing::debug!(%addr, "TLS handshake timed out");
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// Whether an accept error is a client hanging up rather than a listener problem: those are
/// routine, and logging each one would be noise, not information (`axum`'s own three kinds).
#[cfg(feature = "tls")]
fn is_connection_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

impl Transport {
    /// What `state.control_name` records: the bound socket path, the pipe name, or the
    /// address. A client reads this value out of `state` and never re-derives it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_api::transport::{Selection, select};
    ///
    /// let transport = select(Selection {
    ///     http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///     ..Selection::default()
    /// })
    /// .expect("bind");
    /// assert_eq!(transport.identity(), "127.0.0.1:9001");
    /// ```
    pub fn identity(&self) -> String {
        match self {
            Self::Local { channel } => channel.identity(),
            Self::Tcp { addr, .. } => addr.to_string(),
            #[cfg(feature = "tls")]
            Self::Https { addr, .. } => addr.to_string(),
        }
    }

    /// The token rule this transport carries (`management-api.md`'s matrix).
    ///
    /// The rule is derived here rather than stored: the local channel is always [`Auth::Local`],
    /// and a TCP transport is [`Auth::for_bind`] of the address it **bound** — so the
    /// bind-address rule has one home, and a transport cannot carry a rule that disagrees with
    /// its own address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_api::{
    ///     router::Auth,
    ///     transport::{Selection, select},
    /// };
    ///
    /// // The rule follows the *bind* address: a reachable one is the strict rule.
    /// let transport = select(Selection {
    ///     http: Some("0.0.0.0:9001".parse().expect("addr")),
    ///     ..Selection::default()
    /// })
    /// .expect("bind");
    /// assert_eq!(transport.auth(), Auth::Tcp);
    /// ```
    pub fn auth(&self) -> Auth {
        match self {
            Self::Local { .. } => Auth::Local,
            Self::Tcp { addr, .. } => Auth::for_bind(*addr),
            #[cfg(feature = "tls")]
            Self::Https { addr, .. } => Auth::for_bind(*addr),
        }
    }
}

/// What the CLI hands the plane: the transport input, where the plane's files go, and the
/// shutdown channel it sends on.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::{Options, Plane, transport::Selection};
/// use tokio::sync::watch;
///
/// # fn main() -> Result<(), tinio_api::Error> {
/// let (stop, _rx) = watch::channel(false);
/// let options = Options {
///     selection: Selection {
///         http: Some("127.0.0.1:9001".parse().expect("addr")),
///         ..Selection::default()
///     },
///     state_dir: PathBuf::from("/run/tinio"),
///     root: PathBuf::from("/srv/data"),
///     host: "127.0.0.1".to_string(),
///     stop,
///     gauges: None,
///     token: None,
/// };
/// let plane = Plane::bind(options)?;
/// plane.publish(9000)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Options {
    /// The three-choose-one input, as `[api]` and the `--api*` flags resolved it.
    pub selection: Selection,
    /// The resolved state directory (design decision 6) — the plane's only filesystem anchor,
    /// and absolute: a **relative** `[api.unix] path` in `selection` joins this directory
    /// before anything binds ([`Plane::bind`]), so what the bind creates and what `state`
    /// records are the same absolute path.
    pub state_dir: PathBuf,
    /// The served root, canonical: `state.root`, and the input the socket name derives from.
    pub root: PathBuf,
    /// The data plane's host, as `/status` reports it.
    pub host: String,
    /// The shutdown channel `/stop` sends on — the same channel the CLI's signal handlers
    /// flip and `DataPlane::serve` reads (decision 12).
    pub stop: watch::Sender<bool>,
    /// The scrape-time gauge source the plane's `/metrics` serves, or `None` for a plane
    /// without one (the route then answers the contract's `404`, `management-api.md:23`). It is
    /// an input rather than something the plane builds because only the caller can: the
    /// registry side is `tinio-server`'s, and `tinio-api` must not depend on that crate (design
    /// decision 23 — cargo refuses the cycle). The **reader task** that runs the source's
    /// expensive half starts with the plane, in [`Plane::serve`].
    pub gauges: Option<Arc<Gauges>>,
    /// The token this plane serves with, when the caller supplies one instead of letting the
    /// plane generate it.
    ///
    /// **`Some` is the launcher's token, and the launcher is also the process that wrote
    /// `state`** — which is why the caller has it to pass: a confined child on Windows cannot
    /// create that file, so the unrestricted launcher does, generating the token with the file.
    /// This plane then verifies `/status` and `/stop` against [`State::with_token`]'s value
    /// rather than a fresh one, so the token a client reads out of that file is the token that
    /// opens the channel (see the field's doc there for what a child that minted its own would
    /// answer). `None` is every plane that owns its `state` file, which is every unconfined run
    /// and every in-place platform — and it is what [`Plane::publish`] is for.
    pub token: Option<String>,
}

/// The `state` this plane serves under: the control channel's identity, the served root, and
/// **the caller's token when it supplied one** ([`Options::token`]) — a fresh one otherwise.
///
/// **One rule, two binds.** `Plane::bind` and `Plane::adopt` both build the state, and the
/// choice between "the launcher's token" and "one of my own" is the same choice in both — so it
/// is stated once here rather than twice, where a later edit to one of them could silently
/// leave the other minting a token for a file it does not own. The port is `None` at this point
/// in both: it is the data plane's, and `publish` is where it arrives.
fn state_for(
    token: Option<String>,
    control_name: impl Into<String>,
    root: &Path,
) -> Result<State, Error> {
    match token {
        Some(token) => State::with_token(control_name, None, root, token),
        None => State::new(control_name, None, root),
    }
}

/// The socket file the bind created, as the deletion path must still find it: the path, and the
/// identity it had then.
///
/// A successor's `bind_unix` reclaims a stale path by unlinking it and binding again
/// (`crate::bind::reclaim_stale` is what makes that safe to do behind a crashed instance), and
/// what it leaves at the same name is a **different file**. Removing by name alone would
/// therefore unlink a live server's socket — and, with the path free again, let a third server
/// bind it, which is single-instance enforcement gone as well as discovery. The recorded identity
/// is what tells the two apart. Windows carries no socket file at all (its `Channel` owns a
/// name, not a path), so there the path is all there is and nothing is ever constructed in
/// practice: [`Channel::socket_path`] answers `None` on that platform.
#[derive(Debug)]
struct SocketFile {
    /// The path the bind created.
    path: PathBuf,
    /// `(dev, ino)` at the bind (`std::os::unix::fs::MetadataExt`), which a replacement changes.
    #[cfg(unix)]
    id: (u64, u64),
}

impl SocketFile {
    /// Records the file at `path` as it is now — called immediately after the bind, so what is
    /// recorded is what the bind created.
    fn at(path: PathBuf) -> Result<Self, Error> {
        #[cfg(unix)]
        {
            let metadata = fs::metadata(&path).map_err(Error::Io)?;
            Ok(Self {
                path,
                id: (metadata.dev(), metadata.ino()),
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self { path })
        }
    }

    /// Whether the file at `path` is still the one recorded above: on unix, the same `(dev, ino)`.
    /// An unreadable file is **not** ours — an unproven claim is not a licence to delete — and a
    /// missing one has nothing left to remove, which the caller's `NotFound` arm also covers.
    fn still_bound(&self) -> bool {
        #[cfg(unix)]
        {
            match fs::metadata(&self.path) {
                Ok(metadata) => (metadata.dev(), metadata.ino()) == self.id,
                Err(_) => false,
            }
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}

/// The management plane: the bound transport, the router it serves, and the two files it owns.
///
/// **One owner for both artifacts** (decision 12): the plane creates the socket at
/// [`Plane::bind`] and owns the `state` file, so it is the plane that writes and removes them —
/// through [`Plane::publish`] (the readiness point) and [`Plane::shutdown`]. The CLI never
/// joins those file names itself, and [`Drop`] is a best-effort backstop for the paths that do
/// not go through `shutdown`.
///
/// **The client never deletes, and the reason is a race.** A `status` or `stop` run removes
/// nothing, because a client that unlinks a socket or a `state` file cannot know whether the
/// server behind it is still live. The one exception is `doctor --fix` (plan C), and it is not
/// a client: it reclaims a stale socket through `crate::bind::reclaim_stale` (the dry-run arm
/// is what its report is built from) and a stale `state` through [`State::remove`], and only
/// after a probe has proved no server answers for that root — the proof is what the rule
/// exists to require, not a second way around it.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_api::{Options, Plane, transport::Selection};
/// use tokio::sync::watch;
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // The CLI's sequence, in order: bind (which is the single-instance lock) → publish
/// // (the readiness point) → serve → shutdown (the plane's own deletions).
/// let (stop, rx) = watch::channel(false);
/// let plane = Plane::bind(Options {
///     selection: Selection {
///         http: Some("127.0.0.1:9001".parse().expect("addr")),
///         ..Selection::default()
///     },
///     state_dir: PathBuf::from("/run/tinio"),
///     root: PathBuf::from("/srv/data"),
///     host: "127.0.0.1".to_string(),
///     stop,
///     gauges: None,
///     token: None,
/// })?;
/// plane.publish(9000)?;
/// plane.serve(rx).await?;
/// plane.shutdown().await
/// # }
/// ```
#[derive(Debug)]
pub struct Plane {
    /// Taken (once) by `serve`: a listener cannot be cloned, so serving consumes it.
    transport: Mutex<Option<Transport>>,
    shared: Shared,
    state_dir: PathBuf,
    /// The socket file the bind created, with the identity it had then: the deletion path
    /// removes it only while it is still that file (see `Plane::remove_owned`). Windows never
    /// records one — the Windows `Channel` has no path — so it is always `None` there.
    socket: Option<SocketFile>,
}

impl Plane {
    /// [`Plane::bind`]'s mirror for a process a launcher **confined**: the control channel
    /// is already created, and this process serves on the instances it inherited.
    ///
    /// **Why adoption rather than a second bind.** The launcher binds the channel because
    /// only it can: the pipe's DACL grants the current user and nobody else, and inside a
    /// container this process's identity is the package SID — so its own
    /// `CreateNamedPipeW` is refused `ERROR_ACCESS_DENIED`, and a child that tried would
    /// fail its startup with a message about a second server. The launcher instead creates
    /// the pool, keeps the single-instance claim, and passes every handle the way it passes
    /// the data plane's listener (sandbox design decision 16, extended to the control
    /// channel); nothing about the DACL, the identity of the claimant, or the refusal a
    /// genuinely concurrent start gets changes.
    ///
    /// **The whole pool and not one handle**: `raws` is what the launcher handed over, and
    /// each element becomes an instance of the channel `serve` rotates (`crate::bind::INSTANCES`
    /// is the count both sides of that handoff use). A child given one instance is back to the
    /// defect this exists to remove — the name answers once and then goes deaf — so a handoff that
    /// carries too few is refused here rather than served as a channel that would stop answering.
    ///
    /// **The name is resolved here and not derived a second time.** `options.selection` is
    /// the same `Selection` the launcher bound — both processes resolve it through the
    /// CLI's one `api_options` rule against the same root — and `crate::bind::pipe_name`
    /// normalises it exactly as [`crate::bind::bind_pipe`] did,
    /// so `state.control_name` records the name the pipe actually carries. The local form
    /// must be a [`LocalForm::Pipe`]: this platform's channel is a pipe, and a selection
    /// that names none is the launcher's own contradiction rather than a run this process
    /// may serve.
    ///
    /// The rest is [`Plane::bind`]'s, deliberately in the same order: the identity is
    /// recorded from the channel, the `state` file is built around it, and the router is
    /// built with the token rule for the local channel. No socket file is recorded —
    /// [`Channel::socket_path`] answers `None` here — which is why [`Plane::shutdown`]
    /// removes only the `state` file on this platform.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] for a selection with no pipe channel in it or a handoff
    /// shorter than `crate::bind::INSTANCES`, and [`crate::Error::Io`] when an inherited handle
    /// is not one — a null value, which no real handoff produces. An unreadable served root is
    /// [`crate::Error::State`], as in the bind.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(windows)]
    /// # {
    /// use std::os::windows::io::RawHandle;
    ///
    /// use tinio_api::{
    ///     Options, Plane,
    ///     transport::{LocalForm, Selection},
    /// };
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // The handles a launcher put on the child's argv — the values `Adopted::controls()`
    /// // carried, one instance each. `crate::bind::INSTANCES` of them: `adopt` refuses fewer,
    /// // because one instance makes the name deaf after its first answer.
    /// # let inherited: [RawHandle; 2] = [0x1000usize as _, 0x1004usize as _];
    /// let plane = Plane::adopt(
    ///     Options {
    ///         selection: Selection {
    ///             local: Some(LocalForm::Pipe(r"\\.\pipe\tinio-mine".to_string())),
    ///             ..Selection::default()
    ///         },
    ///         state_dir: "/run/tinio".into(),
    ///         root: "/srv/data".into(),
    ///         host: "127.0.0.1".to_string(),
    ///         stop,
    ///         gauges: None,
    ///         // The launcher's token, when this plane is the child it confined.
    ///         token: None,
    ///     },
    ///     &inherited,
    /// )?;
    /// // **No `publish` here, and that is the ruling rather than an omission**: this plane's
    /// // `state` file is the launcher's — it wrote it, with the token above — and this process
    /// // could not write it if it tried (the file's DACL names the user, and a confined child's
    /// // token does not). The plane serves; the launcher publishes.
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    #[cfg(windows)]
    pub fn adopt(options: Options, raws: &[RawHandle]) -> Result<Self, Error> {
        // The local form, resolved for its **name** alone — the handles are the objects, and
        // this name is what the state file records. Same rule, same definer as the bind's.
        let Some(LocalForm::Pipe(name)) = options.selection.local.as_ref() else {
            return Err(Error::Transport(
                "a confined child must be handed a named-pipe control channel: this selection \
                 names none, so there is nothing to adopt"
                    .to_string(),
            ));
        };
        // **Fewer instances than the channel needs is a refused handoff, not a channel to serve.**
        // A client only ever reaches a *listening* instance, and this process cannot create the
        // next one — `CreateNamedPipeW` on the channel's current-user-only DACL is refused inside
        // the container — so a pool of one answers its first client and is then connected for as
        // long as it serves: the name goes deaf, which is exactly the defect the pool exists to
        // remove. `bind::INSTANCES` is the floor and the same constant the bind creates and the
        // launcher hands over, so a future change to the pool size moves this check with it
        // instead of leaving a silent downgrade behind. More than the floor is fine: extra
        // instances only mean more listeners.
        if raws.len() < bind::INSTANCES {
            return Err(Error::Transport(format!(
                "a confined child must be handed the control channel's whole pool: {} instance(s) \
                 were handed over and {} are needed — one instance makes the name deaf after its \
                 first answer, and this process cannot create the next one",
                raws.len(),
                bind::INSTANCES
            )));
        }
        let name = bind::pipe_name(name);
        // The raw conversion, and it is `dacl`'s: this crate's designated `unsafe` site, where
        // the launcher's handoff is the safety argument. One instance per inherited handle.
        let mut handles = Vec::with_capacity(raws.len());
        for raw in raws {
            handles.push(dacl::adopted_pipe(*raw)?);
        }
        let transport = Transport::Local {
            channel: Channel { name, handles },
        };
        Self::assemble(options, transport)
    }

    /// [`Plane::bind`]'s mirror for a process a launcher **confined**: the local control channel
    /// is already bound, and this process serves on the socket it inherited.
    ///
    /// **Why adoption rather than a second bind.** The macOS profile grants the confined child no
    /// `network-bind` at all, so a child that called [`crate::bind::bind_unix`] would die with
    /// `could not bind … Operation not permitted` — and a rule that permitted the bind would be a
    /// grant on every socket rather than a filter on the one the child wants. The launcher is
    /// unconfined, so *it* binds the channel, and the child serves on the descriptor it inherits;
    /// the profile therefore carries no network rule, exactly as it carries none for the data
    /// plane, whose listener travels the same way (sandbox design decision 16, extended to the
    /// control channel).
    ///
    /// **The single-instance claim is the launcher's, and it is neither weakened nor duplicated.**
    /// The launcher's [`crate::bind::bind_unix`] is the bind that reclaimed a stale path and
    /// claimed the socket file, so a second server on the same root is refused there — before any
    /// child exists. This function binds nothing: it records the channel it was handed.
    ///
    /// **The identity is resolved here through the one definer, and not re-derived from the
    /// descriptor.** `options.selection` is the same `Selection` the launcher bound — both
    /// processes resolve it through the CLI's one `api_options` rule against the same root — and
    /// [`resolve_socket_path`] turns its `[api.unix] path` into the absolute path
    /// [`Plane::bind`] would have bound, so `state.control_name` records the path the launcher's
    /// [`crate::bind::Channel::identity`] reported and the child actually serves on. The local
    /// form must be a [`LocalForm::Unix`]: this platform's channel is a socket file, and a
    /// selection that names none is the launcher's own contradiction rather than a run this
    /// process may serve.
    ///
    /// The rest is [`Plane::bind`]'s, deliberately in the same order: the identity is recorded
    /// from the channel, the `state` file is built around it, and the router is built with the
    /// token rule for the local channel. The socket file is recorded too — the launcher created
    /// it, and the child is the process that serves on it and so the one that removes it on the
    /// way out, under the same identity check [`Plane::bind`]'s deletion uses.
    ///
    /// **The listener arrives as an object and not as an integer.** Turning an inherited
    /// descriptor number back into a socket is `unsafe`, and this crate carries no site for it;
    /// the platform crate that rebuilds it (the macOS backend's
    /// `tinio_sandbox_macos::launch::control`, one of that crate's **three** enumerated
    /// `#[allow(unsafe_code)]` sites — the `inherited_fd` rebuild, the libSystem FFI module the
    /// in-process mechanism applies through, and that mechanism's validator's `fork`) is what
    /// hands the object over.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Transport`] for a selection with no unix channel in it, and
    /// [`crate::Error::State`] for an unreadable served root — the bind's own failures, minus
    /// everything about binding.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(unix)]
    /// # {
    /// use std::{os::unix::net::UnixListener, path::PathBuf};
    ///
    /// use tinio_api::{
    ///     Options, Plane,
    ///     transport::{LocalForm, Selection},
    /// };
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // The launcher's half: it bound the socket and cleared `FD_CLOEXEC` on it. What this
    /// // process holds is the descriptor that crossed the exec, rebuilt into a listener by the
    /// // platform crate.
    /// let listener = UnixListener::bind("/tmp/tinio-0123456789abcdef.sock")?;
    /// let plane = Plane::adopt(
    ///     Options {
    ///         selection: Selection {
    ///             local: Some(LocalForm::Unix(PathBuf::from(
    ///                 "/tmp/tinio-0123456789abcdef.sock",
    ///             ))),
    ///             ..Selection::default()
    ///         },
    ///         state_dir: PathBuf::from("/tmp"),
    ///         root: PathBuf::from("/srv/data"),
    ///         host: "127.0.0.1".to_string(),
    ///         stop,
    ///         gauges: None,
    ///         // The launcher's token, when this plane is the child it confined.
    ///         token: None,
    ///     },
    ///     listener,
    /// )?;
    /// // **No `publish` here, and that is the ruling rather than an omission**: this plane's
    /// // `state` file is the launcher's — it wrote it, with the token above — so the plane
    /// // serves, and the launcher publishes.
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    #[cfg(unix)]
    pub fn adopt(options: Options, listener: UnixListener) -> Result<Self, Error> {
        // The local form, resolved for its **path** alone — the socket is the object, and this
        // path is what the state file records. Same rule, same definer as the bind's.
        let Some(LocalForm::Unix(path)) = options.selection.local.as_ref() else {
            return Err(Error::Transport(
                "a confined child must be handed a unix control channel: this selection names \
                 none, so there is nothing to adopt"
                    .to_string(),
            ));
        };
        let channel = Channel {
            listener,
            path: resolve_socket_path(path, &options.state_dir),
        };
        let transport = Transport::Local { channel };
        Self::assemble(options, transport)
    }

    /// Bind a new control channel, or adopt the one a launcher already handed this process.
    ///
    /// An empty `controls` list is every run that owns the single-instance claim itself: unconfined,
    /// in-place, or a selection this process binds. A non-empty list is the inherited pool.
    /// Windows adopts those values as pipe handles. macOS adopts `socket`, the listener the sandbox
    /// rebuilt from the inherited descriptor — this crate does not turn a descriptor number back
    /// into a socket. Any other target refuses a non-empty list: an in-place process was not handed
    /// a channel.
    ///
    /// **`socket` is a parameter on macOS alone, and it is `#[cfg]`'d rather than passed by every
    /// caller.** It used to be an `Option<InheritedSocket>` everywhere — a placeholder struct with
    /// an `absent()` constructor on the targets that inherit no unix control socket — which made
    /// four places agree about a value two of the three platforms never read: the two type
    /// definitions, the caller's conversion into them, and the `let _ = socket;` this body used to
    /// write. The axis is the sandbox facade's own (`tinio_sandbox::platform_facts` reports a
    /// control socket on macOS alone, and `tinio_sandbox::adopted_control` succeeds for that target
    /// only), so the parameter carries the same `#[cfg]`. Windows never names it — its control
    /// channel is a named pipe whose handles travel as `controls` values — and neither does Linux,
    /// whose confinement is in place and whose serving process is the one that bound. **The
    /// signature therefore differs by target**, which is the honest spelling of a handoff only one
    /// mechanism performs.
    ///
    /// # Errors
    ///
    /// The bind's errors, the adopt's errors, a macOS handoff with no socket, or a non-empty list
    /// on a target that does not launch a child.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// let options = Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    /// };
    /// // An empty list binds on every target, which is the arm this example can spell once — and
    /// // the argument macOS alone carries is spelled on the arm that has it.
    /// # #[cfg(target_os = "macos")]
    /// let plane = Plane::open(options, &[], None)?;
    /// # #[cfg(not(target_os = "macos"))]
    /// let plane = Plane::open(options, &[])?;
    /// let _ = plane;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(
        options: Options,
        controls: &[i64],
        #[cfg(target_os = "macos")] socket: Option<UnixListener>,
    ) -> Result<Self, Error> {
        if controls.is_empty() {
            return Self::bind(options);
        }
        #[cfg(windows)]
        {
            let raws: Vec<RawHandle> = controls
                .iter()
                .copied()
                .map(|raw| raw as usize as RawHandle)
                .collect();
            Self::adopt(options, &raws)
        }
        #[cfg(target_os = "macos")]
        {
            let Some(socket) = socket else {
                return Err(Error::Transport(
                    "a confined child must be handed a unix control channel: the inherited \
                     descriptors name one, and this process was not given the socket"
                        .to_string(),
                ));
            };
            Self::adopt(options, socket)
        }
        #[cfg(not(any(windows, target_os = "macos")))]
        {
            Err(Error::Transport(
                "this platform confines in place and does not adopt an inherited control channel"
                    .to_string(),
            ))
        }
    }

    /// Applies the three-choose-one rule and binds the winner — which **is** the
    /// single-instance lock (decision 12) — then builds the router around it.
    ///
    /// The local form is resolved to an **absolute** path *before* the bind (design decision 24,
    /// and the rule [`crate::state::socket_path`]'s doc states): an absolute `[api.unix] path`
    /// is used verbatim, a relative one joins `state_dir`, and the path the bind creates is
    /// therefore the identity `state.control_name` records. A relative name would stop
    /// resolving the moment the daemon `chdir("/")`s — which is exactly when `status` and
    /// `stop` need to dial it — so this is a correctness step, not a convenience.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// // No runtime in sight: the TCP arm binds with `std` and `serve` converts it.
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    /// })?;
    /// plane.publish(9000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(mut options: Options) -> Result<Self, Error> {
        // **The selection leaves `options` rather than being cloned out of it.** `select` consumes
        // the selection while [`Plane::assemble`] takes the whole `Options`, and the two needs used
        // to be met with a clone — of three `Option`s, which is why it was affordable, and of
        // nothing the assemble reads, which is why the take is the same shape with no copy. What
        // stays behind is `Selection::default()`, and nothing downstream looks at it: the assemble
        // reads the token, the root, the host, the stop channel, the gauges and the state dir, and
        // the transport `select` just built is its answer to the selection.
        let mut selection = mem::take(&mut options.selection);
        if let Some(LocalForm::Unix(path)) = &mut selection.local {
            *path = resolve_socket_path(path, &options.state_dir);
        }
        let transport = select(selection)?;
        Self::assemble(options, transport)
    }

    /// The tail [`Plane::bind`] and both [`Plane::adopt`]s share: the `state` file built from the
    /// transport's identity, the socket file that identity records when the transport has a path,
    /// and the router around both.
    ///
    /// **One rule, three constructions**, and the order is the bind's: the identity is recorded as
    /// the socket file is built because that is the only moment it is certainly this plane's file
    /// (`Plane::remove_owned` compares against it later), and the `state` file is built around
    /// the identity before the router exists. Both adopts hand over a channel the **launcher**
    /// created, so what they record is the launcher's file as it stands when they read it; that is
    /// the same rule, not a second one.
    ///
    /// **The `_ => None` arm is reachable, and [`Plane::bind`] is what reaches it.** [`select`]
    /// answers [`Transport::Tcp`] (and `Https`, under `tls`) for an http/https selection, and
    /// neither carries a socket path; only [`Transport::Local`] has one to record. Both adopts
    /// construct `Local` and so never arrive there — but the arm is the bind's, not theirs, so it
    /// stays in the one shape all three go through rather than being narrowed to the adopts.
    ///
    /// **What is recorded is not checked against `options.selection`.** The adopts trust the
    /// object they were handed to be the channel at the path that selection names — meeting by
    /// construction, which is the launcher's and the platform crate's to guarantee, as
    /// [`Plane::adopt`] states; nothing here re-derives or compares the two.
    ///
    /// The caller's source travels in with the context and nowhere else, so the route's default
    /// state stays `None` (`Context::new`): a plane built without one has no `/metrics`, and the
    /// API tests can assert both halves of that.
    fn assemble(options: Options, transport: Transport) -> Result<Self, Error> {
        let state = state_for(options.token.clone(), transport.identity(), &options.root)?;
        let socket = match &transport {
            Transport::Local { channel } => {
                channel.socket_path().map(SocketFile::at).transpose()?
            }
            _ => None,
        };
        let shared = Arc::new(
            Context::new(state, options.host, transport.auth(), options.stop)
                .with_gauges(options.gauges),
        );
        Ok(Self {
            transport: Mutex::new(Some(transport)),
            shared,
            state_dir: options.state_dir,
            socket,
        })
    }

    /// Serves until the shutdown channel flips — the plane's own `/stop` sends on that same
    /// channel — then stops accepting, drains the connections already accepted, and returns.
    /// The transport is taken out of the plane here, which makes this a one-shot call; the
    /// drain is `axum::serve`'s `with_graceful_shutdown` (and, on Windows, the pipe loop's
    /// `JoinSet`), and the ≤ 10 s bound on it is the CLI's — this future is what the CLI awaits
    /// with that bound (decision 12), so the plane never invents a second timeout.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # async fn run() -> Result<(), tinio_api::Error> {
    /// let (stop, rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    /// })?;
    /// // `/stop` sends on the very channel this awaits: the plane can end its own serve,
    /// // and the CLI's signal handlers flip the same one.
    /// plane.serve(rx).await
    /// # }
    /// ```
    pub async fn serve(&self, stop: watch::Receiver<bool>) -> Result<(), Error> {
        let transport = self
            .transport
            .lock()
            .map_err(|_| Error::Internal("the plane's transport lock was poisoned".to_string()))?
            .take()
            .ok_or_else(|| Error::Internal("the plane is already serving".to_string()))?;

        // The metrics reader starts here, the one place that has both the `Arc<Gauges>` and the
        // runtime: it watches this same channel, so it exits with the plane rather than being
        // aborted. Holding the handle is what makes that visible in this frame, and nothing joins
        // it — a dropped `JoinHandle` only **detaches** its task, so `let _ = ...` here would
        // leave the very same loop running; what ends it is the channel, not this function's
        // return. A plane with `gauges: None` starts nothing.
        let _reader = self
            .shared
            .gauges()
            .map(|gauges| gauges.spawn_reader(stop.clone()));
        serve_transport(transport, self.shared.clone(), stop).await
    }

    /// Writes `state` with the data plane's port — the readiness point (decision 12), after
    /// both planes are bound. `/status` reports the same port from here on, and a second call
    /// changes neither it nor the file: published once, the first port is the one both carry.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # fn main() -> Result<(), tinio_api::Error> {
    /// let (stop, _rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    /// })?;
    /// // The data plane's port, and the moment `status` can find this server.
    /// plane.publish(9000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn publish(&self, port: u16) -> Result<(), Error> {
        // Published once: a second call keeps the first port, which is what the file holds and
        // what `/status` reports — so the two cannot disagree.
        if self.shared.port.get().is_some() {
            return Ok(());
        }
        let mut state = self.shared.state.clone();
        state.port = Some(port);
        state.write(&self.state_dir)?;
        let _ = self.shared.port.set(port);
        Ok(())
    }

    /// The plane's last act, and the plane's only deletion site — `doctor --fix` reclaims the
    /// same two files, but only after proving no server answers, through `reclaim_stale` and
    /// `State::remove`. The serves have already ended on the shared channel (the CLI's bounded
    /// `≤ 10 s` wait is on their futures). Async so the CLI's shutdown path is one `await` in
    /// the sequence that also awaits the data plane's drain.
    ///
    /// **It removes only what this plane still owns** — the socket file when it is still the one
    /// the bind created, and `state` when it still names this process. The gate is released
    /// before this runs, so a successor can already hold the path (see `Plane::remove_owned`);
    /// deleting by name alone would strip that live server of both artifacts.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::PathBuf;
    ///
    /// use tinio_api::{Options, Plane, transport::Selection};
    /// use tokio::sync::watch;
    ///
    /// # async fn run() -> Result<(), tinio_api::Error> {
    /// let (stop, rx) = watch::channel(false);
    /// let plane = Plane::bind(Options {
    ///     selection: Selection {
    ///         http: Some("127.0.0.1:9001".parse().expect("addr")),
    ///         ..Selection::default()
    ///     },
    ///     state_dir: PathBuf::from("/run/tinio"),
    ///     root: PathBuf::from("/srv/data"),
    ///     host: "127.0.0.1".to_string(),
    ///     stop,
    ///     gauges: None,
    ///     token: None,
    /// })?;
    /// plane.publish(9000)?;
    /// plane.serve(rx).await?;
    /// // The socket and `state` are the plane's to remove, after the serves have ended.
    /// plane.shutdown().await
    /// # }
    /// ```
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.remove_owned()
    }

    /// Removes the two artifacts this plane still owns — the last act of [`Plane::shutdown`],
    /// and of [`Drop`] when a caller never reaches it. One implementation, so the two cannot
    /// drift; only their error handling differs.
    ///
    /// **Ownership, not liveness.** The plane's bind *is* the single-instance gate, and the gate
    /// is released the moment `serve` returns — while the CLI still has the data plane's drain to
    /// await before it calls `shutdown`. A successor that passes the gate inside that window (the
    /// unix bind reclaims whatever stale path it finds) has by then published its own `state` and
    /// bound its own socket, so a deletion by name alone would delete **the successor's** live
    /// socket and `state` — stripping a running server of discovery and, with the path free
    /// again, letting a third one in. Each artifact is therefore checked against what this plane
    /// created: the socket by file identity (`(dev, ino)`, which a replacement changes) and
    /// `state` by the pid it names (`State::new` writes this process's; a successor's names its
    /// own).
    ///
    /// Anything not demonstrably ours is left alone: a `state` file that will not parse, cannot
    /// be read, or names another process is not this plane's to remove.
    fn remove_owned(&self) -> Result<(), Error> {
        // One condition, two halves of "ours": the plane bound a socket file at all, and the file
        // at that path is still the one it bound.
        if let Some(socket) = &self.socket
            && socket.still_bound()
        {
            remove_if_present(&socket.path)?;
        }
        match State::read(&self.state_dir) {
            Ok(Some(state)) if state.pid == process::id() => State::remove(&self.state_dir),
            // `Ok(None)` — nothing was published, or a successor has already removed it — and
            // every unreadable or foreign file land here, and all of them mean "not ours".
            _ => Ok(()),
        }
    }
}

impl Drop for Plane {
    /// Best-effort backstop only — the paths that matter call [`Plane::shutdown`], and this runs
    /// the same ownership-checked deletion (`Plane::remove_owned`) one last time. Errors are
    /// deliberately swallowed: nothing may panic on the way out of a failed startup.
    fn drop(&mut self) {
        if let Ok(mut transport) = self.transport.lock() {
            transport.take();
        }
        let _ = self.remove_owned();
    }
}

/// Dispatches to the transport's own serve. **The conversion into tokio's types happens in
/// the arms below, inside the runtime** — `serve_local` and `serve_tcp` take the `std`
/// listeners Task 2 and `select` bound before it existed (and `serve_https` converts too,
/// Task 6) — which is the whole reason `Plane::bind` can be the single-instance lock at a point
/// where no runtime exists.
async fn serve_transport(
    transport: Transport,
    shared: Shared,
    stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    match transport {
        Transport::Local { channel, .. } => serve_local(channel, shared, stop).await,
        Transport::Tcp { listener, .. } => serve_tcp(listener, shared, stop).await,
        // The `tls` arm, and the one place `select`'s HTTPS selection is served: the arm is
        // compiled out with the feature, so a build without it refuses the selection rather than
        // reaching here (that refusal is `select`'s, and Task 5's test covers it).
        #[cfg(feature = "tls")]
        Transport::Https {
            listener,
            cert,
            key,
            ..
        } => serve_https(listener, &cert, &key, shared, stop).await,
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::fs;
    // Under `tls` the glob below also brings in tokio's `TcpStream`; the stall test wants the
    // plain blocking one, and an explicit import shadows a glob.
    #[cfg(feature = "tls")]
    use std::net::TcpStream;
    #[cfg(unix)]
    use std::time::Duration;
    // The pipe tests' own imports: the raw dial, the silent connection the drain is asserted
    // on, and the completed-but-unclosed one the bound is. Nothing else in this module names
    // them.
    #[cfg(windows)]
    use std::time::Duration;
    #[cfg(feature = "tls")]
    use std::time::Instant;
    // No `Arc` here: the module's own import carries it in through `super::*`, and only the unix
    // and TLS legs of this module name it.
    use std::{
        net::{SocketAddr, TcpListener},
        path::{Path, PathBuf},
        process,
    };

    use http::Method;
    #[cfg(windows)]
    use hyper::{Request, client::conn::http1};
    use secrecy::ExposeSecret;
    use tempfile::TempDir;
    use tokio::sync::watch;
    #[cfg(unix)]
    use tokio::time::sleep;
    // The pipe tests' own import: the raw dial. Nothing else in this module names it.
    #[cfg(windows)]
    use tokio::{net::windows::named_pipe::ClientOptions, time::sleep};

    use super::*;
    #[cfg(feature = "tls")]
    use crate::test_support::{fixture_pair, https_get};
    #[cfg(unix)]
    use crate::{bind::bind_unix, test_support::sock};
    use crate::{
        client,
        router::{Auth, Context},
        state::State,
        test_support::gauged,
    };

    /// A `GET` call with the token when the leg has one: every request these tests make is one,
    /// and [`client::Call`] is the shape the crate's client takes.
    fn get<'a>(target: &'a str, token: Option<&'a str>) -> client::Call<'a> {
        client::Call {
            target,
            method: Method::GET,
            token,
        }
    }

    /// The one channel a plane uses: the sender lives in the `Context` (`/stop` sends on it),
    /// the receiver is handed to `serve_local` — the real wiring, so the tests exercise the
    /// shape the CLI uses rather than a second one.
    #[cfg(unix)]
    fn shared() -> (Shared, watch::Receiver<bool>) {
        let state = State::new(
            "tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state");
        let (tx, rx) = watch::channel(false);
        (
            Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx)),
            rx,
        )
    }

    /// A TCP plane over `dir`, which doubles as its state dir and its root: the shape a run
    /// binds when `[api]` selects http, and the one `Plane::bind` must reach with no runtime
    /// anywhere in scope (the rule `the_plane_binds_without_a_runtime` exists to keep).
    fn plane(dir: &Path) -> Plane {
        let (stop, _rx) = watch::channel(false);
        Plane::bind(Options {
            selection: Selection {
                http: Some("127.0.0.1:0".parse().expect("addr")),
                ..Selection::default()
            },
            state_dir: dir.to_path_buf(),
            root: dir.to_path_buf(),
            host: "127.0.0.1".to_string(),
            stop,
            gauges: None,
            token: None,
        })
        .expect("a TCP bind needs no runtime")
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_serves_status() {
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_local(channel, shared, stop));
        let status = client::get_local(&path, get("/status", Some(&token)))
            .await
            .expect("status");
        assert_eq!(status.status, 200);
        assert!(status.body.contains("\"running\":true"), "{}", status.body);

        serving.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_unix_channel_leaves_a_replaced_socket_alone() {
        // The bind's reclaim path must not touch a socket a live server is behind: this
        // test proves the pair — the second bind refuses rather than unlinking.
        let dir = TempDir::new().expect("temp dir");
        let path = sock(&dir);
        let channel = bind_unix(&path).expect("bind");
        let (shared, stop) = shared();
        let serving = tokio::spawn(serve_local(channel, shared, stop));
        sleep(Duration::from_millis(50)).await;

        assert!(bind_unix(&path).is_err(), "a live socket is not residue");
        serving.abort();
    }

    #[test]
    fn exactly_one_transport_may_be_enabled() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().expect("addr");
        let both = select(Selection {
            local: Some(LocalForm::Unix(PathBuf::from(
                "/tmp/tinio-0123456789abcdef.sock",
            ))),
            http: Some(addr),
            https: None,
        });
        assert!(
            both.is_err(),
            "the local channel and TCP are mutually exclusive"
        );

        let http_and_https = select(Selection {
            local: None,
            http: Some(addr),
            https: Some(HttpsOptions {
                addr,
                cert: PathBuf::from("/tmp/cert.pem"),
                key: PathBuf::from("/tmp/key.pem"),
            }),
        });
        #[cfg(feature = "tls")]
        assert!(
            http_and_https.is_err(),
            "http and https are mutually exclusive"
        );
        #[cfg(not(feature = "tls"))]
        assert!(
            http_and_https.is_err(),
            "https without the tls feature is a usage error"
        );

        let none = select(Selection {
            local: None,
            http: None,
            https: None,
        });
        assert!(none.is_err(), "no transport at all is a startup error");
    }

    /// The rule the whole task exists to keep, and the one the serving tests below cannot
    /// assert: **the bind needs no runtime.** `select` and `Plane::bind` run before the CLI's
    /// runtime exists (design §1: steps a–j precede k), which is what makes the plane the
    /// single-instance lock — so the TCP arm binds with `std` and `serve_tcp` converts inside
    /// the runtime. A plain `#[test]` has no reactor at all, so reaching for a tokio listener
    /// in either bind path stops this compiling rather than passing quietly.
    #[test]
    fn the_plane_binds_without_a_runtime() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());

        // `publish` is the readiness point and the only writer of `state` (decision 12).
        plane.publish(9000).expect("publish");
        let state = State::read(dir.path()).expect("read").expect("published");
        assert_eq!(state.port, Some(9000));
        assert!(
            state.control_name.starts_with("127.0.0.1:"),
            "the identity is the bound address: {}",
            state.control_name
        );

        // Published once: a second call is a no-op, so the file and `/status` cannot disagree.
        plane.publish(9001).expect("publish again");
        let state = State::read(dir.path()).expect("read").expect("published");
        assert_eq!(
            state.port,
            Some(9000),
            "the first port is the one `state` keeps"
        );

        // ...and the plane owns both artifacts, so `Drop` is the backstop that removes what
        // `publish` wrote on the paths that never reach `shutdown`.
        drop(plane);
        assert!(
            State::read(dir.path()).expect("read").is_none(),
            "dropping the plane removes the state file it published"
        );
    }

    /// **The two answers `state_for` has, and why the difference is load-bearing.** A plane that
    /// supplies a token is the confined child of a launcher that already published `state` with
    /// that token: minting a fresh one here would leave every client that found the server
    /// refused `401` by it. A plane that supplies none owns its `state` file, and generates the
    /// token it is about to write into it.
    #[test]
    fn a_supplied_token_is_the_one_the_plane_serves_with() {
        let dir = TempDir::new().expect("temp dir");
        let token = "ab".repeat(32);
        let adopted = state_for(Some(token.clone()), "127.0.0.1:9001", dir.path()).expect("state");
        assert_eq!(
            adopted.token.expose_secret().as_str(),
            token.as_str(),
            "the caller's token, verbatim"
        );

        let own = state_for(None, "127.0.0.1:9001", dir.path()).expect("state");
        assert_eq!(
            own.token.expose_secret().len(),
            64,
            "a fresh token, as `State::new` writes"
        );
        assert_ne!(own.token.expose_secret().as_str(), token.as_str());
    }

    /// The gate is released before the deletions: `serve` returns the moment the shared channel
    /// flips, and the CLI still has the data plane's drain to await before it calls `shutdown`
    /// (decision 12's sequence). A successor that passes the gate in that window has published
    /// its own `state`, so the predecessor must remove the file only while it is still its own.
    ///
    /// **This is also the launcher-written file's test, and the pid is what makes it one.** The
    /// confined child's plane reads a `state` file that names the launcher — not itself — and
    /// this is that same shape: the guard holds, so `shutdown` leaves the file alone and the
    /// child never even attempts the deletion its confinement would refuse.
    #[tokio::test]
    async fn a_state_file_from_another_process_survives_shutdown() {
        let dir = TempDir::new().expect("temp dir");
        let plane = plane(dir.path());

        let mut successor = State::new("127.0.0.1:9001", Some(9001), dir.path()).expect("state");
        successor.pid = process::id().wrapping_add(1);
        successor.write(dir.path()).expect("write");

        plane.shutdown().await.expect("shutdown");

        let kept = State::read(dir.path()).expect("read").expect("still there");
        assert_ne!(
            kept.pid,
            process::id(),
            "a successor's state file is not this plane's to remove"
        );
    }

    /// The socket half of the same rule: a successor's `bind_unix` reclaims the path by
    /// unlinking it and binding again, which leaves a different file at the same name. The
    /// predecessor must not unlink *that* — deleting a live server's socket would break
    /// discovery and, with the path free again, the single-instance gate with it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_replaced_socket_file_survives_shutdown() {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("tinio-0123456789abcdef.sock");
        let (stop, _rx) = watch::channel(false);
        let plane = Plane::bind(Options {
            selection: Selection {
                local: Some(LocalForm::Unix(path.clone())),
                ..Selection::default()
            },
            state_dir: dir.path().to_path_buf(),
            root: dir.path().to_path_buf(),
            host: "127.0.0.1".to_string(),
            stop,
            gauges: None,
            token: None,
        })
        .expect("bind");
        assert!(path.exists(), "the bind created the socket file");

        // What the successor leaves behind: the same name, a different file — and therefore a
        // different inode, which is the whole reason the deletion checks identity rather than
        // the path.
        fs::remove_file(&path).expect("remove the bound socket");
        fs::write(&path, b"a successor's socket").expect("replace");

        plane.shutdown().await.expect("shutdown");

        assert!(
            path.exists(),
            "a successor's socket file is not this plane's to remove"
        );
    }

    #[tokio::test]
    async fn a_loopback_tcp_plane_does_not_guard_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");
        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let (shared, _) = gauged(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        // A loopback bind has the local channel's trust level, so `/metrics` is open there:
        // the scrape reaches the handler over a real socket and answers the concrete `200`
        // (Task 7 registered the route, so the token-less request is no longer "anything but
        // a 401" — an unrouted path would have answered `404`).
        let open = client::get_http(addr, get("/metrics", None))
            .await
            .expect("request");
        assert_eq!(open.status, 200, "a loopback bind keeps /metrics open");
        let guarded = client::get_http(addr, get("/status", None))
            .await
            .expect("request");
        assert_eq!(
            guarded.status, 401,
            "/status still needs the token on a loopback bind"
        );
        let allowed = client::get_http(addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }

    #[tokio::test]
    async fn a_non_loopback_tcp_plane_requires_the_token() {
        // The rule follows the *bind* address, so a wildcard bind is the strict one; the
        // client still dials 127.0.0.1, because a wildcard is not a connect address.
        let listener = TcpListener::bind("0.0.0.0:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let bound = listener.local_addr().expect("addr");
        let addr = SocketAddr::from(([127, 0, 0, 1], bound.port()));
        let state = State::new(
            bound.to_string(),
            Some(bound.port()),
            Path::new("/srv/data"),
        )
        .expect("state");
        let (tx, stop) = watch::channel(false);
        let (shared, _) = gauged(Context::new(state, "0.0.0.0", Auth::for_bind(bound), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_tcp(listener, shared, stop));
        // Every registered endpoint, `/metrics` included: a wildcard bind is the strict one, so
        // the scrape needs the token here even though the client dials loopback. The source is
        // attached, which is what makes the 401 the guard's answer rather than a `404` from an
        // unrouted path (an unrouted path never reaches the middleware — the guard is a
        // `route_layer`, `router.rs`).
        let guarded = client::get_http(addr, get("/status", None))
            .await
            .expect("request");
        assert_eq!(
            guarded.status, 401,
            "a non-loopback bind guards every registered endpoint"
        );
        let guarded_metrics = client::get_http(addr, get("/metrics", None))
            .await
            .expect("request");
        assert_eq!(
            guarded_metrics.status, 401,
            "/metrics is guarded on a non-loopback bind too"
        );
        let tokened_metrics = client::get_http(addr, get("/metrics", Some(&token)))
            .await
            .expect("request");
        assert_eq!(tokened_metrics.status, 200, "the token opens the scrape");
        let allowed = client::get_http(addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(allowed.status, 200);

        serving.abort();
    }

    /// The HTTPS transport end to end: the `std` listener `select` produces, `serve_https`
    /// converting it inside the runtime, and a rustls client reading `/status` back over a real
    /// handshake — against the committed fixture pair, so nothing is generated at test time on any
    /// platform.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn https_serves_status_over_tls() {
        let (cert, key) = fixture_pair();
        // Bound as `std` and handed over unconverted: that is the shape `select` produces and
        // `serve_https` consumes, and converting here would test a wiring that does not exist.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");

        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        // The pair is moved into the spawned future rather than borrowed from the test body:
        // `tokio::spawn` wants `'static`, and a `&cert`/`&key` here would outlive nothing.
        let serving =
            tokio::spawn(async move { serve_https(listener, &cert, &key, shared, stop).await });
        let answer = https_get(&addr, get("/status", Some(&token)))
            .await
            .expect("request");
        assert_eq!(answer.status, 200, "{}", answer.body);

        serving.abort();
    }

    /// The property [`HANDSHAKE_TIMEOUT`] exists for, and the one the happy path cannot show: a
    /// client that connects and then says nothing must not take the accept loop with it.
    ///
    /// The bare socket below never sends a ClientHello, so its handshake can never complete, and
    /// `accept` is where that handshake runs — so without the timeout this test hangs rather than
    /// failing, which is exactly what a stalled client would do to `/status`, `/stop` and the
    /// readiness probe in production. With it, the silent connection is released and a real client
    /// is answered while the test's end of that socket is still open — the test never closes it,
    /// which is what makes the answer evidence that the loop was freed and not merely that the
    /// test gave up waiting.
    ///
    /// **The elapsed time is reported, not asserted on**: a five-second bound is a liveness floor
    /// ([`HANDSHAKE_TIMEOUT`]), and asserting a duration here would be a timing test that fails on
    /// a loaded machine.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn a_silent_connection_does_not_stall_the_tls_listener() {
        let (cert, key) = fixture_pair();
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.set_nonblocking(true).expect("nonblocking");
        let addr = listener.local_addr().expect("addr");

        let state =
            State::new(addr.to_string(), Some(addr.port()), Path::new("/srv/data")).expect("state");
        let (tx, stop) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::for_bind(addr), tx));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving =
            tokio::spawn(async move { serve_https(listener, &cert, &key, shared, stop).await });

        // Connected and silent: the accept loop is inside this one's handshake until the timeout
        // drops it, so nothing else is accepted in the meantime.
        let silent = TcpStream::connect(addr).expect("connect the silent socket");

        let started = Instant::now();
        let answer = https_get(&addr, get("/status", Some(&token)))
            .await
            .expect("request");
        let elapsed = started.elapsed();
        assert_eq!(answer.status, 200, "{}", answer.body);
        eprintln!("stall regression: /status answered in {elapsed:?} with a silent socket open");
        // Closed only here, after the assertion, and that ordering is the whole proof: `/status`
        // was answered while this connection was still open, because the test never closed it.
        // Nothing here can assert the socket's liveness from this side — the timeout closes the
        // *server's* end, so a peer check or a read would succeed or fail either way and would
        // prove nothing about what released the loop.
        drop(silent);
        serving.abort();
    }

    /// **The pool answers every dial, back to back, with no client retry.** This is the property
    /// the confined run's `status`/`stop` depend on: every client dials the one name, and
    /// `bind_pipe` binds a pool, so while one instance is connected another is listening. The
    /// retry in [`dial`] exists only to *measure* that — it is asserted to be zero, which is the
    /// count the rejected single-instance reuse could never reach (it needed retries on every run,
    /// and the next dial was killed mid-request when it did not get one).
    ///
    /// The stop half is the drain: a connection established and silent when the channel flips is
    /// still answered, and only after the client lets go does the serve future return. That is the
    /// property the previous single-instance loop got from its `JoinSet`, now through one task per
    /// instance — the serving task is not the one watching the shutdown channel.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_pipe_pool_serves_back_to_back_dials_with_no_gap() {
        const DIALS: usize = 200;
        let name = bind::pipe_name(&format!(r"\\.\pipe\tinio-pool-{}", process::id()));
        // The production bind, pool included: its instances are what `serve_local` rotates.
        let channel = bind::bind_pipe(&name).expect("bind");
        let (tx, rx) = watch::channel(false);
        let state = State::new(name.clone(), Some(9000), Path::new("/srv/data")).expect("state");
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx.clone()));
        let token = shared.state.token.expose_secret().as_str().to_owned();

        let serving = tokio::spawn(serve_local(channel, shared, rx));
        let mut retried = 0usize;
        for nth in 1..=DIALS {
            let (answer, attempts) = dial(&name, &token, nth).await;
            retried += attempts;
            assert_eq!(answer.status, 200, "dial {nth}: {}", answer.body);
            assert!(
                answer.body.contains("\"running\":true"),
                "dial {nth}: {}",
                answer.body
            );
        }
        assert_eq!(
            retried, 0,
            "a dial landed in a gap the instance pool exists to close"
        );
        eprintln!("pipe pool: {DIALS} sequential dials, {retried} retries");

        // A connection established and silent when the stop fires.
        let stream = ClientOptions::new()
            .open(&name)
            .expect("dial the control pipe");
        let (mut sender, connection) = http1::handshake(TokioIo::new(stream))
            .await
            .expect("handshake");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // The instance whose `connect` this dial completed has been handed the connection by now.
        sleep(Duration::from_millis(100)).await;
        tx.send(true).expect("the instance loops are listening");

        let request = Request::builder()
            .method("GET")
            .uri("/status")
            .header("x-tinio-token", &token)
            .body(String::new())
            .expect("request");
        let response = sender
            .send_request(request)
            .await
            .expect("the drain answers the connection that was open when the stop fired");
        assert_eq!(
            response.status().as_u16(),
            200,
            "the open connection was abandoned rather than drained"
        );
        // Only when the client lets go does the serving instance's loop see the stop — and only
        // then does `serve_local` return, which is the drain this awaits rather than aborts.
        drop(sender);
        serving
            .await
            .expect("the serve task ends rather than panicking")
            .expect("serve returns Ok");
    }

    /// The same sequential dials through `Plane`: the path `management.rs` drives, kept here so a
    /// regression in `Plane::serve`'s wiring is diagnosable without the integration suite.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_plane_serves_sequential_pipe_connections() {
        let dir = TempDir::new().expect("temp dir");
        let name = bind::pipe_name(&format!(r"\\.\pipe\tinio-plane-{}", process::id()));
        let (stop, rx) = watch::channel(false);
        let plane = Arc::new(
            Plane::bind(Options {
                selection: Selection {
                    local: Some(LocalForm::Pipe(name.clone())),
                    ..Selection::default()
                },
                state_dir: dir.path().to_path_buf(),
                root: dir.path().to_path_buf(),
                host: "127.0.0.1".to_string(),
                stop,
                gauges: None,
                token: None,
            })
            .expect("bind"),
        );
        plane.publish(9000).expect("publish");
        let token = plane.shared.state.token.expose_secret().as_str().to_owned();
        let serving = tokio::spawn({
            let plane = Arc::clone(&plane);
            async move { plane.serve(rx).await }
        });

        for nth in 1..=4 {
            let (answer, _) = dial(&name, &token, nth).await;
            assert_eq!(answer.status, 200, "dial {nth}: {}", answer.body);
        }
        // The crate's own client, twice: `management.rs`'s two steps, which is where the
        // integration suite diverges from the raw dials above.
        for nth in 1..=2 {
            let report = client::status(dir.path()).await.expect("status reads");
            assert!(report.running, "client status {nth} did not report running");
        }
        assert!(!serving.is_finished(), "the plane is still serving");
        serving.abort();
    }

    /// One dial of `name`, retrying a connection that lands in a gap. **Zero retries is the
    /// expected count**: the bind's pool means a listening instance is always there, so the retry
    /// exists only to make a regression a reported number rather than a broken pipe — and to make
    /// this helper assert nothing about the loop it is measuring.
    ///
    /// The crate's own pipe client makes a single `open` with no wait — the right shape for a
    /// `status` probe, which is why any waiting belongs to this test.
    #[cfg(windows)]
    async fn dial(name: &str, token: &str, nth: usize) -> (client::RawResponse, usize) {
        use tokio::{net::windows::named_pipe::ClientOptions, task::yield_now};

        for attempt in 0.. {
            assert!(
                attempt < 1_000,
                "dial {nth}: the pool never had a listening instance"
            );
            if let Ok(stream) = ClientOptions::new().open(name) {
                match client::exchange(TokioIo::new(stream), get("/status", Some(token))).await {
                    Ok(answer) => return (answer, attempt),
                    // The connection was killed between the open and the answer: the gap again.
                    Err(_) => continue,
                }
            }
            // Every instance busy: `ERROR_PIPE_BUSY`, which is momentary with a pool — the
            // instances' `connect` calls run on this very runtime. Yielding is what lets them.
            yield_now().await;
        }
        unreachable!("the loop above returns or asserts")
    }

    /// A handoff with fewer instances than the channel needs is **refused, not served**: one
    /// instance answers its first client and is then connected for as long as it serves, and a
    /// confined child cannot create the next one — so the name would go deaf, which is the defect
    /// the pool removes. The refusal happens before any handle is converted, which is why the
    /// value below never has to name a real object.
    #[cfg(windows)]
    #[test]
    fn adopting_fewer_instances_than_the_pool_needs_is_refused() {
        let (stop, _rx) = watch::channel(false);
        let options = || Options {
            selection: Selection {
                local: Some(LocalForm::Pipe(r"\\.\pipe\tinio-adopt".to_string())),
                ..Selection::default()
            },
            state_dir: PathBuf::from("/run/tinio"),
            root: PathBuf::from("/srv/data"),
            host: "127.0.0.1".to_string(),
            stop: stop.clone(),
            gauges: None,
            token: None,
        };

        for short in [
            Vec::new(),
            vec![0x1000usize as RawHandle; bind::INSTANCES - 1],
        ] {
            let rendered = Plane::adopt(options(), &short)
                .expect_err("a handoff shorter than the pool is refused")
                .to_string();
            assert!(
                rendered.contains("whole pool"),
                "names the refusal: {rendered}"
            );
            assert!(
                rendered.contains("deaf"),
                "names what a single instance costs: {rendered}"
            );
            assert!(
                rendered.contains(&bind::INSTANCES.to_string()),
                "names the count the channel needs: {rendered}"
            );
        }
    }
}
