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
#[cfg(feature = "tls")]
use std::io;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::io::OwnedHandle;
#[cfg(feature = "tls")]
use std::time::Duration;
use std::{
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
use tokio::net::UnixListener;
#[cfg(windows)]
use tokio::task::JoinSet;
#[cfg(feature = "tls")]
use tokio::time::{sleep, timeout};
use tokio::{net::TcpListener as TokioTcpListener, sync::watch};
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig, server::TlsStream};

#[cfg(windows)]
use crate::dacl;
use crate::{
    Error,
    bind::{self, Channel},
    metrics::Gauges,
    router::{Auth, Context, Shared, router},
    state::{State, remove_if_present},
};

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
        let listener = UnixListener::from_std(listener).map_err(Error::Io)?;
        axum::serve(listener, router(shared))
            .with_graceful_shutdown(until_stopped(stop))
            .await
            .map_err(|err| Error::Transport(format!("unix channel: {err}")))
    }
    #[cfg(windows)]
    {
        // The pipe form carries its own name, so the identity is passed on rather than re-derived.
        let Channel { name, handle } = channel;
        serve_pipe(handle, &name, shared, stop).await
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

/// The named-pipe accept loop: one connection at a time, HTTP/1 keep-alive off, and the
/// shutdown channel watched while no client is connecting.
#[cfg(windows)]
async fn serve_pipe(
    handle: OwnedHandle,
    identity: &str,
    shared: Shared,
    mut stop: watch::Receiver<bool>,
) -> Result<(), Error> {
    // The raw handle becomes tokio's own type here, inside the runtime — the constructor needs
    // the reactor, and `dacl` is the crate's one raw-Windows module (Task 2's note).
    let mut pipe = dacl::adopt_pipe(handle).map_err(Error::Io)?;
    // In-flight connections, so "cease accepting" is not "drop the requests already answered".
    let mut connections = JoinSet::new();
    // Built once, above the loop: nothing about the router varies per connection, and its clone
    // is an `Arc` bump (`axum::Router` keeps its state behind one), so a per-connection rebuild
    // would re-derive what this one already holds.
    let app = router(shared);

    loop {
        tokio::select! {
            _ = stop.changed() => break,
            connected = pipe.connect() => {
                connected.map_err(|err| Error::Transport(format!("{identity}: pipe connect: {err}")))?;
            }
        }
        let connected = pipe;
        // The next instance must exist before this connection is served, or the pipe name
        // disappears between two connections. `first_pipe_instance` is false here: the
        // single-instance claim was made by the bind, and re-asserting it would refuse our
        // own successor.
        let next = dacl::create_pipe(identity, false).map_err(Error::Io)?;
        pipe = dacl::adopt_pipe(next).map_err(Error::Io)?;

        let app = app.clone();
        connections.spawn(async move {
            // `http1` borrows the builder mutably (`hyper-util`'s
            // `auto::Builder::http1(&mut self)`), so the binding is `mut`.
            let mut builder = auto::Builder::new(TokioExecutor::new());
            // axum's `Router` is a tower service and this loop is hyper's own, so the bridge is
            // explicit — `serve_connection` wants `hyper::service::Service`, and axum wraps with
            // exactly this adapter in its own server (`axum-0.8.9/src/serve/mod.rs:388`).
            let service = TowerToHyperService::new(app);
            // The pipe has no half-close, so keep-alive would let a client wait forever for
            // a response delimitation TCP would have provided (`research.md:99`).
            if let Err(err) = builder
                .http1()
                .keep_alive(false)
                .serve_connection(TokioIo::new(connected), service)
                .await
            {
                tracing::debug!(error = %err, "management pipe connection ended");
            }
        });
        // Reap what has already finished: a `JoinSet` holds a completed task until it is
        // joined, so without this the set would keep one dead entry per connection served
        // for the process's life — and every `status`/`stop` probe is a connection.
        while connections.try_join_next().is_some() {}
    }

    // The drain: every connection already accepted runs to completion before this returns.
    while connections.join_next().await.is_some() {}
    Ok(())
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
    /// })?;
    /// plane.publish(9000)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind(options: Options) -> Result<Self, Error> {
        let mut selection = options.selection;
        if let Some(LocalForm::Unix(path)) = &mut selection.local {
            *path = resolve_socket_path(path, &options.state_dir);
        }
        let transport = select(selection)?;
        let state = State::new(transport.identity(), None, &options.root)?;
        // The identity is recorded here, at the bind, because that is the only moment it is
        // certainly this plane's file: `remove_owned` compares against it later.
        let socket = match &transport {
            Transport::Local { channel } => {
                channel.socket_path().map(SocketFile::at).transpose()?
            }
            _ => None,
        };
        // The caller's source travels in with the context and nowhere else, so the route's
        // default state stays `None` (`Context::new`): a plane built without one has no
        // `/metrics`, and the API tests can assert both halves of that.
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
    use tempfile::TempDir;
    use tokio::sync::watch;
    #[cfg(unix)]
    use tokio::time::sleep;

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
        let token = shared.state.token.clone();

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

    /// The gate is released before the deletions: `serve` returns the moment the shared channel
    /// flips, and the CLI still has the data plane's drain to await before it calls `shutdown`
    /// (decision 12's sequence). A successor that passes the gate in that window has published
    /// its own `state`, so the predecessor must remove the file only while it is still its own.
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
        let token = shared.state.token.clone();

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
        let token = shared.state.token.clone();

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
        let token = shared.state.token.clone();

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
        let token = shared.state.token.clone();

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
}
