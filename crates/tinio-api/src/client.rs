//! The `status`/`stop` client (`management-api.md:63`): read `state`, speak HTTP to the
//! channel it names, and answer in terms the CLI can print.
//!
//! One request path serves all three local forms: dial the stream (TCP, unix socket or
//! named pipe), then run hyper's HTTP/1 client connection over it. Only the dial differs,
//! which is why there is one `exchange` and three two-line dialers rather than three clients.
//!
//! The channel is read from `state.control_name`, never derived: the plane decided the
//! socket's path (or the pipe's name), resolved it to an absolute one before the bind, and
//! this side only dials what it recorded — which is what makes the client survive a daemon
//! that `chdir("/")`s after its startup. That also
//! means one state dir per call — a caller with several candidates (read-write and read-only)
//! calls these functions once per candidate and picks the one that answered; nothing here
//! searches, and nothing here deletes: `state` and the socket belong to the plane's
//! `shutdown`. A control name too long for `sun_path` cannot be dialed either, and the remedy
//! is the one the bind names (`[api.unix] path` may point at a short directory).

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
};

use http::Method;
use http_body_util::BodyExt;
use hyper::{Request, client::conn::http1};
use hyper_util::rt::TokioIo;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::windows::named_pipe::ClientOptions;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

use crate::{Error, router::StatusBody, state::State};

/// One management request: the path, the method and the token to present.
///
/// The three travel together because every dialer needs all three: [`get_local`], [`get_http`],
/// [`exchange`] and `request` each take one value rather than a positional triple, so a call
/// site cannot transpose the target and the method, and the token cannot be left behind when a
/// signature grows. Named `Call` rather than `Request` because this module also builds
/// `hyper::Request`s in [`exchange`], and two `Request`s in one file is one name too many.
///
/// # Examples
///
/// ```rust
/// use http::Method;
/// use tinio_api::client::Call;
///
/// // `None` is a real case and not a default: the local channel and a loopback bind serve
/// // `/metrics` openly, because a Prometheus scrape cannot send a custom header.
/// let scrape = Call {
///     target: "/metrics",
///     method: Method::GET,
///     token: None,
/// };
/// assert_eq!(scrape.target, "/metrics");
/// assert!(scrape.token.is_none());
/// ```
#[derive(Debug, Clone)]
pub struct Call<'a> {
    /// The request target, e.g. `/status`.
    pub target: &'a str,
    /// The HTTP method (`GET`, `POST`).
    pub method: Method,
    /// The token to present in `X-Tinio-Token`; `None` sends no such header.
    pub token: Option<&'a str>,
}

/// A response as the client hands it over: status line plus body text.
///
/// # Examples
///
/// ```rust
/// use tinio_api::client::RawResponse;
///
/// let answer = RawResponse {
///     status: 200,
///     body: r#"{"running":true}"#.to_string(),
/// };
/// assert_eq!(answer.status, 200);
/// assert!(answer.body.contains("running"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawResponse {
    /// The HTTP status code.
    pub status: u16,
    /// The body, decoded as UTF-8 (lossily: a body we did not write is not ours to reject).
    pub body: String,
}

/// What `status` reports: the state file, plus the server's own answer when it replied.
///
/// # Examples
///
/// ```rust
/// use tinio_api::client::Report;
///
/// // The answer a state dir with nothing in it gets: no file read, nothing answered.
/// let report = Report {
///     running: false,
///     state: None,
///     status: None,
/// };
/// assert!(!report.running);
/// assert!(report.status.is_none());
/// ```
#[derive(Debug, Clone)]
pub struct Report {
    /// Whether a server answered the probe.
    pub running: bool,
    /// The state file, when there was one.
    pub state: Option<State>,
    /// The `/status` body, when the probe succeeded.
    pub status: Option<StatusBody>,
}

/// The channel a `state` file names.
///
/// The classification is by shape, and it is sound because the producer normalises: a Windows
/// pipe name is always the full `\\.\pipe\…` form, because `bind_pipe` (Task 2) prefixes a bare
/// `[api.pipe] path` before it creates anything and records what it created. A bare name would
/// otherwise be dialled as a unix path and fail with "a unix control channel on this platform"
/// — the reason the normalisation is the bind's job rather than this reader's.
#[derive(Debug, Clone)]
enum Control {
    /// A unix socket path (`state.control_name` is the absolute path the bind created).
    Unix(PathBuf),
    /// A Windows named pipe, in the full `\\.\pipe\…` form the bind normalised.
    Pipe(String),
    /// A TCP address — the transport `[api]` may select instead of the local channel.
    Tcp(SocketAddr),
}

/// The channel `state.control_name` names, classified by shape (see [`Control`]).
fn control(state: &State) -> Result<Control, Error> {
    let name = state.control_name.as_str();
    if name.starts_with(r"\\.\pipe\") {
        return Ok(Control::Pipe(name.to_string()));
    }
    if let Ok(addr) = name.parse::<SocketAddr>() {
        return Ok(Control::Tcp(addr));
    }
    if name.is_empty() {
        return Err(Error::Transport(
            "the state file names no control channel".to_string(),
        ));
    }
    Ok(Control::Unix(PathBuf::from(name)))
}

/// The call this module's three CLI-shaped entry points ([`probe`], [`status`], [`stop`]) make
/// against `state`'s own channel: the target and method are the caller's, and the token is the
/// one `state` carries, because that channel is the plane's own.
fn call_for<'a>(state: &'a State, target: &'a str, method: Method) -> Call<'a> {
    Call {
        target,
        method,
        token: Some(state.token.as_str()),
    }
}

/// Whether a server answers behind `state`'s control channel.
///
/// A connection that is refused, times out or never completes the handshake is `false`, not an
/// error: "is a server there?" has `false` as one of its answers, and a caller that had to
/// distinguish the two would report a stopped server as a failure.
///
/// **A server that answers is `true` whatever it answers** — this asks whether anything is
/// behind the channel, not whether it is this plane, which is [`status`]'s question and the
/// only one that reads the body. A control name that something else has taken over therefore
/// answers `true` here and `running: false` there, and a caller that needs "is a tinio server
/// running here" wants [`status`].
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::{client::probe, state::State};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// let state = State::new(
///     "/run/tinio/tinio-0123456789abcdef.sock",
///     Some(9000),
///     Path::new("/srv/data"),
/// )?;
/// // `false` covers both "nothing listening" and "nothing dialable here".
/// if probe(&state).await {
///     println!("a server answers behind this channel");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn probe(state: &State) -> bool {
    request(state, call_for(state, "/status", Method::GET))
        .await
        .is_ok()
}

/// Reads `state` and probes it: a missing file is "stopped", a failed probe is "stopped",
/// and only an unreadable file is an error (`contracts/cli.md:58`).
///
/// A `200` whose body is not this plane's contract is "stopped" too, not an error: the answer
/// came from something that is not this server, and a garbled answer is not a running *tinio*
/// server by the contract's definition. [`State::read`]'s failure above is this function's one
/// error path (`contracts/cli.md:58`).
///
/// The state file is kept in the report either way — a caller that finds a file nobody answers
/// for has something to report (and `doctor --fix`, not this client, is what may reclaim it).
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::client::status;
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // One candidate: the CLI resolves them (read-write and read-only) and calls this per
/// // candidate, keeping the one that answered.
/// let report = status(Path::new("/run/tinio")).await?;
/// if report.running {
///     println!("a server is running here");
/// } else {
///     println!("stopped");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn status(state_dir: &Path) -> Result<Report, Error> {
    let Some(state) = State::read(state_dir)? else {
        return Ok(Report {
            running: false,
            state: None,
            status: None,
        });
    };
    match request(&state, call_for(&state, "/status", Method::GET)).await {
        Ok(raw) if raw.status == 200 => {
            // The contract's body or nothing (R29). A `200` carrying something else came from a
            // server that is not this plane — a garbled answer is not a running *tinio* server
            // by the contract's definition — so it is reported as stopped rather than raised:
            // `State::read`'s failure above is this function's one error path
            // (`contracts/cli.md:58`). **Measured, and not the same answer as `probe`'s**:
            // `probe` says `true` for this server, because it only checks that the request
            // completed. The two ask different questions — is anything answering behind the
            // channel (`probe`, what `doctor --fix` must not disturb) versus is a tinio server
            // running here (`status`) — and are not interchangeable.
            let status = serde_json::from_str::<StatusBody>(&raw.body).ok();
            Ok(Report {
                running: status.is_some(),
                state: Some(state),
                status,
            })
        }
        _ => Ok(Report {
            running: false,
            state: Some(state),
            status: None,
        }),
    }
}

/// Sends `POST /stop` and reports whether the server accepted it. The caller polls for the
/// exit; this function answers only what the control channel said (`contracts/cli.md:68`).
///
/// **Nothing here deletes.** `state` and the socket belong to the plane, which removes them on
/// its own way out (`Plane::shutdown`); a client that unlinked either could strip a live
/// server of its discovery — the race its own doc states.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use tinio_api::client::stop;
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // `Ok` is the `202`: the server is draining. The exit is what the caller then waits for.
/// stop(Path::new("/run/tinio")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn stop(state_dir: &Path) -> Result<(), Error> {
    let Some(state) = State::read(state_dir)? else {
        return Err(Error::Transport(
            "no running server for this root".to_string(),
        ));
    };
    let raw = request(&state, call_for(&state, "/stop", Method::POST)).await?;
    if raw.status == 202 {
        Ok(())
    } else {
        Err(Error::Transport(format!("/stop answered {}", raw.status)))
    }
}

/// One request over an explicit unix socket path — the seam the transport tests and the
/// integration suite use, and the dialer `request`'s unix arm delegates to.
///
/// The whole request is the caller's [`Call`], method included, because this seam serves more
/// than the CLI's `GET`s: `POST /stop` and any other method go through here too. A `None` token
/// is how `/metrics` is read on the local channel, which serves it openly (the contract's
/// bind-address rule).
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
///
/// use http::Method;
/// use tinio_api::client::{Call, get_local};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // The path `state.control_name` records; the token is the one `state` carries.
/// let answer = get_local(
///     Path::new("/run/tinio/tinio-0123456789abcdef.sock"),
///     Call {
///         target: "/status",
///         method: Method::GET,
///         token: Some("the token from `state`"),
///     },
/// )
/// .await?;
/// assert_eq!(answer.status, 200);
/// # Ok(())
/// # }
/// ```
pub async fn get_local(path: &Path, call: Call<'_>) -> Result<RawResponse, Error> {
    #[cfg(unix)]
    {
        let stream = UnixStream::connect(path).await.map_err(Error::Io)?;
        exchange(TokioIo::new(stream), call).await
    }
    #[cfg(not(unix))]
    {
        let _ = (path, call);
        Err(Error::Transport(
            "a unix socket is not a Windows transport".to_string(),
        ))
    }
}

/// One request over an explicit TCP address — the seam the transport tests and the integration
/// suite use, and the dialer `request`'s TCP arm delegates to.
///
/// The call carries the method for the reason [`get_local`]'s doc gives, and a `None` token is
/// how `/metrics` is read on a **loopback-bound** transport, which keeps that one endpoint open
/// (a scraper cannot send a custom header).
///
/// # Examples
///
/// ```no_run
/// use http::Method;
/// use tinio_api::client::{Call, get_http};
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// let addr = "127.0.0.1:9001".parse().expect("addr");
/// let answer = get_http(
///     addr,
///     Call {
///         target: "/metrics",
///         method: Method::GET,
///         token: None,
///     },
/// )
/// .await?;
/// assert_eq!(answer.status, 200);
/// # Ok(())
/// # }
/// ```
pub async fn get_http(addr: SocketAddr, call: Call<'_>) -> Result<RawResponse, Error> {
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    exchange(TokioIo::new(stream), call).await
}

/// The address to dial for a **recorded** control address.
///
/// `state.control_name` records what the bind bound, and a wildcard bind bound the wildcard —
/// `[api.http] host = "0.0.0.0"` records `0.0.0.0:9001`, which is truthful and load-bearing
/// (`Auth::for_bind` and `doctor` reason from exactly that value). It is not a **connect**
/// address: Windows refuses it outright (`WSAEADDRNOTAVAIL`, measured on this branch's host)
/// while `127.0.0.1:<recorded port>` connects, and `status` maps any dial failure to
/// `running: false` — so without this substitution `tinio status` would report a running server
/// as "stopped", deterministically, on every wildcard bind. `state.control_name` itself stays
/// what the bind wrote: this function changes only the address a connection is made to, at the
/// one place the recorded name becomes a connection.
fn dialable(addr: SocketAddr) -> SocketAddr {
    let ip = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, addr.port())
}

/// One request to the channel `state` names: the three dialers, and [`exchange`] under them.
///
/// The call is the whole request, token included, and every caller in this module fills it with
/// `state`'s own ([`call_for`]): `/status` and `/stop` are guarded on every transport.
///
/// The TCP and unix arms are [`get_http`] and [`get_local`], not second copies of them: those
/// two carry the platform refusals, so there is one definition of each and not two.
async fn request(state: &State, call: Call<'_>) -> Result<RawResponse, Error> {
    match control(state)? {
        // Dialled through [`dialable`]: a wildcard bind's recorded address is what it bound and
        // is not connectable, and this is the one place the recorded name becomes a connection.
        Control::Tcp(addr) => get_http(dialable(addr), call).await,
        Control::Unix(path) => get_local(&path, call).await,
        Control::Pipe(name) => {
            #[cfg(windows)]
            {
                let client = ClientOptions::new().open(&name).map_err(Error::Io)?;
                exchange(TokioIo::new(client), call).await
            }
            #[cfg(not(windows))]
            {
                let _ = name;
                Err(Error::Transport(
                    "a named-pipe control channel on this platform".to_string(),
                ))
            }
        }
    }
}

/// One request over an already-dialed stream: the HTTP/1 client connection, the request, and
/// the response read back as text. **The single `exchange` for the whole crate** — `request`
/// (the three dialers, with `probe`/`status`/`stop` on top of them), [`get_local`], [`get_http`],
/// the transport tests, `test_support::https_get` for the TLS leg and the integration suite are
/// all its callers, so there is one definition and not two. `pub` for the callers that dial
/// themselves: the TLS client and the tests' own transports.
///
/// # Examples
///
/// ```no_run
/// use http::Method;
/// use hyper_util::rt::TokioIo;
/// use tinio_api::client::{Call, exchange};
/// use tokio::net::TcpStream;
///
/// # async fn run() -> Result<(), tinio_api::Error> {
/// // An already-dialed stream, whatever it is: this is the HTTP/1 exchange and nothing else.
/// let stream = TcpStream::connect("127.0.0.1:9001").await?;
/// let answer = exchange(
///     TokioIo::new(stream),
///     Call {
///         target: "/status",
///         method: Method::GET,
///         token: None,
///     },
/// )
/// .await?;
/// assert_eq!(answer.status, 200);
/// # Ok(())
/// # }
/// ```
pub async fn exchange<I>(io: TokioIo<I>, call: Call<'_>) -> Result<RawResponse, Error>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let Call {
        target,
        method,
        token,
    } = call;
    let (mut sender, connection) = http1::handshake(io)
        .await
        .map_err(|err| Error::Transport(format!("handshake: {err}")))?;
    tokio::spawn(async move {
        // The connection future ends when either side is done; a failure here is the
        // caller's response error, reported there rather than twice.
        let _ = connection.await;
    });

    let mut builder = Request::builder().method(method).uri(target);
    if let Some(token) = token {
        builder = builder.header("x-tinio-token", token);
    }
    let request = builder
        .body(String::new())
        .map_err(|err| Error::Transport(format!("request: {err}")))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?;
    let status = response.status().as_u16();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|err| Error::Transport(format!("{target}: {err}")))?
        .to_bytes();
    Ok(RawResponse {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    // The two socket tests below need `bind_unix` and `serve_local`, which do not exist on
    // Windows, so *their* imports are gated rather than the module: the TCP test at the bottom
    // is platform-neutral and runs on both hosts (it is the only leg of this module that
    // exercises `Control::Tcp`). The Windows pipe dialer and the DACL are Task 10's, gated there.
    #[cfg(unix)]
    use std::sync::Arc;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        path::Path,
        thread,
    };

    use tempfile::TempDir;
    #[cfg(unix)]
    use tokio::sync::watch;

    use super::*;
    use crate::state::State;
    #[cfg(unix)]
    use crate::{
        bind::bind_unix,
        router::{Auth, Context},
        state::socket_path,
        transport::serve_local,
    };

    #[cfg(unix)]
    #[tokio::test]
    async fn status_reports_a_running_server_and_stop_ends_it() {
        let dir = TempDir::new().expect("temp dir");
        // The derived default (the state dir doubles as the root) — no fixed `tinio.sock`.
        let path = socket_path(dir.path(), dir.path()).expect("socket path");
        let channel = bind_unix(&path).expect("bind");
        let state = State::new(
            path.to_string_lossy().into_owned(),
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state");
        state.write(dir.path()).expect("write");

        // `mut`: `watch::Receiver::changed` takes `&mut self`, and the clone below is what
        // `serve_local` waits on — the same receiver, borrowed rather than moved.
        let (tx, mut rx) = watch::channel(false);
        let shared = Arc::new(Context::new(state, "127.0.0.1", Auth::Local, tx));
        // `Plane::publish` is what sets this in a real run (Task 5, the readiness point); the
        // client only reads it back through `/status`.
        shared.port.set(9000).expect("the port is published once");
        // The same channel twice, as the plane wires it: the sender is the router's (`/stop`),
        // the clone is what `serve_local` waits on — so the assertion below proves the pair.
        let serving = tokio::spawn(serve_local(channel, shared, rx.clone()));

        let report = status(dir.path()).await.expect("status");
        assert!(report.running);
        assert_eq!(report.status.expect("body").port, Some(9000));

        stop(dir.path()).await.expect("stop");
        rx.changed().await.expect("the channel flips");
        serving.abort();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_stopped_server_is_reported_rather_than_failing() {
        let dir = TempDir::new().expect("temp dir");
        let report = status(dir.path()).await.expect("status");
        assert!(
            !report.running,
            "no state file means nothing is running here"
        );
        assert!(
            stop(dir.path()).await.is_err(),
            "stop with nothing to stop is an error"
        );
    }

    /// R29, pinned: a `200` whose body is not this plane's contract is `false`, not an `Err`.
    /// The bare listener is the smallest server that can answer that way — no router, no plane,
    /// just a status line and a body — and it is also this module's only test of `Control::Tcp`
    /// and of `request`'s TCP dialer.
    ///
    /// The listener is `std` and answered on a blocking thread rather than tokio's: this crate's
    /// tokio graph carries no `io-util`, so `AsyncReadExt`/`AsyncWriteExt` do not exist here, and
    /// driving `poll_read`/`poll_write` by hand would be more machinery than one canned answer
    /// is worth (the alternative would be a feature this crate does not ask for).
    #[tokio::test]
    async fn a_two_hundred_that_is_not_the_contract_is_not_a_running_server() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let mut request = [0u8; 1024];
            // The request is drained first — closing a socket with data still unread is an RST,
            // which could take the answer with it — then the answer goes out whole, and the last
            // read holds the socket open until the client hangs up.
            let _ = stream.read(&mut request);
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\nnot-json");
            let _ = stream.read(&mut request);
        });

        let dir = TempDir::new().expect("temp dir");
        State::new(addr.to_string(), Some(9000), Path::new("/srv/data"))
            .expect("state")
            .write(dir.path())
            .expect("write");

        let report = status(dir.path())
            .await
            .expect("a garbled answer is an answer, not a failure");
        assert!(
            !report.running,
            "a body that is not the contract's is not this tinio server"
        );
        assert!(report.status.is_none(), "and there is no body to report");
        assert!(
            report.state.is_some(),
            "the state file it read is still the caller's to report"
        );
    }
}
