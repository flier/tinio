//! Test-only helpers the crate's tests share: the committed TLS fixture pair and its client, and
//! the derived socket path.
//!
//! `#[cfg(test)]` and registered in `lib.rs`, so none of it is compiled into a build and none of
//! it ships (R8). The fixture paths are platform-neutral; the rustls client is behind
//! `#[cfg(feature = "tls")]` because `tokio-rustls` is an optional dependency, and a
//! default-graph test build has no TLS client at all — gating the client, not the module, is what
//! keeps `cargo test -p tinio-api` building without the feature.

use std::path::{Path, PathBuf};
#[cfg(feature = "tls")]
use std::{fs::File, net::SocketAddr, sync::Arc};

#[cfg(feature = "tls")]
use hyper_util::rt::TokioIo;
#[cfg(unix)]
use tempfile::TempDir;
#[cfg(feature = "tls")]
use tokio::net::TcpStream;
#[cfg(feature = "tls")]
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, RootCertStore,
        pki_types::{CertificateDer, ServerName, pem::PemObject},
    },
};

#[cfg(feature = "tls")]
use crate::Error;
#[cfg(feature = "tls")]
use crate::client::{self, Call, RawResponse};
#[cfg(unix)]
use crate::state::socket_path;

/// The committed test-only PEM pair under `tests/fixtures`: the certificate, then its key.
///
/// Absolute, because `CARGO_MANIFEST_DIR` is the crate root at compile time — the tests find the
/// fixtures whatever the process's working directory is. `tests/fixtures/README` is where the pair's
/// provenance lives: it is self-signed test material, and its own commands regenerate it.
///
/// The `dead_code` allowance is only for a build without `tls`: with the feature off its one
/// caller, `https_get` below, does not exist either.
#[cfg_attr(
    not(feature = "tls"),
    expect(
        dead_code,
        reason = "the fixture pair's one caller, `https_get`, is behind the same feature, so in a build without `tls` this function is deliberately unreachable"
    )
)]
pub(crate) fn fixture_pair() -> (PathBuf, PathBuf) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    (dir.join("cert.pem"), dir.join("key.pem"))
}

/// Issues `call` against `addr` over TLS — the TLS mirror of [`crate::client::get_http`], for the
/// tests that serve a router over `serve_https`. The call carries the token like any other
/// ([`crate::client::Call`]).
///
/// The fixture certificate is the client's **only** trust anchor. That is not a shortcut around
/// verification: the same file is the server's leaf, so the handshake this dialer completes is a
/// real one, and it fails if the server presents anything else. The pair is self-signed, so the
/// leaf is also its own anchor — which is why `tests/fixtures/README` pins `CA:FALSE`, the one
/// shape webpki accepts on both sides of that arrangement.
#[cfg(feature = "tls")]
pub(crate) async fn https_get(addr: &SocketAddr, call: Call<'_>) -> Result<RawResponse, Error> {
    let (cert, _) = fixture_pair();
    let connector = TlsConnector::from(Arc::new(client_config(&cert)?));
    let stream = TcpStream::connect(addr).await.map_err(Error::Io)?;
    let tls = connector
        .connect(server_name(addr), stream)
        .await
        .map_err(Error::Io)?;
    client::exchange(TokioIo::new(tls), call).await
}

/// A `ClientConfig` trusting `cert` and nothing else — the fixture pair's own certificate, read
/// the same way the server reads it, so both sides fail on the same parse.
#[cfg(feature = "tls")]
fn client_config(cert: &Path) -> Result<ClientConfig, Error> {
    let mut roots = RootCertStore::empty();
    for parsed in CertificateDer::pem_reader_iter(File::open(cert).map_err(Error::Io)?) {
        let der = parsed.map_err(|err| Error::Transport(format!("{}: {err}", cert.display())))?;
        roots
            .add(der)
            .map_err(|err| Error::Transport(format!("{}: {err}", cert.display())))?;
    }
    Ok(ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// The name the client verifies against: the address it dialed, as an IP, so SNI carries nothing
/// and the check is the fixture certificate's `IP:127.0.0.1` SAN entry.
#[cfg(feature = "tls")]
fn server_name(addr: &SocketAddr) -> ServerName<'static> {
    ServerName::IpAddress(addr.ip().into())
}

/// The derived default socket path for a temp dir that doubles as the state dir and the root —
/// exactly what a run binds when `[api.unix] path` does not override it ([`crate::state::socket_path`]).
/// One definition, because `bind`'s and `transport`'s tests each need the same path.
#[cfg(unix)]
pub(crate) fn sock(dir: &TempDir) -> PathBuf {
    socket_path(dir.path(), dir.path()).expect("socket path")
}
