//! Management-plane [`Selection`](crate::_api::transport::Selection) from the resolved config and `--api*`.

use std::path::{Path, PathBuf};

use super::{ApiFlags, Args};
use crate::{
    _api::{
        state as api_state,
        transport::{HttpsOptions, LocalForm, Selection},
    },
    _config::api,
    Error,
    resolve::{Resolution, authority_addr, bind_addr},
};

/// The TCP management transports' default port (`contracts/cli.md`), from the schema.
fn default_api_port() -> u16 {
    api::Http::default().port
}

/// The CLI's half of the plane's transport rule (decision 7): the resolved `[api]` plus the
/// `--api*` flags, as plan A's `Selection`.
///
/// The presence of **any** `--api` flag disables every transport the flags do not name — the
/// local channel is the tool's default and not the operator's choice — while without one the
/// config file is the only thing selecting transports, and `--no-api-unix` is the switch for
/// the local channel it would otherwise enable. Exclusivity is not decided here: `select` owns
/// it inside `Plane::bind`, so two callers cannot enforce it differently.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::{Args, api_options},
///     resolve::{locate, server},
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution = server(located, &args, &Provenance::default())?;
/// // An auto-created root enables the local channel, so this is not an empty selection.
/// assert!(api_options(&resolution, &args)?.local.is_some());
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "api")]
pub fn api_options(resolution: &Resolution, args: &Args) -> Result<Selection, Error> {
    if args.api.urls.is_empty() {
        api_from_config(resolution, &args.api)
    } else {
        api_from_flags(resolution, &args.api)
    }
}

/// The selection the config file names (decision 7): a config with **no `[api]` section at
/// all** enables the derived local channel, and a config that names any subsection selects
/// exactly the subsections it has.
#[cfg(feature = "api")]
fn api_from_config(resolution: &Resolution, flags: &ApiFlags) -> Result<Selection, Error> {
    let mut selection = Selection::default();
    // Three states, not two: the local channel can be enabled *with a path*, enabled with the
    // **derived** name (a section that carries no path), or not enabled at all — and the last
    // is what a config that names other transports and not this one means. The configured value
    // travels **as written**: the relative→state dir join is `Plane::bind`'s
    // (`transport::resolve_socket_path`, decision 24), and doing it here too would be a second
    // answer to a question that already has one owner.
    let local = match resolution.config.api.as_ref() {
        // No `[api]` section at all: the local channel is the tool's default, enabled with the
        // derived name (decision 7).
        None => Some(None),
        Some(api) => match local_section(api) {
            None => None,
            Some(path) if path.as_os_str().is_empty() => Some(None),
            Some(path) => Some(Some(path)),
        },
    };

    if !flags.no_api_unix {
        selection.local = match local {
            Some(None) => Some(derived_local(resolution)?),
            Some(Some(path)) => Some(local_form(&path)),
            None => None,
        };
    }
    if let Some(api) = resolution.config.api.as_ref() {
        if let Some(http) = api.http.as_ref() {
            selection.http = Some(bind_addr("api.http.host", &http.host, http.port)?);
        }
        if let Some(https) = api.https.as_ref() {
            selection.https = Some(HttpsOptions {
                addr: bind_addr("api.https.host", &https.http.host, https.http.port)?,
                cert: https.cert.clone(),
                key: https.key.clone(),
            });
        }
    }
    Ok(selection)
}

/// The selection the `--api` flags name: one flag per scheme replaces that scheme's subsection,
/// and every transport the flags do not name is off.
#[cfg(feature = "api")]
fn api_from_flags(resolution: &Resolution, flags: &ApiFlags) -> Result<Selection, Error> {
    let mut selection = Selection::default();
    for url in &flags.urls {
        let (scheme, rest) = url.split_once("://").unwrap_or(("http", url.as_str()));
        match scheme {
            // The local channel's two spellings are per-platform, and the contract makes the
            // other platform's a usage error rather than a transport that cannot work.
            "unix" => {
                #[cfg(unix)]
                {
                    selection.local = Some(local_form(Path::new(rest)));
                }
                #[cfg(windows)]
                {
                    let _ = rest;
                    return Err(Error::Usage(format!(
                        "`{url}` is not a Windows transport: the local channel is a named pipe"
                    )));
                }
            }
            "pipe" => {
                #[cfg(unix)]
                return Err(Error::Usage(format!(
                    "`{url}` is not a unix transport: the local channel is a socket"
                )));
                #[cfg(windows)]
                {
                    selection.local = Some(LocalForm::Pipe(rest.to_string()));
                }
            }
            "http" => selection.http = Some(authority_addr(rest, default_api_port())?),
            "https" => {
                // Decision 17 / `contracts/cli.md:53`: `https://` in a build without `tls` is a
                // **usage** error (exit 2). The scheme is the flag's, and the build cannot serve
                // it — the same class as `unix://` on Windows above, and deliberately not the
                // transport error `select` would raise for a selection this build cannot honour.
                #[cfg(not(feature = "tls"))]
                {
                    let _ = resolution;
                    return Err(Error::Usage(format!(
                        "`{url}` needs a build with the `tls` feature: this one has https compiled out"
                    )));
                }
                #[cfg(feature = "tls")]
                {
                    let addr = authority_addr(rest, default_api_port())?;
                    // The pair comes from the flags, then from the config's own `[api.https]`;
                    // an https transport without a pair is `contracts/config.md:151`'s refusal.
                    let configured = resolution
                        .config
                        .api
                        .as_ref()
                        .and_then(|api| api.https.as_ref());
                    let cert = flags
                        .api_cert
                        .clone()
                        .or_else(|| configured.map(|https| https.cert.clone()));
                    let key = flags
                        .api_key
                        .clone()
                        .or_else(|| configured.map(|https| https.key.clone()));
                    selection.https = Some(HttpsOptions {
                        addr,
                        cert: cert.ok_or_else(|| missing_https("api.https.cert"))?,
                        key: key.ok_or_else(|| missing_https("api.https.key"))?,
                    });
                }
            }
            other => {
                return Err(Error::Usage(format!(
                    "`--api {url}` has no usable scheme `{other}`: use `unix://`, `pipe://`, `http://` or `https://`"
                )));
            }
        }
    }
    if flags.no_api_unix {
        selection.local = None;
    }
    Ok(selection)
}

/// The refusal an https transport without its pair gets, naming the missing key. `tls`-gated
/// like its only caller: without the feature there is no https arm to raise it from.
#[cfg(all(feature = "api", feature = "tls"))]
fn missing_https(key: &str) -> Error {
    Error::Config(_config::Error::invalid_value(
        key,
        "an https management transport needs a certificate and a key: pass `--api-cert` and `--api-key`, or configure `[api.https]`",
    ))
}

/// The local channel's configured path, per platform: `[api.unix]` on unix and `[api.pipe]` on
/// Windows. The other platform's section does not exist to read.
#[cfg(feature = "api")]
fn local_section(api: &api::Config) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        api.unix.as_ref().map(|unix| unix.path.clone())
    }
    #[cfg(windows)]
    {
        api.pipe.as_ref().map(|pipe| pipe.path.clone())
    }
}

/// A configured local-channel path-or-name as the transport takes it, per platform: on unix the
/// socket path, on Windows the pipe **name** — Windows' pipe namespace is flat, so `[api.pipe]
/// path` names a channel and never a path. Neither arm joins anything: the relative→state dir
/// rule is `transport::resolve_socket_path`'s, applied once, by `Plane::bind`.
#[cfg(feature = "api")]
pub(crate) fn local_form(path: &Path) -> LocalForm {
    #[cfg(windows)]
    {
        LocalForm::Pipe(path.to_string_lossy().into_owned())
    }
    #[cfg(not(windows))]
    {
        LocalForm::Unix(path.to_path_buf())
    }
}

/// The derived local channel: plan A's `state::socket_path`, never a fixed `tinio.sock`
/// (decision 24) — and on Windows the contract's bare pipe name
/// (`contracts/management-api.md:5`), which is that socket's own stem rather than the socket
/// path itself: a path in the pipe namespace would publish the server's local layout to every
/// user able to enumerate `\\.\pipe\`.
#[cfg(feature = "api")]
fn derived_local(resolution: &Resolution) -> Result<LocalForm, Error> {
    #[cfg(windows)]
    {
        api_state::pipe_name(&resolution.root)
            .map(LocalForm::Pipe)
            .map_err(Error::from)
    }
    #[cfg(not(windows))]
    {
        api_state::socket_path(&resolution.state_dir, &resolution.root)
            .map(LocalForm::Unix)
            .map_err(Error::from)
    }
}
