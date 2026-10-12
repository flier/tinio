//! `tinio status` — the thin client that reports whether a server is running for a root.
//!
//! Everything here is plan A's `client::status` over the two candidates `client_resolution` and
//! `probe_pair` resolve (decision 19): this module renders what it is told and decides no
//! state directory of its own. **Nothing is created and nothing is deleted**: the root and its
//! configuration are reached through the chain that *reads* a configuration rather than writing
//! one (a config file is generated on a `server`'s first start and nowhere else — design
//! decisions 14 and 26), so the root this command was asked about is left exactly as it was
//! found, and `state` and the socket belong to the plane, whose `shutdown` is their only
//! ordinary removal site.
//!
//! The command is an `async fn` awaited on the process's **one** runtime: the state read and the
//! probe are async work on that runtime, and this module has no runtime of its own to build,
//! enter or shut down (design decision 2).

use std::process::ExitCode;

use serde::Serialize;

pub use super::DirArgs as Args;
use super::{Answer, client_resolution, probe_pair};
use crate::{
    _util::render::renderer,
    Error,
    resolve::{Located, Resolution, authority},
};

/// The human report. Layout lives here, values in [`StatusView`].
const TEMPLATE: &str = include_str!("status.hbs");

/// Reports the state of the server for the root `args` names.
///
/// The output is the contract's five fields, one per line (`contracts/cli.md:58`:
/// `running|stopped`, endpoint, root, PID, started) plus the state directory the answer came
/// from — the one of decision 19's pair that answered, so the operator can see which it was.
/// Exit 0 for a running server and 1 otherwise, with the message on **stdout**: the command
/// reports the state rather than erroring out of it, and only an unreadable `state` file is an
/// `Err`.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a state directory that cannot be derived, and the
/// one probe failure the contract makes an error: a `state` file that is there and cannot be
/// read.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::status::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn status() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let code = run(args, located).await?;
/// // 0 when a server answered for that root, 1 when the answer was "stopped".
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// `located` is the resolution's discovery half — the root, the mode and the state dir, resolved
/// once by `lib.rs` for this command and the server alike — and `args` is the **authoritative**
/// parse's, so a credential pair `.env` supplied is folded exactly as the server folds it.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let (resolution, candidates) = client_resolution(args, located)?;
    let answer = probe_pair(&candidates).await?;
    println!("{}", render(&answer, &resolution)?);
    Ok(if answer.running() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

/// The report as the lines the contract lists, plus the directory it came from.
///
/// A running server's own `/status` body is what the fields report — it carries all five, and
/// they are the server's answers rather than this command's reconstruction of them. A stopped
/// one has no endpoint, pid or start time, and `-` stands in for them: the values a **stale**
/// `state` file still carries are deliberately not printed as if they were live, and the last
/// line says which of the two stopped shapes this is.
///
/// # Errors
///
/// The template failed to render. [`TEMPLATE`] is compile-time text, so this is a programming
/// error rather than an operator-facing one; it is still an `Err` because library code does not
/// panic.
fn render(answer: &Answer, resolution: &Resolution) -> Result<String, Error> {
    renderer()
        .render_template(TEMPLATE, &StatusView::new(answer, resolution))
        .map(|text| text.trim_end().to_string())
        .map_err(|source| Error::Render {
            context: "the status report could not be rendered",
            source,
        })
}

/// The values [`TEMPLATE`] interpolates. Serde's external tag is the variant Handlebars sees.
#[derive(Serialize)]
enum StatusView<'a> {
    Running {
        endpoint: String,
        root: String,
        pid: u32,
        started: &'a str,
        state_dir: String,
    },
    Stopped {
        root: String,
        state_dir: String,
        control_name: Option<&'a str>,
    },
}

impl<'a> StatusView<'a> {
    fn new(answer: &'a Answer, resolution: &Resolution) -> Self {
        let state_dir = answer.state_dir.display().to_string();
        match answer.report.status.as_ref() {
            Some(status) => Self::Running {
                endpoint: endpoint(&status.host, status.port),
                root: status.root.clone(),
                pid: status.pid,
                started: &status.started_at,
                state_dir,
            },
            None => Self::Stopped {
                root: resolution.root.display().to_string(),
                state_dir,
                // A file nobody answers for: the template names the channel and calls it stale.
                // No file at all: the template says the root is uninitialised.
                control_name: answer
                    .report
                    .state
                    .as_ref()
                    .map(|state| state.control_name.as_str()),
            },
        }
    }
}

/// The endpoint field: `host:port`, with an IPv6 literal bracketed the way a socket address
/// spells one (the rule is [`authority`]'s) — `/status` reports the bare address it bound
/// (`::1`), and `::1:9000` would read as something else entirely.
fn endpoint(host: &str, port: Option<u16>) -> String {
    match port {
        Some(port) => authority(host, port),
        // The data plane's port is published before readiness, so this is a server that bound
        // its management channel and nothing else — the host alone is everything it can say.
        None => host.to_string(),
    }
}

/// The rendering's own rules, which no process-level leg can pin: the fields a stopped server
/// has no answer for, and the endpoint's IPv6 spelling.
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::{
        _api::{State, client::Report, router::StatusBody},
        _config::Config,
    };

    /// A resolution with nothing in it but what these legs read.
    fn resolution(root: &str) -> Resolution {
        Resolution {
            pg_root: None,
            root: PathBuf::from(root),
            config: Config::default(),
            config_text: String::new(),
            config_path: PathBuf::from(root).join(".tinio/.tinio.toml"),
            read_only: false,
            state_dir: PathBuf::from(root).join(".tinio"),
            state_dir_override: None,
            config_error: None,
        }
    }

    /// A stopped answer from `state_dir`, with a `state` file whose control channel is
    /// `control`.
    fn stopped(state_dir: &str, control: Option<&str>) -> Answer {
        Answer {
            state_dir: PathBuf::from(state_dir),
            report: Report {
                running: false,
                state: control.map(|control| {
                    State::new(control, Some(9000), Path::new("/srv/data")).expect("state")
                }),
                status: None,
            },
        }
    }

    /// A running answer whose `/status` body is what the fields report.
    fn running(state_dir: &str) -> Answer {
        Answer {
            state_dir: PathBuf::from(state_dir),
            report: Report {
                running: true,
                state: None,
                status: Some(StatusBody {
                    running: true,
                    pid: 42,
                    host: "127.0.0.1".to_string(),
                    root: "/srv/data".to_string(),
                    port: Some(9000),
                    started_at: "2026-08-21T12:00:00Z".to_string(),
                    version: "0.1.0".to_string(),
                }),
            },
        }
    }

    #[test]
    fn a_running_answer_is_the_servers_five_fields() {
        let text = render(&running("/srv/data/.tinio"), &resolution("/srv/data"))
            .expect("status template");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(
            lines,
            [
                "running",
                "endpoint: 127.0.0.1:9000",
                "root: /srv/data",
                "pid: 42",
                "started: 2026-08-21T12:00:00Z",
                "state dir: /srv/data/.tinio",
            ]
        );
    }

    #[test]
    fn a_stopped_answer_is_a_reported_state_on_one_line_each() {
        let resolution = resolution("/srv/data");

        // No file at all: the uninitialised shape.
        let text =
            render(&stopped("/srv/data/.tinio", None), &resolution).expect("status template");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], "stopped", "{text}");
        assert_eq!(lines[1], "endpoint: -", "{text}");
        assert_eq!(lines[2], "root: /srv/data", "{text}");
        assert_eq!(lines[3], "pid: -", "{text}");
        assert_eq!(lines[4], "started: -", "{text}");
        assert_eq!(lines[5], "state dir: /srv/data/.tinio", "{text}");
        assert!(lines[6].contains("no server has run"), "{text}");

        // A stale file: the same five fields, and the channel the file names in the message —
        // its pid and start time are **not** printed as if a server were up.
        let text = render(
            &stopped(
                "/srv/data/.tinio",
                Some("/run/tinio/tinio-0123456789abcdef.sock"),
            ),
            &resolution,
        )
        .expect("status template");
        assert!(text.contains("stopped"), "{text}");
        assert!(!text.contains("pid: 0"), "{text}");
        assert!(
            text.contains("/run/tinio/tinio-0123456789abcdef.sock"),
            "the message names the channel nothing answers on: {text}"
        );
        assert!(text.contains("stale"), "{text}");
    }

    #[test]
    fn the_endpoint_brackets_an_ipv6_literal() {
        // `/status` reports the bare address it bound, so `[::1]:9000` is the one spelling that
        // round-trips.
        assert_eq!(endpoint("127.0.0.1", Some(9000)), "127.0.0.1:9000");
        assert_eq!(endpoint("::1", Some(9000)), "[::1]:9000");
        assert_eq!(endpoint("0.0.0.0", None), "0.0.0.0");
    }
}
