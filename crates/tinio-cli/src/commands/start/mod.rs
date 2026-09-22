//! `tinio server` — the process sequence of design §1.
//!
//! Nothing here may construct an `FsStorage` before the confinement decision is made:
//! constructing one opens the state database, and under a launcher the *child* opens it
//! (sandbox design decision 18 and its shared plan's §4).
//!
//! This module builds no runtime either: [`run()`] is steps a–j, [`serve()`] is the half that
//! runs on the one the process builds (decision 2). A launcher returns from [`run()`] with the
//! status it supervised and never reaches step k at all.

#[cfg(feature = "api")]
pub(crate) mod api;
mod args;
#[cfg(feature = "api")]
mod metrics;
mod run;
mod serve;

use std::process::ExitCode;

#[cfg(feature = "api")]
pub use self::{api::api_options, args::ApiFlags};
#[doc(inline)]
pub use self::{args::Args, run::run, serve::serve};
#[cfg(feature = "api")]
use crate::_api::Plane;
use crate::{
    _fs::FsStorage,
    _sandbox::Adopted,
    resolve::{Pipelines, Resolution},
    signal,
};

/// What §1's pre-runtime half leaves for the process's one runtime.
///
/// **No `Debug`**: the payload's `Ready` holds a [`Pipelines`] and a plane, and
/// `tinio_server::pipeline::Pipelines` implements no `Debug` at all — so a derive here would
/// not compile, and a hand-written one could print only a fraction of the value. Nothing
/// prints a `Server`: it is matched on by the dispatcher and by no one else.
///
/// # Examples
///
/// ```rust
/// use std::process::ExitCode;
///
/// use tinio_cli::commands::start::Server;
///
/// // A launcher's arm carries the status it supervised and never builds a runtime.
/// let launched = Server::Launched(ExitCode::SUCCESS);
/// assert!(matches!(launched, Server::Launched(_)));
/// ```
pub enum Server {
    /// The macOS/Windows launcher: the confined child has been supervised, and its status is
    /// this process's answer. A launching process never builds a runtime at all.
    Launched(ExitCode),
    /// The serving half: steps a–j are fixed, and step k — the runtime — is `run::on_runtime`'s.
    /// Boxed because the serving half is the larger variant by far, and
    /// `clippy::large_enum_variant` is a `-D warnings` error here.
    Ready(Box<Ready>),
}

/// The serving half's state, built by [`run()`] and read by nothing but [`serve()`].
///
/// `storage`, `pipelines` and the plane's own fields are here rather than in [`serve()`] for
/// one reason: `Plane::bind` is the plane's single-instance gate and belongs **before** the
/// repair, and the plane's `/metrics` route is fed by a `Gauges` built from this task's
/// `ServerMetrics` — which reads the storage. `Plane::bind` and `select` are synchronous by
/// design, so none of this needs a runtime, which is exactly why it can precede one.
///
/// **`plane` is the one `api`-gated field** (and `ServerMetrics`, `refresh_handle` and the
/// adapter's `MetricsSource` impl are gated with it), because `tinio-api` is an optional
/// dependency of this crate: in a `--no-default-features` build there is no plane, no state
/// file and no single-instance gate, which is the api-off build the design's Risk table
/// describes. The serving half still serves, and nothing else in this struct mentions the
/// plane's crate.
pub struct Ready {
    /// The resolution steps a–e produced: root, config, mode, state dir.
    resolution: Resolution,
    /// The flags the run resolved — the **authoritative** parse's, `.env` included.
    ///
    /// The resolution has folded every one of them into the config already; what the serving
    /// half still reads is the credential pair's origin, which is a property of the arguments
    /// (were the two variables set?) rather than of the resolved config, and the sandbox plans'
    /// shared serving signature (§4) takes `args` beside `storage`, the inherited listener and
    /// `stop` for whatever the confinement needs next.
    args: Args,
    /// The adopted launch payload, when this process is a confined child.
    adopted: Option<Adopted>,
    /// The signal handlers, installed before the runtime existed (§1 step j).
    handles: signal::Handles,
    /// The storage the pre-runtime half opened; [`serve()`] binds it to the data plane.
    storage: FsStorage,
    /// The pipelines' handles — shut down and drained at the end of [`serve()`], IO first.
    pipelines: Pipelines,
    /// The bound management plane — the control channel, created before the repair runs, and
    /// the owner of the one `Gauges` the adapter was injected into. There is **no** `metrics`
    /// field beside it: the `Arc<ServerMetrics>` moved into that registry in [`run()`], and
    /// nothing in the serving half would have a use for a second handle — the scrape's cadence
    /// and the scan's schedule are the plane's reader's, not this module's.
    #[cfg(feature = "api")]
    plane: Plane,
    /// Optional hook fired once `state` is published (Windows SCM reports `Running` here).
    #[cfg_attr(not(windows), allow(dead_code))]
    on_ready: Option<Box<dyn FnOnce() + Send>>,
}

impl Ready {
    /// The process's one shutdown channel, for the SCM control handler (`service::control_event`).
    #[cfg_attr(not(windows), allow(dead_code))]
    pub(crate) fn handles(&self) -> &signal::Handles {
        &self.handles
    }

    /// Install a one-shot callback invoked after `state` publication (readiness).
    #[cfg(windows)]
    pub(crate) fn set_on_ready(&mut self, on_ready: impl FnOnce() + Send + 'static) {
        self.on_ready = Some(Box::new(on_ready));
    }
}
