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
mod serving;

use std::process::ExitCode;

/// The one `(Config, Access)` builder the doctor's pg leg shares with the serving half.
#[cfg(feature = "pg")]
pub(crate) use self::run::pg_backend_config;
#[cfg(feature = "api")]
pub use self::{api::api_options, args::ApiFlags};
#[doc(inline)]
pub use self::{args::Args, run::run, serve::serve};
#[cfg(feature = "api")]
use crate::_api::Plane;
#[cfg(feature = "pg")]
use crate::_pg;
use crate::{
    _fs::FsStorage,
    _sandbox::Adopted,
    resolve::{Pipelines, Resolution},
    signal,
};

/// A constructed backend and its already-bound management plane — what the serving half hands to
/// [`serve_backend`](serve::serve_backend) once the backend exists.
///
/// **The plane rides with the storage because the two are built together and the plane's field is
/// the one `api`-gated value**: a filesystem run builds both in the pre-runtime half
/// (`Served::Fs`), a pg run builds both inside the runtime, and the serving body reads one value
/// either way. It is a struct rather than a pair of enum-field bindings so neither `#[cfg]` has to
/// reach into a variant pattern.
pub(crate) struct Prepared<S> {
    /// The constructed backend, bound to the data plane by
    /// [`serve_backend`](serve::serve_backend).
    pub(crate) storage: S,
    /// The bound management plane — the control channel, created before the repair runs
    /// (`api` builds only).
    #[cfg(feature = "api")]
    pub(crate) plane: Plane,
}

/// The backend a running server serves — the **one seam** the serving half dispatches on.
///
/// **Why an enum rather than two serving halves.** §1's pre-runtime half resolves the run, forks,
/// confines and opens the storage; §1's serving half (the runtime's) binds the planes, repairs
/// and serves. The storage is the only thing that differs between a filesystem run and a pg one,
/// so it is the only thing this value varies — the serving code itself is written once, generic
/// over `S: ServingBackend` (`serve::serve_backend`).
///
/// **The two arms carry different things because their construction points differ, and that is
/// the pre-runtime problem stated rather than hidden.** `FsStorage::new` is synchronous, so the
/// filesystem storage and its management plane are built in [`run`](super::run::run) — before
/// the runtime, which is where §1 step k puts it. `PgStorage::connect` is `async` (its pool
/// connects, it migrates and it takes the ownership lock **on a runtime**), and `run` is not
/// async, so the pg arm carries the resolved construction inputs and the storage is built inside
/// the runtime [`serve`](super::serve::serve) runs on. Neither arm spawns a runtime of its own:
/// the process has exactly one (design decision 2), and it is `run::on_runtime`'s.
///
/// **The filesystem arm is boxed**, for the reason [`Server::Ready`] is: a whole `FsStorage` and
/// a bound plane dwarf the pg arm's two small values, and `clippy::large_enum_variant` is a
/// `-D warnings` error here.
pub(crate) enum Served {
    /// A filesystem run: the storage and the bound management plane the pre-runtime half built
    /// (the plane's bind is the plane's single-instance gate, so it still precedes the repair).
    Fs(Box<Prepared<FsStorage>>),
    /// A pg run: the runtime configuration and the access mode, to be constructed inside the
    /// runtime. The mode is [`Access::ReadWrite`](crate::_pg::Access::ReadWrite) for a writable
    /// instance — which takes the PG12 ownership guard — and `ReadOnly` for one that does not.
    #[cfg(feature = "pg")]
    Pg {
        /// The runtime configuration: the resolved dsn (`TINIO_PG_DSN` folded in) and pool size.
        config: _pg::Config,
        /// The run's access mode (PG6/PG12): `ReadWrite` migrates and takes the ownership lock,
        /// `ReadOnly` does neither.
        access: _pg::Access,
    },
}

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
/// `backend` and `pipelines` are here rather than in [`serve()`] for one reason: the management
/// plane's bind is the plane's single-instance gate and belongs **before** the repair, and the
/// plane's `/metrics` route is fed by a `Gauges` built from this task's `ServerMetrics` — which
/// reads the storage. For the filesystem backend (`Served::Fs`) those are built in `run` and
/// carried here, so the bind precedes the runtime as it always has; for pg (`Served::Pg`) the
/// storage is constructed inside the runtime and its plane is bound there, before the repair.
/// `Plane::bind` and `select` are synchronous by design, so none of this needs a runtime, which
/// is exactly why the filesystem arm can precede one.
///
/// **The backend is the one generic seam** (`Served`): the serving half is written once over
/// `S: ServingBackend` and hands `serve` whichever storage this run resolved, so no serving code
/// is duplicated per backend. `Served::Fs`'s `plane` is the one `api`-gated field (and
/// `ServerMetrics`, `refresh_handle` and the adapter's `MetricsSource` impl are gated with it),
/// because `tinio-api` is an optional dependency of this crate: in a `--no-default-features`
/// build there is no plane, no state file and no single-instance gate, which is the api-off build
/// the design's Risk table describes. The serving half still serves, and nothing else in this
/// struct mentions the plane's crate.
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
    /// The backend this run serves ([`Served`]): the filesystem storage and plane the
    /// pre-runtime half built, or the pg inputs the runtime constructs from.
    backend: Served,
    /// The pipelines' handles — shut down and drained at the end of [`serve()`], IO first.
    pipelines: Pipelines,
    /// Optional hook fired once `state` is published (Windows SCM reports `Running` here).
    on_ready: Option<Box<dyn FnOnce() + Send>>,
}

impl Ready {
    /// The process's one shutdown channel, for the SCM control handler (`service::control_event`).
    #[cfg_attr(
        not(windows),
        expect(
            dead_code,
            reason = "the accessor serves the SCM control handler (`service::control_event`), which exists on Windows only"
        )
    )]
    pub(crate) fn handles(&self) -> &signal::Handles {
        &self.handles
    }

    /// Install a one-shot callback invoked after `state` publication (readiness).
    #[cfg(windows)]
    pub(crate) fn set_on_ready(&mut self, on_ready: impl FnOnce() + Send + 'static) {
        self.on_ready = Some(Box::new(on_ready));
    }
}
