//! The launcher contract shared by the child-launching platforms.

pub mod payload;
#[cfg(unix)]
pub mod probe;
pub mod relay;
pub mod supervise;

mod adopt;
mod control;
mod plan;

// The fork-probe handshake, on its own line rather than in the group above: it
// exists only where a launcher forks a throwaway child — the two unix platforms,
// and neither of the others — so its names cannot be published unconditionally.
#[cfg(unix)]
pub use self::probe::{ProbeSteps, await_verdict, report_verdict};
// `Adopted`, `adopt`, `filter_argv`, `ControlHandle` and `LaunchPlan` are declared in the
// submodules above and republished here, because `lib.rs` names them as `launcher::{Adopted,
// adopt, LaunchPlan}`: a path the crate root re-exports has to resolve from this module.
pub use self::{
    adopt::{Adopted, adopt, filter_argv},
    control::ControlHandle,
    payload::{
        CONTROL_FD_PREFIX, LISTENER_FD_PREFIX, LaunchPayload, SandboxSpec, control_fd_arg,
        control_fd_from_arg, is_control_fd_arg, is_launcher_fd_arg, is_listener_fd_arg,
        listener_fd_arg, listener_fd_from_arg,
    },
    plan::LaunchPlan,
    relay::{Observer, Terminal},
    supervise::{Child, Readiness, supervise},
};
