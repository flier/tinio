//! The `--sandbox` seam: three channels, one decision, one confinement (design §7, decision 10).
//!
//! The three channels are the `--sandbox` flag, `TINIO_SANDBOX` and `[server] sandbox`, and they
//! collapse to one answer in [`plan`], which is also where every *static* refusal is raised —
//! before the fork, so the operator's shell sees it. [`confine`] is the *dynamic* half, after
//! the fork and before the runtime exists: on Linux this process enters the jail **in place**;
//! on Windows and macOS the listener is bound here and a confined child is launched to serve
//! on it.
//!
//! **This module is compiled in every build**, because the feature gates behaviour and not
//! linkage: `TINIO_SANDBOX` and `[server] sandbox` cannot be compiled away, and code that does
//! not exist cannot refuse. The feature takes the `--sandbox` *flag* and nothing else: the
//! variable keeps an argument of its own in that build (hidden, `start::Args::sandbox`'s
//! feature-off declaration), so the channel still reaches this module and still refuses.
//!
//! **Confinement mechanics are [`tinio_sandbox::serve::Spec`]** — this module owns enablement,
//! product refusals, launcher API handoff/`state` publish, and FR-036's OTLP trade
//! (`lost_export`, whose event `crate::commands::start::run` emits); it does not name platform
//! `Backend` / `Options` types.

mod child;
mod confine;
mod plan;
mod product;

#[cfg(test)]
mod tests;

pub(crate) use child::on_stop;
pub use child::{adopted_control, adopted_listener, apply_self_confinement, filter_argv};
pub use confine::confine;
pub(crate) use confine::lost_export;
pub use plan::{Channel, Request, StateDirSource, plan, resolve_flag};
#[cfg(all(test, feature = "sandbox"))]
pub(crate) use product::{
    refuse_in_root_state_dir, refuse_underived_state_dir, refuse_unwritable_state_dir,
    sandbox_request,
};

use crate::{_sandbox, Error};

/// Map a facade confinement error into the CLI's exit-code vocabulary.
///
/// An operator typo ([`tinio_sandbox::Error::is_usage`]) is usage (exit 2). A child-side
/// handoff this platform does not perform stays the operational sentence the facade's source
/// carries. Every other sandbox failure stays operational (exit 1).
fn sandbox_err(err: _sandbox::Error) -> Error {
    if err.is_usage() {
        return Error::Usage(err.to_string());
    }
    match err {
        // The three handoff steps, named by the crate that raises them: a caller matching the
        // literals here would have this answer (exit 1, operational) change silently under a
        // rename in the facade.
        _sandbox::Error::Step {
            step:
                _sandbox::STEP_ADOPTED_LISTENER
                | _sandbox::STEP_ADOPTED_CONTROL
                | _sandbox::STEP_ON_STOP,
            source,
        } => Error::Operational(source.to_string()),
        other => Error::from(other),
    }
}
