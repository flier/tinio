//! FR-023's half of the startup, where it is not the mode: the tool's state
//! directory when the served root is only ever read.
//!
//! The mode itself is resolved in `main` — it is three precedence levels read
//! straight off design §10 step 4's sources, and folding them anywhere else
//! would put a step of the sequence in a helper — while the relocation below is
//! a decision with a diagnostic, asked by two processes.

use std::{
    error::Error,
    path::{Path, PathBuf},
};

use tinio_config::state;

use crate::events::emit_on_error;

/// Where the tool's state lives in read-only mode, with the one diagnostic that
/// says where the answer came from.
///
/// **`None` is not "nowhere" but "the backend's own default"**: normal mode
/// leaves state at `<root>/.tinio/`, and answering that here would freeze a path
/// the settings already carry — the exposure rule is the one reader that wants
/// the resolved path rather than the absence, and it substitutes the default
/// itself.
///
/// Two processes ask this question, `main` and the confined child, and they have
/// to get the same answer: a `TINIO_STATE_DIR` that one of them read is a
/// confined server whose state directory the launcher exposed differently from
/// the one the child writes to.
///
/// **The report comes from the read that resolved the path rather than from a
/// second one**, which is why it is a callback and not a `String` returned
/// beside the directory: a caller that re-read `TINIO_STATE_DIR` to caption its
/// line could caption it with a source the path did not come from. `main` is
/// the process that reports — its stderr is where the operator's other startup
/// diagnostics go, and the diagnostic is the only thing the two call sites ever
/// differed in — so the child passes a no-op, its launcher having printed the
/// same line already.
pub fn resolve_read_only_state_dir(
    root: &Path,
    read_only: bool,
    report: impl FnOnce(&Path, Option<&str>),
) -> Result<Option<PathBuf>, Box<dyn Error + Send + Sync>> {
    if !read_only {
        return Ok(None);
    }
    // `TINIO_STATE_DIR` wins over the derived layout: the read-only
    // deployments where there is no writable home are exactly the ones
    // that need the escape hatch.
    let dir = match emit_on_error(state::env_state_dir())? {
        Some(dir) => {
            report(&dir, Some(state::STATE_DIR_VAR));
            dir
        }
        None => {
            let dir = emit_on_error(state::default_home_state_dir(root))?;
            report(&dir, None);
            dir
        }
    };
    Ok(Some(dir))
}
