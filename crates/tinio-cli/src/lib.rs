//! Command-line interface for tinio.
//!
//! Implements the lifecycle commands — `server`/`start`, `status`, `stop`,
//! `doctor` — with a Minio-style invocation (positional directory argument,
//! default ports 9000/9001). The full `run()` entry point (clap argument
//! parsing, command dispatch, global exit-code mapping) is implemented in
//! US2 (task T066); the placeholder below exists so the facade binary in
//! `crates/tinio` compiles from the workspace setup phase onward.
//!
//! The command implementations land in `commands/` (start, status, stop,
//! doctor).

mod error;

use std::process::ExitCode;

pub use self::error::Error;

/// Entry point for the `tinio` CLI.
///
/// Parses command-line arguments, dispatches to the requested command, and
/// maps every outcome onto the documented exit codes (0 success, 1
/// operational error, 2 usage error).
///
/// # Examples
///
/// ```no_run
/// let _ = tinio_cli::run();
/// ```
///
/// # Placeholder
///
/// Returns success without doing anything until task T066 implements the real
/// argument parsing and command dispatch.
pub fn run() -> ExitCode {
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;

    #[test]
    fn placeholder_run_returns_success() {
        assert_eq!(super::run(), ExitCode::SUCCESS);
    }
}
