//! Thin binary entry point: all logic lives in the tinio-cli crate.

use std::process::ExitCode;

fn main() -> ExitCode {
    tinio_cli::run()
}
