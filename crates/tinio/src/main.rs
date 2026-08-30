//! Thin binary entry point: all logic lives in the tinio-cli crate.

use std::process::ExitCode;

use tinio::_cli;

fn main() -> ExitCode {
    _cli::run()
}
