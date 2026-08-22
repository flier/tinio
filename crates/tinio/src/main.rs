//! Thin binary entry point: all logic lives in the tinio-cli crate.

fn main() -> std::process::ExitCode {
    tinio_cli::run()
}
