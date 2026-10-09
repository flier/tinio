//! The workspace's cargo-xtask entry point.
//!
//! `.cargo/config.toml` aliases `cargo xtask` to `cargo run --package xtask --`,
//! so the tool is a plain binary over clap's derive API with no dependency on
//! `cargo-sqlx`. `cli` owns the argument grammar in that derive -- the `sqlx`
//! command with its `check` and `update` subcommands -- `sqlx_cache` owns the
//! PostgreSQL statement cache, and `Error` is the crate's thiserror error: a
//! usage error exits 2, a run that could not finish exits 1.
//!
//! Exit codes: `0` on success, `1` when a run finds cache drift or cannot
//! generate the cache, `2` on a usage error -- clap's own rejections, plus a
//! missing dsn, overlapping paths or an empty generated set, which only the run
//! can see.

mod cli;
mod sqlx_cache;

use std::process::ExitCode;

use clap::{CommandFactory, Parser, error::ErrorKind};
use cli::{Cli, Command};
use sqlx_cache::run as run_sqlx_cache;
use thiserror::Error;

/// A failed run and the exit code it reports.
#[derive(Debug, Error)]
enum Error {
    /// The invocation is invalid as written; exit 2.
    #[error("usage: {0}")]
    Usage(String),
    /// The command ran but could not finish; exit 1.
    #[error("runtime: {0}")]
    Runtime(String),
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Sqlx(args) => match run_sqlx_cache(args.command) {
            Ok(code) => code,
            Err(Error::Usage(message)) => usage(&message),
            Err(failure) => {
                eprintln!("error: {failure}");
                ExitCode::from(1)
            }
        },
    }
}

/// Report a usage error the way clap does: the message, the usage line, exit 2.
fn usage(message: &str) -> ! {
    Cli::command()
        .error(ErrorKind::InvalidValue, message)
        .exit()
}
