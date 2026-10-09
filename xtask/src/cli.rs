//! The argument grammar for `cargo xtask`, in clap's derive API.
//!
//! `sqlx` takes one mode subcommand, `check` or `update`, and each leaf takes
//! three options. Every field's doc comment is its help text, and clap itself
//! rejects a missing or unknown subcommand, an unknown flag or a missing value
//! -- printing the usage line and exiting 2, which is the contract the
//! hand-rolled grammar used to keep.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// The workspace's developer commands.
#[derive(Debug, Parser)]
#[command(
    name = "cargo xtask",
    bin_name = "cargo xtask",
    about = "The workspace's own commands"
)]
pub(crate) struct Cli {
    /// The command to run.
    #[command(subcommand)]
    pub(crate) command: Command,
}

/// What `cargo xtask` can be asked to do.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Generate, compare or rewrite the PostgreSQL statement cache.
    Sqlx(SqlxArgs),
}

/// The `sqlx` command and the mode it runs in.
#[derive(Debug, Args)]
pub(crate) struct SqlxArgs {
    /// What to do with the generated entries.
    #[command(subcommand)]
    pub(crate) command: SqlxCommand,
}

/// What `sqlx` does with the generated entries.
#[derive(Debug, Subcommand)]
pub(crate) enum SqlxCommand {
    /// Compare against the cache and report drift; never writes.
    Check(SqlxCacheArgs),
    /// Make the cache exactly the generated set.
    Update(SqlxCacheArgs),
}

impl SqlxCommand {
    /// What this invocation does with the generated entries.
    pub(crate) fn mode(&self) -> Mode {
        match self {
            Self::Check(_) => Mode::Check,
            Self::Update(_) => Mode::Write,
        }
    }
}

/// Options of the `sqlx check` and `sqlx update` subcommands.
#[derive(Debug, Args)]
pub(crate) struct SqlxCacheArgs {
    /// The database to describe statements against; else `$DATABASE_URL`, else `$TINIO_PG_DSN`.
    #[arg(long, value_name = "URL")]
    pub(crate) dsn: Option<String>,
    /// The cache directory; defaults to the workspace-root `.sqlx/`.
    #[arg(long, value_name = "PATH")]
    pub(crate) cache_dir: Option<PathBuf>,
    /// The scratch directory; defaults to the workspace-root `target/tmp/sqlx-cache`.
    #[arg(long, value_name = "PATH")]
    pub(crate) scratch_dir: Option<PathBuf>,
}

/// What `sqlx` does with the generated entries.
#[derive(Clone, Copy)]
pub(crate) enum Mode {
    /// Compare against the cache and report drift; never writes.
    Check,
    /// Make the cache exactly the generated set.
    Write,
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use clap::{Parser, error::ErrorKind};

    use super::{Cli, Command, Mode, SqlxCacheArgs, SqlxCommand};

    /// The subcommand of a command line that must parse.
    fn command_of(argv: &[&str]) -> SqlxCommand {
        match Cli::try_parse_from(argv)
            .expect("the command line should parse")
            .command
        {
            Command::Sqlx(args) => args.command,
        }
    }

    /// The leaf options of a parsed subcommand.
    fn args_of(command: &SqlxCommand) -> &SqlxCacheArgs {
        match command {
            SqlxCommand::Check(args) | SqlxCommand::Update(args) => args,
        }
    }

    /// The error kind of a command line that must be rejected.
    fn error_kind(argv: &[&str]) -> ErrorKind {
        Cli::try_parse_from(argv)
            .expect_err("the command line should be rejected")
            .kind()
    }

    #[test]
    fn no_subcommand_is_rejected() {
        assert_eq!(
            error_kind(&["xtask"]),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn a_mode_is_required() {
        assert_eq!(
            error_kind(&["xtask", "sqlx"]),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
    }

    #[test]
    fn both_modes_together_are_rejected() {
        assert_eq!(
            error_kind(&["xtask", "sqlx", "check", "update"]),
            ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn check_is_accepted_and_selects_check_mode() {
        let command = command_of(&["xtask", "sqlx", "check"]);
        assert!(matches!(command.mode(), Mode::Check));
        assert!(args_of(&command).dsn.is_none());
    }

    #[test]
    fn update_is_accepted_and_selects_write_mode() {
        let command = command_of(&["xtask", "sqlx", "update"]);
        assert!(matches!(command.mode(), Mode::Write));
        assert!(args_of(&command).dsn.is_none());
    }

    #[test]
    fn dsn_and_directories_are_parsed() {
        let command = command_of(&[
            "xtask",
            "sqlx",
            "check",
            "--dsn",
            "postgres://tinio:tinio@127.0.0.1:55432/tinio_prepare?sslmode=disable",
            "--cache-dir",
            "some/cache",
            "--scratch-dir",
            "some/scratch",
        ]);
        let args = args_of(&command);
        assert_eq!(
            args.dsn.as_deref(),
            Some("postgres://tinio:tinio@127.0.0.1:55432/tinio_prepare?sslmode=disable")
        );
        assert_eq!(args.cache_dir.as_deref(), Some(Path::new("some/cache")));
        assert_eq!(args.scratch_dir.as_deref(), Some(Path::new("some/scratch")));
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        assert_eq!(
            error_kind(&["xtask", "sqlx", "check", "--bogus"]),
            ErrorKind::UnknownArgument
        );
    }

    #[test]
    fn an_unknown_subcommand_is_rejected() {
        assert_eq!(
            error_kind(&["xtask", "sqlx", "bogus"]),
            ErrorKind::InvalidSubcommand
        );
    }
}
