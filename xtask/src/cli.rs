//! The argument grammar for `cargo xtask`, in clap's derive API.
//!
//! `sqlx` takes one mode subcommand, `check` or `update`, and each leaf takes
//! three options; `bench-ab` takes the two legs and the shape of the run. Every
//! field's doc comment is its help text, and clap itself rejects a missing or
//! unknown subcommand, an unknown flag or a missing value -- printing the usage
//! line and exiting 2, which is the contract the hand-rolled grammar used to
//! keep.

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
    /// Measure one operation between two builds, interleaved, and decide.
    BenchAb(BenchAbArgs),
}

/// The `bench-ab` command: one named operation, two builds, alternating rounds.
#[derive(Debug, Args)]
pub(crate) struct BenchAbArgs {
    /// The measurement binary both legs run (the `ab` bench target).
    #[arg(long, value_name = "PATH")]
    pub(crate) exe: PathBuf,
    /// The second build's measurement binary; defaults to `--exe`, which
    /// compares two operations of one build.
    #[arg(long, value_name = "PATH")]
    pub(crate) exe_b: Option<PathBuf>,
    /// The operation leg A measures.
    #[arg(long, value_name = "OP")]
    pub(crate) a: String,
    /// The operation leg B measures.
    #[arg(long, value_name = "OP")]
    pub(crate) b: String,
    /// Timed samples per leg per round.
    #[arg(long, default_value_t = 2000, value_name = "N")]
    pub(crate) iters: u32,
    /// Untimed invocations per leg per round.
    #[arg(long, default_value_t = 200, value_name = "N")]
    pub(crate) warmup: u32,
    /// Rounds; each runs both legs, in the order that round alternates to.
    #[arg(long, default_value_t = 7, value_name = "N")]
    pub(crate) rounds: u32,
    /// The factor by which a round's two flush controls may differ before the
    /// round is dropped as regime-mixed.
    #[arg(long, default_value_t = 2.0, value_name = "F")]
    pub(crate) flush_band: f64,
    /// The surviving rounds a verdict needs.
    #[arg(long, default_value_t = 5, value_name = "N")]
    pub(crate) min_rounds: usize,
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

    use super::{BenchAbArgs, Cli, Command, Mode, SqlxCacheArgs, SqlxCommand};

    /// The subcommand of a command line that must parse.
    fn command_of(argv: &[&str]) -> SqlxCommand {
        match Cli::try_parse_from(argv)
            .expect("the command line should parse")
            .command
        {
            Command::Sqlx(args) => args.command,
            Command::BenchAb(_) => panic!("not a sqlx command line"),
        }
    }

    /// The `bench-ab` options of a command line that must parse.
    fn bench_ab_of(argv: &[&str]) -> BenchAbArgs {
        match Cli::try_parse_from(argv)
            .expect("the command line should parse")
            .command
        {
            Command::BenchAb(args) => args,
            Command::Sqlx(_) => panic!("not a bench-ab command line"),
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
    fn bench_ab_takes_both_legs_and_defaults_the_run_shape() {
        let args = bench_ab_of(&[
            "xtask",
            "bench-ab",
            "--exe",
            "ab.exe",
            "--a",
            "noop",
            "--b",
            "spin_20us",
        ]);
        assert_eq!(args.exe, Path::new("ab.exe"));
        assert!(args.exe_b.is_none(), "one binary compares two operations");
        assert_eq!(args.a, "noop");
        assert_eq!(args.b, "spin_20us");
        assert_eq!(args.iters, 2000);
        assert_eq!(args.warmup, 200);
        assert_eq!(args.rounds, 7);
        assert_eq!(args.flush_band, 2.0);
        assert_eq!(args.min_rounds, 5);
    }

    #[test]
    fn bench_ab_takes_a_second_build_and_the_run_shape() {
        let args = bench_ab_of(&[
            "xtask",
            "bench-ab",
            "--exe",
            "a.exe",
            "--exe-b",
            "b.exe",
            "--a",
            "head_object_plain",
            "--b",
            "head_object_plain",
            "--rounds",
            "11",
            "--iters",
            "5000",
            "--flush-band",
            "1.5",
            "--min-rounds",
            "9",
        ]);
        assert_eq!(args.exe_b.as_deref(), Some(Path::new("b.exe")));
        assert_eq!(args.rounds, 11);
        assert_eq!(args.iters, 5000);
        assert_eq!(args.flush_band, 1.5);
        assert_eq!(args.min_rounds, 9);
    }

    #[test]
    fn bench_ab_requires_both_operations() {
        assert_eq!(
            error_kind(&["xtask", "bench-ab", "--exe", "ab.exe", "--a", "noop"]),
            ErrorKind::MissingRequiredArgument
        );
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
