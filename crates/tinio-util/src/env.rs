//! The `TINIO_*` environment-variable names.
//!
//! One definition per name, shared by the CLI that parses them and the tests that set or clear
//! them. The reading itself is the CLI's: every variable is a clap argument with an `env`
//! attribute (`crates/tinio-cli/src/cli.rs`), and nothing here reads a value.

/// The environment variable that selects read-only mode (FR-023).
pub const READ_ONLY_VAR: &str = "TINIO_READ_ONLY";

/// The environment variable that selects sandbox mode — decision 1's third channel, beside the
/// `--sandbox` flag and the `[server] sandbox` key.
pub const SANDBOX_VAR: &str = "TINIO_SANDBOX";

/// The environment variable that overrides the derived state dir (FR-023).
///
/// An explicit state dir used verbatim instead of the `~/.tinio/roots/<hash>/` derivation — the
/// escape hatch for the environments read-only mode is actually for (service accounts,
/// containers, immutable images) where the home may be absent or unwritable. It is consulted in
/// both modes, and what it moves differs between them: a read-only run's served state dir *is*
/// this value when it is set, while a read-write run's served dir stays `<root>/.tinio` and the
/// variable answers in place of the second derivation a client makes there.
pub const STATE_DIR_VAR: &str = "TINIO_STATE_DIR";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_names_are_the_contracts() {
        assert_eq!(READ_ONLY_VAR, "TINIO_READ_ONLY");
        assert_eq!(SANDBOX_VAR, "TINIO_SANDBOX");
        assert_eq!(STATE_DIR_VAR, "TINIO_STATE_DIR");
    }
}
