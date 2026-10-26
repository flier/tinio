//! The in-process server's backend selector: the [`Backend`] enum and the
//! `TINIO_E2E_BACKEND` grammar that names one.
//!
//! A module of its own, with no dependency beyond `std`, so its own unit tests
//! can ride a default-harness target: the cucumber binary is `harness = false`
//! and runs none of its `#[test]`s, and `tests/traceability.rs` path-includes
//! this file the way it includes `steps/tag_filter.rs`.
//!
//! `Tier` is compiled only for the `tier` feature, and a build without that
//! feature **refuses the value** rather than falling back to the filesystem:
//! the tag route (`@tier`) already fails loudly there — the step module is not
//! compiled, so the run stops on undefined steps — and a silently filesystem
//! server under a tier request would be the same fail-open shape.

/// Which storage backend the in-process server runs on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Backend {
    #[default]
    Fs,
    Mem,
    /// The shared PostgreSQL backend (`tinio_pg::PgStorage`), served only in a `--features pg`
    /// build (PG8/PG17).
    Pg,
    /// The tiered backend (`tinio_tier::TierStorage`), served only in a `--features tier` build
    /// (T25): the warm filesystem and the pg cold plane under the layer's own tasks.
    #[cfg(feature = "tier")]
    Tier,
}

/// The backend a `TINIO_E2E_BACKEND` value names, or `None` for an unset or
/// unrecognized one (the tag path then falls back to the default, as before).
///
/// `tier` is the tier pass's own value (T25); in a build without the feature it
/// is a refusal, not a fallback.
pub fn backend_from_env(raw: Option<&str>) -> Option<Backend> {
    match raw {
        Some("mem") => Some(Backend::Mem),
        Some("pg") => Some(Backend::Pg),
        #[cfg(feature = "tier")]
        Some("tier") => Some(Backend::Tier),
        #[cfg(not(feature = "tier"))]
        Some("tier") => panic!("TINIO_E2E_BACKEND=tier needs a build with --features tier"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    //! The variable's grammar: the three always-available spellings, the tier
    //! spelling under its feature, and the refusal in a build without it.

    // The tests module also compiles into the cucumber binary (harness disabled — it never runs
    // there), where no `#[test]` reaches these imports.
    #[allow(
        unused_imports,
        clippy::allow_attributes,
        reason = "test support: the tests module compiles into the cucumber binary too (harness disabled — never runs there), where no `#[test]` reaches these imports"
    )]
    use super::{Backend, backend_from_env};

    #[test]
    fn the_shared_backends_parse_and_anything_else_is_unset() {
        assert_eq!(backend_from_env(Some("mem")), Some(Backend::Mem));
        assert_eq!(backend_from_env(Some("pg")), Some(Backend::Pg));
        for raw in [None, Some(""), Some("fs"), Some("MEM"), Some("tier ")] {
            assert_eq!(backend_from_env(raw), None, "{raw:?}");
        }
    }

    #[cfg(feature = "tier")]
    #[test]
    fn the_tier_backend_parses() {
        assert_eq!(backend_from_env(Some("tier")), Some(Backend::Tier));
    }

    #[cfg(not(feature = "tier"))]
    #[test]
    #[should_panic(expected = "needs a build with --features tier")]
    fn a_tier_backend_without_the_feature_refuses() {
        let _ = backend_from_env(Some("tier"));
    }
}
