//! The tolerant on/off switch grammar, defined once: `1`/`true` and `0`/`false`, trimmed.
//!
//! [`parse_switch`] has two readers, and they are the two channels that decide whether a run
//! protects its root or confines itself: `tinio-config`'s `TINIO_READ_ONLY`/`TINIO_SANDBOX`
//! variables (`sources.rs`), and the CLI's `--read-only`/`--sandbox` flags, which carry an `env`
//! attribute and therefore parse through a `value_parser` of their own
//! (`crates/tinio-cli/src/commands/mod.rs`).
//!
//! **The two copies this module replaced are why it exists.** While each side carried its own
//! `match`, the four accepted spellings and the refusal's wording happened to be identical —
//! which is the state two implementations are in exactly as long as an author remembers the other
//! exists. A fifth spelling, or a reworded refusal, added on one side would leave
//! `TINIO_READ_ONLY=yes` and `--read-only=yes` answering differently, and the value being parsed is
//! the one that decides whether the process confines itself: a default where a typo used to be a
//! startup error is the failure both designs exist to prevent, so the rule gets one definer and no
//! reader keeps a copy.

/// Parse an on/off switch value: `1`/`true` and `0`/`false`, trimmed. Anything else is refused.
///
/// **Refused rather than defaulted, an empty value included**, which is never "unset" and never
/// `false`: the caller reads this answer to decide whether to protect its root or confine itself,
/// so a value nobody recognises has to stop the run rather than pick one of the two.
///
/// **The refusal is a [`String`] and not an error type of this crate's, and that is the callers'
/// shape rather than an omission.** The message is the whole payload. One reader is clap's
/// `value_parser` contract, which is `Result<T, String>` and takes nothing else; the other folds
/// the same message into `tinio-config`'s `Error::invalid_value` beside its variable name, so a
/// typed error here would be converted back into this string at both ends and every reader would
/// still name its own type. A `String` carries the one decision — the exact spelling the contract
/// documents — and adds no dependency to a module whose content is one `match`.
///
/// # Examples
///
/// ```
/// use tinio_util::switch::parse_switch;
///
/// assert!(parse_switch(" 1 ").unwrap());
/// assert!(!parse_switch("false").unwrap());
/// assert!(parse_switch("yes").unwrap_err().contains("0/1/true/false"));
/// ```
pub fn parse_switch(raw: &str) -> Result<bool, String> {
    match raw.trim() {
        "1" | "true" => Ok(true),
        "0" | "false" => Ok(false),
        other => Err(format!("expected 0/1/true/false, got `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_four_spellings_parse_and_the_rest_is_refused() {
        for raw in ["1", "true", " 1 ", "\ttrue\n"] {
            assert!(parse_switch(raw).unwrap(), "{raw:?}");
        }
        for raw in ["0", "false", " 0 "] {
            assert!(!parse_switch(raw).unwrap(), "{raw:?}");
        }
        // An empty value and an unknown spelling are errors, never "unset" or `false`; the
        // refusal names the grammar and quotes what it was given.
        for raw in ["", "yes", "TRUE", "2"] {
            let err = parse_switch(raw).unwrap_err();
            assert!(err.contains("0/1/true/false"), "{raw:?}: {err}");
            assert!(err.contains(raw), "{raw:?}: {err}");
        }
    }
}
