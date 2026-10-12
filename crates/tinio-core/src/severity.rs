//! How bad a diagnostic finding is — the three levels every renderer of one spells the same.
//!
//! **One type, two producers.** `tinio-cli`'s doctor report and `tinio-pg`'s doctor rows both
//! answer "how bad is this?" and the rows are folded into the report, so the vocabulary lives
//! here rather than being spelled once per crate and bridged by a hand-written mapping: a
//! mapping is one more place a level has to be added, and a missed arm is a compile error only
//! where someone remembered to write the arm.
//!
//! `Display` and the JSON spelling are the **same** derivation — the variant's name, lowercased
//! — rather than a hand-written word table beside `serde`'s, so one place spells the variants.
//!
//! # Examples
//!
//! ```
//! use tinio_core::severity::Severity;
//!
//! assert_eq!(Severity::Error.to_string(), "error");
//! assert!(Severity::Error > Severity::Warn);
//! ```

use parse_display::Display;
use serde::Serialize;

/// The severity a finding carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Display)]
#[serde(rename_all = "lowercase")]
#[display(style = "lowercase")]
pub enum Severity {
    /// Nothing to do.
    Ok,
    /// Something the operator should look at, repaired or not.
    Warn,
    /// Something that could not be repaired.
    Error,
}
