//! The storage class of an object in a tiered backend.

use parse_display::{Display, FromStr};
use serde::{Deserialize, Serialize};

/// The class an object is stored in: the filesystem plane (`STANDARD`) or
/// the PostgreSQL cold plane (`STANDARD_IA`).
///
/// The wire spellings are S3's (`STANDARD`, `STANDARD_IA`, T10). Only
/// `GetObjectAttributes` reports the class; a backend without tiers answers
/// `STANDARD` for every object.
///
/// # Examples
///
/// ```rust
/// use tinio_core::Class;
///
/// assert_eq!(Class::StandardIa.to_string(), "STANDARD_IA");
/// assert_eq!("STANDARD_IA".parse::<Class>().unwrap(), Class::StandardIa);
/// assert!("GLACIER".parse::<Class>().is_err());
/// assert_eq!(
///     serde_json::to_string(&Class::StandardIa).unwrap(),
///     r#""STANDARD_IA""#
/// );
/// ```
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Display, FromStr, Serialize, Deserialize,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[display(style = "SNAKE_CASE")]
pub enum Class {
    /// The filesystem plane: every object of a backend without tiers.
    #[default]
    Standard,
    /// The PostgreSQL cold plane.
    StandardIa,
}

impl Class {
    /// The S3 wire spelling.
    ///
    /// Same spelling as derived [`Display`]/[`FromStr`] (`SNAKE_CASE`).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::Class;
    ///
    /// assert_eq!(Class::Standard.as_str(), "STANDARD");
    /// assert_eq!(Class::StandardIa.as_str(), "STANDARD_IA");
    /// assert_eq!(Class::StandardIa.to_string(), Class::StandardIa.as_str());
    /// ```
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::StandardIa => "STANDARD_IA",
        }
    }
}
