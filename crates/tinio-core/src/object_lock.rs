//! S3 Object Lock: the stored model, the rule wire and the pure rules
//! (FR-043, OL10–OL13).
//!
//! **The model.** [`Config`] is the bucket's stored value: an enabled bit and
//! one optional default-retention rule. The storage layer splits it into the
//! enabled bit and the rule wire ([`Config::rule_wire`] /
//! [`Config::from_rule_wire`]). [`Lock`] is the per-version value; the store
//! splits it into the mode text, the retain-until instant and the legal-hold
//! text, and [`Lock::from_columns`] reads them back.
//!
//! **The plane JSON.** The management plane's bodies and the CLI's wire are the model's own
//! `serde` shape (OL30), so there is no second DTO: the fields are `snake_case`, [`Mode`] and
//! [`LegalHold`] keep their uppercase spellings, a [`Period`] is `{"days": N}` or
//! `{"years": N}`, and [`Retention::retain_until`] is an RFC3339 instant in UTC.
//!
//! **The rule wire.** The one internal encoding, owned here:
//! `MODE,PERIOD_KIND,COUNT` — `MODE` is `GOVERNANCE` or `COMPLIANCE`,
//! `PERIOD_KIND` is `DAYS` or `YEARS`, and `COUNT` is decimal. For example
//! `GOVERNANCE,DAYS,30` and `COMPLIANCE,YEARS,7`. The empty string is no rule.
//! The encoding is canonical uppercase and lossless for every valid rule; the
//! decoder is lenient about the `COUNT` spelling only (`01`, `+1`).
//! Decoding never errors: a corrupt or out-of-bounds wire loses the rule,
//! keeps the enabled bit, and logs one warning — the same self-healing read
//! as the lifecycle configuration, so no stored rule can silently become a
//! different one.
//!
//! **Validation is the request path's.** [`Invalid`] carries the two classes
//! in one vocabulary, reusing [`crate::lifecycle::InvalidKind`] (R14):
//! [`Shape`](crate::lifecycle::InvalidKind::Shape) is a document that does
//! not match the published schema (`MalformedXML` on the S3 surface), and
//! [`Semantic`](crate::lifecycle::InvalidKind::Semantic) is every value
//! violation (`InvalidRequest`). The model's shapes make two structural
//! guarantees instead of validating them: a [`Rule`] always carries a
//! [`DefaultRetention`], and a [`Period`] is always exactly one of days or
//! years. The S3 surface refuses the XML forms that would be empty or carry
//! both before it constructs one.
//!
//! **The rules are pure** (OL10). [`write_lock`] resolves the write-time
//! lock, [`apply_retention`] applies a retention update, [`apply_lock`]
//! applies the retention and the legal hold as one update, [`may_delete`]
//! checks a permanent delete, and [`default_retention_at`] evaluates a
//! default period. [`Bypass`] is a contract newtype, never a bare bool. An
//! expired retention is inert; COMPLIANCE and a legal hold cannot be
//! bypassed; the governance bypass never changes a legal hold (OL11).
//! [`Mode`] and [`LegalHold`] spell their display and parse forms once, so
//! the rule wire, the stored columns and the S3 headers agree.

use std::{
    str::FromStr,
    time::{Duration, SystemTime},
};

use derive_more::Display;
use parse_display::{Display as ParseDisplay, FromStr};
use serde::{Deserialize, Serialize};
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

use crate::lifecycle::{InvalidKind, SECONDS_PER_DAY};

#[cfg(test)]
mod tests;

/// The largest `DefaultRetention` day count (OL10: days `1..=36_500`).
pub const MAX_DAYS: u32 = 36_500;
/// The largest `DefaultRetention` year count (OL10: years `1..=100`).
pub const MAX_YEARS: u32 = 100;

// The reason each validation refusal carries, spelled once.
const ENABLED: &str = "ObjectLockEnabled is Enabled";
const DAYS_RANGE: &str = "DefaultRetention Days is between 1 and 36500";
const YEARS_RANGE: &str = "DefaultRetention Years is between 1 and 100";
const RETAIN_FUTURE: &str = "RetainUntilDate is in the future";
const LEGAL_HOLD_STATUS: &str = "the legal hold status is ON or OFF";

// The detail each fail-closed column decode carries. Only the decoder reports
// a detail; the protection rules answer their reason alone.
const PARTIAL_RETENTION: &str = "the stored retention carries only one of mode and retain-until";
const CORRUPT_MODE: &str = "the stored retention mode is not GOVERNANCE or COMPLIANCE";
const CORRUPT_LEGAL_HOLD: &str = "the stored legal hold is not ON or OFF";

/// A bucket's stored Object Lock configuration: the enabled bit and at most
/// one default-retention rule (OL4, OL13).
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::{Config, DefaultRetention, Mode, Period, Rule};
///
/// let config = Config {
///     enabled: true,
///     rule: Some(Rule {
///         default_retention: DefaultRetention {
///             mode: Mode::Governance,
///             period: Period::Days(30),
///         },
///     }),
/// };
/// assert_eq!(config.validate(), Ok(()));
/// assert_eq!(config.rule_wire(), "GOVERNANCE,DAYS,30");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Whether the bucket has Object Lock enabled. A stored configuration is
    /// always enabled; the request path validates the bit.
    pub enabled: bool,
    /// The default retention a version written without an explicit one
    /// receives; `None` for a bucket that only enables the feature.
    #[serde(default)]
    pub rule: Option<Rule>,
}

impl Config {
    /// Whether this is a configuration the request path may store (OL13).
    ///
    /// [`InvalidKind::Semantic`] throughout: the stored bit must be true, and
    /// a rule's period must be days `1..=36_500` or years `1..=100`. An
    /// empty rule and a rule carrying both periods are unrepresentable in
    /// [`Rule`] and [`Period`]; the S3 surface refuses those XML forms
    /// before constructing one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{lifecycle::InvalidKind, object_lock::Config};
    ///
    /// let refused = Config {
    ///     enabled: false,
    ///     rule: None,
    /// }
    /// .validate()
    /// .expect_err("only an enabled configuration is accepted");
    /// assert_eq!(refused.kind, InvalidKind::Semantic);
    /// assert_eq!(
    ///     Config {
    ///         enabled: true,
    ///         rule: None
    ///     }
    ///     .validate(),
    ///     Ok(())
    /// );
    /// ```
    pub fn validate(&self) -> Result<(), Invalid> {
        if !self.enabled {
            return Err(Invalid {
                reason: ENABLED,
                kind: InvalidKind::Semantic,
            });
        }
        if let Some(rule) = &self.rule {
            rule.validate()?;
        }
        Ok(())
    }

    /// The rule wire: `MODE,PERIOD_KIND,COUNT`, or the empty string for no
    /// rule (see the module docs; this module owns the encoding).
    ///
    /// Canonical and stable: every valid rule has exactly one spelling, and
    /// [`Config::from_rule_wire`] reads it back losslessly. The **encoder** is
    /// canonical; the **decoder is deliberately lenient about the count
    /// spelling** — a `u32` parse accepts a leading `+` and leading zeros
    /// (`+1`, `01`), which decode to the same rule and re-encode canonically.
    /// Only the count is lenient: the mode and period kinds are exact.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object_lock::{Config, DefaultRetention, Mode, Period, Rule};
    ///
    /// let config = Config {
    ///     enabled: true,
    ///     rule: Some(Rule {
    ///         default_retention: DefaultRetention {
    ///             mode: Mode::Compliance,
    ///             period: Period::Years(7),
    ///         },
    ///     }),
    /// };
    /// assert_eq!(config.rule_wire(), "COMPLIANCE,YEARS,7");
    /// assert_eq!(
    ///     Config {
    ///         enabled: true,
    ///         rule: None
    ///     }
    ///     .rule_wire(),
    ///     ""
    /// );
    /// ```
    pub fn rule_wire(&self) -> String {
        match &self.rule {
            None => String::new(),
            Some(rule) => match rule.default_retention.period {
                Period::Days(days) => {
                    format!("{},DAYS,{days}", rule.default_retention.mode)
                }
                Period::Years(years) => {
                    format!("{},YEARS,{years}", rule.default_retention.mode)
                }
            },
        }
    }

    /// Decode the rule wire, keeping `enabled` whatever the wire says.
    ///
    /// The empty wire is no rule. A corrupt or out-of-bounds wire is also no
    /// rule, after one warning: decoding never errors, and the enabled bit —
    /// the fact that drives enforcement — survives a damaged rule (the
    /// lifecycle's self-healing read, OL4). The count is read leniently
    /// (see [`Config::rule_wire`]); everything else is exact.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object_lock::{Config, Period};
    ///
    /// let config = Config::from_rule_wire(true, "GOVERNANCE,DAYS,30");
    /// assert!(config.enabled);
    /// assert_eq!(
    ///     config
    ///         .rule
    ///         .as_ref()
    ///         .map(|rule| rule.default_retention.period),
    ///     Some(Period::Days(30))
    /// );
    ///
    /// let corrupt = Config::from_rule_wire(true, "not a rule");
    /// assert!(corrupt.enabled);
    /// assert_eq!(corrupt.rule, None);
    /// ```
    pub fn from_rule_wire(enabled: bool, wire: &str) -> Config {
        if wire.is_empty() {
            return Config {
                enabled,
                rule: None,
            };
        }
        match parse_rule(wire) {
            Some(rule) => Config {
                enabled,
                rule: Some(rule),
            },
            None => {
                tracing::warn!(rule = wire, "discarding an unreadable object lock rule");
                Config {
                    enabled,
                    rule: None,
                }
            }
        }
    }
}

/// One default-retention rule: the mode and period a version written without
/// an explicit retention inherits.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::{DefaultRetention, Mode, Period, Rule};
///
/// let rule = Rule {
///     default_retention: DefaultRetention {
///         mode: Mode::Governance,
///         period: Period::Days(30),
///     },
/// };
/// assert_eq!(rule.default_retention.mode, Mode::Governance);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// The one default retention; a rule always carries it.
    pub default_retention: DefaultRetention,
}

impl Rule {
    fn validate(&self) -> Result<(), Invalid> {
        match self.default_retention.period {
            Period::Days(days) if !(1..=MAX_DAYS).contains(&days) => Err(Invalid {
                reason: DAYS_RANGE,
                kind: InvalidKind::Semantic,
            }),
            Period::Years(years) if !(1..=MAX_YEARS).contains(&years) => Err(Invalid {
                reason: YEARS_RANGE,
                kind: InvalidKind::Semantic,
            }),
            _ => Ok(()),
        }
    }
}

/// A rule's mode and period together: exactly one period by construction.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::{DefaultRetention, Mode, Period};
///
/// let default = DefaultRetention {
///     mode: Mode::Compliance,
///     period: Period::Years(7),
/// };
/// assert_eq!(default.period, Period::Years(7));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    /// The mode the default retention is placed in.
    pub mode: Mode,
    /// The period, counted from the version's write.
    pub period: Period,
}

/// How long a default retention lasts: days, or calendar years.
///
/// A `Years` period is calendar years, so 2024-02-29 plus one year is
/// 2025-02-28, not 2025-03-01 (OL10).
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::Period;
///
/// assert_eq!(Period::Days(30), Period::Days(30));
/// assert_ne!(Period::Days(30), Period::Years(30));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Period {
    /// Days, `1..=36_500`.
    Days(u32),
    /// Calendar years, `1..=100`.
    Years(u32),
}

/// The retention mode the S3 wire spells `GOVERNANCE` or `COMPLIANCE`.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::Mode;
///
/// assert_eq!(Mode::Governance.to_string(), "GOVERNANCE");
/// assert_eq!("COMPLIANCE".parse(), Ok(Mode::Compliance));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, ParseDisplay, FromStr, Serialize, Deserialize)]
#[display(style = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum Mode {
    /// Removable or shorten-able by a request carrying the governance
    /// bypass.
    Governance,
    /// Never removable, shortenable or mode-changed, bypass or not.
    Compliance,
}

/// A mode and the instant it protects a version until.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{Mode, Retention};
///
/// let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
/// let retention = Retention {
///     mode: Mode::Governance,
///     retain_until: now + Duration::from_secs(1),
/// };
/// assert_eq!(retention.validate(now), Ok(()));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Retention {
    /// The mode this retention is placed in.
    pub mode: Mode,
    /// The instant protection ends, strictly in the future on the request
    /// path. Serialized as an RFC3339 instant in UTC.
    #[serde(with = "rfc3339")]
    pub retain_until: SystemTime,
}

impl Retention {
    /// Whether the request path may place this retention at `now` (OL10,
    /// OL17).
    ///
    /// The only value violation is a date that is not strictly in the future;
    /// the mode is an enum, so it is always one of the two valid values. The
    /// class is [`InvalidKind::Semantic`] — the S3 surface answers
    /// `InvalidRequest`, the code this design pins for a past date.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::object_lock::{Mode, Retention};
    ///
    /// let now = SystemTime::UNIX_EPOCH + Duration::from_secs(100);
    /// let expired = Retention {
    ///     mode: Mode::Compliance,
    ///     retain_until: now,
    /// };
    /// assert!(expired.validate(now).is_err());
    ///
    /// let future = Retention {
    ///     mode: Mode::Compliance,
    ///     retain_until: now + Duration::from_secs(1),
    /// };
    /// assert_eq!(future.validate(now), Ok(()));
    /// ```
    pub fn validate(&self, now: SystemTime) -> Result<(), Invalid> {
        if self.retain_until <= now {
            return Err(Invalid {
                reason: RETAIN_FUTURE,
                kind: InvalidKind::Semantic,
            });
        }
        Ok(())
    }
}

/// A version's legal hold: `ON` refuses every permanent delete and has no
/// bypass, in either retention mode (OL11).
///
/// [`Default`] is `Off`: an absent hold is no hold.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::LegalHold;
///
/// assert_eq!(LegalHold::On.to_string(), "ON");
/// assert_eq!("OFF".parse(), Ok(LegalHold::Off));
/// ```
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, ParseDisplay, FromStr, Serialize, Deserialize,
)]
#[display(style = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
pub enum LegalHold {
    /// A hold is in force.
    On,
    /// No hold.
    #[default]
    Off,
}

impl LegalHold {
    /// Parse the request path's legal-hold status — exactly `ON` or `OFF`.
    ///
    /// Anything else is the schema class ([`InvalidKind::Shape`], the S3
    /// surface's `MalformedXML`), including the lowercase spellings: the
    /// header, the columns and the rule wire share one uppercase spelling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object_lock::LegalHold;
    ///
    /// assert_eq!(LegalHold::parse("ON"), Ok(LegalHold::On));
    /// assert!(LegalHold::parse("maybe").is_err());
    /// ```
    pub fn parse(value: &str) -> Result<Self, Invalid> {
        Self::from_str(value).map_err(|_| Invalid {
            reason: LEGAL_HOLD_STATUS,
            kind: InvalidKind::Shape,
        })
    }
}

/// A version's lock: an optional retention and a legal hold.
///
/// A lock-free version has no stored row; the store calls
/// [`Lock::from_columns`] with the three absent columns to say so.
///
/// [`Default`] is the empty lock — no retention and [`LegalHold::Off`]. It
/// carries no fact ([`Lock::carries_fact`] is `false`), so it is never a
/// stored row; it is the value a store answers when an update leaves none.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::{LegalHold, Lock};
///
/// let unlocked = Lock {
///     retention: None,
///     legal_hold: LegalHold::Off,
/// };
/// assert!(unlocked.retention.is_none());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lock {
    /// The retention in force, if any.
    pub retention: Option<Retention>,
    /// The legal hold, independent of the retention.
    pub legal_hold: LegalHold,
}

impl Lock {
    /// Whether this lock carries a fact the store must keep: a retention or an
    /// `ON` legal hold.
    ///
    /// This is the encode side's one test for "is this a lock row". A lock with
    /// neither fact is not a lock — the stores remove the row instead of
    /// writing an all-empty one — so the predicate and [`Lock::from_columns`]
    /// agree by construction.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object_lock::{LegalHold, Lock};
    ///
    /// let hold = Lock {
    ///     retention: None,
    ///     legal_hold: LegalHold::On,
    /// };
    /// assert!(hold.carries_fact());
    ///
    /// let empty = Lock {
    ///     retention: None,
    ///     legal_hold: LegalHold::Off,
    /// };
    /// assert!(!empty.carries_fact());
    /// ```
    pub fn carries_fact(&self) -> bool {
        self.retention.is_some() || self.legal_hold == LegalHold::On
    }

    /// Decode one `version_object_lock` row, fail-closed on every bad arm
    /// (OL5, OL10).
    ///
    /// The three columns are the stored mode text, the retain-until Unix
    /// nanoseconds and the legal-hold text. This function is pure; the store
    /// logs the refusal it gets back.
    ///
    /// - all three absent — `Ok(None)`: there is no row (or the row carries
    ///   no lock), which is an unlocked version.
    /// - a complete, valid retention and hold — `Ok(Some(lock))`.
    /// - mode and date both absent, hold `ON` — `Ok(Some(lock))` with no
    ///   retention: a legal-hold-only row.
    /// - mode and date both absent, hold `OFF` — `Ok(None)`: a row that
    ///   carries neither a retention nor a hold is not a lock.
    /// - the hold missing, or text other than `ON`/`OFF` —
    ///   `Err(RefusalReason::LegalHold)`, the non-bypassable reason: a hold
    ///   that cannot be read may hide one that is in force.
    /// - only one of mode and date present — `Err(RefusalReason::Compliance)`,
    ///   the conservative non-bypassable reason for a partial pair.
    /// - an unknown mode — `Err(RefusalReason::Compliance)`, also
    ///   fail-closed. The retain-until column is an `i64` of Unix nanoseconds,
    ///   and every `i64` is a representable instant, so the column type alone
    ///   rules out an unreadable date.
    ///
    /// The hold is checked first: a row that is bad in both ways reports the
    /// hold, the stronger protection.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::object_lock::{LegalHold, Lock};
    ///
    /// let hold = Lock::from_columns(None, None, Some("ON"))
    ///     .expect("a complete hold column decodes")
    ///     .expect("it is a lock");
    /// assert_eq!(hold.legal_hold, LegalHold::On);
    /// assert!(hold.retention.is_none());
    ///
    /// assert_eq!(Lock::from_columns(None, None, None), Ok(None));
    /// assert!(
    ///     Lock::from_columns(Some("GOVERNANCE"), None, Some("OFF")).is_err(),
    ///     "a partial mode/date pair fails closed"
    /// );
    /// ```
    pub fn from_columns(
        mode: Option<&str>,
        retain_until: Option<i64>,
        legal_hold: Option<&str>,
    ) -> Result<Option<Lock>, CorruptRow> {
        if mode.is_none() && retain_until.is_none() && legal_hold.is_none() {
            return Ok(None);
        }
        let legal_hold = match legal_hold {
            Some(text) => text
                .parse::<LegalHold>()
                .map_err(|_| corrupt_row(RefusalReason::LegalHold, CORRUPT_LEGAL_HOLD))?,
            None => return Err(corrupt_row(RefusalReason::LegalHold, CORRUPT_LEGAL_HOLD)),
        };
        let retention = match (mode, retain_until) {
            (None, None) => None,
            (Some(mode), Some(retain_until)) => {
                let mode = mode
                    .parse::<Mode>()
                    .map_err(|_| corrupt_row(RefusalReason::Compliance, CORRUPT_MODE))?;
                Some(Retention {
                    mode,
                    retain_until: instant_from_nanos(retain_until),
                })
            }
            _ => return Err(corrupt_row(RefusalReason::Compliance, PARTIAL_RETENTION)),
        };
        Ok(wrap(Lock {
            retention,
            legal_hold,
        }))
    }

    /// Encode this lock as the three stored `version_object_lock` columns — the inverse of
    /// [`Lock::from_columns`].
    ///
    /// The columns are the stored mode text, the retain-until Unix nanoseconds and the
    /// legal-hold text. A lock with no retention leaves the first two absent; the hold is
    /// always a spelling, `ON` or `OFF`.
    ///
    /// **The nanos are clamped to `i64`.** A stored column is an `i64` of Unix nanoseconds
    /// (roughly 1678–2262), while [`SystemTime`] reaches further; an instant outside that
    /// window is clamped to [`i64::MAX`] or [`i64::MIN`], so the encoder never refuses and the
    /// round trip is exact for every instant the column can hold.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::object_lock::{LegalHold, Lock, Mode, Retention};
    ///
    /// let lock = Lock {
    ///     retention: Some(Retention {
    ///         mode: Mode::Compliance,
    ///         retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(100),
    ///     }),
    ///     legal_hold: LegalHold::On,
    /// };
    /// let (mode, retain_until, legal_hold) = lock.columns();
    /// assert_eq!(mode.as_deref(), Some("COMPLIANCE"));
    /// assert_eq!(retain_until, Some(100 * 1_000_000_000));
    /// assert_eq!(legal_hold, "ON");
    /// ```
    pub fn columns(&self) -> (Option<String>, Option<i64>, String) {
        let (mode, retain_until) = match &self.retention {
            Some(retention) => (
                Some(retention.mode.to_string()),
                Some(nanos_since_epoch(retention.retain_until)),
            ),
            None => (None, None),
        };
        (mode, retain_until, self.legal_hold.to_string())
    }
}

/// The authority a retention update or a permanent delete carries: none, or
/// the governance bypass.
///
/// A contract newtype, never a bare `bool`: the bypass is only ever about an
/// unexpired GOVERNANCE retention, and naming it keeps the call sites from
/// reading like a generic flag (OL10).
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::Bypass;
///
/// assert_ne!(Bypass::None, Bypass::Governance);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bypass {
    /// The request carries no bypass.
    None,
    /// The request carries the governance bypass, and the caller's
    /// authority check granted it.
    Governance,
}

/// A stored row that fails closed: the class it refuses with, and the detail
/// the store logs.
///
/// Only the corrupt-row decoder reports a detail; the protection rules answer
/// their [`RefusalReason`] alone. The S3 surface maps [`CorruptRow::reason`];
/// [`CorruptRow::detail`] is never a wire value (D5).
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::{CorruptRow, RefusalReason};
///
/// let refusal = CorruptRow {
///     reason: RefusalReason::Compliance,
///     detail: "the stored mode/date pair is partial",
/// };
/// assert_eq!(
///     refusal.to_string(),
///     "compliance: the stored mode/date pair is partial"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display("{reason}: {detail}")]
pub struct CorruptRow {
    /// The class the decode refuses with.
    pub reason: RefusalReason,
    /// The operator-facing detail; one line, never a wire value.
    pub detail: &'static str,
}

/// Why a lock refused a write or a delete.
///
/// # Examples
///
/// ```rust
/// use tinio_core::object_lock::RefusalReason;
///
/// assert_eq!(RefusalReason::LegalHold.to_string(), "legal hold");
/// assert_ne!(RefusalReason::Compliance, RefusalReason::Governance);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
pub enum RefusalReason {
    /// A legal hold is in force; there is no bypass (OL11).
    #[display("legal hold")]
    LegalHold,
    /// A compliance retention is in force; there is no bypass (OL11).
    #[display("compliance")]
    Compliance,
    /// A governance retention is in force and the request did not carry the
    /// governance bypass (OL11).
    #[display("governance")]
    Governance,
}

/// A refused configuration or retention, in the lifecycle's two classes.
///
/// The kind is [`crate::lifecycle::InvalidKind`], reused so the two
/// subresources have one vocabulary (R14): `Shape` is `MalformedXML` and
/// `Semantic` is `InvalidRequest` at the S3 surface.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{lifecycle::InvalidKind, object_lock::Invalid};
///
/// let invalid = Invalid {
///     reason: "RetainUntilDate is in the future",
///     kind: InvalidKind::Semantic,
/// };
/// assert_eq!(invalid.to_string(), "RetainUntilDate is in the future");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Display)]
#[display("{reason}")]
pub struct Invalid {
    /// The operator-facing reason.
    pub reason: &'static str,
    /// The class the S3 surface maps to a code.
    pub kind: InvalidKind,
}

/// Resolve the lock a write stores: the explicit retention when the request
/// carries one, else the bucket's default (OL10).
///
/// A write that carries only a legal-hold header still calls this, so it
/// still receives the default retention; the caller then applies the
/// requested hold over the returned lock. The returned legal hold is always
/// [`LegalHold::Off`] because a legal hold is not a retention.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{DefaultRetention, LegalHold, Mode, Period, Rule, write_lock};
///
/// let rule = Rule {
///     default_retention: DefaultRetention {
///         mode: Mode::Governance,
///         period: Period::Days(7),
///     },
/// };
/// let now = SystemTime::UNIX_EPOCH;
/// let lock = write_lock(None, Some(&rule), now);
/// assert_eq!(
///     lock.retention.expect("the default is applied").retain_until,
///     now + Duration::from_secs(7 * 86_400)
/// );
/// assert_eq!(lock.legal_hold, LegalHold::Off);
/// ```
pub fn write_lock(explicit: Option<&Retention>, rule: Option<&Rule>, now: SystemTime) -> Lock {
    Lock {
        retention: explicit.cloned().or_else(|| {
            rule.map(|rule| Retention {
                mode: rule.default_retention.mode,
                retain_until: default_retention_at(rule, now),
            })
        }),
        legal_hold: LegalHold::Off,
    }
}

/// Apply a retention update: `Some` sets a retention, `None` clears one, and
/// a version with no lock is left alone (OL11).
///
/// The current lock's legal hold is preserved in every arm. An expired
/// retention is inert, so it constrains nothing. While an unexpired retention
/// is in force:
///
/// - COMPLIANCE may be extended (`retain_until` not earlier) in the same
///   mode; shortening, removal and a change of mode are refused, with or
///   without [`Bypass::Governance`].
/// - GOVERNANCE may be extended freely; shortening (including a mode change
///   to COMPLIANCE with an earlier date) needs [`Bypass::Governance`], and
///   removal needs it too.
///
/// The request path validates `requested` first ([`Retention::validate`]); a
/// past date is not re-checked here.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{Bypass, LegalHold, Lock, Mode, Retention, apply_retention};
///
/// let now = SystemTime::UNIX_EPOCH;
/// let current = Lock {
///     retention: Some(Retention {
///         mode: Mode::Governance,
///         retain_until: now + Duration::from_secs(100),
///     }),
///     legal_hold: LegalHold::Off,
/// };
/// let later = Retention {
///     mode: Mode::Governance,
///     retain_until: now + Duration::from_secs(200),
/// };
/// assert!(apply_retention(Some(&current), Some(&later), Bypass::None, now).is_ok());
///
/// let sooner = Retention {
///     mode: Mode::Governance,
///     retain_until: now + Duration::from_secs(50),
/// };
/// assert!(apply_retention(Some(&current), Some(&sooner), Bypass::None, now).is_err());
/// ```
pub fn apply_retention(
    current: Option<&Lock>,
    requested: Option<&Retention>,
    bypass: Bypass,
    now: SystemTime,
) -> Result<Option<Lock>, RefusalReason> {
    let mut lock = current.cloned().unwrap_or_default();
    let active = lock
        .retention
        .as_ref()
        .filter(|retention| in_force(retention, now))
        .copied();
    let Some(active) = active else {
        lock.retention = requested.copied();
        return Ok(wrap(lock));
    };
    match requested {
        None => match active.mode {
            Mode::Compliance => Err(RefusalReason::Compliance),
            Mode::Governance if bypass == Bypass::Governance => {
                lock.retention = None;
                Ok(wrap(lock))
            }
            Mode::Governance => Err(RefusalReason::Governance),
        },
        Some(requested) => {
            let shortens = requested.retain_until < active.retain_until;
            match active.mode {
                Mode::Compliance if shortens || requested.mode == Mode::Governance => {
                    return Err(RefusalReason::Compliance);
                }
                Mode::Governance if shortens && bypass != Bypass::Governance => {
                    return Err(RefusalReason::Governance);
                }
                _ => {}
            }
            lock.retention = Some(*requested);
            Ok(Some(lock))
        }
    }
}

/// Apply a legal-hold change to `current`, returning the lock to store — `None` when the result
/// carries no fact, which is the stores' "there is no row" ([`Lock::carries_fact`], OL5).
///
/// The hold is independent of the retention: the current retention is carried through
/// untouched, and a lock with neither a retention nor an `ON` hold is not a row at all.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{LegalHold, Lock, Mode, Retention, apply_legal_hold};
///
/// let now = SystemTime::UNIX_EPOCH;
/// let locked = Lock {
///     retention: Some(Retention {
///         mode: Mode::Compliance,
///         retain_until: now + Duration::from_secs(100),
///     }),
///     legal_hold: LegalHold::Off,
/// };
/// let held = apply_legal_hold(Some(&locked), LegalHold::On).expect("a hold is a fact");
/// assert_eq!(held.legal_hold, LegalHold::On);
/// assert_eq!(held.retention, locked.retention);
///
/// // Clearing the only fact leaves no row.
/// assert!(apply_legal_hold(Some(&held), LegalHold::Off).is_some());
/// let hold_only = Lock {
///     retention: None,
///     legal_hold: LegalHold::On,
/// };
/// assert!(apply_legal_hold(Some(&hold_only), LegalHold::Off).is_none());
/// ```
pub fn apply_legal_hold(current: Option<&Lock>, requested: LegalHold) -> Option<Lock> {
    let mut lock = current.cloned().unwrap_or_default();
    lock.legal_hold = requested;
    wrap(lock)
}

/// Apply one combined update to `current`: the retention member and the legal
/// hold member, in that order, as one decision.
///
/// `retention` is the plane's three-state member — `None` leaves the retention
/// alone, `Some(None)` clears it and `Some(Some(retention))` sets it — and
/// `legal_hold` is its two-state member (`None` leaves the hold alone, a value
/// sets it). A refusal from the retention arm answers immediately, so the hold
/// is never applied on top of a refused retention. The result is the lock the
/// store must keep, or `None` when it carries no fact.
///
/// # Examples
///
/// ```
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{
///     Bypass, LegalHold, Lock, Mode, RefusalReason, Retention, apply_lock,
/// };
///
/// let now = SystemTime::UNIX_EPOCH;
/// let compliance = Retention {
///     mode: Mode::Compliance,
///     retain_until: now + Duration::from_secs(100),
/// };
/// let current = Lock {
///     retention: Some(compliance),
///     legal_hold: LegalHold::Off,
/// };
/// let sooner = Retention {
///     mode: Mode::Compliance,
///     retain_until: now + Duration::from_secs(50),
/// };
/// let refused = apply_lock(
///     Some(&current),
///     Some(Some(&sooner)),
///     Some(LegalHold::On),
///     Bypass::Governance,
///     now,
/// );
/// assert_eq!(refused, Err(RefusalReason::Compliance));
///
/// let held = apply_lock(Some(&current), None, Some(LegalHold::On), Bypass::None, now)
///     .expect("leaving the retention alone");
/// assert_eq!(held.expect("the hold is a fact").legal_hold, LegalHold::On);
/// ```
pub fn apply_lock(
    current: Option<&Lock>,
    retention: Option<Option<&Retention>>,
    legal_hold: Option<LegalHold>,
    bypass: Bypass,
    now: SystemTime,
) -> Result<Option<Lock>, RefusalReason> {
    let after_retention = match retention {
        Some(requested) => apply_retention(current, requested, bypass, now)?,
        None => current.cloned().and_then(wrap),
    };
    Ok(match legal_hold {
        Some(hold) => apply_legal_hold(after_retention.as_ref(), hold),
        None => after_retention.and_then(wrap),
    })
}

/// Whether a permanent delete of this version may proceed (OL8, OL11).
///
/// A legal hold refuses in either mode and ignores the bypass. An unexpired
/// COMPLIANCE retention refuses and ignores the bypass. An unexpired
/// GOVERNANCE retention refuses unless the request carries
/// [`Bypass::Governance`]. An expired retention is inert.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{
///     Bypass, LegalHold, Lock, Mode, RefusalReason, Retention, may_delete,
/// };
///
/// let now = SystemTime::UNIX_EPOCH;
/// let locked = Lock {
///     retention: Some(Retention {
///         mode: Mode::Compliance,
///         retain_until: now + Duration::from_secs(10),
///     }),
///     legal_hold: LegalHold::Off,
/// };
/// let refusal =
///     may_delete(Some(&locked), now, Bypass::Governance).expect_err("compliance has no bypass");
/// assert_eq!(refusal, RefusalReason::Compliance);
/// ```
pub fn may_delete(
    lock: Option<&Lock>,
    now: SystemTime,
    bypass: Bypass,
) -> Result<(), RefusalReason> {
    let Some(lock) = lock else {
        return Ok(());
    };
    if lock.legal_hold == LegalHold::On {
        return Err(RefusalReason::LegalHold);
    }
    match lock
        .retention
        .as_ref()
        .filter(|retention| in_force(retention, now))
    {
        None => Ok(()),
        Some(retention) if retention.mode == Mode::Compliance => Err(RefusalReason::Compliance),
        Some(_) if bypass == Bypass::Governance => Ok(()),
        Some(_) => Err(RefusalReason::Governance),
    }
}

/// The refusal a delete would meet, or `None` when the delete may proceed.
///
/// One spelling of [`may_delete`] for every store: the store loads the lock
/// inside its own transaction and maps this answer onto its error type.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{
///     Bypass, LegalHold, Lock, Mode, RefusalReason, Retention, delete_refusal,
/// };
///
/// let now = SystemTime::UNIX_EPOCH;
/// let locked = Lock {
///     retention: Some(Retention {
///         mode: Mode::Compliance,
///         retain_until: now + Duration::from_secs(10),
///     }),
///     legal_hold: LegalHold::Off,
/// };
/// assert_eq!(
///     delete_refusal(Some(&locked), now, Bypass::Governance),
///     Some(RefusalReason::Compliance)
/// );
/// assert_eq!(delete_refusal(None, now, Bypass::None), None);
/// ```
pub fn delete_refusal(
    lock: Option<&Lock>,
    now: SystemTime,
    bypass: Bypass,
) -> Option<RefusalReason> {
    may_delete(lock, now, bypass).err()
}

/// The instant a default retention ends: `now` plus the rule's period
/// (OL10).
///
/// `Days` is a fixed span of 24-hour days. `Years` is calendar years in UTC,
/// keeping the time of day, with a Feb-29 anchor in a non-leap target year
/// falling back to Feb-28. The rule's bounds are the request path's; a rule
/// that bypassed validation and names an unrepresentable instant falls back to
/// `now` — an immediately inert retention — and logs one warning, so the
/// failure is defined and visible rather than silent (D6).
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::object_lock::{DefaultRetention, Mode, Period, Rule, default_retention_at};
///
/// let rule = Rule {
///     default_retention: DefaultRetention {
///         mode: Mode::Governance,
///         period: Period::Days(10),
///     },
/// };
/// assert_eq!(
///     default_retention_at(&rule, SystemTime::UNIX_EPOCH),
///     SystemTime::UNIX_EPOCH + Duration::from_secs(864_000)
/// );
/// ```
pub fn default_retention_at(rule: &Rule, now: SystemTime) -> SystemTime {
    match rule.default_retention.period {
        Period::Days(days) => {
            let span = Duration::from_secs(u64::from(days) * SECONDS_PER_DAY);
            now.checked_add(span).unwrap_or_else(|| {
                tracing::warn!(
                    days,
                    "object lock default retention exceeds the representable range; \
                     leaving the retention inert at `now`"
                );
                now
            })
        }
        Period::Years(years) => {
            let anchor = OffsetDateTime::from(now);
            let Some(date) = add_years(anchor.date(), years) else {
                tracing::warn!(
                    years,
                    "object lock default retention lands outside the representable \
                     calendar; leaving the retention inert at `now`"
                );
                return now;
            };
            instant_at(date, anchor.time()).unwrap_or_else(|| {
                tracing::warn!(
                    years,
                    "object lock default retention is not a representable instant; \
                     leaving the retention inert at `now`"
                );
                now
            })
        }
    }
}

/// Add calendar years to a UTC date, falling back from Feb 29 to Feb 28 when
/// the target year is not a leap year; `None` when the target year is outside
/// [`Date`]'s range.
fn add_years(date: Date, years: u32) -> Option<Date> {
    let year = date
        .year()
        .saturating_add(i32::try_from(years).unwrap_or(i32::MAX));
    if let Ok(moved) = date.replace_year(year) {
        return Some(moved);
    }
    // The one in-range refusal is a Feb-29 anchor in a non-leap target year:
    // the documented Feb-28 fallback. An out-of-range year has no date.
    (date.month() == Month::February && date.day() == 29)
        .then(|| Date::from_calendar_date(year, Month::February, 28).ok())
        .flatten()
}

/// The instant a UTC date and time name, or `None` when the platform cannot
/// represent it.
fn instant_at(date: Date, time: Time) -> Option<SystemTime> {
    let offset = PrimitiveDateTime::new(date, time).assume_utc() - OffsetDateTime::UNIX_EPOCH;
    let magnitude = offset.unsigned_abs();
    if offset.is_negative() {
        SystemTime::UNIX_EPOCH.checked_sub(magnitude)
    } else {
        SystemTime::UNIX_EPOCH.checked_add(magnitude)
    }
}

/// Decode the stored Unix nanoseconds of a retain-until column.
///
/// Every `i64` of nanoseconds is representable — the span is roughly 1678 to
/// 2262, inside [`SystemTime`]'s range on every supported platform — so the
/// column type alone rules out an unreadable instant.
fn instant_from_nanos(nanos: i64) -> SystemTime {
    let duration = Duration::from_nanos(nanos.unsigned_abs());
    if nanos >= 0 {
        SystemTime::UNIX_EPOCH + duration
    } else {
        SystemTime::UNIX_EPOCH - duration
    }
}

/// Encode one retain-until instant as the stored Unix nanoseconds, clamped to `i64`.
///
/// The column cannot name an instant outside roughly 1678–2262, so a farther one is clamped to
/// the nearer of [`i64::MIN`] and [`i64::MAX`] rather than refused.
fn nanos_since_epoch(instant: SystemTime) -> i64 {
    match instant.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(ahead) => i64::try_from(ahead.as_nanos()).unwrap_or(i64::MAX),
        Err(err) => i64::try_from(err.duration().as_nanos()).map_or(i64::MIN, |nanos| -nanos),
    }
}

/// Whether a retention protects the version at `now`: an expired retention is
/// inert, so the instant decides, never the mode.
fn in_force(retention: &Retention, now: SystemTime) -> bool {
    retention.retain_until > now
}

/// The one home of "a lock with no retention and no hold is not a lock".
fn wrap(lock: Lock) -> Option<Lock> {
    lock.carries_fact().then_some(lock)
}

const fn corrupt_row(reason: RefusalReason, detail: &'static str) -> CorruptRow {
    CorruptRow { reason, detail }
}

/// Parse the rule wire; `None` for anything not exactly one valid rule.
///
/// The count is read leniently (`u32::parse` accepts `+1` and `01`), so a
/// non-canonical spelling still decodes; re-encoding it produces the canonical
/// wire. Every other field is exact.
fn parse_rule(wire: &str) -> Option<Rule> {
    let mut fields = wire.split(',');
    let mode = fields.next()?.parse::<Mode>().ok()?;
    let kind = fields.next()?;
    let count = fields.next()?.parse::<u32>().ok()?;
    if fields.next().is_some() {
        return None;
    }
    let period = match (kind, count) {
        ("DAYS", days) if (1..=MAX_DAYS).contains(&days) => Period::Days(days),
        ("YEARS", years) if (1..=MAX_YEARS).contains(&years) => Period::Years(years),
        _ => return None,
    };
    Some(Rule {
        default_retention: DefaultRetention { mode, period },
    })
}

/// A [`SystemTime`] as an RFC3339 instant in UTC — the plane's only spelling.
///
/// The encoder formats through [`OffsetDateTime`], which omits a zero fractional part, so a
/// whole-second instant is `2030-01-01T00:00:00Z`. The decoder accepts any RFC3339 spelling
/// `time` reads, including a fractional part and a numeric offset.
mod rfc3339 {
    use std::time::SystemTime;

    use serde::{Deserialize, Deserializer, Serializer, de::Error as _, ser::Error as _};
    use time::{OffsetDateTime, format_description::well_known::Rfc3339};

    pub(super) fn serialize<S: Serializer>(
        instant: &SystemTime,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let at = OffsetDateTime::from(*instant);
        let text = at.format(&Rfc3339).map_err(S::Error::custom)?;
        serializer.serialize_str(&text)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<SystemTime, D::Error> {
        let text = String::deserialize(deserializer)?;
        let at = OffsetDateTime::parse(&text, &Rfc3339).map_err(D::Error::custom)?;
        Ok(SystemTime::from(at))
    }
}
