//! The lifecycle section (`[lifecycle]`) and its seed (FR-042; design LC13, LC22).
//!
//! **Pacing, never a gate** (LC22): an absent section means the pass's own defaults, not "off" —
//! the switch that turns lifecycle off is `[s3] lifecycle` (LC23), read by the serving half. The
//! keys are Minio-shaped like `[scanner]`'s: two durations a pass paces itself by.
//!
//! **The rules are a seed, not an authority** (LC13): each one is upserted by `(bucket, id)` into
//! its bucket's stored configuration at a run's first pass, and a rule removed from the file
//! deletes nothing, because the file cannot tell that apart from a rule a client added.

use std::time::Duration;

use garde::Validate;
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use super::nonzero_duration;
use crate::_core::{
    bucket, lifecycle,
    lifecycle::{DEFAULT_CYCLE, DEFAULT_DELAY},
};
#[cfg(feature = "lifecycle")]
use crate::{
    _lifecycle::{Options, Seed},
    resolve::Resolution,
};

/// A day count's ceiling in the model's carrier: `Expiration.Days` and its siblings are `i32`, so
/// a week, a year or the parser's own floor are what the file spells and this is what it refuses.
const MAX_DAYS: u32 = i32::MAX as u32;

/// `concurrency`: one transition at a time, the gentlest walk over a store the server is also
/// serving (the runner's own default).
const DEFAULT_CONCURRENCY: usize = 1;

/// The pass's pacing and the declarative seed (`[lifecycle]`; the section is optional).
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_cli::config::lifecycle::Config;
///
/// let defaults = Config::default();
/// assert_eq!(defaults.delay, Duration::from_secs(10));
/// assert_eq!(defaults.cycle, Duration::from_secs(3600));
/// assert_eq!(defaults.concurrency, 1);
/// assert!(defaults.rules.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault, Serialize, Deserialize, Validate)]
#[garde(custom(validate_rules))]
#[serde(default)]
pub struct Config {
    /// Pause after each action inside a pass, so a sweep never saturates the store the server is
    /// serving.
    #[garde(skip)]
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_DELAY")]
    pub delay: Duration,
    /// Wait between the end of one pass and the start of the next. Floored at 1s by the parser
    /// (`config::nonzero_duration`): zero would run pass after pass back to back instead of
    /// pacing the sweep.
    #[garde(custom(nonzero_duration))]
    #[serde(with = "humantime_serde")]
    #[default(_code = "DEFAULT_CYCLE")]
    pub cycle: Duration,
    /// How many transitions one pass runs at once. Zero would mean a pass that can only skip
    /// the action, so it is refused where the document is parsed rather than read as one.
    #[garde(range(min = 1))]
    #[default(_code = "DEFAULT_CONCURRENCY")]
    pub concurrency: usize,
    /// The rules the file declares (`[[lifecycle.rules]]`, repeatable). Last, so the serializer
    /// emits them after the two scalar keys — TOML's own ordering rule.
    #[garde(skip)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,
}

/// One rule of the seed: the merge key, the filter prefix, and whichever actions the fields name.
///
/// **`bucket` and `id` are required**: `id` is the merge key, so an id-less rule is a parse error,
/// and a rule naming no bucket cannot be written anywhere.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::config::lifecycle::Rule;
///
/// let rule: Rule = toml::from_str(
///     "bucket = \"data\"\nid = \"expire-logs\"\nprefix = \"logs/\"\nexpire_days = 30",
/// )
/// .expect("a rule");
/// assert_eq!(rule.id, "expire-logs");
/// assert_eq!(rule.expire_days, Some(30));
/// assert_eq!(rule.noncurrent_expire_days, None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    /// The bucket the rule is written to.
    pub bucket: String,
    /// The rule's ID: the merge key, so it is required.
    pub id: String,
    /// The rule's filter prefix (`Filter.Prefix`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// `Expiration.Days`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expire_days: Option<u32>,
    /// `NoncurrentVersionExpiration.NoncurrentDays`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub noncurrent_expire_days: Option<u32>,
    /// `AbortIncompleteMultipartUpload.DaysAfterInitiation`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_days: Option<u32>,
}

impl Rule {
    /// This file rule as the model's, carrying only the actions its fields name.
    ///
    /// The filter is built empty when `prefix` is absent, which the model reads as every object
    /// in the bucket — the same reading its own `Filter` default has.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_cli::config::lifecycle::Rule;
    ///
    /// let rule: Rule =
    ///     toml::from_str("bucket = \"data\"\nid = \"r\"\nabort_incomplete_days = 3").expect("a rule");
    /// let model = rule.rule();
    /// assert!(model.expiration.is_none(), "no expire_days, no expiration");
    /// assert_eq!(
    ///     model
    ///         .abort_incomplete_multipart_upload
    ///         .map(|abort| abort.days_after_initiation),
    ///     Some(3)
    /// );
    /// ```
    pub fn rule(&self) -> lifecycle::Rule {
        lifecycle::Rule::days(
            Some(self.id.clone()),
            self.prefix.clone(),
            self.expire_days.map(day_count),
            self.noncurrent_expire_days.map(day_count),
            self.abort_incomplete_days.map(day_count),
        )
    }
}

/// A validated day count in the model's carrier. [`validate_rules`] caps it at [`MAX_DAYS`], so
/// the saturation is unreachable — it is what keeps the conversion total without a panic.
fn day_count(days: u32) -> i32 {
    i32::try_from(days).unwrap_or(i32::MAX)
}

/// The seed's rules, checked where a document is parsed rather than where it is applied: the
/// bucket name is the storage contract's own (`Seed.bucket` is a `bucket::Name`), and a rule must
/// name at least one action with a day count the model's `1..=i32::MAX` carrier accepts — the seed
/// writes through the storage contract, which does not run the request path's validation (LC8).
fn validate_rules(config: &Config, _context: &()) -> garde::Result {
    for (index, rule) in config.rules.iter().enumerate() {
        if let Err(error) = bucket::name(rule.bucket.clone()) {
            return Err(garde::Error::new(format!(
                "rules[{index}].bucket: `{}` is not a bucket name ({error})",
                rule.bucket
            )));
        }
        let actions = [
            ("expire_days", rule.expire_days),
            ("noncurrent_expire_days", rule.noncurrent_expire_days),
            ("abort_incomplete_days", rule.abort_incomplete_days),
        ];
        if actions.iter().all(|(_, days)| days.is_none()) {
            return Err(garde::Error::new(format!(
                "rules[{index}]: names no action — one of expire_days, noncurrent_expire_days, abort_incomplete_days is required"
            )));
        }
        for (key, days) in actions {
            if let Some(days) = days.filter(|days| !(1..=MAX_DAYS).contains(days)) {
                return Err(garde::Error::new(format!(
                    "rules[{index}].{key}: {days} is outside 1..={MAX_DAYS}"
                )));
            }
        }
    }
    Ok(())
}

/// The pass's options and the declared seed, from the resolved document (LC13, LC22).
///
/// **The section's absence is the defaults, never "off"**: whether the pass runs at all is
/// `[s3] lifecycle`, and that gate is the serving half's (LC23), not this mapping's.
///
/// A rule whose bucket name this build rejects is skipped with a warning rather than panicking on
/// the serving path. [`Config::parse`](crate::config::Config::parse) refuses the name first, so
/// this guard covers only a document that was built in code rather than parsed.
///
/// **Crate-visible, not public API**: the serving half is its only caller, and the facade
/// re-exports the section, not its mapping.
#[cfg(feature = "lifecycle")]
pub(crate) fn options(resolution: &Resolution) -> (Options, Vec<Seed>) {
    let defaults = Config::default();
    let section = resolution.config.lifecycle.as_ref().unwrap_or(&defaults);
    let options = Options {
        delay: section.delay,
        cycle: section.cycle,
        concurrency: section.concurrency,
    };
    let seeds = section
        .rules
        .iter()
        .filter_map(|rule| match bucket::name(rule.bucket.clone()) {
            Ok(bucket) => Some(Seed {
                bucket,
                rule: rule.rule(),
            }),
            Err(error) => {
                tracing::warn!(
                    bucket = %rule.bucket,
                    %error,
                    "a [lifecycle] rule names a bucket this build rejects; skipping it"
                );
                None
            }
        })
        .collect();
    (options, seeds)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "lifecycle")]
    use std::path::PathBuf;

    #[cfg(feature = "lifecycle")]
    use super::*;
    use crate::config::{Config as Root, Error};

    /// A resolution over one parsed document — the conversion's whole input, and a fixture only
    /// the `lifecycle`-featured tests need (`options` is the feature's).
    #[cfg(feature = "lifecycle")]
    fn resolution(text: &str) -> Resolution {
        Resolution {
            pg_root: None,
            root: PathBuf::from("/srv/data"),
            config: Root::parse(text).expect("config"),
            identity: Default::default(),
            config_text: String::new(),
            config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
            read_only: false,
            state_dir: PathBuf::from("/srv/data/.tinio"),
            state_dir_override: None,
            config_error: None,
        }
    }

    /// A root document carrying one seed rule through toml, wrapped so each test states only the
    /// section it is about.
    fn seeded(section: &str) -> String {
        format!("version = 1\n{section}")
    }

    /// LC22's defaults, as the section's own values and as an absent section's: the pass paces
    /// itself by them either way, because the absence of `[lifecycle]` is not "off".
    #[cfg(feature = "lifecycle")]
    #[test]
    fn the_section_defaults_to_ten_seconds_and_one_hour() {
        let defaults = Config::default();
        assert_eq!(defaults.delay, Duration::from_secs(10));
        assert_eq!(defaults.cycle, Duration::from_secs(3600));
        assert!(defaults.rules.is_empty());

        let (pass, seeds) = options(&resolution("version = 1\n"));
        assert_eq!(pass.delay, defaults.delay);
        assert_eq!(pass.cycle, defaults.cycle);
        assert!(seeds.is_empty(), "no section, no seed");

        let (configured, _) = options(&resolution(&seeded(
            "[lifecycle]\ndelay = \"2s\"\ncycle = \"30m\"\n",
        )));
        assert_eq!(configured.delay, Duration::from_secs(2));
        assert_eq!(configured.cycle, Duration::from_secs(30 * 60));
    }

    /// LC22's cadence is paced, not gated: a zero cycle would run pass after pass back to back,
    /// so the parser floors it at one second — while `delay`'s zero stays a legal pacing choice
    /// (`config::nonzero_duration`).
    #[test]
    fn a_zero_cycle_is_a_config_error() {
        let error = Root::parse(&seeded("[lifecycle]\ncycle = \"0s\"\n"))
            .expect_err("a zero cycle is refused");
        assert!(matches!(error, Error::InvalidValue { .. }), "{error}");
        assert!(error.to_string().contains("lifecycle.cycle"), "{error}");

        let parsed = Root::parse(&seeded("[lifecycle]\ndelay = \"0s\"\ncycle = \"1s\"\n"))
            .expect("the floor parses, and a zero delay stays legal");
        assert_eq!(parsed.lifecycle.as_ref().unwrap().cycle.as_secs(), 1);
    }

    /// The pass's worker count is the operator's: one by default, a written value replaces it,
    /// and zero is refused at parse because a pass with no worker could only skip the action.
    #[test]
    fn the_concurrency_defaults_to_one_and_rejects_zero() {
        assert_eq!(super::Config::default().concurrency, 1);

        let parsed = Root::parse(&seeded("[lifecycle]\nconcurrency = 4\n")).expect("config");
        assert_eq!(parsed.lifecycle.as_ref().unwrap().concurrency, 4);

        let error = Root::parse(&seeded("[lifecycle]\nconcurrency = 0\n"))
            .expect_err("a zero worker count is refused");
        assert!(matches!(error, Error::InvalidValue { .. }), "{error}");
        assert!(
            error.to_string().contains("lifecycle.concurrency"),
            "{error}"
        );
    }

    /// The wiring: the value the document carries is what the runner's options receive.
    #[cfg(feature = "lifecycle")]
    #[test]
    fn the_concurrency_reaches_the_pass_options() {
        let (configured, _) = options(&resolution(&seeded("[lifecycle]\nconcurrency = 3\n")));
        assert_eq!(configured.concurrency, 3);

        let (defaults, _) = options(&resolution("version = 1\n"));
        assert_eq!(defaults.concurrency, 1, "no section, the default worker");
    }

    /// LC13: `id` is the merge key, so a file rule without one is a configuration error rather
    /// than a rule that cannot be merged.
    #[test]
    fn an_idless_file_rule_is_a_config_error() {
        let error = Root::parse(&seeded(
            "[[lifecycle.rules]]\nbucket = \"data\"\nexpire_days = 1\n",
        ))
        .expect_err("an id-less rule is refused");
        assert!(matches!(error, Error::Parse { .. }), "{error}");
        assert!(error.to_string().contains("id"), "{error}");
    }

    /// The seed writes through the storage contract, whose own bucket type is `bucket::Name`, so
    /// a name that type rejects is refused where the document is parsed.
    #[test]
    fn an_invalid_bucket_name_is_a_config_error() {
        let error = Root::parse(&seeded(
            "[[lifecycle.rules]]\nbucket = \"Not_A_Bucket\"\nid = \"r\"\nexpire_days = 1\n",
        ))
        .expect_err("an invalid bucket name is refused");
        assert!(matches!(error, Error::InvalidValue { .. }), "{error}");
        assert!(error.to_string().contains("rules[0].bucket"), "{error}");
    }

    /// A rule that names no action, and a day count the model's carrier cannot hold, are the same
    /// class: the seed writes without the request path's validation, so the file is where both are
    /// refused (LC8).
    #[test]
    fn a_rule_with_no_action_or_an_impossible_day_count_is_a_config_error() {
        let no_action = Root::parse(&seeded(
            "[[lifecycle.rules]]\nbucket = \"data\"\nid = \"r\"\n",
        ))
        .expect_err("a rule naming no action is refused");
        assert!(
            no_action.to_string().contains("names no action"),
            "{no_action}"
        );

        let zero = Root::parse(&seeded(
            "[[lifecycle.rules]]\nbucket = \"data\"\nid = \"r\"\nexpire_days = 0\n",
        ))
        .expect_err("a zero-day action is refused");
        assert!(zero.to_string().contains("rules[0].expire_days"), "{zero}");
    }

    /// The conversion is per rule and per field: each declared action reaches the model's own
    /// member, an undeclared one stays absent, and the bucket travels as the contract's name.
    #[cfg(feature = "lifecycle")]
    #[test]
    fn the_seeds_convert_per_bucket() {
        let (_, seeds) = options(&resolution(&seeded(
            "[lifecycle]\n\
             [[lifecycle.rules]]\n\
             bucket = \"logs\"\n\
             id = \"expire-logs\"\n\
             prefix = \"logs/\"\n\
             expire_days = 30\n\
             noncurrent_expire_days = 7\n\
             [[lifecycle.rules]]\n\
             bucket = \"logs\"\n\
             id = \"abort-uploads\"\n\
             abort_incomplete_days = 3\n\
             [[lifecycle.rules]]\n\
             bucket = \"media\"\n\
             id = \"expire-media\"\n\
             expire_days = 1\n",
        )));
        let named: Vec<(String, &str)> = seeds
            .iter()
            .map(|seed| {
                (
                    seed.bucket.to_string(),
                    seed.rule.id.as_deref().unwrap_or(""),
                )
            })
            .collect();
        assert_eq!(
            named,
            [
                ("logs".to_owned(), "expire-logs"),
                ("logs".to_owned(), "abort-uploads"),
                ("media".to_owned(), "expire-media"),
            ],
            "one seed per file rule, in the file's order, named by its bucket"
        );

        let logs = &seeds[0].rule;
        assert_eq!(logs.status, lifecycle::Status::Enabled);
        assert_eq!(logs.filter.prefix.as_deref(), Some("logs/"));
        assert_eq!(
            logs.expiration.as_ref().and_then(|days| days.days),
            Some(30)
        );
        assert_eq!(
            logs.noncurrent_version_expiration
                .as_ref()
                .and_then(|days| days.noncurrent_days),
            Some(7)
        );
        assert!(
            logs.abort_incomplete_multipart_upload.is_none(),
            "a rule declares only the actions its fields name"
        );

        let abort = &seeds[1].rule;
        assert!(abort.filter.prefix.is_none(), "no prefix is every object");
        assert!(abort.expiration.is_none());
        assert!(abort.noncurrent_version_expiration.is_none());
        assert_eq!(
            abort
                .abort_incomplete_multipart_upload
                .as_ref()
                .map(|abort| abort.days_after_initiation),
            Some(3)
        );
    }
}
