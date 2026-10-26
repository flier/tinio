//! The pure matcher and the day arithmetic (LC10), and which buckets the
//! server-wide knobs still own (LC21).

use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use smart_default::SmartDefault;
use time::{OffsetDateTime, SignedDuration};

use super::model::{And, Config, Expiration, Filter, Rule, Status, Tag, Transition};
use crate::{bucket, object};

/// One object's facts, all of them from the listing the pass already made
/// (LC10): what a rule's filter is matched on, and the instant the day counts
/// start from.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{lifecycle::Facts, object};
///
/// let key = object::key("logs/a.txt").expect("a valid object key");
/// let tags = object::Tags::empty();
/// let facts = Facts {
///     key: &key,
///     size: 10,
///     last_modified: SystemTime::UNIX_EPOCH,
///     tags: &tags,
/// };
/// assert_eq!(facts.size, 10);
/// ```
#[derive(Debug)]
pub struct Facts<'a> {
    pub key: &'a object::Key,
    pub size: u64,
    pub last_modified: SystemTime,
    pub tags: &'a object::Tags,
}

impl<'a> Facts<'a> {
    /// The facts a listing already carries (LC10): no read per object.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{lifecycle::Facts, object};
    ///
    /// let key = object::key("logs/a.txt").expect("a valid object key");
    /// let tags = object::Tags::empty();
    /// let facts = Facts::new(&key, 10, SystemTime::UNIX_EPOCH, &tags);
    /// assert_eq!(facts.size, 10);
    /// ```
    pub fn new(
        key: &'a object::Key,
        size: u64,
        last_modified: SystemTime,
        tags: &'a object::Tags,
    ) -> Self {
        Self {
            key,
            size,
            last_modified,
            tags,
        }
    }
}

/// One rule's bounds on a key's noncurrent versions: how long they stay, and
/// how many of them — AWS deletes only when **both** of that rule's thresholds
/// are exceeded, which is why a plan is never merged across rules.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::NoncurrentPlan;
///
/// let plan = NoncurrentPlan {
///     days: 30,
///     keep: Some(3),
/// };
/// assert_eq!(plan.keep, Some(3));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoncurrentPlan {
    pub days: u32,
    pub keep: Option<u32>,
}

impl Config {
    /// Every `Enabled` rule whose filter selects this object, in the stored
    /// order (LC10) — lazy and allocation-free, because the pass asks per
    /// object on every cycle.
    ///
    /// `facts` shares `'a` with the rules: the filter is evaluated as the
    /// iterator is consumed, so the borrow has to live as long as the answer.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{
    ///     lifecycle::{Config, Expiration, Facts, Filter, Rule, Status},
    ///     object,
    /// };
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Filter {
    ///             prefix: Some("logs/".into()),
    ///             ..Filter::default()
    ///         },
    ///         expiration: Some(Expiration {
    ///             days: Some(30),
    ///             date: None,
    ///             expired_object_delete_marker: None,
    ///         }),
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// let key = object::key("logs/a.txt").expect("a valid object key");
    /// let tags = object::Tags::empty();
    /// let facts = Facts {
    ///     key: &key,
    ///     size: 0,
    ///     last_modified: SystemTime::UNIX_EPOCH,
    ///     tags: &tags,
    /// };
    /// assert_eq!(config.rules_for(&facts).count(), 1);
    /// ```
    pub fn rules_for<'a>(&'a self, facts: &'a Facts<'_>) -> impl Iterator<Item = &'a Rule> + 'a {
        self.rules
            .iter()
            .filter(move |rule| rule.status == Status::Enabled && rule.filter.matches(facts))
    }

    /// When this object's current version falls due: the **earliest** instant
    /// across the matching rules' `Expiration`s (AWS: "the shorter expiration
    /// policy is honored"), or `None` when none of them expires on a clock.
    ///
    /// A `Date` is absolute — the caller compares it with its own clock — and
    /// `Days` expires at [`midnight_utc`] of `last_modified + days`. A rule
    /// carrying only `ExpiredObjectDeleteMarker` names no instant: it is an
    /// action on a version chain, not a date (LC2).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{
    ///     lifecycle::{Config, Expiration, Facts, Filter, Rule, Status},
    ///     object,
    /// };
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Filter::default(),
    ///         expiration: Some(Expiration {
    ///             days: Some(1),
    ///             date: None,
    ///             expired_object_delete_marker: None,
    ///         }),
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// let key = object::key("a.txt").expect("a valid object key");
    /// let tags = object::Tags::empty();
    /// let facts = Facts {
    ///     key: &key,
    ///     size: 0,
    ///     last_modified: SystemTime::UNIX_EPOCH,
    ///     tags: &tags,
    /// };
    /// assert!(config.expiry_for(&facts).is_some());
    /// ```
    pub fn expiry_for(&self, facts: &Facts<'_>) -> Option<SystemTime> {
        self.rules_for(facts)
            .filter_map(|rule| rule.expiration.as_ref())
            .filter_map(|expiration| {
                instant_from_parts(expiration.date, expiration.days, facts.last_modified)
            })
            .min()
    }

    /// One plan per matching `Enabled` rule that carries
    /// `NoncurrentVersionExpiration` — the rule's own bounds, lazily, never a
    /// merge across rules (LC10).
    ///
    /// AWS evaluates both thresholds per rule and acts when any rule's pair is
    /// exceeded, so the caller asks the union: "does any plan let this version
    /// go?". A merged pair cannot answer that. Take A `{10 days, keep 5}` and B
    /// `{30 days, keep 2}` matching one object: a version 20 days old with 3
    /// newer versions is short of A's keep count and short of B's age, so both
    /// rules retain it — while the merged `{10, 2}` would delete it.
    ///
    /// `days` is the rule's `NoncurrentDays`; MinIO's count-only extension
    /// (accepted per LC10) means `0`. `keep` is its `NewerNoncurrentVersions`,
    /// `None` when the rule names none.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{
    ///     lifecycle::{Config, Facts, NoncurrentVersionExpiration, Rule, Status},
    ///     object,
    /// };
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: None,
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
    ///             noncurrent_days: Some(30),
    ///             newer_noncurrent_versions: Some(3),
    ///         }),
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// let key = object::key("a.txt").expect("a valid object key");
    /// let tags = object::Tags::empty();
    /// let facts = Facts {
    ///     key: &key,
    ///     size: 0,
    ///     last_modified: SystemTime::UNIX_EPOCH,
    ///     tags: &tags,
    /// };
    /// let plan = config.noncurrent_plans(&facts).next().expect("one plan");
    /// assert_eq!((plan.days, plan.keep), (30, Some(3)));
    /// ```
    pub fn noncurrent_plans<'a>(
        &'a self,
        facts: &'a Facts<'_>,
    ) -> impl Iterator<Item = NoncurrentPlan> + 'a {
        self.rules_for(facts)
            .filter_map(|rule| rule.noncurrent_version_expiration.as_ref())
            .map(|noncurrent| NoncurrentPlan {
                days: noncurrent.noncurrent_days.map_or(0, count),
                keep: noncurrent.newer_noncurrent_versions.map(count),
            })
    }

    /// The smallest `DaysAfterInitiation` among the matching rules, or `None`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{
    ///     lifecycle::{AbortIncompleteMultipartUpload, Config, Facts, Rule, Status},
    ///     object,
    /// };
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: None,
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
    ///             days_after_initiation: 7,
    ///         }),
    ///     }],
    /// };
    /// let key = object::key("a.txt").expect("a valid object key");
    /// let tags = object::Tags::empty();
    /// let facts = Facts {
    ///     key: &key,
    ///     size: 0,
    ///     last_modified: SystemTime::UNIX_EPOCH,
    ///     tags: &tags,
    /// };
    /// assert_eq!(config.abort_after(&facts), Some(7));
    /// ```
    pub fn abort_after(&self, facts: &Facts<'_>) -> Option<u32> {
        self.rules_for(facts)
            .filter_map(|rule| rule.abort_incomplete_multipart_upload.as_ref())
            .map(|abort| count(abort.days_after_initiation))
            .min()
    }

    /// Whether any `Enabled` rule carries `NoncurrentVersionExpiration` — the
    /// question a bucket's server-wide retention thresholds ask (LC21): a bucket
    /// this answers `true` for is governed by its own rules for that action, so
    /// `[s3] noncurrent_keep` / `noncurrent_expire_days` do not apply to it.
    ///
    /// No [`Facts`] and no filter: the threshold a rule displaces is
    /// bucket-blind, so the whole bucket is governed however narrowly a rule
    /// selects.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, NoncurrentVersionExpiration, Rule, Status};
    ///
    /// fn rule() -> Rule {
    ///     Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: None,
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }
    /// }
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
    ///             noncurrent_days: Some(30),
    ///             newer_noncurrent_versions: None,
    ///         }),
    ///         ..rule()
    ///     }],
    /// };
    /// assert!(config.governs_noncurrent_version_expiration());
    /// assert!(!config.governs_abort_incomplete_multipart_upload());
    /// ```
    pub fn governs_noncurrent_version_expiration(&self) -> bool {
        self.governs(|rule| rule.noncurrent_version_expiration.is_some())
    }

    /// Whether any `Enabled` rule carries `AbortIncompleteMultipartUpload` —
    /// [`governs_noncurrent_version_expiration`](Self::governs_noncurrent_version_expiration)'s
    /// sibling for the abandoned-upload action (LC21).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{AbortIncompleteMultipartUpload, Config, Rule, Status};
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: None,
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
    ///             days_after_initiation: 7,
    ///         }),
    ///     }],
    /// };
    /// assert!(config.governs_abort_incomplete_multipart_upload());
    /// ```
    pub fn governs_abort_incomplete_multipart_upload(&self) -> bool {
        self.governs(|rule| rule.abort_incomplete_multipart_upload.is_some())
    }

    /// Whether any `Enabled` rule carries an `Expiration` that expires an
    /// object on a clock (`Days` or `Date`) — the question the object walk asks
    /// before paying for a listing (LC21).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, Expiration, Rule, Status};
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: Some(Expiration {
    ///             days: Some(30),
    ///             date: None,
    ///             expired_object_delete_marker: None,
    ///         }),
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// assert!(config.governs_expiration());
    /// ```
    pub fn governs_expiration(&self) -> bool {
        self.governs(|rule| {
            rule.expiration
                .as_ref()
                .is_some_and(|expiration| expiration.days.is_some() || expiration.date.is_some())
        })
    }

    /// Whether any `Enabled` rule carries `ExpiredObjectDeleteMarker` — an
    /// action on a version chain (LC2), so the object walk answers it nothing.
    /// `Some(false)` governs nothing: the rule takes no action.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, Expiration, Rule, Status};
    ///
    /// let config = Config {
    ///     rules: vec![Rule {
    ///         id: None,
    ///         status: Status::Enabled,
    ///         filter: Default::default(),
    ///         expiration: Some(Expiration {
    ///             days: None,
    ///             date: None,
    ///             expired_object_delete_marker: Some(true),
    ///         }),
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     }],
    /// };
    /// assert!(config.governs_expired_object_delete_marker());
    /// ```
    pub fn governs_expired_object_delete_marker(&self) -> bool {
        self.governs(|rule| {
            rule.expiration
                .as_ref()
                .is_some_and(Expiration::removes_delete_marker)
        })
    }

    /// Whether any `Enabled` rule passes `pred` — the one reading of the
    /// `Enabled` gate every `governs_*` question uses.
    fn governs(&self, pred: impl Fn(&Rule) -> bool) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.status == Status::Enabled && pred(rule))
    }
}

/// `anchor + days`, rounded forward to the next midnight UTC (LC10; AWS's own
/// example: 2014-01-15T10:30Z plus 3 days is 2014-01-19T00:00Z).
///
/// Always strictly forward, so an anchor that is already midnight still lands on
/// the following one. MinIO's `ExpectedExpiryTime` truncates after `days + 1`
/// the same way but short-circuits `days == 0` to the modification time; this
/// does not, and no caller needs it to: zero is not a valid `Expiration.Days`
/// (`validate` floors it at 1), and the count-only noncurrent form expresses
/// "immediate" as an age of `0` days compared by the caller, never as an expiry
/// instant through this function.
///
/// # Examples
///
/// ```rust
/// use std::time::{Duration, SystemTime};
///
/// use tinio_core::lifecycle::midnight_utc;
///
/// // 1970-01-01T12:00Z plus one day rounds to 1970-01-03T00:00Z.
/// let noon = SystemTime::UNIX_EPOCH + Duration::from_secs(43_200);
/// assert_eq!(
///     midnight_utc(noon, 1),
///     SystemTime::UNIX_EPOCH + Duration::from_secs(172_800)
/// );
/// ```
pub fn midnight_utc(anchor: SystemTime, days: u32) -> SystemTime {
    let day = OffsetDateTime::from(anchor)
        .date()
        .saturating_add(SignedDuration::days(i64::from(days) + 1));
    SystemTime::from(day.midnight().assume_utc())
}

impl Filter {
    /// Whether this filter selects the object: every predicate it carries, and
    /// an empty filter selects every object.
    fn matches(&self, facts: &Facts<'_>) -> bool {
        prefix_matches(facts.key, self.prefix.as_deref())
            && self.tag.as_ref().is_none_or(|tag| tag.matches(facts.tags))
            && size_bounds(facts.size, self.size_greater_than, self.size_less_than)
            && self.and.as_ref().is_none_or(|and| and.matches(facts))
    }
}

impl And {
    fn matches(&self, facts: &Facts<'_>) -> bool {
        prefix_matches(facts.key, self.prefix.as_deref())
            && self.tags.iter().all(|tag| tag.matches(facts.tags))
            && size_bounds(facts.size, self.size_greater_than, self.size_less_than)
    }
}

impl Tag {
    /// Exact key and value; an object's other tags do not exclude it.
    fn matches(&self, tags: &object::Tags) -> bool {
        tags.iter()
            .any(|(key, value)| key == self.key && value == self.value)
    }
}

fn prefix_matches(key: &object::Key, prefix: Option<&str>) -> bool {
    prefix.is_none_or(|prefix| key.starts_with(prefix))
}

/// Both bounds are strict, as `<ObjectSizeGreaterThan>` and
/// `<ObjectSizeLessThan>` read.
fn size_bounds(size: u64, greater: Option<u64>, less: Option<u64>) -> bool {
    greater.is_none_or(|bound| size > bound) && less.is_none_or(|bound| size < bound)
}

impl Transition {
    /// The instant this transition names, or `None` when it names none the
    /// platform's clock can hold.
    ///
    /// The same clock an `Expiration` reads: `Days` counts to [`midnight_utc`]
    /// of `last_modified + days`, and `Date` is absolute — so the two actions
    /// can never name different days for the same object.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{Class, lifecycle::Transition};
    ///
    /// let noon = SystemTime::UNIX_EPOCH + Duration::from_secs(43_200);
    /// let after_thirty = Transition {
    ///     days: Some(30),
    ///     date: None,
    ///     storage_class: Class::StandardIa,
    /// };
    /// assert_eq!(
    ///     after_thirty.instant(noon),
    ///     Some(SystemTime::UNIX_EPOCH + Duration::from_secs(31 * 86_400))
    /// );
    /// ```
    pub fn instant(&self, last_modified: SystemTime) -> Option<SystemTime> {
        instant_from_parts(self.date, self.days, last_modified)
    }
}

/// The instant a `Days`/`Date` pair names, the one conversion both a rule's
/// `Expiration` and its `Transition` read.
fn instant_from_parts(
    date: Option<i64>,
    days: Option<i32>,
    last_modified: SystemTime,
) -> Option<SystemTime> {
    if let Some(date) = date {
        return from_unix_seconds(date);
    }
    days.map(|days| midnight_utc(last_modified, count(days)))
}

/// A `Date` is Unix seconds at midnight UTC, so its instant needs no calendar.
///
/// `validate` floors a `Date` only at midnight UTC, so the value can name a year
/// the platform's clock cannot hold (Windows starts in 1601 and stops in 30828).
/// Such a value names **no** instant: checked arithmetic, no panic, and never a
/// fallback to the epoch — a nonsense date must retain the object, never expire
/// it.
pub(in crate::lifecycle) fn from_unix_seconds(seconds: i64) -> Option<SystemTime> {
    let magnitude = Duration::from_secs(seconds.unsigned_abs());
    if seconds < 0 {
        SystemTime::UNIX_EPOCH.checked_sub(magnitude)
    } else {
        SystemTime::UNIX_EPOCH.checked_add(magnitude)
    }
}

/// A validated configuration's counts are `>= 1` (LC8), so this only decides
/// what an unvalidated row means: the largest count, which retains data rather
/// than expiring it on a nonsense value.
fn count(value: i32) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Which buckets the server-wide `[s3]` knobs still own (LC21, LC23).
///
/// Built once per pass from the rules read. A bucket whose stored
/// configuration carries an action is governed by that rule, so the matching
/// knob does not apply to it. A run that builds no lifecycle pass governs
/// nothing. A run that builds the pass and cannot read the rules stands every
/// bucket down: a knob must not beat a rule the pass cannot see.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, lifecycle::ActionOwners};
///
/// let name = bucket::name("data").unwrap();
/// let off = ActionOwners::unread(false);
/// assert!(
///     off.knob_applies_abort(&name),
///     "no pass, the knob still applies"
/// );
/// let unread = ActionOwners::unread(true);
/// assert!(
///     !unread.knob_applies_abort(&name),
///     "a pass that cannot read the rules stands the bucket down"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, SmartDefault)]
pub struct ActionOwners {
    #[default(_code = "Governance::Buckets(HashSet::new())")]
    noncurrent: Governance,
    #[default(_code = "Governance::Buckets(HashSet::new())")]
    abort: Governance,
}

/// One action's governed set.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Governance {
    /// These buckets are governed. Every other bucket keeps the knob.
    Buckets(HashSet<String>),
    /// Every bucket is governed. The knob applies to none.
    All,
}

impl ActionOwners {
    /// Fold one rules read. `pass` is whether this run builds the lifecycle
    /// pass; with none, no bucket is governed.
    pub fn from_configs(configs: &[(bucket::Name, Config)], pass: bool) -> Self {
        if !pass {
            return Self::knobs();
        }
        let mut noncurrent = HashSet::new();
        let mut abort = HashSet::new();
        for (name, config) in configs {
            if config.governs_noncurrent_version_expiration() {
                noncurrent.insert(name.to_string());
            }
            if config.governs_abort_incomplete_multipart_upload() {
                abort.insert(name.to_string());
            }
        }
        Self {
            noncurrent: Governance::Buckets(noncurrent),
            abort: Governance::Buckets(abort),
        }
    }

    /// The rules could not be read. `pass` chooses which failure that is:
    /// a built pass stands every bucket down, and a run with no pass keeps
    /// the knobs.
    pub fn unread(pass: bool) -> Self {
        if pass {
            Self::stood_down()
        } else {
            Self::knobs()
        }
    }

    fn knobs() -> Self {
        Self {
            noncurrent: Governance::Buckets(HashSet::new()),
            abort: Governance::Buckets(HashSet::new()),
        }
    }

    fn stood_down() -> Self {
        Self {
            noncurrent: Governance::All,
            abort: Governance::All,
        }
    }

    /// Whether `[s3] noncurrent_keep` / `noncurrent_expire_days` still apply
    /// to `bucket`.
    pub fn knob_applies_noncurrent(&self, bucket: &bucket::Name) -> bool {
        self.noncurrent.applies(bucket)
    }

    /// Whether `[s3] multipart_expire_days` still applies to `bucket`.
    pub fn knob_applies_abort(&self, bucket: &bucket::Name) -> bool {
        self.abort.applies(bucket)
    }

    /// The bucket names an abort stage excludes from the knob, or `None` when
    /// every bucket stands down and the stage should not run.
    pub fn abort_excluded(&self) -> Option<Vec<String>> {
        self.abort.names()
    }
}

impl Governance {
    fn applies(&self, bucket: &bucket::Name) -> bool {
        match self {
            Self::All => false,
            Self::Buckets(names) => !names.contains(bucket.as_ref()),
        }
    }

    fn names(&self) -> Option<Vec<String>> {
        match self {
            Self::All => None,
            Self::Buckets(names) => Some(names.iter().cloned().collect()),
        }
    }
}
