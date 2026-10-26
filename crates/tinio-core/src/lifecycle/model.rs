//! The stored lifecycle model and its request-path validation (LC3, LC8, LC17).

use std::{collections::HashSet, fmt, time::Duration};

use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

use crate::Class;

/// Maximum rules per configuration (AWS: 1,000, "not adjustable").
pub(in crate::lifecycle) const MAX_RULES: usize = 1000;
/// Maximum rule ID length, in characters (AWS: 255).
pub(in crate::lifecycle) const MAX_RULE_ID: usize = 255;
/// The ceiling a size predicate is bounded by: 50 TB decimal, the object
/// maximum AWS states both as "50 TB" and as 48.828125 TiB.
pub(in crate::lifecycle) const MAX_SIZE: u64 = 50_000_000_000_000;
/// The largest `NewerNoncurrentVersions` AWS accepts.
const MAX_NEWER_NONCURRENT: i32 = 100;
/// The transition floor AWS enforces: an object may not leave `STANDARD`
/// before its thirtieth day (T11).
const MIN_TRANSITION_DAYS: i32 = 30;
/// Seconds in a day: a `Date` is always midnight UTC, so its Unix seconds
/// are a multiple of this. The one home of the value, shared by the model's
/// date check and the pass's age arithmetic.
pub const SECONDS_PER_DAY: u64 = 86_400;

/// The pass's per-object pacing default (LC22): ten seconds. The one home of
/// the value, shared by `tinio-lifecycle`'s `Options` and the CLI's
/// `[lifecycle]` section so the two cannot drift.
pub const DEFAULT_DELAY: Duration = Duration::from_secs(10);
/// The pass's full-pass cadence default (LC22): one hour, the sweeper's own
/// interval. The one home of the value, shared as [`DEFAULT_DELAY`] is.
pub const DEFAULT_CYCLE: Duration = Duration::from_secs(3600);

// The reason each refusal carries, spelled once.
pub(in crate::lifecycle) const TOO_MANY_RULES: &str = "a configuration carries at most 1000 rules";
pub(in crate::lifecycle) const RULE_ID_TOO_LONG: &str = "a rule ID is at most 255 characters";
pub(in crate::lifecycle) const RULE_ID_DUPLICATE: &str =
    "rule IDs are unique within a configuration";
pub(in crate::lifecycle) const FILTER_ARITY: &str =
    "Filter carries exactly one of Prefix, Tag, ObjectSizeGreaterThan, ObjectSizeLessThan or And";
pub(in crate::lifecycle) const AND_ARITY: &str = "And carries two or more predicates";
pub(in crate::lifecycle) const TAG_KEYS_UNIQUE: &str = "tag filter keys are unique";
pub(in crate::lifecycle) const SIZE_ORDER: &str =
    "ObjectSizeGreaterThan is less than ObjectSizeLessThan";
pub(in crate::lifecycle) const SIZE_CEILING: &str =
    "a size predicate is at most the 50 TB object ceiling";
pub(in crate::lifecycle) const NO_ACTION: &str = "a rule carries at least one action";
pub(in crate::lifecycle) const NO_RULES: &str = "a configuration carries at least one rule";
pub(in crate::lifecycle) const DAYS_MIN: &str = "Expiration Days is at least 1";
pub(in crate::lifecycle) const NONCURRENT_DAYS_MIN: &str = "NoncurrentDays is at least 1";
pub(in crate::lifecycle) const ABORT_DAYS_MIN: &str = "DaysAfterInitiation is at least 1";
pub(in crate::lifecycle) const DATE_MIDNIGHT: &str = "Expiration Date is midnight UTC";
pub(in crate::lifecycle) const DAYS_AND_DATE: &str = "Expiration carries Days or Date, never both";
pub(in crate::lifecycle) const DELETE_MARKER_DAYS: &str =
    "ExpiredObjectDeleteMarker excludes Days and Date";
pub(in crate::lifecycle) const TAG_FILTER_ACTION: &str =
    "a tag filter excludes AbortIncompleteMultipartUpload and ExpiredObjectDeleteMarker";
pub(in crate::lifecycle) const NEWER_NEEDS_FILTER: &str = "NewerNoncurrentVersions needs a Filter";
pub(in crate::lifecycle) const NEWER_RANGE: &str = "NewerNoncurrentVersions is between 1 and 100";
pub(in crate::lifecycle) const TRANSITION_CLASS: &str = "Transition StorageClass is STANDARD_IA";
pub(in crate::lifecycle) const TRANSITION_DAYS_MIN: &str = "Transition Days is at least 30";
pub(in crate::lifecycle) const TRANSITION_DAYS_AND_DATE: &str =
    "Transition carries Days or Date, never both";
pub(in crate::lifecycle) const TRANSITION_DATE_MIDNIGHT: &str = "Transition Date is midnight UTC";

/// The reason every write path refuses a transition on a backend without tiers (T11).
///
/// One home for the sentence the S3 surface, the management plane, the CLI and the seed answer
/// with: a stored transition would promise a migration the backend can never run.
pub const TRANSITIONS_NEED_TIERS: &str =
    "storage-class transitions are not supported: this server has no storage tiers";

/// A bucket's lifecycle configuration: the stored row and the management
/// plane's body are this one shape (LC17).
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::{Config, Expiration, Filter, Rule, Status};
///
/// let config = Config {
///     rules: vec![Rule {
///         id: Some("expire-logs".into()),
///         status: Status::Enabled,
///         filter: Filter::default(),
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
/// assert_eq!(config.validate(), Ok(()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub rules: Vec<Rule>,
}

impl Config {
    /// Whether this is a configuration the S3 surface may store (LC8).
    ///
    /// [`InvalidKind::Shape`] is the schema class — a filter that is not
    /// exactly one predicate, an `And` with fewer than two, a size range
    /// outside `And` (`MalformedXML` on the S3 surface);
    /// [`InvalidKind::Semantic`] is everything else (`InvalidRequest`): an
    /// empty configuration, the 1000-rule and 255-character ID limits, ID
    /// uniqueness, at least one action per rule (and at least one member in
    /// each action), `Days ≥ 1` for all three day counts, a `Date` at
    /// midnight UTC, `Days`/`Date`/`ExpiredObjectDeleteMarker` exclusivity,
    /// the actions a tag filter excludes, duplicate tag keys within an `And`,
    /// the size order and ceiling,
    /// `NewerNoncurrentVersions ∈ 1..=100` with a filter to scope it, and a
    /// transition targeting `STANDARD_IA` at `Days ≥ 30` or a midnight
    /// `Date`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, InvalidKind};
    ///
    /// let refused = Config { rules: Vec::new() }
    ///     .validate()
    ///     .expect_err("an empty configuration is refused");
    /// assert_eq!(refused.kind, InvalidKind::Semantic);
    /// ```
    pub fn validate(&self) -> Result<(), Invalid> {
        if self.rules.is_empty() {
            return Err(Invalid {
                rule: None,
                reason: NO_RULES,
                kind: InvalidKind::Semantic,
            });
        }
        if self.rules.len() > MAX_RULES {
            return Err(Invalid {
                rule: None,
                reason: TOO_MANY_RULES,
                kind: InvalidKind::Semantic,
            });
        }
        let mut seen = HashSet::with_capacity(self.rules.len());
        for rule in &self.rules {
            if let Some(id) = &rule.id
                && !seen.insert(id.as_str())
            {
                return Err(rule.refusal(RULE_ID_DUPLICATE, InvalidKind::Semantic));
            }
        }
        self.rules.iter().try_for_each(Rule::validate)
    }

    /// The first rule that asks for a storage-class transition, or `None` (T11's gate).
    ///
    /// One home for "which rule the refusal names": every write path asks this before it stores
    /// a configuration its backend cannot honour, and the answer is the rule whose ID the
    /// refusal carries.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{
    ///     Class,
    ///     lifecycle::{Config, Expiration, Rule, Status, Transition},
    /// };
    ///
    /// let rule = |transitions| Rule {
    ///     id: Some("archive".into()),
    ///     status: Status::Enabled,
    ///     filter: Default::default(),
    ///     expiration: Some(Expiration {
    ///         days: Some(30),
    ///         date: None,
    ///         expired_object_delete_marker: None,
    ///     }),
    ///     transitions,
    ///     noncurrent_version_expiration: None,
    ///     abort_incomplete_multipart_upload: None,
    /// };
    /// let config = Config {
    ///     rules: vec![rule(vec![Transition {
    ///         days: Some(30),
    ///         date: None,
    ///         storage_class: Class::StandardIa,
    ///     }])],
    /// };
    /// assert_eq!(
    ///     config.transition_rule().and_then(|rule| rule.id.as_deref()),
    ///     Some("archive")
    /// );
    /// ```
    pub fn transition_rule(&self) -> Option<&Rule> {
        self.rules.iter().find(|rule| !rule.transitions.is_empty())
    }

    /// Upsert `rule` by its ID: the rule carrying the same ID is replaced in
    /// place, and a rule whose ID is new is appended — LC14's merge key, one
    /// home for the seed and `tinio lifecycle add`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, Expiration, Rule, Status};
    ///
    /// let rule = |id: &str, days: i32| Rule {
    ///     id: Some(id.into()),
    ///     status: Status::Enabled,
    ///     filter: Default::default(),
    ///     expiration: Some(Expiration {
    ///         days: Some(days),
    ///         date: None,
    ///         expired_object_delete_marker: None,
    ///     }),
    ///     transitions: Vec::new(),
    ///     noncurrent_version_expiration: None,
    ///     abort_incomplete_multipart_upload: None,
    /// };
    /// let mut config = Config {
    ///     rules: vec![rule("logs", 30)],
    /// };
    /// config.upsert_rule(rule("logs", 7));
    /// config.upsert_rule(rule("tmp", 1));
    /// assert_eq!(config.rules.len(), 2, "an upsert never adds a duplicate");
    /// assert_eq!(
    ///     config.rules[0]
    ///         .expiration
    ///         .as_ref()
    ///         .and_then(|expiration| expiration.days),
    ///     Some(7),
    ///     "the rule carrying the ID is replaced in place"
    /// );
    /// ```
    pub fn upsert_rule(&mut self, rule: Rule) {
        match self.rules.iter().position(|stored| stored.id == rule.id) {
            Some(index) => self.rules[index] = rule,
            None => self.rules.push(rule),
        }
    }

    /// Upsert `rules` into this configuration and validate the result.
    ///
    /// The seed's merge (LC13, LC14): same id replaces, a new id appends, and
    /// the 1000-rule cap is the merged document's, not the file's. The caller
    /// stores the result only on `Ok`, inside the transaction that read `self`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Config, Expiration, Rule, Status};
    ///
    /// let rule = |id: &str| Rule {
    ///     id: Some(id.into()),
    ///     status: Status::Enabled,
    ///     filter: Default::default(),
    ///     expiration: Some(Expiration {
    ///         days: Some(1),
    ///         date: None,
    ///         expired_object_delete_marker: None,
    ///     }),
    ///     transitions: Vec::new(),
    ///     noncurrent_version_expiration: None,
    ///     abort_incomplete_multipart_upload: None,
    /// };
    /// let stored = Config {
    ///     rules: vec![rule("keep"), rule("replace")],
    /// };
    /// let merged = stored.apply_rules(&[rule("replace")]).unwrap();
    /// assert_eq!(
    ///     merged.rules.len(),
    ///     2,
    ///     "the rule the seed does not name stays"
    /// );
    /// ```
    ///
    /// # Errors
    ///
    /// [`Config::validate`]'s refusal, when the merged document is not one the
    /// request path would store.
    pub fn apply_rules(&self, rules: &[Rule]) -> Result<Config, Invalid> {
        let mut config = self.clone();
        for rule in rules {
            config.upsert_rule(rule.clone());
        }
        config.validate()?;
        Ok(config)
    }

    /// The wire: this configuration as JSON. The row the store keeps and the
    /// management plane's body are this shape, so there is nothing to keep in
    /// step (LC17).
    ///
    /// No member is refused for being unknown, so a field a later binary adds
    /// survives an earlier one's read (LC17, LC8).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::Config;
    ///
    /// let wire = Config { rules: Vec::new() }.to_wire();
    /// assert_eq!(wire, "{\"rules\":[]}");
    /// ```
    #[expect(
        clippy::unwrap_used,
        reason = "infallible: a `Config` is strings, integers and vectors, which `serde_json` always serializes"
    )]
    pub fn to_wire(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    /// Decode the wire: `None` for a value that is not one, after one
    /// warning. A corrupt row is never an error and never a configuration
    /// read as "retain everything" (LC8's self-healing read, the RustFS
    /// post-mortem).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::Config;
    ///
    /// let config = Config::from_wire("{\"rules\":[]}").expect("the wire decodes");
    /// assert!(config.rules.is_empty());
    /// assert_eq!(Config::from_wire("not json"), None);
    /// ```
    pub fn from_wire(wire: &str) -> Option<Config> {
        match serde_json::from_str(wire) {
            Ok(config) => Some(config),
            Err(err) => {
                tracing::warn!(%err, "discarding an unreadable lifecycle configuration");
                None
            }
        }
    }
}

/// A refused configuration: the offending rule's ID when it has one, the
/// reason, and the class the S3 surface maps to a code.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::{Invalid, InvalidKind};
///
/// let refused = Invalid {
///     rule: Some("logs".into()),
///     reason: "a rule carries at least one action",
///     kind: InvalidKind::Semantic,
/// };
/// assert_eq!(
///     refused.to_string(),
///     "lifecycle rule logs: a rule carries at least one action"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid {
    pub rule: Option<String>,
    pub reason: &'static str,
    pub kind: InvalidKind,
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.rule {
            Some(id) => write!(f, "lifecycle rule {id}: {}", self.reason),
            None => f.write_str(self.reason),
        }
    }
}

/// The two classes LC8 splits: a document that does not match the published
/// schema (`MalformedXML`) and every semantic violation (`InvalidRequest`).
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::InvalidKind;
///
/// assert_ne!(InvalidKind::Shape, InvalidKind::Semantic);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidKind {
    Shape,
    Semantic,
}

/// What one atomic merge of rules into a stored configuration did.
///
/// The seed's write (LC13). The read, the upsert and the validation happen
/// in the store's one transaction, so a configuration written between them
/// cannot be replaced by the snapshot the merge started from, and a merge
/// that does not validate is not stored.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::{Invalid, InvalidKind, Merged};
///
/// let refused = Merged::Refused(Invalid {
///     rule: None,
///     reason: "a configuration carries at most 1000 rules",
///     kind: InvalidKind::Semantic,
/// });
/// assert!(matches!(refused, Merged::Refused(_)));
/// assert_ne!(Merged::Absent, Merged::Stored);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Merged {
    /// The bucket has no row. Nothing was written.
    Absent,
    /// The rules were upserted and the configuration stored.
    Stored,
    /// The merged configuration is not valid. Nothing was written.
    Refused(Invalid),
}

/// One lifecycle rule: what it selects, and what to do with it.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::{Expiration, Filter, Rule, Status};
///
/// let rule = Rule {
///     id: Some("expire-logs".into()),
///     status: Status::Enabled,
///     filter: Filter::default(),
///     expiration: Some(Expiration {
///         days: Some(30),
///         date: None,
///         expired_object_delete_marker: None,
///     }),
///     transitions: Vec::new(),
///     noncurrent_version_expiration: None,
///     abort_incomplete_multipart_upload: None,
/// };
/// assert_eq!(rule.id.as_deref(), Some("expire-logs"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rule {
    pub id: Option<String>,
    pub status: Status,
    /// Absent on the wire means every object, as does an empty one (LC8).
    #[serde(default)]
    pub filter: Filter,
    pub expiration: Option<Expiration>,
    /// Where the current version moves, and when (T11). Empty means none, and
    /// an empty list is not serialised.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<Transition>,
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

impl Rule {
    /// The Days-based rule the CLI's `add` verb and the configuration seed both
    /// spell: `Enabled`, the given prefix, and one action per day count.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::Rule;
    ///
    /// let rule = Rule::days(
    ///     Some("expire-logs".into()),
    ///     Some("logs/".into()),
    ///     Some(30),
    ///     None,
    ///     None,
    /// );
    /// assert_eq!(rule.filter.prefix.as_deref(), Some("logs/"));
    /// assert_eq!(
    ///     rule.expiration.and_then(|expiration| expiration.days),
    ///     Some(30)
    /// );
    /// assert!(rule.noncurrent_version_expiration.is_none());
    /// ```
    pub fn days(
        id: Option<String>,
        prefix: Option<String>,
        expire_days: Option<i32>,
        noncurrent_days: Option<i32>,
        abort_days: Option<i32>,
    ) -> Self {
        Self {
            id,
            status: Status::Enabled,
            filter: Filter {
                prefix,
                ..Filter::default()
            },
            expiration: expire_days.map(|days| Expiration {
                days: Some(days),
                date: None,
                expired_object_delete_marker: None,
            }),
            transitions: Vec::new(),
            noncurrent_version_expiration: noncurrent_days.map(|days| {
                NoncurrentVersionExpiration {
                    noncurrent_days: Some(days),
                    newer_noncurrent_versions: None,
                }
            }),
            abort_incomplete_multipart_upload: abort_days.map(|days| {
                AbortIncompleteMultipartUpload {
                    days_after_initiation: days,
                }
            }),
        }
    }

    fn refusal(&self, reason: &'static str, kind: InvalidKind) -> Invalid {
        Invalid {
            rule: self.id.clone(),
            reason,
            kind,
        }
    }

    fn validate(&self) -> Result<(), Invalid> {
        if self
            .id
            .as_ref()
            .is_some_and(|id| id.chars().count() > MAX_RULE_ID)
        {
            return Err(self.refusal(RULE_ID_TOO_LONG, InvalidKind::Semantic));
        }
        if let Some((reason, kind)) = self.filter.violation() {
            return Err(self.refusal(reason, kind));
        }
        if self.expiration.is_none()
            && self.transitions.is_empty()
            && self.noncurrent_version_expiration.is_none()
            && self.abort_incomplete_multipart_upload.is_none()
        {
            return Err(self.refusal(NO_ACTION, InvalidKind::Semantic));
        }
        let expires_delete_marker = self
            .expiration
            .as_ref()
            .is_some_and(Expiration::removes_delete_marker);
        if self.filter.uses_tags()
            && (self.abort_incomplete_multipart_upload.is_some() || expires_delete_marker)
        {
            return Err(self.refusal(TAG_FILTER_ACTION, InvalidKind::Semantic));
        }
        if let Some(reason) = self.expiration.as_ref().and_then(Expiration::violation) {
            return Err(self.refusal(reason, InvalidKind::Semantic));
        }
        if let Some(reason) = self
            .noncurrent_version_expiration
            .as_ref()
            .and_then(NoncurrentVersionExpiration::violation)
        {
            return Err(self.refusal(reason, InvalidKind::Semantic));
        }
        if let Some(reason) = self
            .abort_incomplete_multipart_upload
            .as_ref()
            .and_then(AbortIncompleteMultipartUpload::violation)
        {
            return Err(self.refusal(reason, InvalidKind::Semantic));
        }
        if let Some(reason) = self.transitions.iter().find_map(Transition::violation) {
            return Err(self.refusal(reason, InvalidKind::Semantic));
        }
        if let Some(noncurrent) = &self.noncurrent_version_expiration {
            let newer = noncurrent.newer_noncurrent_versions;
            if newer.is_some() && self.filter.is_everything() {
                return Err(self.refusal(NEWER_NEEDS_FILTER, InvalidKind::Semantic));
            }
            if newer.is_some_and(|count| !(1..=MAX_NEWER_NONCURRENT).contains(&count)) {
                return Err(self.refusal(NEWER_RANGE, InvalidKind::Semantic));
            }
        }
        Ok(())
    }
}

/// Whether a rule runs, or is stored and skipped.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::Status;
///
/// assert_ne!(Status::Enabled, Status::Disabled);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Enabled,
    Disabled,
}

/// What a rule selects: exactly one predicate, or none for every object
/// (an empty `Filter` applies to the whole bucket, as AWS documents it).
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::Filter;
///
/// let logs = Filter {
///     prefix: Some("logs/".into()),
///     ..Filter::default()
/// };
/// assert_eq!(logs.prefix.as_deref(), Some("logs/"));
/// ```
#[derive(Debug, SmartDefault, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub prefix: Option<String>,
    pub tag: Option<Tag>,
    pub size_greater_than: Option<u64>,
    pub size_less_than: Option<u64>,
    pub and: Option<And>,
}

impl Filter {
    fn predicates(&self) -> usize {
        [
            self.prefix.is_some(),
            self.tag.is_some(),
            self.size_greater_than.is_some(),
            self.size_less_than.is_some(),
            self.and.is_some(),
        ]
        .into_iter()
        .filter(|set| *set)
        .count()
    }

    /// No predicate at all: every object in the bucket.
    fn is_everything(&self) -> bool {
        self.predicates() == 0
    }

    /// Whether this filter selects on a tag — its own, or one inside [`And`].
    ///
    /// One home for the rule: the validation refuses the actions a tag filter
    /// excludes by it, and a consumer whose facts carry no tags asks it to know
    /// which rules it cannot evaluate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::{Filter, Tag};
    ///
    /// let by_prefix = Filter {
    ///     prefix: Some("logs/".into()),
    ///     ..Filter::default()
    /// };
    /// assert!(!by_prefix.uses_tags());
    ///
    /// let by_tag = Filter {
    ///     tag: Some(Tag {
    ///         key: "k".into(),
    ///         value: "v".into(),
    ///     }),
    ///     ..Filter::default()
    /// };
    /// assert!(by_tag.uses_tags());
    /// ```
    pub fn uses_tags(&self) -> bool {
        self.tag.is_some() || self.and.as_ref().is_some_and(|and| !and.tags.is_empty())
    }

    /// Whether any predicate bounds the object's size, including one inside
    /// `And`. An abort rule asks this before it sums an upload's parts.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::Filter;
    ///
    /// let bounded = Filter {
    ///     size_less_than: Some(10),
    ///     ..Filter::default()
    /// };
    /// assert!(bounded.uses_size());
    /// assert!(!Filter::default().uses_size());
    /// ```
    pub fn uses_size(&self) -> bool {
        self.size_greater_than.is_some()
            || self.size_less_than.is_some()
            || self.and.as_ref().is_some_and(And::uses_size)
    }

    fn violation(&self) -> Option<(&'static str, InvalidKind)> {
        if self.predicates() > 1 {
            return Some((FILTER_ARITY, InvalidKind::Shape));
        }
        size_violation(self.size_greater_than, self.size_less_than)
            .or_else(|| self.and.as_ref().and_then(And::violation))
    }
}

/// Two or more predicates that must all hold.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::{And, Tag};
///
/// let and = And {
///     prefix: Some("logs/".into()),
///     tags: vec![Tag {
///         key: "env".into(),
///         value: "prod".into(),
///     }],
///     ..And::default()
/// };
/// assert_eq!(and.tags.len(), 1);
/// ```
#[derive(Debug, SmartDefault, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct And {
    pub prefix: Option<String>,
    pub tags: Vec<Tag>,
    pub size_greater_than: Option<u64>,
    pub size_less_than: Option<u64>,
}

impl And {
    fn uses_size(&self) -> bool {
        self.size_greater_than.is_some() || self.size_less_than.is_some()
    }

    fn predicates(&self) -> usize {
        usize::from(self.prefix.is_some())
            + self.tags.len()
            + usize::from(self.size_greater_than.is_some())
            + usize::from(self.size_less_than.is_some())
    }

    fn violation(&self) -> Option<(&'static str, InvalidKind)> {
        if self.predicates() < 2 {
            return Some((AND_ARITY, InvalidKind::Shape));
        }
        let mut keys = HashSet::with_capacity(self.tags.len());
        if !self.tags.iter().all(|tag| keys.insert(tag.key.as_str())) {
            return Some((TAG_KEYS_UNIQUE, InvalidKind::Semantic));
        }
        size_violation(self.size_greater_than, self.size_less_than)
    }
}

/// The size bounds both predicate bags share: the greater bound sits below
/// the less one, and neither passes the object ceiling.
fn size_violation(greater: Option<u64>, less: Option<u64>) -> Option<(&'static str, InvalidKind)> {
    if greater.is_some_and(|size| size > MAX_SIZE) || less.is_some_and(|size| size > MAX_SIZE) {
        return Some((SIZE_CEILING, InvalidKind::Semantic));
    }
    matches!((greater, less), (Some(greater), Some(less)) if greater >= less)
        .then_some((SIZE_ORDER, InvalidKind::Semantic))
}

/// One object tag a filter selects on: exact key and exact value.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::Tag;
///
/// let tag = Tag {
///     key: "env".into(),
///     value: "prod".into(),
/// };
/// assert_eq!(tag.key, "env");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// When the current version expires: `Days` after it was created, or at
/// `Date`.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::Expiration;
///
/// let expiration = Expiration {
///     days: Some(30),
///     date: None,
///     expired_object_delete_marker: None,
/// };
/// assert_eq!(expiration.days, Some(30));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expiration {
    /// Days after the object was created; at least 1.
    pub days: Option<i32>,
    /// Unix seconds, always midnight UTC — a multiple of `86400`. A date in
    /// the past is accepted: AWS makes the objects eligible immediately, and
    /// the matcher expires them on its next pass.
    pub date: Option<i64>,
    /// Expire a delete marker that has no noncurrent versions behind it.
    pub expired_object_delete_marker: Option<bool>,
}

impl Expiration {
    /// Whether this expiration's action is `ExpiredObjectDeleteMarker` — the
    /// predicate the validator and the pass share, so the flag is read in one
    /// place.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::lifecycle::Expiration;
    ///
    /// let marker = Expiration {
    ///     days: None,
    ///     date: None,
    ///     expired_object_delete_marker: Some(true),
    /// };
    /// assert!(marker.removes_delete_marker());
    /// let plain = Expiration {
    ///     days: Some(1),
    ///     date: None,
    ///     expired_object_delete_marker: None,
    /// };
    /// assert!(!plain.removes_delete_marker());
    /// ```
    pub fn removes_delete_marker(&self) -> bool {
        self.expired_object_delete_marker == Some(true)
    }

    fn violation(&self) -> Option<&'static str> {
        if self.days.is_none() && self.date.is_none() && self.expired_object_delete_marker.is_none()
        {
            return Some(NO_ACTION);
        }
        let dated = self.days.is_some() || self.date.is_some();
        if self.expired_object_delete_marker == Some(true) && dated {
            return Some(DELETE_MARKER_DAYS);
        }
        if self.days.is_some() && self.date.is_some() {
            return Some(DAYS_AND_DATE);
        }
        if self.days.is_some_and(|days| days < 1) {
            return Some(DAYS_MIN);
        }
        self.date
            .is_some_and(|date| date.rem_euclid(SECONDS_PER_DAY as i64) != 0)
            .then_some(DATE_MIDNIGHT)
    }
}

/// When the current version moves to `STANDARD_IA`: `Days` after it was
/// created, or at `Date` (T11).
///
/// `STANDARD_IA` is the only target: this build's tiers are the filesystem
/// plane and the cold plane, so there is nowhere else to go, and a
/// transition that names its current class would not be a transition.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{Class, lifecycle::Transition};
///
/// let transition = Transition {
///     days: Some(30),
///     date: None,
///     storage_class: Class::StandardIa,
/// };
/// assert_eq!(transition.storage_class, Class::StandardIa);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transition {
    /// Days after the object was created; at least 30.
    pub days: Option<i32>,
    /// Unix seconds, always midnight UTC. A date in the past is accepted: AWS
    /// makes the objects eligible immediately.
    pub date: Option<i64>,
    /// The target class; `STANDARD_IA` is the only accepted one.
    pub storage_class: Class,
}

impl Transition {
    fn violation(&self) -> Option<&'static str> {
        if self.days.is_none() && self.date.is_none() {
            return Some(NO_ACTION);
        }
        if self.days.is_some() && self.date.is_some() {
            return Some(TRANSITION_DAYS_AND_DATE);
        }
        if self.storage_class != Class::StandardIa {
            return Some(TRANSITION_CLASS);
        }
        if self.days.is_some_and(|days| days < MIN_TRANSITION_DAYS) {
            return Some(TRANSITION_DAYS_MIN);
        }
        self.date
            .is_some_and(|date| date.rem_euclid(SECONDS_PER_DAY as i64) != 0)
            .then_some(TRANSITION_DATE_MIDNIGHT)
    }
}

/// When a noncurrent version expires, counted from its successor's creation.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::NoncurrentVersionExpiration;
///
/// let noncurrent = NoncurrentVersionExpiration {
///     noncurrent_days: Some(30),
///     newer_noncurrent_versions: Some(3),
/// };
/// assert_eq!(noncurrent.newer_noncurrent_versions, Some(3));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Days the version has been noncurrent; at least 1.
    pub noncurrent_days: Option<i32>,
    /// How many noncurrent versions to keep, `1..=100`.
    pub newer_noncurrent_versions: Option<i32>,
}

impl NoncurrentVersionExpiration {
    fn violation(&self) -> Option<&'static str> {
        if self.noncurrent_days.is_none() && self.newer_noncurrent_versions.is_none() {
            return Some(NO_ACTION);
        }
        self.noncurrent_days
            .is_some_and(|days| days < 1)
            .then_some(NONCURRENT_DAYS_MIN)
    }
}

/// When an incomplete multipart upload is aborted, counted from its
/// initiation.
///
/// # Examples
///
/// ```rust
/// use tinio_core::lifecycle::AbortIncompleteMultipartUpload;
///
/// let abort = AbortIncompleteMultipartUpload {
///     days_after_initiation: 7,
/// };
/// assert_eq!(abort.days_after_initiation, 7);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortIncompleteMultipartUpload {
    /// Days since the upload was initiated; at least 1.
    pub days_after_initiation: i32,
}

impl AbortIncompleteMultipartUpload {
    fn violation(&self) -> Option<&'static str> {
        (self.days_after_initiation < 1).then_some(ABORT_DAYS_MIN)
    }
}
