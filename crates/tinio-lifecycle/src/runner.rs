//! The pass: one stateless sweep of every bucket's stored rules (design LC4,
//! LC12).
//!
//! Three actions are the API's own deletion — the engine owns no version-aware
//! removal of its own (LC2) — and the fourth is the storage contract's own
//! move:
//!
//! - **current versions** — [`ObjectOps::delete_object`](crate::_core::storage::ObjectOps::delete_object):
//!   a delete marker in a versioned bucket, an unlink otherwise;
//! - **transitions** — [`Storage::transition`](crate::_core::storage::Storage::transition):
//!   the object moves to its rule's storage class, on a backend with tiers
//!   only. Expiration is evaluated first and wins when both are due, a delete
//!   marker is never moved, and a bucket that records versions skips this
//!   action alone (T11);
//! - **noncurrent versions** — [`VersionOps::delete_object_version`](crate::_core::storage::VersionOps::delete_object_version):
//!   permanent, evaluated per key over the whole chain, newest first;
//! - **incomplete uploads** — [`MultipartOps::abort_multipart_upload`](crate::_core::storage::MultipartOps::abort_multipart_upload).
//!
//! The pass keeps no cursor (LC12): it re-lists each cycle, so a restarted run
//! resumes by re-deriving what is due, and every action is idempotent — an
//! item that is already gone is the outcome the action wanted.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::{StreamExt as _, stream};
use smart_default::SmartDefault;
#[cfg(test)]
use tinio_util::testing::metrics::test_lock;
use tokio::sync::watch;
use tracing::{field, instrument};

use crate::{
    _core::{
        Class, ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams, bucket,
        lifecycle::{self, DEFAULT_CYCLE, DEFAULT_DELAY},
        object, object_lock,
        storage::{self, Storage},
        version::{self, CurrentVersion, IdMarker, ListParams, State},
    },
    _util::{
        metrics::{LockOperation, record_protected_deletion},
        pacing,
    },
    clock::Clock,
    metrics::{self, Kind},
};

/// One page of any listing. The pass walks every page: a bucket larger than
/// one page is the normal case, not an edge.
const PAGE: usize = 1000;

/// The transitions one pass runs at once (T14): one, the gentlest walk over a
/// store the server is also serving.
const DEFAULT_CONCURRENCY: usize = 1;

/// How hard the pass works: the pause between two actions and the wait between
/// two passes — the operator's `[lifecycle]` keys — and how many transitions
/// run at once, which this build sets in code only.
///
/// # Examples
///
/// ```rust
/// use std::time::Duration;
///
/// use tinio_lifecycle::Options;
///
/// let defaults = Options::default();
/// assert_eq!(defaults.delay, Duration::from_secs(10));
/// assert_eq!(defaults.cycle, Duration::from_secs(3600));
/// assert_eq!(defaults.concurrency, 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, SmartDefault)]
pub struct Options {
    /// Pause after each action inside a pass, so a sweep never saturates a
    /// store the server is serving.
    #[default(_code = "DEFAULT_DELAY")]
    pub delay: Duration,
    /// Wait between the end of one pass and the start of the next.
    #[default(_code = "DEFAULT_CYCLE")]
    pub cycle: Duration,
    /// How many transitions one pass runs at once (T14). A zero is read as one:
    /// a pass with no worker could only skip the action.
    #[default(_code = "DEFAULT_CONCURRENCY")]
    pub concurrency: usize,
}

/// What one pass did — the pass's counters, one summary line (LC16, OL19).
///
/// # Examples
///
/// ```rust
/// use tinio_lifecycle::Summary;
///
/// let idle = Summary::default();
/// assert_eq!(idle.expired_objects, 0);
/// assert_eq!(idle.transitioned_objects, 0);
/// assert_eq!(idle.protected_versions, 0);
/// assert!(idle.buckets_with_rules == 0);
/// ```
#[derive(Debug, Clone, Copy, SmartDefault, PartialEq, Eq)]
pub struct Summary {
    /// Buckets carrying a stored configuration this pass looked at.
    pub buckets_with_rules: usize,
    /// Current versions expired.
    pub expired_objects: u64,
    /// Current versions moved to another storage class by a transition. An
    /// object already in its rule's class is a no-op and is not counted.
    pub transitioned_objects: u64,
    /// Versions removed permanently: noncurrent ones, and delete markers that
    /// were their key's only version.
    pub expired_versions: u64,
    /// Noncurrent versions a stored Object Lock protected: a counted skip,
    /// never an error (OL19). One refusal increments this counter, so it
    /// counts refusals per pass, not distinct versions — a version an action
    /// reaches twice increments it twice.
    pub protected_versions: u64,
    /// Incomplete uploads aborted.
    pub aborted_uploads: u64,
    /// Failed reads and actions. An item that vanished before its action is
    /// not one: that outcome already holds.
    pub errors: u64,
}

/// One rule to upsert into a bucket's stored configuration — the configuration
/// file's seed (LC13).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{bucket, lifecycle};
/// use tinio_lifecycle::Seed;
///
/// let seed = Seed {
///     bucket: bucket::name("data").unwrap(),
///     rule: lifecycle::Rule {
///         id: Some("expire-logs".into()),
///         status: lifecycle::Status::Enabled,
///         filter: lifecycle::Filter {
///             prefix: Some("logs/".into()),
///             ..lifecycle::Filter::default()
///         },
///         expiration: Some(lifecycle::Expiration {
///             days: Some(30),
///             date: None,
///             expired_object_delete_marker: None,
///         }),
///         transitions: Vec::new(),
///         noncurrent_version_expiration: None,
///         abort_incomplete_multipart_upload: None,
///     },
/// };
/// assert_eq!(
///     seed.rule.expiration.and_then(|expiration| expiration.days),
///     Some(30)
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Seed {
    /// The bucket the rule is written to.
    pub bucket: bucket::Name,
    /// The rule itself, merged by its `id`.
    pub rule: lifecycle::Rule,
}

/// The lifecycle pass over one backend: options, a clock, and the stop signal
/// it shares with the server.
///
/// # Examples
///
/// Any [`Storage`] backend works; this runs one pass over an in-memory store
/// with the wall clock and the pause defaults.
///
/// ```rust
/// use std::{sync::Arc, time::SystemTime};
///
/// use tinio_lifecycle::{FixedClock, Options, Runner};
/// use tinio_mem::MemoryStorage;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let (stop, stopped) = tokio::sync::watch::channel(false);
/// let runner = Runner::new(
///     Arc::new(MemoryStorage::new().expect("an in-memory store")),
///     Options::default(),
///     Arc::new(FixedClock(SystemTime::now())),
///     stopped,
/// );
/// let summary = runner.pass().await;
/// assert_eq!(summary.buckets_with_rules, 0, "an empty store has no rules");
/// # drop(stop);
/// # }
/// ```
pub struct Runner<S: Storage> {
    storage: Arc<S>,
    options: Options,
    clock: Arc<dyn Clock>,
    stop: watch::Receiver<bool>,
}

/// One bucket's sweep: the configuration, the pass's instant and its counters —
/// the four values every action walks with.
struct Sweep<'a> {
    bucket: &'a bucket::Name,
    config: &'a lifecycle::Config,
    now: SystemTime,
    /// Whether a noncurrent rule selects on tags — shared by the whole chain
    /// walk, so the rule set is not re-scanned per key.
    reads_tags: bool,
    summary: &'a mut Summary,
}

/// Which object action a [`Runner::walk_objects`] walk serves.
#[derive(Clone, Copy)]
enum ObjectAction {
    /// Expire a due current version.
    Expire,
    /// Move a due current version to its rule's storage class.
    Transition,
}

impl<S: Storage> Runner<S> {
    /// Build a pass. `stop` is the server's own shutdown channel: the pass
    /// ends quietly once it turns true.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::SystemTime};
    ///
    /// use tinio_lifecycle::{FixedClock, Options, Runner};
    /// use tinio_mem::MemoryStorage;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let (_stop, stopped) = tokio::sync::watch::channel(false);
    /// let runner = Runner::new(
    ///     Arc::new(MemoryStorage::new().expect("an in-memory store")),
    ///     Options::default(),
    ///     Arc::new(FixedClock(SystemTime::now())),
    ///     stopped,
    /// );
    /// assert_eq!(runner.pass().await.errors, 0);
    /// # }
    /// ```
    pub fn new(
        storage: Arc<S>,
        options: Options,
        clock: Arc<dyn Clock>,
        stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            storage,
            options,
            clock,
            stop,
        }
    }

    /// Apply the file's rules to the stored configurations (LC13): upsert each
    /// rule by `(bucket, id)` into its bucket's own configuration.
    ///
    /// One-way, deliberately: a rule removed from the file is not deleted from
    /// the bucket, because the file cannot distinguish that from a rule a
    /// client added. A rule naming a bucket that does not exist is warned about
    /// and skipped — a bucket created later is seeded on the next start.
    ///
    /// **The seed is a write path, so the tier gate covers it** (T11): on a
    /// backend whose [`Storage::supports_tiers`] is `false`, a bucket whose
    /// rules carry a transition is refused wholesale — nothing of that bucket
    /// is written, and the refusal is counted in [`Summary::errors`] like a
    /// configuration the model rejects.
    ///
    /// The answer counts the buckets that took a rule in
    /// [`buckets_with_rules`](Summary::buckets_with_rules), and the ones whose
    /// write failed in [`errors`](Summary::errors); the action counters are the
    /// pass's and stay zero. A stop ends the seed where it stands: the buckets
    /// it never reached stay unseeded, and are not counted.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::SystemTime};
    ///
    /// use tinio_core::{acl::Acl, bucket, lifecycle, storage::BucketOps};
    /// use tinio_lifecycle::{FixedClock, Options, Runner, Seed};
    /// use tinio_mem::MemoryStorage;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let storage = Arc::new(MemoryStorage::new().expect("an in-memory store"));
    /// let name = bucket::name("logs").expect("a bucket name");
    /// storage
    ///     .create_bucket(&name, None, &Acl::default_private(None))
    ///     .await
    ///     .expect("a bucket");
    /// let (_stop, stopped) = tokio::sync::watch::channel(false);
    /// let runner = Runner::new(
    ///     Arc::clone(&storage),
    ///     Options::default(),
    ///     Arc::new(FixedClock(SystemTime::now())),
    ///     stopped,
    /// );
    /// let seeds = [Seed {
    ///     bucket: name,
    ///     rule: lifecycle::Rule {
    ///         id: Some("expire-logs".into()),
    ///         status: lifecycle::Status::Enabled,
    ///         filter: lifecycle::Filter::default(),
    ///         expiration: Some(lifecycle::Expiration {
    ///             days: Some(30),
    ///             date: None,
    ///             expired_object_delete_marker: None,
    ///         }),
    ///         transitions: Vec::new(),
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     },
    /// }];
    /// let summary = runner.seed(&seeds).await;
    /// assert_eq!(summary.buckets_with_rules, 1, "one bucket took the rule");
    /// # }
    /// ```
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    pub async fn seed(&self, seeds: &[Seed]) -> Summary {
        let mut summary = Summary::default();
        // One write per bucket: the rules of a bucket land together. The index
        // keeps first-seen order, so a stop lands where the file's order says.
        let mut buckets: Vec<(&bucket::Name, Vec<lifecycle::Rule>)> = Vec::new();
        let mut index: HashMap<&bucket::Name, usize> = HashMap::new();
        for seed in seeds {
            match index.get(&seed.bucket) {
                Some(&at) => buckets[at].1.push(seed.rule.clone()),
                None => {
                    index.insert(&seed.bucket, buckets.len());
                    buckets.push((&seed.bucket, vec![seed.rule.clone()]));
                }
            }
        }
        for (bucket, rules) in buckets {
            // Checked per bucket, before it: a stop ends the seed where it
            // stands, so the summary reports what it wrote and not what a
            // shutdown interrupted.
            if self.stopping() {
                return summary;
            }
            // The tier gate (T11): a backend without tiers refuses the bucket's merge when any
            // of its rules carries a transition. Nothing of the bucket is written, and the
            // refusal is the counted failure a configuration the model rejects gets.
            if !self.storage.supports_tiers()
                && let Some(rule) = rules.iter().find(|rule| !rule.transitions.is_empty())
            {
                let invalid = lifecycle::Invalid {
                    rule: rule.id.clone(),
                    reason: lifecycle::TRANSITIONS_NEED_TIERS,
                    kind: lifecycle::InvalidKind::Semantic,
                };
                tracing::warn!(%invalid, "a lifecycle seed was refused");
                summary.errors += 1;
                continue;
            }
            // One store call: the read, the upsert and the validation are the
            // store's transaction, so a configuration written between them is
            // not replaced by this snapshot (LC13).
            match self.storage.merge_bucket_lifecycle(bucket, &rules).await {
                Ok(lifecycle::Merged::Stored) => summary.buckets_with_rules += 1,
                Ok(lifecycle::Merged::Absent) => pass_failed(
                    storage::no_such_bucket(bucket),
                    &mut summary,
                    "seed a bucket's configuration",
                ),
                Ok(lifecycle::Merged::Refused(invalid)) => {
                    tracing::warn!(%invalid, "a lifecycle seed was refused");
                    summary.errors += 1;
                }
                Err(error) => pass_failed(error, &mut summary, "seed a bucket's configuration"),
            }
        }
        summary
    }

    /// One pass over every bucket that carries a configuration, and the summary
    /// line (LC16): one `info` per pass, nothing per object at `info`.
    ///
    /// Stops between two actions, never inside one, and never mid-bucket: a
    /// stop ends the pass, and the next pass re-lists from scratch because the
    /// pass keeps no cursor (LC12).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::SystemTime};
    ///
    /// use tinio_lifecycle::{FixedClock, Options, Runner};
    /// use tinio_mem::MemoryStorage;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let (_stop, stopped) = tokio::sync::watch::channel(false);
    /// let runner = Runner::new(
    ///     Arc::new(MemoryStorage::new().expect("an in-memory store")),
    ///     Options::default(),
    ///     Arc::new(FixedClock(SystemTime::now())),
    ///     stopped,
    /// );
    /// let summary = runner.pass().await;
    /// assert_eq!(summary.buckets_with_rules, 0, "nothing is configured");
    /// assert_eq!(summary.errors, 0);
    /// # }
    /// ```
    #[instrument(
        target = "tinio::lifecycle",
        level = "info",
        name = "tinio.lifecycle.pass",
        skip_all,
        fields(
            buckets_with_rules = field::Empty,
            expired_objects = field::Empty,
            transitioned_objects = field::Empty,
            expired_versions = field::Empty,
            protected_versions = field::Empty,
            aborted_uploads = field::Empty,
            errors = field::Empty
        )
    )]
    pub async fn pass(&self) -> Summary {
        let _pass = metrics::PassTimer::new();
        let mut summary = Summary::default();
        let now = self.clock.now();
        // One scan for every stored configuration, then the bucket walk reads
        // the map. Intersecting with `list_buckets` is LC19's rule: a rule row
        // whose bucket vanished out of band is never looked up, so it cannot
        // expire a re-created bucket's objects.
        let mut configs: HashMap<_, _> = match self.storage.lifecycles().await {
            Ok(configs) => configs.into_iter().collect(),
            Err(error) => {
                pass_failed(error, &mut summary, "read the lifecycle rules");
                HashMap::new()
            }
        };
        let mut start_after: Option<String> = None;
        if !configs.is_empty() {
            'pages: loop {
                let page = match self
                    .storage
                    .list_buckets(
                        ListBucketsParams {
                            prefix: String::new(),
                            start_after: start_after.clone(),
                            max_buckets: PAGE,
                        },
                        None,
                        None,
                    )
                    .await
                {
                    Ok(page) => page,
                    Err(error) => {
                        pass_failed(error, &mut summary, "list buckets");
                        break;
                    }
                };
                for bucket in &page.buckets {
                    let Some(config) = configs.remove(&bucket.name) else {
                        continue;
                    };
                    summary.buckets_with_rules += 1;
                    let reads_tags = config.rules.iter().any(|rule| {
                        rule.status == lifecycle::Status::Enabled
                            && rule.noncurrent_version_expiration.is_some()
                            && rule.filter.uses_tags()
                    });
                    let mut sweep = Sweep {
                        bucket: &bucket.name,
                        config: &config,
                        now,
                        reads_tags,
                        summary: &mut summary,
                    };
                    if !self.expire(&mut sweep).await {
                        break 'pages;
                    }
                }
                if configs.is_empty() {
                    break;
                }
                if !page.truncated {
                    break;
                }
                match page.next_start_after {
                    Some(marker) => start_after = Some(marker),
                    None => break,
                }
            }
        }
        // The summary rides on the span, so the close line prints it once instead
        // of a second event beside the line.
        tracing::Span::current().record("buckets_with_rules", summary.buckets_with_rules);
        tracing::Span::current().record("expired_objects", summary.expired_objects);
        tracing::Span::current().record("transitioned_objects", summary.transitioned_objects);
        tracing::Span::current().record("expired_versions", summary.expired_versions);
        tracing::Span::current().record("protected_versions", summary.protected_versions);
        tracing::Span::current().record("aborted_uploads", summary.aborted_uploads);
        tracing::Span::current().record("errors", summary.errors);
        summary
    }

    /// The background future: a pass, then a cycle's wait, until the stop
    /// signal.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::{sync::Arc, time::SystemTime};
    ///
    /// use tinio_lifecycle::{FixedClock, Options, Runner};
    /// use tinio_mem::MemoryStorage;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let (stop, stopped) = tokio::sync::watch::channel(false);
    /// let runner = Runner::new(
    ///     Arc::new(MemoryStorage::new().expect("an in-memory store")),
    ///     Options::default(),
    ///     Arc::new(FixedClock(SystemTime::now())),
    ///     stopped,
    /// );
    /// let run = tokio::spawn(async move { runner.run().await });
    /// stop.send(true).expect("the pass is listening");
    /// run.await.expect("the pass ends cleanly");
    /// # }
    /// ```
    #[instrument(
        level = "info",
        target = "tinio::lifecycle",
        name = "tinio.lifecycle.run",
        skip(self)
    )]
    async fn cycle(&self) -> bool {
        self.pass().await;
        self.wait(self.options.cycle).await
    }

    pub async fn run(&self) {
        loop {
            if self.stopping() {
                return;
            }
            // The loop body is the unit: one `run` span per cycle, so a
            // process-length future never carries one process-length span.
            let keep_going = self.cycle().await;
            if !keep_going {
                return;
            }
        }
    }

    /// Whether the run is stopping.
    fn stopping(&self) -> bool {
        *self.stop.borrow()
    }

    /// Sleep `duration`, cut short by the stop signal. `false` once the run is
    /// stopping, so every caller's next step is the return.
    async fn wait(&self, duration: Duration) -> bool {
        if self.stopping() {
            return false;
        }
        pacing::sleep_checked(duration, duration, &self.stop).await;
        !self.stopping()
    }

    /// One bucket's four actions. `false` once the run is stopping.
    async fn expire(&self, sweep: &mut Sweep<'_>) -> bool {
        // A bucket whose rules carry no `Days`/`Date` expiration pays nothing
        // for the object listing (LC21's per-action ownership, read from the
        // model's own predicate).
        if sweep.config.governs_expiration() && !self.expire_objects(sweep).await {
            return false;
        }
        // Expiration is evaluated first: a transition never moves an object
        // this pass is already deleting, and it is a bucket-level decision —
        // a bucket that records versions skips this action alone (T11).
        if governs_transitions(sweep.config) && !self.transition_objects(sweep).await {
            return false;
        }
        // One owner per action: a bucket whose rules carry neither version
        // action pays nothing for the version listing (LC21's per-action
        // ownership, read from the model's own predicates).
        if governs_versions(sweep.config) && !self.expire_versions(sweep).await {
            return false;
        }
        if sweep.config.governs_abort_incomplete_multipart_upload() {
            return self.abort_uploads(sweep).await;
        }
        true
    }

    /// Walk every page of the sweep's bucket, running `action` on each current
    /// version the listing describes.
    ///
    /// The one skeleton the object actions share: the listing parameters, the
    /// per-item stop check, [`lifecycle::Facts`] from the listing and the
    /// truncation cursor. `false` once the run is stopping.
    async fn walk_objects(&self, sweep: &mut Sweep<'_>, action: ObjectAction) -> bool {
        let mut due = Vec::new();
        let mut start_after: Option<String> = None;
        loop {
            let page = match self
                .storage
                .list_objects(ListObjectsParams {
                    bucket: sweep.bucket.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    start_after: start_after.clone(),
                    max_keys: PAGE,
                })
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    read_failed(
                        error,
                        sweep.bucket,
                        sweep.summary,
                        "list a bucket's objects",
                        Kind::Object,
                    );
                    return true;
                }
            };
            for info in &page.objects {
                // Checked per item, so a shutdown interrupts a long listing
                // walk and not only its actions.
                if self.stopping() {
                    return false;
                }
                // The listing is the facts (LC10): no read per object.
                let facts =
                    lifecycle::Facts::new(&info.key, info.size, info.last_modified, &info.tags);
                let keep_going = match action {
                    ObjectAction::Expire => self.expire_object(sweep, &facts, info).await,
                    ObjectAction::Transition => {
                        self.transition_object(sweep, &facts, info, &mut due).await
                    }
                };
                if !keep_going {
                    return false;
                }
            }
            // A page-scoped action runs what it collected before the next page
            // is listed, so a move never waits for the whole bucket.
            let keep_going = match action {
                ObjectAction::Expire => true,
                ObjectAction::Transition => {
                    due.is_empty() || self.transition_due(sweep, &mut due).await
                }
            };
            if !keep_going {
                return false;
            }
            if !page.truncated {
                return true;
            }
            match page.next_start_after {
                Some(marker) => start_after = Some(marker),
                None => return true,
            }
        }
    }

    /// Expire current versions, page by page.
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    async fn expire_objects(&self, sweep: &mut Sweep<'_>) -> bool {
        self.walk_objects(sweep, ObjectAction::Expire).await
    }

    /// Expire one current version. `false` once the run is stopping.
    async fn expire_object(
        &self,
        sweep: &mut Sweep<'_>,
        facts: &lifecycle::Facts<'_>,
        info: &object::Info,
    ) -> bool {
        let Some(expiry) = sweep.config.expiry_for(facts) else {
            return true;
        };
        if expiry > sweep.now {
            return true;
        }
        match self.due(sweep.bucket, &info.key).await {
            Ok(true) => {}
            Ok(false) => return true,
            Err(error) => {
                read_failed(
                    error,
                    sweep.bucket,
                    sweep.summary,
                    "read a due key's current version",
                    Kind::Object,
                );
                return true;
            }
        }
        if counted(
            self.storage.delete_object(sweep.bucket, &info.key).await,
            sweep.summary,
            Kind::Object,
        ) {
            sweep.summary.expired_objects += 1;
        }
        self.wait(self.options.delay).await
    }

    /// Move the bucket's due current versions to their rule's storage class,
    /// `Options.concurrency` moves at a time.
    ///
    /// Only a backend with tiers and a bucket that records no versions runs a
    /// transition (T11); everything else pays one versioning read and nothing
    /// more. That read is the runner's own, not a second opinion on the tier
    /// backend's: a versioned bucket is never listed for moves at all, and the
    /// backend's own skip is silent, so counting it here would report a
    /// transition that never happened. A key whose expiration is also due is
    /// left to expiration — this walk runs after [`Runner::expire_objects`] and
    /// re-asks the question, so the order holds even over a stale listing. A
    /// key whose current slot is a delete marker is never moved, and a target
    /// class the object already holds is a no-op: the class is read before the
    /// move, so a demoted key costs one read on every later pass and no move.
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    async fn transition_objects(&self, sweep: &mut Sweep<'_>) -> bool {
        if !self.storage.supports_tiers() {
            return true;
        }
        match self.storage.bucket_versioning(sweep.bucket).await {
            Ok(State::Unversioned) => {}
            Ok(_) => return true,
            Err(error) => {
                read_failed(
                    error,
                    sweep.bucket,
                    sweep.summary,
                    "read a bucket's versioning state",
                    Kind::Object,
                );
                return true;
            }
        }
        self.walk_objects(sweep, ObjectAction::Transition).await
    }

    /// Collect one due current version for its page's move. `false` once the
    /// run is stopping.
    async fn transition_object(
        &self,
        sweep: &mut Sweep<'_>,
        facts: &lifecycle::Facts<'_>,
        info: &object::Info,
        due: &mut Vec<DueTransition>,
    ) -> bool {
        // Expiration is evaluated first and wins when both are due.
        if sweep
            .config
            .expiry_for(facts)
            .is_some_and(|expiry| expiry <= sweep.now)
        {
            return true;
        }
        let Some((at, class)) = transition_for(sweep.config, facts) else {
            return true;
        };
        if at > sweep.now {
            return true;
        }
        // A delete marker holds no content to move (T11). Paid only for
        // a key already due.
        match self.due(sweep.bucket, &info.key).await {
            Ok(true) => {}
            Ok(false) => return true,
            Err(error) => {
                read_failed(
                    error,
                    sweep.bucket,
                    sweep.summary,
                    "read a due key's current version",
                    Kind::Object,
                );
                return true;
            }
        }
        // A target class the object already holds is a no-op.
        match self.storage.storage_class(sweep.bucket, &info.key).await {
            Ok(current) if current == class => return true,
            Ok(_) => {}
            Err(error) => {
                read_failed(
                    error,
                    sweep.bucket,
                    sweep.summary,
                    "read a due key's storage class",
                    Kind::Object,
                );
                return true;
            }
        }
        due.push(DueTransition {
            key: info.key.clone(),
            class,
        });
        true
    }

    /// Run one page's due transitions, `Options.concurrency` at a time, each
    /// followed by the operator's pause. `false` once the run is stopping.
    ///
    /// The buffer is drained, never taken, so its allocation is reused across
    /// pages. The pause lives inside the workers: a pause taken serially
    /// between them would serialize exactly the moves the worker count was
    /// raised to parallelize.
    async fn transition_due(&self, sweep: &mut Sweep<'_>, due: &mut Vec<DueTransition>) -> bool {
        let workers = self.options.concurrency.max(1);
        let now = sweep.now;
        let bucket = sweep.bucket;
        let mut moves = stream::iter(due.drain(..).map(|due| async move {
            let outcome = self
                .storage
                .transition(bucket, &due.key, due.class, now)
                .await;
            self.wait(self.options.delay).await;
            outcome
        }))
        .buffer_unordered(workers);
        while let Some(outcome) = moves.next().await {
            let outcome: Result<(), storage::Error> = outcome.map_err(Into::into);
            // The action family's contract holds for a transition as for every
            // other action: `ok` when it did what it wanted — an object already
            // gone included — and `error` when the store refused, so the two
            // series stay paired.
            match outcome {
                Ok(()) => {
                    metrics::record_action(Kind::Object, true);
                    sweep.summary.transitioned_objects += 1;
                }
                Err(error @ storage::Error::NoSuchKey(_))
                | Err(error @ storage::Error::NoSuchBucket(_)) => {
                    // The object vanished before the move: the outcome the
                    // action wanted, so it is `ok` — and not a transition.
                    metrics::record_action(Kind::Object, true);
                    tracing::debug!(%error, "a lifecycle transition found its object already gone");
                }
                Err(error) => {
                    action_failed(error, sweep.summary, "transition an object", Kind::Object);
                }
            }
            if self.stopping() {
                return false;
            }
        }
        true
    }

    /// Whether a due key still has a content version to expire (LC2): a key
    /// whose current slot is already a delete marker is skipped, or the pass
    /// would add one marker per cycle. Paid only for keys already due.
    async fn due(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <S as Storage>::Error> {
        Ok(self.storage.current_version(bucket, key).await? != CurrentVersion::Marker)
    }

    /// Expire noncurrent versions and expired delete markers, per key over the
    /// whole version chain.
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    async fn expire_versions(&self, sweep: &mut Sweep<'_>) -> bool {
        // A bucket that records no versions has no noncurrent version and no
        // marker, so its chain listing has nothing for either action.
        match self.storage.bucket_versioning(sweep.bucket).await {
            Ok(State::Unversioned) => return true,
            Ok(_) => {}
            Err(error) => {
                read_failed(
                    error,
                    sweep.bucket,
                    sweep.summary,
                    "read a bucket's versioning state",
                    Kind::Version,
                );
                return true;
            }
        }
        let mut chain: Vec<version::Entry> = Vec::new();
        let mut cursor: Option<(String, IdMarker)> = None;
        loop {
            let page = match self
                .storage
                .list_object_versions(
                    sweep.bucket,
                    ListParams {
                        prefix: None,
                        delimiter: None,
                        key_marker: cursor.as_ref().map(|(key, _)| key.clone()),
                        version_id_marker: cursor.as_ref().map(|(_, marker)| marker.clone()),
                        max_keys: PAGE,
                    },
                )
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    read_failed(
                        error,
                        sweep.bucket,
                        sweep.summary,
                        "list a bucket's versions",
                        Kind::Version,
                    );
                    return true;
                }
            };
            for entry in page.entries {
                if self.stopping() {
                    return false;
                }
                // A key's chain is acted on only once it is whole: its ranks
                // count versions of that key, and a page's resume marker names
                // the page's last entry — never one this pass deleted.
                if chain.last().is_some_and(|last| last.key != entry.key) {
                    if !self.expire_chain(sweep, &chain).await {
                        return false;
                    }
                    chain.clear();
                }
                chain.push(entry);
            }
            if !page.is_truncated {
                break;
            }
            match (page.next_key_marker, page.next_version_id_marker) {
                (Some(key), Some(marker)) => cursor = Some((key, IdMarker(marker))),
                _ => break,
            }
        }
        if chain.is_empty() {
            return true;
        }
        self.expire_chain(sweep, &chain).await
    }

    /// One key's chain, newest first.
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    async fn expire_chain(&self, sweep: &mut Sweep<'_>, chain: &[version::Entry]) -> bool {
        // The stop is checked here, not only per listed entry: the chain left
        // after the listing loop reaches this call with no check of its own, and
        // it is acted on only while the run still runs.
        if self.stopping() {
            return false;
        }
        let Some((newest, noncurrent)) = chain.split_first() else {
            return true;
        };
        if noncurrent.is_empty() {
            // A marker holds no tags, so this key's facts carry none — a
            // tag-filtered rule cannot select a marker.
            let tags = object::Tags::empty();
            let facts =
                lifecycle::Facts::new(&newest.key, newest.size, newest.last_modified, &tags);
            // `ExpiredObjectDeleteMarker` removes a marker that is its key's
            // only version; a marker over a content version is left alone.
            let removes_markers = sweep.config.rules_for(&facts).any(|rule| {
                rule.expiration
                    .as_ref()
                    .is_some_and(lifecycle::Expiration::removes_delete_marker)
            });
            if !removes_markers || !newest.is_delete_marker {
                return true;
            }
            if counted(
                self.storage
                    .delete_object_version(
                        sweep.bucket,
                        &newest.key,
                        newest.id.as_ref(),
                        object_lock::Bypass::None,
                    )
                    .await,
                sweep.summary,
                Kind::Marker,
            ) {
                sweep.summary.expired_versions += 1;
            }
            return self.wait(self.options.delay).await;
        }
        // A noncurrent version became noncurrent when its successor was
        // written — the chain is newest first, so the successor is the entry
        // before it.
        let mut became_noncurrent = newest.last_modified;
        for (rank, entry) in noncurrent.iter().enumerate() {
            if self.stopping() {
                return false;
            }
            let anchor = became_noncurrent;
            became_noncurrent = entry.last_modified;
            // A delete marker leaves through `ExpiredObjectDeleteMarker` alone.
            if entry.is_delete_marker {
                continue;
            }
            // A version's tags are its own, so a tag filter is evaluated per
            // version: one storage read per version per pass, over a walk the
            // page size already bounds. A key's versions never repeat in the
            // walk, so there is no cache to keep. The null version carries
            // none (LC10): it pays no read and a tag filter never matches it,
            // so it is retained.
            let tags = if sweep.reads_tags && entry.id.is_some() {
                match self
                    .storage
                    .version_tags(sweep.bucket, &entry.key, entry.id.as_ref())
                    .await
                {
                    Ok(tags) => tags,
                    // An unreadable version is retained, never over-deleted,
                    // and the next pass retries it.
                    Err(error) => {
                        read_failed(
                            error,
                            sweep.bucket,
                            sweep.summary,
                            "read a version's tags",
                            Kind::Version,
                        );
                        continue;
                    }
                }
            } else {
                object::Tags::empty()
            };
            let facts = lifecycle::Facts::new(&entry.key, entry.size, entry.last_modified, &tags);
            // The plans are per rule, never merged (LC10): a version goes only
            // when one rule's own pair of thresholds is exceeded.
            let due = sweep.config.noncurrent_plans(&facts).any(|plan| {
                due_on_midnight(anchor, plan.days, sweep.now)
                    && plan
                        .keep
                        .is_none_or(|keep| rank >= usize::try_from(keep).unwrap_or(usize::MAX))
            });
            if !due {
                continue;
            }
            if counted(
                self.storage
                    .delete_object_version(
                        sweep.bucket,
                        &entry.key,
                        entry.id.as_ref(),
                        object_lock::Bypass::None,
                    )
                    .await,
                sweep.summary,
                Kind::Version,
            ) {
                sweep.summary.expired_versions += 1;
            }
            if !self.wait(self.options.delay).await {
                return false;
            }
        }
        true
    }

    /// Abort incomplete uploads past their rule's clock, page by page.
    #[instrument(target = "tinio::lifecycle", level = "debug", skip_all)]
    async fn abort_uploads(&self, sweep: &mut Sweep<'_>) -> bool {
        // A size predicate matches the bytes already uploaded. The listing
        // carries none, so a rule that bounds size pays one part walk; a rule
        // that does not keeps the zero the bounds ignore.
        let reads_size = sweep.config.rules.iter().any(|rule| {
            rule.status == lifecycle::Status::Enabled
                && rule.abort_incomplete_multipart_upload.is_some()
                && rule.filter.uses_size()
        });
        let mut cursor: Option<(String, String)> = None;
        loop {
            let page = match self
                .storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket: sweep.bucket.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: cursor.as_ref().map(|(key, _)| key.clone()),
                    upload_id_marker: cursor.as_ref().map(|(_, id)| id.clone()),
                    max_uploads: PAGE,
                })
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    read_failed(
                        error,
                        sweep.bucket,
                        sweep.summary,
                        "list a bucket's incomplete uploads",
                        Kind::Upload,
                    );
                    return true;
                }
            };
            for upload in &page.uploads {
                if self.stopping() {
                    return false;
                }
                let size = if reads_size {
                    match upload_bytes(self, sweep.bucket, upload).await {
                        Ok(size) => size,
                        Err(error) => {
                            read_failed(
                                error,
                                sweep.bucket,
                                sweep.summary,
                                "read an upload's parts",
                                Kind::Upload,
                            );
                            continue;
                        }
                    }
                } else {
                    0
                };
                let facts =
                    lifecycle::Facts::new(&upload.key, size, upload.initiated_at, &upload.tags);
                let Some(days) = sweep.config.abort_after(&facts) else {
                    continue;
                };
                if !due_on_midnight(upload.initiated_at, days, sweep.now) {
                    continue;
                }
                if counted(
                    self.storage
                        .abort_multipart_upload(sweep.bucket, &upload.key, &upload.upload_id)
                        .await,
                    sweep.summary,
                    Kind::Upload,
                ) {
                    sweep.summary.aborted_uploads += 1;
                }
                if !self.wait(self.options.delay).await {
                    return false;
                }
            }
            if !page.truncated {
                return true;
            }
            match (page.next_key_marker, page.next_upload_id_marker) {
                (Some(key), Some(id)) => cursor = Some((key, id)),
                _ => return true,
            }
        }
    }
}

/// One current version a pass moves: the key and the class it lands in.
struct DueTransition {
    key: object::Key,
    class: Class,
}

/// Whether any `Enabled` rule carries a transition — the question the bucket
/// walk asks before paying for the object listing's transition checks (LC21's
/// per-action ownership).
fn governs_transitions(config: &lifecycle::Config) -> bool {
    config
        .rules
        .iter()
        .any(|rule| rule.status == lifecycle::Status::Enabled && !rule.transitions.is_empty())
}

/// The earliest transition any rule selecting `facts` asks for, with the class
/// it moves to, or `None` when no such rule matches.
///
/// The earliest instant wins, as the shortest expiration policy does: the
/// rule's own [`Transition::instant`](lifecycle::Transition::instant) reads
/// the shared clock, so a `Days` count lands on the same midnight an
/// `Expiration.Days` would.
fn transition_for<'a>(
    config: &'a lifecycle::Config,
    facts: &'a lifecycle::Facts<'_>,
) -> Option<(SystemTime, Class)> {
    config
        .rules_for(facts)
        .flat_map(|rule| rule.transitions.iter())
        .filter_map(|transition| {
            transition
                .instant(facts.last_modified)
                .map(|at| (at, transition.storage_class))
        })
        .min_by_key(|(at, _)| *at)
}

/// Whether any rule needs the version listing at all — `NoncurrentVersionExpiration`
/// or `ExpiredObjectDeleteMarker`, the model's two actions on a version chain.
fn governs_versions(config: &lifecycle::Config) -> bool {
    config.governs_noncurrent_version_expiration() || config.governs_expired_object_delete_marker()
}

/// Whether `days` counted from `anchor` have elapsed by `now`.
///
/// A positive count expires at [`midnight_utc`](lifecycle::midnight_utc) of
/// `anchor + days`, the clock `Expiration.Days` already uses. Zero is the
/// count-only form: due at `anchor`, not at the following midnight. A clock
/// that stepped back never expires anything early.
fn due_on_midnight(anchor: SystemTime, days: u32, now: SystemTime) -> bool {
    if days == 0 {
        return now >= anchor;
    }
    now >= lifecycle::midnight_utc(anchor, days)
}

/// The bytes of `upload`'s stored parts. An incomplete upload has no object
/// size of its own; the parts are that size.
async fn upload_bytes<S: Storage>(
    runner: &Runner<S>,
    bucket: &bucket::Name,
    upload: &tinio_core::MultipartUpload,
) -> Result<u64, S::Error> {
    let mut size = 0u64;
    let mut marker = None;
    loop {
        let page = runner
            .storage
            .list_parts(ListPartsParams {
                bucket: bucket.clone(),
                key: upload.key.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: PAGE,
                part_number_marker: marker,
            })
            .await?;
        for part in &page.parts {
            size = size.saturating_add(part.size);
        }
        if !page.truncated {
            return Ok(size);
        }
        let Some(next) = page.next_part_number_marker else {
            return Ok(size);
        };
        marker = Some(next);
    }
}

/// One action's outcome: `true` when it counts as done.
///
/// A deleted item is the outcome the action wanted, so an item that vanished
/// first is counted, not failed — otherwise the pass's accounting would depend
/// on whether a backend's delete happens to be idempotent (S3's are). Either
/// way the action family records one sample under `kind`.
///
/// A permanent delete a stored lock refuses is the third outcome (OL19): a
/// **counted skip** — the summary's [`protected_versions`](Summary::protected_versions)
/// grows and the protected-deletions counter records the operation and reason.
/// It is not an error and does not count as expired.
fn counted<M, E: Into<storage::Error>>(
    outcome: Result<M, E>,
    summary: &mut Summary,
    kind: Kind,
) -> bool {
    match outcome.map_err(Into::into) {
        Ok(_) => {
            metrics::record_action(kind, true);
            true
        }
        Err(error @ storage::Error::Locked(reason)) => {
            tracing::debug!(%error, "a lifecycle action was refused by an object lock");
            // The family is process-global: an exact-value test on the same
            // binary holds `test_lock::window()`, and the writer takes the
            // gate so a parallel test's refusal cannot inflate its delta.
            #[cfg(test)]
            let _gate = test_lock::writer();
            record_protected_deletion(LockOperation::Lifecycle, reason);
            summary.protected_versions += 1;
            false
        }
        Err(error) => {
            if matches!(
                error,
                storage::Error::NoSuchKey(_)
                    | storage::Error::NoSuchVersion(_)
                    | storage::Error::NoSuchUpload(_)
                    | storage::Error::NoSuchBucket(_)
            ) {
                tracing::debug!(%error, "a lifecycle action found its item already gone");
                metrics::record_action(kind, true);
                true
            } else {
                action_failed(error, summary, "act on an item", kind);
                false
            }
        }
    }
}

/// One read's failure: a bucket that vanished mid-pass is gone, not broken
/// (R41) — logged and skipped at every read site, never counted as an error.
/// Everything else is [`action_failed`]'s one warning and one error sample.
fn read_failed<E: Into<storage::Error>>(
    error: E,
    bucket: &bucket::Name,
    summary: &mut Summary,
    action: &str,
    kind: Kind,
) {
    let error: storage::Error = error.into();
    if matches!(error, storage::Error::NoSuchBucket(_)) {
        tracing::debug!(%bucket, %error, "a bucket vanished during the lifecycle pass");
    } else {
        action_failed(error, summary, action, kind);
    }
}

/// A failed action: one warning, one summary error, one `outcome="error"`
/// sample under the action's own `kind`. The pass carries on; the next item
/// or the next cycle retries.
fn action_failed<E: Into<storage::Error>>(
    error: E,
    summary: &mut Summary,
    action: &str,
    kind: Kind,
) {
    let error: storage::Error = error.into();
    tracing::warn!(%error, action, "the lifecycle pass could not continue");
    summary.errors += 1;
    metrics::record_action(kind, false);
}

/// A failure outside one action — a pass-level read, or a seed write. The
/// actions family has no kind to file it under, so only the summary counts it.
fn pass_failed<E: Into<storage::Error>>(error: E, summary: &mut Summary, action: &str) {
    let error: storage::Error = error.into();
    tracing::warn!(%error, action, "the lifecycle pass could not continue");
    summary.errors += 1;
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;
