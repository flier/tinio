//! The pass: one stateless sweep of every bucket's stored rules (design LC4,
//! LC12).
//!
//! Three actions, and each is the API's own deletion — the engine owns no
//! version-aware removal of its own (LC2):
//!
//! - **current versions** — [`ObjectOps::delete_object`](crate::_core::storage::ObjectOps::delete_object):
//!   a delete marker in a versioned bucket, an unlink otherwise;
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

use smart_default::SmartDefault;
use tokio::sync::watch;

use crate::{
    _core::{
        ListBucketsParams, ListObjectsParams, ListPartsParams, ListUploadsParams, bucket,
        lifecycle::{self, DEFAULT_CYCLE, DEFAULT_DELAY},
        object,
        storage::{self, Storage},
        version::{self, CurrentVersion, IdMarker, ListParams, State},
    },
    _util::pacing,
    clock::Clock,
};

/// One page of any listing. The pass walks every page: a bucket larger than
/// one page is the normal case, not an edge.
const PAGE: usize = 1000;

/// How hard the pass works: the pause between two actions and the wait between
/// two passes. Both are the operator's `[lifecycle]` keys.
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
}

/// What one pass did — the five counters of the pass's one summary line (LC16).
///
/// # Examples
///
/// ```rust
/// use tinio_lifecycle::Summary;
///
/// let idle = Summary::default();
/// assert_eq!(idle.expired_objects, 0);
/// assert!(idle.buckets_with_rules == 0);
/// ```
#[derive(Debug, Clone, Copy, SmartDefault, PartialEq, Eq)]
pub struct Summary {
    /// Buckets carrying a stored configuration this pass looked at.
    pub buckets_with_rules: usize,
    /// Current versions expired.
    pub expired_objects: u64,
    /// Versions removed permanently: noncurrent ones, and delete markers that
    /// were their key's only version.
    pub expired_versions: u64,
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
    ///         noncurrent_version_expiration: None,
    ///         abort_incomplete_multipart_upload: None,
    ///     },
    /// }];
    /// let summary = runner.seed(&seeds).await;
    /// assert_eq!(summary.buckets_with_rules, 1, "one bucket took the rule");
    /// # }
    /// ```
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
            // One store call: the read, the upsert and the validation are the
            // store's transaction, so a configuration written between them is
            // not replaced by this snapshot (LC13).
            match self.storage.merge_bucket_lifecycle(bucket, &rules).await {
                Ok(lifecycle::Merged::Stored) => summary.buckets_with_rules += 1,
                Ok(lifecycle::Merged::Absent) => failed(
                    storage::no_such_bucket(bucket),
                    &mut summary,
                    "seed a bucket's configuration",
                ),
                Ok(lifecycle::Merged::Refused(invalid)) => {
                    tracing::warn!(%invalid, "a lifecycle seed was refused");
                    summary.errors += 1;
                }
                Err(error) => failed(error, &mut summary, "seed a bucket's configuration"),
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
    pub async fn pass(&self) -> Summary {
        let mut summary = Summary::default();
        let now = self.clock.now();
        // One scan for every stored configuration, then the bucket walk reads
        // the map. Intersecting with `list_buckets` is LC19's rule: a rule row
        // whose bucket vanished out of band is never looked up, so it cannot
        // expire a re-created bucket's objects.
        let mut configs: HashMap<_, _> = match self.storage.lifecycles().await {
            Ok(configs) => configs.into_iter().collect(),
            Err(error) => {
                failed(error, &mut summary, "read the lifecycle rules");
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
                        failed(error, &mut summary, "list buckets");
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
        tracing::info!(
            buckets_with_rules = summary.buckets_with_rules,
            expired_objects = summary.expired_objects,
            expired_versions = summary.expired_versions,
            aborted_uploads = summary.aborted_uploads,
            errors = summary.errors,
            "lifecycle pass"
        );
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
    pub async fn run(&self) {
        loop {
            if self.stopping() {
                return;
            }
            self.pass().await;
            if !self.wait(self.options.cycle).await {
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

    /// One bucket's three actions. `false` once the run is stopping.
    async fn expire(&self, sweep: &mut Sweep<'_>) -> bool {
        // A bucket whose rules carry no `Days`/`Date` expiration pays nothing
        // for the object listing (LC21's per-action ownership, read from the
        // model's own predicate).
        if sweep.config.governs_expiration() && !self.expire_objects(sweep).await {
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

    /// Expire current versions, page by page.
    async fn expire_objects(&self, sweep: &mut Sweep<'_>) -> bool {
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
                let Some(expiry) = sweep.config.expiry_for(&facts) else {
                    continue;
                };
                if expiry > sweep.now {
                    continue;
                }
                match self.due(sweep.bucket, &info.key).await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(error) => {
                        read_failed(
                            error,
                            sweep.bucket,
                            sweep.summary,
                            "read a due key's current version",
                        );
                        continue;
                    }
                }
                if counted(
                    self.storage.delete_object(sweep.bucket, &info.key).await,
                    sweep.summary,
                ) {
                    sweep.summary.expired_objects += 1;
                }
                if !self.wait(self.options.delay).await {
                    return false;
                }
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
                    .delete_object_version(sweep.bucket, &newest.key, newest.id.as_ref())
                    .await,
                sweep.summary,
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
                        read_failed(error, sweep.bucket, sweep.summary, "read a version's tags");
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
                    .delete_object_version(sweep.bucket, &entry.key, entry.id.as_ref())
                    .await,
                sweep.summary,
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
/// on whether a backend's delete happens to be idempotent (S3's are).
fn counted<M, E: Into<storage::Error>>(outcome: Result<M, E>, summary: &mut Summary) -> bool {
    match outcome.map_err(Into::into) {
        Ok(_) => true,
        Err(error) => {
            if matches!(
                error,
                storage::Error::NoSuchKey(_)
                    | storage::Error::NoSuchVersion(_)
                    | storage::Error::NoSuchUpload(_)
                    | storage::Error::NoSuchBucket(_)
            ) {
                tracing::debug!(%error, "a lifecycle action found its item already gone");
                true
            } else {
                failed(error, summary, "act on an item");
                false
            }
        }
    }
}

/// One read's failure: a bucket that vanished mid-pass is gone, not broken
/// (R41) — logged and skipped at every read site, never counted as an error.
/// Everything else is [`failed`]'s one warning and one counter.
fn read_failed<E: Into<storage::Error>>(
    error: E,
    bucket: &bucket::Name,
    summary: &mut Summary,
    action: &str,
) {
    let error: storage::Error = error.into();
    if matches!(error, storage::Error::NoSuchBucket(_)) {
        tracing::debug!(%bucket, %error, "a bucket vanished during the lifecycle pass");
    } else {
        failed(error, summary, action);
    }
}

/// A failed read or write: one warning, one counter.
fn failed<E: Into<storage::Error>>(error: E, summary: &mut Summary, action: &str) {
    let error: storage::Error = error.into();
    tracing::warn!(%error, action, "the lifecycle pass could not continue");
    summary.errors += 1;
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;
