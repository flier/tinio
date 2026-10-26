//! The tier index: placement, warm candidates, heat and the root identity.
//!
//! The index is the tier layer's own redb database at
//! `<root>/.tinio/tier.redb`. It is the placement authority (T2, T19): the
//! [`placement`](Index::placement) table holds one [`Placement`] per
//! `(bucket, key)`, carrying the [`ColdInfo`] a pg-placed object is served
//! from; the [`warm`](Index::scan_warm) table holds the capacity-demotion
//! candidates keyed `(mtime, bucket, key)` so an ascending scan is
//! oldest-first; the [`heat`](Index::scan_heat) table holds the read counts
//! the promotion queue reads; the [`meta`](Index::root_id) table holds the
//! format version and the root identity.
//!
//! Construction is fail-closed (T17): the file is created owner-only on
//! Unix behind a format gate, and the index compacts while the database is
//! still exclusively owned when its measured fragmentation reaches the
//! configured `index_compact_percent`. Placement writes commit with
//! [`Durability::Immediate`], so a placement is on disk when the call
//! returns.
//!
//! A read-only index (T27) serves every read and refuses every write with
//! [`Error::ReadOnly`].

use std::{
    collections::{BTreeMap, HashMap},
    fs, mem,
    ops::Bound,
    path::Path,
    sync::{Mutex, PoisonError},
    time::SystemTime,
};

use redb::{
    Database, Durability, ReadOnlyDatabase, ReadTransaction, ReadableDatabase as _,
    ReadableTable as _, TableDefinition, WriteTransaction, backends::InMemoryBackend,
};
use serde::{Deserialize, Serialize};
use tinio_fs::database::{Stats, needs_compact};

use crate::{
    _core::{
        ETag, bucket, checksum,
        meta::bucket::{decode_acl, decode_owner, encode_grants, encode_owner},
        object, storage, version,
    },
    Error,
};

/// The format version this build writes. A database recording any other
/// version is refused at open; it is never migrated.
///
/// # Examples
///
/// ```rust
/// use tinio_tier::FORMAT_VERSION;
///
/// assert_eq!(FORMAT_VERSION, 1);
/// ```
pub const FORMAT_VERSION: u64 = 1;

/// The `meta` key holding the format version.
const FORMAT_KEY: &str = "format";
/// The `meta` key holding the root identity.
const ROOT_ID_KEY: &str = "root_id";

/// `(bucket, key)` → the JSON-encoded [`Placement`].
const PLACEMENT: TableDefinition<'static, (&'static str, &'static str), &'static [u8]> =
    TableDefinition::new("placement");
/// `(mtime nanos, bucket, key)` → no payload: membership is the candidate.
const WARM: TableDefinition<'static, (u64, &'static str, &'static str), ()> =
    TableDefinition::new("warm");
/// `(bucket, key)` → the candidate's mtime nanos: the reverse map a
/// replacement uses to retire the row it supersedes.
const WARM_KEY: TableDefinition<'static, (&'static str, &'static str), u64> =
    TableDefinition::new("warm_key");
/// `(bucket, key)` → the JSON-encoded [`HeatRow`].
const HEAT: TableDefinition<'static, (&'static str, &'static str), &'static [u8]> =
    TableDefinition::new("heat");
/// The most heat rows one flush transaction absorbs, so a large drain never
/// holds one write transaction for the whole batch.
const HEAT_FLUSH_CHUNK: usize = 512;
/// The index metadata keys (format version and root identity).
const META: TableDefinition<'static, &'static str, &'static [u8]> = TableDefinition::new("meta");

/// Which content plane a placement names.
///
/// `Fs` is the warm tier (the object has a file); `Pg` is the cold tier
/// (the object's bytes live in PostgreSQL and its metadata is the
/// placement's [`ColdInfo`]).
///
/// # Examples
///
/// ```rust
/// use tinio_tier::Tier;
///
/// assert_ne!(Tier::Fs, Tier::Pg);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// The warm filesystem plane.
    Fs,
    /// The cold PostgreSQL plane.
    Pg,
}

/// The lifecycle state of one placement.
///
/// A cold write path records [`State::Copying`] before it publishes to
/// PostgreSQL, so garbage collection skips in-flight content (T12); a
/// delete records [`State::Deleting`] as its tombstone (T13).
///
/// # Examples
///
/// ```rust
/// use tinio_tier::State;
///
/// assert_ne!(State::Live, State::Copying);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    /// The placement names the object's settled location.
    Live,
    /// A demotion is copying the object into the cold plane.
    Copying,
    /// A delete tombstone; garbage collection finishes it after the grace.
    Deleting,
}

impl State {
    /// The state's lowercase wire spelling, as the index persists it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::State;
    ///
    /// assert_eq!(State::Live.as_str(), "live");
    /// assert_eq!(State::Copying.as_str(), "copying");
    /// assert_eq!(State::Deleting.as_str(), "deleting");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Copying => "copying",
            Self::Deleting => "deleting",
        }
    }
}

/// The cold metadata authority for a pg-placed object (T2).
///
/// The fields are the filesystem row's wire elements, so restoring a warm
/// row from a cold object reproduces the row exactly: the ETag wire, the
/// canonical tags wire, the recorded checksum wire, the owner canonical
/// ID, the canonical ACL grants wire, the version identity wire, and the
/// version-recording flag. The ACL's owner rides [`ColdInfo::owner`]
/// (`object::Info` keeps the ACL owner element empty on a read, exactly
/// as the fs row does).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     ETag, acl,
///     object::{self, Info, Tags},
/// };
/// use tinio_tier::ColdInfo;
///
/// let info = Info {
///     key: object::key("dir/a.txt").expect("a valid object key"),
///     size: 4,
///     last_modified: std::time::SystemTime::UNIX_EPOCH,
///     etag: "d41d8cd98f00b204e9800998ecf8427e"
///         .parse::<ETag>()
///         .expect("a valid etag"),
///     tags: Tags::empty(),
///     checksum: None,
///     owner: None,
///     acl: acl::Acl::default_private(None),
///     version_id: None,
///     bucket_records_versions: false,
/// };
///
/// let cold = ColdInfo::from_info(&info);
/// assert_eq!(cold.etag, "d41d8cd98f00b204e9800998ecf8427e");
/// assert_eq!(cold.to_info().expect("the info decodes"), info);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdInfo {
    /// The object key.
    pub key: String,
    /// The object size in bytes.
    pub size: u64,
    /// The object's last modification time.
    #[serde(with = "time_nanos")]
    pub last_modified: SystemTime,
    /// The ETag wire form (content MD5 hex, or the composed multipart
    /// `"<md5hex>-N"` form).
    pub etag: String,
    /// The canonical tags wire (empty when the object has no tags).
    pub tags: String,
    /// The recorded checksum wire (empty when the object has none).
    pub checksum: String,
    /// The owner's canonical ID wire (empty when none was recorded).
    pub owner: String,
    /// The canonical ACL grants wire (the ACL owner is [`ColdInfo::owner`]).
    pub acl: String,
    /// The version identity wire; the literal `null` names the null version.
    pub version_id: String,
    /// Whether the bucket records versions (the version-header rule).
    pub bucket_records_versions: bool,
}

impl ColdInfo {
    /// Capture a filesystem row's metadata as the cold authority.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{
    ///     ETag, acl,
    ///     object::{self, Info, Tags},
    /// };
    /// use tinio_tier::ColdInfo;
    ///
    /// let info = Info {
    ///     key: object::key("a.txt").expect("a valid object key"),
    ///     size: 1,
    ///     last_modified: std::time::SystemTime::UNIX_EPOCH,
    ///     etag: "d41d8cd98f00b204e9800998ecf8427e"
    ///         .parse::<ETag>()
    ///         .expect("a valid etag"),
    ///     tags: Tags::empty(),
    ///     checksum: None,
    ///     owner: None,
    ///     acl: acl::Acl::default_private(None),
    ///     version_id: None,
    ///     bucket_records_versions: false,
    /// };
    /// assert_eq!(ColdInfo::from_info(&info).size, 1);
    /// ```
    pub fn from_info(info: &object::Info) -> Self {
        Self {
            key: info.key.as_ref().to_owned(),
            size: info.size,
            last_modified: info.last_modified,
            etag: info.etag.to_string(),
            tags: info.tags.to_wire(),
            checksum: info
                .checksum
                .as_ref()
                .map(checksum::Recorded::to_wire)
                .unwrap_or_default(),
            owner: encode_owner(info.owner.as_ref()),
            acl: encode_grants(&info.acl),
            version_id: version::wire_of(info.version_id.as_ref()),
            bucket_records_versions: info.bucket_records_versions,
        }
    }

    /// Restore the filesystem row's metadata from the cold authority.
    ///
    /// # Errors
    ///
    /// [`storage::Error::InvalidKey`] for a key the domain rejects and
    /// [`storage::Error::InvalidETag`] for an unparsable ETag; the tags,
    /// checksum, owner, ACL and version elements self-heal exactly as the
    /// filesystem row's own decode does.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_tier::ColdInfo;
    ///
    /// let cold = ColdInfo {
    ///     key: "a.txt".to_owned(),
    ///     size: 2,
    ///     last_modified: SystemTime::UNIX_EPOCH,
    ///     etag: "d41d8cd98f00b204e9800998ecf8427e".to_owned(),
    ///     tags: String::new(),
    ///     checksum: String::new(),
    ///     owner: String::new(),
    ///     acl: String::new(),
    ///     version_id: "null".to_owned(),
    ///     bucket_records_versions: false,
    /// };
    /// let info = cold.to_info().expect("the info decodes");
    /// assert_eq!(&*info.key, "a.txt");
    /// assert_eq!(info.size, 2);
    /// ```
    pub fn to_info(&self) -> Result<object::Info, Error> {
        Ok(object::Info {
            key: object::key(self.key.clone())?,
            size: self.size,
            last_modified: self.last_modified,
            etag: self.etag.parse::<ETag>().map_err(storage::Error::from)?,
            tags: object::Tags::from_wire_limited(&self.tags, object::OBJECT_TAGS_MAX),
            checksum: checksum::Recorded::from_wire_opt(&self.checksum),
            owner: decode_owner(&self.owner),
            acl: decode_acl(&self.acl),
            version_id: version::Id::from_wire(&self.version_id),
            bucket_records_versions: self.bucket_records_versions,
        })
    }
}

/// One object's placement in the index.
///
/// The [`tier`](Placement::tier) names the settled plane, the
/// [`state`](Placement::state) its lifecycle, the
/// [`cold`](Placement::cold) authority the cold metadata (present on a
/// pg-placed object), and the [`mtime`](Placement::mtime) the placement's
/// recorded time — the state's start for a transient state, the move's
/// completion for a settled cold placement, and the object's own modification
/// time for a warm placement.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_tier::{Placement, State, Tier};
///
/// let placement = Placement {
///     tier: Tier::Fs,
///     state: State::Live,
///     cold: None,
///     mtime: SystemTime::UNIX_EPOCH,
/// };
/// assert_eq!(placement.tier, Tier::Fs);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    /// The plane the object's bytes live in.
    pub tier: Tier,
    /// The placement's lifecycle state.
    pub state: State,
    /// The cold metadata authority; `Some` for a pg-placed object.
    pub cold: Option<ColdInfo>,
    /// The placement's recorded time. A `copying` or `deleting` state records
    /// the instant the state began (the grace base the collection pass
    /// measures); a settled `pg` placement records the move's completion time;
    /// a warm (`fs`) placement records the object's modification time — the
    /// warm candidate's ordering key.
    #[serde(with = "time_nanos")]
    pub mtime: SystemTime,
}

impl Placement {
    /// Whether the placement is a settled cold object: the `pg` tier in its
    /// live state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_tier::{Placement, State, Tier};
    ///
    /// let placement = Placement {
    ///     tier: Tier::Pg,
    ///     state: State::Live,
    ///     cold: None,
    ///     mtime: SystemTime::UNIX_EPOCH,
    /// };
    /// assert!(placement.is_cold_live());
    /// ```
    pub fn is_cold_live(&self) -> bool {
        self.tier == Tier::Pg && self.state == State::Live
    }

    /// Whether the placement is a settled warm object: the `fs` tier in its
    /// live state.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_tier::{Placement, State, Tier};
    ///
    /// let placement = Placement {
    ///     tier: Tier::Fs,
    ///     state: State::Live,
    ///     cold: None,
    ///     mtime: SystemTime::UNIX_EPOCH,
    /// };
    /// assert!(placement.is_warm_live());
    /// ```
    pub fn is_warm_live(&self) -> bool {
        self.tier == Tier::Fs && self.state == State::Live
    }
}

/// One warm candidate, in the order the warm table stores it.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{bucket, object};
/// use tinio_tier::WarmCandidate;
///
/// let candidate = WarmCandidate {
///     bucket: bucket::name("data").expect("a valid bucket name"),
///     key: object::key("a.txt").expect("a valid object key"),
///     mtime: SystemTime::UNIX_EPOCH,
/// };
/// assert_eq!(&*candidate.key, "a.txt");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarmCandidate {
    /// The candidate's bucket.
    pub bucket: bucket::Name,
    /// The candidate's key.
    pub key: object::Key,
    /// The recorded modification time (the ordering key).
    pub mtime: SystemTime,
}

/// One heat row: the reads recorded for a key and when they happened.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{bucket, object};
/// use tinio_tier::HeatEntry;
///
/// let entry = HeatEntry {
///     bucket: bucket::name("data").expect("a valid bucket name"),
///     key: object::key("a.txt").expect("a valid object key"),
///     reads: 3,
///     last_read: SystemTime::UNIX_EPOCH,
/// };
/// assert_eq!(entry.reads, 3);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeatEntry {
    /// The row's bucket.
    pub bucket: bucket::Name,
    /// The row's key.
    pub key: object::Key,
    /// The accumulated read count.
    pub reads: u64,
    /// When the counted reads last happened; a drain carries the counter's own
    /// read time, not the drain time.
    pub last_read: SystemTime,
}

/// The outcome of one absorbing pass: the leading rows that reached the
/// table, in the drain's order, and the failure that stopped it, if any.
#[derive(Debug)]
pub(crate) struct Absorbed {
    /// How many leading rows were committed; a failure leaves the rest for
    /// the next flush.
    pub(crate) committed: usize,
    /// The failure that stopped the pass, if any.
    pub(crate) error: Option<Error>,
}

/// The persisted heat payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct HeatRow {
    reads: u64,
    last_read: u64,
}

/// The outcome of one heat-expiry scan: the rows it removed and the oldest
/// read time it left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HeatExpiry {
    /// How many rows the scan removed.
    pub(crate) removed: usize,
    /// The oldest `last_read` still stored, or `None` when no row remains.
    /// Adding the caller's retention names the first instant a stored row
    /// can expire.
    pub(crate) oldest: Option<SystemTime>,
}

/// The database arm: writable on the serving path, read-only on the
/// read-only run (T27).
enum Arm {
    Writable(Database),
    ReadOnly(ReadOnlyDatabase),
}

impl Arm {
    fn begin_read(&self) -> Result<ReadTransaction, Error> {
        match self {
            Self::Writable(db) => Ok(db.begin_read()?),
            Self::ReadOnly(db) => Ok(db.begin_read()?),
        }
    }

    fn begin_write(&self) -> Result<WriteTransaction, Error> {
        match self {
            Self::Writable(db) => Ok(db.begin_write()?),
            Self::ReadOnly(_) => Err(Error::ReadOnly),
        }
    }
}

/// The tier index.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{bucket, object};
/// use tinio_tier::{Index, Placement, State, Tier};
///
/// let index = Index::open_in_memory().expect("the index opens");
/// let data = bucket::name("data").expect("a valid bucket name");
/// let key = object::key("a.txt").expect("a valid object key");
/// let placement = Placement {
///     tier: Tier::Fs,
///     state: State::Live,
///     cold: None,
///     mtime: SystemTime::UNIX_EPOCH,
/// };
/// index
///     .set_placement(&data, &key, &placement)
///     .expect("the placement is written");
/// assert_eq!(
///     index.placement(&data, &key).expect("the placement is read"),
///     Some(placement)
/// );
/// ```
pub struct Index {
    db: Arm,
    /// Reads recorded since the last flush, keyed by `(bucket, key)`.
    pending_heat: Mutex<BTreeMap<(String, String), u64>>,
}

impl Index {
    /// Open (or create) the index at `path`.
    ///
    /// The parent directory is created; on Unix the file is created
    /// owner-only (`0600`). The format gate refuses a database recording
    /// another version before any table is served. The database compacts
    /// while this call still exclusively owns it (T17), but only when its
    /// measured fragmentation reaches [`storage::DEFAULT_COMPACT_THRESHOLD_PERCENT`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let dir = tempfile::tempdir().expect("a temp dir");
    /// let index = Index::open(dir.path().join("tier.redb")).expect("the index opens");
    /// assert_eq!(index.root_id().expect("the id is read"), None);
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_with_compact_percent(path, storage::DEFAULT_COMPACT_THRESHOLD_PERCENT)
    }

    /// Open (or create) the index at `path`, compacting it when its
    /// fragmentation reaches `compact_percent`.
    ///
    /// The tier layer passes its configured `index_compact_percent` here, so
    /// the key gates the one compaction the constructor can run: redb needs
    /// exclusive access, which only this not-yet-shared moment provides
    /// (T17). A database below the 64 MiB floor is never compacted — an
    /// indexed rewrite of a small file reclaims nothing.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let dir = tempfile::tempdir().expect("a temp dir");
    /// let index = Index::open_with_compact_percent(dir.path().join("tier.redb"), 30)
    ///     .expect("the index opens");
    /// assert_eq!(index.root_id().expect("the id is read"), None);
    /// ```
    pub fn open_with_compact_percent(
        path: impl AsRef<Path>,
        compact_percent: u8,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        create_parent(path)?;
        prepare_file(path)?;
        let mut db = Database::create(path)?;
        restrict_file(path)?;
        ensure_schema(&mut db, path)?;
        if compact_due(&db, compact_percent)? {
            db.compact()?;
        }
        Ok(Self::writable(db))
    }

    /// Open the existing index at `path` read-only (T27).
    ///
    /// The open creates nothing and compacts nothing; the format gate still
    /// runs, every mutating method answers [`Error::ReadOnly`], and
    /// [`Index::record_read`] is ignored rather than buffered.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let dir = tempfile::tempdir().expect("a temp dir");
    /// let path = dir.path().join("tier.redb");
    /// drop(Index::open(&path).expect("the index is created"));
    /// let index = Index::open_read_only(&path).expect("the index opens read-only");
    /// assert_eq!(index.root_id().expect("the id is read"), None);
    /// ```
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref();
        let db = ReadOnlyDatabase::open(path)?;
        gate_read_only(&db, path)?;
        Ok(Self {
            db: Arm::ReadOnly(db),
            pending_heat: Mutex::new(BTreeMap::new()),
        })
    }

    /// Open a fresh in-memory index — the test and mem-plane construction
    /// path.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// assert_eq!(index.root_id().expect("the id is read"), None);
    /// ```
    pub fn open_in_memory() -> Result<Self, Error> {
        let mut db = Database::builder().create_with_backend(InMemoryBackend::new())?;
        ensure_schema(&mut db, Path::new("<memory>"))?;
        Ok(Self::writable(db))
    }

    fn writable(db: Database) -> Self {
        Self {
            db: Arm::Writable(db),
            pending_heat: Mutex::new(BTreeMap::new()),
        }
    }

    /// The recorded root identity, or `None` on a fresh index.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// index
    ///     .set_root_id(*b"0123456789abcdef")
    ///     .expect("the id is written");
    /// assert_eq!(
    ///     index.root_id().expect("the id is read"),
    ///     Some(*b"0123456789abcdef")
    /// );
    /// ```
    pub fn root_id(&self) -> Result<Option<[u8; 16]>, Error> {
        self.read(|txn| {
            let table = txn.open_table(META)?;
            let Some(guard) = table.get(ROOT_ID_KEY)? else {
                return Ok(None);
            };
            let id = <[u8; 16]>::try_from(guard.value()).map_err(|_| {
                Error::Storage(storage::corrupt("the tier index root id is not 16 bytes"))
            })?;
            Ok(Some(id))
        })
    }

    /// Record the root identity.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// index
    ///     .set_root_id(*b"0123456789abcdef")
    ///     .expect("the id is written");
    /// assert_eq!(
    ///     index.root_id().expect("the id is read"),
    ///     Some(*b"0123456789abcdef")
    /// );
    /// ```
    pub fn set_root_id(&self, id: [u8; 16]) -> Result<(), Error> {
        self.write(|txn| {
            txn.open_table(META)?.insert(ROOT_ID_KEY, id.as_slice())?;
            Ok(())
        })
    }

    /// The placement of `key` in `bucket`, or `None` when the key is warm
    /// and has no index row.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Index, Placement, State, Tier};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .set_placement(
    ///         &data,
    ///         &key,
    ///         &Placement {
    ///             tier: Tier::Pg,
    ///             state: State::Live,
    ///             cold: None,
    ///             mtime: SystemTime::UNIX_EPOCH,
    ///         },
    ///     )
    ///     .expect("the placement is written");
    /// assert_eq!(
    ///     index
    ///         .placement(&data, &key)
    ///         .expect("the placement is read")
    ///         .map(|p| p.tier),
    ///     Some(Tier::Pg)
    /// );
    /// ```
    pub fn placement(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Placement>, Error> {
        self.read(|txn| {
            let table = txn.open_table(PLACEMENT)?;
            let Some(guard) = table.get((bucket.as_ref().as_str(), key.as_ref().as_str()))? else {
                return Ok(None);
            };
            Ok(Some(serde_json::from_slice(guard.value())?))
        })
    }

    /// Upsert `key`'s placement.
    ///
    /// The write commits with [`Durability::Immediate`]: the placement is
    /// durable when this returns.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Index, Placement, State, Tier};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .set_placement(
    ///         &data,
    ///         &key,
    ///         &Placement {
    ///             tier: Tier::Pg,
    ///             state: State::Live,
    ///             cold: None,
    ///             mtime: SystemTime::UNIX_EPOCH,
    ///         },
    ///     )
    ///     .expect("the placement is written");
    /// assert!(
    ///     index
    ///         .placement(&data, &key)
    ///         .expect("the placement is read")
    ///         .is_some()
    /// );
    /// ```
    pub fn set_placement(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
    ) -> Result<(), Error> {
        let bytes = serde_json::to_vec(placement)?;
        self.write(|txn| {
            txn.set_durability(Durability::Immediate)?;
            txn.open_table(PLACEMENT)?.insert(
                (bucket.as_ref().as_str(), key.as_ref().as_str()),
                bytes.as_slice(),
            )?;
            Ok(())
        })
    }

    /// Upsert `key`'s placement and, when `warm` names a time, replace its
    /// warm candidate — both rows in one [`Durability::Immediate`]
    /// transaction.
    ///
    /// This is [`Index::set_placement`] and [`Index::warm_replace`] fused for
    /// the warm reset: the reset's placement and candidate describe one
    /// object, so they land atomically at one fsync rather than two. A
    /// `None` time records the placement alone (a folder marker is never a
    /// capacity candidate).
    pub(crate) fn set_placement_and_warm(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        placement: &Placement,
        warm: Option<SystemTime>,
    ) -> Result<(), Error> {
        let bytes = serde_json::to_vec(placement)?;
        let nanos = warm.map(storage::to_nanos);
        let bucket = bucket.as_ref().as_str();
        let key = key.as_ref().as_str();
        self.write(move |txn| {
            txn.set_durability(Durability::Immediate)?;
            txn.open_table(PLACEMENT)?
                .insert((bucket, key), bytes.as_slice())?;
            if let Some(nanos) = nanos {
                let superseded = txn
                    .open_table(WARM_KEY)?
                    .get((bucket, key))?
                    .map(|guard| guard.value());
                if let Some(superseded) = superseded {
                    txn.open_table(WARM)?.remove((superseded, bucket, key))?;
                }
                txn.open_table(WARM_KEY)?.insert((bucket, key), nanos)?;
                txn.open_table(WARM)?.insert((nanos, bucket, key), ())?;
            }
            Ok(())
        })
    }

    /// Remove `key`'s placement (idempotent). The warm candidate is a
    /// separate row; the write paths that clear a placement remove the
    /// candidate with [`Index::warm_remove`] when they own one.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Index, Placement, State, Tier};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .set_placement(
    ///         &data,
    ///         &key,
    ///         &Placement {
    ///             tier: Tier::Fs,
    ///             state: State::Live,
    ///             cold: None,
    ///             mtime: SystemTime::UNIX_EPOCH,
    ///         },
    ///     )
    ///     .expect("the placement is written");
    /// index.clear(&data, &key).expect("the placement is cleared");
    /// assert_eq!(
    ///     index.placement(&data, &key).expect("the placement is read"),
    ///     None
    /// );
    /// ```
    pub fn clear(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        self.write(|txn| {
            txn.set_durability(Durability::Immediate)?;
            txn.open_table(PLACEMENT)?
                .remove((bucket.as_ref().as_str(), key.as_ref().as_str()))?;
            Ok(())
        })
    }

    /// The placements of `bucket` in key order, starting at `prefix`, after
    /// `after` (exclusive) when given, at most `limit` rows.
    ///
    /// The scan is a keyed range scan: it touches only the requested
    /// prefix and never walks another bucket.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Index, Placement, State, Tier};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let placement = Placement {
    ///     tier: Tier::Fs,
    ///     state: State::Live,
    ///     cold: None,
    ///     mtime: SystemTime::UNIX_EPOCH,
    /// };
    /// for name in ["docs/a.txt", "docs/b.txt", "images/c.png"] {
    ///     index
    ///         .set_placement(&data, &object::key(name).expect("a valid key"), &placement)
    ///         .expect("the placement is written");
    /// }
    /// let page = index
    ///     .scan_placement(&data, "docs/", None, 10)
    ///     .expect("the scan runs");
    /// assert_eq!(page.len(), 2);
    /// ```
    pub fn scan_placement(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(object::Key, Placement)>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.read(|txn| {
            let table = txn.open_table(PLACEMENT)?;
            let bucket = bucket.as_ref().as_str();
            let start = match after {
                Some(after) if after >= prefix => Bound::Excluded((bucket, after)),
                _ => Bound::Included((bucket, prefix)),
            };
            let mut page = Vec::new();
            for entry in table.range::<(&str, &str)>((start, Bound::Unbounded))? {
                let (key, value) = entry?;
                let (entry_bucket, entry_key) = key.value();
                if entry_bucket != bucket || !entry_key.starts_with(prefix) {
                    break;
                }
                page.push((
                    object::key(entry_key.to_owned())?,
                    serde_json::from_slice(value.value())?,
                ));
                if page.len() == limit {
                    break;
                }
            }
            Ok(page)
        })
    }

    /// Record `key` as a capacity-demotion candidate at `mtime`, replacing any
    /// candidate already recorded for the key.
    ///
    /// The warm table is keyed `(mtime, bucket, key)`, so
    /// [`Index::scan_warm`] answers oldest-first. This is the one write that
    /// establishes a candidate: it delegates to [`Index::warm_replace`], so no
    /// path can leave a superseded row behind.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index
    ///     .warm_insert(
    ///         &data,
    ///         &object::key("a.txt").expect("a key"),
    ///         SystemTime::UNIX_EPOCH,
    ///     )
    ///     .expect("the candidate is inserted");
    /// assert_eq!(index.scan_warm(10).expect("the scan runs").len(), 1);
    /// ```
    pub fn warm_insert(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        self.warm_replace(bucket, key, mtime)
    }

    /// Record `key` as a capacity-demotion candidate at `mtime`, retiring the
    /// row a previous recorded time left behind.
    ///
    /// The warm table is keyed `(mtime, bucket, key)`, so a rewrite at a new
    /// modification time would otherwise leave the superseded row in place —
    /// and because it sorts first, the rewritten object would be demoted at
    /// its *old* age. This is the replace every warm write performs; the
    /// reverse map makes the retirement a point lookup, not a table scan.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .warm_insert(&data, &key, SystemTime::UNIX_EPOCH)
    ///     .expect("the first candidate is inserted");
    /// let later = SystemTime::UNIX_EPOCH + Duration::from_secs(60);
    /// index
    ///     .warm_replace(&data, &key, later)
    ///     .expect("the candidate is replaced");
    /// let warm = index.scan_warm(10).expect("the scan runs");
    /// assert_eq!(warm.len(), 1);
    /// assert_eq!(warm[0].mtime, later);
    /// ```
    pub fn warm_replace(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        let nanos = storage::to_nanos(mtime);
        let bucket = bucket.as_ref().as_str();
        let key = key.as_ref().as_str();
        self.write(move |txn| {
            let mut warm = txn.open_table(WARM)?;
            let mut reverse = txn.open_table(WARM_KEY)?;
            write_warm_row(&mut warm, &mut reverse, bucket, key, nanos)
        })
    }

    /// Record each `(key, mtime)` in `rows` as a capacity candidate, skipping
    /// a key whose placement the warm plane does not own.
    ///
    /// The post-bind rebuild pages the object rows; this is its one
    /// transaction per page, replacing one placement read plus one
    /// `warm_replace` commit per object. A key with no placement is a warm
    /// object and becomes a candidate; a key whose placement is not `fs`/live
    /// is skipped, exactly as the per-object rebuild skipped it. A folder
    /// marker must not be passed — it is a directory, never a candidate.
    pub(crate) fn warm_rebuild_page(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, SystemTime)],
    ) -> Result<(), Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let bucket = bucket.as_ref().as_str();
        self.write(move |txn| {
            let placements = txn.open_table(PLACEMENT)?;
            let mut warm = txn.open_table(WARM)?;
            let mut reverse = txn.open_table(WARM_KEY)?;
            for (key, mtime) in rows {
                let key = key.as_ref().as_str();
                if let Some(placement) = placements.get((bucket, key))? {
                    let placement: Placement = serde_json::from_slice(placement.value())?;
                    if !placement.is_warm_live() {
                        continue;
                    }
                }
                let nanos = storage::to_nanos(*mtime);
                write_warm_row(&mut warm, &mut reverse, bucket, key, nanos)?;
            }
            Ok(())
        })
    }

    /// Remove `key`'s candidate recorded at `mtime` (idempotent).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a key");
    /// index
    ///     .warm_insert(&data, &key, SystemTime::UNIX_EPOCH)
    ///     .expect("the candidate is inserted");
    /// index
    ///     .warm_remove(&data, &key, SystemTime::UNIX_EPOCH)
    ///     .expect("the candidate is removed");
    /// assert!(index.scan_warm(10).expect("the scan runs").is_empty());
    /// ```
    pub fn warm_remove(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        mtime: SystemTime,
    ) -> Result<(), Error> {
        let nanos = storage::to_nanos(mtime);
        let bucket = bucket.as_ref().as_str();
        let key = key.as_ref().as_str();
        self.write(move |txn| {
            txn.open_table(WARM)?.remove((nanos, bucket, key))?;
            let mut reverse = txn.open_table(WARM_KEY)?;
            let recorded = reverse.get((bucket, key))?.map(|guard| guard.value());
            if recorded == Some(nanos) {
                reverse.remove((bucket, key))?;
            }
            Ok(())
        })
    }

    /// The warm candidates oldest-first, at most `limit` rows.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index
    ///     .warm_insert(
    ///         &data,
    ///         &object::key("a.txt").expect("a key"),
    ///         SystemTime::UNIX_EPOCH,
    ///     )
    ///     .expect("a candidate is inserted");
    /// assert_eq!(
    ///     index.scan_warm(1).expect("the scan runs")[0]
    ///         .key
    ///         .as_ref()
    ///         .as_str(),
    ///     "a.txt"
    /// );
    /// ```
    pub fn scan_warm(&self, limit: usize) -> Result<Vec<WarmCandidate>, Error> {
        self.scan_warm_after(None, limit)
    }

    /// The warm candidates oldest-first that sort strictly after `after`, at
    /// most `limit` rows.
    ///
    /// `after` is the last row an earlier page answered, so a pass resumes
    /// past that `(mtime, bucket, key)` position instead of re-reading the
    /// table's oldest rows for every page. A row removed since the last page
    /// leaves the cursor valid: the bound names a position, not a row.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let a = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .warm_insert(&data, &a, SystemTime::UNIX_EPOCH)
    ///     .expect("the candidate is inserted");
    /// let first = index
    ///     .scan_warm(1)
    ///     .expect("the scan runs")
    ///     .pop()
    ///     .expect("a row");
    /// let page = index
    ///     .scan_warm_after(Some(&first), 10)
    ///     .expect("the resumed scan runs");
    /// assert!(page.is_empty(), "the only row was already answered");
    /// ```
    pub fn scan_warm_after(
        &self,
        after: Option<&WarmCandidate>,
        limit: usize,
    ) -> Result<Vec<WarmCandidate>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.read(|txn| {
            let table = txn.open_table(WARM)?;
            let start = match after {
                Some(candidate) => Bound::Excluded((
                    storage::to_nanos(candidate.mtime),
                    candidate.bucket.as_ref().as_str(),
                    candidate.key.as_ref().as_str(),
                )),
                None => Bound::Included((0u64, "", "")),
            };
            let mut page = Vec::new();
            for entry in table.range::<(u64, &str, &str)>((start, Bound::Unbounded))? {
                let (key, _) = entry?;
                let (mtime, bucket, key) = key.value();
                page.push(WarmCandidate {
                    bucket: bucket::name(bucket.to_owned())?,
                    key: object::key(key.to_owned())?,
                    mtime: storage::from_nanos(mtime),
                });
                if page.len() == limit {
                    break;
                }
            }
            Ok(page)
        })
    }

    /// Count one read of `key`.
    ///
    /// The count is buffered in memory and reaches the heat table on
    /// [`Index::flush_heat`], so a GET never pays a durable write (T9).
    /// A read-only index ignores the read rather than buffering it (T27).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a key");
    /// index.record_read(&data, &key);
    /// index.record_read(&data, &key);
    /// index.flush_heat().expect("the heat row is flushed");
    /// assert_eq!(index.scan_heat(10).expect("the scan runs")[0].reads, 2);
    /// ```
    pub fn record_read(&self, bucket: &bucket::Name, key: &object::Key) {
        if matches!(self.db, Arm::ReadOnly(_)) {
            return;
        }
        let mut pending = self
            .pending_heat
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *pending
            .entry((bucket.as_ref().to_owned(), key.as_ref().to_owned()))
            .or_default() += 1;
    }

    /// Persist the reads buffered since the last flush, merging them into
    /// the stored counts and stamping `last_read` with the flush time.
    ///
    /// A read-only index refuses the write.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index.record_read(&data, &object::key("a.txt").expect("a key"));
    /// index.flush_heat().expect("the heat row is flushed");
    /// assert_eq!(index.scan_heat(10).expect("the scan runs").len(), 1);
    /// ```
    pub fn flush_heat(&self) -> Result<(), Error> {
        if matches!(self.db, Arm::ReadOnly(_)) {
            return Err(Error::ReadOnly);
        }
        let pending = {
            let mut pending = self
                .pending_heat
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            mem::take(&mut *pending)
        };
        if pending.is_empty() {
            return Ok(());
        }
        let now = storage::to_nanos(SystemTime::now());
        self.write(move |txn| {
            let mut table = txn.open_table(HEAT)?;
            for ((bucket, key), reads) in pending {
                let stored = table
                    .get((bucket.as_str(), key.as_str()))?
                    .map(|guard| serde_json::from_slice::<HeatRow>(guard.value()))
                    .transpose()?;
                let row = HeatRow {
                    reads: stored.map_or(0, |row| row.reads).saturating_add(reads),
                    last_read: now,
                };
                let bytes = serde_json::to_vec(&row)?;
                table.insert((bucket.as_str(), key.as_str()), bytes.as_slice())?;
            }
            Ok(())
        })
    }

    /// Merge externally drained heat counters into the durable heat table.
    ///
    /// [`Heat::drain`](crate::Heat::drain) answers the rows the in-memory
    /// counters accumulated; this is the write that stores them, stamping
    /// each row with the read time it carries. The stored count is the sum,
    /// so a chunked flush never loses the earlier chunks. The merge runs in
    /// transactions of at most `HEAT_FLUSH_CHUNK` rows, so one large drain
    /// never holds a single write transaction for the whole batch.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{HeatEntry, Index};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index
    ///     .absorb_heat(&[HeatEntry {
    ///         bucket: data.clone(),
    ///         key: object::key("a.txt").expect("a key"),
    ///         reads: 3,
    ///         last_read: SystemTime::now(),
    ///     }])
    ///     .expect("the rows are stored");
    /// assert_eq!(index.scan_heat(10).expect("the scan runs")[0].reads, 3);
    /// ```
    pub fn absorb_heat(&self, rows: &[HeatEntry]) -> Result<(), Error> {
        // The public merge has no window: every row adds to what is stored.
        self.absorb_heat_report(rows, SystemTime::UNIX_EPOCH)
            .error
            .map_or(Ok(()), Err)
    }

    /// Merge `rows` in chunks, answering how many leading rows committed and
    /// the failure that stopped the pass.
    ///
    /// Rows are absorbed in order, so a failure's
    /// [`Absorbed::committed`] count names exactly `rows[..committed]`: the
    /// caller can forget only what reached the table and retry the rest.
    ///
    /// A stored row whose `last_read` is older than `stale_before` starts a
    /// fresh window — its delta replaces the stored count instead of adding to
    /// it — so old reads kept alive by a longer idle retention cannot combine
    /// with a single fresh one.
    pub(crate) fn absorb_heat_report(
        &self,
        rows: &[HeatEntry],
        stale_before: SystemTime,
    ) -> Absorbed {
        if rows.is_empty() {
            return Absorbed {
                committed: 0,
                error: None,
            };
        }
        if matches!(self.db, Arm::ReadOnly(_)) {
            return Absorbed {
                committed: 0,
                error: Some(Error::ReadOnly),
            };
        }
        let stale_before = storage::to_nanos(stale_before);
        let mut committed = 0;
        for chunk in rows.chunks(HEAT_FLUSH_CHUNK) {
            let batch = chunk.to_vec();
            let cutoff = stale_before;
            let written = self.write(move |txn| {
                let mut table = txn.open_table(HEAT)?;
                for row in batch {
                    let bucket = row.bucket.as_ref().as_str();
                    let key = row.key.as_ref().as_str();
                    let stored = table
                        .get((bucket, key))?
                        .map(|guard| serde_json::from_slice::<HeatRow>(guard.value()))
                        .transpose()?;
                    let merged = match stored {
                        Some(stored) if stored.last_read >= cutoff => HeatRow {
                            reads: stored.reads.saturating_add(row.reads),
                            last_read: storage::to_nanos(row.last_read),
                        },
                        // No row, or one that left the window: the delta
                        // starts a fresh count rather than reviving an old
                        // total.
                        _ => HeatRow {
                            reads: row.reads,
                            last_read: storage::to_nanos(row.last_read),
                        },
                    };
                    let bytes = serde_json::to_vec(&merged)?;
                    table.insert((bucket, key), bytes.as_slice())?;
                }
                Ok(())
            });
            match written {
                Ok(()) => committed += chunk.len(),
                Err(err) => {
                    return Absorbed {
                        committed,
                        error: Some(err),
                    };
                }
            }
        }
        Absorbed {
            committed: rows.len(),
            error: None,
        }
    }

    /// The stored heat rows in `(bucket, key)` order, at most `limit` rows.
    ///
    /// Only flushed rows are visible; reads still buffered by
    /// [`Index::record_read`] wait for [`Index::flush_heat`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index.record_read(&data, &object::key("a.txt").expect("a key"));
    /// index.flush_heat().expect("the heat row is flushed");
    /// let rows = index.scan_heat(10).expect("the scan runs");
    /// assert_eq!(rows[0].reads, 1);
    /// ```
    pub fn scan_heat(&self, limit: usize) -> Result<Vec<HeatEntry>, Error> {
        self.scan_heat_after(None, limit)
    }

    /// The stored heat rows in `(bucket, key)` order that sort strictly after
    /// `after`, at most `limit` rows.
    ///
    /// `after` is the last row an earlier page answered, so a restore pages
    /// the table without re-reading its first rows.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let a = object::key("a.txt").expect("a valid object key");
    /// index.record_read(&data, &a);
    /// index.flush_heat().expect("the heat row is flushed");
    /// let first = index
    ///     .scan_heat(1)
    ///     .expect("the scan runs")
    ///     .pop()
    ///     .expect("a row");
    /// let page = index
    ///     .scan_heat_after(Some((&first.bucket, &first.key)), 10)
    ///     .expect("the resumed scan runs");
    /// assert!(page.is_empty(), "the only row was already answered");
    /// ```
    pub fn scan_heat_after(
        &self,
        after: Option<(&bucket::Name, &object::Key)>,
        limit: usize,
    ) -> Result<Vec<HeatEntry>, Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.read(|txn| {
            let table = txn.open_table(HEAT)?;
            let start = match after {
                Some((bucket, key)) => {
                    Bound::Excluded((bucket.as_ref().as_str(), key.as_ref().as_str()))
                }
                None => Bound::Unbounded,
            };
            let mut page = Vec::new();
            for entry in table.range::<(&str, &str)>((start, Bound::Unbounded))? {
                let (key, value) = entry?;
                let (bucket, key) = key.value();
                let row: HeatRow = serde_json::from_slice(value.value())?;
                page.push(HeatEntry {
                    bucket: bucket::name(bucket.to_owned())?,
                    key: object::key(key.to_owned())?,
                    reads: row.reads,
                    last_read: storage::from_nanos(row.last_read),
                });
                if page.len() == limit {
                    break;
                }
            }
            Ok(page)
        })
    }

    /// When `key`'s durable heat row was last read, or `None` when the index
    /// holds no row for it.
    ///
    /// The row is the flushed half of the heat evidence; the in-memory
    /// counters answer [`Heat::last_read`](crate::Heat::last_read) for reads
    /// that have not reached disk yet.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{HeatEntry, Index};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a key");
    /// assert_eq!(
    ///     index.heat_last_read(&data, &key).expect("the lookup runs"),
    ///     None
    /// );
    /// let now = SystemTime::now();
    /// index
    ///     .absorb_heat(&[HeatEntry {
    ///         bucket: data.clone(),
    ///         key: key.clone(),
    ///         reads: 2,
    ///         last_read: now,
    ///     }])
    ///     .expect("the row is stored");
    /// assert_eq!(
    ///     index.heat_last_read(&data, &key).expect("the row reads"),
    ///     Some(now)
    /// );
    /// ```
    pub fn heat_last_read(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<SystemTime>, Error> {
        self.read(|txn| {
            let stored = txn
                .open_table(HEAT)?
                .get((bucket.as_ref().as_str(), key.as_ref().as_str()))?
                .map(|guard| serde_json::from_slice::<HeatRow>(guard.value()))
                .transpose()?;
            Ok(stored.map(|row| storage::from_nanos(row.last_read)))
        })
    }

    /// Drop `key`'s durable heat row, if one is stored.
    ///
    /// A mutation calls this so an object's reads do not outlive the bytes they
    /// described: an overwrite, a delete, a copy destination or a completed
    /// multipart upload leaves no evidence behind (T9). A read-only index
    /// refuses the write.
    pub(crate) fn remove_heat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        let bucket = bucket.as_ref().to_owned();
        let key = key.as_ref().to_owned();
        self.write(move |txn| {
            txn.open_table(HEAT)?
                .remove((bucket.as_str(), key.as_str()))?;
            Ok(())
        })
    }

    /// Drop every heat row flushed before `cutoff`, answering the number
    /// removed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::Index;
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// index.record_read(&data, &object::key("a.txt").expect("a key"));
    /// index.flush_heat().expect("the heat row is flushed");
    /// assert_eq!(
    ///     index
    ///         .expire_heat(SystemTime::now())
    ///         .expect("the row expires"),
    ///     1
    /// );
    /// ```
    pub fn expire_heat(&self, cutoff: SystemTime) -> Result<usize, Error> {
        Ok(self.expire_heat_report(cutoff)?.removed)
    }

    /// Drop every heat row flushed before `cutoff` and report what survived.
    ///
    /// The scan decodes the whole heat table, so a caller keeps an in-memory
    /// watermark from [`HeatExpiry::oldest`] and skips whole passes while no
    /// row can have expired. The table holds the oldest `last_read` the scan
    /// left behind; adding the caller's retention to it names the first
    /// instant a stored row can expire.
    pub(crate) fn expire_heat_report(&self, cutoff: SystemTime) -> Result<HeatExpiry, Error> {
        let cutoff = storage::to_nanos(cutoff);
        self.write(|txn| {
            let mut table = txn.open_table(HEAT)?;
            let mut expired = Vec::new();
            let mut oldest: Option<u64> = None;
            for entry in table.iter()? {
                let (key, value) = entry?;
                let (bucket, key) = key.value();
                let row: HeatRow = serde_json::from_slice(value.value())?;
                if row.last_read < cutoff {
                    expired.push((bucket.to_owned(), key.to_owned()));
                } else {
                    oldest = Some(oldest.map_or(row.last_read, |at| at.min(row.last_read)));
                }
            }
            let removed = expired.len();
            for (bucket, key) in expired {
                table.remove((bucket.as_str(), key.as_str()))?;
            }
            Ok(HeatExpiry {
                removed,
                oldest: oldest.map(storage::from_nanos),
            })
        })
    }

    /// Remove every index row belonging to `bucket` — placements, warm
    /// candidates and heat rows.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::time::SystemTime;
    ///
    /// use tinio_core::{bucket, object};
    /// use tinio_tier::{Index, Placement, State, Tier};
    ///
    /// let index = Index::open_in_memory().expect("the index opens");
    /// let data = bucket::name("data").expect("a valid bucket name");
    /// let key = object::key("a.txt").expect("a valid object key");
    /// index
    ///     .set_placement(
    ///         &data,
    ///         &key,
    ///         &Placement {
    ///             tier: Tier::Fs,
    ///             state: State::Live,
    ///             cold: None,
    ///             mtime: SystemTime::UNIX_EPOCH,
    ///         },
    ///     )
    ///     .expect("the placement is written");
    /// index
    ///     .warm_insert(&data, &key, SystemTime::UNIX_EPOCH)
    ///     .expect("the candidate is inserted");
    /// index.record_read(&data, &key);
    /// index.flush_heat().expect("the heat row is flushed");
    /// index.remove_bucket(&data).expect("the bucket is removed");
    /// assert_eq!(
    ///     index.placement(&data, &key).expect("the placement is read"),
    ///     None
    /// );
    /// assert!(index.scan_warm(10).expect("the warm scan runs").is_empty());
    /// assert!(index.scan_heat(10).expect("the heat scan runs").is_empty());
    /// ```
    pub fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.as_ref().to_owned();
        self.write(move |txn| {
            {
                let mut table = txn.open_table(PLACEMENT)?;
                let keys = pair_keys(&table, &bucket)?;
                for (bucket, key) in keys {
                    table.remove((bucket.as_str(), key.as_str()))?;
                }
            }
            {
                let mut table = txn.open_table(HEAT)?;
                let keys = pair_keys(&table, &bucket)?;
                for (bucket, key) in keys {
                    table.remove((bucket.as_str(), key.as_str()))?;
                }
            }
            {
                let mut table = txn.open_table(WARM)?;
                let keys: Vec<(u64, String, String)> = {
                    let mut keys = Vec::new();
                    for entry in table.iter()? {
                        let (key, _) = entry?;
                        let (mtime, entry_bucket, key) = key.value();
                        if entry_bucket == bucket.as_str() {
                            keys.push((mtime, entry_bucket.to_owned(), key.to_owned()));
                        }
                    }
                    keys
                };
                for (mtime, bucket, key) in keys {
                    table.remove((mtime, bucket.as_str(), key.as_str()))?;
                }
            }
            {
                let mut table = txn.open_table(WARM_KEY)?;
                let keys: Vec<(String, String)> = {
                    let mut keys = Vec::new();
                    for entry in table.range((bucket.as_str(), "")..)? {
                        let (key, _) = entry?;
                        let (entry_bucket, entry_key) = key.value();
                        if entry_bucket != bucket.as_str() {
                            break;
                        }
                        keys.push((entry_bucket.to_owned(), entry_key.to_owned()));
                    }
                    keys
                };
                for (bucket, key) in keys {
                    table.remove((bucket.as_str(), key.as_str()))?;
                }
            }
            Ok(())
        })
    }

    /// Compact the index.
    ///
    /// Redb requires exclusive access, so this runs only while the
    /// constructor still owns the database (T17) or in an offline tool; it
    /// never runs from a shared index in the background.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_tier::Index;
    ///
    /// let dir = tempfile::tempdir().expect("a temp dir");
    /// let mut index = Index::open(dir.path().join("tier.redb")).expect("the index opens");
    /// index.compact().expect("the index compacts");
    /// ```
    pub fn compact(&mut self) -> Result<bool, Error> {
        match &mut self.db {
            Arm::Writable(db) => Ok(db.compact()?),
            Arm::ReadOnly(_) => Err(Error::ReadOnly),
        }
    }

    fn read<T>(&self, f: impl FnOnce(&ReadTransaction) -> Result<T, Error>) -> Result<T, Error> {
        let txn = self.db.begin_read()?;
        f(&txn)
    }

    fn write<T>(
        &self,
        f: impl FnOnce(&mut WriteTransaction) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut txn = self.db.begin_write()?;
        match f(&mut txn) {
            Ok(value) => {
                txn.commit()?;
                Ok(value)
            }
            Err(err) => {
                let _ = txn.abort();
                Err(err)
            }
        }
    }
}

/// Compact never triggers below the shared 64 MiB floor: a small database
/// gains nothing from a rewrite, and the constructor must not pay for one.
///
/// Whether the index should be compacted at this open is the store crate's own
/// rule ([`needs_compact`]): the fragmentation ratio reaches
/// `threshold_percent` (%), but never below the floor.
fn compact_due(db: &Database, threshold_percent: u8) -> Result<bool, Error> {
    let txn = db.begin_write()?;
    let stats = txn.stats()?;
    let snapshot = Stats {
        allocated_bytes: stats
            .allocated_pages()
            .saturating_mul(stats.page_size() as u64),
        fragmented_bytes: stats.fragmented_bytes(),
    };
    let due = needs_compact(&snapshot, threshold_percent);
    txn.abort()?;
    Ok(due)
}

/// Write one warm candidate row into an open transaction: retire the row the
/// reverse map's previous time left behind, then record the reverse entry and
/// the new `(mtime, bucket, key)` row.
///
/// [`Index::warm_replace`]'s single write and [`Index::warm_rebuild_page`]'s
/// per-page transaction share this, so both replace the superseded row with
/// the same discipline: the reverse map is the point lookup, and a stale row
/// is removed before the new pair is inserted.
fn write_warm_row(
    warm: &mut redb::Table<'_, (u64, &'static str, &'static str), ()>,
    reverse: &mut redb::Table<'_, (&'static str, &'static str), u64>,
    bucket: &str,
    key: &str,
    nanos: u64,
) -> Result<(), Error> {
    if let Some(superseded) = reverse.get((bucket, key))?.map(|guard| guard.value()) {
        warm.remove((superseded, bucket, key))?;
    }
    reverse.insert((bucket, key), nanos)?;
    warm.insert((nanos, bucket, key), ())?;
    Ok(())
}

/// The `(bucket, key)` rows of `bucket` in a pair-keyed table. The range
/// iterator is dropped before any removal, so the collected keys are owned.
fn pair_keys<T>(table: &T, bucket: &str) -> Result<Vec<(String, String)>, Error>
where
    T: redb::ReadableTable<(&'static str, &'static str), &'static [u8]>,
{
    let mut keys = Vec::new();
    for entry in table.range::<(&str, &str)>((bucket, "")..)? {
        let (key, _) = entry?;
        let (entry_bucket, entry_key) = key.value();
        if entry_bucket != bucket {
            break;
        }
        keys.push((entry_bucket.to_owned(), entry_key.to_owned()));
    }
    Ok(keys)
}

/// Create `path`'s parent directory when it has one.
fn create_parent(path: &Path) -> Result<(), Error> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if !parent.as_os_str().is_empty() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Pre-create the index file owner-only on Unix, so the file never exists
/// with a wider mode (T17). The platform provides no POSIX mode; the
/// format gate and redb's own open flags protect the file there.
#[cfg(unix)]
fn prepare_file(path: &Path) -> Result<(), Error> {
    crate::atomic::append_private(path)?;
    Ok(())
}

/// See the Unix arm: Windows has no POSIX mode to set.
#[cfg(not(unix))]
fn prepare_file(_path: &Path) -> Result<(), Error> {
    Ok(())
}

/// Tighten an existing index file to `0600` on Unix (an older process may
/// have created it before the mode existed).
#[cfg(unix)]
fn restrict_file(path: &Path) -> Result<(), Error> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

/// See the Unix arm.
#[cfg(not(unix))]
fn restrict_file(_path: &Path) -> Result<(), Error> {
    Ok(())
}

/// Create the five tables and gate the recorded format version.
fn ensure_schema(db: &mut Database, path: &Path) -> Result<(), Error> {
    let txn = db.begin_write()?;
    {
        txn.open_table(PLACEMENT)?;
        txn.open_table(WARM)?;
        txn.open_table(WARM_KEY)?;
        txn.open_table(HEAT)?;
        let mut meta = txn.open_table(META)?;
        let stored = meta
            .get(FORMAT_KEY)?
            .map(|guard| decode_version(guard.value()));
        match stored {
            None => {
                meta.insert(FORMAT_KEY, FORMAT_VERSION.to_le_bytes().as_slice())?;
            }
            Some(Some(found)) if found == FORMAT_VERSION => {}
            Some(Some(found)) => return Err(unsupported_version(path, found)),
            Some(None) => return Err(unsupported_version(path, 0)),
        }
        backfill_warm_map(&txn)?;
    }
    txn.commit()?;
    Ok(())
}

/// One-time pre-release backfill of the warm reverse map.
///
/// An index written before the reverse map existed keeps its WARM rows but has
/// no WARM_KEY entries, so [`Index::warm_replace`] cannot retire them and the
/// superseded rows would demote a rewritten object at its old age. When WARM
/// is non-empty and WARM_KEY is empty, this scans WARM once, keeps only the
/// newest row per `(bucket, key)` (deleting the older duplicates) and records
/// the map entry a replacement needs.
///
/// The tier index was never released, so the format version stays `1`: the
/// marker and downgrade gates already refuse an older binary a tier root, and
/// this is the one derived table the gate does not have to version.
fn backfill_warm_map(txn: &WriteTransaction) -> Result<(), Error> {
    if txn.open_table(WARM_KEY)?.iter()?.next().is_some() {
        return Ok(());
    }
    let rows: Vec<(u64, String, String)> = {
        let mut rows = Vec::new();
        for entry in txn.open_table(WARM)?.iter()? {
            let (key, _) = entry?;
            let (mtime, bucket, key) = key.value();
            rows.push((mtime, bucket.to_owned(), key.to_owned()));
        }
        rows
    };
    let mut newest: HashMap<(String, String), u64> = HashMap::new();
    for (mtime, bucket, key) in &rows {
        let slot = newest
            .entry((bucket.clone(), key.clone()))
            .or_insert(*mtime);
        *slot = (*slot).max(*mtime);
    }
    {
        let mut table = txn.open_table(WARM)?;
        for (mtime, bucket, key) in &rows {
            if newest.get(&(bucket.clone(), key.clone())) != Some(mtime) {
                table.remove((*mtime, bucket.as_str(), key.as_str()))?;
            }
        }
    }
    let mut map = txn.open_table(WARM_KEY)?;
    for ((bucket, key), mtime) in newest {
        map.insert((bucket.as_str(), key.as_str()), mtime)?;
    }
    Ok(())
}

/// Gate an already-open read-only database: a missing or foreign format
/// version is refused, and nothing is written.
fn gate_read_only(db: &ReadOnlyDatabase, path: &Path) -> Result<(), Error> {
    let txn = db.begin_read()?;
    let meta = txn.open_table(META)?;
    match meta
        .get(FORMAT_KEY)?
        .map(|guard| decode_version(guard.value()))
    {
        Some(Some(found)) if found == FORMAT_VERSION => Ok(()),
        Some(Some(found)) => Err(unsupported_version(path, found)),
        _ => Err(unsupported_version(path, 0)),
    }
}

/// Decode the stored little-endian format version.
fn decode_version(bytes: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(bytes).ok().map(u64::from_le_bytes)
}

/// The unsupported-version refusal of either gate.
fn unsupported_version(path: &Path, found: u64) -> Error {
    Error::UnsupportedVersion {
        path: path.to_path_buf(),
        found,
        expected: FORMAT_VERSION,
    }
}

/// `SystemTime` as Unix nanoseconds in the JSON payload.
mod time_nanos {
    use std::time::SystemTime;

    use serde::{Deserialize as _, Deserializer, Serializer};

    use crate::_core::storage::{from_nanos, to_nanos};

    pub fn serialize<S: Serializer>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(to_nanos(*time))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SystemTime, D::Error> {
        let nanos = u64::deserialize(deserializer)?;
        Ok(from_nanos(nanos))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use redb::{Database, TableDefinition};
    use tempfile::tempdir;
    use tinio_store_redb::compact::MIN_ALLOCATED;

    use super::*;
    use crate::{
        _core::{
            acl::{Acl, OwnerId},
            bucket,
            checksum::{self, Algorithm, Part, Type, Value},
            object, version,
        },
        Error,
    };

    fn at(nanos: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos)
    }

    #[test]
    fn compaction_threshold_gates_the_constructor() {
        let floor = MIN_ALLOCATED;
        let below = Stats {
            allocated_bytes: floor - 1,
            fragmented_bytes: floor - 1,
        };
        assert!(
            !needs_compact(&below, 0),
            "a small database is never compacted, even at zero percent"
        );
        let under = Stats {
            allocated_bytes: floor,
            fragmented_bytes: floor / 5 - 1,
        };
        assert!(
            !needs_compact(&under, 20),
            "19.99% fragmentation is below a 20% threshold"
        );
        let over = Stats {
            allocated_bytes: floor,
            fragmented_bytes: (floor / 5) + 1,
        };
        assert!(
            needs_compact(&over, 20),
            "20%+ fragmentation reaches a 20% threshold"
        );
        let clean = Stats {
            allocated_bytes: floor,
            fragmented_bytes: 0,
        };
        assert!(
            needs_compact(&clean, 0),
            "a zero threshold compacts any database past the floor"
        );
    }

    fn bucket() -> bucket::Name {
        bucket::name("data").expect("a valid bucket name")
    }

    fn key(key: &str) -> object::Key {
        object::key(key).expect("a valid object key")
    }

    fn fs_placement(mtime: SystemTime) -> Placement {
        Placement {
            tier: Tier::Fs,
            state: State::Live,
            cold: None,
            mtime,
        }
    }

    fn cold_info() -> object::Info {
        let owner =
            OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .expect("a valid canonical ID");
        object::Info {
            key: key("dir/a.txt"),
            size: 42,
            last_modified: at(1_000),
            etag: "d41d8cd98f00b204e9800998ecf8427e"
                .parse()
                .expect("a valid etag"),
            tags: object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())])
                .expect("valid tags"),
            checksum: Some(checksum::Recorded {
                part: Part {
                    algorithm: Algorithm::Crc32,
                    value: Value("NhCmhg==".to_owned()),
                },
                kind: Type::FullObject,
            }),
            owner: Some(owner.clone()),
            acl: Acl::from_parts(None, Acl::default_private(Some(owner)).grants),
            version_id: Some(version::id("v1").expect("a valid version id")),
            bucket_records_versions: true,
        }
    }

    #[test]
    fn placement_round_trips_and_is_durable() {
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        let b = bucket();
        let k = key("a.txt");
        let placement = Placement {
            tier: Tier::Pg,
            state: State::Copying,
            cold: None,
            mtime: at(700),
        };
        {
            let index = Index::open(&path).expect("the index opens");
            index
                .set_placement(&b, &k, &placement)
                .expect("the placement is written");
        }

        let index = Index::open(&path).expect("the index reopens");
        assert_eq!(
            index.placement(&b, &k).expect("the placement is read"),
            Some(placement)
        );
        assert_eq!(
            index
                .placement(&b, &key("missing.txt"))
                .expect("a missing placement reads as absent"),
            None
        );
    }

    #[test]
    fn placement_carries_cold_info() {
        let info = cold_info();
        let b = bucket();
        let k = info.key.clone();
        let cold = ColdInfo::from_info(&info);
        assert_eq!(cold.key, "dir/a.txt");
        assert_eq!(cold.size, 42);
        assert!(cold.tags.contains("env=prod"));

        let index = Index::open_in_memory().expect("an in-memory index");
        let placement = Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: Some(cold.clone()),
            mtime: info.last_modified,
        };
        index
            .set_placement(&b, &k, &placement)
            .expect("the placement is written");
        let got = index
            .placement(&b, &k)
            .expect("the placement is read")
            .expect("the placement exists");
        let stored = got.cold.expect("the placement carries cold info");
        assert_eq!(stored, cold);
        assert_eq!(stored.to_info().expect("the info decodes"), info);
    }

    #[test]
    fn scan_placement_is_prefix_after_limit() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        let other = bucket::name("other").expect("a valid bucket name");
        for name in ["a/1", "a/2", "a/3", "b/1"] {
            index
                .set_placement(&b, &key(name), &fs_placement(at(1)))
                .expect("the placement is written");
        }
        index
            .set_placement(&other, &key("a/1"), &fs_placement(at(1)))
            .expect("the other bucket's placement is written");

        let all = index
            .scan_placement(&b, "a/", None, 10)
            .expect("the scan runs");
        let names: Vec<&str> = all.iter().map(|(k, _)| k.as_ref().as_str()).collect();
        assert_eq!(names, ["a/1", "a/2", "a/3"]);

        let page = index
            .scan_placement(&b, "a/", Some("a/1"), 1)
            .expect("the page runs");
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].0.as_ref().as_str(), "a/2");

        // An `after` before the prefix keeps the scan at the prefix start,
        // and an `after` past every key answers nothing.
        let before = index
            .scan_placement(&b, "a/", Some("0"), 10)
            .expect("the scan runs");
        let names: Vec<&str> = before.iter().map(|(k, _)| k.as_ref().as_str()).collect();
        assert_eq!(names, ["a/1", "a/2", "a/3"]);
        assert!(
            index
                .scan_placement(&b, "a/", Some("z"), 10)
                .expect("the scan runs")
                .is_empty()
        );

        // The empty prefix reads the whole bucket in key order.
        let whole = index
            .scan_placement(&b, "", None, 10)
            .expect("the scan runs");
        let names: Vec<&str> = whole.iter().map(|(k, _)| k.as_ref().as_str()).collect();
        assert_eq!(names, ["a/1", "a/2", "a/3", "b/1"]);

        // Another bucket's identically named key is never included.
        let other_page = index
            .scan_placement(&other, "a/", None, 10)
            .expect("the other scan runs");
        assert_eq!(other_page.len(), 1);
        assert_eq!(other_page[0].0.as_ref().as_str(), "a/1");

        assert!(
            index
                .scan_placement(&b, "a/", None, 0)
                .expect("an empty page runs")
                .is_empty()
        );
    }

    #[test]
    fn warm_scans_oldest_first() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        index
            .warm_insert(&b, &key("late.txt"), at(300))
            .expect("a candidate is inserted");
        index
            .warm_insert(&b, &key("early.txt"), at(100))
            .expect("a candidate is inserted");
        index
            .warm_insert(&b, &key("middle.txt"), at(200))
            .expect("a candidate is inserted");
        // Two candidates share one mtime: the tuple key orders them by key.
        index
            .warm_insert(&b, &key("same-b.txt"), at(150))
            .expect("a candidate is inserted");
        index
            .warm_insert(&b, &key("same-a.txt"), at(150))
            .expect("a candidate is inserted");

        let all = index.scan_warm(10).expect("the scan runs");
        let names: Vec<&str> = all.iter().map(|c| c.key.as_ref().as_str()).collect();
        assert_eq!(
            names,
            [
                "early.txt",
                "same-a.txt",
                "same-b.txt",
                "middle.txt",
                "late.txt"
            ]
        );
        assert_eq!(all[0].mtime, at(100));
        assert_eq!(all[1].mtime, at(150));
        assert_eq!(all[2].mtime, at(150));

        let page = index.scan_warm(2).expect("the page runs");
        assert_eq!(page.len(), 2);
        assert_eq!(page[1].key.as_ref().as_str(), "same-a.txt");
        assert!(index.scan_warm(0).expect("an empty page runs").is_empty());
    }

    #[test]
    fn remove_bucket_clears_every_table() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let data = bucket();
        let other = bucket::name("other").expect("a valid bucket name");
        let k = key("a.txt");
        for b in [data.clone(), other.clone()] {
            index
                .set_placement(&b, &k, &fs_placement(at(1)))
                .expect("the placement is written");
            index
                .warm_insert(&b, &k, at(1))
                .expect("the candidate is inserted");
            index.record_read(&b, &k);
        }
        index.flush_heat().expect("the heat rows are flushed");
        assert_eq!(
            index
                .scan_placement(&data, "", None, 10)
                .expect("the scan runs")
                .len(),
            1
        );
        assert_eq!(index.scan_warm(10).expect("the scan runs").len(), 2);
        assert_eq!(index.scan_heat(10).expect("the scan runs").len(), 2);

        index.remove_bucket(&data).expect("the bucket is removed");
        assert!(
            index
                .scan_placement(&data, "", None, 10)
                .expect("the scan runs")
                .is_empty()
        );
        assert_eq!(
            index.placement(&data, &k).expect("the placement is read"),
            None
        );
        let warm = index.scan_warm(10).expect("the scan runs");
        assert_eq!(warm.len(), 1);
        assert_eq!(warm[0].bucket, other);
        let heat = index.scan_heat(10).expect("the scan runs");
        assert_eq!(heat.len(), 1);
        assert_eq!(heat[0].bucket, other);
        let reverse = index
            .read(|txn| {
                let table = txn.open_table(WARM_KEY)?;
                Ok(table
                    .get((data.as_ref().as_str(), k.as_ref().as_str()))?
                    .map(|guard| guard.value()))
            })
            .expect("the reverse map is read");
        assert_eq!(reverse, None, "the reverse map is cleared with the bucket");
        let kept = index
            .read(|txn| {
                let table = txn.open_table(WARM_KEY)?;
                Ok(table
                    .get((other.as_ref().as_str(), k.as_ref().as_str()))?
                    .map(|guard| guard.value()))
            })
            .expect("the kept reverse map is read");
        assert_eq!(
            kept,
            Some(storage::to_nanos(at(1))),
            "another bucket's reverse entry survives"
        );
    }

    #[test]
    fn a_legacy_warm_table_is_backfilled_and_deduplicated() {
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        let data = bucket();
        let a = key("a.txt");
        let b = key("b.txt");
        {
            let db = Database::create(&path).expect("a raw legacy database");
            let txn = db.begin_write().expect("a write transaction");
            {
                let mut warm = txn.open_table(WARM).expect("the warm table");
                warm.insert((1_000u64, "data", "a.txt"), ())
                    .expect("the superseded row");
                warm.insert((2_000u64, "data", "a.txt"), ())
                    .expect("the newest row for the key");
                warm.insert((1_500u64, "data", "b.txt"), ())
                    .expect("another key's row");
                let mut meta = txn.open_table(META).expect("the meta table");
                meta.insert(FORMAT_KEY, FORMAT_VERSION.to_le_bytes().as_slice())
                    .expect("the recorded format");
            }
            txn.commit().expect("the write commits");
        }

        let index = Index::open(&path).expect("the index opens and backfills");

        let warm = index.scan_warm(10).expect("the scan runs");
        assert_eq!(warm.len(), 2, "one row per key after the dedupe");
        assert_eq!(warm[0].key, b);
        assert_eq!(warm[0].mtime, at(1_500));
        assert_eq!(warm[1].key, a);
        assert_eq!(warm[1].mtime, at(2_000), "the newest row is kept");

        let mapped = index
            .read(|txn| {
                let table = txn.open_table(WARM_KEY)?;
                Ok(table
                    .get((data.as_ref().as_str(), b.as_ref().as_str()))?
                    .map(|guard| guard.value()))
            })
            .expect("the rebuilt map is read");
        assert_eq!(
            mapped,
            Some(storage::to_nanos(at(1_500))),
            "the backfill recorded every kept row"
        );

        let replacement = at(3_000);
        index
            .warm_replace(&data, &a, replacement)
            .expect("the replacement runs");
        let warm = index.scan_warm(10).expect("the scan runs");
        assert_eq!(warm.len(), 2);
        let replaced = warm
            .iter()
            .find(|candidate| candidate.key == a)
            .expect("the replaced key is present");
        assert_eq!(
            replaced.mtime, replacement,
            "the backfilled map lets the replacement retire the right row"
        );
        assert!(
            warm.iter().all(|candidate| candidate.mtime != at(1_000)),
            "the superseded legacy row is gone"
        );
    }

    #[test]
    fn warm_removes_on_demote() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        index
            .warm_insert(&b, &key("a.txt"), at(100))
            .expect("a candidate is inserted");
        index
            .warm_insert(&b, &key("b.txt"), at(200))
            .expect("a candidate is inserted");
        index
            .warm_remove(&b, &key("a.txt"), at(100))
            .expect("the candidate is removed");
        let left = index.scan_warm(10).expect("the scan runs");
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].key.as_ref().as_str(), "b.txt");
        index
            .warm_remove(&b, &key("a.txt"), at(100))
            .expect("an absent candidate removes as a no-op");
    }

    #[test]
    fn heat_rows_expire() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        let hot = key("hot.txt");
        let cold = key("cold.txt");
        index.record_read(&b, &hot);
        index.record_read(&b, &hot);
        index.record_read(&b, &cold);
        index.flush_heat().expect("the heat rows are flushed");

        let rows = index.scan_heat(10).expect("the scan runs");
        assert_eq!(rows.len(), 2);
        let row = rows
            .iter()
            .find(|entry| entry.key == hot)
            .expect("the hot key is present");
        assert_eq!(row.reads, 2);
        assert_eq!(
            index
                .expire_heat(SystemTime::UNIX_EPOCH)
                .expect("an old cutoff expires nothing"),
            0
        );

        let cutoff = SystemTime::now() + Duration::from_secs(1);
        assert_eq!(index.expire_heat(cutoff).expect("the rows expire"), 2);
        assert!(index.scan_heat(10).expect("the scan runs").is_empty());
        assert_eq!(
            index
                .expire_heat(cutoff)
                .expect("a second expiry removes nothing"),
            0
        );
    }

    /// The warm reset's placement and candidate land together: one
    /// transaction replaces both rows, and a `None` time records the
    /// placement alone.
    #[test]
    fn the_warm_reset_writes_the_placement_and_candidate_together() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        let k = key("a.txt");
        index
            .warm_insert(&b, &k, at(1))
            .expect("a stale candidate is planted");
        index
            .set_placement_and_warm(&b, &k, &fs_placement(at(9)), Some(at(9)))
            .expect("the reset is written");
        assert_eq!(
            index.placement(&b, &k).expect("the placement reads"),
            Some(fs_placement(at(9)))
        );
        let warm = index.scan_warm(10).expect("the scan runs");
        assert_eq!(warm.len(), 1, "the stale candidate was retired");
        assert_eq!(warm[0].mtime, at(9));

        let marker = key("dir/");
        index
            .set_placement_and_warm(&b, &marker, &fs_placement(at(5)), None)
            .expect("the marker's placement is written alone");
        assert!(
            index
                .placement(&b, &marker)
                .expect("the placement reads")
                .is_some()
        );
        assert_eq!(
            index.scan_warm(10).expect("the scan runs").len(),
            1,
            "no candidate is recorded for the marker"
        );
    }

    /// One rebuild transaction records the warm rows of a page and skips a
    /// key whose placement the warm plane does not own.
    #[test]
    fn a_rebuild_page_skips_placements_the_warm_plane_does_not_own() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        let cold = key("cold.txt");
        let warm = key("warm.txt");
        let cold_placement = Placement {
            tier: Tier::Pg,
            state: State::Live,
            cold: None,
            mtime: at(1),
        };
        index
            .set_placement(&b, &cold, &cold_placement)
            .expect("the cold placement is written");

        index
            .warm_rebuild_page(&b, &[(cold.clone(), at(2)), (warm.clone(), at(3))])
            .expect("the page is recorded");

        let candidates = index.scan_warm(10).expect("the scan runs");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].key, warm);
        assert_eq!(candidates[0].mtime, at(3));
        assert_eq!(
            index.placement(&b, &cold).expect("the placement reads"),
            Some(cold_placement),
            "the rebuild leaves the cold placement alone"
        );
    }

    /// The expiry scan names the oldest row it left behind, which is what the
    /// caller's skip watermark is built from.
    #[test]
    fn the_expiry_report_names_the_oldest_surviving_row() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        index
            .absorb_heat(&[
                HeatEntry {
                    bucket: b.clone(),
                    key: key("old.txt"),
                    reads: 1,
                    last_read: at(100),
                },
                HeatEntry {
                    bucket: b.clone(),
                    key: key("kept.txt"),
                    reads: 1,
                    last_read: at(300),
                },
            ])
            .expect("the rows are stored");

        let report = index
            .expire_heat_report(at(200))
            .expect("the old row expires");
        assert_eq!(report.removed, 1);
        assert_eq!(report.oldest, Some(at(300)));

        let report = index
            .expire_heat_report(at(400))
            .expect("the remaining row expires");
        assert_eq!(report.removed, 1);
        assert_eq!(report.oldest, None, "no row survives to expire again");
    }

    /// A drain larger than one flush transaction is absorbed whole: every row
    /// of every chunk reaches the table with its count and read time.
    #[test]
    fn absorb_heat_merges_more_than_one_chunk() {
        let index = Index::open_in_memory().expect("an in-memory index");
        let b = bucket();
        let now = SystemTime::now();
        let rows: Vec<HeatEntry> = (0..HEAT_FLUSH_CHUNK + 7)
            .map(|n| HeatEntry {
                bucket: b.clone(),
                key: key(&format!("key-{n:04}")),
                reads: 2,
                last_read: now,
            })
            .collect();
        index.absorb_heat(&rows).expect("the rows are absorbed");

        let stored = index.scan_heat(usize::MAX).expect("the scan runs");
        assert_eq!(
            stored.len(),
            rows.len(),
            "every row of the second chunk is stored"
        );
        assert!(stored.iter().all(|row| row.reads == 2));
        assert_eq!(
            index
                .heat_last_read(&b, &key(&format!("key-{:04}", rows.len() - 1)))
                .expect("the row reads"),
            Some(now)
        );
    }

    #[test]
    fn the_root_id_round_trips() {
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        let root = *b"0123456789abcdef";
        {
            let index = Index::open(&path).expect("the index opens");
            assert_eq!(index.root_id().expect("the id is read"), None);
            index.set_root_id(root).expect("the id is written");
        }
        let index = Index::open(&path).expect("the index reopens");
        assert_eq!(index.root_id().expect("the id is read"), Some(root));
    }

    #[test]
    fn a_format_mismatch_is_refused() {
        const META: TableDefinition<'static, &'static str, &'static [u8]> =
            TableDefinition::new("meta");
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        {
            let db = Database::create(&path).expect("a raw database");
            let txn = db.begin_write().expect("a write transaction");
            {
                let mut meta = txn.open_table(META).expect("the meta table");
                meta.insert("format", 7u64.to_le_bytes().as_slice())
                    .expect("a mismatched version");
            }
            txn.commit().expect("the write commits");
        }

        let err = match Index::open(&path) {
            Ok(_) => panic!("the mismatch must be refused"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                Error::UnsupportedVersion {
                    found: 7,
                    expected: FORMAT_VERSION,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        drop(Index::open(&path).expect("the index opens"));
        let mode = std::fs::metadata(&path)
            .expect("the file exists")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "the index file is owner-only");
    }

    #[test]
    fn compaction_reopens_clean() {
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        let b = bucket();
        let kept = key("kept.txt");
        let placement = fs_placement(at(7));
        {
            let mut index = Index::open(&path).expect("the index opens");
            for i in 0..300u32 {
                let k = key(&format!("churn/{i}"));
                index
                    .set_placement(&b, &k, &fs_placement(at(u64::from(i))))
                    .expect("a churn placement is written");
            }
            for i in 0..300u32 {
                index
                    .clear(&b, &key(&format!("churn/{i}")))
                    .expect("a churn placement is cleared");
            }
            index
                .set_placement(&b, &kept, &placement)
                .expect("the kept placement is written");
            let compacted = index.compact().expect("the index compacts");
            assert!(
                compacted,
                "the churned index retains pages that compaction must reclaim"
            );
        }

        let index = Index::open(&path).expect("the index reopens");
        assert_eq!(
            index.placement(&b, &kept).expect("the placement is read"),
            Some(placement)
        );
        assert!(
            index
                .scan_placement(&b, "churn/", None, 10)
                .expect("the scan runs")
                .is_empty()
        );
    }

    #[test]
    fn read_only_open_refuses_writes() {
        let dir = tempdir().expect("a temp dir");
        let path = dir.path().join("tier.redb");
        let b = bucket();
        let k = key("a.txt");
        let placement = fs_placement(at(11));
        {
            let index = Index::open(&path).expect("the index opens");
            index
                .set_root_id(*b"0123456789abcdef")
                .expect("the id is written");
            index
                .set_placement(&b, &k, &placement)
                .expect("the placement is written");
        }

        let mut index = Index::open_read_only(&path).expect("the index opens read-only");
        assert_eq!(
            index.root_id().expect("the id is read"),
            Some(*b"0123456789abcdef")
        );
        assert_eq!(
            index.placement(&b, &k).expect("the placement is read"),
            Some(placement.clone())
        );

        // A read-only index ignores heat reads rather than buffering them.
        index.record_read(&b, &k);
        assert!(
            index
                .pending_heat
                .lock()
                .expect("the heat buffer is not poisoned")
                .is_empty(),
            "a read-only index buffers no heat"
        );

        assert!(matches!(
            index.set_root_id(*b"fedcba9876543210"),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            index.set_placement(&b, &k, &placement),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(index.clear(&b, &k), Err(Error::ReadOnly)));
        assert!(matches!(
            index.warm_insert(&b, &k, at(1)),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            index.warm_remove(&b, &k, at(1)),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            index.warm_replace(&b, &k, at(2)),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            index.set_placement_and_warm(&b, &k, &placement, Some(at(2))),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(
            index.warm_rebuild_page(&b, &[(k.clone(), at(2))]),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(index.flush_heat(), Err(Error::ReadOnly)));
        assert!(matches!(
            index.expire_heat(SystemTime::UNIX_EPOCH),
            Err(Error::ReadOnly)
        ));
        assert!(matches!(index.remove_bucket(&b), Err(Error::ReadOnly)));
        assert!(matches!(index.compact(), Err(Error::ReadOnly)));
    }
}
