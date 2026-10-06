//! The version domain: opaque version identifiers, the three bucket states,
//! and the shapes the version operations exchange.
//!
//! A version identifier is opaque to every consumer: the storage layer mints
//! it, the wire echoes it, and **ordering comes from the row's sequence, never
//! from the identifier's shape** (the design's version-ID ruling). `None` in an
//! `Option<Id>` is the `null` version of a bucket that was unversioned when the
//! object was written, or suspended when it was.

use std::{str::FromStr, time::SystemTime};

use derive_more::{AsRef, Deref};
use parse_display::{Display, FromStr};
use uuid::Uuid;

use crate::{
    etag::ETag,
    object,
    storage::{self, Error},
};

/// The wire cap on a version identifier (AWS: 1024 bytes).
pub const MAX_ID_LEN: usize = 1024;

/// Mint a fresh version identifier — the one home of the backends' mint.
///
/// The identifier's shape is free to change (the design's version-ID ruling
/// keeps ordering in the stored sequence, never in the identifier); today it
/// is the hyphenated UUIDv4 wire. That wire is 36 ASCII bytes, so it satisfies
/// the 1..=1024-byte rule by construction and this call is infallible where
/// [`id`] validates an untrusted wire — the doctest pins that the two agree.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::{self, Id};
///
/// let minted = version::mint();
/// assert!(
///     Id::new(minted.as_str()).is_ok(),
///     "a minted id passes the validated constructor"
/// );
/// assert!(version::id(minted.as_str()).is_ok());
/// assert_ne!(
///     minted.as_str(),
///     version::mint().as_str(),
///     "each mint is a fresh identifier"
/// );
/// ```
pub fn mint() -> Id {
    // Constructed in the module that owns the invariant, never through a
    // fallible parse: the uuid wire is 36 ASCII bytes, inside the wire rule
    // by construction, so there is no fallible step to `expect` away
    // (constitution II).
    Id(Uuid::new_v4().to_string())
}

/// An opaque version identifier.
///
/// `FromStr` is written by hand rather than derived — a recorded deviation
/// from `docs/style.md`'s derive rule, the same one [`crate::etag::ETag`] and
/// [`crate::blob::BlockHash`] record: a `parse-display` regex matches Unicode
/// scalars, so it cannot express the wire cap of **1..=1024 bytes**, and a
/// 1024-character non-ASCII identifier would parse through the derived path
/// while the backends bound the same identifier at 1024 bytes. The parse
/// delegates to [`id`], the one validated constructor, so the two cannot
/// disagree about any input.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version;
///
/// let id = version::id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY").unwrap();
/// assert_eq!(id.as_str(), "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY");
/// assert_eq!(id.to_string(), id.as_str());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Deref, AsRef)]
#[display("{0}")]
#[as_ref(forward)]
pub struct Id(String);

/// Validate and wrap a version identifier from an untrusted source.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version;
///
/// assert!(version::id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY").is_ok());
/// assert!(
///     version::id("null").is_ok(),
///     "the literal is a legal wire value"
/// );
///
/// assert!(version::id("").is_err(), "an empty identifier is rejected");
/// assert!(
///     version::id(&"x".repeat(version::MAX_ID_LEN + 1)).is_err(),
///     "the wire form is capped"
/// );
/// ```
pub fn id(id: impl Into<String>) -> Result<Id, Error> {
    let id = id.into();
    if id.is_empty() || id.len() > MAX_ID_LEN {
        return Err(storage::invalid_version(id));
    }
    Ok(Id(id))
}

impl Id {
    /// The raw wire form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Read a wire value: the literal `null` is the null version, anything
    /// else must be a valid identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::version::Id;
    ///
    /// assert_eq!(
    ///     Id::from_wire("null"),
    ///     None,
    ///     "the literal reads as the null version"
    /// );
    /// assert_eq!(Id::from_wire("abc").unwrap().as_str(), "abc");
    /// assert_eq!(
    ///     Id::from_wire(""),
    ///     None,
    ///     "a malformed value reads as no version"
    /// );
    /// ```
    pub fn from_wire(value: &str) -> Option<Id> {
        if value == "null" {
            return None;
        }
        Self::new(value).ok()
    }

    /// Checked constructor.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::version::Id;
    ///
    /// assert_eq!(Id::new("abc").unwrap().as_str(), "abc");
    /// assert!(Id::new("").is_err());
    /// ```
    pub fn new(id: &str) -> Result<Id, Error> {
        self::id(id)
    }
}

/// The wire form of an optional identity: the literal `null` for the null
/// version — the convention every stored element uses, and the form the
/// version index is keyed by.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version;
///
/// assert_eq!(version::wire_of(None), "null");
/// let id = version::id("abc").unwrap();
/// assert_eq!(version::wire_of(Some(&id)), "abc");
/// ```
pub fn wire_of(id: Option<&Id>) -> String {
    id.map_or_else(|| "null".to_owned(), |id| id.as_str().to_owned())
}

/// The address a request's `versionId` names: no version (the current one),
/// the literal `null` version, or a specific id.
///
/// The three states are the wire's, and [`Address::from_wire`] is the one
/// parse that tells them apart: an absent parameter is
/// [`Address::Current`], the literal `null` is [`Address::Null`], and
/// anything else must pass [`Id::new`] — so a malformed value is an error,
/// never the silent collapse into the null version that [`Id::from_wire`]
/// alone would take. The type is the one home of the presence the handlers
/// branch on: a request names a version exactly when the address is not
/// [`Address::Current`].
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::{self, Address};
///
/// assert_eq!(Address::from_wire(None).unwrap(), Address::Current);
/// assert_eq!(Address::from_wire(Some("null")).unwrap(), Address::Null);
/// assert_eq!(
///     Address::from_wire(Some("abc")).unwrap(),
///     Address::Id(version::id("abc").unwrap())
/// );
/// assert!(
///     Address::from_wire(Some("")).is_err(),
///     "a malformed value is an error, never the null version"
/// );
///
/// assert!(!Address::Current.is_addressed());
/// assert!(Address::Null.is_addressed(), "the literal names a version");
/// assert_eq!(Address::Current.id(), None);
/// assert_eq!(Address::Null.id(), None, "the null version has no id");
/// let id = version::id("abc").unwrap();
/// assert_eq!(Address::Id(id).id().map(|id| id.as_str()), Some("abc"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Address {
    /// No `versionId` on the wire: the request addresses the current
    /// version.
    Current,
    /// The literal `null`: the request addresses the null version.
    Null,
    /// A specific version id.
    Id(Id),
}

impl Address {
    /// Read a request's `versionId` off the wire — the one parse of the
    /// three states.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::version::Address;
    ///
    /// assert_eq!(Address::from_wire(None).unwrap(), Address::Current);
    /// assert_eq!(Address::from_wire(Some("null")).unwrap(), Address::Null);
    /// assert!(
    ///     Address::from_wire(Some("x".repeat(1025).as_str())).is_err(),
    ///     "the identifier rule bounds every other value"
    /// );
    /// ```
    pub fn from_wire(value: Option<&str>) -> Result<Address, Error> {
        let Some(raw) = value else {
            return Ok(Address::Current);
        };
        if raw == wire_of(None) {
            return Ok(Address::Null);
        }
        Id::new(raw).map(Address::Id)
    }

    /// Whether the request named a version at all (`false` only for
    /// [`Address::Current`]) — the presence the fail-open guard and every
    /// addressed branch key on.
    pub fn is_addressed(&self) -> bool {
        !matches!(self, Address::Current)
    }

    /// The specific id the address names: `None` for the current address
    /// and for the null one — the null version's identity is the absence
    /// of an id, the `None` an `Option<Id>` spells it with.
    pub fn id(&self) -> Option<&Id> {
        match self {
            Address::Id(id) => Some(id),
            Address::Current | Address::Null => None,
        }
    }
}

impl FromStr for Id {
    type Err = Error;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        id(raw)
    }
}

impl TryFrom<&str> for Id {
    type Error = Error;

    /// Validate an untrusted wire into an identifier — the same rule and the
    /// same error as [`id`], so a caller composes it with `?` and the two
    /// cannot disagree. There is deliberately no infallible `From<&str>`: a
    /// panic on a malformed identifier is not a library path's answer
    /// (constitution II).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::version::Id;
    ///
    /// let id = Id::try_from("abc").unwrap();
    /// assert_eq!(id.as_str(), "abc");
    /// assert!(Id::try_from("").is_err(), "an empty wire is rejected");
    /// ```
    fn try_from(raw: &str) -> Result<Self, Self::Error> {
        id(raw)
    }
}

/// The bucket's versioning state.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::State;
///
/// assert_eq!(State::default(), State::Unversioned);
/// assert_eq!(State::Enabled.to_string(), "Enabled");
/// assert!(State::Enabled.records_versions());
/// assert!(!State::Unversioned.records_versions());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, FromStr)]
pub enum State {
    /// The default: every write replaces the previous object.
    #[default]
    #[display("Unversioned")]
    Unversioned,
    /// Every write creates a version; the previous becomes noncurrent.
    #[display("Enabled")]
    Enabled,
    /// Writes replace the `null` version in place; deletes insert a `null` marker.
    #[display("Suspended")]
    Suspended,
}

impl State {
    /// Whether writes create versions.
    pub fn is_enabled(self) -> bool {
        self == State::Enabled
    }

    /// Whether writes replace the null version in place.
    pub fn is_suspended(self) -> bool {
        self == State::Suspended
    }

    /// Whether the bucket records versions at all (enabled or suspended) —
    /// the one home of the rule behind the version-identity carriers
    /// (`x-amz-version-id` on a null identity, `Info::bucket_records_versions`).
    pub fn records_versions(self) -> bool {
        self != State::Unversioned
    }
}

/// What a version-creating write or a delete produced.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::Marker;
///
/// assert_eq!(
///     Marker::none(),
///     Marker {
///         id: None,
///         is_delete_marker: false
///     }
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Marker {
    /// The version the operation created or removed; `None` is the null version.
    pub id: Option<Id>,
    /// Whether the version is a delete marker.
    pub is_delete_marker: bool,
}

impl Marker {
    /// No version was created (an unversioned bucket).
    pub fn none() -> Self {
        Self::default()
    }
}

/// The shape of one key's current version, as the bounded per-key probe
/// ([`VersionOps::current_version`](crate::storage::VersionOps::current_version))
/// sees it — the question behind the plain read's 404 delete-marker
/// disclosure, answered by exact-key reads rather than a listing page.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::CurrentVersion;
///
/// assert_eq!(CurrentVersion::default(), CurrentVersion::Absent);
/// assert_ne!(CurrentVersion::Object, CurrentVersion::Marker);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CurrentVersion {
    /// The key has no current version at all.
    #[default]
    Absent,
    /// The current version is a content object.
    Object,
    /// The current version is a delete marker.
    Marker,
}

/// One version as the listing returns it.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{ETag, object, version};
///
/// let entry = version::Entry {
///     key: object::key("a.txt").unwrap(),
///     id: None,
///     seq: 1,
///     is_delete_marker: false,
///     size: 4,
///     last_modified: SystemTime::UNIX_EPOCH,
///     etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
/// };
/// assert_eq!(entry.seq, 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The object key the version belongs to.
    pub key: object::Key,
    /// The version identifier; `None` is the null version.
    pub id: Option<Id>,
    /// The per-key sequence, newest first when sorted descending.
    pub seq: u64,
    /// Whether this version is a delete marker.
    pub is_delete_marker: bool,
    /// The version's content length (0 for a marker).
    pub size: u64,
    /// When the version was written.
    pub last_modified: SystemTime,
    /// The version's ETag.
    pub etag: ETag,
}

/// The pagination cursor of `ListObjectVersions`.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::IdMarker;
///
/// let cursor = IdMarker("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY".into());
/// assert_eq!(cursor.0, "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdMarker(pub String);

/// `ListObjectVersions`' request parameters.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::{IdMarker, ListParams};
///
/// let params = ListParams {
///     prefix: Some("dir/".into()),
///     delimiter: Some("/".into()),
///     key_marker: Some("dir/a.txt".into()),
///     version_id_marker: Some(IdMarker("abc".into())),
///     max_keys: 100,
/// };
/// assert_eq!(params.max_keys, 100);
/// ```
#[derive(Debug, Clone, Default)]
pub struct ListParams {
    /// Only keys with this prefix.
    pub prefix: Option<String>,
    /// Group keys at this delimiter into common prefixes.
    pub delimiter: Option<String>,
    /// Resume after this key.
    pub key_marker: Option<String>,
    /// Resume after this version of `key_marker`.
    pub version_id_marker: Option<IdMarker>,
    /// The page size.
    pub max_keys: usize,
}

/// One page of versions.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::Listing;
///
/// let page = Listing {
///     entries: vec![],
///     common_prefixes: vec!["dir/".into()],
///     is_truncated: true,
///     next_key_marker: Some("dir/a.txt".into()),
///     next_version_id_marker: Some("abc".into()),
/// };
/// assert!(page.is_truncated);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Listing {
    /// The page's versions, newest first within each key.
    pub entries: Vec<Entry>,
    /// The delimiter roll-up.
    pub common_prefixes: Vec<String>,
    /// Whether more versions remain.
    pub is_truncated: bool,
    /// The next page's key cursor.
    pub next_key_marker: Option<String>,
    /// The next page's version cursor.
    pub next_version_id_marker: Option<String>,
}

/// The page cut, the delimiter roll-up and the resume markers of a version
/// listing — the one shape both backends share, so they answer the same page
/// for the same rows. `entries` must already be in the listing's order (by
/// key, the current version first, then the noncurrent versions newest
/// first): `key_marker` skips the keys up to and including the marker (alone)
/// or resumes after the named version (with `version_id_marker`); a delimiter
/// rolls a group of keys up as one common prefix.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{ETag, object, version};
///
/// let entry = |key: &str| version::Entry {
///     key: object::key(key).unwrap(),
///     id: None,
///     seq: 0,
///     is_delete_marker: false,
///     size: 0,
///     last_modified: SystemTime::UNIX_EPOCH,
///     etag: ETag::EMPTY,
/// };
/// let page = version::paginate(
///     &[entry("a.txt"), entry("b.txt")],
///     &version::ListParams {
///         max_keys: 1,
///         ..Default::default()
///     },
/// );
/// assert_eq!(page.entries.len(), 1, "the page cut at max_keys");
/// assert!(page.is_truncated, "more versions remain");
/// assert_eq!(page.next_key_marker.as_deref(), Some("a.txt"));
/// assert_eq!(page.next_version_id_marker.as_deref(), Some("null"));
/// ```
pub fn paginate(entries: &[Entry], params: &ListParams) -> Listing {
    /// The last thing a page emitted — a version or a rolled-up prefix; the
    /// resume markers are read off it. The version arm keeps only the cursor
    /// pair (the key and the version-id wire, `null` included), so emitting
    /// an entry does not clone it a second time just to remember where the
    /// page stopped.
    enum Emitted {
        Entry(String, String),
        Prefix(String),
    }

    /// The entry's key, as the pager's borrowed view.
    fn key_of(entry: &Entry) -> &str {
        entry.key.as_ref()
    }

    let start = match (
        params.key_marker.as_deref(),
        params.version_id_marker.as_ref(),
    ) {
        (None, _) => 0,
        (Some(key), None) => entries.partition_point(|entry| key_of(entry) <= key),
        (Some(key), Some(marker)) => entries
            .iter()
            .position(|entry| key_of(entry) == key && wire_of(entry.id.as_ref()) == marker.0)
            .map_or_else(
                || entries.partition_point(|entry| key_of(entry) <= key),
                |index| index + 1,
            ),
    };

    let mut page: Vec<Entry> = Vec::new();
    let mut common_prefixes: Vec<String> = Vec::new();
    let mut last: Option<Emitted> = None;
    let mut truncated = false;
    let prefix = params.prefix.as_deref().unwrap_or_default();
    if params.max_keys > 0 {
        for entry in &entries[start..] {
            let rolled = params
                .delimiter
                .as_deref()
                .and_then(|delimiter| storage::common_prefix(key_of(entry), prefix, delimiter));
            match rolled {
                Some(rolled) => {
                    // The last emitted thing *is* the prefix-group state: a
                    // prefix repeats only while the same group is being
                    // rolled up, so no separate `last_prefix` is kept.
                    if matches!(&last, Some(Emitted::Prefix(previous)) if previous == rolled) {
                        continue;
                    }
                    if page.len() + common_prefixes.len() == params.max_keys {
                        truncated = true;
                        break;
                    }
                    last = Some(Emitted::Prefix(rolled.to_owned()));
                    common_prefixes.push(rolled.to_owned());
                }
                None => {
                    if page.len() + common_prefixes.len() == params.max_keys {
                        truncated = true;
                        break;
                    }
                    last = Some(Emitted::Entry(
                        key_of(entry).to_owned(),
                        wire_of(entry.id.as_ref()),
                    ));
                    page.push(entry.clone());
                }
            }
        }
    }
    let (next_key_marker, next_version_id_marker) = match (truncated, last) {
        (true, Some(Emitted::Entry(key, version))) => (Some(key), Some(version)),
        (true, Some(Emitted::Prefix(rolled))) => (Some(rolled), None),
        _ => (None, None),
    };
    Listing {
        entries: page,
        common_prefixes,
        is_truncated: truncated,
        next_key_marker,
        next_version_id_marker,
    }
}

/// A version-addressed read's outcome.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::version::ReadResult;
///
/// let read = ReadResult::Marker {
///     id: None,
///     last_modified: SystemTime::UNIX_EPOCH,
/// };
/// assert!(matches!(read, ReadResult::Marker { id: None, .. }));
/// ```
// The plan's pinned interface keeps `Object` owning the whole
// `GetObjectResult` (plan 3 renders both variants directly); the size gap vs
// the marker variant is the price of that shape — the value is built once per
// read and never held in a collection.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ReadResult {
    /// An object version's content and metadata.
    Object(storage::GetObjectResult),
    /// The version is a delete marker: no content, but an identity.
    Marker {
        /// The marker's identifier (`None` = the null version).
        id: Option<Id>,
        /// When the marker was written.
        last_modified: SystemTime,
    },
}

/// A version-addressed head's outcome.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{ETag, acl, object, version};
///
/// let head = version::HeadResult::Object(object::Info {
///     key: object::key("a.txt").unwrap(),
///     size: 4,
///     last_modified: SystemTime::UNIX_EPOCH,
///     etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
///     tags: object::Tags::empty(),
///     checksum: None,
///     owner: None,
///     acl: acl::Acl::default_private(None),
///     version_id: None,
///     bucket_records_versions: false,
/// });
/// assert!(matches!(head, version::HeadResult::Object(_)));
/// ```
#[derive(Debug)]
pub enum HeadResult {
    /// An object version's metadata.
    Object(object::Info),
    /// The version is a delete marker.
    Marker {
        /// The marker's identifier (`None` = the null version).
        id: Option<Id>,
        /// When the marker was written.
        last_modified: SystemTime,
    },
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn a_version_id_is_opaque_but_never_empty_or_unbounded() {
        assert!(id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo").is_ok());
        assert!(id("null").is_ok(), "the literal is a legal wire value");
        assert!(id("").is_err());
        assert!(
            id("x".repeat(1025)).is_err(),
            "AWS caps the wire form at 1024 bytes"
        );
    }

    #[test]
    fn the_hand_written_from_str_and_the_validated_constructor_enforce_one_rule() {
        // Two spellings of one rule: `id` answers the contract error the
        // backends call, and the hand-written `FromStr` parse delegates to it
        // (the recorded deviation from the derive rule). They must never
        // drift — in either direction.
        let wire = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY";
        assert_eq!(Id::from_str(wire).unwrap(), id(wire).unwrap());
        assert!(
            "null".parse::<Id>().is_ok(),
            "the literal is a legal identifier"
        );
        for bad in ["", &"x".repeat(MAX_ID_LEN + 1)] {
            assert!(id(bad).is_err(), "the constructor must reject {bad:?}");
            assert!(
                bad.parse::<Id>().is_err(),
                "the derived parse must reject {bad:?}"
            );
        }
        assert!(
            "x".repeat(MAX_ID_LEN).parse::<Id>().is_ok(),
            "the cap is inclusive"
        );

        // The cap counts BYTES on both paths: 600 × "é" is 1200 bytes across
        // 600 characters, so a Unicode-scalar regex would wave it through the
        // parse while every backend refuses the same identifier.
        let non_ascii = "é".repeat(600);
        assert_eq!(non_ascii.len(), 1200, "the fixture is 1200 bytes");
        assert!(
            id(&non_ascii).is_err(),
            "the constructor counts bytes, not characters"
        );
        assert!(
            non_ascii.parse::<Id>().is_err(),
            "the parse must count bytes too, not characters"
        );
    }

    #[test]
    fn the_null_version_reads_as_absent() {
        assert_eq!(Id::from_wire("null"), None);
        assert_eq!(Id::from_wire("abc").unwrap().as_str(), "abc");
    }

    #[test]
    fn the_three_wire_addresses_are_told_apart_by_the_one_parse() {
        let address = |value: Option<&str>| Address::from_wire(value).unwrap();
        assert_eq!(address(None), Address::Current);
        assert_eq!(address(Some("null")), Address::Null);
        assert_eq!(address(Some("abc")), Address::Id(id("abc").unwrap()));
        assert_eq!(
            address(Some(&"x".repeat(MAX_ID_LEN))),
            Address::Id(id("x".repeat(MAX_ID_LEN)).unwrap()),
            "the cap is inclusive for a specific id too"
        );
        // A malformed value is this parse's error, never the null version:
        // `Id::from_wire` alone answers the same `None` for both, which is
        // the collapse the validated constructor exists to forbid.
        for bad in ["", &"x".repeat(MAX_ID_LEN + 1)] {
            assert!(
                Address::from_wire(Some(bad)).is_err(),
                "{bad:?} must not parse as an address"
            );
            assert_eq!(
                Id::from_wire(bad),
                None,
                "…while the value-only parse cannot tell it from the literal"
            );
        }
    }

    #[test]
    fn the_address_presence_and_id_read_the_three_states() {
        let address = |value: Option<&str>| Address::from_wire(value).unwrap();
        assert!(!address(None).is_addressed(), "no version was named");
        assert!(
            address(Some("null")).is_addressed(),
            "the literal names one"
        );
        assert!(address(Some("abc")).is_addressed());
        assert_eq!(address(None).id(), None);
        assert_eq!(
            address(Some("null")).id(),
            None,
            "the null version has no id"
        );
        assert_eq!(address(Some("abc")).id().map(Id::as_str), Some("abc"));
        // The current and the null address answer the same `id()` — the
        // id alone cannot carry which one was named, `is_addressed` is the
        // one discriminant (the reason the handlers hold an address rather
        // than a parallel presence flag).
        assert_eq!(address(None).id(), address(Some("null")).id());
        assert_ne!(
            address(None).is_addressed(),
            address(Some("null")).is_addressed()
        );
    }

    #[test]
    fn the_three_bucket_states_round_trip_through_their_wire_names() {
        for state in [State::Unversioned, State::Enabled, State::Suspended] {
            assert_eq!(State::from_str(&state.to_string()).unwrap(), state);
        }
        assert!(State::Enabled.is_enabled() && !State::Suspended.is_enabled());
    }

    #[test]
    fn a_malformed_wire_reads_as_no_version_like_the_literal_so_callers_validate_first() {
        // `from_wire` answers `None` for two very different inputs: the
        // literal `null` (the null version — what the doc promises) and any
        // wire that fails the identifier rule. A caller that must tell "the
        // null version" from "a malformed value" — an S3 `?versionId=`
        // parameter, say — has to validate first through [`id`]/[`Id::new`];
        // only a stored-row decode may read a corrupt element as the null
        // version (its own documented self-heal).
        assert_eq!(
            Id::from_wire("null"),
            None,
            "the literal is the null version"
        );
        assert_eq!(Id::from_wire(""), None, "an empty wire is malformed");
        assert_eq!(
            Id::from_wire(&"x".repeat(MAX_ID_LEN + 1)),
            None,
            "an over-cap wire is malformed"
        );
        assert_eq!(
            Id::from_wire("null"),
            Id::from_wire(""),
            "the null literal and a malformed value collapse into the same `None`; \
             only the validated constructor tells them apart"
        );
        assert!(id("null").is_ok(), "the literal is a legal identifier");
        assert!(id("").is_err() && id("x".repeat(MAX_ID_LEN + 1)).is_err());
    }

    #[test]
    fn the_wire_cap_is_1024_bytes_inclusive_on_both_construction_paths() {
        let at_cap = "x".repeat(MAX_ID_LEN);
        let over_cap = "x".repeat(MAX_ID_LEN + 1);
        assert!(id(&at_cap).is_ok(), "1024 bytes is inside the cap");
        assert!(at_cap.parse::<Id>().is_ok(), "the derived parse agrees");
        assert!(id(&over_cap).is_err(), "1025 bytes is outside the cap");
        assert!(over_cap.parse::<Id>().is_err(), "the derived parse agrees");
        assert!(
            id("x").is_ok() && "x".parse::<Id>().is_ok(),
            "one byte is the floor"
        );
        assert!(
            id("").is_err() && "".parse::<Id>().is_err(),
            "empty is outside the cap"
        );

        // The cap counts BYTES: 512 × "é" is 1024 bytes across 512
        // characters and is accepted; 513 × "é" is 1026 bytes across 513
        // characters — under any character-count cap — and is rejected.
        let multibyte_at_cap = "é".repeat(512);
        assert_eq!(
            multibyte_at_cap.len(),
            MAX_ID_LEN,
            "the fixture sits exactly on the byte cap"
        );
        assert_eq!(
            multibyte_at_cap.chars().count(),
            512,
            "…while its character count is half the cap"
        );
        assert!(
            id(&multibyte_at_cap).is_ok(),
            "the byte cap is inclusive for multi-byte identifiers"
        );
        assert!(multibyte_at_cap.parse::<Id>().is_ok(), "both paths agree");

        let multibyte_over_cap = "é".repeat(513);
        assert_eq!(
            multibyte_over_cap.len(),
            MAX_ID_LEN + 2,
            "the fixture is two bytes over the cap"
        );
        assert_eq!(
            multibyte_over_cap.chars().count(),
            513,
            "…and stays under any character count"
        );
        assert!(
            id(&multibyte_over_cap).is_err(),
            "the constructor counts bytes, not characters"
        );
        assert!(
            multibyte_over_cap.parse::<Id>().is_err(),
            "the parse counts bytes too"
        );
    }

    #[test]
    fn marker_none_is_the_default_and_carries_no_identity() {
        // The doc claims `Marker::none()` is the `Default`; the two spellings
        // must stay the same value.
        assert_eq!(Marker::none(), Marker::default());
        assert_eq!(
            Marker::none(),
            Marker {
                id: None,
                is_delete_marker: false
            },
            "no version was created: no identity, no marker"
        );
    }
}
