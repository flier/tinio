//! The crate's error type, its projections, and dsn redaction.
//!
//! Every rendering is redacted: no dsn, no password, and no driver message
//! carrying URL userinfo ever reaches a log line or an S3 mapping (PG5). The
//! connection failures name the host and the database only.

use std::{io, time::Duration};

use tinio_core::{
    bucket,
    content::ContentKey,
    object,
    storage::{self, ByteRange},
};

/// What a store operation failed with.
///
/// # Examples
///
/// ```
/// use tinio_store_pg::Error;
///
/// // A database ahead of the binary is refused with the pg remedy: the
/// // version it wants and a dump/restore path — never a rebuild, because
/// // these tables hold the only copy of the bytes (PG11).
/// let err = Error::SchemaTooNew {
///     found: 2,
///     supported: 1,
/// };
/// let message = err.to_string();
/// assert!(message.contains("pg_dump"));
/// assert!(message.contains("never drop and rebuild"));
/// ```

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A connection attempt failed. The message is redacted and names the
    /// host and the database, never the password.
    #[error("postgres connect to {host}/{database}: {message}")]
    Connect {
        /// The host the connect named.
        host: String,
        /// The database the connect named.
        database: String,
        /// The driver's own diagnosis, redacted.
        message: String,
    },
    /// A statement failed. The driver's own diagnosis is kept (redacted): the
    /// bare "db error" rendering hides every constraint a caller can trip.
    #[error("postgres: {0}")]
    Database(String),
    /// A write the database refused because the session is read-only
    /// (`SQLSTATE 25006`).
    ///
    /// The engine's write-on-read rule degrades a read whose cold entry would
    /// have materialized a row only when the failure *says* read-only
    /// ([`storage::Error::Io`] carrying `io::ErrorKind::ReadOnlyFilesystem`),
    /// and a read-only instance's listing arrives here: its cold entry heals
    /// through `ObjectMeta::put_preserving`, whose contract promises the
    /// in-transaction bucket-row assertion, so the `FOR UPDATE` is refused by
    /// the session before anything is written. Spelling that refusal as a
    /// read-only I/O error is what lets the listing serve instead of failing
    /// (found by the 2026-10-13 review round).
    #[error("postgres: the session is read-only: {0}")]
    ReadOnlySession(String),
    /// The dsn asks for a TLS mode this store does not serve (PG10, R1): only
    /// `disable` and `require` are accepted, and only an explicit `disable`
    /// gives up TLS. The driver's `prefer` cannot be mapped onto anything safe
    /// — sqlx sets `accept_invalid_certs` for every mode below `verify-ca`, so
    /// a `prefer` session installs a verifier that accepts any certificate —
    /// and a mode the driver's own parse would drop is a mode the operator
    /// would believe was in force.
    #[error(
        "postgres sslmode refused: {reason}; this store accepts only `disable` and `require`, and only an explicit `disable` gives up TLS (require verifies the chain and the hostname)"
    )]
    SslMode {
        /// What was asked for and by whom, in one clause.
        reason: String,
    },
    /// A **writable** store was handed a pool whose sessions are opened
    /// read-only (PG6, layer 2). The other direction is repaired by re-opening
    /// the pool's session; this one cannot be — the pool's options carry the
    /// read-only startup option this crate appended, and a pool holds no
    /// un-redacted dsn to rebuild them from — so a store the caller asked to
    /// write with must be built over a pool that may write.
    #[error(
        "a writable store cannot be opened over a read-only pool: build it with Pool::open, or Pool::open_for(dsn, n, Access::ReadWrite), so its sessions may write"
    )]
    ReadOnlyPoolForAWriter,
    /// The server encoding is not UTF-8, so `COLLATE "C"` would order the
    /// server encoding's bytes rather than the contract's UTF-8 sequence
    /// (PG4). The store refuses to serve.
    #[error(
        "postgres server encoding is {encoding}, not UTF-8: COLLATE \"C\" would order the server encoding's bytes, not the contract's UTF-8 sequence"
    )]
    Encoding {
        /// The encoding the server reported.
        encoding: String,
    },
    /// The shared advisory lock could not be taken: another session holds it.
    /// The message names the lock's key and the holding session, which is what
    /// a second writable instance has to report (PG12).
    #[error("postgres advisory lock {key} is held by {holder}")]
    LockHeld {
        /// The advisory lock's key.
        key: i64,
        /// The holding session's identity (`application_name` and client
        /// address), or the reason this role cannot see it: PostgreSQL hides
        /// another session's details from a role that is neither a superuser
        /// nor a member of `pg_read_all_stats`.
        holder: String,
    },
    /// The migration lock was still held when the runner's bounded wait ran out
    /// (PG11): another instance is migrating this database, so this start
    /// refuses with the lock's name rather than blocking forever.
    #[error(
        "postgres advisory lock {key} was still held after {waited:?}: another instance is migrating this database"
    )]
    MigrationLockBusy {
        /// The migration lock's key.
        key: i64,
        /// How long the runner waited before refusing.
        waited: Duration,
    },
    /// The database's applied schema version is newer than this binary knows
    /// (PG11). The remedy is the pg one — a newer binary, or dump/restore —
    /// never a rebuild: these tables hold the only copy of the bytes.
    #[error(
        "postgres schema version {found} is newer than this binary's {supported}: upgrade the binary, or dump the database with pg_dump and restore it into a database this binary created — never drop and rebuild, because these tables hold the only copy of the bytes"
    )]
    SchemaTooNew {
        /// The version the database records.
        found: i64,
        /// The newest version this binary migrates to.
        supported: i64,
    },
    /// A read-only store was pointed at a database whose schema version it
    /// does not match (PG11): a read-only instance never migrates.
    #[error(
        "postgres schema version {found} does not match this binary's {supported}, and a read-only instance never migrates: run a writable instance against this database first"
    )]
    SchemaMismatch {
        /// The version the database records.
        found: i64,
        /// The version this binary requires.
        supported: i64,
    },
    /// A **writable** inspection was pointed at a database whose schema is
    /// behind this binary (PG11). The inspection path never migrates, so it
    /// cannot hand out a writable store the schema gate has not passed — and
    /// unlike the serving read-only path, refusing is the only safe answer,
    /// because the caller asked for write access.
    #[error(
        "postgres schema version {found} is behind this binary's {supported}, and an inspecting store never migrates: open a writable store with connect to migrate the database first"
    )]
    SchemaBehind {
        /// The version the database records.
        found: i64,
        /// The version this binary migrates to.
        supported: i64,
    },
    /// The checksum the database recorded for a migration disagrees with the
    /// embedded file (R5, PG11). The schema is one file per version and
    /// `SCHEMA_VERSION` says which, so an edit to a landed migration is exactly
    /// the change that silently diverges a fresh database from an existing one:
    /// the recorded digest is the only thing that can tell, and a disagreement
    /// is refused with the pg remedy rather than served.
    #[error(
        "postgres schema checksum for version {version} is {found} in this database and {expected} in this binary: the schema the database was built from is not the schema this binary carries, so upgrade the binary to the one that built it, or dump the database with pg_dump and restore it into a database this binary created — never drop and rebuild, because these tables hold the only copy of the bytes"
    )]
    SchemaChecksum {
        /// The version whose recorded checksum disagrees.
        version: i64,
        /// The digest the database recorded, as hex.
        found: String,
        /// The digest of the embedded migration, as hex.
        expected: String,
    },
    /// The bucket row is absent.
    #[error("no such bucket: {0}")]
    NoSuchBucket(bucket::Name),
    /// The object row, or the content the call named, is absent.
    #[error("no such key: {0}")]
    NoSuchKey(object::Key),
    /// The upload row is absent, or it names another key.
    #[error("no such multipart upload: {0}")]
    NoSuchUpload(String),
    /// The bucket still holds an object, an upload row or content bytes.
    #[error("bucket is not empty: {0}")]
    NotEmpty(bucket::Name),
    /// The bucket row already exists.
    #[error("already exists: {0}")]
    AlreadyExists(bucket::Name),
    /// The content key holds no bytes.
    #[error("no content for {0:?}")]
    MissingContent(ContentKey),
    /// The block is absent.
    #[error("no such block: {0}")]
    NoSuchBlock(String),
    /// A stored row cannot be decoded: an object row whose etag is
    /// domain-invalid (a point read self-heals it to absent; the strict
    /// traversal fails on it), or a content generation whose rows are not a
    /// contiguous run of chunks.
    #[error("corrupt stored row at {bucket}/{key}")]
    Corrupt {
        /// The bucket the row was read from.
        bucket: bucket::Name,
        /// The key the row is stored at.
        key: String,
    },
    /// The requested window cannot be satisfied for the content's length.
    #[error("invalid byte range: requested {range:?} on {size} bytes")]
    InvalidRange {
        /// The requested range.
        range: ByteRange,
        /// The content's length in bytes.
        size: u64,
    },
    /// Another publish moved the key's live generation between this publish's
    /// allocation and its visibility flip, or the generation's chunk run is
    /// short of the body that was staged.
    #[error("a concurrent publish moved the live generation")]
    PublishRaced,
    /// A version row already stored at the row's sequence names a different
    /// version. Overwriting it would replace another version's committed
    /// content and drop its blob out of the live set for the sweep.
    #[error("version row conflicts at {bucket}/{key} seq {seq}")]
    VersionConflict {
        /// The bucket the conflicting row is stored in.
        bucket: bucket::Name,
        /// The key the conflicting row is stored at.
        key: String,
        /// The sequence both rows name.
        seq: u64,
    },
    /// A restore names a version row that is absent, or one whose identity is
    /// not the version the caller named: the row at `seq` is the call's
    /// premise, and a slot naming a version the store no longer holds must
    /// never be written.
    #[error("version restore refused at {bucket}/{key} seq {seq}")]
    RestoreConflict {
        /// The bucket the restored row was looked up in.
        bucket: bucket::Name,
        /// The key the restored row was looked up at.
        key: String,
        /// The sequence the caller named.
        seq: u64,
    },
    /// A version slot was asked to move backwards: the sequence the caller
    /// named is lower than the one the key's slot already records. A slot
    /// never moves backwards (an equal sequence is the crash window's re-run).
    #[error("version slot would move backwards at {bucket}/{key} seq {seq}")]
    SlotRewind {
        /// The bucket the slot belongs to.
        bucket: bucket::Name,
        /// The key the slot belongs to.
        key: String,
        /// The refused (lower) sequence the caller asked to advance to.
        seq: u64,
    },
    /// A content-stream read failed while a digest compare re-read the body.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// The contract's own failure, absorbed whole: the projection of a
    /// not-found or a refusal would otherwise degrade to `Io` (D22).
    #[error(transparent)]
    Storage(#[from] storage::Error),
}

impl Error {
    /// A connection failure carrying the redacted driver message beside the
    /// non-secret host and database from the parsed dsn.
    pub(crate) fn connect(host: &str, database: &str, raw: &str, dsn: &str) -> Self {
        Self::Connect {
            host: host.to_owned(),
            database: database.to_owned(),
            message: redact_dsn(dsn, raw),
        }
    }

    /// A database failure carrying the redacted driver message.
    pub(crate) fn database(raw: &str, dsn: &str) -> Self {
        Self::Database(redact_dsn(dsn, raw))
    }
}

/// `read_only_sql_transaction`: PostgreSQL's refusal of a write on a session
/// whose `default_transaction_read_only` is on. The one SQLSTATE this store
/// reads structurally, because the engine's write-on-read degradation keys on
/// it ([`Error::ReadOnlySession`]).
const READ_ONLY_SQLSTATE: &str = "25006";

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        let raw = match &err {
            sqlx::Error::Database(db) => format!(
                "{} (code {}, constraint {:?}, table {:?})",
                db.message(),
                db.code().unwrap_or_default(),
                db.constraint(),
                db.table()
            ),
            other => other.to_string(),
        };
        // No dsn is in scope on this hop, so the redaction is the userinfo
        // strip: the password never reaches a rendering even when the driver
        // embedded the connection string in its own message.
        let message = redact_userinfo(&raw);
        // A read-only session's refusal is the one driver error this store
        // reads structurally rather than merely keeping: `25006` is
        // `read_only_sql_transaction`, and the engine's write-on-read
        // degradation keys on it (see `Error::ReadOnlySession`).
        if matches!(&err, sqlx::Error::Database(db) if db.code().as_deref() == Some(READ_ONLY_SQLSTATE))
        {
            return Self::ReadOnlySession(message);
        }
        Self::Database(message)
    }
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::NoSuchBucket(name) => storage::Error::NoSuchBucket(name),
            Error::NoSuchKey(key) => storage::Error::NoSuchKey(key),
            Error::NoSuchUpload(id) => storage::Error::NoSuchUpload(id),
            Error::NotEmpty(name) => storage::Error::NotEmpty(name),
            Error::AlreadyExists(name) => storage::Error::AlreadyExists(name),
            Error::InvalidRange { range, size } => storage::Error::InvalidRange { range, size },
            Error::MissingContent(ContentKey::Object(key)) => storage::Error::NoSuchKey(key),
            Error::MissingContent(ContentKey::Part { upload_id, .. }) => {
                storage::Error::NoSuchUpload(upload_id)
            }
            Error::NoSuchBlock(hash) => storage::Error::Io(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no such block: {hash}"),
            )),
            Error::Io(err) => storage::Error::Io(err),
            // A read-only refusal keeps its kind, because that is the whole
            // point of the variant: the engine's `served_without_writing`
            // degrades only on `ReadOnlyFilesystem` (see `ReadOnlySession`).
            Error::ReadOnlySession(message) => {
                storage::Error::Io(io::Error::new(io::ErrorKind::ReadOnlyFilesystem, message))
            }
            Error::Storage(inner) => inner,
            other => storage::Error::Io(io::Error::other(other.to_string())),
        }
    }
}

/// Redact `raw` for `dsn`: the dsn itself, the dsn's password, and any URL
/// userinfo password are replaced.
pub(crate) fn redact_dsn(dsn: &str, raw: &str) -> String {
    let mut out = raw.replace(dsn, "<dsn>");
    if let Some(password) = dsn_password(dsn)
        && !password.is_empty()
    {
        out = out.replace(&password, "***");
    }
    redact_userinfo(&out)
}

/// The password in a URL dsn's userinfo (`scheme://user:pass@host`).
fn url_userinfo_password(dsn: &str) -> Option<&str> {
    let (_, rest) = dsn.split_once("://")?;
    let authority = rest.split(['/', '?']).next().unwrap_or(rest);
    let (userinfo, _) = authority.rsplit_once('@')?;
    let (_, password) = userinfo.split_once(':')?;
    Some(password)
}

/// The password of a keyword dsn token (`password=...` or `password='...'`).
/// The key match ignores ASCII case, so `Password=` is the same token.
fn keyword_password(dsn: &str) -> Option<&str> {
    dsn.split_whitespace().find_map(|token| {
        let (key, value) = token.split_once('=')?;
        key.eq_ignore_ascii_case("password")
            .then(|| value.trim_matches('\''))
    })
}

/// The password a dsn carries, in URL or keyword form. Both forms share the
/// splitters [`redact_userinfo`] uses.
fn dsn_password(dsn: &str) -> Option<String> {
    url_userinfo_password(dsn)
        .or_else(|| keyword_password(dsn))
        .filter(|password| !password.is_empty())
        .map(str::to_owned)
}

/// `password=` in any ASCII case. The matched slice keeps the caller's spelling.
const PASSWORD_EQ: &str = "password=";

/// The byte index of a `password=` key, ignoring ASCII case.
fn find_password_eq(haystack: &str) -> Option<usize> {
    let needle = PASSWORD_EQ.as_bytes();
    haystack
        .as_bytes()
        .windows(needle.len())
        .position(|window| {
            window
                .iter()
                .zip(needle)
                .all(|(got, want)| got.eq_ignore_ascii_case(want))
        })
}

/// Replace one `password=` token's value with `***`, including a single-quoted
/// value, and return the unconsumed tail.
fn mask_password_token(rest: &str) -> (String, &str) {
    if let Some(quoted) = rest.strip_prefix('\'')
        && let Some(end) = quoted.find('\'')
    {
        return ("***".to_owned(), &quoted[end + 1..]);
    }
    let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    ("***".to_owned(), &rest[end..])
}

/// Strip every URL userinfo password and every `password=` value from a message.
/// The keyword match ignores ASCII case and keeps the key's original spelling.
pub(crate) fn redact_userinfo(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(index) = rest.find("://") {
        let (head, tail) = rest.split_at(index + 3);
        out.push_str(head);
        let end = tail
            .find(|c: char| c.is_whitespace() || c == '/' || c == '\'')
            .unwrap_or(tail.len());
        let authority = &tail[..end];
        match authority.rsplit_once('@') {
            Some((userinfo, host)) => match userinfo.split_once(':') {
                Some((user, _password)) => {
                    out.push_str(user);
                    out.push_str(":***@");
                    out.push_str(host);
                }
                None => out.push_str(authority),
            },
            None => out.push_str(authority),
        }
        rest = &tail[end..];
    }
    out.push_str(rest);
    let marked = out;
    let mut out = String::with_capacity(marked.len());
    let mut rest = marked.as_str();
    while let Some(index) = find_password_eq(rest) {
        out.push_str(&rest[..index]);
        let key_end = index + PASSWORD_EQ.len();
        out.push_str(&rest[index..key_end]);
        let (masked, tail) = mask_password_token(&rest[key_end..]);
        out.push_str(&masked);
        rest = tail;
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    // The tests address the crate by absolute path on purpose: a shortened path
    // would not prove the public one.
    #![allow(clippy::absolute_paths)]

    use super::{Error, redact_dsn, redact_userinfo};
    use crate::_core::storage::ByteRange;

    #[test]
    fn a_url_dsn_loses_its_password() {
        let dsn = "postgres://tinio:sup3rsecret@127.0.0.1:55432/tinio";
        let redacted = redact_dsn(dsn, dsn);
        assert_eq!(redacted, "<dsn>");
        assert!(!redacted.contains("sup3rsecret"));
    }

    #[test]
    fn a_driver_message_loses_its_userinfo_password() {
        let raw = "error connecting to postgres://tinio:sup3rsecret@127.0.0.1:55432/tinio";
        let redacted = redact_userinfo(raw);
        assert!(redacted.contains("tinio:***@127.0.0.1:55432"), "{redacted}");
        assert!(!redacted.contains("sup3rsecret"), "{redacted}");
    }

    #[test]
    fn a_keyword_dsn_loses_its_password() {
        let dsn = "host=127.0.0.1 user=tinio password=sup3rsecret dbname=tinio";
        let redacted = redact_dsn(dsn, "connection refused");
        assert_eq!(redacted, "connection refused");
        let redacted = redact_dsn(dsn, dsn);
        assert!(!redacted.contains("sup3rsecret"), "{redacted}");
    }

    /// A projection case: the variant to project, and the check the projection
    /// must satisfy.
    type ProjectionCase = (Error, fn(&crate::_core::storage::Error) -> bool);

    #[test]
    fn the_projection_keeps_the_contract_variants() {
        use tinio_core::{bucket, object, storage};
        let name = bucket::name("data").unwrap();
        let key = object::key("a.txt").unwrap();
        let cases: Vec<ProjectionCase> = vec![
            (Error::NoSuchBucket(name.clone()), |err| {
                matches!(err, storage::Error::NoSuchBucket(_))
            }),
            (Error::NoSuchKey(key.clone()), |err| {
                matches!(err, storage::Error::NoSuchKey(_))
            }),
            (Error::NotEmpty(name.clone()), |err| {
                matches!(err, storage::Error::NotEmpty(_))
            }),
            (Error::AlreadyExists(name), |err| {
                matches!(err, storage::Error::AlreadyExists(_))
            }),
        ];
        for (error, matches_variant) in cases {
            let projected: storage::Error = error.into();
            assert!(matches_variant(&projected), "{projected}");
        }
    }

    /// The variants a contract call can see, projected: the two halves of the
    /// content key space answer the two contract shapes the S3 layer classifies
    /// on, a missing block is `NotFound` rather than a bare I/O error, and the
    /// corrupt-row and range failures keep their identity.
    #[test]
    fn the_projection_keeps_the_content_block_and_range_variants() {
        use tinio_core::{bucket, content::ContentKey, multipart, object, storage};

        let key = object::key("a.txt").unwrap();
        let upload_id = "6f1d0f4a-5c2e-4b8a-9f31-2d7c9e5a1b40".to_owned();
        let projected: storage::Error =
            Error::MissingContent(ContentKey::Object(key.clone())).into();
        assert!(
            matches!(&projected, storage::Error::NoSuchKey(named) if named == &key),
            "{projected}"
        );
        let projected: storage::Error = Error::MissingContent(ContentKey::Part {
            upload_id: upload_id.clone(),
            number: multipart::part_number(2).expect("a valid part number"),
        })
        .into();
        assert!(
            matches!(&projected, storage::Error::NoSuchUpload(named) if named == &upload_id),
            "{projected}"
        );
        let rendered = Error::MissingContent(ContentKey::Object(key.clone())).to_string();
        assert!(rendered.contains("no content for"), "{rendered}");

        let hash = "0".repeat(64);
        let projected: storage::Error = Error::NoSuchBlock(hash.clone()).into();
        let storage::Error::Io(io) = projected else {
            panic!("a missing block must project onto an I/O error, got {projected:?}");
        };
        assert_eq!(io.kind(), std::io::ErrorKind::NotFound);
        assert!(io.to_string().contains(&hash), "{io}");

        let corrupt = Error::Corrupt {
            bucket: bucket::name("data").unwrap(),
            key: "o:a.txt".to_owned(),
        };
        assert_eq!(corrupt.to_string(), "corrupt stored row at data/o:a.txt");
        let projected: storage::Error = corrupt.into();
        let storage::Error::Io(io) = projected else {
            panic!("a corrupt row must project onto an I/O error, got {projected:?}");
        };
        assert!(io.to_string().contains("corrupt stored row"), "{io}");

        let projected: storage::Error = Error::InvalidRange {
            range: ByteRange::From(4),
            size: 4,
        }
        .into();
        assert!(
            matches!(&projected, storage::Error::InvalidRange { size: 4, .. }),
            "{projected}"
        );
    }

    #[test]
    fn a_password_outside_the_dsn_is_redacted_too() {
        // The driver may name the password it refused; the dsn's own password
        // is replaced wherever it appears in the message.
        let dsn = "postgres://tinio:sup3rsecret@db.internal:5432/tinio";
        assert_eq!(
            redact_dsn(dsn, "password authentication failed for sup3rsecret"),
            "password authentication failed for ***"
        );
    }

    #[test]
    fn a_dsn_without_a_password_redacts_nothing_else() {
        // The password replacement is skipped when there is no password, and a
        // message that never carried one is returned as the driver wrote it.
        assert_eq!(
            redact_dsn("host=db.internal user=tinio", "connection refused"),
            "connection refused"
        );
        // An authority with no userinfo, and one with a user but no password,
        // are left standing: there is nothing to mask.
        assert_eq!(
            redact_userinfo("connecting to postgres://db.internal:5432/tinio"),
            "connecting to postgres://db.internal:5432/tinio"
        );
        assert_eq!(
            redact_userinfo("connecting to postgres://tinio@db.internal:5432/tinio"),
            "connecting to postgres://tinio@db.internal:5432/tinio"
        );
    }

    #[test]
    fn a_mixed_case_password_key_is_masked() {
        assert_eq!(
            redact_userinfo("host=db Password=sup3rsecret user=tinio"),
            "host=db Password=*** user=tinio"
        );
        let dsn = "host=db Password=sup3rsecret user=tinio";
        let redacted = redact_dsn(dsn, "authentication failed for Password=sup3rsecret");
        assert!(!redacted.contains("sup3rsecret"), "{redacted}");
        assert!(redacted.contains("Password=***"), "{redacted}");
    }

    #[test]
    fn a_quoted_keyword_password_is_masked_whole() {
        // A keyword dsn quotes a password that holds a space; the whole quoted
        // value goes, not just its first token.
        assert_eq!(
            redact_userinfo("host=db password='s3 cret' user=tinio"),
            "host=db password=*** user=tinio"
        );
    }

    #[test]
    fn the_remedy_is_not_a_rebuild() {
        let err = Error::SchemaTooNew {
            found: 9,
            supported: 3,
        };
        let message = err.to_string();
        assert!(message.contains("pg_dump"), "{message}");
        assert!(message.contains("restore"), "{message}");
        // The remedy must never prescribe a rebuild; it names one only to forbid
        // it, so the prohibition is what is checked for.
        assert!(message.contains("never drop and rebuild"), "{message}");
        assert!(!message.contains("delete"), "{message}");
    }
}
