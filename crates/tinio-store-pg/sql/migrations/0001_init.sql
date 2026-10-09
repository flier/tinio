-- The initial tinio-store-pg schema (migration v1). Every key column is
-- text COLLATE "C": the contracts order by the key's UTF-8 byte sequence,
-- the database's default collation is not "C", and "C" orders the bytes of
-- the server encoding -- which is why the connect-time UTF-8 check (PG4) is
-- load-bearing. Every ordered read repeats the collation.
--
-- The wall-clock columns -- buckets.created, object_meta.mtime,
-- uploads.initiated_at, version_rows.created, blocks.published -- are
-- PostgreSQL's own timestamptz: the database keeps the instant, sqlx decodes it
-- natively, and the domain's SystemTime meets it once, in src/timestamp.rs.
-- version_rows.became_noncurrent stays bigint: the contract exposes it as a raw
-- u64 stamp, not an instant.
--
-- The four tag-set columns -- buckets.tags, object_meta.tags, uploads.tags and
-- version_rows.tags -- are PostgreSQL's own jsonb: a JSON object of tag key to
-- tag value, the empty set stored as `{}`. tinio_core::object::Tags carries the
-- matching sqlx mapping behind its `sqlx` feature, so a statement binds and a
-- row field decodes the contract type itself. jsonb compares by value, which is
-- exactly what `tags = excluded.tags` and `tags IS DISTINCT FROM excluded.tags`
-- mean.
--
-- The four ACL columns -- buckets.acl, object_meta.acl, uploads.acl and
-- version_rows.acl -- are the composite types declared just below: acl_grantee
-- is one grantee (its variant tag and its value), acl_grant pairs it with the
-- permission and acl adds the owner, so the contract's Acl rides one column
-- and there is no separate owner column. An absent owner is a NULL attribute;
-- the Rust mapping lives on the contract types themselves -- tinio_core::acl's
-- Acl, Grant, Grantee and Permission, behind that crate's `sqlx` feature --
-- so the store binds and decodes them with no mirror of its own.
--
-- The closed-set text columns -- version_rows.kind, content_heads.kind,
-- content.kind and bucket_versioning.state -- are PostgreSQL's own enum types:
-- each label is exactly the wire string the code already writes, so no stored
-- or contract meaning changes, and the database now refuses a value outside
-- the set. The three types are declared next to the tables that use them; the
-- version and bucket-state mappings live on tinio_core::meta::version::Kind and
-- tinio_core::version::State (behind that crate's `sqlx` feature), and the
-- content plane's own discriminator stays beside its only plane
-- (tinio-store-pg::content), because no contract type names that set.
-- The three checksum-algorithm columns stay
-- text: the algorithm set is the S3 API's own registry (which grows with the
-- API), so the columns keep the API's own labels. They are still bound and
-- decoded as tinio_core::checksum::Algorithm through that type's `sqlx` feature
-- mapping, which spells the same uppercase wire. Two columns keep the raw wire
-- string instead: upload_checksums.kind carries the empty string for "no
-- derivation fixed", and object_parts.algorithm carries it for "no per-part
-- checksum" (the column is NOT NULL, so the empty sentinel has no NULL to
-- become), values no closed set can hold.

-- The ACL composite: one grantee is its own composite and one grant pairs it
-- with a permission, so the ACL adds its owner. grantee_kind tags the two
-- Grantee variants (canonical for a canonical account, group for one of the
-- three documented group URIs), grantee_value is the canonical ID or the group
-- URI, and grants is the variable array (an array of a composite rides the
-- derived array support).
CREATE TYPE acl_grantee AS (
    grantee_kind text,
    grantee_value text
);

CREATE TYPE acl_grant AS (
    grantee acl_grantee,
    permission text
);

CREATE TYPE acl AS (
    owner text,
    grants acl_grant[]
);

CREATE TABLE IF NOT EXISTS buckets (
    name text COLLATE "C" PRIMARY KEY,
    created timestamptz NOT NULL,
    tags jsonb NOT NULL,
    acl acl NOT NULL,
    cors text NOT NULL
);

CREATE TABLE IF NOT EXISTS object_meta (
    bucket text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    etag text NOT NULL,
    size bigint NOT NULL,
    mtime timestamptz NOT NULL,
    identity bigint NOT NULL,
    tags jsonb NOT NULL,
    checksum text NOT NULL,
    acl acl NOT NULL,
    PRIMARY KEY (bucket, key)
);

CREATE TABLE IF NOT EXISTS object_parts (
    bucket text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    number bigint NOT NULL,
    size bigint NOT NULL,
    algorithm text NOT NULL,
    value text NOT NULL,
    PRIMARY KEY (bucket, key, number)
);

CREATE TABLE IF NOT EXISTS uploads (
    bucket text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    initiated_at timestamptz NOT NULL,
    tags jsonb NOT NULL,
    acl acl NOT NULL,
    PRIMARY KEY (bucket, upload_id)
);

CREATE TABLE IF NOT EXISTS parts (
    bucket text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    number bigint NOT NULL,
    etag text NOT NULL,
    PRIMARY KEY (bucket, upload_id, number)
);

CREATE TABLE IF NOT EXISTS part_checksums (
    bucket text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    number bigint NOT NULL,
    algorithm text NOT NULL,
    value text NOT NULL,
    PRIMARY KEY (bucket, upload_id, number)
);

CREATE TABLE IF NOT EXISTS upload_checksums (
    bucket text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    algorithm text NOT NULL,
    kind text NOT NULL,
    PRIMARY KEY (bucket, upload_id)
);

-- The bucket's versioning state is a closed set of three, so PostgreSQL's own
-- enum holds it. The labels are `tinio_core::version::State`'s Display wires,
-- which that type's `sqlx` mapping binds directly (`PascalCase`).
CREATE TYPE bucket_state AS ENUM ('Unversioned', 'Enabled', 'Suspended');

CREATE TABLE IF NOT EXISTS bucket_versioning (
    bucket text COLLATE "C" PRIMARY KEY,
    state bucket_state NOT NULL
);

-- A version row is either an object version or a delete marker; the labels are
-- `tinio_core::meta::version::Kind`'s wires, bound by that type's `sqlx`
-- mapping (`lowercase`).
CREATE TYPE version_kind AS ENUM ('object', 'marker');

CREATE TABLE IF NOT EXISTS version_rows (
    bucket text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    seq bigint NOT NULL,
    id text NOT NULL,
    blob text NOT NULL,
    size bigint NOT NULL,
    etag text NOT NULL,
    created timestamptz NOT NULL,
    became_noncurrent bigint NOT NULL,
    kind version_kind NOT NULL,
    tags jsonb NOT NULL,
    checksum text NOT NULL,
    acl acl NOT NULL,
    PRIMARY KEY (bucket, key, seq)
);

CREATE TABLE IF NOT EXISTS version_index (
    bucket text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    id text NOT NULL,
    seq bigint NOT NULL,
    PRIMARY KEY (bucket, key, id)
);

-- The key's current slot. It cannot ride object_meta: the contract's object row
-- carries no version columns, and the suite advances a slot for a key that has
-- no object row at all (the spike's finding F3).
CREATE TABLE IF NOT EXISTS current_slots (
    bucket text COLLATE "C" NOT NULL,
    key text COLLATE "C" NOT NULL,
    version_id text,
    seq bigint NOT NULL,
    PRIMARY KEY (bucket, key)
);

-- The content plane (PG18, PG20, PG24). content holds the chunks of every
-- generation; content_heads records which generation is live. publish writes
-- chunks for a fresh seq and then moves live_seq in one statement; a partial
-- publish never moves it, so it is invisible.
-- Content is keyed by a discriminator that is part of every primary key: an
-- object's chunks or a multipart part's chunks. The labels are the strings the
-- content plane writes; its own `ContentKind` owns the mapping, because no
-- contract type names the set.
CREATE TYPE content_kind AS ENUM ('object', 'part');

CREATE TABLE IF NOT EXISTS content_heads (
    bucket text COLLATE "C" NOT NULL,
    kind content_kind NOT NULL,
    ckey text COLLATE "C" NOT NULL,
    next_seq bigint NOT NULL,
    live_seq bigint,
    live_len bigint NOT NULL DEFAULT 0,
    -- The caller's publish identity, stored as a bigint bit pattern. NULL is an
    -- untagged publish: a tagged removal does not take it.
    publish_id bigint,
    PRIMARY KEY (bucket, kind, ckey)
);

CREATE TABLE IF NOT EXISTS content (
    bucket text COLLATE "C" NOT NULL,
    kind content_kind NOT NULL,
    ckey text COLLATE "C" NOT NULL,
    seq bigint NOT NULL,
    chunk_no integer NOT NULL,
    chunk bytea NOT NULL,
    PRIMARY KEY (bucket, kind, ckey, seq, chunk_no)
);

ALTER TABLE content ALTER COLUMN chunk SET STORAGE EXTERNAL;

-- The block plane (PG14, PG19): a block's chunks are 8 MiB rows, and every row
-- carries the block's declared length and the publish's comparable timestamp.
CREATE TABLE IF NOT EXISTS blocks (
    hash text COLLATE "C" NOT NULL,
    seq integer NOT NULL,
    chunk bytea NOT NULL,
    len bigint NOT NULL,
    published timestamptz NOT NULL,
    PRIMARY KEY (hash, seq)
);

ALTER TABLE blocks ALTER COLUMN chunk SET STORAGE EXTERNAL;
