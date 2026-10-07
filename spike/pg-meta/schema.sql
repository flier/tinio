-- pg-meta spike schema (P1 Task 2). The seed for crates/tinio-store-pg (D25).
--
-- Every key column is text COLLATE "C": the contracts order by the key's
-- UTF-8 byte sequence (spec property 2), the database's default collation is
-- en_US.utf8, and "C" orders the bytes of the server encoding -- which is why
-- the connect-time UTF-8 check (PG4) is load-bearing. Every ORDER BY repeats
-- the collation.
--
-- The forms below are the ones the pg design defers to this spike
-- (PG18-PG23); each is recorded in docs/superpowers/pg-meta-spike-findings.md.

-- The metadata plane

CREATE TABLE IF NOT EXISTS buckets (
    name    text COLLATE "C" PRIMARY KEY,
    created bigint NOT NULL,
    tags    text NOT NULL,
    owner   text NOT NULL,
    acl     text NOT NULL,
    cors    text NOT NULL
);

CREATE TABLE IF NOT EXISTS object_meta (
    bucket   text COLLATE "C" NOT NULL,
    key      text COLLATE "C" NOT NULL,
    etag     text NOT NULL,
    size     bigint NOT NULL,
    mtime    bigint NOT NULL,
    identity bigint NOT NULL,
    tags     text NOT NULL,
    checksum text NOT NULL,
    owner    text NOT NULL,
    acl      text NOT NULL,
    PRIMARY KEY (bucket, key)
);

CREATE TABLE IF NOT EXISTS object_parts (
    bucket    text COLLATE "C" NOT NULL,
    key       text COLLATE "C" NOT NULL,
    number    bigint NOT NULL,
    size      bigint NOT NULL,
    algorithm text NOT NULL,
    value     text NOT NULL,
    PRIMARY KEY (bucket, key, number)
);

CREATE TABLE IF NOT EXISTS uploads (
    bucket       text COLLATE "C" NOT NULL,
    upload_id    text COLLATE "C" NOT NULL,
    key          text COLLATE "C" NOT NULL,
    initiated_at bigint NOT NULL,
    tags         text NOT NULL,
    owner        text NOT NULL,
    acl          text NOT NULL,
    PRIMARY KEY (bucket, upload_id)
);

CREATE TABLE IF NOT EXISTS parts (
    bucket    text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    number    bigint NOT NULL,
    etag      text NOT NULL,
    PRIMARY KEY (bucket, upload_id, number)
);

CREATE TABLE IF NOT EXISTS part_checksums (
    bucket    text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    number    bigint NOT NULL,
    algorithm text NOT NULL,
    value     text NOT NULL,
    PRIMARY KEY (bucket, upload_id, number)
);

CREATE TABLE IF NOT EXISTS upload_checksums (
    bucket    text COLLATE "C" NOT NULL,
    upload_id text COLLATE "C" NOT NULL,
    algorithm text NOT NULL,
    kind      text NOT NULL,
    PRIMARY KEY (bucket, upload_id)
);

-- The version plane

CREATE TABLE IF NOT EXISTS bucket_versioning (
    bucket text COLLATE "C" PRIMARY KEY,
    state  text NOT NULL
);

CREATE TABLE IF NOT EXISTS version_rows (
    bucket            text COLLATE "C" NOT NULL,
    key               text COLLATE "C" NOT NULL,
    seq               bigint NOT NULL,
    id                text NOT NULL,
    blob              text NOT NULL,
    size              bigint NOT NULL,
    etag              text NOT NULL,
    created           bigint NOT NULL,
    became_noncurrent bigint NOT NULL,
    kind              text NOT NULL,
    tags              text NOT NULL,
    checksum          text NOT NULL,
    owner             text NOT NULL,
    acl               text NOT NULL,
    PRIMARY KEY (bucket, key, seq)
);

CREATE TABLE IF NOT EXISTS version_index (
    bucket text COLLATE "C" NOT NULL,
    key    text COLLATE "C" NOT NULL,
    id     text NOT NULL,
    seq    bigint NOT NULL,
    PRIMARY KEY (bucket, key, id)
);

-- The key's current slot. It cannot ride object_meta: the contract's object
-- row carries no version columns (D37), and the conformance suite advances a
-- slot for a key that has no object row at all. Pinned by the spike (finding
-- F3 in the findings document).
CREATE TABLE IF NOT EXISTS current_slots (
    bucket     text COLLATE "C" NOT NULL,
    key        text COLLATE "C" NOT NULL,
    version_id text,
    seq        bigint NOT NULL,
    PRIMARY KEY (bucket, key)
);

-- The content plane (PG18, PG20, PG24)

-- The generation model: content holds the chunks of every generation and
-- content_heads records which generation is live. publish writes chunks for
-- a fresh seq and then moves live_seq in one statement; a partial publish
-- never moves it, so it is invisible (PG18/PG22's incompleteness marker).
CREATE TABLE IF NOT EXISTS content_heads (
    bucket   text COLLATE "C" NOT NULL,
    kind     text NOT NULL CHECK (kind IN ('object', 'part')),
    ckey     text COLLATE "C" NOT NULL,
    next_seq bigint NOT NULL,
    live_seq bigint,
    live_len bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, kind, ckey)
);

CREATE TABLE IF NOT EXISTS content (
    bucket   text COLLATE "C" NOT NULL,
    kind     text NOT NULL CHECK (kind IN ('object', 'part')),
    ckey     text COLLATE "C" NOT NULL,
    seq      bigint NOT NULL,
    chunk_no integer NOT NULL,
    chunk    bytea NOT NULL,
    PRIMARY KEY (bucket, kind, ckey, seq, chunk_no)
);

ALTER TABLE content ALTER COLUMN chunk SET STORAGE EXTERNAL;

-- The block plane (PG14/PG19): the form the spike pins for PgBlocks, which
-- P3 implements. A block's rows carry its declared length and the publish's
-- comparable timestamp, and a publish fills gaps with ON CONFLICT DO UPDATE
-- so an interrupted publish is never served as a truncated block.
CREATE TABLE IF NOT EXISTS blocks (
    hash      text COLLATE "C" NOT NULL,
    seq       integer NOT NULL,
    chunk     bytea NOT NULL,
    len       bigint NOT NULL,
    published bigint NOT NULL,
    PRIMARY KEY (hash, seq)
);

ALTER TABLE blocks ALTER COLUMN chunk SET STORAGE EXTERNAL;

-- The applied schema version (the seed for P3's migrations)

CREATE TABLE IF NOT EXISTS schema_version (
    id         integer PRIMARY KEY,
    applied_at bigint NOT NULL,
    note       text NOT NULL
);
