CREATE TABLE IF NOT EXISTS schema_version (
    id integer PRIMARY KEY,
    applied_at bigint NOT NULL,
    note text NOT NULL,
    checksum text NOT NULL DEFAULT ''
)
