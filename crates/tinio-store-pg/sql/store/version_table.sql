-- The DDL that records an applied migration. It is created outside the
-- migration list so version 1 can record itself.
CREATE TABLE IF NOT EXISTS schema_version (
    id integer PRIMARY KEY,
    applied_at bigint NOT NULL,
    note text NOT NULL
)
