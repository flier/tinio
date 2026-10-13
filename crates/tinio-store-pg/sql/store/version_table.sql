-- The DDL that records an applied migration. It is created outside the
-- migration list so version 1 can record itself.
--
-- checksum is sha-384 of the migration's own text, hex-rendered, recorded by
-- the runner as it applies the step (R5). A store opening this database
-- compares the row against the migration embedded in the binary, so an edit to
-- a landed migration -- the one change that silently diverges a fresh database
-- from an existing one -- is refused rather than served. The default is for a
-- row written by hand (a test's planted version) and never matches an embedded
-- digest, so a row that does not record one is refused rather than trusted.
CREATE TABLE IF NOT EXISTS schema_version (
    id integer PRIMARY KEY,
    applied_at bigint NOT NULL,
    note text NOT NULL,
    checksum text NOT NULL DEFAULT ''
)
