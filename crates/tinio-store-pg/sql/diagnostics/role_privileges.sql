-- The inspecting role's DML grants over the store's own relations (PG6, the
-- privilege layer). `can_write` is true when the role holds INSERT, UPDATE or
-- DELETE on **any** relation of any schema the store's tables live in: one
-- relation a reader can write is the guarantee broken.
--
-- The scope is the **store's bookkeeping table**, not the session's
-- `search_path`. `current_schema()` answers for whichever schema the session
-- resolves first, so a session opened with `options=-c search_path=...` that
-- does not name the store's schema read zero rows, `bool_or` answered NULL, the
-- `coalesce` below printed `false`, and the diagnosis certified a role it had
-- not looked at (the 2026-10-13 review). Every schema holding a
-- `schema_version` table is a schema this store owns relations in, and the
-- answer names them all, so a reader can see what was checked.
--
-- The relation kinds are the ones a role could still write through: an ordinary
-- table, a partitioned table, a view (DML on an automatically updatable view
-- writes through to its base table) and a foreign table.
WITH store_schemas AS (
    SELECT n.nspname
    FROM pg_class c JOIN pg_namespace n
        ON n.oid = c.relnamespace
    WHERE c.relname = 'schema_version' AND c.relkind IN ('r', 'p')
)

SELECT
    current_user::text AS "role!",
    coalesce(
        (
            SELECT string_agg(s.nspname, ', ' ORDER BY s.nspname)
            FROM store_schemas s
        ),
        ''
    )::text AS "schemas!",
    coalesce(
        bool_or(
            has_table_privilege(current_user, c.oid, 'INSERT')
            OR has_table_privilege(current_user, c.oid, 'UPDATE')
            OR has_table_privilege(current_user, c.oid, 'DELETE')
        ),
        FALSE
    ) AS "can_write!"
FROM pg_class c JOIN pg_namespace n
    ON n.oid = c.relnamespace
WHERE
    n.nspname IN (SELECT nspname FROM store_schemas)
    AND c.relkind IN ('r', 'p', 'v', 'f')
