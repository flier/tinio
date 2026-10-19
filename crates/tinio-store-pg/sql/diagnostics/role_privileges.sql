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
