SELECT
    current_user::text AS "role!",
    coalesce(current_schema(), '')::text AS "schema!",
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
    n.nspname = current_schema()
    AND c.relkind = 'r'
