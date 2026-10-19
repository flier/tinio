SELECT count(*) AS "recent!" FROM pg_stat_activity
WHERE
    datname = current_database()
    AND pid <> pg_backend_pid()
    AND query_start > now() - interval '400 milliseconds'
