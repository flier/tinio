-- How many other sessions in this database have run a statement inside the
-- window: an idle session keeps the query it last ran, so a count of one here
-- is the ownership guard's dedicated connection re-probing on its cadence.
SELECT count(*) AS "recent!" FROM pg_stat_activity
WHERE
    datname = current_database()
    AND pid <> pg_backend_pid()
    AND query_start > now() - interval '400 milliseconds'
