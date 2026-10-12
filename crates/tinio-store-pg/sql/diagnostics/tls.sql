SELECT
    s.ssl AS "enabled!",
    s.version AS "version",
    s.cipher AS "cipher",
    s.bits AS "bits"
FROM pg_stat_ssl AS s
WHERE s.pid = pg_backend_pid()
