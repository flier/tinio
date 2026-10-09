SELECT count(*) AS "held!" FROM pg_locks
WHERE
    locktype = 'advisory'
    AND classid = $1
    AND objid = $2
    AND objsubid = 1
    AND pid = pg_backend_pid()
