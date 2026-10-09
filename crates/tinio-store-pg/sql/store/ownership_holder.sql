SELECT
    coalesce(a.application_name, '') AS "application_name!",
    coalesce(host(a.client_addr), '') AS "client_addr!"
FROM pg_locks l JOIN pg_stat_activity a
    ON a.pid = l.pid
WHERE
    l.locktype = 'advisory'
    AND l.database
    = (SELECT oid FROM pg_database WHERE datname = current_database())
    AND l.classid = $1
    AND l.objid = $2
    AND l.objsubid = 1
    AND l.granted
LIMIT 1
