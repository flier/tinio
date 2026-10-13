SELECT 1 AS "locked!"
FROM (SELECT pg_advisory_lock(hashtext($1), hashtext($2))) AS held
