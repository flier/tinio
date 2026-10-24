SELECT l.mode, l.retain_until, l.legal_hold
FROM version_object_lock AS l
WHERE
    l.bucket = $1
    AND EXISTS (
        SELECT 1
        FROM version_lock_owned AS o
        WHERE
            o.bucket = l.bucket
            AND o.key = l.key
            AND o.version_id = l.version_id
    )
ORDER BY l.key, l.version_id
FOR UPDATE OF l
