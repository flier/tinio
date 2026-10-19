SELECT
    u.bucket,
    u.upload_id
FROM uploads AS u
WHERE
    u.initiated_at < $1
    AND NOT EXISTS (
        SELECT 1
        FROM parts AS p
        WHERE
            p.bucket = u.bucket
            AND p.upload_id = u.upload_id
            AND p.modified_at >= $2
    )
    AND u.bucket <> ALL ($3)
ORDER BY u.bucket COLLATE "C", u.upload_id COLLATE "C"
LIMIT $4
