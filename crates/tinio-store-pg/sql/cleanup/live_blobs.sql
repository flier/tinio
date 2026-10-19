SELECT
    r.bucket,
    r.key,
    r.seq,
    r.blob
FROM version_rows AS r
WHERE
    (r.bucket, r.key) > ($1::text COLLATE "C", $2::text COLLATE "C")
    OR (
        r.bucket = $1::text COLLATE "C"
        AND r.key = $2::text COLLATE "C"
        AND r.seq > $3::bigint
    )
ORDER BY r.bucket COLLATE "C", r.key COLLATE "C", r.seq
LIMIT $4
