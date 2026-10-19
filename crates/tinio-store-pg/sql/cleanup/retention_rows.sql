SELECT
    r.bucket AS "bucket: bucket::Name",
    r.key,
    r.seq,
    r.became_noncurrent,
    (extract(EPOCH FROM r.created) * 1000000000)::bigint AS "created_nanos!"
FROM version_rows AS r
LEFT JOIN current_slots AS s ON s.bucket = r.bucket AND s.key = r.key
WHERE
    (
        (s.seq IS NOT NULL AND s.seq <> r.seq)
        OR r.became_noncurrent <> 0
    )
    AND (
        (r.bucket, r.key) > ($1::text COLLATE "C", $2::text COLLATE "C")
        OR (
            r.bucket = $1::text COLLATE "C"
            AND r.key = $2::text COLLATE "C"
            AND r.seq < $3::bigint
        )
    )
ORDER BY r.bucket COLLATE "C", r.key COLLATE "C", r.seq DESC
LIMIT $4
