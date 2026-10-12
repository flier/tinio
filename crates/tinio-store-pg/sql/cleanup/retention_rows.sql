SELECT
    r.bucket AS "bucket: bucket::Name",
    r.key,
    r.seq,
    r.became_noncurrent,
    (extract(EPOCH FROM r.created) * 1000000000)::bigint AS "created_nanos!"
FROM version_rows AS r
LEFT JOIN current_slots AS s ON s.bucket = r.bucket AND s.key = r.key
WHERE s.seq IS DISTINCT FROM r.seq OR r.became_noncurrent <> 0
ORDER BY r.bucket COLLATE "C", r.key COLLATE "C", r.seq DESC
