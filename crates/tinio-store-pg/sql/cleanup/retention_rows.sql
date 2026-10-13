-- One keyset page of the retention scan: `$1`..`$3` are the cursor -- the last
-- `(bucket, key, seq)` the previous page returned, exclusive -- and `$4` the
-- page size, so a pass holds one page of the version relation rather than every
-- history row in the store. The order is newest-first per key, which the
-- reader's retention rule needs (position 0 is the newest), so a page boundary
-- can fall inside a key and the caller must not judge that key until it has the
-- key's last row.
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
