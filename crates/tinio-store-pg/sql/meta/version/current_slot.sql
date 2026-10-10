SELECT
    s.version_id,
    s.is_delete_marker,
    r.kind AS "kind?: Kind"
FROM current_slots s
LEFT JOIN version_rows r
    ON r.bucket = s.bucket AND r.key = s.key AND r.seq = s.seq
WHERE s.bucket = $1 AND s.key = $2
