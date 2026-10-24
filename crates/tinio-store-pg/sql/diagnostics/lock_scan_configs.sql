-- The doctor's Object Lock scan (OL31): every stored bucket rule, so the
-- model's own decoder can report the ones that do not read. An empty wire is
-- "no rule"; the scan discards only a non-empty wire.
SELECT
    l.bucket,
    l.enabled,
    l.rule
FROM bucket_object_lock AS l
ORDER BY l.bucket COLLATE "C"
