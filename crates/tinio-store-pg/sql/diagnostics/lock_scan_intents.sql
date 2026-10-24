-- The doctor's Object Lock scan (OL31): every multipart lock intent, in the
-- table's own key order, so the model's decoder can report the corrupt ones.
SELECT
    l.bucket,
    l.upload_id,
    l.mode,
    l.retain_until,
    l.legal_hold
FROM upload_object_lock AS l
ORDER BY
    l.bucket COLLATE "C",
    l.upload_id COLLATE "C"
