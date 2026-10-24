INSERT INTO upload_object_lock (
    bucket, upload_id, mode, retain_until, legal_hold
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (bucket, upload_id) DO UPDATE SET
    mode = excluded.mode,
    retain_until = excluded.retain_until,
    legal_hold = excluded.legal_hold
