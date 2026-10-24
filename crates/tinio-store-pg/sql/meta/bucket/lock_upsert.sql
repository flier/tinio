INSERT INTO bucket_object_lock (bucket, enabled, rule)
VALUES ($1, $2, $3)
ON CONFLICT (bucket) DO UPDATE SET
    enabled = excluded.enabled,
    rule = excluded.rule
