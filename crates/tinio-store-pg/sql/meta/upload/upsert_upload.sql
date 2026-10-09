INSERT INTO uploads (bucket, upload_id, key, initiated_at, tags, acl)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (bucket, upload_id) DO UPDATE SET
    key = excluded.key, initiated_at = excluded.initiated_at,
    tags = excluded.tags, acl = excluded.acl
