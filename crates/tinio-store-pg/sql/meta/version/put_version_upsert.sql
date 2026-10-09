INSERT INTO version_rows
(
    bucket, key, seq, id, blob, size, etag, created, became_noncurrent,
    kind, tags, checksum, acl
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (bucket, key, seq) DO UPDATE SET
    id = excluded.id, blob = excluded.blob, size = excluded.size,
    etag = excluded.etag, created = excluded.created,
    became_noncurrent = excluded.became_noncurrent, kind = excluded.kind,
    tags = excluded.tags, checksum = excluded.checksum,
    acl = excluded.acl
