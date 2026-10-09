INSERT INTO object_meta
(bucket, key, etag, size, mtime, identity, tags, checksum, acl)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (bucket, key) DO UPDATE SET
    etag = excluded.etag, size = excluded.size, mtime = excluded.mtime,
    identity = excluded.identity, tags = excluded.tags,
    checksum = excluded.checksum, acl = excluded.acl
