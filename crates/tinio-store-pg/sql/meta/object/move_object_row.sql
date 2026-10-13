INSERT INTO object_meta
(bucket, key, etag, size, mtime, identity, tags, checksum, acl)
SELECT bucket, $3, etag, size, mtime, identity, tags, checksum, acl
FROM object_meta
WHERE bucket = $1 AND key = $2
ON CONFLICT (bucket, key) DO UPDATE SET
    etag = excluded.etag, size = excluded.size, mtime = excluded.mtime,
    identity = excluded.identity, tags = excluded.tags,
    checksum = excluded.checksum, acl = excluded.acl
