INSERT INTO object_meta
(bucket, key, etag, size, mtime, identity, tags, checksum, acl)
VALUES (
    $1, $2, $3, 0, to_timestamp(0), 0, '{}', '',
    row(NULL, ARRAY[]::acl_grant[])::acl
)
ON CONFLICT (bucket, key) DO NOTHING
