INSERT INTO object_meta
(bucket, key, etag, size, mtime, identity, tags, checksum, acl)
SELECT
    $1, u.key, u.etag, u.size, u.mtime, u.identity, u.tags, u.checksum,
    ($9::acl[])[u.ord::int]
FROM
    unnest(
        $2::text[], $3::text[], $4::bigint[], $5::timestamptz[], $6::bigint[],
        $7::jsonb[], $8::text[]
    ) WITH ORDINALITY
        AS u (key, etag, size, mtime, identity, tags, checksum, ord)
ON CONFLICT (bucket, key) DO UPDATE SET
    etag = excluded.etag, size = excluded.size, mtime = excluded.mtime,
    identity = excluded.identity, tags = excluded.tags,
    checksum = excluded.checksum, acl = excluded.acl
