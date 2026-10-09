SELECT
    seq, id, blob, size, etag, created, became_noncurrent,
    kind AS "kind: Kind",
    tags AS "tags: object::Tags", checksum, acl AS "acl: Acl"

FROM version_rows WHERE bucket = $1 ORDER BY seq
