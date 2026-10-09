SELECT
    etag, size, mtime, identity,
    tags AS "tags: object::Tags", checksum, acl AS "acl: Acl"
FROM object_meta WHERE bucket = $1 AND key = $2
