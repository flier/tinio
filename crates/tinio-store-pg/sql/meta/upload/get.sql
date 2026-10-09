SELECT
    u.upload_id,
    u.key,
    u.initiated_at,
    u.tags AS "tags: object::Tags",
    u.acl AS "acl: Acl",
    c.algorithm AS "algorithm?: checksum::Algorithm",
    c.kind AS "kind?"
FROM uploads u LEFT JOIN upload_checksums c
    ON c.bucket = u.bucket AND c.upload_id = u.upload_id
WHERE u.bucket = $1 AND u.upload_id = $2
