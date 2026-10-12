SELECT
    o.key, o.etag, o.size, o.mtime, o.identity,
    o.tags AS "tags: object::Tags", o.checksum, o.acl AS "acl: Acl",
    s.seq AS "slot_seq?", s.version_id AS "version_id?",
    s.is_delete_marker AS "is_delete_marker?",
    r.seq AS "row_seq?", r.id AS "id?", r.blob AS "blob?",
    r.size AS "row_size?", r.etag AS "row_etag?", r.created AS "created?",
    r.became_noncurrent AS "became_noncurrent?",
    r.kind AS "row_kind?: Kind", r.tags AS "row_tags?: object::Tags",
    r.checksum AS "row_checksum?", r.acl AS "row_acl?: Acl"
FROM object_meta o
LEFT JOIN current_slots s
    ON s.bucket = o.bucket AND s.key = o.key
LEFT JOIN version_rows r
    ON r.bucket = o.bucket AND r.key = o.key
WHERE o.bucket = $1
ORDER BY o.key COLLATE "C", r.seq DESC
