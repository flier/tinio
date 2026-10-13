-- bucket_listing_page.sql's sibling for a prefix with no upper bound: one
-- whose whole body is `U+10FFFF` characters, the last code point (which has no
-- successor), the empty prefix included. There is no string greater than all
-- the keys that start with such a prefix -- `U+10FFFF`, the largest code point,
-- may itself be extended -- so the statement spells no upper bound and `$2` is
-- the whole range. It is exact either way: every key at or above such a prefix
-- starts with it, because a key whose first differing character were smaller
-- would sort below the prefix and one greater cannot exist. `$5` is the prefix
-- and `starts_with` the definer that states it.
--
-- The rest is the bounded read's own shape, documented in
-- bucket_listing_page.sql: `$2` the scan's lower bound (never below the prefix,
-- never below the cursor -- which is also what makes each inner index scan of
-- the joins start at the cursor), `$3` the row inside `$2`'s key the read
-- resumes after, `$4` the cap.
SELECT
    o.key,
    o.etag,
    o.size,
    o.mtime,
    o.identity,
    o.tags AS "tags: object::Tags",
    o.checksum,
    o.acl AS "acl: Acl",
    s.seq AS "slot_seq?",
    s.version_id AS "version_id?",
    s.is_delete_marker AS "is_delete_marker?",
    slot_row.kind AS "slot_kind?: Kind",
    r.seq AS "row_seq?",
    r.id AS "id?",
    r.blob AS "blob?",
    r.size AS "row_size?",
    r.etag AS "row_etag?",
    r.created AS "created?",
    r.became_noncurrent AS "became_noncurrent?",
    r.kind AS "row_kind?: Kind",
    r.tags AS "row_tags?: object::Tags",
    r.checksum AS "row_checksum?",
    r.acl AS "row_acl?: Acl"
FROM object_meta AS o
LEFT JOIN current_slots AS s
    ON s.bucket = o.bucket AND s.key = o.key AND s.key >= $2
LEFT JOIN version_rows AS slot_row
    ON
        slot_row.bucket = o.bucket
        AND slot_row.key = o.key
        AND slot_row.key >= $2
        AND slot_row.seq = s.seq
LEFT JOIN version_rows AS r
    ON r.bucket = o.bucket AND r.key = o.key AND r.key >= $2
WHERE
    o.bucket = $1
    AND o.key >= $2
    AND starts_with(o.key, $5)
    AND (
        $3::bigint IS NULL
        OR o.key > $2
        OR (o.key = $2 AND r.seq < $3)
    )
ORDER BY o.key COLLATE "C", r.seq DESC
LIMIT $4
