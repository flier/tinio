-- list_uploads_chunk.sql's sibling for a prefix with no upper bound: one whose
-- whole body is `U+10FFFF` characters, the last code point (which has no
-- successor), the empty prefix included. There is no string greater than all
-- the keys that start with such a prefix -- `U+10FFFF`, the largest code
-- point, may itself be extended -- so the statement spells no upper bound and
-- `$2` is the whole range. It is exact either way: every key at or above such
-- a prefix starts with it, because a key whose first differing character were
-- smaller would sort below the prefix and one greater cannot exist. `$3` is
-- the prefix and `starts_with` the definer that states it; `$2` is the scan's
-- lower bound (never below the prefix, never below the cursor), which is also
-- what makes the cursor a seek. `$4`/`$5` are the row the read resumes after,
-- the `('', '')` sentinel leaving that bound a no-op, and `$6` the cap.
SELECT
    u.upload_id,
    u.key,
    u.initiated_at,
    u.tags AS "tags: object::Tags",
    u.acl AS "acl: Acl",
    c.algorithm AS "algorithm?",
    c.kind AS "kind?"
FROM uploads u LEFT JOIN upload_checksums c
    ON c.bucket = u.bucket AND c.upload_id = u.upload_id
WHERE
    u.bucket = $1
    AND u.key >= $2
    AND starts_with(u.key, $3)
    AND (u.key, u.upload_id) > ($4, $5)
ORDER BY u.key COLLATE "C", u.upload_id COLLATE "C"
LIMIT $6
