-- The bounded read of one chunk of the prefix's uploads: the same projection
-- as the statement it replaces, but seeked to a cursor and capped, so a listing
-- page costs its own rows rather than the whole prefix (F17).
--
-- `$2`/`$3` are the range's bounds and `$4` the prefix itself. `$3` is the
-- prefix's last character incremented: UTF-8 orders by code point, so
-- `[prefix, upper)` is exactly the keys that start with it -- no upload is
-- missed and none is invented. The plane's `prefix_upper` drops a trailing
-- `U+10FFFF` -- the last code point, which has no successor -- and increments
-- the character before it, which makes the range a superset of the prefix
-- set: `starts_with` is the definer that narrows it then, filtering the
-- range's own rows, never the bucket. A prefix whose whole body is
-- `U+10FFFF` characters (the empty prefix included) has no upper bound at
-- all -- every key at or above it already starts with it -- and is read by
-- list_uploads_chunk_open.sql, which spells no `$3`.
--
-- The bounds are explicit byte comparisons rather than a bare `starts_with`
-- call for the plan's sake: only a range survives into an `Index Cond` under
-- `plan_cache_mode = force_generic_plan`, where a function call becomes a
-- filter that walks every upload of the bucket.
--
-- `$5`/`$6` are the row the read resumes after, compared as the tuple
-- `(key, upload_id)` -- the byte order `uploads_order` builds and ORDER BY
-- reads. `('', '')` is the "no cursor" sentinel: every stored key is longer
-- than the empty string, so the bound is a no-op when the caller gave none.
-- `$2` is what makes the cursor a seek rather than a filter: the scan starts
-- at the cursor, never at the prefix the cursor sits inside.
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
    AND u.key < $3
    AND starts_with(u.key, $4)
    AND (u.key, u.upload_id) > ($5, $6)
ORDER BY u.key COLLATE "C", u.upload_id COLLATE "C"
LIMIT $7
