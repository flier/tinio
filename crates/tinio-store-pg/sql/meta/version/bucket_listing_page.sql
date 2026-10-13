-- The bounded read of one chunk of a bucket's version listing: the same join
-- and projection as bucket_listing.sql, but seeked to a cursor, bounded by the
-- listing's prefix and capped, so one listing page costs its own rows rather
-- than the whole bucket (380k rows and 182ms for one 1000-key page of a
-- 200k-object bucket).
--
-- `$2` is the scan's lower bound and `$3` the row inside its key the read
-- resumes after. The statements' order is `key`, then the key's own rows
-- newest first, which is the order the join already reads in: the outer
-- object_meta key and the inner version_rows sequence descend through their
-- primary keys, so the ORDER BY stays an index order and the LIMIT stops the
-- walk at the page rather than after it.
--
-- `$3` is NULL for the first chunk (the key its bound names is read whole --
-- a `version-id-marker` resumes *inside* that key, so its rows are needed),
-- a real sequence for a continuation inside a key (`r.seq < $3` is exactly
-- the rows the listing still owes after it, because the rows arrive newest
-- first), and the sequence below every storable one (`-1`: sequences are
-- non-negative) for a key whose rows are done -- a key the join answered a
-- NULL sequence for holds no version row at all.
--
-- `$5` is the listing's prefix and `$6` its exclusive upper bound, the
-- prefix's last character incremented: UTF-8 orders by code point, so
-- `[prefix, upper)` is exactly the keys that start with it -- no key is
-- missed and none is invented. The plane's `prefix_upper` drops a trailing
-- `U+10FFFF` -- the last code point, which has no successor -- and increments
-- the character before it instead, which makes the range a superset of the
-- prefix set: `starts_with` is the definer that narrows it then, filtering
-- the range's own rows, never the bucket. A prefix whose whole body is
-- `U+10FFFF` characters (the empty prefix included) has no upper bound at all
-- -- every key at or above it already starts with it -- and is read by
-- bucket_listing_page_open.sql, which spells no `$6`. The bounds are an
-- explicit range rather than a bare `starts_with` call for the plan's sake:
-- only a range survives into an `Index Cond` under
-- `plan_cache_mode = force_generic_plan`, where a function call becomes a
-- filter that walks every key of the bucket.
--
-- The range is what makes this read the listing's own keys: `version::paginate`
-- rolls a matching key up and emits every other key as an entry, so without the
-- narrow the page carries the bucket's keys the prefix never asked for -- which
-- fs's and mem's walks filter out of their key streams and pg's listing served.
--
-- Both version_rows joins carry `$2` in their ON clause, and so does
-- current_slots. It is a no-op -- a row's key is its object row's key, which
-- `$2` already bounds, and a LEFT JOIN keeps an unmatched object row either
-- way -- and it is what lets each inner index scan start at the cursor.
-- Without it the planner merges the joins over the bucket's whole ranges, and
-- a page 50k keys into the bucket reads every row before it: 205k buffers
-- against this bound's few thousand.
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
    AND o.key < $6
    AND starts_with(o.key, $5)
    AND (
        $3::bigint IS NULL
        OR o.key > $2
        OR (o.key = $2 AND r.seq < $3)
    )
ORDER BY o.key COLLATE "C", r.seq DESC
LIMIT $4
