-- One keyset page of the upload-gone part scan (PG7): `$1`/`$2` are the
-- cursor -- the last `(bucket, ckey)` the previous page returned, exclusive --
-- and `$3` the page size, so a pass reads one page of the head relation
-- rather than all of it.
--
-- The keyset is spelled as scalar comparisons rather than a row comparison:
-- `content_heads_pkey` is `(bucket, kind, ckey)`, the regex above already
-- gives the plan an index condition on `(kind, ckey)`, and the scalar form
-- keeps the index-only scan that serves this order taking the cursor as a
-- filter -- measured at 33 buffers and 0.5 ms for a page, where the row
-- comparison made the planner sort a sequential scan instead.
SELECT
    h.bucket COLLATE "C" AS "bucket!: bucket::Name",
    h.ckey COLLATE "C" AS "ckey!"
FROM content_heads AS h
WHERE
    h.kind = 'part'::content_kind
    AND h.ckey ~ '^p:[0-9a-f]+:[0-9]+$'
    AND NOT EXISTS (
        SELECT 1
        FROM uploads AS u
        WHERE
            u.bucket = h.bucket
            AND u.upload_id = convert_from(
                decode(split_part(h.ckey, ':', 2), 'hex'),
                'UTF8'
            )
    )
    AND (
        h.bucket > $1::text COLLATE "C"
        OR (
            h.bucket = $1::text COLLATE "C"
            AND h.ckey > $2::text COLLATE "C"
        )
    )
ORDER BY h.bucket COLLATE "C", h.ckey COLLATE "C"
LIMIT $3
