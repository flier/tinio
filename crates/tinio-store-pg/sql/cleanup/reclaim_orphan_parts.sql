WITH doomed AS (
    SELECT
        u.bucket,
        u.ckey
    FROM unnest($1::text[], $2::text[]) AS u (bucket, ckey)
),

gone_chunks AS (
    DELETE FROM content AS c
    USING doomed AS d
    WHERE
        c.bucket = d.bucket
        AND c.kind = 'part'::content_kind
        AND c.ckey = d.ckey
)

DELETE FROM content_heads AS h
USING doomed AS d
WHERE
    h.bucket = d.bucket
    AND h.kind = 'part'::content_kind
    AND h.ckey = d.ckey
