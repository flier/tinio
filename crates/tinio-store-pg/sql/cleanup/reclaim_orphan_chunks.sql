WITH doomed AS (
    SELECT
        u.bucket,
        u.kind::content_kind AS kind,
        u.ckey,
        u.seq
    FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[])
        AS u (bucket, kind, ckey, seq)
)

DELETE FROM content AS c
USING doomed AS d
WHERE
    c.bucket = d.bucket
    AND c.kind = d.kind
    AND c.ckey = d.ckey
    AND c.seq = d.seq
