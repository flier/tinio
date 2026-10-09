WITH keys AS (
    SELECT u.kind::content_kind AS kind, u.ckey
    FROM unnest($2::text[], $3::text[]) AS u (kind, ckey)
),

gone_chunks AS (
    DELETE FROM content AS c
    USING keys AS k
    WHERE c.bucket = $1 AND c.kind = k.kind AND c.ckey = k.ckey
)

DELETE FROM content_heads AS h
USING keys AS k
WHERE h.bucket = $1 AND h.kind = k.kind AND h.ckey = k.ckey
