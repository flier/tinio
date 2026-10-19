WITH keys AS (
    SELECT u.kind::content_kind AS kind, u.ckey
    FROM unnest($2::text[], $3::text[]) AS u (kind, ckey)
),

gone_chunks AS (
    DELETE FROM content AS c
    USING keys AS k
    WHERE c.bucket = $1 AND c.kind = k.kind AND c.ckey = k.ckey
    RETURNING 1
)

UPDATE content_heads AS h
SET
    live_seq = NULL,
    live_len = 0,
    publish_id = NULL,
    next_seq = h.next_seq + 1
FROM keys AS k
WHERE
    h.bucket = $1
    AND h.kind = k.kind
    AND h.ckey = k.ckey
    AND (SELECT count(*) FROM gone_chunks) >= 0
