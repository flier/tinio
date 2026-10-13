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

-- The head row stays (R2): `next_seq` moves past the drained generation, so a
-- generation id is never reused and a stage that outlived the drain cannot
-- splice its tail into the next writer's rows. `live_seq` clears in the same
-- statement as the chunks, so no reader sees a live head whose run is gone.
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
