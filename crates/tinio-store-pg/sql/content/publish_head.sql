WITH locked AS (
    SELECT 1
    FROM content
    WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND seq = $4
    FOR UPDATE
)

UPDATE content_heads
SET live_seq = $4, live_len = $5, publish_id = $6
WHERE
    bucket = $1
    AND kind = $2
    AND ckey = $3
    AND next_seq = $4
    AND (SELECT count(*) FROM locked) = $7
