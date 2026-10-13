-- The re-home's flip: the sequence is one this publish allocated and the head
-- will never hand out again, so no other writer is writing rows at it and the
-- generation's own completeness is the whole guard. The `next_seq = $4` of
-- publish_head cannot be asked here, because the move that filled this sequence
-- ran before this statement rather than under the head's row lock.
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
    AND (SELECT count(*) FROM locked) = $7
