WITH locked AS MATERIALIZED (
    SELECT live_seq
    FROM content_heads
    WHERE bucket = $1
    FOR UPDATE
),

empty AS (
    SELECT coalesce(bool_and(live_seq IS NULL), TRUE) AS ok
    FROM locked
),

del_heads AS (
    DELETE FROM content_heads
    WHERE bucket = $1 AND (SELECT ok FROM empty)
    RETURNING 1
),

del_chunks AS (
    DELETE FROM content
    WHERE bucket = $1 AND (SELECT ok FROM empty)
    RETURNING 1
)

SELECT ok AS "empty!" FROM empty
