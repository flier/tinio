WITH empty AS (
    SELECT NOT EXISTS (
        SELECT 1 FROM content_heads
        WHERE bucket = $1 AND live_seq IS NOT NULL
    ) AS ok
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
