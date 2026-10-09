SELECT NOT EXISTS (
    SELECT 1 FROM content_heads WHERE bucket = $1 AND live_seq IS NOT NULL
) AS "empty!"
