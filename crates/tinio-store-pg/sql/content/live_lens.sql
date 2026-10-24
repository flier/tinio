SELECT
    ckey COLLATE "C" AS "ckey!",
    live_seq AS "seq!",
    live_len AS "len!"
FROM content_heads
WHERE
    bucket = $1
    AND ckey = any($2)
    AND live_seq IS NOT NULL
