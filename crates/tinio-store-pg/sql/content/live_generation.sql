SELECT
    live_seq AS "seq!",
    live_len AS "len!",
    publish_id
FROM content_heads
WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND live_seq IS NOT NULL
