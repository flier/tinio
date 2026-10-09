SELECT
    live_seq AS "live_seq!",
    live_len AS "live_len!",
    publish_id AS "publish_id?"
FROM content_heads
WHERE
    bucket = $1
    AND kind = $2
    AND ckey = $3
    AND live_seq IS NOT NULL
FOR UPDATE
