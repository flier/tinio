UPDATE content_heads
SET live_seq = $4, live_len = $5, publish_id = $6
WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND live_seq = $7
