DELETE FROM content_heads
WHERE bucket = $1 AND kind = $2 AND ckey = $3
