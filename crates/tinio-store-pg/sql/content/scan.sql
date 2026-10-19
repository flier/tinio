SELECT ckey, live_len AS "live_len!" FROM content_heads
WHERE
    bucket = $1
    AND kind = 'object'::content_kind
    AND live_seq IS NOT NULL
    AND ckey >= $2
    AND ckey < $3
    AND ckey > $4
    AND starts_with(ckey, $2)
ORDER BY ckey COLLATE "C"
LIMIT $5
