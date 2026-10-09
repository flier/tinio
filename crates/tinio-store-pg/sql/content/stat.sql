SELECT live_len AS "live_len!" FROM content_heads
WHERE
    bucket = $1 AND kind = 'object'::content_kind AND ckey = $2
    AND live_seq IS NOT NULL
