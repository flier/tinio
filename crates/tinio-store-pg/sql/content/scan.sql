SELECT ckey, live_len AS "live_len!" FROM content_heads
WHERE
    bucket = $1 AND kind = 'object'::content_kind AND live_seq IS NOT NULL
    AND starts_with(ckey, $2)
    AND ($3::text IS NULL OR ckey > $3)
ORDER BY ckey COLLATE "C" LIMIT $4
