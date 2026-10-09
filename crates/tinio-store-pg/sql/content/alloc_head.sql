INSERT INTO content_heads
(bucket, kind, ckey, next_seq, live_seq, live_len)
VALUES ($1, $2, $3, 1, NULL, 0)
ON CONFLICT (bucket, kind, ckey) DO UPDATE
    SET next_seq = content_heads.next_seq + 1
RETURNING next_seq
