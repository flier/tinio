WITH locked AS MATERIALIZED (
    SELECT name FROM buckets WHERE name = $1 FOR UPDATE
),

allocated AS (
    INSERT INTO content_heads (
        bucket, kind, ckey, next_seq, live_seq, live_len
    )
    SELECT $1, $2, $3, 1, NULL, 0
    WHERE EXISTS (SELECT 1 FROM locked)
    ON CONFLICT (bucket, kind, ckey) DO UPDATE
        SET
            next_seq = content_heads.next_seq + 1,
            allocated_at = now()
    RETURNING next_seq
)

SELECT next_seq
FROM allocated
