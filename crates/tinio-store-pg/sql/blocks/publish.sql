INSERT INTO blocks (hash, seq, chunk, len, published)
SELECT $1, u.seq, u.chunk, $4, $5
FROM unnest($2::int4[], $3::bytea[]) AS u (seq, chunk)
WHERE
    NOT EXISTS (
        SELECT 1
        FROM blocks
        WHERE hash = $1
    )
ON CONFLICT (hash, seq) DO NOTHING
