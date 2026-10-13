INSERT INTO blocks (hash, seq, chunk, len, published)
SELECT $1, u.seq, u.chunk, $4, $5
FROM unnest($2::int4[], $3::bytea[]) AS u (seq, chunk)
ON CONFLICT (hash, seq) DO NOTHING
