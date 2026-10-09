INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk)
SELECT $1, $2, $3, $4, u.n, u.c
FROM unnest($5::int[], $6::bytea[]) AS u (n, c)
ON CONFLICT (bucket, kind, ckey, seq, chunk_no) DO UPDATE
    SET chunk = excluded.chunk
