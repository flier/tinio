INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk)
SELECT bucket, kind, $1, $2, chunk_no, chunk FROM content
WHERE bucket = $3 AND kind = $4 AND ckey = $5 AND seq = $6
