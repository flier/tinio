INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published)
SELECT bucket, kind, $1, $2, chunk_no, chunk, now() FROM content
WHERE
    bucket = $3
    AND kind = $4
    AND ckey = $5
    AND seq = $6
    AND chunk_no BETWEEN $7 AND $8
