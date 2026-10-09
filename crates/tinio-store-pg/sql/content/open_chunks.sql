SELECT chunk_no, chunk FROM content
WHERE
    bucket = $1 AND kind = $2 AND ckey = $3 AND seq = $4
    AND chunk_no BETWEEN $5 AND $6
ORDER BY chunk_no
