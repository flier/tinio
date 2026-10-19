DELETE FROM content
WHERE
    bucket = $1
    AND kind = 'object'
    AND ckey = $2
    AND seq = $3
    AND chunk_no = $4
