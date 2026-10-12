UPDATE content
SET seq = $5
WHERE bucket = $1 AND kind = $2 AND ckey = $3 AND seq = $4
