SELECT seq, chunk
FROM blocks
WHERE hash = $1 AND seq >= $2 AND seq <= $3
ORDER BY seq
