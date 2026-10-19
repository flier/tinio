DELETE FROM blocks
WHERE hash = $1 AND seq = $2
