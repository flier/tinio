SELECT DISTINCT ON (hash COLLATE "C") hash, len, published
FROM blocks
WHERE hash > $1
ORDER BY hash COLLATE "C", seq
LIMIT $2
