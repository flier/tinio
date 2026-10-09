SELECT number, size, algorithm, value FROM object_parts
WHERE bucket = $1 AND key = $2 ORDER BY number
