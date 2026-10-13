INSERT INTO object_parts (bucket, key, number, size, algorithm, value)
SELECT bucket, $3, number, size, algorithm, value
FROM object_parts
WHERE bucket = $1 AND key = $2
ON CONFLICT (bucket, key, number) DO UPDATE SET
    size = excluded.size, algorithm = excluded.algorithm,
    value = excluded.value
