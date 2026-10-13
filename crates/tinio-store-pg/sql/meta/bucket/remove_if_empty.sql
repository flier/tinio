DELETE FROM buckets AS b
WHERE
    b.name = $1
    AND NOT EXISTS (
        SELECT 1 FROM object_meta AS o WHERE o.bucket = b.name
    )
    AND NOT EXISTS (SELECT 1 FROM uploads AS u WHERE u.bucket = b.name)
RETURNING b.name
