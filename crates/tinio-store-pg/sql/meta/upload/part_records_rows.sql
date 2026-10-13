SELECT
    p.number,
    p.etag,
    c.algorithm AS "algorithm?",
    c.value AS "value?",
    p.modified_at
FROM parts p
LEFT JOIN part_checksums c
    ON c.bucket = p.bucket AND c.upload_id = p.upload_id AND c.number = p.number
WHERE
    p.bucket = $1
    AND p.upload_id = $2
    AND p.number IN (SELECT unnest($3::int8[]))
