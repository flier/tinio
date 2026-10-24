SELECT l.enabled, l.rule
FROM buckets AS b
LEFT JOIN bucket_object_lock AS l ON l.bucket = b.name
WHERE b.name = $1
