SELECT id, blob, size, kind AS "kind: Kind", became_noncurrent
FROM version_rows
WHERE bucket = $1 AND key = $2 AND seq = $3
FOR UPDATE
