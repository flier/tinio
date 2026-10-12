UPDATE version_rows
SET became_noncurrent = $4
WHERE bucket = $1 AND key = $2 AND seq = $3 AND became_noncurrent = 0
