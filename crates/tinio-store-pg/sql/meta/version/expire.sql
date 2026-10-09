DELETE FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = $3
RETURNING id
