DELETE FROM version_index
WHERE bucket = $1 AND key = $2 AND id = $3 AND seq = $4
