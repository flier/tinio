DELETE FROM version_index
WHERE bucket = $1 AND key = $2 AND seq = $3 AND id <> $4
