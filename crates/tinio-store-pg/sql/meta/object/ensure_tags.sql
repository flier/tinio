UPDATE object_meta SET tags = $3
WHERE bucket = $1 AND key = $2 AND tags IS DISTINCT FROM $3
