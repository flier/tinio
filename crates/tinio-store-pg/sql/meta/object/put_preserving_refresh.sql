UPDATE object_meta SET etag = $3, size = $4, mtime = $5, identity = $6
WHERE bucket = $1 AND key = $2
