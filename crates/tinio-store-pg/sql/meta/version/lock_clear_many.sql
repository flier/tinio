DELETE FROM version_object_lock
WHERE bucket = $1 AND key = $2 AND version_id = any($3::text[])
