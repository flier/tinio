SELECT version_id, mode, retain_until, legal_hold
FROM version_object_lock
WHERE bucket = $1 AND key = $2 AND version_id = ANY($3::text[])
FOR UPDATE
