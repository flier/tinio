SELECT mode, retain_until, legal_hold
FROM version_object_lock
WHERE bucket = $1 AND key = $2 AND version_id = $3
FOR UPDATE
