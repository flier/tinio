SELECT mode, retain_until, legal_hold
FROM upload_object_lock
WHERE bucket = $1 AND upload_id = $2
