SELECT version_id, seq, is_delete_marker
FROM current_slots
WHERE bucket = $1 AND key = $2
FOR UPDATE
