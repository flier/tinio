SELECT seq FROM current_slots
WHERE bucket = $1 AND key = $2
FOR UPDATE
