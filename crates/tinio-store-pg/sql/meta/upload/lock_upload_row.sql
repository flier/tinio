SELECT 1 FROM uploads
WHERE bucket = $1 AND upload_id = $2 AND key = $3
FOR UPDATE
