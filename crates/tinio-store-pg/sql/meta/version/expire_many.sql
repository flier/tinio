DELETE FROM version_rows
WHERE bucket = $1 AND key = $2 AND seq IN (SELECT unnest($3::int8[]))
RETURNING seq, id
