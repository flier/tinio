SELECT seq, id, kind AS "kind: Kind"
FROM version_rows
WHERE bucket = $1 AND key = $2 AND seq = ANY($3::int8[])
FOR UPDATE
