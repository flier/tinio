DELETE FROM version_rows WHERE bucket = $1 AND key = $2 AND seq = ANY($3)
    RETURNING seq, id
