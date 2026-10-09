WITH displaced AS (
    DELETE FROM version_rows
    WHERE bucket = $1 AND key = $2 AND id = 'null' AND seq <> $3
    RETURNING seq
)

DELETE FROM version_index
WHERE
    bucket = $1 AND key = $2 AND id = 'null'
    AND seq IN (SELECT seq FROM displaced)
