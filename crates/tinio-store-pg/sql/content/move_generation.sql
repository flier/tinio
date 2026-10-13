-- One bounded window of a superseded generation's re-key: `chunk_no` names the
-- rows this statement moves, so a re-home rewrites the body in windows rather
-- than in one statement over the whole generation (PG22).
UPDATE content
SET seq = $5
WHERE
    bucket = $1
    AND kind = $2
    AND ckey = $3
    AND seq = $4
    AND chunk_no BETWEEN $6 AND $7
