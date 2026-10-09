-- Cut one chunk row out of a live generation, behind the store's back, so the
-- rows are no longer a contiguous run from zero: a gap at the head of the run
-- or a run that stops early are the same corruption.
DELETE FROM content
WHERE
    bucket = $1
    AND kind = 'object'
    AND ckey = $2
    AND seq = $3
    AND chunk_no = $4
