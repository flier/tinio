-- One keyset page of the block sweep's live-reference scan: every version
-- row's blob wire, so the sweep folds one page of rows into its live set
-- instead of materialising every version row in the store. `$1`..`$3` are the
-- cursor -- the last row the previous page returned, exclusive -- and `$4` the
-- page size.
--
-- The order is the `version_rows` primary key's own, `(bucket, key, seq)`
-- ascending: the fold does not care which end of a key's history it starts
-- from, so this scan takes the order the primary key already serves, with no
-- sort at all (the retention scan below reads the same rows newest-first per
-- key and pays an incremental sort for it). The keyset is spelled as scalar
-- comparisons rather than a row comparison, so the primary key's index scan
-- carries it as a filter and the page can stop at the limit.
--
-- The row's blob is a wire (tinio-core's `encode_blob`); the fold decodes it
-- and skips a row whose wire does not decode, exactly as the contract's
-- `bucket_rows` walk does, so one corrupt row cannot fail the sweep.
SELECT
    r.bucket,
    r.key,
    r.seq,
    r.blob
FROM version_rows AS r
WHERE
    (r.bucket, r.key) > ($1::text COLLATE "C", $2::text COLLATE "C")
    OR (
        r.bucket = $1::text COLLATE "C"
        AND r.key = $2::text COLLATE "C"
        AND r.seq > $3::bigint
    )
ORDER BY r.bucket COLLATE "C", r.key COLLATE "C", r.seq
LIMIT $4
