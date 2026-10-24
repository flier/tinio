-- The doctor's Object Lock scan (OL31): every per-version lock row, with the
-- ownership rule the drain uses, read from the schema's one view
-- (`version_lock_owned`). A row that resolves to neither an object version row
-- nor a non-marker current slot is an orphan.
SELECT
    l.bucket,
    l.key,
    l.version_id,
    l.mode,
    l.retain_until,
    l.legal_hold,
    (
        EXISTS (
            SELECT 1
            FROM version_lock_owned AS o
            WHERE
                o.bucket = l.bucket
                AND o.key = l.key
                AND o.version_id = l.version_id
        )
    ) AS "owned!"
FROM version_object_lock AS l
ORDER BY
    l.bucket COLLATE "C",
    l.key COLLATE "C",
    l.version_id COLLATE "C"
