SELECT
    h.bucket COLLATE "C" AS "bucket!: bucket::Name",
    h.ckey COLLATE "C" AS "ckey!"
FROM content_heads AS h
WHERE
    h.kind = 'part'::content_kind
    AND h.ckey ~ '^p:[0-9a-f]+:[0-9]+$'
    AND NOT EXISTS (
        SELECT 1
        FROM uploads AS u
        WHERE
            u.bucket = h.bucket
            AND u.upload_id = convert_from(
                decode(split_part(h.ckey, ':', 2), 'hex'),
                'UTF8'
            )
    )
    AND (
        h.bucket > $1::text COLLATE "C"
        OR (
            h.bucket = $1::text COLLATE "C"
            AND h.ckey > $2::text COLLATE "C"
        )
    )
ORDER BY h.bucket COLLATE "C", h.ckey COLLATE "C"
LIMIT $3
