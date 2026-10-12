SELECT h.bucket AS "bucket: bucket::Name", h.ckey
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
ORDER BY h.bucket COLLATE "C", h.ckey COLLATE "C"
