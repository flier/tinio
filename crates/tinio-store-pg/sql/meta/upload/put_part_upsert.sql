INSERT INTO parts (bucket, upload_id, number, etag, modified_at)
VALUES (
    $1, $2, $3, $4,
    (extract(EPOCH FROM clock_timestamp()) * 1000)::bigint
)
ON CONFLICT (bucket, upload_id, number) DO UPDATE SET
    etag = excluded.etag, modified_at = excluded.modified_at
