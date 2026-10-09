INSERT INTO parts (bucket, upload_id, number, etag) VALUES ($1, $2, $3, $4)
ON CONFLICT (bucket, upload_id, number) DO UPDATE SET etag = excluded.etag
