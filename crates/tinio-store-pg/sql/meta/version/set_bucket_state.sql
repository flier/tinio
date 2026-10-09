INSERT INTO bucket_versioning (bucket, state) VALUES ($1, $2)
ON CONFLICT (bucket) DO UPDATE SET state = excluded.state
