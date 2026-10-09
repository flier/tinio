INSERT INTO version_index (bucket, key, id, seq) VALUES ($1, $2, $3, $4)
ON CONFLICT (bucket, key, id) DO UPDATE SET seq = excluded.seq
