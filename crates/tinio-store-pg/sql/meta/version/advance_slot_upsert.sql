INSERT INTO current_slots (bucket, key, version_id, seq)
VALUES ($1, $2, $3, $4)
ON CONFLICT (bucket, key) DO UPDATE SET
    version_id = excluded.version_id, seq = excluded.seq
