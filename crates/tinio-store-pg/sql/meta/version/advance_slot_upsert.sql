INSERT INTO current_slots (bucket, key, version_id, seq, is_delete_marker)
VALUES (
    $1, $2, $3, $4,
    COALESCE(
        (SELECT kind = 'marker' FROM version_rows
         WHERE bucket = $1 AND key = $2 AND seq = $4),
        false
    )
)
ON CONFLICT (bucket, key) DO UPDATE SET
    version_id = excluded.version_id,
    seq = excluded.seq,
    is_delete_marker = excluded.is_delete_marker
