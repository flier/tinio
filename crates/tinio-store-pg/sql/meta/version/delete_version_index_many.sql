DELETE FROM version_index AS v
USING unnest($3::text[], $4::bigint[]) AS u (id, seq)
WHERE v.bucket = $1 AND v.key = $2 AND v.id = u.id AND v.seq = u.seq
