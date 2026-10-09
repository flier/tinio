INSERT INTO object_parts (bucket, key, number, size, algorithm, value)
SELECT $1, $2, u.n, u.s, u.a, u.v
FROM unnest($3::bigint[], $4::bigint[], $5::text[], $6::text[])
    AS u (n, s, a, v)
