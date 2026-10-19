WITH doomed AS (
    SELECT
        u.bucket,
        u.kind::content_kind AS kind,
        u.ckey,
        u.seq
    FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[])
        AS u (bucket, kind, ckey, seq)
)

DELETE FROM content AS c
USING doomed AS d
WHERE
    c.bucket = d.bucket
    AND c.kind = d.kind
    AND c.ckey = d.ckey
    AND c.seq = d.seq
    AND c.published <= now() - ($5::bigint * interval '1 second')
    AND NOT EXISTS (
        SELECT 1
        FROM content_heads AS h
        WHERE
            h.bucket = c.bucket AND h.kind = c.kind
            AND h.ckey = c.ckey
            AND (
                h.live_seq = c.seq
                OR (
                    h.next_seq = c.seq
                    AND (
                        h.allocated_at > now() - (
                            $5::bigint * interval '1 second'
                        )
                        OR EXISTS (
                            SELECT 1
                            FROM content AS fresh
                            WHERE
                                fresh.bucket = c.bucket
                                AND fresh.kind = c.kind
                                AND fresh.ckey = c.ckey
                                AND fresh.seq = c.seq
                                AND fresh.published > now() - (
                                    $5::bigint * interval '1 second'
                                )
                        )
                    )
                )
            )
    )
