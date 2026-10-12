SELECT DISTINCT
    c.bucket COLLATE "C" AS "bucket!: bucket::Name",
    c.kind AS "kind: ContentKind",
    c.ckey COLLATE "C" AS "ckey!",
    c.seq
FROM content AS c
WHERE
    c.published <= now() - ($1::bigint * interval '1 second')
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
                            $1::bigint * interval '1 second'
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
                                    $1::bigint * interval '1 second'
                                )
                        )
                    )
                )
            )
    )
ORDER BY c.bucket COLLATE "C", c.ckey COLLATE "C", c.seq
