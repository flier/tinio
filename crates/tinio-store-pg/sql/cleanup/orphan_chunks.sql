-- One keyset page of the orphan-chunk scan (PG7): `$2`..`$5` are the cursor --
-- the last generation the previous page returned, exclusive -- and `$6` the
-- page size. The order is the cursor's own `(bucket, kind, ckey, seq)`, so a
-- pass reads every generation once and holds one page of them rather than the
-- whole content relation.
--
-- The grace window (`$1` seconds) is carried by two predicates here, and the
-- removal repeats both: the row's own `published` stamp must be inside the
-- window's past (`<= now() - $1`), which spares a generation written inside the
-- window, and the head's `allocated_at` must be too (`> now() - $1`), which
-- spares the head's current allocation -- the generation `stage` commits chunks
-- at, and the one `publish` has not named live yet. The stamp predicate is what
-- keeps a publish in flight out of the sweep; a zero grace would turn a
-- just-stamped row into an orphan, so it is never weakened to a bare age.
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
    AND (c.bucket, c.kind, c.ckey, c.seq) > (
        $2::text COLLATE "C",
        $3::content_kind,
        $4::text COLLATE "C",
        $5::bigint
    )
ORDER BY c.bucket COLLATE "C", c.kind, c.ckey COLLATE "C", c.seq
LIMIT $6
