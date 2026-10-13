-- Remove the content rows one page of the scan returned (`$1`..`$4` are the
-- page's keys, `$5` the grace the scan read them with).
--
-- The predicate is the scan's own, applied again to the keys this page already
-- read, because the two phases are two statements and the store keeps serving
-- between them:
--   * `NOT EXISTS (content_heads ...)`: a generation the flip made **live**
--     after the scan named it a candidate must not lose its bytes -- the head
--     row and the ETag stay, so deleting them would leave a served object whose
--     content is gone;
--   * `published <= now() - $5`: the same grace the scan read, so a row a
--     publish rewrote in place inside the window (an `insert_chunks` fill sets
--     the stamp) is not deleted out from under the writer.
-- The grace bearer is the row's own `published` stamp; the head's
-- `allocated_at` is the second bearer inside the head gate, exactly as in the
-- scan. A generation the head names live is spared whatever the stamps say.
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
