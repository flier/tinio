-- The prefix bound is an explicit byte range, not a `starts_with` call: only a
-- range survives into the plan's `Index Cond` under
-- `plan_cache_mode = force_generic_plan`, where a function call is a filter
-- that walks every head of the bucket.
--
-- `$2` is the prefix and `$3` its exclusive upper bound, the prefix's last
-- character incremented. UTF-8 orders by code point, so `[prefix, upper)` is
-- exactly the strings that start with the prefix: no key is missed and none is
-- invented. The plane's `prefix_upper` drops a trailing `U+10FFFF` — the last
-- code point, which has no successor — and increments the character before it
-- instead, which makes the range a superset of the prefix set; `starts_with`
-- below is the definer that narrows it then, and it filters the range's own
-- rows, never the bucket. `$4` is the exclusive cursor, with '' as the "no
-- cursor" sentinel: every stored key is longer than the empty string, so the
-- bound is a no-op when the caller gave none.
SELECT ckey, live_len AS "live_len!" FROM content_heads
WHERE
    bucket = $1
    AND kind = 'object'::content_kind
    AND live_seq IS NOT NULL
    AND ckey >= $2
    AND ckey < $3
    AND ckey > $4
    AND starts_with(ckey, $2)
ORDER BY ckey COLLATE "C"
LIMIT $5
