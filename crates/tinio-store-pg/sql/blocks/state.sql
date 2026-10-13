SELECT
    count(*) AS "rows!",
    coalesce(max(seq), -1) AS "last!",
    coalesce(max(len), 0) AS "len!"
FROM blocks
WHERE hash = $1
