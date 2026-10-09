WITH locked AS MATERIALIZED (
    SELECT name FROM buckets WHERE name = $1 FOR UPDATE
),

removed AS (
    DELETE FROM buckets AS b
    WHERE
        b.name = $1
        AND EXISTS (SELECT 1 FROM locked)
        AND NOT EXISTS (SELECT 1 FROM object_meta AS o WHERE o.bucket = b.name)
        AND NOT EXISTS (SELECT 1 FROM uploads AS u WHERE u.bucket = b.name)
    RETURNING b.name
)

SELECT
    exists(SELECT 1 FROM locked) AS "present!",
    exists(SELECT 1 FROM removed) AS "removed!"
