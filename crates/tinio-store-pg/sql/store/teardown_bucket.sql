WITH locked_heads AS MATERIALIZED (
    SELECT live_seq
    FROM content_heads
    WHERE bucket = $1
    FOR UPDATE
),

blocked AS (
    SELECT
        exists(
            SELECT 1 FROM object_meta AS o WHERE o.bucket = $1
        ) AS objects,
        exists(SELECT 1 FROM uploads AS u WHERE u.bucket = $1) AS uploads,
        exists(
            SELECT 1 FROM locked_heads WHERE live_seq IS NOT NULL
        ) AS live,
        exists(
            SELECT 1 FROM version_rows AS v WHERE v.bucket = $1
        ) AS versions,
        exists(
            SELECT 1 FROM version_object_lock AS l WHERE l.bucket = $1
        ) AS version_locks,
        exists(
            SELECT 1 FROM upload_object_lock AS u WHERE u.bucket = $1
        ) AS upload_locks
),

gate AS (
    SELECT
        not(
            coalesce((SELECT objects FROM blocked), FALSE)
            OR coalesce((SELECT uploads FROM blocked), FALSE)
            OR coalesce((SELECT live FROM blocked), FALSE)
            OR coalesce((SELECT versions FROM blocked), FALSE)
            OR coalesce((SELECT version_locks FROM blocked), FALSE)
            OR coalesce((SELECT upload_locks FROM blocked), FALSE)
        ) AS clear
    WHERE (SELECT count(*) FROM locked_heads) >= 0
),

del_versions AS (
    DELETE FROM version_rows
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_index AS (
    DELETE FROM version_index
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_slots AS (
    DELETE FROM current_slots
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_state AS (
    DELETE FROM bucket_versioning
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_content AS (
    DELETE FROM content
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_heads AS (
    DELETE FROM content_heads
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_bucket_locks AS (
    DELETE FROM bucket_object_lock
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_version_locks AS (
    DELETE FROM version_object_lock
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_upload_locks AS (
    DELETE FROM upload_object_lock
    WHERE bucket = $1 AND (SELECT clear FROM gate)
    RETURNING 1
),

del_bucket AS (
    DELETE FROM buckets
    WHERE name = $1 AND (SELECT clear FROM gate)
    RETURNING name
)

SELECT
    answered.clear AS "clear!",
    answered.removed AS "removed!"
FROM (
    SELECT
        coalesce((SELECT clear FROM gate), FALSE) AS clear,
        exists(SELECT 1 FROM del_bucket) AS removed,
        (SELECT count(*) FROM del_versions) AS versions_gone,
        (SELECT count(*) FROM del_index) AS index_gone,
        (SELECT count(*) FROM del_slots) AS slots_gone,
        (SELECT count(*) FROM del_state) AS state_gone,
        (SELECT count(*) FROM del_content) AS content_gone,
        (SELECT count(*) FROM del_heads) AS heads_gone,
        (SELECT count(*) FROM del_bucket_locks) AS bucket_locks_gone,
        (SELECT count(*) FROM del_version_locks) AS version_locks_gone,
        (SELECT count(*) FROM del_upload_locks) AS upload_locks_gone
) AS answered
