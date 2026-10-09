WITH gone_parts AS (
    DELETE FROM parts WHERE bucket = $1
),

gone_part_checksums AS (
    DELETE FROM part_checksums WHERE bucket = $1
),

gone_checksums AS (
    DELETE FROM upload_checksums WHERE bucket = $1
)

DELETE FROM uploads WHERE bucket = $1
