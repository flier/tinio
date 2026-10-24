INSERT INTO buckets (name, created, tags, acl, cors)
VALUES ($1, $2, $3, row(NULL, ARRAY[]::acl_grant[])::acl, '')
ON CONFLICT (
    name) DO UPDATE SET tags = excluded.tags
WHERE buckets.tags IS DISTINCT FROM excluded.tags
RETURNING (xmax = 0) AS inserted
