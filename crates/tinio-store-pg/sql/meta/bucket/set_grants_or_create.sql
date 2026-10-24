INSERT INTO buckets (name, created, tags, acl, cors)
VALUES ($1, $2, '{}', $3, '')
ON CONFLICT (
    name) DO UPDATE SET acl.grants = ($3::acl).grants
WHERE (buckets.acl).grants IS DISTINCT FROM ($3::acl).grants
RETURNING (xmax = 0) AS inserted
