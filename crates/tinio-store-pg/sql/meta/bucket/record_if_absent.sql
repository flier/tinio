INSERT INTO buckets (name, created, tags, acl, cors)
VALUES ($1, $2, '{}', row(NULL, ARRAY[]::acl_grant[])::acl, '')
ON CONFLICT (name) DO NOTHING
