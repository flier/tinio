INSERT INTO buckets (name, created, tags, acl, cors)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (name) DO UPDATE
    SET
        created = excluded.created,
        tags = excluded.tags,
        acl = excluded.acl, cors = excluded.cors
