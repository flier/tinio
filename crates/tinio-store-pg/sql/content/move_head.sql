UPDATE content_heads SET ckey = $3
WHERE bucket = $1 AND kind = 'object'::content_kind AND ckey = $2
