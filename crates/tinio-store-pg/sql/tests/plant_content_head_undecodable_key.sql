-- A live head whose encoded key starts with the object prefix but cannot decode
-- into an object key: a traversal has to report the row, never yield the key.
INSERT INTO content_heads
(bucket, kind, ckey, next_seq, live_seq, live_len)
VALUES (
    $1, 'object', 'o:', 1, 1, 0
)
