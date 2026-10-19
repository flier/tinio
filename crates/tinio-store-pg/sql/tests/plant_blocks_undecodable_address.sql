INSERT INTO blocks (hash, seq, chunk, len, published) VALUES (
    'not-a-hex-address', 0, ''::bytea, 1, now()
)
