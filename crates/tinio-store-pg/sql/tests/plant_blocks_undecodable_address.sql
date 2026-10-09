-- A block row whose address is not an address this store ever wrote: decoding
-- it back has to be reported, never skipped.
INSERT INTO blocks (hash, seq, chunk, len, published) VALUES (
    'not-a-hex-address', 0, ''::bytea, 1, now()
)
