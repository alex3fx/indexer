-- Chunked source archive storage (~512KB chunks per bytecode identity).
CREATE TABLE IF NOT EXISTS source_store (
    hash  blob,
    seq   tinyint,
    chunk int,
    data  blob,
    PRIMARY KEY ((hash, seq), chunk)
);
