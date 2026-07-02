-- Deduplicated bytecode store with SHA256 collision resolution.
-- PK: (hash, seq) where seq=0 always, seq>0 only on real sha256 collision.
-- Alongside existing bytecode_store (untouched).
CREATE TABLE IF NOT EXISTS bytecode_store_v2 (
    hash        blob,
    seq         tinyint,
    bytecode    blob,
    check_hash  blob,       -- keccak256(bytecode), independent check hash
    size        int,
    kind        tinyint,    -- 0=deployed, 1=creation
    verified    boolean,
    verified_at timestamp,
    verified_via_address blob,
    abi         blob,       -- plain json; Scylla ZstdCompressor handles compression
    source_ref  text,
    PRIMARY KEY ((hash), seq)
) WITH compression = {'sstable_compression': 'ZstdCompressor'};
