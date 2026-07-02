-- Reverse index: bytecode identity → contract addresses.
-- bucket = address[0] (first byte) — spreads wide bytecodes across partitions.
CREATE TABLE IF NOT EXISTS addresses_by_bytecode (
    hash         blob,
    seq          tinyint,
    bucket       smallint,
    address      text,
    block_number bigint,
    PRIMARY KEY ((hash, seq, bucket), address)
);
