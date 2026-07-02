-- Per-bytecode counters. Counter tables can't use BATCH UNLOGGED; write via EXECUTE.
CREATE TABLE IF NOT EXISTS bytecode_stats (
    hash          blob,
    seq           tinyint,
    address_count counter,
    PRIMARY KEY ((hash, seq))
);
