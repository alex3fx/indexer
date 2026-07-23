CREATE TABLE IF NOT EXISTS eth.forked_blocks (
    block_number bigint,
    block_hash text,
    miner text,
    block_timestamp bigint,
    era text,
    depth int,
    reorg_group_id text,
    affected_txns_count_orphan int,
    affected_txns_count_lost int,
    affected_logs_count_orphan int,
    affected_logs_count_lost int,
    affected_traces_count_orphan int,
    affected_traces_count_lost int,
    PRIMARY KEY (block_number, block_hash)
);
