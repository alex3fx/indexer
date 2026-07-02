-- Contract address → bytecode identity mapping (v2, uses BytecodeId references).
-- Alongside existing contracts_by_addresses (untouched).
CREATE TABLE IF NOT EXISTS contracts_by_address_v2 (
    address        text,
    block_number   bigint,
    bytecode_hash  blob,
    bytecode_seq   tinyint,
    creation_hash  blob,
    creation_seq   tinyint,
    tx_hash        text,
    deployer       text,
    PRIMARY KEY ((address), block_number)
) WITH CLUSTERING ORDER BY (block_number DESC);
