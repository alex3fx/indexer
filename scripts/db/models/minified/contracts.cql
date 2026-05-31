CREATE TABLE IF NOT EXISTS contracts (
  chunk int,
  block_number bigint,
  transaction_index int,
  transaction_hash text,
  trace_index int,
  block_timestamp_s bigint,
  block_timestamp_ms bigint,
  address text,
  creation_method tinyint,
  creator_address text,
  contract_factory text,
  creation_bytecode text,
  deployed_bytecode text,
  PRIMARY KEY ((chunk), block_number, transaction_index, trace_index)
) WITH CLUSTERING ORDER BY (block_number ASC, transaction_index ASC, trace_index ASC);