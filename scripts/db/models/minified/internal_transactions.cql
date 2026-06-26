CREATE TABLE IF NOT EXISTS internal_transactions (
  chunk int,
  block_number bigint,
  block_timestamp_s bigint,
  block_timestamp_ms bigint,
  transaction_index int,
  transaction_hash text,
  trace_index int,
  from_address text,
  to_address text,
  value varint,
  PRIMARY KEY ((chunk), block_number, transaction_index, trace_index)
) WITH CLUSTERING ORDER BY (block_number ASC, transaction_index ASC, trace_index ASC);