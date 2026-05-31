CREATE TABLE IF NOT EXISTS block_completions (
  chunk          int,
  block_number   bigint,
  tx_count       int,
  log_count      int,
  itx_count      int,
  contract_count int,
  PRIMARY KEY ((chunk), block_number)
) WITH CLUSTERING ORDER BY (block_number ASC);