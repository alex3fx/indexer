CREATE TABLE IF NOT EXISTS contracts_by_addresses (
  address text,
  creator text,
  tx_hash text,
  block_number bigint,
  timestamp bigint,
  contract_factory text,
  creation_bytecode text,
  deployed_bytecode text,
  PRIMARY KEY (address)
);