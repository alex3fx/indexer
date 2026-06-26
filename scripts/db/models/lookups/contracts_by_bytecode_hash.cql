CREATE TABLE IF NOT EXISTS contracts_by_bytecode_hash (
  bytecode_hash  blob,
  address        text,
  creation_block bigint,
  PRIMARY KEY (bytecode_hash, address)
) WITH CLUSTERING ORDER BY (address ASC);
