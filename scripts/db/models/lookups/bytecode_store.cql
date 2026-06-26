CREATE TABLE IF NOT EXISTS bytecode_store (
  bytecode_hash blob,
  bytecode      blob,
  size          int,
  first_seen_block bigint,
  has_collision boolean,
  PRIMARY KEY (bytecode_hash)
);
