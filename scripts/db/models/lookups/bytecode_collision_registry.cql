-- Populated only on sha256 collision (probability astronomically small).
-- Exists so a collision is never silently lost — both conflicting bytecodes
-- and their owning addresses are recorded here for post-hoc audit.
CREATE TABLE IF NOT EXISTS bytecode_collision_registry (
  bytecode_hash  blob,
  bytecode       blob,
  address        text,
  creation_block bigint,
  PRIMARY KEY (bytecode_hash, bytecode, address)
) WITH CLUSTERING ORDER BY (bytecode ASC, address ASC);
