CREATE TABLE IF NOT EXISTS erc20_owners (
  address text,
  chain_id int,
  initial_owner text,
  latest_owner text,
  is_ownership_renounced boolean,
  updated_at_block bigint,
  updated_at_timestamp bigint,
  PRIMARY KEY (address)
);
