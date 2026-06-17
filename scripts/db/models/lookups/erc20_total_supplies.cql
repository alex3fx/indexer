CREATE TABLE IF NOT EXISTS erc20_total_supplies (
  address text,
  chain_id int,
  initial_total_supply text,
  latest_total_supply text,
  updated_at_block bigint,
  updated_at_timestamp bigint,
  PRIMARY KEY (address)
);
