CREATE TABLE IF NOT EXISTS erc20_self_destructed (
  address text,
  chain_id int,
  at_block bigint,
  at_timestamp bigint,
  PRIMARY KEY (address)
);
