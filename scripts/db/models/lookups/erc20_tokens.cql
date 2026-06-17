CREATE TABLE IF NOT EXISTS erc20_tokens (
  address text,
  chain_id int,
  name text,
  symbol text,
  decimals smallint,
  has_balance_of boolean,
  has_transfer boolean,
  has_transfer_from boolean,
  has_approve boolean,
  has_allowance boolean,
  is_standard_decimals boolean,
  is_fully_following_standard boolean,
  is_minimally_following_standard boolean,
  is_partially_following_standard boolean,
  is_not_following_standard boolean,
  detection_version int,
  PRIMARY KEY (address)
);
