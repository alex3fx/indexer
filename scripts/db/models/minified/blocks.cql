CREATE TABLE IF NOT EXISTS blocks (
  chunk int,
  number bigint,
  timestamp_s bigint,
  timestamp_ms bigint,
  miner text,
  PRIMARY KEY ((chunk), number)
) WITH CLUSTERING ORDER BY (number ASC);