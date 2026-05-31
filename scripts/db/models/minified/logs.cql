CREATE TABLE IF NOT EXISTS logs (
  chunk int,
  block_number bigint,
  transaction_index int,
  log_index int,
  block_timestamp_s bigint,
  block_timestamp_ms bigint,
  address text,
  data text,
  topic_zeroth text,
  topic_first text,
  topic_second text,
  topic_third text,
  rest_topics list<text>,
  transaction_hash text,
  removed boolean,
  PRIMARY KEY ((chunk), block_number, transaction_index, log_index)
) WITH CLUSTERING ORDER BY (block_number ASC, transaction_index ASC, log_index ASC);