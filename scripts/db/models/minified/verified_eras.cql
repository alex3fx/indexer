CREATE TABLE IF NOT EXISTS verified_eras (
  era               bigint,
  verified_at_ms    bigint,
  missing_found     int,
  missing_reindexed int,
  PRIMARY KEY (era)
);
