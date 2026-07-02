-- Populated only when sha256 collision is detected (astronomically rare).
-- Records that the hash partition has more than one distinct bytecode.
-- Alongside existing bytecode_collision_registry (untouched).
CREATE TABLE IF NOT EXISTS collision_registry_v2 (
    hash        blob PRIMARY KEY,
    detected_at timestamp,
    seq_count   tinyint,
    note        text
);
