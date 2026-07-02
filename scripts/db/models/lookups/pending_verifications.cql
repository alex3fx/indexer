-- Verification requests queued for addresses not yet indexed.
-- Resolved when the indexer later encounters the contract deploy.
CREATE TABLE IF NOT EXISTS pending_verifications (
    address     blob PRIMARY KEY,
    source      blob,
    abi         blob,
    received_at timestamp
);
