#!/usr/bin/env bash
# Backfills missing-block spans for the ETH ERC-20 indexer, by re-running the
# indexer binary across each span. Idempotent — primary key is chunk+block_number,
# so re-covering already-good blocks at span edges is safe.
#
# Usage:
#   backfill_spans_eth.sh <node: 07|60> <rpc_url> <spans_file> <log_file> [fetch_workers=10] [span_timeout=3600]
#
# spans_file: lines of "FROM TO" (inclusive), produced by make_spans_eth.py.
# Credentials must be in the environment (not on command line):
#   SCYLLA_DB_USERNAME, SCYLLA_DB_PASSWORD (required)
#   REDIS_PASSWORD (required — for CM_CONNECTION_URL checkpoint)
#
# Don't run at high concurrency against the same RPC node as a live indexer.

set -euo pipefail

NODE="$1"
RPC_URL="$2"
SPANS_FILE="$3"
LOG_FILE="$4"
FETCH_WORKERS="${5:-10}"
SPAN_TIMEOUT="${6:-3600}"

BIN="${INDEXER_BIN:-$HOME/raw_erc20_v9}"

: "${SCYLLA_DB_USERNAME:?SCYLLA_DB_USERNAME must be set}"
: "${SCYLLA_DB_PASSWORD:?SCYLLA_DB_PASSWORD must be set}"
: "${REDIS_PASSWORD:?REDIS_PASSWORD must be set}"

SCYLLA_DB_HOST="${SCYLLA_DB_HOST:-127.0.0.1}"
SCYLLA_DB_PORT="${SCYLLA_DB_PORT:-9042}"
SCYLLA_DB_KEYSPACE="${SCYLLA_DB_KEYSPACE:-eth}"

# Neighbor and reserve RPC (same as tuner defaults)
_NEIGHBOR_DEFAULTS_07="http://100.64.0.60:8545"
_NEIGHBOR_DEFAULTS_60="http://100.64.0.7:8545"
if [ "$NODE" = "07" ]; then
    NEIGHBOR_RPC="${NEIGHBOR_RPC_URL:-$_NEIGHBOR_DEFAULTS_07}"
else
    NEIGHBOR_RPC="${NEIGHBOR_RPC_URL:-$_NEIGHBOR_DEFAULTS_60}"
fi
RESERVE_RPC="${RESERVE_RPC_URL:-https://ethereum-rpc.publicnode.com}"

# Redis DB: 07→1, 60→2, else 3 (backfill uses own namespace to not clobber live watermark)
if [ "$NODE" = "07" ]; then REDIS_DB=1; elif [ "$NODE" = "60" ]; then REDIS_DB=2; else REDIS_DB=3; fi
CM_URL="${CM_CONNECTION_URL:-redis://:${REDIS_PASSWORD}@127.0.0.1:6379/${REDIS_DB}}"

GRAYLOG_HOST="${LOGS_GRAYLOG_HOST:-144.76.108.185}"
GRAYLOG_PORT="${LOGS_GRAYLOG_PORT:-12201}"
GRAYLOG_APP="indexer-eth-${NODE}-backfill"

total=$(wc -l < "$SPANS_FILE")
echo "[backfill-$NODE] starting: $total spans, fetch_workers=$FETCH_WORKERS, log=$LOG_FILE" | tee -a "$LOG_FILE"

i=0
while read -r FROM TO; do
  i=$((i+1))
  echo "[backfill-$NODE] span $i/$total: $FROM → $TO ($((TO-FROM+1)) blocks)" | tee -a "$LOG_FILE"
  env MODE=production \
      EVM_CHAIN_ID=1 \
      PRIMARY_RPC_HTTPS="$RPC_URL" \
      BACKUP_RPC_HTTPS="$NEIGHBOR_RPC" \
      BACKUP_RPC_HTTPS_2="$RESERVE_RPC" \
      CM_CONNECTION_URL="$CM_URL" \
      SCYLLA_DB_HOST="$SCYLLA_DB_HOST" \
      SCYLLA_DB_PORT="$SCYLLA_DB_PORT" \
      SCYLLA_DB_KEYSPACE="$SCYLLA_DB_KEYSPACE" \
      SCYLLA_DB_USERNAME="$SCYLLA_DB_USERNAME" \
      SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
      SCYLLA_CHUNK_BUCKETS=24 \
      SCYLLA_CHUNK_ERA=12000 \
      FETCH_WORKERS="$FETCH_WORKERS" \
      SAVE_EVERY=100 \
      LOGS_GRAYLOG_HOST="$GRAYLOG_HOST" \
      LOGS_GRAYLOG_PORT="$GRAYLOG_PORT" \
      LOGS_GRAYLOG_APP="$GRAYLOG_APP" \
      timeout "$SPAN_TIMEOUT" "$BIN" --from="$FROM" --to="$TO" >> "$LOG_FILE" 2>&1 \
    || echo "[backfill-$NODE] span $i/$total ($FROM-$TO) FAILED or timed out (>${SPAN_TIMEOUT}s) — needs follow-up" | tee -a "$LOG_FILE"
done < "$SPANS_FILE"

echo "[backfill-$NODE] all $total spans done" | tee -a "$LOG_FILE"
