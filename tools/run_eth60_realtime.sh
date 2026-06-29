#!/usr/bin/env bash
# Direct realtime launch for eth60 — no tuner, no --to.
# Binary queries HEAD from WSS, catches up historically, then enters WSS realtime loop.
# Resume from watermark on restart.
#
# Use this when the tuner is not needed (fixed FETCH_WORKERS=64, no AIMD).
# For tuner-managed realtime mode, use run_tuner_60.sh with TO_BLOCK=0.
#
# Deployed to ~/run_eth60_realtime.sh on 100.64.0.4, launched 2026-06-29.
set -uo pipefail

export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
export REDIS_PASSWORD=ZCy8k4G6pcRYVFfm

BIN="$HOME/raw_erc20_v11"
LOG="$HOME/eth_index_60.log"
REDIS_DB=2

WATERMARK=$(grep -aoP 'Accum \d+→\K\d+' "$LOG" 2>/dev/null | sort -n | tail -1)
FROM=${WATERMARK:-25422404}

echo "[realtime] starting from block $FROM (no --to → realtime WSS after catch-up)" >> "$LOG"

exec env \
  MODE=production \
  EVM_CHAIN_ID=1 \
  PRIMARY_RPC_HTTPS=http://100.64.0.60:8545 \
  PRIMARY_RPC_WSS=ws://100.64.0.60:8546 \
  BACKUP_RPC_HTTPS=http://100.64.0.7:8545 \
  BACKUP_RPC_HTTPS_2=https://ethereum-rpc.publicnode.com \
  CM_CONNECTION_URL="redis://:${REDIS_PASSWORD}@127.0.0.1:6379/${REDIS_DB}" \
  SCYLLA_DB_HOST=127.0.0.1 \
  SCYLLA_DB_PORT=9042 \
  SCYLLA_DB_KEYSPACE=eth \
  SCYLLA_DB_USERNAME="$SCYLLA_DB_USERNAME" \
  SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
  SCYLLA_CHUNK_BUCKETS=24 \
  SCYLLA_CHUNK_ERA=12000 \
  FETCH_WORKERS=64 \
  SAVE_EVERY=100 \
  LOGS_GRAYLOG_HOST=144.76.108.185 \
  LOGS_GRAYLOG_PORT=12201 \
  LOGS_GRAYLOG_APP=indexer-eth-60 \
  "$BIN" --from="$FROM" >> "$LOG" 2>&1
