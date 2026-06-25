#!/usr/bin/env bash
# Backfills missing-block spans found by find_missing_blocks.py, by re-running the
# (fixed, never-skip) indexer binary across each span. Idempotent on overwrite —
# safe to re-cover already-good blocks at span edges.
# Written 2026-06-23 — see CONTEXT.md item under "Активный план" for the incident
# this recovers from (historical-sync give-up-on-fetch-failure data loss bug).
#
# Usage: backfill_spans.sh <node: 62|63> <rpc_url> <spans_file> <log_file>
# spans_file: lines of "FROM TO" (inclusive), as produced from find_missing_blocks.py output.

set -euo pipefail

NODE="$1"
RPC_URL="$2"
SPANS_FILE="$3"
LOG_FILE="$4"
BIN="${INDEXER_BIN:-/data/pol_index/raw_pol_v3_watermark}"
FETCH_WORKERS="${5:-8}"
SPAN_TIMEOUT="${6:-3600}"

# Scylla password is REQUIRED from the environment — no hardcoded default. Other
# Scylla/GrayLog settings have safe (non-secret) defaults matching this deployment.
: "${SCYLLA_DB_PASSWORD:?SCYLLA_DB_PASSWORD must be set in the environment (see RUNBOOK.md)}"
SCYLLA_DB_HOST="${SCYLLA_DB_HOST:-127.0.0.1}"
SCYLLA_DB_PORT="${SCYLLA_DB_PORT:-9042}"
SCYLLA_DB_KEYSPACE="${SCYLLA_DB_KEYSPACE:-pol}"
SCYLLA_DB_USERNAME="${SCYLLA_DB_USERNAME:-cassandra}"
CM_CONNECTION_URL="${CM_CONNECTION_URL:-redis://127.0.0.1:6379/5}"
LOGS_GRAYLOG_HOST="${LOGS_GRAYLOG_HOST:-144.76.108.185}"
LOGS_GRAYLOG_PORT="${LOGS_GRAYLOG_PORT:-12201}"

echo "[backfill-$NODE] starting, $(wc -l < "$SPANS_FILE") spans, log=$LOG_FILE" | tee -a "$LOG_FILE"

i=0
total=$(wc -l < "$SPANS_FILE")
while read -r FROM TO; do
  i=$((i+1))
  echo "[backfill-$NODE] span $i/$total: $FROM -> $TO ($((TO-FROM+1)) blocks)" | tee -a "$LOG_FILE"
  env MODE=production EVM_CHAIN_ID=137 \
      CM_CONNECTION_URL="$CM_CONNECTION_URL" \
      SCYLLA_DB_HOST="$SCYLLA_DB_HOST" SCYLLA_DB_PORT="$SCYLLA_DB_PORT" SCYLLA_DB_KEYSPACE="$SCYLLA_DB_KEYSPACE" \
      SCYLLA_DB_USERNAME="$SCYLLA_DB_USERNAME" SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
      SCYLLA_CHUNK_BUCKETS=64 SCYLLA_CHUNK_ERA=32000 WS_DELAY_MS=0 \
      FETCH_WORKERS="$FETCH_WORKERS" \
      ACCUM_TXS_LANES=21 ACCUM_LOG_LANES=11 ACCUM_ITX_LANES=8 \
      RPC_URL="$RPC_URL" \
      LOGS_GRAYLOG_HOST="$LOGS_GRAYLOG_HOST" LOGS_GRAYLOG_PORT="$LOGS_GRAYLOG_PORT" \
      LOGS_GRAYLOG_APP="indexer-pol-$NODE-backfill" \
      timeout "$SPAN_TIMEOUT" "$BIN" --from="$FROM" --to="$TO" >> "$LOG_FILE" 2>&1 \
    || echo "[backfill-$NODE] span $i/$total ($FROM-$TO) FAILED or TIMED OUT (>${SPAN_TIMEOUT}s) — flagged for manual follow-up" | tee -a "$LOG_FILE"
done < "$SPANS_FILE"

echo "[backfill-$NODE] all spans done" | tee -a "$LOG_FILE"
