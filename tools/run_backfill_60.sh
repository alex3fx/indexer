#!/usr/bin/env bash
# Bytecode-store backfill: node .60, blocks 12 700 001 → 25 422 776.
# Uses Redis DB=4 (dedicated cursor, doesn't touch DB=2 used by realtime).
# Auto-restarts on crash, resumes from log watermark.
set -uo pipefail

BIN="$HOME/raw_erc20_v12"
LOG="$HOME/eth_backfill_60.log"
FROM_DEFAULT=12700001
TO=25422776
REDIS_DB=4

while true; do
    LAST=$(grep -oP 'Accum \d+→\K\d+' "$LOG" 2>/dev/null | sort -n | tail -1)
    if [[ -n "$LAST" && "$LAST" -ge "$TO" ]]; then
        echo "$(date -u +%FT%TZ)  Backfill 60 complete at block $LAST" | tee -a "$LOG"
        exit 0
    fi
    FROM=${LAST:-$FROM_DEFAULT}
    echo "$(date -u +%FT%TZ)  Starting backfill 60: FROM=$FROM TO=$TO" | tee -a "$LOG"

    env \
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
        SCYLLA_DB_USERNAME=cassandra \
        SCYLLA_DB_PASSWORD=cassandra \
        SCYLLA_CHUNK_BUCKETS=24 \
        SCYLLA_CHUNK_ERA=12000 \
        FETCH_WORKERS=64 \
        SAVE_EVERY=100 \
        "$BIN" --from="$FROM" --to="$TO" >> "$LOG" 2>&1 || true

    echo "$(date -u +%FT%TZ)  Process exited, restarting in 5s..." | tee -a "$LOG"
    sleep 5
done
