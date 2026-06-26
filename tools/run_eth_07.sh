#!/usr/bin/env bash
# ETH indexer — node .7 instance (blocks 0 → 12,700,000)
# Primary: 100.64.0.7:8545  |  Backup1: 100.64.0.60:8545  |  Backup2: publicnode
# Redis DB=1 (isolated from node-60 instance which uses DB=2)
# Auto-restart loop; resumes from own log (NOT shared Redis cursor).

set -uo pipefail

BINARY="$HOME/raw_erc20_v8"
LOG="$HOME/eth_index_07.log"
REDIS_BASE="redis://:YOUR_REDIS_PASSWORD@100.64.0.4:6379"

while true; do
    # Determine resume point from own log.
    LAST=$(grep -oP 'Accum \d+→\K\d+' "$LOG" 2>/dev/null | tail -1 || true)
    if [[ -n "$LAST" ]]; then
        FROM=$(( LAST + 1 ))
    else
        FROM=0
    fi

    echo "$(date -u +%FT%TZ)  Starting node-07 instance: FROM=$FROM TO=12700000" | tee -a "$LOG"

    EVM_CHAIN_ID=1 \
    MODE=production \
    LOGS_GRAYLOG_HOST=144.76.108.185 \
    LOGS_GRAYLOG_PORT=12201 \
    LOGS_GRAYLOG_APP=indexer-eth-07 \
    PRIMARY_RPC_HTTPS=http://100.64.0.7:8545 \
    PRIMARY_RPC_WSS=ws://100.64.0.7:8546 \
    BACKUP_RPC_HTTPS=http://100.64.0.60:8545 \
    BACKUP_RPC_HTTPS_2=https://ethereum-rpc.publicnode.com \
    CM_CONNECTION_URL="${REDIS_BASE}/1" \
    SCYLLA_DB_HOST=127.0.0.1 \
    SCYLLA_DB_PORT=9042 \
    SCYLLA_DB_KEYSPACE=eth \
    SCYLLA_DB_USERNAME=cassandra \
    SCYLLA_DB_PASSWORD=cassandra \
    FETCH_WORKERS=64 \
    SAVE_EVERY=100 \
    SCYLLA_CHUNK_BUCKETS=64 \
    "$BINARY" --from="$FROM" --to=12700000 2>&1 | tee -a "$LOG" || true

    echo "$(date -u +%FT%TZ)  node-07 instance exited — restarting in 5s" | tee -a "$LOG"
    sleep 5
done
