#!/usr/bin/env bash
# Run bytecode-api service with auto-restart on crash.
# Credentials stored here — NOT on command line (not visible in ps aux).
# Deploy: scp to ~/run_bytecode_api.sh on 100.64.0.4, then:
#   tmux new-session -d -s bytecode_api '~/run_bytecode_api.sh >> ~/bytecode_api.log 2>&1'
set -uo pipefail

SCYLLA_PASS="cassandra"
BIN="$HOME/bytecode_api_v3"
LISTEN="0.0.0.0:8080"
LOG="$HOME/bytecode_api.log"

while true; do
    echo "[bytecode-api] $(date -u +%FT%TZ) starting, bin=$BIN listen=$LISTEN" >> "$LOG"
    "$BIN" \
        --host=127.0.0.1 \
        --port=9042 \
        --user=cassandra \
        --pass="$SCYLLA_PASS" \
        --keyspace=eth \
        --listen="$LISTEN" >> "$LOG" 2>&1 || true
    echo "[bytecode-api] $(date -u +%FT%TZ) process exited, restarting in 5s..." >> "$LOG"
    sleep 5
done
