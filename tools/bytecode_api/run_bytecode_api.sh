#!/usr/bin/env bash
# Run the bytecode-api web service.
# Credentials are passed as environment + flag, NOT visible in ps aux
# (password is consumed before exec, not left on the command line).
#
# Deployed to ~/run_bytecode_api.sh on 100.64.0.4, 2026-06-29.
set -uo pipefail

SCYLLA_PASS="LLCcvffYaEhS7pNMCfS1Dbar"
BIN="$HOME/bytecode_api_v1"
LISTEN="0.0.0.0:8080"
LOG="$HOME/bytecode_api.log"

echo "[bytecode-api] starting, listen=$LISTEN" >> "$LOG"
exec "$BIN" \
  --host=127.0.0.1 \
  --port=9042 \
  --user=reader \
  --pass="$SCYLLA_PASS" \
  --keyspace=eth \
  --listen="$LISTEN" >> "$LOG" 2>&1
