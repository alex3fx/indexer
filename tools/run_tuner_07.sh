#!/usr/bin/env bash
# ETH tuner — node .07 (realtime from HEAD, 2026-06-29)
# Previously: 0 → 12,700,000 (historical, completed 2026-06-29)
# Now: HEAD → ∞ (realtime backup alongside eth60)
# Credentials in file, not on command line (not visible in ps aux).
set -uo pipefail

export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
export REDIS_PASSWORD=ZCy8k4G6pcRYVFfm
export GRAFANA_USER=alexey.smolyakov@lotos.io
export GRAFANA_PASS='OX8OYykA2!jtWv'
export GRAFANA_URL=https://grafana.lotos-team.com
export GRAFANA_PROM_UID=PBFA97CFB590B2093
export INDEXER_BIN="$HOME/raw_erc20_v10"
export TUNER_LOG="$HOME/eth_index_07.log"

exec python3 "$HOME/dynamic_tuner_eth.py" 07 http://100.64.0.7:8545 25418655 999000000
