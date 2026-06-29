#!/usr/bin/env bash
# ETH tuner — node .60 (blocks 12,700,001 → HEAD, then realtime)
# Credentials in file, not on command line (not visible in ps aux).
set -uo pipefail

export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
export REDIS_PASSWORD=ZCy8k4G6pcRYVFfm
export GRAFANA_USER=alexey.smolyakov@lotos.io
export GRAFANA_PASS='OX8OYykA2!jtWv'
export GRAFANA_URL=https://grafana.lotos-team.com
export GRAFANA_PROM_UID=PBFA97CFB590B2093
export INDEXER_BIN="$HOME/raw_erc20_v9"
export TUNER_LOG="$HOME/eth_index_60.log"

# TO_BLOCK=999000000 — never-reached sentinel; tuner runs indefinitely (follows HEAD)
exec python3 "$HOME/dynamic_tuner_eth.py" 60 http://100.64.0.60:8545 12700001 999000000
