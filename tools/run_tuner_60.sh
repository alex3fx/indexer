#!/usr/bin/env bash
# ETH tuner — node .60 (realtime: catches up to HEAD, then follows via WSS)
# Credentials in file, not on command line (not visible in ps aux).
set -uo pipefail

export SCYLLA_DB_USERNAME=cassandra
export SCYLLA_DB_PASSWORD=cassandra
export REDIS_PASSWORD=ZCy8k4G6pcRYVFfm
export GRAFANA_USER=alexey.smolyakov@lotos.io
export GRAFANA_PASS='OX8OYykA2!jtWv'
export GRAFANA_URL=https://grafana.lotos-team.com
export GRAFANA_PROM_UID=PBFA97CFB590B2093
export INDEXER_BIN="$HOME/raw_erc20_v11"
export TUNER_LOG="$HOME/eth_index_60.log"

# TO_BLOCK=0 → realtime mode: binary launched without --to, discovers HEAD from WSS,
# does historical catch-up, then enters WSS realtime loop (log.info per block → GrayLog).
exec python3 "$HOME/dynamic_tuner_eth.py" 60 http://100.64.0.60:8545 12700001 0
