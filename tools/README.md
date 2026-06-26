# Tools

Supporting scripts for running and operating the dual-node historical indexer. Full env-var
reference and step-by-step launch instructions: **`docs/HOWTOSTART.md`**.

All scripts read credentials from the environment — `SCYLLA_DB_PASSWORD` is required (no
hardcoded default anywhere in this directory); `GRAFANA_USER`/`GRAFANA_PASS` are optional
(only needed for the tuner's load-aware signal).

## `dynamic_tuner_eth.py` + `run_tuner_07.sh` / `run_tuner_60.sh` — ETH tuner (active)

Bootstrap-probes FETCH_WORKERS (W=16/32/48/64 for node .07; W=32/48/64/96 for node .60), then
runs a continuous AIMD control loop driven by the RPC node's `node_load1` from Grafana/Prometheus.
Restarts the indexer on crash, resumes from `[watermark] N` in the log, sends alerts to GrayLog.

`run_tuner_07.sh` / `run_tuner_60.sh` are credential wrappers — credentials live in the file, not
on the command line (not visible in `ps aux`). Deployed to `~/` on `100.64.0.4`.

```bash
# Start (tmux sessions eth07 / eth60 on 100.64.0.4)
tmux new-session -d -s eth07 '~/run_tuner_07.sh >> ~/eth_tuner_07.log 2>&1'
tmux new-session -d -s eth60 '~/run_tuner_60.sh >> ~/eth_tuner_60.log 2>&1'

# Status
tail -f ~/eth_tuner_07.log
tail -f ~/eth_tuner_60.log
grep 'steady:' ~/eth_tuner_07.log | tail -5   # blk/s and FETCH_WORKERS

# Stop
tmux kill-session -t eth07
tmux kill-session -t eth60
```

Hardware: node .07 = 48 cores (157.90.65.123), node .60 = 96 cores (100.64.0.60).
Load target: 0.85–0.95 of core count. Cooldown between adjustments: 90s.
**Status:** running (2026-06-26), requires binary v8+, supersedes `run_eth_07.sh`/`run_eth_60.sh`.

---

## `run_eth_07.sh` / `run_eth_60.sh` — static wrapper scripts (superseded)

Simple auto-restart loops with fixed FETCH_WORKERS=64. Superseded by `dynamic_tuner_eth.py`
which probes and adjusts FETCH_WORKERS automatically. Kept for reference / manual override.

**Status:** superseded (2026-06-26), requires binary v8+

---

## `dynamic_tuner.py` — Polygon indexer tuner (not for ETH)

Polygon-specific (chain_id=137, nodes .62/.63, keyspace=pol). See the Polygon indexer repo
(`/home/alex/lotos/task1/devindexer/indexer`) for usage. Use `dynamic_tuner_eth.py` for ETH.

```bash
export SCYLLA_DB_PASSWORD='...'
export GRAFANA_USER='...' GRAFANA_PASS='...'   # optional
python3 dynamic_tuner.py <node_id> <rpc_url> <from_block_if_no_log_yet> <to_block>
```

## `monitor_pol_v3.py` — heartbeat/alert monitor

Tails the node log files, sends GELF heartbeats (block height, blk/s, ETA, alive status) to
GrayLog periodically, and fires immediate alerts on `[ALERT]`/`[WARNING]` log lines. Seeds its
read position at the current end-of-file on startup, so restarting the monitor never re-sends
old alerts as new.

```bash
python3 monitor_pol_v3.py
```

## `find_missing_blocks.py` — integrity scanner

Scans every `(lane, era)` chunk partition in a block range and prints the exact missing block
numbers (one per line) — an arithmetic-sequence check per lane, not `ALLOW FILTERING`, so it
doesn't under-sample like naive chunk-number enumeration does.

```bash
export SCYLLA_DB_PASSWORD='...'
python3 find_missing_blocks.py FROM_BLOCK TO_BLOCK [LANES=64] [ERA=32000] [TABLE=blocks] [CONCURRENCY=64]
```

Run this after any historical sync range is claimed complete, and after any manual backfill,
before trusting the data as gap-free.

## `backfill_spans.sh` — targeted re-sync for known gaps

Re-runs the indexer binary across each `FROM TO` span from a file (group `find_missing_blocks.py`
output into contiguous ranges first). Idempotent — primary key is `chunk + block_number`, so it's
safe to re-cover already-good blocks at span edges.

```bash
export SCYLLA_DB_PASSWORD='...'
./backfill_spans.sh <node_id> <rpc_url> <spans_file> <log_file> [fetch_workers=8] [span_timeout=3600]
```

Don't run this concurrently with live indexing at high concurrency against the same RPC node —
the two compete for the node's resources. Keep `fetch_workers` low (5-10) during backfill.
