# Tools

Supporting scripts for running and operating the dual-node historical indexer. Full env-var
reference and step-by-step launch instructions: **`docs/HOWTOSTART.md`**.

All scripts read credentials from the environment — `SCYLLA_DB_PASSWORD` is required (no
hardcoded default anywhere in this directory); `GRAFANA_USER`/`GRAFANA_PASS` are optional
(only needed for the tuner's load-aware signal).

## `run_eth_07.sh` / `run_eth_60.sh` — dual-node ETH indexer launch scripts

Two-instance setup: node `.7` covers `[0, 12_700_000]`, node `.60` covers `[12_700_001, HEAD]`.
Each script runs an auto-restart loop and resumes from its own log file (not the shared Redis cursor).
Three-tier RPC fallback: primary → neighbor → `ethereum-rpc.publicnode.com`.

Before first use:
1. Replace `YOUR_REDIS_PASSWORD` with the actual Redis password.
2. Set `SCYLLA_DB_PASSWORD`, keyspace/user/host for the target cluster.
3. Deploy binary as `~/raw_erc20_v7` on `100.64.0.4` (see `CONTEXT.md` for deploy commands).

```bash
# On 100.64.0.4, in separate tmux panes:
nohup ./tools/run_eth_07.sh &
nohup ./tools/run_eth_60.sh &
```

Env vars that matter:
- `CM_CONNECTION_URL` — `redis://...@host:port/<DB>` — node-07 uses DB=1, node-60 uses DB=2
- `PRIMARY_RPC_HTTPS` / `BACKUP_RPC_HTTPS` / `BACKUP_RPC_HTTPS_2` — override chain defaults

**Status:** working (2026-06-26), requires binary v7+

---

## `dynamic_tuner.py` — launch + load-aware concurrency supervisor

Bootstrap-probes a few `FETCH_WORKERS` values, then runs a continuous AIMD control loop driven
by the RPC node's `node_load1` (Grafana/Prometheus) — raises concurrency when load is low, backs
off when high. Restarts on crash with exponential backoff. Resumes from the log's last
`[watermark] N` line automatically (the true contiguous-safe resume point — see
`docs/HOWTOSTART.md` for why this differs from the `Accum X→Y` line).

```bash
export SCYLLA_DB_PASSWORD='...'
export GRAFANA_USER='...' GRAFANA_PASS='...'   # optional
python3 dynamic_tuner.py <node_id> <rpc_url> <from_block_if_no_log_yet> <to_block>
```

Per-deployment topology (RPC URLs, Prometheus instance labels, worker bounds) is set near the
top of the script — edit `_NEIGHBOR_DEFAULTS`/`_NODE_INFO`/`_WORKER_BOUNDS`, or override via env
(`NEIGHBOR_RPC_URL`, `NODE_62_INSTANCE`, etc.) without touching the script.

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
