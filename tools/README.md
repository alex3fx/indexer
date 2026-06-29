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
**Status:** running (2026-06-27), requires binary v10+, supersedes `run_eth_07.sh`/`run_eth_60.sh`.
**v3 chunk scheme active:** `SCYLLA_CHUNK_BUCKETS=24`, `SCYLLA_CHUNK_ERA=12000`
(`chunk = (block % 24) + 24 * (block // 12000)` — 500 blk/partition, 24 partitions/era).
Switched from LANES=64 flat (hot partition risk) via TRUNCATE+RESTART on 2026-06-27.
**Realtime mode (2026-06-29):** `TO_BLOCK=0` in `run_tuner_60.sh` → binary launched without `--to`,
catches up historically, enters WSS realtime loop. Per-block `log.info` → GrayLog every ~12s.

---

## `run_eth_07.sh` / `run_eth_60.sh` — static wrapper scripts (superseded)

Simple auto-restart loops with fixed FETCH_WORKERS=64. Superseded by `dynamic_tuner_eth.py`
which probes and adjusts FETCH_WORKERS automatically. Kept for reference / manual override.

**Status:** superseded (2026-06-26), requires binary v8+. Do NOT use — chunk scheme was LANES=64 flat (incompatible with current v3 scheme)

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
export SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD='...' SCYLLA_DB_KEYSPACE=eth
python3.12 find_missing_blocks.py FROM_BLOCK TO_BLOCK [LANES=24] [ERA=12000] [TABLE=blocks] [CONCURRENCY=128]
```

Run this after any historical sync range is claimed complete, and after any manual backfill,
before trusting the data as gap-free. Output: one missing block number per line on stdout;
summary ("Total missing: N") to stderr. Requires `python3.12` (cassandra-driver installed there).

**2026-06-29 run**: found 6,032 missing blocks across 235 regions in 0→25.4M.
Results saved to `~/missing_blocks_eth.txt` on 100.64.0.4.

## `make_spans_eth.py` — groups missing blocks into re-index spans

Takes `find_missing_blocks.py` output (one block number per line) and groups consecutive missing
blocks into contiguous `FROM TO` spans for backfill. Splits output at block 12,700,000 (the
.07/.60 RPC split point) into two separate files.

```bash
python3 make_spans_eth.py <missing_blocks.txt> [gap_tolerance=500] [padding=10]
# produces: missing_blocks_eth_spans_07.txt and missing_blocks_eth_spans_60.txt
```

**2026-06-29**: 6,032 missing blocks → 232 spans (134 for .07, 98 for .60).

## `backfill_spans_eth.sh` — targeted re-sync for known gaps (ETH)

Re-runs the ETH indexer binary across each `FROM TO` span from a file. Idempotent — primary key
is `chunk + block_number`, so re-covering already-good blocks at span edges is safe.
Uses `SCYLLA_CHUNK_BUCKETS=24 SCYLLA_CHUNK_ERA=12000 EVM_CHAIN_ID=1 SCYLLA_DB_KEYSPACE=eth`.

```bash
export SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD='...' REDIS_PASSWORD='...'
./backfill_spans_eth.sh <node: 07|60> <rpc_url> <spans_file> <log_file> [fetch_workers=10] [span_timeout=3600]
```

Deployed to `~/` on 100.64.0.4 along with `run_backfill_07.sh` / `run_backfill_60.sh` wrappers.
Running in tmux sessions `backfill07` / `backfill60` as of 2026-06-29.

**Status:** active (2026-06-29) — backfilling 6,032 missing blocks from overload period.
Logs: `~/eth_backfill_07.log`, `~/eth_backfill_60.log`.

## `run_eth60_realtime.sh` — direct realtime launch (no tuner)

Direct launch of eth60 without tuner — fixed `FETCH_WORKERS=64`, no AIMD, no probing.
Binary started without `--to`: discovers HEAD from WSS, catches up historically, enters
WSS realtime loop (`log.info` per block → GrayLog every ~12s).

```bash
# On 100.64.0.4, in tmux session eth60:
tmux new-session -d -s eth60 '~/run_eth60_realtime.sh'
# Resume: watermark auto-detected from eth_index_60.log
```

**Status:** deployed (2026-06-29) on 100.64.0.4. Launched when tuner is not needed.
For AIMD-managed realtime, prefer `run_tuner_60.sh` with `TO_BLOCK=0`.

---

## `bytecode_api/` — HTTP service: bytecode lookup and clone-search (active)

Go HTTP service that answers two questions given a contract address or deployed bytecode hex:

1. **Get bytecode** for a contract address → `/bytecode?address=0x...`
2. **Find all contracts with identical bytecode** → `/same?address=0x...` or `/same?bytecode=0x...`

Reads from Scylla `eth` keyspace (read-only account) via `contracts_by_addresses` and
`contracts_by_bytecode_hash`. Authentication via `--pass` flag (not visible in `ps aux`).

**Endpoints:**

```
GET  /health                            → "ok"
GET  /bytecode?address=0x{addr}         → {"address":"0x...", "bytecode":"0x60806..."}
GET  /same?address=0x{addr}             → {"bytecode_hash":"0x...", "count":N, "addresses":[...]}
GET  /same?bytecode=0x{hex}             → same (short bytecodes; prefer POST for large)
POST /same  {"address":"0x..."}         → same as GET /same?address=
POST /same  {"bytecode":"0x..."}        → same as GET /same?bytecode= (no URL size limit)
POST /bytecode  {"address":"0x..."}     → same as GET /bytecode?address=
```

**Source:** `tools/bytecode_api/main.go` + `go.mod`/`go.sum`

**Build** (requires Go on the target server — local Go is currently broken):

```bash
# On 100.64.0.4:
export PATH=$HOME/go/bin:$PATH
cd ~/bytecode_api_src
go build -o ~/bytecode_api_v1 .
```

**Deployed:** `100.64.0.4:8080`, binary `~/bytecode_api_v1`, run script `~/run_bytecode_api.sh`

```bash
# Start (tmux session bytecode_api on 100.64.0.4):
tmux new-session -d -s bytecode_api '~/run_bytecode_api.sh'

# Test:
curl http://100.64.0.4:8080/health
curl "http://100.64.0.4:8080/same?address=0x219e497a09202a3534f653e63faaeab6689c1d22"

# Stop:
tmux kill-session -t bytecode_api
```

**Status:** running (2026-06-29). Port 8080 bound to `0.0.0.0`.
Scylla auth: user `reader` (read-only), password in `run_bytecode_api.sh`.

---

## `backfill_spans.sh` — targeted re-sync for known gaps (Polygon)

**Polygon-specific** (chain_id=137, BUCKETS=64, ERA=32000, keyspace=pol). For ETH use
`backfill_spans_eth.sh` above.

```bash
export SCYLLA_DB_PASSWORD='...'
./backfill_spans.sh <node_id> <rpc_url> <spans_file> <log_file> [fetch_workers=8] [span_timeout=3600]
```
