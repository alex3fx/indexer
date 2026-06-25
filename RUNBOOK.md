# Polygon/EVM Indexer — Runbook

Operational guide for building, deploying, and running the dual-node historical
indexer plus its supporting tools (load-aware tuner, monitor, integrity checker,
backfill). Written for review/handoff — contains no credentials or environment-specific
secrets; every credential is read from the environment at runtime (see "Required secrets"
below).

## 1. Build

Toolchain: Zig `0.17.0-dev` (this exact dev snapshot — API has moved since).

```bash
zig build -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast
# binary -> .zig/build/bin/raw
```

**Always build `-Doptimize=ReleaseFast`.** `Debug` hangs the pool workers on this
Zig version (known toolchain quirk, not a code bug). `ReleaseSafe` works but is
~3-5x slower than `ReleaseFast` for this workload.

## 2. Environment variables

### Required (binary refuses to start without these — no hardcoded defaults)

| Variable | Meaning |
|---|---|
| `MODE` | `production` \| `development` \| `local` |
| `EVM_CHAIN_ID` | e.g. `137` for Polygon |
| `CM_CONNECTION_URL` | Redis connection string, `redis://[:password@]host:port/db` |
| `SCYLLA_DB_HOST` | Scylla/Cassandra contact point |
| `SCYLLA_DB_PORT` | usually `9042` |
| `SCYLLA_DB_USERNAME` | Scylla auth username |
| `SCYLLA_DB_PASSWORD` | Scylla auth password — **never commit this, never hardcode a default** |

### Optional (sensible defaults baked into the binary)

| Variable | Default | Meaning |
|---|---|---|
| `SCYLLA_DB_KEYSPACE` | `eth` | target keyspace |
| `SCYLLA_DB_LOCAL_DATACENTER` | `datacenter1` | for DC-aware load balancing |
| `SCYLLA_CHUNK_BUCKETS` | (none — old scheme) | lane count for v3 chunk scheme, e.g. `64` |
| `SCYLLA_CHUNK_ERA` | (none) | block-era size for v3 chunk scheme, e.g. `32000` |
| `LOGS_GRAYLOG_HOST` / `_PORT` / `_APP` | `127.0.0.1` / `12201` / `indexer` | GELF/TCP log sink |
| `TIME_ZONE` | `0` | integer hour offset for local-mode timestamps |
| `RPC_URL` / `RPC_WSS` | (chain config default) | override the primary RPC node |
| `NEIGHBOR_RPC_URL` | (unset → disabled) | second retry tier (see §4) |
| `RESERVE_RPC_URL` / `BACKUP_RPC_HTTPS` | (unset → disabled) | third retry tier, supports `https://` |
| `FETCH_WORKERS` | chain config default | historical-sync fetch concurrency |
| `SAVE_EVERY` | chain config default | blocks accumulated per Scylla save batch |
| `ACCUM_TXS_LANES` / `ACCUM_LOG_LANES` / `ACCUM_ITX_LANES` | chain config default | parallel Scylla write lanes per entity type |

### Required secrets (get these from your secrets manager / team lead — never in code or chat history committed to a repo)

- Scylla password (`SCYLLA_DB_PASSWORD`)
- Grafana password, if using the load-aware tuner's Prometheus signal (`GRAFANA_PASS`, see §5)
- GrayLog REST API password, if using the GrayLog query examples in this doc (separate from the
  GELF ingestion port, which on most setups has no auth)

## 3. CLI flags

```
raw --from=<block> --to=<block>
```
Historical sync range. Omitting `--to` switches to realtime mode (follows chain head via WSS)
once `--from` catches up.

## 4. Retry chain (historical sync)

Per block: **primary** RPC → **neighbor** RPC (`NEIGHBOR_RPC_URL`, e.g. the other half of a
dual-node split) → **backup** RPC (`RESERVE_RPC_URL`/`BACKUP_RPC_HTTPS`, can be `https://`,
routed through a `curl` subprocess to avoid a Zig stdlib TLS bug — see `TODO.md`) → after
`MAX_GIVE_UP_CYCLES` (5) full cycles, explicitly recorded as skipped in `pol.skipped_blocks`
(never silently dropped). The give-up counter is Redis-backed (`giveup:{blockNum}` key in the
configured Redis DB) so it survives process restarts.

A **watermark** mechanism tracks the true contiguous-safe-to-resume-from frontier (printed as
`[watermark] N` in stdout), independent of out-of-order worker completion — always resume from
the last `[watermark] N`, never from the `Accum X→Y` save-batch log line (which can be ahead of
an in-flight block).

## 5. Supporting tools (`tools/` in the ops repo)

All of these read credentials from the environment — set `SCYLLA_DB_PASSWORD` (and
`GRAFANA_USER`/`GRAFANA_PASS` if applicable) before running any of them.

### `dynamic_tuner.py` — launcher + load-aware concurrency supervisor

Replaces a static bash restart-loop. Bootstrap-probes a few `FETCH_WORKERS` values, then runs a
continuous AIMD control loop driven by the RPC node's `node_load1` (Grafana/Prometheus) — raises
concurrency when load is low, backs off when high. Also restarts on crash with exponential backoff,
and resumes from the log's last `[watermark] N` line automatically.

```bash
export SCYLLA_DB_PASSWORD='...'
export GRAFANA_USER='...' GRAFANA_PASS='...'   # optional — degrades to error-only tuning if unset
python3 dynamic_tuner.py <node_id: 62|63> <rpc_url> <from_block_if_no_log_yet> <to_block>
```

Per-node topology (RPC URLs, Prometheus instance labels, worker bounds) is configured near the
top of the script — either edit `_NEIGHBOR_DEFAULTS`/`_NODE_INFO`/`_WORKER_BOUNDS` for your
topology, or override via env (`NEIGHBOR_RPC_URL`, `NODE_62_INSTANCE`, `NODE_63_INSTANCE`, etc.).

**Gotcha**: the tuner's `resume_from()` prioritizes the last `[watermark] N` line in the log file
over the `from_block` CLI argument. To force a different resume point, append a line
`[watermark] <N>  # reason` to the end of the log file before restarting.

### `monitor_pol_v3.py` — heartbeat/alert monitor

Tails both node logs, sends GELF heartbeats (block height, blk/s, ETA, alive status) to GrayLog
every 5 cycles (~5 min), and fires immediate alerts on `[ALERT]`/`[WARNING]` lines. Seeds its file
read-position at current EOF on startup so a monitor restart never re-sends old alerts as new.

```bash
export RUN_DIR=/path/to/full_index_run     # optional, defaults shown in script
export NODE_62_FROM=... NODE_62_TO=... NODE_63_FROM=... NODE_63_TO=...  # optional, edit topology
python3 monitor_pol_v3.py
```

### `find_missing_blocks.py` — integrity scanner

Scans every `(lane, era)` chunk partition in a block range and reports exact missing block
numbers (arithmetic-sequence check within each lane — not `ALLOW FILTERING`, no birthday-paradox
under-sampling). Output is one block number per line on stdout — pipe straight into a backfill
span list.

```bash
export SCYLLA_DB_PASSWORD='...'
python3 find_missing_blocks.py FROM_BLOCK TO_BLOCK [LANES=64] [ERA=32000] [TABLE=blocks] [CONCURRENCY=64]
```

Run this after any historical sync range claims to be complete, and after any manual
backfill/recovery, before trusting the data as gap-free.

### `backfill_spans.sh` — targeted re-sync for known gaps

Re-runs the indexer binary across each `FROM TO` span from a spans file (group `find_missing_blocks.py`
output into contiguous ranges first). Idempotent (primary key is `chunk + block_number`) — safe to
re-cover already-good blocks at span edges.

```bash
export SCYLLA_DB_PASSWORD='...'
./backfill_spans.sh <node: 62|63> <rpc_url> <spans_file> <log_file> [fetch_workers=8] [span_timeout=3600]
```

**Don't run backfill concurrently with live indexing at high `FETCH_WORKERS` on the same RPC
node** — contention degrades both. Keep backfill `FETCH_WORKERS` low (5-10) and watch live
throughput doesn't regress.

## 6. Recovering a single explicitly-skipped block

Blocks that exhaust the full retry chain are recorded in `pol.skipped_blocks` (`resolved=false`),
never silently dropped. To manually recover one (e.g. against a different public RPC that
happens to serve it):

```bash
export SCYLLA_DB_PASSWORD='...'
MODE=production EVM_CHAIN_ID=<chain> \
CM_CONNECTION_URL='redis://127.0.0.1:6379/<throwaway_db>' \
SCYLLA_DB_HOST=... SCYLLA_DB_PORT=9042 SCYLLA_DB_KEYSPACE=... \
SCYLLA_DB_USERNAME=... SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
SCYLLA_CHUNK_BUCKETS=64 SCYLLA_CHUNK_ERA=32000 \
RPC_URL=<primary> RESERVE_RPC_URL=<alternate public RPC> \
FETCH_WORKERS=1 \
./raw --from=<block> --to=<block>
```

Use a **throwaway Redis DB number** distinct from the live nodes' DBs so this doesn't touch the
real checkpoint/give-up state. After confirming the block is saved (log line
`[<block>] T:.. L:.. IT:.. (backup)` plus a direct `SELECT` against `pol.blocks`), mark it resolved:

```sql
UPDATE pol.skipped_blocks SET resolved=true WHERE block_number=<block>;
```

## 7. Database access (read-only, for analysts)

The Scylla CQL port (`9042`) must be bound to a reachable interface (`--rpc-address 0.0.0.0` +
`--broadcast-rpc-address <reachable IP>` in the Scylla startup flags) for non-localhost clients —
`listen_address`/`rpc_address: localhost` in `scylla.yaml` is the Scylla *default* and will refuse
external connections. **Note**: if Scylla's container/process was started with explicit
`--rpc-address`/`--broadcast-rpc-address` flags, those override `scylla.yaml` — editing the yaml
alone and restarting will not take effect; the process needs those flags changed at its actual
startup command.

A read-only role (`SELECT`-only, no `MODIFY`/DDL) should be created for external/analyst access
rather than sharing the superuser credential:

```sql
CREATE ROLE reader WITH PASSWORD = '<generate-a-strong-password>' AND LOGIN = true;
GRANT SELECT ON KEYSPACE <keyspace> TO reader;
```

Verify the grant is actually read-only before handing out the credential:
```bash
cqlsh <host> 9042 -u reader -p '<password>' -k <keyspace> -e "INSERT INTO skipped_blocks (...) VALUES (...);"
# must fail with: Unauthorized: User reader has no MODIFY permission on ...
```

## 8. Known limitations (see `TODO.md` for the full writeup)

- A Zig 0.17-dev `std.crypto.ml_kem` codegen bug SIGILLs any direct `std.http.Client` HTTPS call
  under `-Doptimize=ReleaseFast`. Worked around by routing all `https://` RPC traffic through a
  `curl` subprocess (`src/core/common/fetch.zig`) instead of fixing the compiler bug itself.
- High write concurrency on the indexer's `FETCH_WORKERS` can shift the bottleneck from the RPC
  node to the database's own write-side capacity (CPU/disk on the Scylla host) when running
  multiple indexer instances against one Scylla cluster — the load-aware tuner only watches the
  RPC node's metrics, not the database host's, so it won't automatically detect or back off from
  this. If raising `FETCH_WORKERS` stops increasing throughput despite low RPC-node load, check
  the database host's own CPU/disk load before assuming the RPC node is the limit.
