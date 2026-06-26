# HOWTOSTART — build, configure, run, recover

See `docs/ABOUT.md` for the architecture overview this guide assumes.

## 1. Build

```bash
zig build -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast
# binary -> .zig/build/bin/raw
```

Always `-Doptimize=ReleaseFast` — `Debug` hangs the pool workers on this Zig dev snapshot.
`zig build test --cache-dir .zig/.cache -Doptimize=ReleaseFast` runs the unit tests (logger
date/GELF formatting, etc.).

## 2. Environment variables

### Required — binary refuses to start without these, no hardcoded defaults

| Variable | Meaning |
|---|---|
| `MODE` | `production` \| `development` \| `local` |
| `EVM_CHAIN_ID` | e.g. `137` (Polygon), `1` (Ethereum), `56` (BSC) — must be a chain configured in `src/core/constants/chains/evm/` |
| `CM_CONNECTION_URL` | Redis connection string: `redis://[:password@]host:port/db` |
| `SCYLLA_DB_HOST`, `SCYLLA_DB_PORT` | Scylla contact point, e.g. `127.0.0.1` / `9042` |
| `SCYLLA_DB_USERNAME`, `SCYLLA_DB_PASSWORD` | Scylla auth — never commit these, never hardcode a default |

### Optional — sensible defaults baked into the binary

| Variable | Default | Meaning |
|---|---|---|
| `SCYLLA_DB_KEYSPACE` | `eth` | target keyspace |
| `SCYLLA_DB_LOCAL_DATACENTER` | `datacenter1` | DC-aware load balancing |
| `LOGS_GRAYLOG_HOST` / `_PORT` / `_APP` | `127.0.0.1` / `12201` / `indexer` | GELF/TCP log sink |
| `TIME_ZONE` | `0` | integer hour offset from UTC, used only for local-mode stdout timestamps |
| `RPC_URL` / `RPC_WSS` | chain config default | override the primary RPC node (needed to run independent processes against different physical nodes, e.g. a dual-node split) |
| `NEIGHBOR_RPC_URL` | unset → disabled | second retry tier, historical sync only |
| `RESERVE_RPC_URL` / `BACKUP_RPC_HTTPS` | unset → disabled | third retry tier; same slot, `RESERVE_RPC_URL` is canonical |
| `SCYLLA_CHUNK_BUCKETS` | chain config default (flat scheme) | lane count for the chunk partition formula, e.g. `64` |
| `SCYLLA_CHUNK_ERA` | `0` (flat scheme) | block-era size for the chunk partition formula, e.g. `32000` |
| `FETCH_WORKERS` | chain config default | historical-sync fetch concurrency |
| `SAVE_EVERY` | chain config default | blocks accumulated per Scylla save batch |
| `ACCUM_TXS_LANES` / `ACCUM_LOG_LANES` / `ACCUM_ITX_LANES` | chain config default | parallel Scylla write connections per entity type |
| `WS_DELAY_MS` | `100` | realtime: delay after a new head notification before fetching (lets the RPC node catch up) |
| `RT_RETRY_DELAY_MS` | `2000` | realtime: delay between retries when a block is unavailable on all nodes |

## 3. CLI flags

```
raw --from=<block> --to=<block>
```

Historical sync over `[from, to]`. Omitting `--to` switches to realtime mode (follows chain head
via WSS) once `--from` catches up to the head. If `--from` is omitted, it resumes from the Redis
cursor (`LATEST_PROCESSED_BLOCK_NUMBER + 1`), or `0` if no cursor exists yet.

## 4. Historical sync

```bash
MODE=production EVM_CHAIN_ID=137 \
CM_CONNECTION_URL='redis://127.0.0.1:6379/0' \
SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 SCYLLA_DB_KEYSPACE=pol \
SCYLLA_DB_USERNAME="$SCYLLA_DB_USERNAME" SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
SCYLLA_CHUNK_BUCKETS=64 SCYLLA_CHUNK_ERA=32000 \
FETCH_WORKERS=64 ACCUM_TXS_LANES=3 ACCUM_LOG_LANES=6 ACCUM_ITX_LANES=20 \
RPC_URL=http://<node>:8545 RESERVE_RPC_URL=https://<public-fallback> \
./raw --from=<block> --to=<block>
```

If interrupted (crash, restart, intentional stop), resume from the **last `[watermark] N` line**
in stdout/the log file — not the last `Accum X→Y` line. Workers complete out of order under
concurrency, so a save batch can be printed with a higher block number than one that's still
in-flight; the watermark is the actual contiguous-safe frontier (see `docs/ABOUT.md`). Resume
with `--from=<watermark+1>`.

A clean historical run ends with an integrity self-check (`[watermark] integrity check passed:
contiguous through <to>` on success, or `[INTEGRITY ERROR] ...` + a non-zero exit if the
watermark didn't reach `to+1` — this should be impossible after a clean run and means
investigate before resuming).

## 5. Realtime mode

Omit `--to` (or let `--from` catch up to a previous `--to`):

```bash
MODE=production EVM_CHAIN_ID=137 \
CM_CONNECTION_URL='redis://127.0.0.1:6379/0' \
SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 SCYLLA_DB_KEYSPACE=pol \
SCYLLA_DB_USERNAME="$SCYLLA_DB_USERNAME" SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
SCYLLA_CHUNK_BUCKETS=64 SCYLLA_CHUNK_ERA=32000 \
RPC_URL=http://<node>:8545 RPC_WSS=ws://<node>:8546 \
RESERVE_RPC_URL=https://<public-fallback> \
./raw --from=<block>
```

Connects via WSS, subscribes to `newHeads`, fills any gap between the cursor and the current
head, then processes new blocks as they arrive. A transient WSS disconnect reconnects with
capped exponential backoff (1s..30s) — it does not give up and exit.

## 6. Supporting tools (`tools/`)

All read credentials from the environment (`SCYLLA_DB_PASSWORD` required, `GRAFANA_USER`/
`GRAFANA_PASS` optional) — see `tools/README.md` for usage of each:

- `dynamic_tuner.py` — launcher + load-aware `FETCH_WORKERS` supervisor (AIMD control loop driven
  by RPC-node `node_load1`), restarts on crash, resumes from `[watermark] N` automatically.
- `monitor_pol_v3.py` — tails node logs, sends GELF heartbeats/alerts to GrayLog.
- `find_missing_blocks.py` — exact-gap integrity scanner (see `docs/INTEGRITY_CHECKS.md`).
- `backfill_spans.sh` — targeted re-sync for spans of known-missing blocks.

## 7. Recovering a single explicitly-skipped block

Blocks that exhaust the full retry chain are recorded in `pol.skipped_blocks`
(`resolved=false`), never silently dropped. To manually recover one (e.g. against a different
public RPC that happens to serve it):

```bash
SCYLLA_DB_PASSWORD="$SCYLLA_DB_PASSWORD" \
MODE=production EVM_CHAIN_ID=<chain> \
CM_CONNECTION_URL='redis://127.0.0.1:6379/<throwaway_db>' \
SCYLLA_DB_HOST=... SCYLLA_DB_PORT=9042 SCYLLA_DB_KEYSPACE=... \
SCYLLA_DB_USERNAME=... \
SCYLLA_CHUNK_BUCKETS=64 SCYLLA_CHUNK_ERA=32000 \
RPC_URL=<primary> RESERVE_RPC_URL=<alternate public RPC> \
FETCH_WORKERS=1 \
./raw --from=<block> --to=<block>
```

Use a **throwaway Redis DB number**, distinct from the live process's DB, so this doesn't touch
the real cursor/give-up-cycle state. After confirming the block is saved (log line `[<block>]
T:.. L:.. IT:.. (backup)` plus a direct `SELECT` against the `blocks` table), mark it resolved:

```sql
UPDATE pol.skipped_blocks SET resolved=true WHERE block_number=<block>;
```

## 8. Database access (read-only, for analysts)

The Scylla CQL port (`9042`) must be bound to a reachable interface
(`--rpc-address 0.0.0.0` + `--broadcast-rpc-address <reachable IP>` in Scylla's startup flags) for
non-localhost clients — `rpc_address: localhost` in `scylla.yaml` is the Scylla default and
refuses external connections. If Scylla was started with explicit `--rpc-address`/
`--broadcast-rpc-address` flags, those override `scylla.yaml`; editing the yaml alone and
restarting will not take effect.

Create a `SELECT`-only role rather than sharing the superuser credential — see
`docs/SCYLLA_READONLY_ACCESS.md` for the exact grant and how to verify it's actually read-only.

## 9. Known limitations

See `TODO.md` for the full writeup. Summary:

- A Zig 0.17-dev `std.crypto.ml_kem` codegen bug SIGILLs any direct `std.http.Client` HTTPS call
  under `-Doptimize=ReleaseFast`. Worked around by routing `https://` RPC traffic through a
  `curl` subprocess instead of fixing the compiler bug itself.
- Running multiple indexer instances against one Scylla cluster can shift the bottleneck from
  RPC-node capacity to the database's own write-side capacity (CPU/disk on the Scylla host). The
  load-aware tuner only watches RPC-node metrics, not the database host's, so it won't detect or
  back off from this automatically — if raising `FETCH_WORKERS` stops increasing throughput
  despite low RPC-node load, check the database host's own CPU/disk load before assuming the RPC
  node is the limit.
