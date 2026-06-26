# indexer

EVM blockchain indexer written in Zig 0.17-dev. Reads blocks (with transactions, receipts,
traces/internal-txs) from a JSON-RPC node, transforms them, and writes to ScyllaDB over the
native CQL v4 binary protocol (no driver — raw TCP, prepared statements, UNLOGGED BATCH frames).

Currently configured for **Ethereum**, **BSC**, and **Polygon** (chain config in
`src/core/constants/chains/evm/`); adding a new EVM chain means adding one config file there.

## Architecture

- **Historical mode** (`--from=<n> --to=<n>`) — N persistent worker threads pull blocks from a
  shared atomic counter, fetch+parse+transform in parallel, and push results through an MPSC
  pipe to a single accumulator thread. Once `SAVE_EVERY` blocks have accumulated, a background
  thread saves the batch to Scylla (parallel writers per table, see below) while workers keep
  fetching the next batch — fetch and save overlap.
- **Realtime mode** (`--from` only, or cursor resumes past `--to`) — subscribes to
  `eth_subscribe(newHeads)` over WSS and processes one block at a time as new heads arrive.
- **Retry chain** (historical and realtime) — per block: primary RPC → neighbor RPC
  (`NEIGHBOR_RPC_URL`, e.g. the other half of a dual-node split) → backup RPC
  (`RESERVE_RPC_URL`, can be `https://`) → a bounded number of backed-off full cycles of the
  above → an explicit, durable record in `pol.skipped_blocks` (never a silent drop — see
  `docs/INTEGRITY_CHECKS.md`).
- **Watermark** — tracks the true contiguous-from-`from` durably-saved frontier independent of
  save-batch order (workers complete out of order under concurrency). Printed as `[watermark] N`
  in stdout; this is the safe resume point after a crash, not the `Accum X→Y` save-batch line.
- **Cursor** (Redis) — `LATEST_PROCESSED_BLOCK_NUMBER` is the realtime-mode resume point, updated
  after every save. Historical-mode resume should use the log's last `[watermark] N` instead (see
  `docs/HOWTOSTART.md` §4).
- **Chunk/partitioning scheme** — `chunk = (block_number % SCYLLA_CHUNK_BUCKETS) +
  SCYLLA_CHUNK_BUCKETS * (block_number / SCYLLA_CHUNK_ERA)` when both are set — bounds partition
  size by era while spreading writes across `SCYLLA_CHUNK_BUCKETS` lanes for shard parallelism;
  falls back to a flat `block_number % SCYLLA_CHUNK_BUCKETS` if only buckets is set,
  or `block_number / chunkSize` (chain config default) if neither is set.
- **HTTPS fallback** — any `https://` RPC URL is routed through a `curl` subprocess
  (`src/core/common/fetch.zig`) instead of `std.http.Client`, working around a Zig 0.17-dev
  codegen bug that SIGILLs on TLS handshakes under `-Doptimize=ReleaseFast` (see `TODO.md`).
  Irrelevant to the hot path since HTTPS is only ever the backup/public-RPC tier.

## Requirements

- Zig `0.17.0-dev.263+0add2dfc4` (or a compatible 0.17 dev build — this dev snapshot's API has
  moved since; pinned exactly because of that)
- ScyllaDB (CQL v4 compatible)
- Redis or DragonflyDB (cursor storage)
- An EVM JSON-RPC node supporting `eth_getBlockByNumber(_, true)`, `eth_getBlockReceipts`, and
  either `trace_block` (Erigon/RETH/parity-style) or `debug_traceBlockByNumber` with `callTracer`
  (GETH-style) — auto-detected per node via `web3_clientVersion` (cached after first call)

## Build

```bash
zig build -p .zig/build --cache-dir .zig/.cache -Doptimize=ReleaseFast
# binary -> .zig/build/bin/raw
```

**Always build `-Doptimize=ReleaseFast`.** `Debug` hangs the pool workers on this Zig dev
snapshot (known toolchain quirk on this version, not a code bug).

## Documentation map

- `docs/HOWTOSTART.md` — environment variables, CLI flags, build/run/recovery procedures
- `docs/INTEGRITY_CHECKS.md` — data-integrity methodology: `pol.skipped_blocks`,
  `find_missing_blocks.py`, watermark semantics
- `docs/SCYLLA_READONLY_ACCESS.md` — granting read-only Scylla access for analysts
- `tools/README.md` — supporting scripts (load-aware launcher, monitor, integrity scanner,
  targeted backfill)
- `TODO.md` — known upstream Zig bug and its workaround
- `docs/DIFF.md`, `docs/SHARD.md` — point-in-time changelog/benchmark snapshots, not living docs
