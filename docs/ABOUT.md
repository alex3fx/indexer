# indexer

EVM blockchain indexer written in Zig 0.17. Reads blocks from a JSON-RPC node,
transforms them, and writes to ScyllaDB via native CQL binary protocol (UNLOGGED BATCH).

## Features

- **Historical mode** — parallel fetch+save pipeline (`PIPELINE=N` workers, each with its own CQL connection pool)
- **Realtime mode** — single-block loop with minimum latency per block (`REALTIME=1`)
- **WebSocket mode** — subscribe to `eth_subscribe(newHeads)` for instant block notifications (`WS_URL=...`)
- **Catchup+Realtime** — sync history in batch mode, then transition to WS realtime seamlessly
- **Native CQL** — no driver overhead; direct TCP, UNLOGGED BATCH frames, per-connection prepared statements
- **Shard-aware partitioning** — `REMAP_MOD=N` distributes writes across N Scylla shards (`chunk = block_number % N`)
- **Reserve RPC fallback** — retry failed blocks via a secondary RPC endpoint (`RESERVE_RPC_URL=...`)

## Performance (real server, 48-core Xeon, ScyllaDB smp=32)

| Parser | Config | ms/block | vs TS reference |
|--------|--------|----------|-----------------|
| **indexer** | PIPELINE=2, REMAP_MOD=32, pool=32 | **12.5 ms** | **4.5×** faster |
| TS reference | 5 workers, BullMQ, cassandra-driver | 55.5 ms | 1.0× |

Localhost (WSL2, smp=16, tmpfs): **5.6 ms/block** with PIPELINE=2.

See [SHARD.md](SHARD.md) for full benchmark history.

## Requirements

- Zig 0.17.0-dev.263+0add2dfc4 (or compatible 0.17 dev build)
- ScyllaDB 6.x
- Redis / DragonflyDB (for cursor storage)
- Ethereum JSON-RPC node (supports `eth_getBlockByNumber`, `eth_getBlockReceipts`, `trace_block`)

## Build

```bash
zig build -Doptimize=ReleaseFast
# binary: zig-out/bin/indexer

# Tuning options (compile-time):
zig build -Doptimize=ReleaseFast \
  -Dpool_size=32 \
  -Dsplit="1,3,6,20,1,1"
```

## Environment variables

### Required

| Variable | Example | Description |
|----------|---------|-------------|
| `RPC_URL` | `http://node:8545` | JSON-RPC endpoint |
| `CM_CONNECTION_URL` | `redis://:pass@host:6379/0` | Redis cursor storage |
| `SCYLLA_DB_CONTACT_POINTS` | `["127.0.0.1:9042"]` | ScyllaDB contact points (JSON array) |

### Optional — common

| Variable | Default | Description |
|----------|---------|-------------|
| `CHAIN_ID` | `1` | EVM chain ID |
| `FROM_BLOCK` | `0` | Start block (overridden by Redis cursor if set) |
| `TO_BLOCK` | `25079196` | End block (historical) |
| `BATCH_SIZE` | `10` | Blocks per fetch batch |
| `PIPELINE` | `1` | Parallel fetch+save workers |
| `REMAP_MOD` | `0` | `chunk = block % N`; 0 = use `RAW_CHUNK_SIZE` |
| `RAW_CHUNK_SIZE` | `1000` | Chunk size when REMAP_MOD=0 |
| `SCYLLA_DB_KEYSPACE` | `eth` | ScyllaDB keyspace |
| `SCYLLA_DB_CREDENTIALS` | `{"username":"cassandra","password":"cassandra"}` | ScyllaDB credentials (JSON) |

### Optional — realtime / WS

| Variable | Default | Description |
|----------|---------|-------------|
| `REALTIME` | `0` | `1` = polling realtime mode |
| `POLL_MS` | `500` | Poll interval when no new block (ignored if WS_URL set) |
| `WS_URL` | `` | WebSocket endpoint; enables WS realtime / catchup+realtime |
| `RESERVE_RPC_URL` | `` | Fallback RPC for retrying failed blocks |

### Optional — advanced

| Variable | Default | Description |
|----------|---------|-------------|
| `RPC_FETCH_MODE` | `0` | `0`=flat-parallel, `1`=batch-method, `2`=block-batch |
| `DUMP_FILE` | `` | Write encoded CQL rows to file instead of ScyllaDB |

## Database schema

```bash
cqlsh <host> <port> -f schema.cql
```

See [schema.cql](schema.cql) for the full DDL.
