# HOWTOSTART — Quick start guide

## 1. Prerequisites

### Zig compiler

```bash
# Download Zig 0.17.0-dev.263+0add2dfc4 for linux-x86_64
wget https://ziglang.org/builds/zig-linux-x86_64-0.17.0-dev.263+0add2dfc4.tar.xz
tar xf zig-linux-x86_64-0.17.0-dev.263+0add2dfc4.tar.xz
export ZIG=$PWD/zig-linux-x86_64-0.17.0-dev.263+0add2dfc4/zig
$ZIG version   # should print: 0.17.0-dev.263+0add2dfc4
```

### ScyllaDB

Install ScyllaDB 6.x. Recommended config (`/etc/default/scylla-server`):

```
SCYLLA_ARGS="--smp=16 --memory=10G --unsafe-bypass-fsync=1 --overprovisioned \
  --max-concurrent-requests-per-shard=65536"
```

Set `smp` to the number of CPU cores you want Scylla to use.
`REMAP_MOD` should equal `smp` for optimal shard distribution.

Create the schema:

```bash
cqlsh <scylla_host> <scylla_port> -f schema.cql
# verify:
cqlsh <scylla_host> <scylla_port> -e "USE eth; DESCRIBE TABLES;"
```

### Redis / DragonflyDB

The indexer uses Redis to store the cursor (`LATEST_PROCESSED_BLOCK_NUMBER`).

```bash
# DragonflyDB via Docker (host network for direct 127.0.0.1 access):
docker run --rm -d --name dragonfly --network host \
  docker.dragonflydb.io/dragonflydb/dragonfly \
  --requirepass yourpassword
```

## 2. Build

```bash
cd indexer/
$ZIG build -Doptimize=ReleaseFast
# binary at: ./zig-out/bin/indexer
```

For a server with smp=32 and 32-connection pool (compile-time tuning):

```bash
$ZIG build -Doptimize=ReleaseFast -Dpool_size=32 -Dsplit="1,3,6,20,1,1"
```

## 3. Historical sync

Two chunking strategies are available:

**Option A — shard-aware (`REMAP_MOD=N`, recommended)**

`chunk = block_number % N` — distributes writes evenly across N Scylla shards.
Set `N` equal to the Scylla `smp` value for optimal load distribution.

```bash
CM_CONNECTION_URL="redis://:yourpassword@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["<scylla_host>:<scylla_port>"]' \
SCYLLA_DB_KEYSPACE=eth \
SCYLLA_DB_CREDENTIALS='{"username":"cassandra","password":"cassandra"}' \
RPC_URL=http://<node_host>:8545 \
CHAIN_ID=1 \
FROM_BLOCK=0 \
TO_BLOCK=21000000 \
BATCH_SIZE=10 \
PIPELINE=2 \
REMAP_MOD=32 \
./zig-out/bin/indexer
```

**Option B — classic chunking (no `REMAP_MOD`)**

`chunk = block_number / RAW_CHUNK_SIZE` — groups consecutive blocks into fixed-size
partitions (default 1000 blocks per chunk: block 0–999 → chunk 0, block 1000–1999 → chunk 1, etc.).
Use this when you want predictable partition boundaries and don't need shard distribution.

```bash
CM_CONNECTION_URL="redis://:yourpassword@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["<scylla_host>:<scylla_port>"]' \
SCYLLA_DB_KEYSPACE=eth \
SCYLLA_DB_CREDENTIALS='{"username":"cassandra","password":"cassandra"}' \
RPC_URL=http://<node_host>:8545 \
CHAIN_ID=1 \
FROM_BLOCK=0 \
TO_BLOCK=21000000 \
BATCH_SIZE=10 \
PIPELINE=2 \
RAW_CHUNK_SIZE=1000 \
./zig-out/bin/indexer
```

`RAW_CHUNK_SIZE=1000` is the default, so you can omit it entirely.
Do **not** set `REMAP_MOD` when using classic chunking.

**Key tuning parameters:**

| Parameter | Recommendation |
|-----------|----------------|
| `PIPELINE` | 2 on same machine; 4+ with remote node (high RTT) |
| `REMAP_MOD` | Equal to Scylla `smp` (e.g. 16 or 32); omit for classic chunking |
| `RAW_CHUNK_SIZE` | 1000 (default); only used when `REMAP_MOD` is not set |
| `BATCH_SIZE` | 10 is safe; reduce to 5 if you see "Batch too large" errors |
| `pool_size` (build flag) | Equal to `smp` |

The indexer stores progress in Redis. If interrupted, restart with the same command —
it resumes from `LATEST_PROCESSED_BLOCK_NUMBER + 1` automatically.

## 4. Realtime mode (polling)

After history is synced, switch to realtime:

```bash
CM_CONNECTION_URL="redis://:yourpassword@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["<scylla_host>:<scylla_port>"]' \
SCYLLA_DB_KEYSPACE=eth \
RPC_URL=http://<node_host>:8545 \
CHAIN_ID=1 \
REALTIME=1 \
POLL_MS=500 \
REMAP_MOD=16 \
./zig-out/bin/indexer
```

## 5. Realtime mode (WebSocket — recommended)

Subscribe to `newHeads` for instant block notifications:

```bash
CM_CONNECTION_URL="redis://:yourpassword@127.0.0.1:6379/0" \
SCYLLA_DB_CONTACT_POINTS='["<scylla_host>:<scylla_port>"]' \
SCYLLA_DB_KEYSPACE=eth \
RPC_URL=http://<node_host>:8545 \
WS_URL=ws://<node_host>:8546/ws \
CHAIN_ID=1 \
REMAP_MOD=16 \
./zig-out/bin/indexer
```

With `WS_URL` set, the indexer automatically:
1. Subscribes to `newHeads` to buffer incoming blocks
2. Syncs history from cursor to the first WS-notified block
3. Switches to realtime processing of WS notifications

## 6. Reserve RPC fallback

If a block fetch fails, retry via a secondary endpoint:

```bash
RPC_URL=http://primary:8545 \
RESERVE_RPC_URL=http://backup:8545 \
...
```

## 7. Verify data

```bash
cqlsh <scylla_host> <scylla_port> << 'EOF'
USE eth;
SELECT count(*) FROM blocks WHERE chunk = 25079;
SELECT count(*) FROM transactions WHERE chunk = 25079;
SELECT count(*) FROM internal_transactions WHERE chunk = 25079;
EOF
```

## 8. Results

After each run, a JSON metrics file is written to `./zig-out/bin/results/indexer_<timestamp>.json`
with per-block fetch/transform/save timings.
