#!/usr/bin/env bash
# Historical benchmark: 2 000 blocks starting at 25 000 000.
# Quick check of throughput at the current chain head zone.
# Outputs: ms total, ms/block, blk/s, per-stage timing from logs.
#
# Usage: bench_hist_25m.sh [binary] [label]
set -euo pipefail

BINARY="${1:-$HOME/raw_unified}"
LABEL="${2:-$(date +%Y%m%d_%H%M%S)}"
REDIS_URL="redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0"

FROM=25000000
BLOCKS=2000
TO=$((FROM + BLOCKS - 1))

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$HOME/bench_hist_25m_${LABEL}_${STAMP}.log"

truncate_tables() {
    docker exec scylla cqlsh -u cassandra -p cassandra \
        -e "USE eth;
            TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
            TRUNCATE internal_transactions; TRUNCATE contracts;
            TRUNCATE contracts_by_addresses; TRUNCATE block_completions;" \
        >/dev/null 2>&1
    sleep 2
}

set_cursor() {
    redis-cli -u "$REDIS_URL" SET LATEST_PROCESSED_BLOCK_NUMBER $((FROM - 1)) >/dev/null 2>&1
}

{
    echo "bench_hist_25m  binary=$BINARY  label=$LABEL"
    echo "range=${FROM}..${TO}  blocks=${BLOCKS}"
    echo ""
} | tee "$LOG"

truncate_tables
set_cursor

t0=$(date +%s%3N)
timeout 1800s env \
    MODE=local EVM_CHAIN_ID=1 \
    CM_CONNECTION_URL="$REDIS_URL" \
    SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 \
    SCYLLA_DB_KEYSPACE=eth \
    SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD=cassandra \
    SAVE_EVERY=24 SCYLLA_CHUNK_BUCKETS=24 \
    "$BINARY" --from="$FROM" --to="$TO" 2>&1 | tee -a "$LOG"
rc=$?
t1=$(date +%s%3N)
ms=$((t1 - t0))

# Parse results from log
python3 - "$LOG" "$BLOCKS" "$ms" <<'PY'
import re, sys, statistics

log_path = sys.argv[1]
blocks   = int(sys.argv[2])
total_ms = int(sys.argv[3])

lines = open(log_path).readlines()

# Parse "Accum X→X: saved=X save=Xms | X blk/s avg"
accum_blk_s = []
for ln in lines:
    m = re.search(r'([\d.]+) blk/s avg', ln)
    if m:
        accum_blk_s.append(float(m.group(1)))

# Parse "Historical sync done: Xms  saved_blocks=X"
done_ms = None; done_blocks = None
for ln in lines:
    m = re.search(r'Historical sync done: ([\d.]+)ms\s+saved_blocks=(\d+)', ln)
    if m:
        done_ms = float(m.group(1)); done_blocks = int(m.group(2))

blk_s_wall = blocks * 1000 / total_ms if total_ms > 0 else 0
ms_per_blk = total_ms / blocks if blocks > 0 else 0

print()
print(f"=== bench_hist_25m RESULT ===")
print(f"blocks={blocks}  from={sys.argv[1].split('_')[-2] if '_' in sys.argv[1] else '?'}")
print(f"wall_ms={total_ms}  ms/blk={ms_per_blk:.1f}  blk/s={blk_s_wall:.2f}")
if done_ms and done_blocks:
    print(f"binary_ms={done_ms:.0f}  saved_blocks={done_blocks}  blk/s_binary={done_blocks*1000/done_ms:.2f}")
if accum_blk_s:
    print(f"accum_blk_s: min={min(accum_blk_s):.1f} max={max(accum_blk_s):.1f} avg={statistics.mean(accum_blk_s):.1f}")

# Node sync estimate at this zone speed
if blk_s_wall > 0:
    est_25m_h = 25_000_000 / blk_s_wall / 3600
    print(f"est_25M_at_this_speed={est_25m_h:.1f}h  ({est_25m_h/24:.1f}d)")
PY

echo ""
echo "log=$LOG"
