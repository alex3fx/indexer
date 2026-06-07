#!/usr/bin/env bash
# Historical zone benchmark: 2 000-block runs at fixed points across 0..25M.
# Each run long enough to reach pipeline steady-state (64 workers all busy).
# Measures blk/s from Accum log lines — excludes startup overhead.
#
# Usage: bench_hist_zones.sh [binary] [label]
set -euo pipefail

BINARY="${1:-$HOME/raw_unified}"
LABEL="${2:-$(date +%Y%m%d_%H%M%S)}"
REDIS_URL="redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0"

BLOCKS=2000   # enough to reach steady-state with 64 workers

# Representative probe point at the end of each zone (last N blocks before milestone).
PROBES=(
    500000       # 0-1M: early era, no txs
    1500000      # 1-4M: frontier era
    3000000      # 1-4M: DAO area
    5000000      # 4-8M: mid-growth
    7000000      # 4-8M: late growth
    10000000     # 8-12M: ICO era
    14000000     # 12-16M
    18000000     # 16-20M
    20500000     # 20-23M
    22000000     # 20-23M
    23500000     # 23-25M
    24500000     # 23-25M: near head
    25000000     # 25M zone
)

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$HOME/bench_hist_zones_${LABEL}_${STAMP}.log"

truncate_tables() {
    docker exec scylla cqlsh -u cassandra -p cassandra \
        -e "USE eth;
            TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
            TRUNCATE internal_transactions; TRUNCATE contracts;
            TRUNCATE contracts_by_addresses; TRUNCATE block_completions;" \
        >/dev/null 2>&1
    sleep 2
}

run_probe() {
    local from=$1
    local to=$((from + BLOCKS - 1))
    redis-cli -u "$REDIS_URL" SET LATEST_PROCESSED_BLOCK_NUMBER $((from - 1)) >/dev/null 2>&1

    local out
    out=$(timeout 1800s env \
        MODE=local EVM_CHAIN_ID=1 \
        CM_CONNECTION_URL="$REDIS_URL" \
        SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 \
        SCYLLA_DB_KEYSPACE=eth \
        SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD=cassandra \
        SAVE_EVERY=24 SCYLLA_CHUNK_BUCKETS=24 \
        "$BINARY" --from="$from" --to="$to" 2>&1 || true)

    # Last Accum blk/s = steady-state throughput
    local last_blk_s done_blk_s wall_blk_s
    last_blk_s=$(echo "$out" | grep -oP '\K[\d.]+(?= blk/s avg)' | tail -1 || echo 0)
    done_blk_s=$(echo "$out" | grep -oP '(?<=saved_blocks=)\d+' | tail -1 || echo 0)
    local done_ms
    done_ms=$(echo "$out" | grep -oP '(?<=sync done: )[\d.]+(?=ms)' | tail -1 || echo 0)
    if [ "${done_ms:-0}" -gt 0 ] 2>/dev/null; then
        wall_blk_s=$(python3 -c "print(f'{$BLOCKS*1000/$done_ms:.1f}')" 2>/dev/null || echo 0)
    else
        wall_blk_s=0
    fi

    echo "$from,$to,$last_blk_s,$wall_blk_s,$done_ms" | tee -a "$LOG"
}

{
    echo "bench_hist_zones  binary=$BINARY  label=$LABEL"
    echo "blocks_per_probe=$BLOCKS"
    echo ""
    printf '%-12s %-12s %12s %12s %10s\n' from to last_blk_s wall_blk_s done_ms
    printf '%s\n' '-------------+------------+--------------+--------------+-----------'
} | tee "$LOG"

truncate_tables

for from in "${PROBES[@]}"; do
    run_probe "$from"
done

# Summary
python3 - "$LOG" <<'PY'
import sys, statistics

rows = []
for ln in open(sys.argv[1]):
    parts = ln.strip().split(',')
    if len(parts) == 5:
        try:
            from_, to_, last_s, wall_s, ms = parts
            rows.append(dict(
                from_=int(from_), to=int(to_),
                last_blk_s=float(last_s),
                wall_blk_s=float(wall_s),
                ms=float(ms),
            ))
        except ValueError:
            pass

if not rows:
    print("No data parsed.")
    sys.exit(0)

interval = 25_000_000.0
# Use last_blk_s (steady-state) for estimate. Weight zones by sample count.
# Simple average per zone.
ZONES = [
    (0,         2_000_000,  "0-2M  "),
    (2_000_000,  6_000_000, "2-6M  "),
    (6_000_000, 12_000_000, "6-12M "),
    (12_000_000,18_000_000, "12-18M"),
    (18_000_000,23_000_000, "18-23M"),
    (23_000_000,25_200_000, "23-25M"),
]
print("\nzone    last_blk_s (steady)  wall_blk_s")
total_s = 0.0
for lo, hi, name in ZONES:
    xs_last = [r["last_blk_s"] for r in rows if lo <= r["from_"] < hi and r["last_blk_s"] > 0]
    xs_wall = [r["wall_blk_s"] for r in rows if lo <= r["from_"] < hi and r["wall_blk_s"] > 0]
    if not xs_last:
        continue
    zone_blocks = hi - lo
    est_s = zone_blocks / statistics.mean(xs_last)
    total_s += est_s
    wstr = f"{statistics.mean(xs_wall):.1f}" if xs_wall else "?"
    print(f"{name}  {statistics.mean(xs_last):8.1f}  {wstr:>12}    est={est_s/3600:.2f}h")

print(f"\ntotal_est_25M={total_s/3600:.1f}h  ({total_s/86400:.1f}d)")
PY
