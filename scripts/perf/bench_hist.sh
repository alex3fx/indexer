#!/usr/bin/env bash
# Historical benchmark: 100-block samples every 100 000 blocks, 0..25.1M (252 points).
# Measures: ms/sample, blk/s per zone, estimates total node sync time.
#
# Usage: bench_hist.sh [binary] [label]
#   binary  path to devindexer binary  (default: ~/raw_unified)
#   label   tag in output filenames    (default: date stamp)
#
# Requires: truncate access to Scylla via docker exec, redis-cli on PATH.
# Run on the indexer server (100.64.0.4).
set -euo pipefail

BINARY="${1:-$HOME/raw_unified}"
LABEL="${2:-$(date +%Y%m%d_%H%M%S)}"
REDIS_URL="redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0"

SAMPLE=100        # blocks per sample
STEP=100000       # gap between sample start points
MAX_START=25100000

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$HOME/bench_hist_${LABEL}_${STAMP}.log"
CSV="$HOME/bench_hist_${LABEL}_${STAMP}.csv"
RUN_DIR="$HOME/bench_hist_runs_${LABEL}_${STAMP}"
mkdir -p "$RUN_DIR"

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
    local from=$1
    local val
    if [ "$from" -eq 0 ]; then val=0; else val=$((from - 1)); fi
    redis-cli -u "$REDIS_URL" SET LATEST_PROCESSED_BLOCK_NUMBER "$val" >/dev/null 2>&1
}

run_sample() {
    local sid=$1 from=$2 to=$3
    local out="$RUN_DIR/s${sid}_${from}_${to}.out"
    set_cursor "$from"
    local t0 t1 ms rc=0
    t0=$(date +%s%3N)
    timeout 300s env \
        MODE=local EVM_CHAIN_ID=1 \
        CM_CONNECTION_URL="$REDIS_URL" \
        SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 \
        SCYLLA_DB_KEYSPACE=eth \
        SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD=cassandra \
        SAVE_EVERY=24 SCYLLA_CHUNK_BUCKETS=24 \
        "$BINARY" --from="$from" --to="$to" >"$out" 2>&1 || rc=$?
    t1=$(date +%s%3N)
    ms=$((t1 - t0))
    local blk_s=0
    [ "$ms" -gt 0 ] && blk_s=$(python3 -c "print(round($SAMPLE*1000/$ms,2))" 2>/dev/null || echo 0)
    printf '%s,%s,%s,%s,%s,%s\n' "$sid" "$from" "$to" "$ms" "$blk_s" "$rc" >> "$CSV"
    printf '%6s | %10d | %10d | %7d | %7s | %2d\n' "$sid" "$from" "$to" "$ms" "$blk_s" "$rc" | tee -a "$LOG"
}

{
    echo "bench_hist  binary=$BINARY  label=$LABEL"
    echo "sample=${SAMPLE}  step=${STEP}  range=0..${MAX_START}"
    echo "log=$LOG  csv=$CSV"
    echo ""
    printf '%6s | %10s | %10s | %7s | %7s | rc\n' sid from to ms blk_s
    printf '%s\n' '-------+------------+------------+---------+---------+---'
} | tee "$LOG"
printf 'sid,from,to,ms,blk_s,rc\n' > "$CSV"

truncate_tables

sid=0
from=0
while [ "$from" -le "$MAX_START" ]; do
    to=$((from + SAMPLE - 1))
    run_sample "$sid" "$from" "$to"
    sid=$((sid + 1))
    from=$((from + STEP))
done

# Inline summary
python3 - "$CSV" <<'PY' | tee -a "$LOG"
import csv, statistics, sys

rows = []
with open(sys.argv[1]) as f:
    for r in csv.DictReader(f):
        r["from"] = int(r["from"]); r["to"] = int(r["to"])
        r["blk_s"] = float(r["blk_s"]); r["rc"] = int(r["rc"])
        rows.append(r)

ok = [r for r in rows if r["rc"] == 0 and r["blk_s"] > 0]
interval = 100_000.0
total_s = sum(interval / r["blk_s"] for r in ok)

print(f"\nsamples_ok={len(ok)}/{len(rows)}")
print(f"estimated_25M_hours={total_s/3600:.2f}  ({total_s/86400:.2f} days)")
print(f"avg_blk_s={statistics.mean(r['blk_s'] for r in ok):.1f}")
print(f"median_blk_s={statistics.median(r['blk_s'] for r in ok):.1f}")

ZONES = [
    (0,        4_000_000,  "0-4M"),
    (4_000_000, 8_000_000, "4-8M"),
    (8_000_000, 12_000_000,"8-12M"),
    (12_000_000,16_000_000,"12-16M"),
    (16_000_000,20_000_000,"16-20M"),
    (20_000_000,25_200_000,"20-25M"),
]
print("\nzone           | avg blk/s | median | est_hours")
print("---------------+-----------+--------+----------")
for lo, hi, name in ZONES:
    xs = [r["blk_s"] for r in ok if lo <= r["from"] < hi]
    if not xs: continue
    sec = sum(100_000/s for s in xs)
    print(f"{name:<15}| {statistics.mean(xs):9.1f} | {statistics.median(xs):6.1f} | {sec/3600:9.2f}")

slowest = sorted(ok, key=lambda r: r["blk_s"])[:10]
print("\nslowest_10:")
for r in slowest:
    print(f"  {r['from']}-{r['to']}: {r['blk_s']:.1f} blk/s")
PY

echo ""
echo "Done.  log=$LOG  csv=$CSV"
