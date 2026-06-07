#!/usr/bin/env bash
# Realtime benchmark: run indexer in realtime mode for SECS seconds.
# Captures [rt] log lines and computes per-stage ms and µs/KB statistics.
#
# Usage: bench_rt.sh [binary] [label] [seconds]
#   binary   path to devindexer binary  (default: ~/raw_unified)
#   label    tag in output filenames    (default: date stamp)
#   seconds  run duration               (default: 300)
set -euo pipefail

BINARY="${1:-$HOME/raw_unified}"
LABEL="${2:-$(date +%Y%m%d_%H%M%S)}"
SECS="${3:-300}"
REDIS_URL="redis://:ZCy8k4G6pcRYVFfm@127.0.0.1:6379/0"

STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="$HOME/bench_rt_${LABEL}_${STAMP}.log"

truncate_tables() {
    docker exec scylla cqlsh -u cassandra -p cassandra \
        -e "USE eth;
            TRUNCATE blocks; TRUNCATE transactions; TRUNCATE logs;
            TRUNCATE internal_transactions; TRUNCATE contracts;
            TRUNCATE contracts_by_addresses; TRUNCATE block_completions;" \
        >/dev/null 2>&1
    sleep 2
}

get_head() {
    curl -sf -X POST http://100.64.0.7:8545 \
        -H 'Content-Type: application/json' \
        -d '{"id":1,"jsonrpc":"2.0","method":"eth_blockNumber","params":[]}' \
        | python3 -c "import sys,json; print(int(json.load(sys.stdin)['result'],16))"
}

set_cursor() {
    redis-cli -u "$REDIS_URL" SET LATEST_PROCESSED_BLOCK_NUMBER "$1" >/dev/null 2>&1
}

{
    echo "bench_rt  binary=$BINARY  label=$LABEL  secs=$SECS"
    echo ""
} | tee "$LOG"

truncate_tables
HEAD=$(get_head)
echo "chain_head=$HEAD" | tee -a "$LOG"
set_cursor "$HEAD"

t0=$(date +%s%3N)
timeout "${SECS}s" env \
    MODE=local EVM_CHAIN_ID=1 \
    CM_CONNECTION_URL="$REDIS_URL" \
    SCYLLA_DB_HOST=127.0.0.1 SCYLLA_DB_PORT=9042 \
    SCYLLA_DB_KEYSPACE=eth \
    SCYLLA_DB_USERNAME=cassandra SCYLLA_DB_PASSWORD=cassandra \
    WS_DELAY_MS=100 \
    "$BINARY" 2>&1 | tee -a "$LOG" || true
t1=$(date +%s%3N)
wall_ms=$((t1 - t0))

LAST=$(redis-cli -u "$REDIS_URL" GET LATEST_PROCESSED_BLOCK_NUMBER 2>/dev/null || echo "$HEAD")
N=$(( LAST - HEAD ))

# Parse [rt] lines and compute statistics
python3 - "$LOG" "$N" "$HEAD" "$LAST" <<'PY'
import re, sys, statistics

log_path = sys.argv[1]
n_blocks = int(sys.argv[2])
head     = int(sys.argv[3])
last     = int(sys.argv[4])

RT_RE = re.compile(
    r'\[rt\] blk=(\d+) tx=(\d+) log=(\d+) itx=(\d+) kb=(\d+)\+(\d+)\+(\d+)'
    r' \| fetch=([\d.]+) parse=([\d.]+) xform=([\d.]+) save=([\d.]+)'
    r' cursor=([\d.]+) \| total=([\d.]+)ms'
)

rows = []
for ln in open(log_path):
    m = RT_RE.search(ln)
    if not m:
        continue
    blk, tx, log_, itx = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    kb = int(m.group(5)) + int(m.group(6)) + int(m.group(7))
    fetch, parse, xform, save, cursor_, total = (
        float(m.group(i)) for i in range(8, 14))
    rows.append(dict(blk=blk, tx=tx, log=log_, itx=itx, kb=kb,
                     fetch=fetch, parse=parse, xform=xform,
                     save=save, cursor=cursor_, total=total))

if not rows:
    print("\nNo [rt] lines found in log.")
    sys.exit(0)

def avg(xs): return statistics.mean(xs)
def med(xs): return statistics.median(xs)
def us_kb(ms_list, kb_list):
    pairs = [(ms, kb) for ms, kb in zip(ms_list, kb_list) if kb > 0]
    return [ms * 1000 / kb for ms, kb in pairs]

kbs     = [r["kb"]     for r in rows]
fetches = [r["fetch"]  for r in rows]
parses  = [r["parse"]  for r in rows]
xforms  = [r["xform"]  for r in rows]
saves   = [r["save"]   for r in rows]
totals  = [r["total"]  for r in rows]

fusk = us_kb(fetches, kbs)
pusk = us_kb(parses,  kbs)
susk = us_kb(saves,   kbs)
tusk = us_kb(totals,  kbs)

print()
print(f"=== bench_rt RESULT  blocks={len(rows)}  from={head+1}  to={last} ===")
print()
print(f"{'stage':<8}  {'avg ms':>8}  {'med ms':>8}  {'avg µs/KB':>10}  {'med µs/KB':>10}")
print(f"{'':-<8}  {'':->8}  {'':->8}  {'':->10}  {'':->10}")
print(f"{'fetch':<8}  {avg(fetches):8.1f}  {med(fetches):8.1f}  {avg(fusk):10.2f}  {med(fusk):10.2f}")
print(f"{'parse':<8}  {avg(parses):8.1f}  {med(parses):8.1f}  {avg(pusk):10.2f}  {med(pusk):10.2f}")
print(f"{'xform':<8}  {avg(xforms):8.1f}  {med(xforms):8.1f}  {'—':>10}  {'—':>10}")
print(f"{'save':<8}  {avg(saves):8.1f}  {med(saves):8.1f}  {avg(susk):10.2f}  {med(susk):10.2f}")
print(f"{'total':<8}  {avg(totals):8.1f}  {med(totals):8.1f}  {avg(tusk):10.2f}  {med(tusk):10.2f}")
print()
print(f"avg_kb/blk={avg(kbs):.0f}  eth_block_time=12000ms  headroom={12000/avg(totals):.0f}x")
PY

echo ""
echo "blocks_captured=$N  wall_ms=$wall_ms"
echo "log=$LOG"
