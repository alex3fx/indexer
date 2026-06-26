import os, sys, threading
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider

# Finds EXACT missing block numbers (chunk=(block%lanes)+lanes*(block/era))
# by scanning every (lane, era) chunk partition in [FROM_BLOCK, TO_BLOCK) and checking that present
# block numbers are exactly `lanes` apart (within one lane's chunk, blocks are a strict arithmetic
# sequence — any larger gap means missing rows).
#
# Usage: python3 find_missing_blocks.py FROM_BLOCK TO_BLOCK [LANES=64] [ERA=32000] [TABLE=blocks] [CONCURRENCY=64]
# Prints one missing block number per line to stdout (so it can be piped straight into a backfill step).
# Scylla connection: SCYLLA_DB_HOST (default 127.0.0.1), SCYLLA_DB_USERNAME (REQUIRED),
# SCYLLA_DB_PASSWORD (REQUIRED), SCYLLA_DB_KEYSPACE (default pol).

FROM_BLOCK = int(sys.argv[1])
TO_BLOCK = int(sys.argv[2])
LANES = int(sys.argv[3]) if len(sys.argv) > 3 else 64
ERA = int(sys.argv[4]) if len(sys.argv) > 4 else 32000
TABLE = sys.argv[5] if len(sys.argv) > 5 else "blocks"
BLOCK_COL = "number" if TABLE == "blocks" else "block_number"
CONCURRENCY = int(sys.argv[6]) if len(sys.argv) > 6 else 64

SCYLLA_USERNAME = os.environ.get("SCYLLA_DB_USERNAME")
if not SCYLLA_USERNAME:
    sys.exit("SCYLLA_DB_USERNAME must be set in the environment (no default)")
SCYLLA_PASSWORD = os.environ.get("SCYLLA_DB_PASSWORD")
if not SCYLLA_PASSWORD:
    sys.exit("SCYLLA_DB_PASSWORD must be set in the environment (no default)")
auth = PlainTextAuthProvider(username=SCYLLA_USERNAME, password=SCYLLA_PASSWORD)
profile = ExecutionProfile(request_timeout=60)
cluster = Cluster(
    [os.environ.get("SCYLLA_DB_HOST", "127.0.0.1")],
    auth_provider=auth, execution_profiles={EXEC_PROFILE_DEFAULT: profile},
)
session = cluster.connect(os.environ.get("SCYLLA_DB_KEYSPACE", "pol"))

era_lo, era_hi = FROM_BLOCK // ERA, (TO_BLOCK - 1) // ERA

jobs = []
for era in range(era_lo, era_hi + 1):
    es, ee = era * ERA, (era + 1) * ERA
    for lane in range(LANES):
        chunk = lane + LANES * era
        # Expected block numbers in this (lane, era): lane+ERA*era_offset for offset in range(ERA//LANES)
        # Clip to [FROM_BLOCK, TO_BLOCK).
        jobs.append((chunk, lane, es, ee))

missing_lock = threading.Lock()
missing = []
failed_chunks = []
sem = threading.Semaphore(CONCURRENCY)


def check_chunk(chunk, lane, es, ee):
    with sem:
        try:
            rows = session.execute(
                f"SELECT {BLOCK_COL} FROM {TABLE} WHERE chunk=%s", (chunk,), timeout=55
            )
            present = set(r[0] for r in rows)
        except Exception as e:
            with missing_lock:
                failed_chunks.append((chunk, str(e)))
            return
        expected = []
        b = es + lane if es % LANES == lane % LANES or True else None
        # First expected block in [es, ee) with (b % LANES) == lane:
        start = es - (es % LANES) + lane
        if start < es:
            start += LANES
        b = start
        while b < ee:
            if FROM_BLOCK <= b < TO_BLOCK:
                expected.append(b)
            b += LANES
        local_missing = [b for b in expected if b not in present]
        if local_missing:
            with missing_lock:
                missing.extend(local_missing)


threads = [threading.Thread(target=check_chunk, args=j) for j in jobs]
i = 0
while i < len(threads):
    batch = threads[i : i + CONCURRENCY]
    for t in batch:
        t.start()
    for t in batch:
        t.join()
    i += CONCURRENCY

missing.sort()
for b in missing:
    print(b)

sys.stderr.write(f"Total missing: {len(missing)}\n")
if failed_chunks:
    sys.stderr.write(f"Chunks that failed to query (treated as not-checked, NOT counted as missing): {len(failed_chunks)}\n")
    for c, e in failed_chunks[:20]:
        sys.stderr.write(f"  chunk={c}: {e}\n")

cluster.shutdown()
