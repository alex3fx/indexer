#!/usr/bin/env python3
"""Groups a sorted list of missing block numbers into contiguous re-index spans.

Usage:
  python3 make_spans_eth.py <missing_blocks.txt> [gap_tolerance=500] [padding=10]

Reads one block number per line from <missing_blocks.txt> (output of find_missing_blocks.py).
Groups any two consecutive missing blocks within <gap_tolerance> of each other into a single
span, then expands each span by <padding> blocks on each side (clamped to >=0).

Writes two output files next to the input:
  <basename>_spans_07.txt  — spans with to_block <= 12700000
  <basename>_spans_60.txt  — spans with from_block > 12700000
  (spans crossing the boundary are split at 12700000)

Each output file has one "FROM TO" line per span (inclusive), ready for backfill_spans_eth.sh.
"""
import sys, os

if len(sys.argv) < 2:
    sys.exit("usage: make_spans_eth.py <missing_blocks.txt> [gap_tolerance=500] [padding=10]")

infile = sys.argv[1]
GAP_TOL = int(sys.argv[2]) if len(sys.argv) > 2 else 500
PADDING = int(sys.argv[3]) if len(sys.argv) > 3 else 10
SPLIT = 12_700_000  # blocks 0..SPLIT go to .07, SPLIT+1..∞ go to .60

blocks = []
with open(infile) as f:
    for line in f:
        line = line.strip()
        if line:
            blocks.append(int(line))

if not blocks:
    print("No missing blocks found — nothing to do.", file=sys.stderr)
    sys.exit(0)

blocks.sort()

# Group into spans
spans = []
lo = hi = blocks[0]
for b in blocks[1:]:
    if b - hi <= GAP_TOL:
        hi = b
    else:
        spans.append((lo, hi))
        lo = hi = b
spans.append((lo, hi))

# Apply padding
spans = [(max(0, s - PADDING), e + PADDING) for s, e in spans]

# Split at SPLIT boundary
spans_07 = []
spans_60 = []
for s, e in spans:
    if e <= SPLIT:
        spans_07.append((s, e))
    elif s > SPLIT:
        spans_60.append((s, e))
    else:
        # spans crossing the boundary
        spans_07.append((s, SPLIT))
        spans_60.append((SPLIT + 1, e))

base = os.path.splitext(infile)[0]
out07 = base + "_spans_07.txt"
out60 = base + "_spans_60.txt"

with open(out07, "w") as f:
    for s, e in spans_07:
        f.write(f"{s} {e}\n")

with open(out60, "w") as f:
    for s, e in spans_60:
        f.write(f"{s} {e}\n")

total_07 = sum(e - s + 1 for s, e in spans_07)
total_60 = sum(e - s + 1 for s, e in spans_60)
print(f"Input: {len(blocks)} missing blocks → {len(spans)} spans (gap_tol={GAP_TOL}, pad={PADDING})")
print(f"  node .07 (0→{SPLIT}): {len(spans_07)} spans, ~{total_07} blocks → {out07}")
print(f"  node .60 ({SPLIT+1}→∞): {len(spans_60)} spans, ~{total_60} blocks → {out60}")
