#!/usr/bin/env python3
"""
Compare two bench_hist.sh CSV files (before/after refactoring).

Usage:
  python3 compare_runs.py before.csv after.csv
"""
import csv, statistics, sys
from pathlib import Path

def load(path):
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(dict(
                sid   = int(r['sid']),
                from_ = int(r['from']),
                to    = int(r['to']),
                ms    = int(r['ms']),
                blk_s = float(r['blk_s']),
                rc    = int(r['rc']),
            ))
    return rows

def zone_name(from_):
    for lo, hi, name in ZONES:
        if lo <= from_ < hi:
            return name
    return "?"

ZONES = [
    (0,         1_000_000,  "0-1M  "),
    (1_000_000,  4_000_000, "1-4M  "),
    (4_000_000,  8_000_000, "4-8M  "),
    (8_000_000, 12_000_000, "8-12M "),
    (12_000_000,16_000_000, "12-16M"),
    (16_000_000,20_000_000, "16-20M"),
    (20_000_000,23_000_000, "20-23M"),
    (23_000_000,25_200_000, "23-25M"),
]

def zone_stats(rows, lo, hi):
    xs = [r['blk_s'] for r in rows if lo <= r['from_'] < hi and r['rc'] == 0 and r['blk_s'] > 0]
    if not xs:
        return None
    return dict(n=len(xs), avg=statistics.mean(xs), med=statistics.median(xs),
                min=min(xs), max=max(xs))

if len(sys.argv) < 3:
    print(__doc__); sys.exit(1)

before = load(sys.argv[1])
after  = load(sys.argv[2])

ok_b = [r for r in before if r['rc'] == 0 and r['blk_s'] > 0]
ok_a = [r for r in after  if r['rc'] == 0 and r['blk_s'] > 0]

interval = 100_000.0
est_b = sum(interval / r['blk_s'] for r in ok_b)
est_a = sum(interval / r['blk_s'] for r in ok_a)

print(f"{'':30} {'BEFORE':>10} {'AFTER':>10} {'DELTA':>8}")
print("-" * 62)
print(f"{'samples ok':<30} {len(ok_b):>10} {len(ok_a):>10}")
print(f"{'overall avg blk/s':<30} {statistics.mean(r['blk_s'] for r in ok_b):>10.1f} "
      f"{statistics.mean(r['blk_s'] for r in ok_a):>10.1f}")
print(f"{'overall median blk/s':<30} {statistics.median(r['blk_s'] for r in ok_b):>10.1f} "
      f"{statistics.median(r['blk_s'] for r in ok_a):>10.1f}")
print(f"{'est 25M hours':<30} {est_b/3600:>10.1f} {est_a/3600:>10.1f} "
      f"{'%+.1f%%' % ((est_a-est_b)/est_b*100):>8}")
print()

print(f"{'zone':<8} {'bef med':>8} {'aft med':>8} {'bef avg':>8} {'aft avg':>8} {'delta%':>7}")
print("-" * 50)
for lo, hi, name in ZONES:
    sb = zone_stats(ok_b, lo, hi)
    sa = zone_stats(ok_a, lo, hi)
    if sb is None or sa is None:
        continue
    delta = (sa['avg'] - sb['avg']) / sb['avg'] * 100
    print(f"{name:<8} {sb['med']:>8.1f} {sa['med']:>8.1f} "
          f"{sb['avg']:>8.1f} {sa['avg']:>8.1f} {delta:>+7.1f}%")

print()
print("Note: 100-block samples underestimate steady-state throughput (pipeline never")
print("reaches full parallelism). Relative comparison is valid; absolute est is ~2-3x high.")
print("Use bench_hist_25m.sh (2000 blocks) for calibrated absolute estimates.")
