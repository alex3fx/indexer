#!/usr/bin/env python3
"""
Offline metric parser for devindexer benchmark logs.

Usage:
  python3 analyze.py hist   <log_file>   [--blocks N] [--step N]
  python3 analyze.py rt     <log_file>
  python3 analyze.py rt_csv <log_file>   # CSV output for spreadsheets

Historical log expected lines:
  Accum X→X: saved=X save=Xms | X blk/s avg
  Historical sync done: Xms  saved_blocks=X

Realtime log expected lines:
  [rt] blk=N tx=N log=N itx=N kb=N+N+N | fetch=N parse=N xform=N save=N cursor=N | total=Nms
"""

import re
import sys
import statistics
from pathlib import Path


# ─── Historical ──────────────────────────────────────────────────────────────

def parse_hist(log_path: str, sample_blocks: int = 100, step: int = 100_000):
    ACCUM_RE = re.compile(
        r'Accum (\d+).(\d+):\s+saved=(\d+)\s+save=([\d.]+)ms\s*\|\s*([\d.]+) blk/s avg'
    )
    DONE_RE = re.compile(r'Historical sync done:\s*([\d.]+)ms\s+saved_blocks=(\d+)')

    entries = []
    done_ms = None
    done_blocks = None

    for ln in Path(log_path).read_text().splitlines():
        m = ACCUM_RE.search(ln)
        if m:
            entries.append(dict(
                blk_start = int(m.group(1)),
                blk_end   = int(m.group(2)),
                saved     = int(m.group(3)),
                save_ms   = float(m.group(4)),
                blk_s     = float(m.group(5)),
            ))
        m2 = DONE_RE.search(ln)
        if m2:
            done_ms     = float(m2.group(1))
            done_blocks = int(m2.group(2))

    if not entries and done_ms is None:
        print(f"No historical metrics found in {log_path}", file=sys.stderr)
        return

    speeds = [e["blk_s"] for e in entries] if entries else []

    print(f"=== Historical: {log_path} ===")
    if done_ms and done_blocks:
        ms_per_blk = done_ms / done_blocks
        blk_s      = done_blocks * 1000 / done_ms
        print(f"total_ms={done_ms:.0f}  saved={done_blocks}  ms/blk={ms_per_blk:.1f}  blk/s={blk_s:.2f}")
        est_25m_h = 25_000_000 / blk_s / 3600
        print(f"est_25M={est_25m_h:.1f}h  ({est_25m_h/24:.1f}d)")

    if speeds:
        print(f"\nAccum batches: n={len(speeds)}  avg={statistics.mean(speeds):.1f}  "
              f"med={statistics.median(speeds):.1f}  "
              f"min={min(speeds):.1f}  max={max(speeds):.1f}")

        ZONES = [
            (0,         4_000_000,  "0-4M  "),
            (4_000_000, 8_000_000,  "4-8M  "),
            (8_000_000, 12_000_000, "8-12M "),
            (12_000_000,16_000_000, "12-16M"),
            (16_000_000,20_000_000, "16-20M"),
            (20_000_000,25_200_000, "20-25M"),
        ]
        print(f"\n{'zone':<8} {'avg blk/s':>10} {'median':>8} {'min':>7} {'max':>7} {'est_h':>7}")
        print("-" * 55)
        total_est_s = 0.0
        for lo, hi, name in ZONES:
            xs = [e["blk_s"] for e in entries if lo <= e["blk_start"] < hi]
            if not xs:
                continue
            sec = sum(step / s for s in xs)
            total_est_s += sec
            print(f"{name:<8} {statistics.mean(xs):10.1f} {statistics.median(xs):8.1f} "
                  f"{min(xs):7.1f} {max(xs):7.1f} {sec/3600:7.2f}")
        if total_est_s > 0:
            print(f"\nest_total_25M={total_est_s/3600:.1f}h  ({total_est_s/86400:.1f}d)")


# ─── Realtime ─────────────────────────────────────────────────────────────────

RT_RE = re.compile(
    r'\[rt\] blk=(\d+) tx=(\d+) log=(\d+) itx=(\d+) kb=(\d+)\+(\d+)\+(\d+)'
    r' \| fetch=([\d.]+) parse=([\d.]+) xform=([\d.]+) save=([\d.]+)'
    r' cursor=([\d.]+) \| total=([\d.]+)ms'
)

def parse_rt_rows(log_path: str):
    rows = []
    for ln in Path(log_path).read_text().splitlines():
        m = RT_RE.search(ln)
        if not m:
            continue
        kb = int(m.group(5)) + int(m.group(6)) + int(m.group(7))
        rows.append(dict(
            blk    = int(m.group(1)),
            tx     = int(m.group(2)),
            log    = int(m.group(3)),
            itx    = int(m.group(4)),
            kb     = kb,
            fetch  = float(m.group(8)),
            parse  = float(m.group(9)),
            xform  = float(m.group(10)),
            save   = float(m.group(11)),
            cursor = float(m.group(12)),
            total  = float(m.group(13)),
        ))
    return rows

def print_rt(log_path: str):
    rows = parse_rt_rows(log_path)
    if not rows:
        print(f"No [rt] lines found in {log_path}", file=sys.stderr)
        return

    def avg(xs):    return statistics.mean(xs)
    def med(xs):    return statistics.median(xs)
    def uskb(ms_l, kb_l):
        return [ms*1000/kb for ms, kb in zip(ms_l, kb_l) if kb > 0]

    kbs     = [r["kb"]     for r in rows]
    fetches = [r["fetch"]  for r in rows]
    parses  = [r["parse"]  for r in rows]
    xforms  = [r["xform"]  for r in rows]
    saves   = [r["save"]   for r in rows]
    totals  = [r["total"]  for r in rows]

    fusk = uskb(fetches, kbs)
    pusk = uskb(parses,  kbs)
    susk = uskb(saves,   kbs)
    tusk = uskb(totals,  kbs)

    print(f"=== Realtime: {log_path}  blocks={len(rows)} ===")
    print()
    print(f"{'stage':<8}  {'avg ms':>8}  {'p50 ms':>8}  {'avg µs/KB':>10}  {'p50 µs/KB':>10}")
    print(f"{'':-<8}  {'':->8}  {'':->8}  {'':->10}  {'':->10}")
    for name, ms_l, usk_l in [
        ("fetch",  fetches, fusk),
        ("parse",  parses,  pusk),
        ("xform",  xforms,  None),
        ("save",   saves,   susk),
        ("total",  totals,  tusk),
    ]:
        usk_avg = f"{avg(usk_l):10.2f}" if usk_l else f"{'—':>10}"
        usk_med = f"{med(usk_l):10.2f}" if usk_l else f"{'—':>10}"
        print(f"{name:<8}  {avg(ms_l):8.1f}  {med(ms_l):8.1f}  {usk_avg}  {usk_med}")

    print()
    print(f"avg_kb={avg(kbs):.0f}  headroom={12000/avg(totals):.0f}x  "
          f"avg_tx={avg(r['tx'] for r in rows):.0f}  "
          f"avg_itx={avg(r['itx'] for r in rows):.0f}")

def print_rt_csv(log_path: str):
    rows = parse_rt_rows(log_path)
    print("blk,tx,log,itx,kb,fetch_ms,parse_ms,xform_ms,save_ms,cursor_ms,total_ms,"
          "fetch_uskb,parse_uskb,save_uskb,total_uskb")
    for r in rows:
        kb = r["kb"]
        f  = lambda ms: f"{ms*1000/kb:.2f}" if kb > 0 else ""
        print(f"{r['blk']},{r['tx']},{r['log']},{r['itx']},{kb},"
              f"{r['fetch']},{r['parse']},{r['xform']},{r['save']},{r['cursor']},{r['total']},"
              f"{f(r['fetch'])},{f(r['parse'])},{f(r['save'])},{f(r['total'])}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def usage():
    print(__doc__)
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()
    cmd, log = sys.argv[1], sys.argv[2]
    if cmd == "hist":
        blocks = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        step   = int(sys.argv[4]) if len(sys.argv) > 4 else 100_000
        parse_hist(log, blocks, step)
    elif cmd == "rt":
        print_rt(log)
    elif cmd == "rt_csv":
        print_rt_csv(log)
    else:
        usage()
