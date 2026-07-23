#!/usr/bin/env python3
"""
Query reth trace_block for 1 sample per 100k blocks in 0-25,400,000.
Saves sorted CSV to find_discrepancy_100k_reth.csv for later Dune comparison.

Filter matches BC transformer: from!="" AND value!=None AND txHash!=None
"""
import requests
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

RETH = "http://100.64.0.60:8545"
MAX_WORKERS = 12
OUTPUT = "find_discrepancy_100k_reth.csv"

# Sample at midpoint of each 100k band: 50k, 150k, 250k, ..., 25,350k
# Using 50000 offset to avoid extremely empty genesis-adjacent blocks
SAMPLE_BLOCKS = [i * 100_000 + 50_000 for i in range(254)]  # 50000..25350000

def apply_filter(trace):
    action = trace.get("action", {}) or {}
    return (
        (action.get("from") or "") != ""
        and action.get("value") is not None
        and trace.get("transactionHash") is not None
    )

def query_block(block_num):
    try:
        r = requests.post(RETH, json={
            "jsonrpc": "2.0", "method": "trace_block",
            "params": [hex(block_num)], "id": 1,
        }, timeout=180)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            return block_num, -1, -1, f"RPC:{data['error'].get('message','?')}"
        traces = data.get("result") or []
        return block_num, sum(1 for t in traces if apply_filter(t)), len(traces), ""
    except Exception as e:
        return block_num, -1, -1, str(e)[:80]

def main():
    print(f"Querying {len(SAMPLE_BLOCKS)} blocks from reth...", flush=True)
    results = {}
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(query_block, b): b for b in SAMPLE_BLOCKS}
        for fut in as_completed(futures):
            block_num, filtered, total, err = fut.result()
            results[block_num] = (filtered, total, err)
            done += 1
            if done % 20 == 0 or done == len(SAMPLE_BLOCKS):
                print(f"  {done}/{len(SAMPLE_BLOCKS)} done", flush=True)

    rows = sorted(results.items())
    with open(OUTPUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["block_number", "reth_filtered", "reth_total_raw", "error"])
        for b, (filt, total, err) in rows:
            w.writerow([b, filt, total, err])

    print(f"\nSaved to {OUTPUT}", flush=True)
    errors = [(b, e) for b, (_, _, e) in rows if e]
    if errors:
        print(f"ERRORS ({len(errors)}):")
        for b, e in errors:
            print(f"  {b}: {e}")

    # Print summary stats
    ok = [(b, filt) for b, (filt, _, e) in rows if not e]
    nonzero = [(b, filt) for b, filt in ok if filt > 0]
    print(f"\nOK blocks: {len(ok)}, non-zero filtered: {len(nonzero)}")
    print("\n--- Top 20 by reth_filtered ---")
    for b, filt in sorted(nonzero, key=lambda x: -x[1])[:20]:
        print(f"  block {b:>10,}: reth={filt}")

if __name__ == "__main__":
    main()
