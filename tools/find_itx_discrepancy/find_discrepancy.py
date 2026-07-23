#!/usr/bin/env python3
"""
Query reth trace_block for sample blocks (1 per ~1M era in 0-25.4M)
and print per-block trace counts so we can compare with Dune (Geth) later.

Filter applied to reth traces matches BC transformer logic:
  - action.from is non-empty  (excludes suicide/reward)
  - action.value is not None  (excludes reward)
  - transactionHash is not None  (excludes reward/miner reward)

Output: CSV block_number,reth_filtered,reth_total_raw
"""
import json
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

RETH = "http://100.64.0.60:8545"

# Sample blocks: 1 per million era.
# Known high-discrepancy blocks from previous investigation are used directly;
# others are midpoints (500k into each era).
SAMPLE_BLOCKS = [
    500_000,      # M0  reth_specific=+157,664/era — expect delta
    1_500_000,    # M1  reth_specific=+142,903/era — expect delta
    2_500_000,    # M2  reth_specific=+18,650/era
    3_500_000,    # M3  reth_specific=+3,532/era — may not have per-block delta
    4_500_000,    # M4  reth_specific=+8,172/era
    5_500_000,    # M5  reth_specific=+6,207/era
    6_500_000,    # M6  reth_specific=+15,097/era
    7_500_000,    # M7  reth_specific=+12,450/era
    8_500_000,    # M8  reth_specific=+12,038/era
    9_500_000,    # M9  reth_specific=+14,102/era
    10_366_004,   # M10 KNOWN: reth=237, Dune(old)=200, reth_specific=+37 (CALL-to-EOA, contract 0x98ad263a)
    11_500_000,   # M11 reth_specific=+30,950/era
    12_500_000,   # M12 reth_specific=+7,324/era
    13_500_000,   # M13 reth_specific=+21,037/era
    14_500_000,   # M14 reth_specific=+4,510/era
    15_500_000,   # M15 reth_specific=+18,380/era
    16_500_000,   # M16 reth_specific=+5,198/era
    17_500_000,   # M17 reth_specific=+6,656/era
    18_500_000,   # M18 reth_specific=+10,897/era
    19_500_000,   # M19 reth_specific=+4,285/era
    20_500_000,   # M20 reth_specific=+5,312/era
    21_500_000,   # M21 reth_specific=+5,778/era
    22_500_000,   # M22 reth_specific=+65,608/era — expect delta
    23_223_000,   # M23 KNOWN HIGH: per-1000-block bucket 23223k has reth_specific=+2,176
    24_706_170,   # M24 KNOWN: reth=968, Dune(old)=949, delta=+19 (19 direct EOA-to-0x01 calls)
    25_100_000,   # M25 reth_specific=+4/era — near zero
]

# Filter: same as BC transformer
# action.from != "" AND action.value is not None AND transactionHash is not None
ZERO_ADDR = "0x0000000000000000000000000000000000000000"

def apply_filter(trace):
    action = trace.get("action", {}) or {}
    frm = action.get("from", "") or ""
    val = action.get("value")
    tx_hash = trace.get("transactionHash")
    return frm != "" and val is not None and tx_hash is not None


def query_block(block_num):
    hex_b = hex(block_num)
    try:
        r = requests.post(RETH, json={
            "jsonrpc": "2.0",
            "method": "trace_block",
            "params": [hex_b],
            "id": 1,
        }, timeout=180)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            return block_num, -1, -1, f"RPC error: {data['error']}"
        traces = data.get("result") or []
        total = len(traces)
        filtered = sum(1 for t in traces if apply_filter(t))
        return block_num, filtered, total, ""
    except Exception as e:
        return block_num, -1, -1, str(e)


def main():
    print("block_number,reth_filtered,reth_total_raw,error", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(query_block, b): b for b in SAMPLE_BLOCKS}
        for fut in as_completed(futures):
            block_num, filtered, total, err = fut.result()
            results.append((block_num, filtered, total, err))
            print(f"{block_num},{filtered},{total},{err}", flush=True)

    # Print sorted summary at end
    print("\n--- sorted by block ---", flush=True)
    print("block_number,reth_filtered,reth_total_raw", flush=True)
    for b, f, t, e in sorted(results):
        if e:
            print(f"{b},ERROR,ERROR  # {e}", flush=True)
        else:
            print(f"{b},{f},{t}", flush=True)


if __name__ == "__main__":
    main()
